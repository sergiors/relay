// Package worker is the long-running Relay runtime, started via `relay start`.
// It loads functions, builds their images, reconciles them live, and consumes
// the Redis stream, blocking until signalled. Configuration comes entirely
// from the environment. It can also expose a Prometheus /metrics endpoint (see
// internal/metrics), gated on the METRICS_ADDR environment variable, and
// flushes the registry into the local state database on a fixed 5-second
// cadence: stats accumulate in memory (the registry is the single source of
// truth), Prometheus reflects them immediately, and SQLite receives the current
// absolute snapshot every interval. The stats flusher (which serializes the
// flush and the reset) lets the socket's `reset stats` command restart the
// persisted totals from zero by capturing a worker-owned baseline, without ever
// mutating the monotonic Prometheus counters. The cron scheduler (internal/cron)
// joins the same lifecycle: constructed, seeded from the loaded schedules,
// started, and stopped on shutdown. Schedules are coordinated through Redis:
// every worker evaluates the cron locally but publishes one stream entry per
// occurrence cluster-wide (atomic publish-if-new), and the consumer group
// delivers that single entry to exactly one worker for execution (at-least-once).
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/config"
	"relay/internal/cron"
	"relay/internal/function"
	gitwh "relay/internal/git/webhook"
	"relay/internal/metrics"
	"relay/internal/reconciler"
	"relay/internal/routing"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/schedule"
	"relay/internal/secrets"
	"relay/internal/state"
	"relay/internal/stream"
)

// statsFlushInterval is the fixed SQLite snapshot cadence. Telemetry, not
// event-processing state: Prometheus stays live in-process, while SQLite
// receives the current absolute snapshot every interval. Deliberately NOT
// env/flag-configurable — a fixed cadence keeps the durability model simple
// (a hard crash loses at most one interval of telemetry).
const statsFlushInterval = 5 * time.Second

// reconcileTimeout bounds every bounded service-reconcile daemon operation:
// the startup orphan container sweep, the startup image keep-set list, the
// coordinator's per-removal operation context (RemoveAndWait/EnqueueRemove), and
// — injected into the ServiceReconciler — each normal pre-build and post-build
// Docker operation inside a per-function Apply. Each such operation gets its own
// fresh bound so one slow Docker call cannot consume the budget of the calls
// that follow. It deliberately does NOT bound Dockerfile builds: a build is
// bounded by runtime.buildTimeout (10m) on a context rooted in the worker
// lifecycle, so a slow image build can never be cut off by this short reconcile
// budget. Apply receives the worker lifecycle context (NOT this constant wrapped
// around the whole pass) and the reconciler derives the per-operation bounds
// from it. The 5s shutdown bounds are a separate, deliberately shorter bound
// (see Run's shutdown tail), not this constant.
const reconcileTimeout = 30 * time.Second

// shutdownServiceTimeout bounds the service-container cleanup during graceful
// shutdown: stopping and removing this worker's persistent service containers
// must not block shutdown forever.
const shutdownServiceTimeout = 30 * time.Second

// effectiveMaxConcurrency mirrors the runner's SetMaxConcurrency normalization
// (<1 → runner.DefaultMaxConcurrency) so the "Concurrency limits" log reflects
// the value actually enforced regardless of the configured raw value.
func effectiveMaxConcurrency(n int) int {
	if n < 1 {
		return runner.DefaultMaxConcurrency
	}
	return n
}

// effectiveMaxBuffered mirrors the stream consumer's MaxBufferedEvents
// normalization (<1 → stream.DefaultMaxBufferedEvents) so the "Concurrency
// limits" log reflects the value actually enforced.
func effectiveMaxBuffered(n int) int {
	if n < 1 {
		return stream.DefaultMaxBufferedEvents
	}
	return n
}

// Run wires the whole worker: startup state, then the reconciler and stream
// consumer. It blocks in Consume until the process is signalled.
func Run(logger *slog.Logger) {
	cfg := config.Load(logger)
	redisOpts, err := config.RedisOptions(cfg.RedisURI)
	if err != nil {
		// Fatal: Redis config is a hard startup requirement (this worker cannot
		// consume without a valid DSN), so exit the process rather than return.
		logger.Error("Redis config invalid", "error", err)
		os.Exit(1)
	}
	client := redis.NewClient(redisOpts)
	defer client.Close()

	// The worker's lifecycle context: cancelled on SIGINT/SIGTERM (or by the
	// deferred stop when Run returns). It is created here, before the runtime
	// Manager, so the manager can root Dockerfile builds in it (see
	// runtime.WithLifecycleContext): a long build is bounded by the runtime's
	// 10m buildTimeout but is still cancelled when Relay shuts down. Every
	// bounded startup daemon operation and the reconciler hooks are likewise
	// rooted here (rather than context.Background), so shutdown cancels them
	// too. It replaces the signal context that used to be created later.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Metrics are opt-in, gated on METRICS_ADDR. Setup only CREATES the registry
	// and /metrics server here (nil, nil when disabled); STARTING them happens
	// later, once the startup wiring is complete.
	metricsInstance, metricsServer := setupMetrics(cfg, logger)

	// A single shared secrets provider, used by both the webhook (below) and the
	// runner (later). Construction is infallible (the dir is created lazily on
	// Set, never Resolve); a missing secret surfaces as a per-invocation Resolve
	// error, and startup does not validate that referenced secrets exist. A
	// construction failure here is fatal — the provider is core to invocation
	// and the webhook.
	secretProvider, err := secrets.NewLocalProvider(secrets.SecretsDir)
	if err != nil {
		logger.Error("Secrets: new local provider failed", "error", err)
		os.Exit(1)
	}

	// The Git webhook server (internal/git/webhook) is opt-in, gated on
	// GIT_WEBHOOK_ADDR. NewServer owns all assembly (git source config, secret
	// reference checks, the coalescing sync scheduler, provider handlers) and
	// returns nil when disabled. The worker only orchestrates: construct, start
	// (bind failure is fatal, matching metrics), and stop on shutdown.
	var gitWebhookServer *gitwh.Server
	if cfg.GitWebhookAddr != "" {
		gitWebhookServer = gitwh.NewServer(cfg.GitWebhookAddr, logger, gitwh.Config{Secrets: secretProvider})
	}

	loader := function.NewLoader(function.Dir, logger)
	functions, err := loader.Load()
	if err != nil {
		// Fatal: the worker cannot run without its function set, so exit the
		// process rather than continue with nothing to serve.
		logger.Error("Load functions failed", "error", err)
		os.Exit(1)
	}
	logger.Info("Loaded functions", "count", len(functions), "root", function.Dir)

	// The state database is a read-only local state view (see internal/state),
	// NOT the source of truth and never drives matching or building. All state
	// errors are non-fatal — Relay runs without the state DB if it is broken.
	st, err := state.Open(state.DBPath)
	if err != nil {
		logger.Warn("State: open failed; continuing without", "error", err)
		st = nil
	}
	if st != nil {
		defer st.Close()
		if err := st.RebuildFromFS(function.Dir); err != nil {
			logger.Warn("State: rebuild from fs failed; continuing", "error", err)
		}
		// Prune state rows for functions no longer on disk BEFORE
		// restorePersistedStats, so a pruned function's stats row is gone before
		// the fresh registry is seeded from it (a function removed while down
		// must not be re-seeded into metrics).
		st.PruneRemoved(function.Dir)
		for _, fn := range functions {
			st.RecordDiscovered(fn)
		}
	}

	// Seed the fresh registry with the persisted cumulative totals so the first
	// snapshot writes them back instead of zeroing SQLite. Gauges are NOT
	// restored (they are point-in-time snapshots refreshed each interval).
	restorePersistedStats(metricsInstance, st)

	// The stats flusher is the single owner of every write of the registry's
	// Relay-visible totals into SQLite. It holds a mutex across BOTH the capture
	// of the current snapshot and the write, so an operator reset over the socket
	// (flusher.ResetStats) can never race a flush into resurrecting pre-reset
	// values: a flush either completes before the reset or captures after it.
	// Shared by statsLoop, finalStatsFlush, and the socket reset command.
	statsFlusher := newStatsFlusher(st, metricsInstance)

	manager, err := runtime.NewManager(
		logger,
		metricsInstance,
		cfg.ConsumerName,
		runtime.WithWarmContainerIdleTimeout(cfg.WarmContainerIdleTimeout),
		// The SAME MAX_CONCURRENCY the runner's global semaphore uses: the
		// runtime clips each function's effective per-function concurrency to it,
		// so a template asking for more than the worker-global cap (e.g. 15 with
		// MAX_CONCURRENCY=8) warms, reports, and admits only the cap's worth. It
		// is startup configuration; a global change requires a worker restart.
		runtime.WithMaxConcurrency(cfg.MaxConcurrency),
		// Root Dockerfile builds in the worker lifecycle: they get an
		// independent 10m bound (runtime.buildTimeout) but are still cancelled
		// when Relay shuts down. Builds must NOT inherit the short 30s
		// reconcile budget the worker uses for normal service operations.
		runtime.WithLifecycleContext(ctx),
	)
	if err != nil {
		// Fatal: the runtime manager owns container execution, which the worker
		// cannot serve without, so exit the process on construction failure.
		logger.Error("Runtime: new manager failed", "error", err)
		os.Exit(1)
	}
	defer manager.Close()

	// The live runtime-pool query socket (see internal/worker/socket.go). It is
	// started now that the manager exists: the CLI's `function inspect` dials it
	// for the LIVE gauges, which are worker-local and never persisted; the
	// cumulative counters stay in /var/lib/relay. The
	// process lock acquired by `relay start` is already held, so removing a stale
	// socket here can never delete an active worker's socket. A bind failure is
	// fatal, matching the metrics and webhook servers: a local bind error is a
	// host/config problem that must surface at startup, not heal invisibly.
	rtSocket, err := NewSocketServer(SocketPath, manager, statsFlusher, logger)
	if err != nil {
		logger.Error("Runtime state socket: start failed", "error", err)
		os.Exit(1)
	}
	defer rtSocket.Close()
	logger.Info("Runtime state socket listening", "path", SocketPath)

	// The service controller converges each function's persistent service
	// containers to its template (manager is the Docker seam; secretProvider is
	// the shared secrets resolver). Service containers now stop on graceful
	// shutdown: the shutdown tail runs ShutdownCleanup for this worker's
	// hostname (cfg.ConsumerName). The coordinator runs the per-function Applys
	// asynchronously (bounded workers, latest-desired-state coalescing), so
	// startup never blocks on a service's Dockerfile build; the startup orphan
	// sweep + image GC run inside the coordinator's exclusive housekeeping window
	// in the background pass. Startup reconciliation remains the crash-recovery
	// path when shutdown cleanup did not execute. The service reconciler itself
	// decides whether routing applies (only services declaring a host are routed)
	// and validates TRAEFIK_NETWORK per routed service; wiring only forwards the
	// configured value.
	svcCtrl := reconciler.NewServiceReconciler(manager, secretProvider, routing.TraefikConfig{
		Network:      cfg.TraefikNetwork,
		EntryPoints:  cfg.TraefikEntryPoints,
		CertResolver: cfg.TraefikCertResolver,
		Priority:     cfg.TraefikPriority,
		HostOverride: cfg.TraefikHostOverride,
	}, logger, reconcileTimeout)
	services := reconciler.NewServiceCoordinator(svcCtrl)
	services.Start(ctx)

	// Conservative startup orphan sweep: before any function is prepared or any
	// container created, remove execution containers a previous Relay process on
	// THIS hostname left behind (a crash mid-invocation). It is label- and
	// hostname-scoped, so other workers' and non-Relay containers are untouched.
	// Bounded so the sweep can never hang startup; on timeout/error we log and
	// continue, leaving the orphans for a later restart.
	sweepCtx, sweepCancel := context.WithTimeout(ctx, reconcileTimeout)
	n, sweepErr := manager.SweepOrphanContainers(sweepCtx, cfg.ConsumerName)
	sweepCancel()
	if sweepErr != nil {
		logger.Warn("Startup: orphan container sweep failed", "error", sweepErr)
	} else if n > 0 {
		logger.Info("Startup: removed orphan containers from a previous relay process", "count", n)
	}

	// Build every function's image. A function whose image cannot be built is
	// marked unavailable so the runner skips it; the rest continue.
	prepared := prepareFunctions(ctx, manager, functions, st, logger)

	// Publish each function's initial desired service state and return
	// immediately. The coordinator's bounded workers converge the states in the
	// background, so startup never blocks on a service's Dockerfile build — nor
	// on the unavailable/no-services removals, which are published the same
	// nonblocking way (the coordinator derives their own fresh bound).
	enqueueStartupServices(prepared, services, logger)

	// Lifecycle-aware background housekeeping. It waits behind the coordinator's
	// barrier for the initial service attempts to settle, then runs the startup
	// cleanup passes in their safe order: orphan container sweep, image sweep
	// (whose keep-set must observe the settled service containers), then
	// dependency GC (which prunes dependency images the image sweep orphaned).
	// Run does NOT call image GC synchronously; housekeepingDone is joined in the
	// shutdown tail so no sweep overlaps shutdown cleanup or the closing state DB
	// and Docker client.
	liveNames := make(map[string]bool, len(functions))
	for _, fn := range functions {
		liveNames[fn.Name] = true
	}
	housekeepingDone := startInitialHousekeeping(ctx, logger, startupHousekeeping{
		exclusive: services.RunExclusive,
		sweep:     func(hctx context.Context) { svcCtrl.SweepOrphans(hctx, liveNames) },
		images:    func(hctx context.Context) { sweepStartupImages(hctx, manager, functions, st, logger) },
		deps:      func(hctx context.Context) { cleanupStartupDependencies(hctx, manager, logger) },
	})

	// The runner executes invocations. It is constructed before the stream
	// consumer so its InvokeHandler can be wired as the consumer's ScheduleRunner
	// seam (schedule-occurrence messages route directly to it, bypassing matching).
	runWorker := runner.NewWithMetrics(prepared, logger, metricsInstance)
	// Stamp the relay.hostname label (the worker/consumer identity) on every
	// execution container, so all invocations carry it.
	runWorker.SetHostname(cfg.ConsumerName)
	// Wire the shared secrets provider; a missing secret surfaces per-invocation,
	// never at startup.
	runWorker.SetSecretProvider(secretProvider)
	// Cap every rule's handler timeout at the value template validation enforces
	// (function.MaxTimeout). Defense in depth: a misconfigured or hot-swapped
	// template can never run a handler past the cap.
	runWorker.SetMaxHandlerTimeout(stream.MaxRuleTimeout)
	// Bound the number of invocations executing concurrently (MAX_CONCURRENCY);
	// a value < 1 falls back to the runner's default.
	runWorker.SetMaxConcurrency(cfg.MaxConcurrency)

	// Expose the live runner to the manual-invocation socket command. It was
	// wired after the socket was created (the runner is built later, once images
	// and services are prepared), so a `relay function invoke` that races the
	// wiring answers invoke_unavailable rather than a torn value. This is what
	// lets the CLI run handlers against the live runtime pool without ever
	// instantiating Docker/runtime in the CLI process. The same runner also
	// serves the `relay dlq replay` semantic command, which re-executes one DLQ
	// entry's exact recorded function/handler once against the current registry.
	rtSocket.SetInvoker(runWorker)
	rtSocket.SetReplayer(runWorker)

	consumer := stream.NewConsumer(stream.ConsumerConfig{
		Client:            client,
		Stream:            cfg.RedisStream,
		Group:             cfg.RedisGroup,
		Consumer:          cfg.ConsumerName,
		Log:               logger,
		Metrics:           metricsInstance,
		MaxBufferedEvents: cfg.MaxBufferedEvents,
		// Schedule occurrences route directly to the runner, bypassing event
		// matching; the runner resolves the handler timeout from the current
		// template and stamps the message's real Redis stream ID.
		ScheduleRunner: runWorker.InvokeHandler,
	})

	// Start the metrics components created earlier. The snapshot logger runs in
	// its own goroutine (exits on ctx); the server binds synchronously — a bind
	// failure (a taken metrics port) is a config error that must surface now, and
	// is FATAL. Both are nil when METRICS_ADDR is unset and simply not started.
	if metricsInstance != nil {
		metricsLogger := metrics.NewMetricsLogger(
			metricsInstance,
			metrics.DefaultLogInterval,
			func(format string, args ...any) { logger.Debug(fmt.Sprintf(format, args...)) },
		)
		go metricsLogger.Start(ctx)

		if err := metricsServer.Start(); err != nil {
			// Fatal: a bind failure (taken metrics port) is a config error that
			// should surface at startup, not retry invisibly.
			logger.Error("Metrics server: start failed", "error", err)
			os.Exit(1)
		}
		logger.Info("Metrics http server listening", "addr", cfg.MetricsAddr)
	}

	// Start the webhook server (created above) right after metrics. It binds
	// synchronously and fails fast on a taken/unparseable GIT_WEBHOOK_ADDR,
	// matching metrics: a webhook port conflict must surface at startup. A nil
	// server means the webhook was disabled, so there is nothing to start.
	if gitWebhookServer != nil {
		if err := gitWebhookServer.Start(); err != nil {
			logger.Error("Git webhook server: start failed", "error", err)
			os.Exit(1)
		}
		logger.Info("Webhook http server listening", "addr", cfg.GitWebhookAddr)
	}

	// Flush the registry into the state database on the fixed cadence. Gated on
	// the metrics instance existing: with metrics disabled there is nothing to
	// snapshot, and a nil-registry flush would clobber the persisted cumulative
	// totals with zeros. The loop parks on ctx so shutdown ordering stays uniform.
	if metricsInstance != nil {
		go statsLoop(ctx, statsFlusher, statsFlushInterval)
	} else {
		go parkUntilShutdown(ctx)
	}

	// Optional internal stream retention (cfg.StreamRetention from
	// REDIS_STREAM_RETENTION): a single goroutine periodically trims the
	// configured stream (XTRIM MINID ~) so entries older than the window are
	// removed. Completely separate from ACK/retry/DLQ semantics; a malformed or
	// non-positive value is logged by config.Load and retention is disabled.
	if cfg.StreamRetention > 0 {
		go retentionLoop(ctx, client, cfg.RedisStream, cfg.StreamRetention, logger)
	}

	if err := consumer.EnsureGroup(ctx); err != nil {
		// Fatal: the consumer group is a hard prerequisite for consumption, so
		// exit the process rather than retry a misconfiguration silently.
		logger.Error("Ensure consumer group failed", "error", err)
		os.Exit(1)
	}

	// The schedule publisher atomically publishes one stream entry per logical
	// occurrence cluster-wide (publish-if-new Lua script) into the same stream the
	// consumer reads, and wires into the scheduler below.
	publisher := schedule.NewPublisher(client, cfg.RedisStream, logger, metricsInstance)

	// The cron scheduler maps each function template's schedules into jobs that
	// publish schedule occurrences through the publisher, seeded from the loaded
	// function set before Start, then converges live via the reconciler's
	// UpdateSchedules/RemoveFunction hooks.
	sched := cron.New(publisher, logger)
	for _, fn := range functions {
		sched.ReplaceFunction(fn.Name, fn.Template)
	}
	logger.Info("Scheduler: schedule jobs registered", "count", sched.JobCount())
	logger.Info(
		"Concurrency limits",
		"max_concurrency", effectiveMaxConcurrency(cfg.MaxConcurrency),
		"max_buffered_events", effectiveMaxBuffered(cfg.MaxBufferedEvents),
	)

	// Watch /functions and reconcile functions live: rebuild changed images,
	// discover new ones, drop removed ones. The runner's registry is swapped
	// atomically behind the snapshots the consumer already uses. The retire hook
	// hands superseded images back to the runner so it can remove them once no
	// in-flight execution uses them. On removal, RemoveFunction first deletes the
	// function's Prometheus series (after the registry entry is swapped to nil)
	// and retires its images; the flush sweep in recordSnapshots re-deletes any
	// series an in-flight invocation may have recreated, so the "removed function
	// => no exposed series" invariant holds even mid-invocation.
	rec := reconciler.New(
		reconciler.Config{
			Root:   function.Dir,
			State:  st,
			Retire: func(_ string, oldImage string) { runWorker.RetireImage(oldImage) },
			RemoveFunction: func(name string) {
				metricsInstance.RemoveFunction(name)
				// Drop the function's reset baseline too, so a re-added function
				// is not offset by a stale pre-removal total.
				statsFlusher.dropFunctionBaseline(name)
				// Drop the function's warm container state first: no new acquire
				// may warm a removed function, idle containers are discarded now,
				// and busy ones are discarded on release. Then retire every image
				// version once idle (the runner's reference guard keeps an image
				// an in-flight execution still needs).
				manager.RemoveFunction(name)
				runWorker.RemoveFunctionSemaphore(name)
				runWorker.RemoveFunctionImages(name)
				sched.RemoveFunction(name) // a removed function never keeps firing
			},
			UpdateSchedules: func(name string, tmpl *function.Template) {
				sched.ReplaceFunction(name, tmpl)
			},
			// Converge the function's persistent service containers whenever its
			// new version is swapped in (and on the skip path when it declares
			// services, so crashed replicas self-heal on the periodic tick). The
			// hook runs synchronously in the reconciler pump goroutine.
			//
			// Apply receives the LIFECYCLE context, NOT a 30s budget wrapped
			// around the whole pass: the ServiceReconciler injects the worker's
			// reconcileTimeout into Reconcile, which derives a fresh bound for
			// each normal Docker operation itself. A Dockerfile build is rooted
			// in the runtime manager lifecycle under buildTimeout, so a long
			// build can never consume the post-build deadline. Shutdown still
			// cancels the operation promptly because ctx is the signal context.
			//
			// The prepared env comes from the current registry entry (the runtime
			// plan env); a nil Prepared (unavailable) falls back to no plan env,
			// mirroring the runner's nil-safe behavior.
			UpdateServices: func(name, fnDir string, tmpl *function.Template, image string) {
				enqueueLiveServices(services, runWorker.Registry(), name, fnDir, tmpl, image)
			},
			// On removal, stop the function's service containers BEFORE the images
			// are retired (reconciler calls RemoveServices before RemoveFunction):
			// running service containers reference those images. RemoveAndWait
			// waits DETERMINISTICALLY for the queued removal to complete; the
			// coordinator derives the operation's own fresh 30s bound rooted in
			// the lifecycle context, and shutdown releases the wait via the
			// lifecycle, so the hook can never let image retirement race the
			// removal. The hook runs in the single pump goroutine, so it never
			// blocks a reconcile of another function.
			RemoveServices: func(name string) {
				services.RemoveAndWait(name)
			},
		},
		runWorker.Registry(),
		manager,
		logger,
	)
	for _, fn := range functions {
		rec.Seed(fn)
	}
	logger.Info("Watching functions for changes", "root", function.Dir)

	// Runs in its own goroutine and stops when ctx is cancelled.
	go rec.Start(ctx)

	// Start the cron scheduler right after the reconciler, so jobs added here
	// (seeded before Start) fire from their first cron tick and jobs the
	// reconciler later converges schedule immediately.
	sched.Start()

	logger.Info("Consuming stream",
		"stream", cfg.RedisStream,
		"group", cfg.RedisGroup,
		"consumer", cfg.ConsumerName,
	)
	if err := consumer.Consume(ctx, runWorker.Handle); err != nil {
		logger.Error("Consume failed", "error", err)
		// The consumer is the worker's raison d'être; a Consume error means the
		// consumption loop has stopped, so exit the process rather than return
		// with nothing running. (Shutdown via a cancelled ctx returns nil, so a
		// non-nil error here is a genuine failure.)
		os.Exit(1)
	}

	// Stop the live runtime-pool query socket first: the worker has stopped
	// consuming, so there is no new live state to serve, and removing the socket
	// prevents `relay function inspect` from resolving a dead endpoint while the
	// rest of shutdown drains. Close stops accepting, closes in-flight
	// connections, joins their bounded handlers, and unlinks the socket file.
	// Non-fatal: a unlink failure must never fail process shutdown.
	if err := rtSocket.Close(); err != nil {
		logger.Warn("Runtime state socket: shutdown failed", "error", err)
	}

	// Bounded graceful shutdown of the cron scheduler, so an in-flight
	// publication observes cancellation and drains within the bound (gocron's
	// Shutdown cancels every job's context). Safe even with zero schedules.
	//
	// NOTE: the scheduler runs AFTER Consume returns here, so a tick racing
	// shutdown could publish an entry during the drain window — harmless under
	// at-least-once: the entry is consumed by another consumer if any; if the
	// whole cluster is down it waits in the stream for the next boot (PEL/group
	// state persists).
	stopSD, cancelSD := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelSD()
	if err := sched.Stop(stopSD); err != nil {
		logger.Warn("Scheduler: graceful shutdown failed", "error", err)
	}

	// Wait for the background startup housekeeping (service barrier, orphan
	// sweep, image sweep, dependency GC) to finish or observe the cancelled
	// lifecycle. Joining it BEFORE ShutdownCleanup guarantees no startup sweep
	// overlaps shutdown cleanup or the closing state DB and Docker client. It is
	// bounded so a wedged sweep can never hang shutdown, and non-fatal for the
	// same reason. On a normal boot the goroutine has long since exited, so this
	// is a no-op receive.
	housekeepingCtx, housekeepingCancel := context.WithTimeout(context.Background(), shutdownServiceTimeout)
	select {
	case <-housekeepingDone:
	case <-housekeepingCtx.Done():
		logger.Warn("Startup: housekeeping did not finish before shutdown", "error", housekeepingCtx.Err())
	}
	housekeepingCancel()

	// Stop and remove this worker's persistent service containers. Order:
	// AFTER the startup housekeeping join and scheduler stop (no more schedule
	// work can start new services), BEFORE the final stats flush (cleanup is
	// bounded work; the flush is the last-chance telemetry write and must not
	// wait behind it). The coordinator Join releases the workers and their
	// waiters and drains in-flight Applys. Non-fatal: ShutdownCleanup logs
	// internally, and a Docker problem or timeout must never fail the process.
	joinCtx, joinCancel := context.WithTimeout(context.Background(), shutdownServiceTimeout)
	if err := services.Join(joinCtx); err != nil {
		logger.Warn("Service: coordinator shutdown failed", "error", err)
	}
	joinCancel()
	shutdownServices(svcCtrl, cfg.ConsumerName, logger)

	// Final flush of the registry into SQLite before the deferred st.Close() runs.
	// Bounded by a short timeout so a wedged SQLite cannot hang shutdown; failure
	// is logged and shutdown continues (telemetry, not state). No-op when metrics
	// are disabled (nil registry).
	finalStatsFlush(statsFlusher)

	// Bounded graceful shutdown of the metrics server, so in-flight scrapes drain
	// rather than being cut off mid-request. No-op when metrics are disabled (the
	// server was never created).
	if metricsServer != nil {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := metricsServer.Stop(stopCtx); err != nil {
			logger.Warn("Metrics server: graceful shutdown failed", "error", err)
		}
	}

	// Bounded graceful shutdown of the webhook server. Order is owned by Stop: the
	// HTTP server shuts down first so no new deliveries arrive, then the scheduler
	// it assembled waits, bounded by the same short ctx, for in-flight sync to
	// drain. A no-op when the webhook is disabled (nil server).
	if gitWebhookServer != nil {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := gitWebhookServer.Stop(stopCtx); err != nil {
			logger.Warn("Git webhook server: graceful shutdown failed", "error", err)
		}
	}

	logger.Info("Shutdown complete")
}

// shutdownServices stops and removes THIS worker's persistent service
// containers during graceful shutdown, bounded by shutdownServiceTimeout.
// Deliberately non-fatal: ShutdownCleanup logs internally, and a Docker problem
// or timeout must never fail the process (no os.Exit anywhere on this path).
func shutdownServices(svcCtrl *reconciler.ServiceReconciler, hostname string, logger *slog.Logger) {
	svcCtx, svcCancel := context.WithTimeout(context.Background(), shutdownServiceTimeout)
	defer svcCancel()
	if _, err := svcCtrl.ShutdownCleanup(svcCtx, hostname); err != nil {
		// ShutdownCleanup already logged the Warn; a Debug here marks the
		// (non-fatal) failure in the shutdown trace without duplicating it.
		logger.Debug("Service: shutdown cleanup returned error", "error", err)
	}
}

// setupMetrics constructs the optional metrics components: a registry and the
// /metrics HTTP server, both nil when METRICS_ADDR is unset. It only CREATES
// them; STARTING happens later in Run once the startup wiring is complete (the
// server binds synchronously, well after ctx setup). The nil-safety contract
// that makes this gating safe: every consumer of the registry is nil-safe, AND
// the stats flush is gated alongside it — a nil registry must never feed
// snapshotStats, or the 5s flush loop would clobber the persisted cumulative
// totals with zeros.
func setupMetrics(cfg config.Config, logger *slog.Logger) (*metrics.Registry, *metrics.Server) {
	if cfg.MetricsAddr == "" {
		return nil, nil
	}
	metricsInstance := metrics.New()
	metricsServer := metrics.NewServer(cfg.MetricsAddr, metricsInstance.Handler(), logger)
	return metricsInstance, metricsServer
}

// prepareFunctions builds each function's image and returns the prepared set. A
// function whose image cannot be built is marked unavailable (the runner skips
// it) rather than failing startup; the rest carry their fresh image. A
// fingerprint that changed between load and build is recomputed so the state DB
// records the final value.
//
// ctx is the worker lifecycle context. It is passed to Prepare so a build (and
// the fast reuse probes) is cancelled on shutdown; Prepare itself roots the
// Dockerfile build in the manager lifecycle with its own 10m buildTimeout, so
// this context's lack of a short deadline is intentional and the build is never
// bounded by the 30s reconcileTimeout.
func prepareFunctions(
	ctx context.Context,
	manager *runtime.Manager,
	functions []function.Function,
	st *state.State,
	logger *slog.Logger,
) []*runner.PreparedFunction {
	preparedCount := 0
	prepared := make([]*runner.PreparedFunction, 0, len(functions))
	for _, fn := range functions {
		prep, err := manager.Prepare(ctx, fn)
		if err != nil {
			logger.Warn("Function: prepare failed", "function", fn.Name, "error", err)
			if st != nil {
				st.RecordReconcileFailure(fn.Name, err)
			}
			prepared = append(prepared, runner.NewUnavailable(fn))
			continue
		}
		if st != nil {
			// The fingerprint may differ from discovery-time if content changed
			// between load and build; the state DB records the final state.
			fp, fperr := function.Fingerprint(fn.Dir)
			if fperr != nil {
				logger.Warn("Function: fingerprint failed", "function", fn.Name, "error", fperr)
				fp = ""
			}
			st.RecordReconcileSuccess(fn.Name, prep.Image, fp, time.Now(), fn)
		}
		prepared = append(prepared, runner.NewPrepared(fn, prep, manager))
		preparedCount++
	}
	logger.Info("Prepared functions", "count", preparedCount)
	return prepared
}

// enqueueLiveServices snapshots the current prepared environment and publishes
// the desired service state without blocking the function reconciler pump. The
// coordinator coalesces updates to the latest desired state per function and its
// bounded workers converge it with the worker LIFECYCLE context, so a long
// Dockerfile build for one function cannot stall the reconciler or the other
// functions. A nil Prepared (unavailable) entry falls back to no plan env,
// mirroring the runner's nil-safe behavior.
func enqueueLiveServices(
	services *reconciler.ServiceCoordinator,
	reg *runner.Registry,
	name, fnDir string,
	tmpl *function.Template,
	image string,
) {
	var preparedEnv []string
	if cur := reg.GetByName(name); cur != nil && cur.Prepared() != nil {
		preparedEnv = cur.Prepared().Env
	}
	services.Enqueue(name, fnDir, tmpl, image, preparedEnv)
}

// enqueueStartupServices publishes each prepared function's initial desired
// service state and returns immediately; the coordinator converges them in the
// background, so startup never blocks on a service's Dockerfile build. The
// unavailable/no-services removals are published the same nonblocking way (see
// the special case below), so startup never blocks on Docker work at all; the
// background housekeeping pass' barrier waits for every published operation to
// settle before touching containers or images.
//
// Prepared (available) functions are enqueued UNCONDITIONALLY — including
// templates that now declare no services: Reconcile with an empty desired set
// stops any containers a previous boot left behind when services were removed
// while Relay was down (the fingerprint was re-seeded from changed content, so
// the reconciler would take the skip path and never converge them otherwise).
//
// An unavailable function (no image this boot) is left alone when its template
// still declares services (its stale containers may still be serving the old
// image, until a later successful reconcile or Remove replaces them); when its
// template no longer declares services, lingering containers are stale by
// definition and are removed via the nonblocking EnqueueRemove. The coordinator
// derives the removal's own fresh reconcileTimeout bound; the housekeeping
// barrier serializes the image sweep behind it exactly as it does for Applys.
func enqueueStartupServices(
	prepared []*runner.PreparedFunction,
	services *reconciler.ServiceCoordinator,
	logger *slog.Logger,
) {
	for _, pf := range prepared {
		fn := pf.Function()
		if prep := pf.Prepared(); prep != nil {
			services.Enqueue(fn.Name, fn.Dir, fn.Template, prep.Image, prep.Env)
			continue
		}
		if len(fn.Template.Services) == 0 {
			services.EnqueueRemove(fn.Name)
		} else {
			logger.Warn("Service: function unavailable; skipping service reconcile", "function", fn.Name)
		}
	}
}

// startupHousekeeping groups the background startup cleanup passes so their
// ordering and lifecycle behavior are deterministic to test without Docker.
// exclusive is the coordinator's housekeeping seam (see
// ServiceCoordinator.RunExclusive): it atomically waits for current service work
// to settle, PAUSES scheduling of new requests for the duration of the callback,
// and resumes/schedules the latest coalesced desired states afterward. The three
// passes run inside that exclusive window in the safe order — orphan container
// sweep, then image sweep (whose keep-set must observe the settled service
// containers), then dependency GC (which prunes dependency images the image
// sweep orphaned) — so no live UpdateServices can race them.
type startupHousekeeping struct {
	exclusive func(context.Context, func(context.Context)) error
	sweep     func(context.Context)
	images    func(context.Context)
	deps      func(context.Context)
}

// startInitialHousekeeping launches the lifecycle-aware startup housekeeping in a
// background goroutine and returns a channel closed when it completes. Startup
// must NOT block on it: Run publishes the initial desired states and returns
// immediately, and only this pass waits at the coordinator barrier for the
// initial service attempts to settle before touching containers or images. Its
// lifecycle is the worker lifecycle, so shutdown cancels an in-flight wait/sweep
// promptly. A barrier failure (lifecycle cancellation or a bounded wait that did
// not clear) skips the sweeps: image/orphan cleanup must never run against an
// unconverged or shutting-down world. An update arriving while the exclusive
// callback runs is coalesced as a pending desired state and scheduled only once
// the callback returns, so it can never run concurrently with a sweep.
func startInitialHousekeeping(
	lifecycle context.Context,
	logger *slog.Logger,
	h startupHousekeeping,
) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		err := h.exclusive(lifecycle, func(hctx context.Context) {
			h.sweep(hctx)
			h.images(hctx)
			h.deps(hctx)
		})
		if err == nil {
			return
		}
		// A cancelled lifecycle is the normal shutdown path, not a barrier
		// failure: log it at Debug and note the sweeps were skipped (they must
		// never run against a shutting-down world).
		if lifecycle.Err() != nil {
			logger.Debug("Startup: housekeeping stopped by shutdown", "error", err)
		} else {
			logger.Warn("Startup: service reconciliation barrier failed", "error", err)
		}
	}()
	return done
}

// sweepStartupImages removes Relay-owned images that no longer correspond to a
// live function version, after every current image is built (reused if
// unchanged) and services are converged. The keep-set holds (a) each function's
// expected fingerprinted image and (b) images the running service containers
// reference. It also keeps (c) any last-active image state recorded for a
// function still on disk — the crash guard for a swap that started but whose
// RecordReconcileSuccess never landed, where the recorded image may still be the
// one serving.
//
// When state is nil (DB failed to open) we cannot distinguish a removed function
// from a mis-fingerprinted one, so orphan removal is skipped entirely — only the
// self-evidently-current prepared images are kept. Conservative: nothing that
// might still serve is ever removed.
func sweepStartupImages(
	lifecycle context.Context,
	manager *runtime.Manager,
	functions []function.Function,
	st *state.State,
	logger *slog.Logger,
) {
	// Images referenced by any Relay-owned service container are kept too: a
	// container kept by the Applys above (unchanged image) or left over from a
	// previous boot that this boot has not yet replaced references its image by
	// label, and the sweep must not remove an image a running container depends
	// on. Removal is deferred to the owning function's reconcile, which replaces
	// the container first and only then retires the image.
	svcCtx, cancel := context.WithTimeout(lifecycle, reconcileTimeout)
	svcContainers, err := manager.ServiceContainerList(svcCtx)
	cancel()
	var serviceImages []string
	if err == nil {
		serviceImages = make([]string, 0, len(svcContainers))
		for _, container := range svcContainers {
			serviceImages = append(serviceImages, container.Image)
		}
	} else {
		logger.Warn("Service: keep-set list failed; continuing without", "error", err)
	}
	// The state keep-set is only armed when the DB was available. When st is
	// nil recordedImages stays empty, so the keep-set falls back to the function
	// and service images alone (and the sweep below is skipped entirely).
	var recordedImages []string
	if st != nil {
		for _, fn := range functions {
			if detail, ok := st.GetFunction(fn.Name); ok && detail.Image != "" {
				recordedImages = append(recordedImages, detail.Image)
			}
		}
	}

	keep := startupImageKeepSet(functions, serviceImages, recordedImages)
	if st != nil {
		if _, err := manager.RemoveImagesExcept(lifecycle, keep); err != nil {
			logger.Warn("Image cleanup: startup sweep failed", "error", err)
		}
	}
}

// startupImageKeepSet computes the set of image references the startup sweep
// must keep: (a) each function's expected fingerprinted image, (b) every image a
// running service container references, and (c) every last-active image recorded
// for a function still on disk — the crash guard for a swap that started but
// whose RecordReconcileSuccess never landed, where the recorded image may still
// be the one serving. It is a pure function so the keep-set policy is unit
// testable without Docker or a state DB; blank image entries are ignored.
func startupImageKeepSet(functions []function.Function, serviceImages, recordedImages []string) map[string]bool {
	keep := make(map[string]bool)
	for _, fn := range functions {
		// The image tag is derived from the IMAGE fingerprint (runtime-only
		// template keys such as `networks` removed), so the keep-set names
		// exactly the image a build/reuse would produce.
		if fp, err := function.ImageFingerprint(fn.Dir); err == nil {
			keep[runtime.ImageRef(fn.Name, fp)] = true
		}
	}

	for _, img := range serviceImages {
		if img != "" {
			keep[img] = true
		}
	}

	for _, img := range recordedImages {
		if img != "" {
			keep[img] = true
		}
	}

	return keep
}

// cleanupStartupDependencies runs dependency GC at startup. The startup image
// sweep may remove superseded function images; dependency cleanup can then
// remove dependency images that no managed function image references anymore.
// It runs OUTSIDE the st != nil gate: unlike RemoveImagesExcept, it needs no
// state keep-set — ownership is derived from the managed-image labels the builds
// just stamped. Best-effort and single-shot: an error is logged and left for the
// next natural lifecycle point; it never retries in a loop.
func cleanupStartupDependencies(lifecycle context.Context, manager *runtime.Manager, logger *slog.Logger) {
	// Rooted in the lifecycle (not context.Background) so shutdown cancels a
	// long dependency GC. It is deliberately unbounded by reconcileTimeout: like
	// the builds it follows, it is lifecycle-bounded rather than
	// reconcile-bounded, and it is a single best-effort pass.
	if _, err := manager.CleanupUnusedDependencies(lifecycle); err != nil {
		logger.Warn("Dependency image cleanup failed", "error", err)
	}
}

// parkUntilShutdown blocks until ctx is cancelled, then returns. It stands in
// for the stats flush goroutine when metrics are disabled (there is nothing to
// snapshot), keeping shutdown ordering uniform: every background loop the
// worker starts either exits on ctx.Done or is explicitly stopped.
func parkUntilShutdown(ctx context.Context) {
	<-ctx.Done()
}

// restorePersistedStats seeds the fresh process-lifetime metrics registry with
// the cumulative counters persisted in the state database, so the first
// snapshot never resets them. The registry only knows this process's lifetime,
// while the SQLite stats rows are cumulative history; seeding bridges them. It
// is nil-safe on both the registry and the state handle (a nil st means the DB
// failed to open, so there is nothing to restore). Gauges are deliberately NOT
// restored: they are point-in-time backlog snapshots refreshed each snapshot.
func restorePersistedStats(metricsInstance *metrics.Registry, st *state.State) {
	if metricsInstance == nil || st == nil {
		return
	}
	if gs, ok := st.Stats(); ok {
		metricsInstance.SeedCounter(metrics.MetricEventsReceived, gs.EventsReceivedTotal)
		metricsInstance.SeedCounter(metrics.MetricEventsMatched, gs.EventsMatchedTotal)
		metricsInstance.SeedCounter(metrics.MetricEventsUnmatched, gs.EventsUnmatchedTotal)
		metricsInstance.SeedCounter(metrics.MetricHandlerSuccess, gs.HandlerSuccessTotal)
		metricsInstance.SeedCounter(metrics.MetricHandlerFailure, gs.HandlerFailureTotal)
		metricsInstance.SeedCounter(metrics.MetricRetries, gs.RetryTotal)
		metricsInstance.SeedCounter(metrics.MetricDLQEntries, gs.DLQTotal)
	}
	for _, fs := range st.AllFunctionStats() {
		metricsInstance.SeedFunctionStat(metrics.FunctionStat{
			Function:            fs.Function,
			EventsMatchedTotal:  fs.EventsMatchedTotal,
			HandlerSuccessTotal: fs.HandlerSuccessTotal,
			HandlerFailureTotal: fs.HandlerFailureTotal,
			RetriesTotal:        fs.RetryTotal,
			DLQTotal:            fs.DLQTotal,
			// Restore the cumulative warm-container pool counters so they stay
			// monotonic across restarts (the same contract as the counters
			// above). The live pool gauges are deliberately not restored.
			WarmAcquiresTotal: fs.WarmAcquiresTotal,
			ColdStartsTotal:   fs.ColdStartsTotal,
			DiscardedTotal:    fs.DiscardedTotal,
			// Parse the RFC3339 timestamp strings back to unix seconds for the
			// registry (an unparseable/empty value parses to a zero time, which
			// SeedFunctionStat skips as "never observed").
			LastExecution: rfc3339ToUnix(fs.LastExecutionAt),
			LastSuccess:   rfc3339ToUnix(fs.LastSuccessAt),
			LastFailure:   rfc3339ToUnix(fs.LastFailureAt),
			LastDLQ:       rfc3339ToUnix(fs.LastDLQAt),
		})
	}
}

// rfc3339ToUnix parses an RFC3339 timestamp string into its unix-seconds value,
// returning 0 ("never observed") for the empty string or an unparseable value.
// It is the inverse of unixSecToRFC3339 at restore time: the state layer stores
// RFC3339 strings, the registry stores unix seconds, and a stale or corrupt
// persisted value is treated exactly like absence rather than erroring the
// restore path (observability is non-fatal).
func rfc3339ToUnix(s string) int64 {
	if s == "" {
		return 0
	}
	parsed, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return 0
	}
	return parsed.Unix()
}

// unixSecToRFC3339 renders a unix-seconds value as an RFC3339 timestamp,
// rendering 0 ("never observed") as the empty string — the state layer's
// convention for "never". It is the inverse of rfc3339ToUnix at flush time.
func unixSecToRFC3339(ts int64) string {
	if ts <= 0 {
		return ""
	}
	return time.Unix(ts, 0).UTC().Format(time.RFC3339)
}

// relayGlobalCounters are the unlabeled cumulative counters the worker persists.
// They are the global counterpart of the per-function counters read by
// FunctionStatsSnapshot, and the set the relay baseline captures/subtracts.
var relayGlobalCounters = []string{
	metrics.MetricEventsReceived,
	metrics.MetricEventsMatched,
	metrics.MetricEventsUnmatched,
	metrics.MetricHandlerSuccess,
	metrics.MetricHandlerFailure,
	metrics.MetricRetries,
	metrics.MetricDLQEntries,
}

// relayBaseline is the worker-owned reset baseline captured at the last
// `reset stats`. It is the worker-side replacement for the metrics registry's
// former subtraction baseline: the registry stays a pure, monotonic Prometheus
// accumulator, and the worker subtracts the reset point when it snapshots, so
// the persisted Relay totals continue from zero while /metrics never moves
// backwards. globals maps a counter name to its raw value at the reset; funcs
// maps a function to its raw per-function snapshot at the reset (counters are
// subtracted, and a timestamp not advanced since the reset is suppressed to
// "never observed" — timestamps are last-observed, not cumulative, so
// subtraction alone cannot represent their reset). It is guarded by the owning
// statsFlusher.mu and takes no lock of its own; the zero value is a valid
// "no reset yet" baseline and a nil *relayBaseline is treated the same.
type relayBaseline struct {
	globals map[string]int64
	funcs   map[string]metrics.FunctionStat
}

// counter returns v (the raw counter value) minus the reset baseline for name.
// A nil or zero baseline returns v unchanged.
func (b *relayBaseline) counter(name string, v int64) int64 {
	if b == nil {
		return v
	}
	return v - b.globals[name]
}

// applyFunctionStat returns fs with every cumulative per-function counter
// reduced by the reset baseline and any timestamp not advanced since the reset
// suppressed to zero ("never observed"). A nil baseline, or a function absent
// from the baseline, leaves fs unchanged. It is read-only: the registry is
// never mutated.
func (b *relayBaseline) applyFunctionStat(fs metrics.FunctionStat) metrics.FunctionStat {
	if b == nil {
		return fs
	}
	base, ok := b.funcs[fs.Function]
	if !ok {
		return fs
	}
	fs.EventsMatchedTotal -= base.EventsMatchedTotal
	fs.HandlerSuccessTotal -= base.HandlerSuccessTotal
	fs.HandlerFailureTotal -= base.HandlerFailureTotal
	fs.RetriesTotal -= base.RetriesTotal
	fs.DLQTotal -= base.DLQTotal
	fs.WarmAcquiresTotal -= base.WarmAcquiresTotal
	fs.ColdStartsTotal -= base.ColdStartsTotal
	fs.DiscardedTotal -= base.DiscardedTotal
	// Timestamps are last-observed, so a value not newer than the reset point
	// belongs to pre-reset history and is reported as "never observed". A
	// post-reset execution always advances the clock past the baseline.
	if fs.LastExecution <= base.LastExecution {
		fs.LastExecution = 0
	}
	if fs.LastSuccess <= base.LastSuccess {
		fs.LastSuccess = 0
	}
	if fs.LastFailure <= base.LastFailure {
		fs.LastFailure = 0
	}
	if fs.LastDLQ <= base.LastDLQ {
		fs.LastDLQ = 0
	}
	return fs
}

// captureRelayBaseline reads the registry's current raw counters and
// per-function snapshot and installs them as the reset baseline. A nil registry
// yields the zero baseline (there is no in-memory source to reset). The caller
// holds the flusher mutex, so the captured values are a consistent reset point
// with respect to any concurrent flush.
func captureRelayBaseline(metricsInstance *metrics.Registry) relayBaseline {
	var b relayBaseline
	if metricsInstance == nil {
		return b
	}
	b.globals = make(map[string]int64, len(relayGlobalCounters))
	for _, name := range relayGlobalCounters {
		b.globals[name] = metricsInstance.Counter(name)
	}
	b.funcs = make(map[string]metrics.FunctionStat)
	for _, fs := range metricsInstance.FunctionStatsSnapshot() {
		b.funcs[fs.Function] = fs
	}
	return b
}

// snapshotStats maps the metrics registry into the state database's Stats
// payload. The registry counter names feed the persisted Stats fields directly,
// with one rename (retries_total → RetryTotal) and the float gauges truncated to
// int64. Each cumulative counter has the worker-owned relay baseline subtracted
// (base), so an operator reset starts the persisted totals from zero while
// Prometheus stays monotonic; before any reset the baseline is zero and this
// equals the raw counter. The backlog gauges are point-in-time values and are
// never baselined. The three event-classification counters are copied verbatim,
// preserving the received == matched + unmatched partition.
// It is nil-safe: a nil registry yields a zero Stats so the snapshot path can
// never panic or block processing.
func snapshotStats(metricsInstance *metrics.Registry, base *relayBaseline) state.Stats {
	if metricsInstance == nil {
		return state.Stats{}
	}
	return state.Stats{
		EventsReceivedTotal: base.counter(
			metrics.MetricEventsReceived, metricsInstance.Counter(metrics.MetricEventsReceived)),
		EventsMatchedTotal: base.counter(
			metrics.MetricEventsMatched, metricsInstance.Counter(metrics.MetricEventsMatched)),
		EventsUnmatchedTotal: base.counter(
			metrics.MetricEventsUnmatched, metricsInstance.Counter(metrics.MetricEventsUnmatched)),
		HandlerSuccessTotal: base.counter(
			metrics.MetricHandlerSuccess, metricsInstance.Counter(metrics.MetricHandlerSuccess)),
		HandlerFailureTotal: base.counter(
			metrics.MetricHandlerFailure, metricsInstance.Counter(metrics.MetricHandlerFailure)),
		RetryTotal: base.counter(
			metrics.MetricRetries, metricsInstance.Counter(metrics.MetricRetries)),
		DLQTotal: base.counter(
			metrics.MetricDLQEntries, metricsInstance.Counter(metrics.MetricDLQEntries)),
		PendingEntries:          int64(metricsInstance.Gauge(metrics.MetricPendingEntries)),
		OldestPendingAgeSeconds: int64(metricsInstance.Gauge(metrics.MetricPendingOldestAge)),
	}
}

// snapshotFunctionStats maps the registry's per-function counters AND latest
// execution-history timestamps into the state layer's FunctionStats rows. Like
// snapshotStats it applies the worker-owned relay baseline (base), so
// per-function totals continue from zero after an operator reset and pre-reset
// timestamps are reported as "never observed", while the Prometheus series stay
// monotonic. The registry stores unix seconds; the state layer stores RFC3339
// strings in the updated_at convention (empty = never observed), so zero
// timestamps map to "". It is nil-safe: a nil registry yields an empty slice so
// the snapshot path can never panic or block processing.
func snapshotFunctionStats(metricsInstance *metrics.Registry, base *relayBaseline) []state.FunctionStats {
	if metricsInstance == nil {
		return nil
	}
	stats := metricsInstance.FunctionStatsSnapshot()
	out := make([]state.FunctionStats, 0, len(stats))
	for _, fs := range stats {
		fs = base.applyFunctionStat(fs)
		out = append(out, state.FunctionStats{
			Function:            fs.Function,
			EventsMatchedTotal:  fs.EventsMatchedTotal,
			HandlerSuccessTotal: fs.HandlerSuccessTotal,
			HandlerFailureTotal: fs.HandlerFailureTotal,
			RetryTotal:          fs.RetriesTotal,
			DLQTotal:            fs.DLQTotal,
			WarmAcquiresTotal:   fs.WarmAcquiresTotal,
			ColdStartsTotal:     fs.ColdStartsTotal,
			DiscardedTotal:      fs.DiscardedTotal,
			LastExecutionAt:     unixSecToRFC3339(fs.LastExecution),
			LastSuccessAt:       unixSecToRFC3339(fs.LastSuccess),
			LastFailureAt:       unixSecToRFC3339(fs.LastFailure),
			LastDLQAt:           unixSecToRFC3339(fs.LastDLQ),
		})
	}
	return out
}

// statsFlusher owns every write of the registry's Relay-visible totals into
// SQLite and the matching in-memory reset. It is the serialization point that
// makes a socket-triggered `reset stats` deterministic against the periodic
// flush: mu is held across BOTH capturing the snapshot and writing it, and
// ResetStats takes the same mutex, so a flush can never interleave a captured
// pre-reset snapshot with the reset (pre-reset values cannot be resurrected).
// It is safe for concurrent use.
type statsFlusher struct {
	mu sync.Mutex
	st *state.State
	// metrics is nil exactly when METRICS_ADDR is unset. The flush paths are
	// gated on a non-nil registry (statsLoop is not started, finalStatsFlush
	// returns early), so a snapshot never runs against nil, which would clobber
	// the persisted cumulative totals with zeros. ResetStats tolerates nil
	// (capturing an empty baseline) so the socket can still reset the state DB.
	metrics *metrics.Registry
	// baseline is the worker-owned reset point captured by ResetStats. It is
	// read and replaced under mu, so a concurrent flush always sees a coherent
	// baseline. Zero value = "no reset yet".
	baseline relayBaseline
}

// newStatsFlusher builds a flusher over the state handle and registry.
func newStatsFlusher(st *state.State, metricsInstance *metrics.Registry) *statsFlusher {
	return &statsFlusher{st: st, metrics: metricsInstance}
}

// flush captures the current Relay-visible snapshot and writes it in one short
// transaction (see recordSnapshots). Held under mu so a concurrent ResetStats
// cannot land between capture and write. Nil-safe on the flusher.
func (flusher *statsFlusher) flush(ctx context.Context) {
	if flusher == nil {
		return
	}
	flusher.mu.Lock()
	defer flusher.mu.Unlock()
	recordSnapshots(ctx, flusher.st, flusher.metrics, &flusher.baseline)
}

// dropFunctionBaseline drops a function's entry from the worker-owned reset
// baseline. It is called from the reconciler's RemoveFunction hook alongside the
// registry's series deletion, so a function removed and later re-added starts
// from its fresh zero-valued series instead of subtracting a stale pre-removal
// total (which would persist a negative value). It is idempotent, nil-safe, and
// safe to call before any reset (a zero baseline has no entry to drop).
func (flusher *statsFlusher) dropFunctionBaseline(name string) {
	if flusher == nil {
		return
	}
	flusher.mu.Lock()
	delete(flusher.baseline.funcs, name)
	flusher.mu.Unlock()
}

// ResetStats resets the worker's accumulated Relay statistics. It captures the
// registry's current raw values as the worker-owned subtraction baseline and
// rewrites the persisted rows to zero, all while holding the SAME mutex flush
// uses, so no captured pre-reset snapshot can be written after the reset. It
// implements StatsResetter for the socket's `reset stats` command and never
// mutates Prometheus: the registry keeps accumulating monotonically, and the
// flush subtracts the baseline when it snapshots. A missing state handle (state
// open failed at startup) reports an error so the socket answers
// stats_reset_failed and the CLI surfaces it rather than masking a failed worker
// reset; the baseline is still captured so the in-memory totals continue from
// zero.
func (flusher *statsFlusher) ResetStats() error {
	if flusher == nil {
		return errors.New("stats flusher unavailable")
	}
	flusher.mu.Lock()
	defer flusher.mu.Unlock()
	flusher.baseline = captureRelayBaseline(flusher.metrics)
	if flusher.st == nil {
		return errors.New("state database unavailable")
	}
	return flusher.st.ResetStats()
}

// statsLoop is the flush loop: it mirrors the in-memory metrics registry into
// the state database on a fixed interval until ctx is cancelled. The first
// flush runs immediately so the stats row exists before the first tick (this
// also makes `relay stats` useful right after startup). It is nil-safe on the
// state handle, so observability can never break processing. The loop's ctx is
// the shutdown ctx; periodic flushes use it directly (a cancelled ctx simply
// stops the loop).
func statsLoop(ctx context.Context, flusher *statsFlusher, interval time.Duration) {
	if flusher == nil || flusher.st == nil {
		<-ctx.Done()
		return
	}
	flusher.flush(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			flusher.flush(ctx)
		}
	}
}

// finalStatsFlush performs a bounded final flush of the registry into SQLite on
// graceful shutdown, so the last interval of telemetry is not lost. It is
// bounded by a short timeout so a wedged SQLite cannot hang shutdown; on
// timeout or error it logs and returns (telemetry, not state). It is nil-safe
// on the flusher. The metrics guard is load-bearing: Run calls this even when
// metrics are disabled (the flusher still holds the state handle), and a
// nil-registry flush would write zero Stats over the persisted cumulative
// totals.
func finalStatsFlush(flusher *statsFlusher) {
	if flusher == nil || flusher.st == nil || flusher.metrics == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	flusher.flush(ctx)
}

// recordSnapshots writes the whole stats snapshot — the global stats row and
// one function_stats row per function with any attributed activity — in a
// single short transaction (see state.RecordStatsSnapshot). base is the
// worker-owned reset baseline subtracted from every cumulative counter (nil =
// no reset yet), so an operator reset starts the persisted totals from zero
// while Prometheus stays monotonic. Before the write it enforces the
// metrics/SQLite consistency invariant: it sweeps the registry with
// SweepFunctionMetrics against state.FunctionNames, deleting any function-scoped
// series whose function no longer has a functions row. This complements the
// reconciler's RemoveFunction hook (which deletes series at removal time) by
// re-deleting series an in-flight invocation may have recreated after removal —
// a removed function exposes NO series on /metrics, and globals are untouched.
// The transaction first prunes orphaned function_stats rows (a removed
// function's row is not re-created even though its registry counters are swept
// just above). Per-function writes are bounded by the function count, so the 5s
// cadence keeps them small. A failed flush is logged and retried next tick with
// the current absolute values; no path resets counters on failure.
func recordSnapshots(
	ctx context.Context,
	st *state.State,
	metricsInstance *metrics.Registry,
	base *relayBaseline,
) {
	// Sweep the registry against the live function set before snapshotting, so
	// a function removed (or swept) this interval cannot persist a stale
	// function_stats row that the orphan-prune would have to reject anyway, and
	// cannot linger on /metrics.
	//
	// Fail-open: if the live set cannot be read (st.FunctionNames returns
	// ok=false), the sweep is skipped entirely rather than run against an empty
	// map — an empty live set would delete every function-scoped series,
	// including live functions'. Sweeping is best-effort and re-applied on the
	// next successful flush; RecordStatsSnapshot below already logs the DB
	// issue, so no new logging is added here.
	if names, ok := st.FunctionNames(); ok {
		active := make(map[string]bool, len(names))
		for _, name := range names {
			active[name] = true
		}
		metricsInstance.SweepFunctionMetrics(active)
	}
	// RecordStatsSnapshot logs internally on error (matching the state package's
	// non-fatal style); the returned error is only for the caller to bound the
	// write with a context.
	_ = st.RecordStatsSnapshot(
		ctx,
		snapshotStats(metricsInstance, base),
		snapshotFunctionStats(metricsInstance, base),
	)
}

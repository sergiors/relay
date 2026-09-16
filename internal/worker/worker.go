// Package worker is the long-running Relay runtime, started via `relay start`.
// It loads functions, builds their images, reconciles them live, and consumes
// the Redis stream, blocking until signalled. Configuration comes entirely
// from the environment. It can also expose a Prometheus /metrics endpoint (see
// internal/metrics), gated on the METRICS_ADDR environment variable, and
// flushes the registry into the local state database on a fixed 5-second
// cadence: stats accumulate in memory (the registry is the single source of
// truth), Prometheus reflects them immediately, and SQLite receives the current
// absolute snapshot every interval. The cron scheduler (internal/cron)
// joins the same lifecycle: constructed, seeded from the loaded schedules,
// started, and stopped on shutdown. Schedules are coordinated through Redis:
// every worker evaluates the cron locally but publishes one stream entry per
// occurrence cluster-wide (atomic publish-if-new), and the consumer group
// delivers that single entry to exactly one worker for execution (at-least-once).
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/config"
	"relay/internal/cron"
	"relay/internal/function"
	gitwh "relay/internal/git/webhook"
	"relay/internal/metrics"
	"relay/internal/reconciler"
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

// startupTimeout bounds every bounded startup daemon operation: the orphan
// container sweep, each single per-function service converge, and the
// reconciler's UpdateServices/RemoveServices hooks. Each call gets its own
// fresh bound so one slow Docker call cannot consume the budget of the calls
// that follow. The 5s shutdown bounds are a separate, deliberately shorter
// bound (see Run's shutdown tail), not this constant.
const startupTimeout = 30 * time.Second

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
		logger.Error(fmt.Sprintf("Redis config: %v", err))
		os.Exit(1)
	}
	client := redis.NewClient(redisOpts)
	defer client.Close()

	// Metrics are opt-in, gated on METRICS_ADDR. Setup only CREATES the registry
	// and /metrics server here (nil, nil when disabled); STARTING them happens
	// later, once the signal ctx exists.
	metricsInstance, metricsServer := setupMetrics(cfg, logger)

	// A single shared secrets provider, used by both the webhook (below) and the
	// runner (later). Construction is infallible (the dir is created lazily on
	// Set, never Resolve); a missing secret surfaces as a per-invocation Resolve
	// error, and startup does not validate that referenced secrets exist. A
	// construction failure here is fatal — the provider is core to invocation
	// and the webhook.
	secretProvider, err := secrets.NewLocalProvider(secrets.SecretsDir)
	if err != nil {
		logger.Error(fmt.Sprintf("Secrets: %v", err))
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
		logger.Error(fmt.Sprintf("Load functions: %v", err))
		os.Exit(1)
	}
	logger.Info(fmt.Sprintf("Loaded %d function(s) from %s", len(functions), function.Dir))

	// The state database is a read-only local state view (see internal/state),
	// NOT the source of truth and never drives matching or building. All state
	// errors are non-fatal — Relay runs without the state DB if it is broken.
	st, err := state.Open(state.DBPath)
	if err != nil {
		logger.Warn(fmt.Sprintf("State: open (continuing without): %v", err))
		st = nil
	}
	if st != nil {
		defer st.Close()
		if err := st.RebuildFromFS(function.Dir); err != nil {
			logger.Warn(fmt.Sprintf("State: rebuild from fs (continuing): %v", err))
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

	manager, err := runtime.NewManager(logger, metricsInstance, cfg.ConsumerName)
	if err != nil {
		// Fatal: the runtime manager owns container execution, which the worker
		// cannot serve without, so exit the process on construction failure.
		logger.Error(fmt.Sprintf("Runtime: %v", err))
		os.Exit(1)
	}
	defer manager.Close()

	// The service controller converges each function's persistent service
	// containers to its template (manager is the Docker seam; secretProvider is
	// the shared secrets resolver). Service containers survive worker shutdown by
	// design — they are persistent and long-lived — so the next boot's per-function
	// Apply + SweepOrphans converges any drift; nothing touches them on shutdown.
	svcCtrl := reconciler.NewServiceReconciler(manager, secretProvider, logger)

	// Conservative startup orphan sweep: before any function is prepared or any
	// container created, remove execution containers a previous Relay process on
	// THIS hostname left behind (a crash mid-invocation). It is label- and
	// hostname-scoped, so other workers' and non-Relay containers are untouched.
	// Bounded so the sweep can never hang startup; on timeout/error we log and
	// continue, leaving the orphans for a later restart.
	sweepCtx, sweepCancel := context.WithTimeout(context.Background(), startupTimeout)
	n, sweepErr := manager.SweepOrphanContainers(sweepCtx, cfg.ConsumerName)
	sweepCancel()
	if sweepErr != nil {
		logger.Warn(fmt.Sprintf("Startup: orphan container sweep: %v", sweepErr))
	} else if n > 0 {
		logger.Info(
			fmt.Sprintf("Startup: removed %d orphan container(s) from a previous relay process", n),
		)
	}

	// Build every function's image. A function whose image cannot be built is
	// marked unavailable so the runner skips it; the rest continue.
	prepared := prepareFunctions(manager, functions, st, logger)

	// Converge persistent service containers AFTER every image is built (so the
	// desired image is present) and BEFORE the startup image sweep (so the sweep's
	// keep-set can include images live containers reference).
	reconcileStartupServices(prepared, functions, svcCtrl, logger)

	// Conservative startup image sweep: remove Relay-owned images no live function
	// or container references. Kept images include live functions' fingerprints,
	// running service containers' images, and state-recorded images (the crash
	// guard for a mid-swap restart).
	sweepStartupImages(manager, functions, st, logger)

	// Lifecycle-driven dependency GC at startup. The image sweep may leave
	// superseded images; dependency cleanup then prunes dependency images no
	// managed function image references anymore. Runs OUTSIDE the st gate (labels,
	// no state keep-set). Best-effort and single-shot — errors are left for the
	// next natural lifecycle point.
	cleanupStartupDependencies(manager, logger)

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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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
			logger.Error(fmt.Sprintf("Metrics server: %v", err))
			os.Exit(1)
		}
		logger.Info(fmt.Sprintf("Metrics http server listening on %s", cfg.MetricsAddr))
	}

	// Start the webhook server (created above) right after metrics. It binds
	// synchronously and fails fast on a taken/unparseable GIT_WEBHOOK_ADDR,
	// matching metrics: a webhook port conflict must surface at startup. A nil
	// server means the webhook was disabled, so there is nothing to start.
	if gitWebhookServer != nil {
		if err := gitWebhookServer.Start(); err != nil {
			logger.Error(fmt.Sprintf("Git webhook server: %v", err))
			os.Exit(1)
		}
		logger.Info(fmt.Sprintf("Webhook http server listening on %s", cfg.GitWebhookAddr))
	}

	// Flush the registry into the state database on the fixed cadence. Gated on
	// the metrics instance existing: with metrics disabled there is nothing to
	// snapshot, and a nil-registry flush would clobber the persisted cumulative
	// totals with zeros. The loop parks on ctx so shutdown ordering stays uniform.
	if metricsInstance != nil {
		go statsLoop(ctx, metricsInstance, st, statsFlushInterval)
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
		logger.Error(fmt.Sprintf("Ensure consumer group: %v", err))
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
	logger.Info(fmt.Sprintf("Scheduler: %d schedule job(s) registered", sched.JobCount()))
	logger.Info(
		fmt.Sprintf(
			"Concurrency limits: MAX_CONCURRENCY=%d MAX_BUFFERED_EVENTS=%d",
			effectiveMaxConcurrency(cfg.MaxConcurrency),
			effectiveMaxBuffered(cfg.MaxBufferedEvents),
		),
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
				runWorker.RemoveFunctionImages(name)
				sched.RemoveFunction(name) // a removed function never keeps firing
			},
			UpdateSchedules: func(name string, tmpl *function.Template) {
				sched.ReplaceFunction(name, tmpl)
			},
			// Converge the function's persistent service containers whenever its
			// new version is swapped in (and on the skip path when it declares
			// services, so crashed replicas self-heal on the periodic tick). The
			// hooks run synchronously in the reconciler pump goroutine, so each is
			// bounded with its own timeout; a slow daemon must not stall a
			// function's reconcile. The contexts are safe to create fresh here —
			// the pump is a single goroutine, so they never race themselves.
			//
			// The prepared env comes from the current registry entry (the runtime
			// plan env); a nil Prepared (unavailable) falls back to no plan env,
			// mirroring the runner's nil-safe behavior.
			UpdateServices: func(name string, tmpl *function.Template, image string) {
				uCtx, cancel := context.WithTimeout(context.Background(), startupTimeout)
				defer cancel()
				var preparedEnv []string
				if cur := runWorker.Registry().GetByName(name); cur != nil && cur.Prepared() != nil {
					preparedEnv = cur.Prepared().Env
				}
				svcCtrl.Apply(uCtx, name, tmpl, image, preparedEnv)
			},
			// On removal, stop the function's service containers BEFORE the images
			// are retired (reconciler calls RemoveServices before RemoveFunction):
			// running service containers reference those images.
			RemoveServices: func(name string) {
				rCtx, cancel := context.WithTimeout(context.Background(), startupTimeout)
				defer cancel()
				svcCtrl.Remove(rCtx, name)
			},
		},
		runWorker.Registry(),
		manager,
		logger,
	)
	for _, fn := range functions {
		rec.Seed(fn)
	}
	logger.Info(fmt.Sprintf("Watching %s for changes", function.Dir))

	// Runs in its own goroutine and stops when ctx is cancelled.
	go rec.Start(ctx)

	// Start the cron scheduler right after the reconciler, so jobs added here
	// (seeded before Start) fire from their first cron tick and jobs the
	// reconciler later converges schedule immediately.
	sched.Start()

	logger.Info(fmt.Sprintf("Consuming stream %q as group %q consumer %q",
		cfg.RedisStream,
		cfg.RedisGroup,
		cfg.ConsumerName,
	))
	if err := consumer.Consume(ctx, runWorker.Handle); err != nil {
		logger.Error(fmt.Sprintf("Consume: %v", err))
		// The consumer is the worker's raison d'être; a Consume error means the
		// consumption loop has stopped, so exit the process rather than return
		// with nothing running. (Shutdown via a cancelled ctx returns nil, so a
		// non-nil error here is a genuine failure.)
		os.Exit(1)
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
		logger.Warn(fmt.Sprintf("Scheduler: graceful shutdown: %v", err))
	}

	// Final flush of the registry into SQLite before the deferred st.Close() runs.
	// Bounded by a short timeout so a wedged SQLite cannot hang shutdown; failure
	// is logged and shutdown continues (telemetry, not state). No-op when metrics
	// are disabled (nil registry).
	finalStatsFlush(metricsInstance, st)

	// Bounded graceful shutdown of the metrics server, so in-flight scrapes drain
	// rather than being cut off mid-request. No-op when metrics are disabled (the
	// server was never created).
	if metricsServer != nil {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := metricsServer.Stop(stopCtx); err != nil {
			logger.Warn(fmt.Sprintf("Metrics server: graceful shutdown: %v", err))
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
			logger.Warn(fmt.Sprintf("Git webhook server: graceful shutdown: %v", err))
		}
	}

	logger.Info("Shutdown complete")
}

// setupMetrics constructs the optional metrics components: a registry and the
// /metrics HTTP server, both nil when METRICS_ADDR is unset. It only CREATES
// them; STARTING happens later in Run once the signal ctx exists (the server
// binds synchronously, well after ctx setup). The nil-safety contract that
// makes this gating safe: every consumer of the registry is nil-safe, AND the
// stats flush is gated alongside it — a nil registry must never feed
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
func prepareFunctions(
	manager *runtime.Manager,
	functions []function.Function,
	st *state.State,
	logger *slog.Logger,
) []*runner.PreparedFunction {
	preparedCount := 0
	prepared := make([]*runner.PreparedFunction, 0, len(functions))
	for _, fn := range functions {
		p, err := manager.Prepare(context.Background(), fn)
		if err != nil {
			logger.Warn(fmt.Sprintf("Function %q: prepare: %v", fn.Name, err))
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
				logger.Warn(fmt.Sprintf("Function %q: fingerprint: %v", fn.Name, fperr))
				fp = ""
			}
			st.RecordReconcileSuccess(fn.Name, p.Image, fp, time.Now(), fn)
		}
		prepared = append(prepared, runner.NewPrepared(fn, p, manager))
		preparedCount++
	}
	logger.Info(fmt.Sprintf("Prepared %d function(s)", preparedCount))
	return prepared
}

// reconcileStartupServices converges each prepared function's persistent service
// containers to its freshly prepared template and image, then sweeps orphaned
// containers for functions no longer on disk. It runs AFTER every function's
// image is built (so the desired image is present) and BEFORE the startup image
// sweep (so the sweep's keep-set can include images the containers we just
// converged reference). Each function gets its OWN bounded context: one slow
// Docker call cannot consume the budget of the functions that follow.
//
// Prepared (available) functions are applied UNCONDITIONALLY — including
// templates that now declare no services: Reconcile with an empty desired set
// stops any containers a previous boot left behind when services were removed
// while Relay was down (the fingerprint was re-seeded from changed content, so
// the reconciler would take the skip path and never converge them otherwise).
//
// An unavailable function (no image this boot) is left alone when its template
// still declares services (its stale containers may still be serving the old
// image, until a later successful reconcile or RemoveAll replaces them); when
// its template no longer declares services, lingering containers are stale by
// definition and are removed now.
func reconcileStartupServices(
	prepared []*runner.PreparedFunction,
	functions []function.Function,
	svcCtrl *reconciler.ServiceReconciler,
	logger *slog.Logger,
) {
	for _, pf := range prepared {
		fn := pf.Function()
		p := pf.Prepared()
		if p == nil {
			// Unavailable function: no image this boot.
			if len(fn.Template.Services) == 0 {
				ctx, cancel := context.WithTimeout(context.Background(), startupTimeout)
				svcCtrl.Remove(ctx, fn.Name)
				cancel()
			} else {
				logger.Warn(fmt.Sprintf("Service: function %q unavailable; skipping service reconcile", fn.Name))
			}
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), startupTimeout)
		svcCtrl.Apply(ctx, fn.Name, fn.Template, p.Image, p.Env)
		cancel()
	}
	// Startup stale-service sweep: remove any service container whose function is
	// not on disk at all (removed while Relay was down, or stale from a previous
	// boot on this host). hostname is NOT part of the predicate — same-host
	// restart is a documented limitation — so this runs once at startup against
	// the full on-disk function set, not per reconcile.
	//
	// liveNames is built from the loaded `functions` slice (on-disk function
	// directories with valid templates). A build-failed (unavailable) function is
	// still "live" because its directory exists and it may have stale containers
	// serving the old image — those must NOT be swept. A persistently-broken
	// template directory is not in `functions` (the loader skips it), so its stale
	// containers could linger until the template is fixed or the dir removed — an
	// accepted v1 edge.
	liveNames := make(map[string]bool, len(functions))
	for _, fn := range functions {
		liveNames[fn.Name] = true
	}
	svcCtrl.SweepOrphans(context.Background(), liveNames)
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
	manager *runtime.Manager,
	functions []function.Function,
	st *state.State,
	logger *slog.Logger,
) {
	keep := make(map[string]bool)
	for _, fn := range functions {
		if fp, err := function.Fingerprint(fn.Dir); err == nil {
			keep[runtime.ImageRef(fn.Name, fp)] = true
		}
	}
	// Images referenced by any Relay-owned service container are kept too: a
	// container kept by the Applys above (unchanged image) or left over from a
	// previous boot that this boot has not yet replaced references its image by
	// label, and the sweep must not remove an image a running container depends
	// on. Removal is deferred to the owning function's reconcile, which replaces
	// the container first and only then retires the image.
	svcCtx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	svcContainers, err := manager.ServiceContainerList(svcCtx)
	cancel()
	if err == nil {
		for _, c := range svcContainers {
			if c.Image != "" {
				keep[c.Image] = true
			}
		}
	} else {
		logger.Warn(fmt.Sprintf("Service: keep-set list (continuing without): %v", err))
	}
	// The state keep-set is only armed when the DB was available.
	if st != nil {
		for _, fn := range functions {
			if d, ok := st.GetFunction(fn.Name); ok && d.Image != "" {
				keep[d.Image] = true
			}
		}
		if _, err := manager.RemoveImagesExcept(context.Background(), keep); err != nil {
			logger.Warn(fmt.Sprintf("Image cleanup: startup sweep: %v", err))
		}
	}
}

// cleanupStartupDependencies runs dependency GC at startup. The startup image
// sweep may remove superseded function images; dependency cleanup can then
// remove dependency images that no managed function image references anymore.
// It runs OUTSIDE the st != nil gate: unlike RemoveImagesExcept, it needs no
// state keep-set — ownership is derived from the managed-image labels the builds
// just stamped. Best-effort and single-shot: an error is logged and left for the
// next natural lifecycle point; it never retries in a loop.
func cleanupStartupDependencies(manager *runtime.Manager, logger *slog.Logger) {
	if _, err := manager.CleanupUnusedDependencies(context.Background()); err != nil {
		logger.Warn(fmt.Sprintf("Dependency image cleanup: %v", err))
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
		metricsInstance.SeedCounter("events_processed_total", gs.EventsProcessedTotal)
		metricsInstance.SeedCounter("handler_success_total", gs.HandlerSuccessTotal)
		metricsInstance.SeedCounter("handler_failure_total", gs.HandlerFailureTotal)
		metricsInstance.SeedCounter("retries_total", gs.RetryTotal)
		metricsInstance.SeedCounter("dlq_entries_total", gs.DLQTotal)
	}
	for _, fs := range st.AllFunctionStats() {
		metricsInstance.SeedFunctionStat(metrics.FunctionStat{
			Function:            fs.Function,
			Events:              fs.EventsProcessedTotal,
			HandlerSuccessTotal: fs.HandlerSuccessTotal,
			HandlerFailureTotal: fs.HandlerFailureTotal,
			RetriesTotal:        fs.RetryTotal,
			DLQTotal:            fs.DLQTotal,
		})
	}
}

// snapshotStats maps the metrics registry into the state database's Stats row.
// The registry counter names feed the SQLite columns directly, with one rename
// (retries_total → RetryTotal) and the float gauges truncated to int64. It is
// nil-safe: a nil registry yields a zero Stats so the snapshot path can never
// panic or block processing.
func snapshotStats(metricsInstance *metrics.Registry) state.Stats {
	if metricsInstance == nil {
		return state.Stats{}
	}
	return state.Stats{
		EventsProcessedTotal:    metricsInstance.Counter("events_processed_total"),
		HandlerSuccessTotal:     metricsInstance.Counter("handler_success_total"),
		HandlerFailureTotal:     metricsInstance.Counter("handler_failure_total"),
		RetryTotal:              metricsInstance.Counter("retries_total"),
		DLQTotal:                metricsInstance.Counter("dlq_entries_total"),
		PendingEntries:          int64(metricsInstance.Gauge("pending_entries")),
		OldestPendingAgeSeconds: int64(metricsInstance.Gauge("pending_oldest_age_seconds")),
	}
}

// funcSnapshotStats maps the registry's per-function counters into the state
// layer's FunctionStats rows. It is nil-safe: a nil registry yields an empty
// slice so the snapshot path can never panic or block processing.
func funcSnapshotStats(metricsInstance *metrics.Registry) []state.FunctionStats {
	if metricsInstance == nil {
		return nil
	}
	stats := metricsInstance.FunctionStatsSnapshot()
	out := make([]state.FunctionStats, 0, len(stats))
	for _, fs := range stats {
		out = append(out, state.FunctionStats{
			Function:             fs.Function,
			EventsProcessedTotal: fs.Events,
			HandlerSuccessTotal:  fs.HandlerSuccessTotal,
			HandlerFailureTotal:  fs.HandlerFailureTotal,
			RetryTotal:           fs.RetriesTotal,
			DLQTotal:             fs.DLQTotal,
		})
	}
	return out
}

// statsLoop is the flush loop: it mirrors the in-memory metrics registry into
// the state database on a fixed interval until ctx is cancelled. The first
// flush runs immediately so the stats row exists before the first tick (this
// also makes `relay stats` useful right after startup). It is nil-safe on both
// the registry and the state handle, so observability can never break
// processing. The loop's ctx is the shutdown ctx; periodic flushes use it
// directly (a cancelled ctx simply stops the loop).
func statsLoop(
	ctx context.Context,
	metricsInstance *metrics.Registry,
	st *state.State,
	interval time.Duration,
) {
	if st == nil {
		<-ctx.Done()
		return
	}
	recordSnapshots(ctx, st, metricsInstance)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			recordSnapshots(ctx, st, metricsInstance)
		}
	}
}

// finalStatsFlush performs a bounded final flush of the registry into SQLite on
// graceful shutdown, so the last interval of telemetry is not lost. It is
// bounded by a short timeout so a wedged SQLite cannot hang shutdown; on
// timeout or error it logs and returns (telemetry, not state). It is nil-safe
// on both the registry and the state handle.
func finalStatsFlush(metricsInstance *metrics.Registry, st *state.State) {
	if metricsInstance == nil || st == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	recordSnapshots(ctx, st, metricsInstance)
}

// recordSnapshots writes the whole stats snapshot — the global stats row and
// one function_stats row per function with any attributed activity — in a
// single short transaction (see state.RecordStatsSnapshot). Before the write it
// enforces the metrics/SQLite consistency invariant: it sweeps the registry with
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
func recordSnapshots(ctx context.Context, st *state.State, metricsInstance *metrics.Registry) {
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
		snapshotStats(metricsInstance),
		funcSnapshotStats(metricsInstance),
	)
}

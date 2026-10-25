// Package worker is the long-running Relay runtime, started via `relay start`.
// It loads apps, builds their images, reconciles them live, and consumes
// the Redis stream, blocking until signalled. Configuration comes entirely
// from the environment.
//
// Startup opens the required PostgreSQL state database first (which takes an
// exclusive per-WORKER_ID PostgreSQL session advisory lock), then runs an
// explicit external-dependency preflight (see runExternalPreflight) BEFORE any
// app is loaded or fingerprinted, and before the runtime socket, services,
// sweeps, preparation, listener starts, background loops, scheduler, and
// reconciler. The fixed preflight order is: Redis stream/consumer-group
// readiness, then Docker runtime-manager readiness, then verification of the
// configured NETWORKS set, then the runtime manager's own background
// maintenance loop is started. A failure at any step short-circuits every later
// phase; a lifecycle cancellation during the preflight is a graceful shutdown.
//
// Immediately after the state database is opened (and before the preflight) the
// worker starts an ownership monitor that re-verifies that lock on its pinned
// connection; if the lock is lost while the process keeps running, the monitor
// cancels the lifecycle, clears readiness, and the worker exits with an
// ownership-loss error after skipping identity-scoped cleanup. The same
// pinned-connection proof is re-run once after the monitor joins at shutdown
// and again immediately before each identity-scoped cleanup, so a loss missed
// between polls still fails closed — see internal/worker/ownership.go and
// internal/state/ownership.go.
//
// Its in-memory accounting registry is always created and
// is the single source of truth for the operational counters; the Prometheus
// /metrics HTTP endpoint (see internal/observability/metrics) is an optional
// exposition of that same registry, gated on the METRICS_ADDR environment
// variable. The worker flushes the registry into the local state database on a
// fixed 5-second cadence independent of METRICS_ADDR: stats accumulate in
// memory, Prometheus (when exposed) reflects them immediately, and PostgreSQL
// receives the current absolute snapshot every interval. The stats flusher
// (which serializes the flush and the reset) lets the socket's `reset stats`
// command restart the
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
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/codes"

	"relay/internal/app"
	"relay/internal/bytesize"
	"relay/internal/config"
	gitwh "relay/internal/git/webhook"
	"relay/internal/observability/metrics"
	"relay/internal/observability/tracing"
	"relay/internal/reconciler"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/secrets"
	"relay/internal/state"
	"relay/internal/stream"
)

// reconcileTimeout bounds every bounded service-reconcile daemon operation:
// the startup orphan container sweep, the startup image keep-set list, the
// coordinator's per-removal operation context (RemoveAndWait/EnqueueRemove), and
// — injected into the ServiceReconciler — each normal pre-resolution and
// post-resolution Docker operation inside a per-app Apply. Each such
// operation gets its own fresh bound so one slow Docker call cannot consume the
// budget of the calls
// that follow. It deliberately does NOT bound Dockerfile builds: a build is
// bounded by runtime.buildTimeout (10m) on a context rooted in the worker
// lifecycle, so a slow image build can never be cut off by this short reconcile
// budget. Apply receives the worker lifecycle context (NOT this constant wrapped
// around the whole pass) and the reconciler derives the per-operation bounds
// from it. The 5s shutdown step bounds are a separate, deliberately shorter
// bound (shutdownStepTimeout), not this constant.
const reconcileTimeout = 30 * time.Second

// effectiveMaxConcurrentInvocations mirrors the runner's
// SetMaxConcurrentInvocations normalization
// (<1 → runner.DefaultMaxConcurrentInvocations) so the "Concurrency limits" log reflects
// the value actually enforced regardless of the configured raw value.
func effectiveMaxConcurrentInvocations(n int) int {
	if n < 1 {
		return runner.DefaultMaxConcurrentInvocations
	}
	return n
}

// effectiveMaxConcurrentBuilds mirrors the runtime's WithMaxConcurrentBuilds
// normalization (<1 → runtime.DefaultMaxConcurrentBuilds) so the "Concurrency
// limits" log reflects the value actually enforced.
func effectiveMaxConcurrentBuilds(n int) int {
	if n < 1 {
		return runtime.DefaultMaxConcurrentBuilds
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

// effectiveMaxEventBytes mirrors the stream consumer's MaxEventBytes
// normalization (<1 → stream.DefaultMaxEventBytes) so the "Concurrency limits"
// log reflects the value actually enforced.
func effectiveMaxEventBytes(n int) int {
	if n < 1 {
		return stream.DefaultMaxEventBytes
	}
	return n
}

// logConcurrencyLimits emits the startup "Concurrency limits" summary. Each
// value is the one actually enforced: the invocation/build/buffered limits go
// through the same <1 normalization their consumers apply, max_warm_containers
// is used verbatim, and max_event_bytes is rendered as a human-readable binary
// size (via bytesize.Format) so the enforced 256 KiB default logs as
// "256KiB" rather than its raw 262144 byte count. It is split out of Run so the
// emitted summary is testable without exercising the full worker startup.
func logConcurrencyLimits(logger *slog.Logger, cfg config.Config) {
	logger.Info(
		"Concurrency limits",
		"max_concurrent_invocations", effectiveMaxConcurrentInvocations(cfg.MaxConcurrentInvocations),
		"max_concurrent_builds", effectiveMaxConcurrentBuilds(cfg.MaxConcurrentBuilds),
		"max_warm_containers", cfg.MaxWarmContainers,
		"max_buffered_events", effectiveMaxBuffered(cfg.MaxBufferedEvents),
		"max_event_bytes", bytesize.Format(int64(effectiveMaxEventBytes(cfg.MaxEventBytes))),
	)
}

// errStartupInterrupted marks a fallible startup operation that failed only
// because the worker lifecycle was cancelled (SIGTERM/SIGINT) while it was in
// flight. Run converts it to a nil return: the process is shutting down anyway,
// the deferred cleanup converges every resource, and a cancelled startup is a
// graceful shutdown, not a startup failure. It is never returned to the CLI.
var errStartupInterrupted = errors.New("startup interrupted by shutdown")

// startupInterrupted reports whether a fallible startup operation failed only
// because the worker lifecycle was cancelled (or its deadline elapsed) rather
// than for a genuine reason. The stream and runtime layers wrap the parent
// context error, so errors.Is is the reliable predicate; an operation error that
// merely races cancellation without being a context error is still treated as
// genuine and surfaced. It deliberately requires ctx.Err() != nil so a stray
// context.Canceled from an unrelated child context is never misclassified.
func startupInterrupted(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() == nil {
		return false
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// logStartupCleanupFailure logs a non-fatal startup cleanup failure at Warn, or
// at Debug when the lifecycle is being cancelled: a sweep interrupted by
// shutdown is the normal path and must not pollute the shutdown trace with
// warnings (the barrier already skips sweeps wholesale on a cancelled
// lifecycle; this covers a cancellation landing mid-pass). The message is
// identical in both cases so a genuine failure stays recognisable.
func logStartupCleanupFailure(ctx context.Context, logger *slog.Logger, msg string, err error) {
	if startupInterrupted(ctx, err) {
		logger.Debug(msg, "error", err)
		return
	}
	logger.Warn(msg, "error", err)
}

// startupResult converts the errStartupInterrupted sentinel into a nil return so
// Run reports a cancelled startup as a graceful success, while any genuine
// startup failure is returned unchanged. It is the single place the conversion
// is decided, so the "cancellation is not a failure" contract is unit-testable
// without exercising Run's full resource wiring.
func startupResult(err error) error {
	if errors.Is(err, errStartupInterrupted) {
		return nil
	}
	return err
}

// preflightDeps is the explicit ordering seam for the worker's
// external-dependency preflight. Production Run wires the four real
// operations; a test wires spies, so the fixed order and the
// stop-at-first-failure boundary are provable without Redis or Docker. It is
// deliberately a plain struct of four named steps, not a generic framework:
// the ordering is the point, and it must stay visible in one place.
type preflightDeps struct {
	// EnsureGroup establishes Redis stream/consumer-group readiness by
	// delegating to stream.EnsureGroup.
	EnsureGroup func(ctx context.Context) error
	// OpenManager establishes Docker readiness and returns the runtime manager
	// every later phase uses. The implementation registers the manager's
	// deferred shutdown before returning, and opens it with deferred
	// maintenance so no manager background loop runs before VerifyNetworks.
	OpenManager func(ctx context.Context) (*runtime.Manager, error)
	// VerifyNetworks verifies the configured NETWORKS set against the manager.
	VerifyNetworks func(ctx context.Context, manager *runtime.Manager) error
	// StartMaintenance starts the runtime manager's deferred background
	// maintenance loop. It is the LAST preflight step, reached only after
	// NETWORKS verification succeeds, so no manager background loop runs while
	// the worker's prerequisites are still unproven. It is infallible: a
	// second call is a no-op, and a manager whose loop was never deferred is
	// already running, so the call is safe for every caller.
	StartMaintenance func(manager *runtime.Manager)
}

// runExternalPreflight runs the worker's external-dependency preflight in the
// fixed order Redis stream/group readiness -> Docker runtime-manager readiness
// -> configured NETWORKS verification -> manager maintenance start. It runs
// after the required state database is opened, and BEFORE any app is loaded or
// fingerprinted and before the runtime socket, services, sweeps, preparation,
// listener starts, background loops, scheduler, and reconciler are touched. A
// failure at any step short-circuits every later step and is returned so Run
// unwinds through its deferred shutdown;
// a failure that only reflects the lifecycle being cancelled is classified as
// errStartupInterrupted for a graceful nil return, and a genuine failure is
// wrapped with its step's context.
//
// The manager is opened with deferred maintenance, and its single background
// maintenance loop is started only as the final step — after NETWORKS
// verification succeeds — so no manager loop runs while a prerequisite is still
// unproven. If verification fails, no loop was ever started and Run's deferred
// shutdown still closes the manager (and its Docker client) cleanly.
//
// The context argument is the startup trace root (a child of the worker
// lifecycle), so each step's span is parented correctly, and a cancellation of
// the lifecycle is observable through it.
func runExternalPreflight(
	ctx context.Context,
	logger *slog.Logger,
	deps preflightDeps,
) (*runtime.Manager, error) {
	_, redisSpan := tracing.Start(ctx, "redis.consumer_group")
	if err := deps.EnsureGroup(ctx); err != nil {
		interrupted := startupInterrupted(ctx, err)
		if !interrupted {
			redisSpan.RecordError(err)
			redisSpan.SetStatus(codes.Error, err.Error())
		}
		redisSpan.End()
		if interrupted {
			logger.Info("Startup: consumer group creation interrupted by shutdown", "error", err)
			return nil, errStartupInterrupted
		}
		return nil, fmt.Errorf("ensure consumer group failed: %w", err)
	}
	redisSpan.End()

	_, managerSpan := tracing.Start(ctx, "runtime.initialize")
	manager, err := deps.OpenManager(ctx)
	if err != nil {
		managerSpan.RecordError(err)
		managerSpan.SetStatus(codes.Error, err.Error())
		managerSpan.End()
		// A lifecycle cancellation during the bounded startup ping is a
		// shutdown, not a daemon failure: classify it like every other fallible
		// startup step so a graceful stop is not reported as an error.
		if startupInterrupted(ctx, err) {
			logger.Info("Startup: runtime manager initialization interrupted by shutdown", "error", err)
			return nil, errStartupInterrupted
		}
		// The runtime manager owns container execution, which the worker cannot
		// serve without. Run's deferred cleanup closes the Redis client.
		return nil, fmt.Errorf("runtime: new manager failed: %w", err)
	}
	managerSpan.End()

	_, networkSpan := tracing.Start(ctx, "network.verify")
	if err := deps.VerifyNetworks(ctx, manager); err != nil {
		networkSpan.RecordError(err)
		networkSpan.SetStatus(codes.Error, err.Error())
		networkSpan.End()
		// A lifecycle cancellation during verification is a shutdown, not a
		// network failure: classify it like every other fallible startup step.
		if startupInterrupted(ctx, err) {
			logger.Info("Startup: NETWORKS verification interrupted by shutdown", "error", err)
			return nil, errStartupInterrupted
		}
		return nil, err
	}
	networkSpan.End()

	// Last: start the manager's deferred maintenance loop. It is deliberately
	// after NETWORKS verification so no manager background loop ran while the
	// preflight's prerequisites were still unproven; only now, when every
	// external dependency is ready, does the warm-container eviction ticker
	// begin. The step is infallible and idempotent.
	if deps.StartMaintenance != nil {
		deps.StartMaintenance(manager)
	}

	return manager, nil
}

// Run wires the whole worker: startup state, then the reconciler and stream
// consumer. It blocks in Consume until the process is signalled or a terminal
// reconciler failure cancels the lifecycle, then runs graceful shutdown. It
// returns an error and never calls os.Exit: the CLI
// boundary owns process exit, and every post-resource startup or runtime failure
// converges through the SAME deferred cleanup as a normal shutdown, rather than
// abandoning live servers, goroutines, containers, and the state DB to process
// exit.
//
// Errors are returned through the named runErr so the ownership-loss override
// (registered before the shutdown defer, and therefore run after it) can join
// errOwnershipLost into whatever the deferred teardown path returned; see
// internal/worker/ownership.go.
func Run(logger *slog.Logger) (runErr error) {
	cfg, err := config.Load(logger)
	if err != nil {
		// Pre-resource failures: nothing owns cleanup yet, so return for the CLI.
		return fmt.Errorf("invalid configuration: %w", err)
	}
	redisOpts, err := config.RedisOptions(cfg.RedisURI)
	if err != nil {
		return fmt.Errorf("redis config invalid: %w", err)
	}
	client := redis.NewClient(redisOpts)

	// The worker lifecycle owns every bounded startup operation, Dockerfile
	// builds, and reconciler hook. Created before the runtime manager so builds
	// are cancelled on SIGINT/SIGTERM or when Run returns.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	// Tracing is optional observability: setup always returns a usable provider,
	// and a setup failure leaves the worker running untraced.
	tracerProvider, tracerErr := tracing.Setup(ctx, logger)
	if tracerErr != nil {
		logger.Warn("Tracing: setup failed; continuing without tracing", "error", tracerErr)
	}

	// Startup trace root. The deferred End below covers every early return; the
	// explicit End at the ready boundary closes it before consumption starts.
	startupCtx, startupSpan := tracing.Start(ctx, "relay.startup")

	// The accounting registry always exists for the persistent PostgreSQL stats
	// flush; only the /metrics HTTP exposition is opt-in (server nil when
	// METRICS_ADDR is unset). The server starts later, after wiring completes.
	metricsInstance, metricsServer := setupMetrics(cfg, logger)

	// Every return path converges through this ordered teardown. Steps register
	// as resources are acquired but run in shutdownStepOrder (Redis FIRST in,
	// LAST out), and the defer is installed before the first fallible resource
	// so an early failure still releases everything acquired. Barriers
	// (scheduler, reconciler, housekeeping, services-join, loops) are strictly
	// joined before a later step closes the dependency they use.
	shutdown := &shutdownRegistry{}
	// A terminal reconciler failure cancels the lifecycle, but Consume reports
	// that cancellation as nil. The shutdown defer drains this after the
	// reconciler barrier has joined its Start goroutine.
	reconcilerFailures := make(chan error, 1)
	// Latched by the ownership monitor and read by readiness, identity-scoped
	// shutdown guards, and the override below.
	ownership := &ownershipLoss{}
	// Registered BEFORE the shutdown defer so it runs AFTER teardown (LIFO):
	// a detected loss turns the final result into a distinctive error.
	defer func() {
		runErr = ownershipRunError(ownership, runErr)
	}()
	// Starts false; set only at the ready-to-consume boundary and cleared as the
	// first shutdown instruction. Bound to the lifecycle so a cancellation that
	// precedes that clear still reports not-ready.
	workerReady := newReadiness(ctx)
	// Redis is acquired first and released last.
	shutdown.register(shutdownStep{
		name:    shutdownStepRedis,
		timeout: 5 * time.Second,
		run:     func(context.Context) error { return client.Close() },
	})
	// Tracing stops last, after every span producer has stopped.
	shutdown.register(shutdownStep{
		name:    shutdownStepTracing,
		timeout: 5 * time.Second,
		run:     tracerProvider.Shutdown,
	})

	// Shutdown order is load-bearing: readiness clears FIRST, then the lifecycle
	// is cancelled, the root span ends, and only then does ordered teardown run.
	defer func() {
		workerReady.setNotReady()
		stop()
		startupSpan.End()
		shutdown.run(logger)
		runErr = reconcilerRunResult(runErr, reconcilerFailures)
	}()

	// The state DB is a REQUIRED dependency (operator view, stats snapshots,
	// schedule outbox) and is opened before preflight or any app work. A failure
	// is fatal; the deferred shutdown releases everything acquired so far.
	st, err := openWorkerState(startupCtx, logger, cfg, metricsInstance)
	if err != nil {
		return startupResult(err)
	}
	// Track ownership immediately after open so a lock lost during startup still
	// cancels the lifecycle; its barrier is registered here so an early failure
	// joins the monitor before the state handle closes.
	//
	// The once-only loss action and the fail-closed proof are ONE seam, so a
	// loss detected by the monitor, the final barrier check, or an
	// identity-cleanup guard latches and skips identity-scoped work identically.
	onOwnershipLost := ownershipLossAction(
		ownership, workerReady, logger, cfg.WorkerID, cfg.InstanceID, stop,
	)
	proof := startOwnershipTracking(
		ctx, st, ownership, onOwnershipLost, logger, shutdown,
	)
	// Registered before further startup gives an early failure and a normal
	// shutdown the same state release; scheduler work is joined before it.
	shutdown.register(shutdownStep{
		name:    shutdownStepState,
		timeout: 10 * time.Second,
		run:     func(context.Context) error { return st.Close() },
	})

	// Fixed external-dependency order: Redis/group, Docker manager, NETWORKS,
	// then the manager's deferred maintenance loop. Any failure short-circuits
	// every later phase; a cancellation here is a graceful shutdown.
	manager, err := runExternalPreflight(startupCtx, logger, preflightDeps{
		EnsureGroup: func(ctx context.Context) error {
			return stream.EnsureGroup(ctx, client, cfg.RedisStream, cfg.RedisGroup)
		},
		// Root the manager in the worker lifecycle, not the preflight span, so
		// builds and the startup ping observe SIGTERM/SIGINT.
		OpenManager: func(_ context.Context) (*runtime.Manager, error) {
			return openRuntimeManager(ctx, logger, metricsInstance, cfg, shutdown)
		},
		VerifyNetworks: func(ctx context.Context, m *runtime.Manager) error {
			// Relay never creates networks, so a missing one must fail startup
			// before any container exists. Unset NETWORKS skips the check.
			return verifyConfiguredNetworks(ctx, m, cfg.Networks)
		},
		// Last: start the deferred warm-container eviction loop only after
		// NETWORKS succeeds. Infallible and idempotent.
		StartMaintenance: func(m *runtime.Manager) {
			m.StartMaintenance()
		},
	})
	if err != nil {
		return startupResult(err)
	}
	logger.Info("Startup: external dependencies ready",
		"redis_stream", cfg.RedisStream,
		"redis_group", cfg.RedisGroup,
		"networks", len(cfg.Networks),
	)

	// Best-effort reclaim of private build-temp sessions a previous process
	// abandoned. Runs after the manager session exists and before any app work;
	// a session is removed only when its owner lock is acquirable.
	scavengeCtx, scavengeCancel := context.WithTimeout(startupCtx, reconcileTimeout)
	scavenged, scavengeErr := manager.ScavengeBuildRoots(scavengeCtx)
	scavengeCancel()
	if scavengeErr != nil {
		logStartupCleanupFailure(
			ctx, logger, "Startup: build temp scavenge failed", scavengeErr,
		)
	} else if scavenged > 0 {
		logger.Info(
			"Startup: removed abandoned build temp sessions from a previous relay process",
			"count", scavenged,
		)
	}

	// One shared secrets provider for the webhook and runner. A missing secret
	// surfaces per invocation; construction failure is fatal.
	secretProvider, err := secrets.NewLocalProvider(secrets.SecretsDir)
	if err != nil {
		return fmt.Errorf("secrets: new local provider failed: %w", err)
	}

	// Optional Git webhook, gated on GIT_WEBHOOK_ADDR. NewServer owns assembly
	// and returns nil when disabled; Run only constructs, starts, and registers
	// its teardown.
	var gitWebhookServer *gitwh.Server
	if cfg.GitWebhookAddr != "" {
		gitWebhookServer = gitwh.NewServer(cfg.GitWebhookAddr, logger, gitwh.Config{Secrets: secretProvider})
	}

	startupSet, err := loadStartupApps(startupCtx, logger, st, metricsInstance)
	if err != nil {
		return startupResult(err)
	}
	apps := startupSet.loaded
	fingerprints := startupSet.fingerprints

	// Single owner of registry-to-PostgreSQL stats writes. Its mutex spans
	// snapshot capture and write, so a socket reset can never be overwritten by
	// a flush holding pre-reset values; shared by the loop, final flush, and
	// socket reset command.
	statsFlusher := newStatsFlusher(st, metricsInstance)
	statsFlusher.retention = cfg.StatsRetention
	statsFlusher.log = logger
	shutdown.register(shutdownStep{
		name:    shutdownStepStatsFlush,
		timeout: 2 * time.Second,
		// Identity-scoped: skip once ownership is lost so this worker's stale
		// totals cannot overwrite a replacement's row. The guard re-proves the
		// lock immediately before the flush.
		run: guardIdentityCleanup(proof, logger, shutdownStepStatsFlush,
			func(stepCtx context.Context) error {
				finalStatsFlush(stepCtx, statsFlusher)
				return nil
			}),
	})

	// The live runtime-pool query socket; its readiness checker stays not-ready
	// until the ready-to-consume boundary installs the live probe.
	rtSocket, err := startRuntimeSocket(
		startupCtx, logger, manager, statsFlusher, workerReady, shutdown,
	)
	if err != nil {
		return err
	}

	// Persistent service controller/coordinator; see startServiceConvergence.
	svcCtrl, services := startServiceConvergence(
		ctx, logger, cfg, manager, secretProvider, proof, metricsInstance, shutdown,
	)

	// Remove execution containers a previous process on this worker identity
	// left behind before preparing any app.
	sweepStartupExecutionOrphans(
		ctx, startupCtx, logger, manager, cfg.WorkerID, proof,
	)

	prepared := prepareAndStartApps(
		ctx, startupCtx, logger, manager, st, startupSet,
		services, svcCtrl, proof, shutdown,
	)

	// Runner before consumer: the consumer wires it as its ScheduleRunner seam.
	runWorker := newWorkerRunner(
		prepared, logger, cfg, metricsInstance, secretProvider, rtSocket,
	)

	consumer := newStreamConsumer(client, logger, cfg, metricsInstance, runWorker)

	// Optional metrics and webhook servers bind synchronously; a bind failure is
	// fatal and each teardown step registers only after a successful Start.
	metricsLoggerDone, err := startMetricsServer(
		ctx, logger, cfg, metricsInstance, metricsServer, shutdown,
	)
	if err != nil {
		return err
	}
	if err := startWebhookServer(
		logger, cfg, gitWebhookServer, shutdown,
	); err != nil {
		return err
	}

	startBackgroundLoops(
		ctx, logger, cfg, client, statsFlusher, metricsLoggerDone, shutdown,
	)

	sched := newWorkerScheduler(
		ctx, startupCtx, logger, cfg, client, st, metricsInstance,
		consumer, apps, shutdown,
	)
	// Emitted after the scheduler's own startup lines to preserve log order.
	logConcurrencyLimits(logger, cfg)

	// Watch /apps and reconcile live. Callback wiring lives in
	// appReconcilerConfig; startAppReconciler owns watch/seed/start ordering.
	rec := reconciler.New(
		appReconcilerConfig(
			app.Dir, st, runWorker, metricsInstance,
			statsFlusher, manager, sched, services,
		),
		runWorker.Registry(),
		manager,
		logger,
	)
	_, reconcilerSpan := tracing.Start(startupCtx, "reconciler.start")
	startAppReconciler(
		ctx, logger, reconcilerSpan, rec, apps, fingerprints,
		startupSet.fencingTokens, prepared, shutdown,
		func(err error) {
			reportReconcilerFailure(ctx, stop, reconcilerFailures, err)
		},
	)
	if ctx.Err() != nil {
		// A failed watch setup latches its error and cancels the lifecycle. A
		// signal during startup is also cancellation, but is otherwise graceful;
		// in either case let the deferred shutdown finish before returning rather
		// than starting the scheduler or claiming readiness.
		reconcilerSpan.End()
		return nil
	}

	// Start the scheduler only after the reconciler watches and joins, so seeded
	// jobs fire on their first tick and live changes converge immediately. Its
	// shutdown barrier was registered at construction.
	sched.Start()
	reconcilerSpan.End()

	// The startup root ends here; consumption establishes its own spans.
	startupSpan.End()

	// Ready-to-consume boundary: the LAST step before Consume. It deliberately
	// does not wait on asynchronous services/housekeeping, optional tracing,
	// PostgreSQL, or per-app success.
	workerReady.startReady(consumer, manager, cfg.Networks)
	logger.Info("Worker ready to consume")

	// Stats rows are keyed by worker identity; the instance id fences a stale
	// incarnation of the same worker out of the row.
	logger.Info("Worker stats identity",
		"worker_id", cfg.WorkerID,
		"instance_id", cfg.InstanceID,
	)

	logger.Info("Consuming stream",
		"stream", cfg.RedisStream,
		"group", cfg.RedisGroup,
		"consumer", cfg.WorkerID,
	)
	var consumeErr error
	if err := consumer.Consume(ctx, runWorker.Handle); err != nil {
		// A Consume error means the loop stopped. A cancelled lifecycle returns
		// nil here, so this is a genuine failure; teardown still runs in order.
		logger.Error("Consume failed", "error", err)
		consumeErr = fmt.Errorf("consume failed: %w", err)
	}

	// All teardown is owned by the registry registered at the top.
	return consumeErr
}

// *runtime.Manager must always implement the reconciler's optional
// shutdown-scoped Docker capability. The reconciler type-asserts the
// configured Docker for it and otherwise silently falls back to the ordinary
// (lifecycle-refused) methods, so this compile-time assertion keeps a
// production regression from degrading graceful service cleanup without
// failing the build. It is always compiled, unlike the capability tests.
var _ reconciler.ShutdownDocker = (*runtime.Manager)(nil)

// setupMetrics constructs the metrics components. The accounting registry is
// ALWAYS constructed — it is the single source of truth for the counters the
// persistent PostgreSQL snapshot flushes, so it must exist even when HTTP
// exposition is disabled. Only the /metrics HTTP server is opt-in: it is nil
// when METRICS_ADDR is unset. It only CREATES them; STARTING the server happens
// later in Run once the startup wiring is complete (the server binds
// synchronously, well after ctx setup). When the address is configured the
// returned server is non-nil, which is also the gate for the periodic snapshot
// logger; the registry itself is never nil.
func setupMetrics(cfg config.Config, logger *slog.Logger) (*metrics.Registry, *metrics.Server) {
	metricsInstance := metrics.New()
	if cfg.MetricsAddr == "" {
		return metricsInstance, nil
	}
	metricsServer := metrics.NewServer(cfg.MetricsAddr, metricsInstance.Handler(), logger)
	return metricsInstance, metricsServer
}

// wireStatusObserver installs the state status observer that projects every
// persisted lifecycle transition onto the one-hot app_status gauge. An
// EMPTY status is the removal/prune signal and deletes the app's status
// series; every other status is written one-hot. It is nil-safe on both the
// state handle and the registry, so a failed state open (or a nil registry in a
// test) is a silent no-op. It centralizes the state→metrics bridge in one
// closure so startup discovery, the reconciler's status writes, and the service
// callbacks all stay in sync without state importing metrics.
//
// When the state handle is nil, no status is projected: every state status write
// is nil-guarded elsewhere, so there is no authoritative status to observe, and
// synthesizing one would invent a value the worker does not actually know. The
// gauge is simply absent in that degraded mode, which is preferable to a wrong
// one.
func wireStatusObserver(st *state.State, metricsInstance *metrics.Registry) {
	if st == nil {
		return
	}
	st.SetStatusObserver(func(name, status string) {
		if status == "" {
			metricsInstance.RemoveAppStatus(name)
			return
		}
		metricsInstance.SetAppStatus(name, status)
	})
}

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
	"io"
	"log/slog"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/codes"

	"relay/internal/app"
	"relay/internal/bytesize"
	"relay/internal/config"
	"relay/internal/cron"
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

// statsFlushInterval is the fixed PostgreSQL snapshot cadence. Telemetry, not
// event-processing state: Prometheus stays live in-process, while PostgreSQL
// receives the current absolute snapshot every interval. Deliberately NOT
// env/flag-configurable — a fixed cadence keeps the durability model simple
// (a hard crash loses at most one interval of telemetry).
const statsFlushInterval = 5 * time.Second

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

// shutdownServiceTimeout is the per-step bound shared by the services-join
// barrier and the service-cleanup step during graceful shutdown. For the barrier
// it is only the wait-before-diagnostic threshold: on expiry the join is
// cancelled and then strictly joined for real, so draining the coordinator and
// its in-flight Applys may take longer. For the cleanup step it bounds the
// best-effort stop/removal of this worker's persistent service containers, so a
// hung Docker call cannot hold shutdown open.
const shutdownServiceTimeout = 30 * time.Second

// shutdownStepTimeout is the per-step bound for the steps that gracefully stop a
// server (scheduler, metrics, webhook). The shutdown registry derives a fresh
// context.Background bound from it per step, so one slow step can never consume
// another step's budget. For the scheduler barrier it is only the
// wait-before-diagnostic threshold: the step is then cancelled and strictly
// joined, so it may exceed this bound. For metrics and webhook (best-effort
// cleanup) it is the actual bound.
const shutdownStepTimeout = 5 * time.Second

// shutdownAggregateTimeout is the aggregate budget for the BEST-EFFORT shutdown
// steps, an internal constant with no user knob. The shutdown registry gives
// every step a context bounded by min(its own cap, what remains of this budget),
// and runs each step in its own goroutine so a step that ignores its context can
// never hang the registry: a best-effort step that misses its deadline is logged
// and the registry moves on to the next step. The budget is deliberately
// generous (2m) relative to the per-step caps (2-30s): with cooperative steps it
// is never reached, and it is the ceiling on how long best-effort cleanup may
// take.
//
// It is NOT a hard cap on process exit. A quiescence barrier step (see
// shutdownStep.barrier) that misses its bound is cancelled and then STRICTLY
// JOINED, so a wedged dependency-holding operation may extend the shutdown past
// this budget. That is a safety-over-latency trade: a later step must never
// close the runtime manager, state DB, or Redis while a reconcile, service pass,
// sweep, loop, or scheduler publisher callback may still be using it. Every
// barrier's joined operation is rooted in the worker lifecycle and individually
// bounded, so it still terminates.
const shutdownAggregateTimeout = 2 * time.Minute

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

// Shutdown step names. Each names a single teardown step; together they are the
// explicit teardown order (shutdownStepOrder) and the structured `step` field
// on a failure log. Registration happens as resources are acquired — which is
// NOT teardown order (Redis is acquired first and released last) — so the order
// is declared here rather than inherited from registration/LIFO.
const (
	shutdownStepOwnership      = "ownership"
	shutdownStepSocket         = "socket"
	shutdownStepScheduler      = "scheduler"
	shutdownStepReconciler     = "reconciler"
	shutdownStepHousekeeping   = "housekeeping"
	shutdownStepServicesJoin   = "services-join"
	shutdownStepServiceCleanup = "service-cleanup"
	shutdownStepLoops          = "loops"
	shutdownStepStatsFlush     = "stats-flush"
	shutdownStepMetrics        = "metrics"
	shutdownStepWebhook        = "webhook"
	shutdownStepManager        = "manager"
	shutdownStepState          = "state"
	shutdownStepTracing        = "tracing"
	shutdownStepRedis          = "redis"
)

// shutdownStepOrder is the single source of truth for graceful-shutdown
// ordering, walked top to bottom by shutdownRegistry.run. Lifecycle
// cancellation is deliberately NOT a step: Run cancels the lifecycle before
// invoking the registry, so background loops, the coordinator, and rooted
// builds observe cancellation before teardown joins them. A step that was never
// registered (an optional resource that never started) is simply skipped.
//
// Each step is one of two kinds, not a per-step choice made at random:
//
//   - QUIESCENCE BARRIER (shutdownStep.barrier): joins a background operation
//     that holds a shared dependency a LATER step tears down. Missed bound is
//     logged, the context is cancelled, and the step is then strictly joined
//     before the registry advances, so it may exceed both its own bound and the
//     aggregate budget. Barriers:
//     ownership (joins the lock monitor and runs the final fail-closed
//     ownership verification before the state handle closes),
//     scheduler (joins publisher callbacks before Redis closes), reconciler
//     (writes state, drives the manager), housekeeping (startup sweeps use the
//     manager and state DB), services-join (coordinator workers use manager and
//     state DB), loops (the stats and retention loops use the state DB and
//     Redis).
//   - BEST-EFFORT CLEANUP (everything else): bounded, logged on timeout or
//     failure, and never joined past its bound. Cleanup:
//     socket, service-cleanup, stats-flush, metrics, webhook, manager, state,
//     tracing, redis.
//
// Only barrier steps gate shared-dependency teardown. A cleanup step that
// ignores its context is surfaced as a timeout and left running by design; none
// of them is a join of a resource-holding operation.
var shutdownStepOrder = []string{
	shutdownStepOwnership,
	shutdownStepSocket,
	shutdownStepScheduler,
	shutdownStepReconciler,
	shutdownStepHousekeeping,
	shutdownStepServicesJoin,
	shutdownStepServiceCleanup,
	shutdownStepLoops,
	shutdownStepStatsFlush,
	shutdownStepMetrics,
	shutdownStepWebhook,
	shutdownStepManager,
	shutdownStepState,
	shutdownStepTracing,
	shutdownStepRedis,
}

// shutdownStep is one teardown action. run receives a fresh context derived
// from context.Background by the registry and bounded by min(its own timeout,
// the remaining aggregate budget): a non-positive timeout means the step is
// bounded only by the aggregate budget (never unbounded). Its error, when
// non-nil, is logged with the structured name and never aborts the remaining
// steps. A step that ignores its context is surfaced as a timeout rather than
// allowed to hang the registry (the registry runs each step in its own
// goroutine).
//
// A step is either a best-effort cleanup step (barrier false) or a quiescence
// barrier (barrier true); see shutdownStepOrder for the full classification.
type shutdownStep struct {
	name    string
	timeout time.Duration
	// barrier marks a QUIESCENCE BARRIER: a join of a background operation that
	// holds a shared dependency the LATER steps tear down (the runtime manager,
	// the state DB, or Redis). A best-effort cleanup step that times out is left
	// running while teardown continues; a barrier step that times out has its
	// context cancelled and is then joined for real before the registry
	// advances. Cancellation alone is insufficient: a non-cooperative reconcile
	// or service pass would keep using the dependency while it is closed. Only
	// steps that gate shared-dependency teardown set this.
	barrier bool
	run     func(context.Context) error
}

// kind labels the step in diagnostics: a quiescence barrier (strictly joined
// past its bound) versus a best-effort cleanup step (logged and skipped on
// timeout). It keeps the timeout log actionable without a second log line.
func (s shutdownStep) kind() string {
	if s.barrier {
		return "barrier"
	}
	return "cleanup"
}

// shutdownRegistry is the small ordered teardown the worker runs on every
// return path once resources exist. Steps are registered as each resource is
// successfully acquired/started, but run strictly by shutdownStepOrder, so
// ordering is explicit rather than defer/LIFO. It is not safe for concurrent
// registration (Run registers from its single startup goroutine).
type shutdownRegistry struct {
	steps []shutdownStep
	// budget overrides shutdownAggregateTimeout when > 0. It exists so tests can
	// exercise the aggregate cap deterministically without waiting minutes;
	// production leaves it zero.
	budget time.Duration
}

// barrierJoin is the per-step half of the registry's barrier contract. It runs
// join under stepCtx; when the bound expires (join returns its context error)
// it runs join AGAIN with context.Background, so the step does not return until
// the operation it guards is actually quiescent. The registry cancels the
// step's context at the deadline and then joins the step goroutine, so this is
// what makes that join wait for the real operation rather than an early
// timeout return — a later step may close the runtime manager or state DB only
// once the reconcile/service pass is done using it.
//
// The second join is deliberately unbounded: every joint operation is rooted in
// the worker lifecycle and bounded by its own per-operation timeout, so it
// terminates. Waiting longer is the point — safety over teardown overlap. The
// original (timeout) error is returned so the registry keeps its normal timeout
// logging.
//
// The second join runs only when the first failed because stepCtx expired: any
// other error means the join already reported completion (a non-context error is
// a genuine join outcome), so a later step may proceed. This mirrors Join's own
// contract — it returns nil once the operation has exited and its context error
// only while still waiting.
func barrierJoin(stepCtx context.Context, join func(context.Context) error) error {
	err := join(stepCtx)
	if err == nil || stepCtx.Err() == nil {
		return err
	}
	_ = join(context.Background())
	return err
}

// waitSignal waits for done under ctx, returning ctx.Err() if the bound fires
// first.
func waitSignal(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// reconcilerBarrierStep is the reconciler shutdown barrier: it joins the
// reconciler's Start (reconcilerDone is closed once its pump/ticker/eventLoop
// have exited). The reconciler writes the state DB and drives the runtime
// manager, so this is a quiescence barrier ahead of manager/state teardown: the
// strict join waits for the real reconcile to exit even past the step bound.
func reconcilerBarrierStep(reconcilerDone <-chan struct{}) shutdownStep {
	return shutdownStep{
		name:    shutdownStepReconciler,
		timeout: 10 * time.Second,
		barrier: true,
		run: func(stepCtx context.Context) error {
			return barrierJoin(stepCtx, func(joinCtx context.Context) error {
				return waitSignal(joinCtx, reconcilerDone)
			})
		},
	}
}

// loopsBarrierStep joins the worker-owned background loops (their done channels
// close on shutdown). The 5s stats loop writes the state DB, so this is a
// quiescence barrier ahead of the state close.
func loopsBarrierStep(loopDones []<-chan struct{}) shutdownStep {
	return shutdownStep{
		name:    shutdownStepLoops,
		timeout: 5 * time.Second,
		barrier: true,
		run: func(stepCtx context.Context) error {
			return barrierJoin(stepCtx, func(joinCtx context.Context) error {
				for _, done := range loopDones {
					if err := waitSignal(joinCtx, done); err != nil {
						return err
					}
				}
				return nil
			})
		},
	}
}

// servicesJoinBarrierStep joins the service coordinator (join is
// ServiceCoordinator.Join; it drains queued/active passes and joins its
// workers). Its workers use the runtime manager and the state DB, so this is a
// quiescence barrier ahead of manager/state teardown.
func servicesJoinBarrierStep(join func(context.Context) error) shutdownStep {
	return shutdownStep{
		name:    shutdownStepServicesJoin,
		timeout: shutdownServiceTimeout,
		barrier: true,
		run: func(stepCtx context.Context) error {
			return barrierJoin(stepCtx, join)
		},
	}
}

// housekeepingBarrierStep joins the background startup housekeeping pass (done
// closes when its exclusive sweeps finish). The sweeps use the runtime manager
// and the state DB, so this is a quiescence barrier ahead of manager/state
// teardown.
func housekeepingBarrierStep(done <-chan struct{}) shutdownStep {
	return shutdownStep{
		name:    shutdownStepHousekeeping,
		timeout: shutdownServiceTimeout,
		barrier: true,
		run: func(stepCtx context.Context) error {
			return barrierJoin(stepCtx, func(joinCtx context.Context) error {
				return waitSignal(joinCtx, done)
			})
		},
	}
}

// schedulerBarrierStep is the scheduler shutdown barrier: it stops the gocron
// scheduler and then strictly joins every in-flight Relay publisher callback.
// A callback calls Publisher.PublishOccurrence, which touches Redis, and
// gocron's own bounded Shutdown can return (ErrStopJobsTimedOut) while a task
// goroutine — and therefore that callback — is still running. Redis is
// released in a later step, so this must be a quiescence barrier: the strict
// join (barrierJoin's unbounded second call) waits for Stop's real completion
// even past the step bound, guaranteeing no callback can touch Redis after
// this step returns. Stop itself is idempotent and joins the same shutdown on
// re-invocation, so the second call converges rather than restarting anything.
func schedulerBarrierStep(stop func(context.Context) error) shutdownStep {
	return shutdownStep{
		name:    shutdownStepScheduler,
		timeout: shutdownStepTimeout,
		barrier: true,
		run: func(stepCtx context.Context) error {
			return barrierJoin(stepCtx, stop)
		},
	}
}

// register adds a teardown step. Registering a name that is not in
// shutdownStepOrder would silently never run, so callers must use the
// shutdownStep* constants.
func (r *shutdownRegistry) register(step shutdownStep) {
	r.steps = append(r.steps, step)
}

// run executes every registered step in shutdownStepOrder, then logs the
// completion marker. Each step gets a fresh context bounded by min(its own
// timeout, the remaining aggregate budget), so one slow step cannot consume
// another's budget. Every step is invoked in its own goroutine and the registry
// selects on its result vs. the step deadline, so a step whose real operation
// ignores context cannot hang the shutdown: the timeout is observed, logged at
// Warn as "Shutdown: step timed out", and the remaining steps still run. A step
// failure is logged as "Shutdown: step failed" and likewise never stops the
// sequence. The result channel is buffered so a non-cooperative step that later
// returns can always send without leaking on a blocked send. A step that PANICS
// is recovered at this boundary and converted into a step failure (with the step
// name and duration), so a broken cleanup cannot crash the process and skip the
// remaining steps: ordered continuation is a core shutdown contract.
//
// The aggregate budget therefore bounds the BEST-EFFORT cleanup steps, not the
// whole teardown: every step is ATTEMPTED, the budget is never a reason to skip
// one, and a barrier (below) may still extend the shutdown past it.
//
// A barrier step (shutdownStep.barrier) is the exception to the
// move-on-after-timeout policy: when its deadline fires the registry cancels
// the step's context and then WAITS for the step's goroutine to actually return
// before advancing, so a later step can never tear down a shared dependency
// (manager, state DB, Redis) while a reconcile, sweep, loop, or publisher
// callback is still using it. The barrier's timeout is logged exactly as a
// cleanup timeout — with `kind=barrier` and the exhausted `bound` — so the
// diagnostics stay actionable and there is no duplicate log, then the strict
// join may extend the shutdown past that bound, deliberately trading teardown
// latency for safety.
//
// Every step logs exactly one timeout line, carrying `kind` (barrier vs
// cleanup) and `bound` (whether the step's own cap or the aggregate budget was
// what expired); a genuine error after a barrier timeout is the one additional
// line, and only when the joined operation does not merely report its context
// error.
func (r *shutdownRegistry) run(logger *slog.Logger) {
	steps := make(map[string]shutdownStep, len(r.steps))
	for _, step := range r.steps {
		steps[step.name] = step
	}
	started := time.Now()
	budgetDuration := shutdownAggregateTimeout
	if r.budget > 0 {
		budgetDuration = r.budget
	}
	budget := time.Now().Add(budgetDuration)
	for _, name := range shutdownStepOrder {
		step, ok := steps[name]
		if !ok {
			continue
		}
		// Every registered step is ATTEMPTED, even once the aggregate budget is
		// exhausted: the step is still invoked (with an already-expired context
		// if the budget is gone) so a cooperative cleanup gets its chance, and
		// the registry logs it as timed out rather than silently skipping it.
		// A barrier is never skipped by exhaustion either; it is still joined.
		deadline := budget
		bound := "aggregate"
		if step.timeout > 0 {
			if own := time.Now().Add(step.timeout); own.Before(deadline) {
				deadline = own
				bound = "step"
			}
		}
		stepCtx, cancel := context.WithDeadline(context.Background(), deadline)
		stepStart := time.Now()
		done := make(chan error, 1)
		go func() {
			// Recover at the step boundary: a panicking cleanup is converted to
			// an error (logged as a step failure with step/duration) so it can
			// never crash the process or abort the ordered sequence. The send is
			// safe on the buffered channel even if the registry already moved on
			// after a timeout.
			defer func() {
				if pv := recover(); pv != nil {
					done <- fmt.Errorf("panic: %v", pv)
				}
			}()
			done <- step.run(stepCtx)
		}()
		select {
		case err := <-done:
			// A cooperative step that returns exactly when its context fires is
			// reported as the timeout it is; a step that returns a context error
			// is likewise a timeout (its bound was reached), not a genuine
			// failure. Any other error is a real step failure.
			ctxErr := stepCtx.Err()
			cancel()
			switch {
			case err == nil:
			case ctxErr != nil && errors.Is(err, ctxErr):
				logger.Warn("Shutdown: step timed out",
					"step", step.name,
					"kind", step.kind(),
					"bound", bound,
					"error", err,
					"duration", time.Since(stepStart),
				)
			default:
				logger.Warn("Shutdown: step failed",
					"step", step.name,
					"kind", step.kind(),
					"error", err,
					"duration", time.Since(stepStart),
				)
			}
		case <-stepCtx.Done():
			// Cancel first so a cooperative barrier observes the cancellation
			// promptly, then (barriers only) join the step for real.
			cancel()
			logger.Warn("Shutdown: step timed out",
				"step", step.name,
				"kind", step.kind(),
				"bound", bound,
				"error", stepCtx.Err(),
				"duration", time.Since(stepStart),
			)
			if step.barrier {
				// Strict join: do NOT advance to a later step (manager/state/
				// Redis teardown) until the operation this barrier joins has
				// actually returned. This is where shutdown may exceed both the
				// step bound and the aggregate budget, by design.
				if err := <-done; err != nil && !errors.Is(err, context.Canceled) &&
					!errors.Is(err, context.DeadlineExceeded) {
					logger.Warn("Shutdown: barrier step failed after timeout",
						"step", step.name,
						"error", err,
						"duration", time.Since(stepStart),
					)
				}
			}
		}
	}
	logger.Info("Shutdown complete", "duration", time.Since(started))
}

// shutdownServices builds the shutdown step that stops and removes THIS worker's
// persistent service containers during graceful shutdown. It is the
// snapshot-first, proof-second, remove-third sequence (see
// guardIdentitySnapshotCleanup): the step SNAPSHOTS the worker's service
// containers, re-proves the pinned ownership lock, and stops/removes exactly the
// captured containers — never re-listing after the proof. Because a replacement
// incarnation with the SAME WORKER_ID can retain/adopt the SAME physical id (the
// reconciler is label-derived and identity-scoped, not process-scoped), the
// protection is the fail-closed
// ownership proof, not id freshness: a latched loss or a failed/ambiguous proof
// removes nothing. The step's context (the registry's fresh bounded one) is
// handed to discovery and removal; the ownership proof uses its own background
// context (proof.prove), so it still detects a loss once the lifecycle is
// cancelled. A discovery error is returned so the shutdown registry surfaces it
// with the structured step name. The detailed per-operation logs live in the
// reconciler, so no duplicate logging happens here.
func shutdownServices(
	svcCtrl *reconciler.ServiceReconciler,
	hostname string,
	proof ownershipProof,
	logger *slog.Logger,
) func(context.Context) error {
	return guardIdentitySnapshotCleanup(proof, logger, shutdownStepServiceCleanup,
		func(stepCtx context.Context) ([]runtime.ServiceContainer, error) {
			return svcCtrl.DiscoverShutdownContainers(stepCtx, hostname)
		},
		func(stepCtx context.Context, own []runtime.ServiceContainer) error {
			return svcCtrl.RemoveShutdownContainers(stepCtx, own)
		},
	)
}

// schedulerStorage is the narrow scheduler view the worker's storage wiring
// needs. *cron.Scheduler satisfies it. It is defined here (not in cron) so the
// worker-owned wiring decision — install the shared handle when available, or
// mark unavailable and start a bootstrap otherwise — is unit-testable with a
// deterministic fake and without a real state DB or gocron.
type schedulerStorage interface {
	SetOutbox(o cron.Outbox)
	StartPendingRetry(ctx context.Context)
	MarkStorageUnavailable()
	StartStorageBootstrap(ctx context.Context, open cron.OutboxOpener)
}

var _ schedulerStorage = (*cron.Scheduler)(nil)

// *runtime.Manager must always implement the reconciler's optional
// shutdown-scoped Docker capability. The reconciler type-asserts the
// configured Docker for it and otherwise silently falls back to the ordinary
// (lifecycle-refused) methods, so this compile-time assertion keeps a
// production regression from degrading graceful service cleanup without
// failing the build. It is always compiled, unlike the capability tests.
var _ reconciler.ShutdownDocker = (*runtime.Manager)(nil)

// wireSchedulerStorage connects the scheduler to its durable outbox. A non-nil
// shared state handle is installed and its durable retry worker started, which
// enables schedule publication. Current Run ALWAYS takes this path: state.Open
// is fatal, so the shared handle is non-nil by the time this runs.
//
// A nil shared handle is therefore a DEFENSIVE fallback, unreachable via the
// current Run, retained as a seam for an embedding host (or a future where the
// shared open could be non-fatal): the scheduler is marked unavailable (so it
// never fires a schedule without a durable store) and a scheduler-owned,
// cancellable bootstrap retries opening the store; on success the bootstrap
// installs it, recovers existing rows, runs the bounded latest-only catch-up,
// and enables publication. It never fails and never gates the event consumer or
// services.
func wireSchedulerStorage(
	ctx context.Context,
	sched schedulerStorage,
	shared *state.State,
	open cron.OutboxOpener,
) {
	if shared != nil {
		sched.SetOutbox(shared)
		sched.StartPendingRetry(ctx)
		return
	}
	sched.MarkStorageUnavailable()
	// Record the lifecycle first so a later bootstrap recovery can start the
	// durable retry worker without re-supplying the context; no worker is started
	// while no outbox is installed.
	sched.StartPendingRetry(ctx)
	sched.StartStorageBootstrap(ctx, open)
}

// schedulerStorageOpener returns the cron.OutboxOpener the worker's DEFENSIVE
// storage-unavailable bootstrap uses to retry opening the scheduler's durable
// outbox (the PostgreSQL state database) when a nil shared handle was supplied.
// Current Run never supplies one because its state.Open is fatal; the opener
// exists so the nil-shared fallback stays a complete, testable seam for an
// embedding host or a future non-fatal open. dsn is cfg.DatabaseURL; it is a
// parameter so the retry path is unit-testable without a real database. The
// opened *state.State is a scheduler-OWNED handle: cron closes it after all
// scheduler work has stopped, so the shared global handle is never touched. It
// deliberately opens an independent handle on the same DSN — the smallest
// clean equivalent — and wires the same status observer so a recovered
// store's status writes still project onto the app_status gauge. It opens with
// NO stats owner: the handle exists only for the schedule outbox and never
// writes stats rows, and an owner without a paired instance is rejected by
// state.Open.
func schedulerStorageOpener(dsn string, logger *slog.Logger, metricsInstance *metrics.Registry) cron.OutboxOpener {
	return func() (cron.Outbox, io.Closer, error) {
		st, err := state.Open(context.Background(), state.Options{
			DSN:             dsn,
			Logger:          logger,
			MaxOpenConns:    config.DefaultDatabaseMaxOpenConns,
			MaxIdleConns:    config.DefaultDatabaseMaxIdleConns,
			ConnMaxLifetime: config.DefaultDatabaseConnMaxLifetime,
			ConnMaxIdleTime: config.DefaultDatabaseConnMaxIdleTime,
			ConnectTimeout:  config.DefaultDatabaseConnectTimeout,
		})
		if err != nil {
			return nil, nil, err
		}
		wireStatusObserver(st, metricsInstance)
		return st, st, nil
	}
}

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

// restorePersistedStats seeds the fresh process-lifetime metrics registry with
// THIS worker's cumulative counters persisted in the state database, so the
// first snapshot never resets them. The registry only knows this process's
// lifetime, while the PostgreSQL stats rows are cumulative history; seeding
// bridges them. Only the worker's OWN rows (keyed by its WorkerID identity)
// are read — never another worker's, so two workers sharing one database never
// double-count. It is nil-safe on both the registry and the state handle. Gauges
// are deliberately NOT restored: they are point-in-time backlog snapshots
// refreshed each snapshot.
func restorePersistedStats(metricsInstance *metrics.Registry, st *state.State) {
	if metricsInstance == nil || st == nil {
		return
	}
	if gs, ok := st.OwnerStats(); ok {
		metricsInstance.SeedCounter(metrics.MetricEventsReceived, gs.EventsReceivedTotal)
		metricsInstance.SeedCounter(metrics.MetricEventsMatched, gs.EventsMatchedTotal)
		metricsInstance.SeedCounter(metrics.MetricEventsUnmatched, gs.EventsUnmatchedTotal)
		metricsInstance.SeedCounter(metrics.MetricHandlerSuccess, gs.HandlerSuccessTotal)
		metricsInstance.SeedCounter(metrics.MetricHandlerFailure, gs.HandlerFailureTotal)
		metricsInstance.SeedCounter(metrics.MetricRetries, gs.RetryTotal)
		metricsInstance.SeedCounter(metrics.MetricDLQEntries, gs.DLQTotal)
	}
	for _, fs := range st.OwnerAppStats() {
		metricsInstance.SeedAppStat(metrics.AppStat{
			App:                 fs.App,
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
			// SeedAppStat skips as "never observed").
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
// They are the global counterpart of the per-app counters read by
// AppStatsSnapshot, and the set the relay baseline captures/subtracts.
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
// maps an app to its raw per-app snapshot at the reset (counters are
// subtracted, and a timestamp not advanced since the reset is suppressed to
// "never observed" — timestamps are last-observed, not cumulative, so
// subtraction alone cannot represent their reset). It is guarded by the owning
// statsFlusher.mu and takes no lock of its own; the zero value is a valid
// "no reset yet" baseline and a nil *relayBaseline is treated the same.
type relayBaseline struct {
	globals map[string]int64
	funcs   map[string]metrics.AppStat
}

// counter returns v (the raw counter value) minus the reset baseline for name.
// A nil or zero baseline returns v unchanged.
func (b *relayBaseline) counter(name string, v int64) int64 {
	if b == nil {
		return v
	}
	return v - b.globals[name]
}

// applyAppStat returns fs with every cumulative per-app counter
// reduced by the reset baseline and any timestamp not advanced since the reset
// suppressed to zero ("never observed"). A nil baseline, or an app absent
// from the baseline, leaves fs unchanged. It is read-only: the registry is
// never mutated.
func (b *relayBaseline) applyAppStat(fs metrics.AppStat) metrics.AppStat {
	if b == nil {
		return fs
	}
	base, ok := b.funcs[fs.App]
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
// per-app snapshot and installs them as the reset baseline. A nil registry
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
	b.funcs = make(map[string]metrics.AppStat)
	for _, fs := range metricsInstance.AppStatsSnapshot() {
		b.funcs[fs.App] = fs
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

// snapshotAppStats maps the registry's per-app counters AND latest
// execution-history timestamps into the state layer's AppStats rows. Like
// snapshotStats it applies the worker-owned relay baseline (base), so
// per-app totals continue from zero after an operator reset and pre-reset
// timestamps are reported as "never observed", while the Prometheus series stay
// monotonic. The registry stores unix seconds; the state layer stores RFC3339
// strings in the updated_at convention (empty = never observed), so zero
// timestamps map to "". It is nil-safe: a nil registry yields an empty slice so
// the snapshot path can never panic or block processing.
func snapshotAppStats(metricsInstance *metrics.Registry, base *relayBaseline) []state.AppStats {
	if metricsInstance == nil {
		return nil
	}
	stats := metricsInstance.AppStatsSnapshot()
	out := make([]state.AppStats, 0, len(stats))
	for _, fs := range stats {
		fs = base.applyAppStat(fs)
		out = append(out, state.AppStats{
			App:                 fs.App,
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
// PostgreSQL and the matching in-memory reset. It is the serialization point that
// makes a socket-triggered `reset stats` deterministic against the periodic
// flush: mu is held across BOTH capturing the snapshot and writing it, and
// ResetStats takes the same mutex, so a flush can never interleave a captured
// pre-reset snapshot with the reset (pre-reset values cannot be resurrected).
// It is safe for concurrent use.
type statsFlusher struct {
	mu sync.Mutex
	st *state.State
	// metrics is the accounting registry the worker always constructs; it is
	// nil only when a caller deliberately passes nil (tests), in which case the
	// snapshot mappers yield zero values. Production always supplies the
	// registry, so a flush never clobbers the persisted cumulative totals.
	metrics *metrics.Registry
	// baseline is the worker-owned reset point captured by ResetStats. It is
	// read and replaced under mu, so a concurrent flush always sees a coherent
	// baseline. Zero value = "no reset yet".
	baseline relayBaseline

	// generation is this worker's last-known global reset epoch (see
	// state.StatsGeneration). It starts at statsGenerationUnknown so the first
	// flush adopts whatever epoch the database holds instead of a spurious
	// rebase, and tracks the epoch every flush returns thereafter.
	generation int64

	// ownershipLost records that another process incarnation of the same
	// WorkerID has claimed this worker's stats row, so this (stale) process must
	// stop writing. It is terminal for the process: the instance id is fixed at
	// startup and a stale incarnation can never reclaim the row, so the warning
	// is logged once and no further writes are attempted.
	ownershipLost bool

	// retention bounds stale-owner row growth (see state.PruneStaleStats); <=0
	// disables compaction. lastPrune throttles pruning to one attempt per
	// retentionTickInterval(retention) after a successful flush.
	retention time.Duration
	lastPrune time.Time

	// log records the one-time ownership-loss warning; it defaults to
	// slog.Default() and the worker installs the process logger.
	log *slog.Logger
}

// statsGenerationUnknown is the sentinel for "this process has not yet observed
// the global reset epoch". A negative generation tells RecordStatsSnapshot to
// adopt the stored epoch rather than treat the first flush as a reset race.
const statsGenerationUnknown = -1

// newStatsFlusher builds a flusher over the state handle and registry. retention
// and the process logger are installed by the worker after construction; tests
// that only exercise the flush path leave the defaults (no compaction, default
// logger).
func newStatsFlusher(st *state.State, metricsInstance *metrics.Registry) *statsFlusher {
	return &statsFlusher{st: st, metrics: metricsInstance, generation: statsGenerationUnknown, log: slog.Default()}
}

// flush captures the current Relay-visible snapshot and writes it in one short
// transaction, then opportunistically compacts stale owner rows. Held under mu
// so a concurrent ResetStats cannot land between capture and write. Nil-safe on
// the flusher.
func (flusher *statsFlusher) flush(ctx context.Context) {
	if flusher == nil {
		return
	}
	flusher.mu.Lock()
	defer flusher.mu.Unlock()
	flusher.flushLocked(ctx)
}

// flushLocked does the flush work with mu already held. The write is fenced two
// ways (see state.RecordStatsSnapshot): the epoch must match the flusher's
// known generation, and this process incarnation must still own the stats row.
// A reset that advanced the epoch is handled by rebasing to the registry's
// current values and retrying ONCE, so the reset's zero is not immediately
// re-counted; a lost ownership stops all writes.
func (flusher *statsFlusher) flushLocked(ctx context.Context) {
	// Sweep the registry against the live app set before snapshotting, so an app
	// removed (or swept) this interval cannot persist a stale app_stats row that
	// the orphan-prune would have to reject anyway, and cannot linger on
	// /metrics.
	//
	// Fail-open: if the live set cannot be read (st.AppNames returns ok=false),
	// the sweep is skipped entirely rather than run against an empty map — an
	// empty live set would delete every app-scoped series, including live apps'.
	// Sweeping is best-effort and re-applied on the next successful flush.
	if names, ok := flusher.st.AppNames(); ok {
		active := make(map[string]bool, len(names))
		for _, name := range names {
			active[name] = true
		}
		flusher.metrics.SweepAppMetrics(active)
	}

	// A stale incarnation must not write at all: writing would either fail the
	// ownership predicate or race a reclaim, and any per-app write would be
	// rejected after the global one anyway.
	if flusher.ownershipLost {
		return
	}

	// At most two attempts: the first under the known epoch; if a reset (possibly
	// from another worker) advanced the epoch in the meantime, rebase the
	// in-memory baseline to the registry's current values and retry once under
	// the new epoch. A second advance is left for the next tick rather than
	// looped on.
	for attempt := 0; attempt < 2; attempt++ {
		gen, err := flusher.st.RecordStatsSnapshot(
			ctx,
			flusher.generation,
			snapshotStats(flusher.metrics, &flusher.baseline),
			snapshotAppStats(flusher.metrics, &flusher.baseline),
		)
		switch {
		case err == nil:
			flusher.generation = gen
			flusher.prune(ctx)
			return
		case errors.Is(err, state.ErrStatsResetAdvanced):
			// The persisted rows were just zeroed by a reset. Rebase so the
			// snapshot restarts from the reset point instead of re-adding the
			// pre-reset totals, and adopt the epoch it happened under.
			flusher.generation = gen
			flusher.baseline = captureRelayBaseline(flusher.metrics)
		case errors.Is(err, state.ErrStatsOwnershipLost):
			if !flusher.ownershipLost {
				flusher.ownershipLost = true
				flusher.log.Warn(
					"Stats: ownership lost; another process claimed this worker's stats row; stopping stats writes",
					"worker", flusher.st.Owner(),
				)
			}
			return
		default:
			// An unexpected failure is already logged by the state package; the
			// next tick retries with the current absolute values.
			return
		}
	}
}

// prune runs best-effort compaction of stale owner rows at most once per
// retentionTickInterval, after a successful flush. retention <=0 disables it.
// The attempted instant is recorded before the call so a persistent prune
// failure does not retry on every tick; a failure is logged and deferred.
func (flusher *statsFlusher) prune(ctx context.Context) {
	if flusher.retention <= 0 {
		return
	}
	now := time.Now()
	if !flusher.lastPrune.IsZero() && now.Sub(flusher.lastPrune) < retentionTickInterval(flusher.retention) {
		return
	}
	flusher.lastPrune = now
	n, err := flusher.st.PruneStaleStats(ctx, flusher.retention)
	if err != nil {
		flusher.log.Warn("Stats: prune stale owners failed; will retry next interval", "error", err)
		return
	}
	if n > 0 {
		flusher.log.Info("Stats: pruned stale owner rows", "removed", n, "retention", flusher.retention)
	}
}

// dropAppBaseline drops an app's entry from the worker-owned reset
// baseline. It is called from the reconciler's RemoveApp hook alongside the
// registry's series deletion, so an app removed and later re-added starts
// from its fresh zero-valued series instead of subtracting a stale pre-removal
// total (which would persist a negative value). It is idempotent, nil-safe, and
// safe to call before any reset (a zero baseline has no entry to drop).
func (flusher *statsFlusher) dropAppBaseline(name string) {
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
// flush subtracts the baseline when it snapshots. The state reset bumps the
// global generation, which the flusher adopts so its next flush does not mistake
// its OWN reset for a concurrent one and rebase a second time. A missing state
// handle (state open failed at startup) reports an error so the socket answers
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
	gen, err := flusher.st.ResetStats()
	if err != nil {
		return err
	}
	flusher.generation = gen
	return nil
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

// finalStatsFlush performs a final flush of the registry into PostgreSQL on
// graceful shutdown, so the last interval of telemetry is not lost. ctx is the
// shutdown step's fresh, bounded context (the registry derives it from
// context.Background), so a wedged PostgreSQL cannot hang shutdown; on timeout or
// error the flush logs and returns (telemetry, not state). It is nil-safe on
// the flusher. The metrics guard is defensive only for direct test callers that
// deliberately build a flusher over a nil registry: in production the registry
// is ALWAYS constructed (see setupMetrics), independent of METRICS_ADDR, so the
// final flush runs whether or not HTTP exposition is enabled, and a nil
// registry (which would write zero Stats over the persisted totals) never
// occurs.
func finalStatsFlush(ctx context.Context, flusher *statsFlusher) {
	if flusher == nil || flusher.st == nil || flusher.metrics == nil {
		return
	}
	flusher.flush(ctx)
}

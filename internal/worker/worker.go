// Package worker is the long-running Relay runtime, started via `relay start`.
// It loads functions, builds their images, reconciles them live, and consumes
// the Redis stream, blocking until signalled. Configuration comes entirely
// from the environment.
//
// Startup begins with an explicit external-dependency preflight (see
// runExternalPreflight) that runs BEFORE any function is loaded or fingerprinted,
// before the state database is opened, and before the runtime socket, services,
// sweeps, preparation, listener starts, background loops, scheduler, and
// reconciler. The fixed order is: Redis stream/consumer-group readiness, then
// Docker runtime-manager readiness, then verification of the configured NETWORKS
// set, then the runtime manager's own background maintenance loop is started. A
// failure at any step short-circuits every later phase; a lifecycle cancellation
// during the preflight is a graceful shutdown.
//
// Its in-memory accounting registry is always created and
// is the single source of truth for the operational counters; the Prometheus
// /metrics HTTP endpoint (see internal/observability/metrics) is an optional
// exposition of that same registry, gated on the METRICS_ADDR environment
// variable. The worker flushes the registry into the local state database on a
// fixed 5-second cadence independent of METRICS_ADDR: stats accumulate in
// memory, Prometheus (when exposed) reflects them immediately, and SQLite
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
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/codes"

	"relay/internal/config"
	"relay/internal/cron"
	"relay/internal/function"
	gitwh "relay/internal/git/webhook"
	"relay/internal/observability/metrics"
	"relay/internal/observability/tracing"
	"relay/internal/reconciler"
	"relay/internal/routing"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/schedule"
	"relay/internal/secrets"
	"relay/internal/source"
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
// — injected into the ServiceReconciler — each normal pre-resolution and
// post-resolution Docker operation inside a per-function Apply. Each such
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
// -> configured NETWORKS verification -> manager maintenance start, BEFORE any
// function is loaded or fingerprinted, before state.Open, and before the runtime
// socket, services, sweeps, preparation, listener starts, background loops,
// scheduler, and reconciler are touched. A failure at any step short-circuits
// every later step and is returned so Run unwinds through its deferred shutdown;
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
// consumer. It blocks in Consume until the process is signalled, then runs the
// graceful shutdown. It returns an error and never calls os.Exit: the CLI
// boundary owns process exit, and every post-resource startup or runtime failure
// converges through the SAME deferred cleanup as a normal shutdown, rather than
// abandoning live servers, goroutines, containers, and the state DB to process
// exit.
func Run(logger *slog.Logger) error {
	cfg, err := config.Load(logger)
	if err != nil {
		// Pre-resource failure: nothing owns cleanup yet, so return the error for
		// the CLI to print and exit on.
		return fmt.Errorf("invalid configuration: %w", err)
	}
	redisOpts, err := config.RedisOptions(cfg.RedisURI)
	if err != nil {
		// Pre-resource failure: nothing owns cleanup yet, so return the error for
		// the CLI to print and exit on.
		return fmt.Errorf("redis config invalid: %w", err)
	}
	client := redis.NewClient(redisOpts)

	// The worker's lifecycle context: cancelled on SIGINT/SIGTERM (or by the
	// deferred shutdown when Run returns). It is created here,
	// before the runtime Manager, so the manager can root Dockerfile builds in
	// it (see runtime.WithLifecycleContext): a long build is bounded by the
	// runtime's 10m buildTimeout but is still cancelled when Relay shuts down.
	// Every bounded startup daemon operation and the reconciler hooks are
	// likewise rooted here (rather than context.Background), so shutdown cancels
	// them too. It replaces the signal context that used to be created later.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	// Tracing is initialized before the root startup span and rooted in the
	// worker lifecycle. It is disabled by default (no endpoint => no exporter, no
	// collector connection); Setup always returns a usable provider, and a setup
	// error is non-fatal observability (the worker runs untraced).
	tracerProvider, tracerErr := tracing.Setup(ctx, logger)
	if tracerErr != nil {
		logger.Warn("Tracing: setup failed; continuing without tracing", "error", tracerErr)
	}

	// The startup trace root. Its children cover each startup phase; it ends at
	// the ready-to-consume boundary immediately before consumer.Consume. The
	// deferred End below is a no-op after the explicit one and guarantees the
	// span closes on every early-return path (before the provider shutdown step
	// flushes it).
	startupCtx, startupSpan := tracing.Start(ctx, "relay.startup")

	// The accounting registry is ALWAYS created; only the /metrics HTTP
	// exposition (and its periodic snapshot logger) is opt-in, gated on
	// METRICS_ADDR. Setup only CREATES them here (server nil when disabled);
	// STARTING the server happens later, once the startup wiring is complete.
	// Because the registry exists even with exposition disabled, the persistent
	// SQLite stats flush is independent of the HTTP listener.
	metricsInstance, metricsServer := setupMetrics(cfg, logger)

	// The graceful shutdown registry converges here on every return path —
	// normal shutdown, a startup failure after resources exist, or a Consume
	// error. Steps are registered as each resource is successfully
	// acquired/started (below), but run in the explicit shutdownStepOrder, so a
	// resource acquired early (Redis) is released last and ordering never
	// depends on registration/LIFO. The defer is registered BEFORE the first
	// fallible resource-owning step, so even a secrets-provider failure releases
	// the Redis client. Best-effort cleanup steps are bounded by min(their own
	// cap, the remaining aggregate budget); their failure or timeout is logged
	// with the step name and never stops the sequence. Quiescence barriers
	// (scheduler, reconciler, housekeeping, services-join, loops) are logged on
	// timeout and then strictly joined before any later step runs, so the
	// aggregate budget bounds cleanup work, not process exit.
	shutdown := &shutdownRegistry{}
	// The worker's readiness state. Its flag starts false and is set true only at
	// the ready-to-consume boundary (just before Consume, once every listener and
	// loop is wired) and cleared as the FIRST instruction of the shutdown defer.
	// It is bound to the worker lifecycle context so a lifecycle cancellation
	// that precedes the deferred clear (a signal during the final startup wiring,
	// or just before Consume returns) already reports not-ready.
	// It backs the `relay health` socket query; see internal/worker/readiness.go.
	workerReady := newReadiness(ctx)
	// Redis is acquired first and released last; a wedged client close must not
	// hang teardown.
	shutdown.register(shutdownStep{
		name:    shutdownStepRedis,
		timeout: 5 * time.Second,
		run:     func(context.Context) error { return client.Close() },
	})
	// Tracing is released LAST, after every other resource has stopped producing
	// spans, under a bounded provider shutdown that flushes the batch processor.
	// A disabled provider's Shutdown is a no-op.
	shutdown.register(shutdownStep{
		name:    shutdownStepTracing,
		timeout: 5 * time.Second,
		run:     tracerProvider.Shutdown,
	})

	// Cancel the lifecycle FIRST: a startup failure returns without a signal
	// having arrived, so background loops, the coordinator, and the manager
	// builds observe cancellation before teardown joins them. The root span is
	// ended BEFORE the registry (and its tracing shutdown step) runs, so an
	// early-return startup failure still flushes its trace.
	//
	// Readiness is cleared as the VERY FIRST instruction, before the lifecycle
	// is cancelled: a `relay health` that races shutdown must observe not-ready
	// rather than a stale true. Everything after that (lifecycle cancellation,
	// span end, ordered graceful teardown with strict dependency barriers) is
	// the normal shutdown sequence.
	defer func() {
		workerReady.setNotReady()
		stop()
		startupSpan.End()
		shutdown.run(logger)
	}()

	// External-dependency preflight, in one explicit fixed order: Redis
	// stream/consumer-group readiness -> Docker runtime-manager readiness ->
	// configured NETWORKS verification. It runs BEFORE any function is loaded or
	// fingerprinted, before state.Open, and before the runtime socket, services,
	// sweeps, preparation, listener starts, background loops, scheduler, and
	// reconciler. A failure at any step short-circuits every later step: Redis
	// or the Docker daemon being unavailable is an operator condition that must
	// surface before the worker touches anything else, and a lifecycle
	// cancellation during the preflight is still a graceful shutdown (the
	// deferred cleanup above released the Redis client and tracing, and the
	// manager is closed by its registered shutdown step when it was opened).
	// The manager's own teardown step is registered by OpenManager only on a
	// successful open, so an early failure never registers a step for a manager
	// that does not exist.
	manager, err := runExternalPreflight(startupCtx, logger, preflightDeps{
		EnsureGroup: func(ctx context.Context) error {
			return stream.EnsureGroup(ctx, client, cfg.RedisStream, cfg.RedisGroup)
		},
		// The closure deliberately ignores the preflight-step context and roots
		// the manager in Run's worker lifecycle `ctx` (the signal context), the
		// same lifecycle the manager owned before this refactor: builds and the
		// startup ping must be cancelled by SIGTERM/SIGINT, and the startup-span
		// context is used only for the phase span in runExternalPreflight.
		OpenManager: func(_ context.Context) (*runtime.Manager, error) {
			m, err := runtime.NewManager(
				logger,
				metricsInstance,
				cfg.ConsumerName,
				runtime.WithWarmContainerIdleTimeout(cfg.WarmContainerIdleTimeout),
				// The SAME MAX_CONCURRENCY the runner's global semaphore uses:
				// the runtime clips each function's effective per-function
				// concurrency to it, so a template asking for more than the
				// worker-global cap (e.g. 15 with MAX_CONCURRENCY=8) warms,
				// reports, and admits only the cap's worth. It is startup
				// configuration; a global change requires a worker restart.
				runtime.WithMaxConcurrency(cfg.MaxConcurrency),
				// The worker-global Docker networks (NETWORKS) every execution
				// container joins at create time. They are verified by the next
				// preflight step before any function is prepared or any container
				// created.
				runtime.WithNetworks(cfg.Networks),
				// Root Dockerfile builds in the WORKER LIFECYCLE (ctx), not the
				// startup-span context: they get an independent 10m bound
				// (runtime.buildTimeout) but are still cancelled when Relay shuts
				// down. Builds must NOT inherit the short 30s reconcile budget
				// the worker uses for normal service operations.
				runtime.WithLifecycleContext(ctx),
				// Do NOT start the warm-container maintenance loop in the
				// constructor: the manager is opened before NETWORKS is
				// verified, and the request is that all configured networks
				// validate before ANY background loop starts. The loop is
				// started by the final preflight step (StartMaintenance) only
				// after verification succeeds; if it fails, the manager (and its
				// Docker client) is still torn down by the registered shutdown
				// step with no loop ever started.
				runtime.WithDeferredMaintenance(),
			)
			if err != nil {
				return nil, err
			}
			// Manager cleanup is context-aware, so the Docker client is only
			// closed after every worker-owned goroutine has stopped and every
			// warm container has been torn down (in parallel, bounded) or the
			// step's finite bound expires. Registered BEFORE the preflight
			// returns so a later preflight-step failure (NETWORKS verification)
			// still converges through the deferred shutdown.
			shutdown.register(shutdownStep{
				name:    shutdownStepManager,
				timeout: 30 * time.Second,
				run:     m.CloseContext,
			})
			return m, nil
		},
		VerifyNetworks: func(ctx context.Context, m *runtime.Manager) error {
			// Verify every configured NETWORKS network exists BEFORE any
			// function is prepared or any container created. The networks are
			// infrastructure owned OUTSIDE Relay — Relay never creates them —
			// so a missing one is an operator condition that must fail startup
			// rather than silently produce containers on the wrong (or no)
			// network. A verify error (a broken daemon) is likewise fatal. The
			// check is skipped entirely when NETWORKS is unset.
			return verifyConfiguredNetworks(ctx, m, cfg.Networks)
		},
		// The manager was opened with deferred maintenance, so this is the
		// single place the warm-container eviction loop starts — only after
		// NETWORKS verification succeeded. For a manager that did not defer,
		// the call is a no-op (the loop is already running).
		StartMaintenance: func(m *runtime.Manager) {
			m.StartMaintenance()
		},
	})
	if err != nil {
		// startupResult converts a lifecycle-cancelled preflight into a
		// graceful nil return (the process is already shutting down) and leaves
		// a genuine prerequisite failure for the CLI to print and exit on.
		return startupResult(err)
	}
	logger.Info("Startup: external dependencies ready",
		"redis_stream", cfg.RedisStream,
		"redis_group", cfg.RedisGroup,
		"networks", len(cfg.Networks),
	)

	// A single shared secrets provider, used by both the webhook (below) and the
	// runner (later). Construction is infallible (the dir is created lazily on
	// Set, never Resolve); a missing secret surfaces as a per-invocation Resolve
	// error, and startup does not validate that referenced secrets exist. A
	// construction failure here is fatal — the provider is core to invocation
	// and the webhook.
	secretProvider, err := secrets.NewLocalProvider(secrets.SecretsDir)
	if err != nil {
		return fmt.Errorf("secrets: new local provider failed: %w", err)
	}

	// The Git webhook server (internal/git/webhook) is opt-in, gated on
	// GIT_WEBHOOK_ADDR. NewServer owns all assembly (git source config, secret
	// reference checks, the coalescing sync scheduler, provider handlers) and
	// returns nil when disabled. The worker only orchestrates: construct, start
	// (bind failure is fatal, matching metrics), and stop on shutdown. Its
	// teardown step is registered only once it has actually started.
	var gitWebhookServer *gitwh.Server
	if cfg.GitWebhookAddr != "" {
		gitWebhookServer = gitwh.NewServer(cfg.GitWebhookAddr, logger, gitwh.Config{Secrets: secretProvider})
	}

	_, loaderSpan := tracing.Start(startupCtx, "functions.load")
	loader := function.NewLoader(function.Dir, logger)
	functions, loadIssues, err := loader.LoadWithDiagnostics()
	if err != nil {
		loaderSpan.RecordError(err)
		loaderSpan.SetStatus(codes.Error, err.Error())
		loaderSpan.End()
		// The worker cannot run without its function set. The deferred cleanup
		// releases the Redis client and stops the servers; the error is returned
		// for the CLI boundary to print and exit on.
		return fmt.Errorf("load functions failed: %w", err)
	}
	loaderSpan.End()
	logger.Info("Loaded functions", "count", len(functions), "invalid", len(loadIssues), "root", function.Dir)

	// Compute every loaded function's content fingerprint — and, for a
	// runtime-backed function, the source selection it was computed from — ONCE
	// for the whole startup, carrying both forward as the immutable startup
	// records. The fingerprint feeds the state phase (rebuild + discovery
	// upserts); the selection feeds Manager.Prepare, which captures it into one
	// immutable snapshot and derives the built image's tag, staged context, and
	// returned identity from that single read — no startup stage re-reads
	// /functions to build. The identity actually built is what the state phase
	// records and the reconciler is seeded with, not this pre-prepare scan. The
	// state DB is a persisted, read-mostly local state view (see internal/state),
	// NOT the source of truth and NOT a snapshot the worker only reads: the worker
	// also writes it (this discovery phase, reconcile outcomes, the 5s stats
	// flush). It never drives matching or building; it is opened after the
	// fingerprints so even a broken DB still yields fingerprints for Prepare and
	// the reconciler.
	_, fingerprintSpan := tracing.Start(startupCtx, "functions.fingerprint")
	fingerprintStart := time.Now()
	startup := selectAndFingerprintFunctions(functions, logger)
	fingerprints := make(map[string]string, len(startup))
	for _, s := range startup {
		fingerprints[s.Function.Name] = s.Fingerprint
	}
	// The state package consumes the narrow (function, fingerprint) pair; the
	// selection stays worker-local because it is a filesystem concern the state
	// layer must not own.
	discovered := discoveredFromStartup(startup)
	fingerprintSpan.End()
	logger.Debug("Startup: fingerprints computed",
		"count", len(startup),
		"duration", time.Since(fingerprintStart),
	)

	// All state errors are non-fatal. A nil handle is never registered, so
	// shutdown simply skips its close.
	_, stateSpan := tracing.Start(startupCtx, "state.initialize")
	st, err := state.Open(state.DBPath)
	if err != nil {
		logger.Warn("State: open failed; continuing without", "error", err)
		stateSpan.RecordError(err)
		st = nil
	}
	if st != nil {
		// Keep the one-hot function_status gauge in sync with every persisted
		// status transition, centrally, so startup discovery, the reconciler, and
		// the service callbacks all flow through one seam. Installed immediately
		// after open, before any discovery/status write, so no transition is
		// missed. Metrics stay decoupled from state (state imports nothing
		// observability-related; the worker owns the closure).
		wireStatusObserver(st, metricsInstance)
		// SQLite close has no context; the shutdown registry bounds its wait.
		shutdown.register(shutdownStep{
			name:    shutdownStepState,
			timeout: 10 * time.Second,
			run:     func(context.Context) error { return st.Close() },
		})
		// The already-computed fingerprint pairs feed both the fresh-database
		// rebuild and the per-function discovery upserts; /functions is never
		// re-read for state and each function's source is hashed exactly once.
		// Present-but-invalid desired definitions are persisted alongside the
		// valid discovery; they stay out of the loaded set, so they never reach
		// the runtime registry, matching, the scheduler, or preparation.
		if rerr := persistStartupDiscovery(st, function.Dir, discovered, loadIssues, logger); rerr != nil {
			stateSpan.RecordError(rerr)
		}
	}
	stateSpan.End()

	// Seed the fresh registry with the persisted cumulative totals so the first
	// snapshot writes them back instead of zeroing SQLite. Gauges are NOT
	// restored (they are point-in-time snapshots refreshed each interval).
	_, statsRestoreSpan := tracing.Start(startupCtx, "state.restore_stats")
	restorePersistedStats(metricsInstance, st)
	statsRestoreSpan.End()

	// The stats flusher is the single owner of every write of the registry's
	// Relay-visible totals into SQLite. It holds a mutex across BOTH the capture
	// of the current snapshot and the write, so an operator reset over the socket
	// (flusher.ResetStats) can never race a flush into resurrecting pre-reset
	// values: a flush either completes before the reset or captures after it.
	// Shared by statsLoop, finalStatsFlush, and the socket reset command.
	statsFlusher := newStatsFlusher(st, metricsInstance)
	shutdown.register(shutdownStep{
		name:    shutdownStepStatsFlush,
		timeout: 2 * time.Second,
		run: func(stepCtx context.Context) error {
			finalStatsFlush(stepCtx, statsFlusher)
			return nil
		},
	})

	// The live runtime-pool query socket (see internal/worker/socket.go). It is
	// started now that the manager exists: the CLI's `function inspect` dials it
	// for the LIVE gauges, which are worker-local and never persisted; the
	// cumulative counters stay in /var/lib/relay. The
	// process lock acquired by `relay start` is already held, so removing a stale
	// socket here can never delete an active worker's socket. A bind failure is
	// fatal, matching the metrics and webhook servers: a local bind error is a
	// host/config problem that must surface at startup, not heal invisibly.
	_, socketSpan := tracing.Start(startupCtx, "runtime.socket")
	rtSocket, err := NewSocketServer(SocketPath, manager, statsFlusher, logger)
	if err != nil {
		socketSpan.RecordError(err)
		socketSpan.SetStatus(codes.Error, err.Error())
		socketSpan.End()
		return fmt.Errorf("runtime state socket: start failed: %w", err)
	}
	socketSpan.End()
	// The readiness query (`relay health`) answers from the worker's readiness
	// state. It is wired now, before the socket serves a request, so the socket
	// always has a checker; the checker's live dependency probe is installed at
	// the ready-to-consume boundary below, and until then its flag is false and
	// it reports not-ready.
	rtSocket.SetReadiness(workerReady)
	// rtSocket.Close takes no context; the shutdown registry bounds its wait.
	shutdown.register(shutdownStep{
		name:    shutdownStepSocket,
		timeout: 5 * time.Second,
		run:     func(context.Context) error { return rtSocket.Close() },
	})
	logger.Info("Runtime state socket listening", "path", SocketPath)

	// The service controller converges each function's persistent service
	// containers to its template (manager is the Docker seam; secretProvider is
	// the shared secrets resolver). Service containers now stop on graceful
	// shutdown: the shutdown tail runs ShutdownCleanup for this worker's
	// hostname (cfg.ConsumerName). The coordinator runs the per-function Applys
	// asynchronously (bounded workers, latest-desired-state coalescing), so
	// startup never blocks on service convergence (e.g. an external image pull);
	// the startup orphan
	// sweep + image GC run inside the coordinator's exclusive housekeeping window
	// in the background pass. Startup reconciliation remains the crash-recovery
	// path when shutdown cleanup did not execute. The service reconciler itself
	// decides whether routing applies (only services declaring a host are routed)
	// and validates TRAEFIK_NETWORK per routed service; wiring only forwards the
	// configured value. The same worker-global NETWORKS set applied to execution
	// containers is forwarded too, so every service container joins it.
	svcCtrl := reconciler.NewServiceReconciler(
		manager,
		secretProvider,
		routing.TraefikConfig{
			Network:      cfg.TraefikNetwork,
			EntryPoints:  cfg.TraefikEntryPoints,
			CertResolver: cfg.TraefikCertResolver,
			Priority:     cfg.TraefikPriority,
			HostOverride: cfg.TraefikHostOverride,
		},
		logger,
		reconcileTimeout,
		reconciler.WithMetrics(metricsInstance),
		reconciler.WithNetworks(cfg.Networks),
	)
	services := reconciler.NewServiceCoordinator(svcCtrl)
	services.Start(ctx)
	// Joining the coordinator releases its workers and waiters and drains
	// in-flight Applys; the join is a strict barrier whose per-step bound is only
	// the wait-before-diagnostic threshold, while the hostname-scoped container
	// cleanup that runs right after is a separate bounded best-effort step.
	shutdown.register(servicesJoinBarrierStep(services.Join))
	shutdown.register(shutdownStep{
		name:    shutdownStepServiceCleanup,
		timeout: shutdownServiceTimeout,
		run: func(stepCtx context.Context) error {
			return shutdownServices(stepCtx, svcCtrl, cfg.ConsumerName)
		},
	})

	// Conservative startup orphan sweep: before any function is prepared or any
	// container created, remove execution containers a previous Relay process on
	// THIS hostname left behind (a crash mid-invocation). It is label- and
	// hostname-scoped, so other workers' and non-Relay containers are untouched.
	// Bounded so the sweep can never hang startup; on timeout/error we log and
	// continue, leaving the orphans for a later restart.
	orphanCtx, orphanSpan := tracing.Start(startupCtx, "orphan.sweep")
	sweepCtx, sweepCancel := context.WithTimeout(orphanCtx, reconcileTimeout)
	n, sweepErr := manager.SweepOrphanContainers(sweepCtx, cfg.ConsumerName)
	sweepCancel()
	if sweepErr != nil {
		// A cancelled lifecycle is a normal shutdown, not a sweep failure.
		orphanSpan.RecordError(sweepErr)
		logStartupCleanupFailure(ctx, logger, "Startup: orphan container sweep failed", sweepErr)
	} else if n > 0 {
		logger.Info("Startup: removed orphan containers from a previous relay process", "count", n)
	}
	orphanSpan.End()

	// Build every function's image. A function whose image cannot be built is
	// marked unavailable so the runner skips it; the rest continue. The startup
	// selections are supplied to Prepare (which captures each into one immutable
	// snapshot) so no startup build re-reads a function's source to derive an
	// identity it already has, nor re-derives the selection policy. The state
	// record and the reconciler seed use the identity each image was ACTUALLY
	// built from, so a source edit during the build is detected by the next audit
	// rather than recorded as current.
	prepareCtx, prepareSpan := tracing.Start(startupCtx, "functions.prepare")
	prepareStart := time.Now()
	prepared := prepareFunctions(prepareCtx, manager, startup, st, logger)
	logger.Debug("Startup: prepared functions", "count", len(prepared), "duration", time.Since(prepareStart))
	prepareSpan.End()

	// Publish each function's initial desired service state and return
	// immediately. The coordinator's bounded workers converge the states in the
	// background, so startup never blocks on service convergence (e.g. an
	// external image pull) — nor on the unavailable/no-services removals, which
	// are published the same nonblocking way (the coordinator derives their own
	// fresh bound).
	_, enqueueSpan := tracing.Start(startupCtx, "services.enqueue")
	enqueueStartupServicesWithState(prepared, services, st, logger)
	enqueueSpan.End()

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
	housekeepingDone := startStartupHousekeeping(ctx, logger, startupHousekeeper{
		exclusive: services.RunExclusive,
		sweep:     func(hctx context.Context) { sweepStartupServiceOrphans(hctx, svcCtrl, liveNames) },
		images:    func(hctx context.Context) { sweepStartupImages(hctx, manager, prepared, st, logger) },
		deps:      func(hctx context.Context) { cleanupStartupDependencies(hctx, manager, logger) },
	})
	// barrier: the startup sweeps use the runtime manager and the state DB, so
	// the manager/state teardown must wait for the pass to actually exit (not
	// merely for this step's bound) before closing either.
	shutdown.register(housekeepingBarrierStep(housekeepingDone))

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

	// Start the metrics components created earlier. The snapshot logger and the
	// /metrics HTTP server are BOTH opt-in (gated on METRICS_ADDR): the registry
	// always exists for the persistent stats flush, but with exposition disabled
	// there is no server to bind (and the periodic logger would only duplicate
	// the stats it already persists). metricsServer is nil exactly when
	// METRICS_ADDR is unset. The logger runs in its own goroutine (exits on ctx);
	// the server binds synchronously — a bind failure (a taken metrics port) is
	// a config error that must surface now, and is FATAL. metricsLoggerDone is
	// joined by the loops shutdown step before the state DB and Redis close, so
	// no logger tick can touch a closed resource.
	var metricsLoggerDone <-chan struct{}
	if metricsServer != nil {
		metricsLogger := metrics.NewMetricsLogger(
			metricsInstance,
			metrics.DefaultLogInterval,
			func(format string, args ...any) {
				logger.Debug(fmt.Sprintf(format, args...))
			},
		)
		done := make(chan struct{})
		metricsLoggerDone = done
		go func() {
			defer close(done)
			metricsLogger.Start(ctx)
		}()

		if err := metricsServer.Start(); err != nil {
			// A bind failure (taken metrics port) is a config error that must
			// surface at startup, not retry invisibly. The deferred cleanup stops
			// the metrics logger goroutine via the cancelled lifecycle.
			return fmt.Errorf("metrics server: start failed: %w", err)
		}
		shutdown.register(shutdownStep{
			name:    shutdownStepMetrics,
			timeout: shutdownStepTimeout,
			run:     metricsServer.Stop,
		})
		logger.Info("Metrics http server listening", "addr", cfg.MetricsAddr)
	}

	// Start the webhook server (created above) right after metrics. It binds
	// synchronously and fails fast on a taken/unparseable GIT_WEBHOOK_ADDR,
	// matching metrics: a webhook port conflict must surface at startup. A nil
	// server means the webhook was disabled, so there is nothing to start.
	if gitWebhookServer != nil {
		if err := gitWebhookServer.Start(); err != nil {
			return fmt.Errorf("git webhook server: start failed: %w", err)
		}
		shutdown.register(shutdownStep{
			name:    shutdownStepWebhook,
			timeout: shutdownStepTimeout,
			run:     gitWebhookServer.Stop,
		})
		logger.Info("Webhook http server listening", "addr", cfg.GitWebhookAddr)
	}

	// Worker-owned background loops. Every loop is tracked by a done channel so
	// the loops shutdown step can join it before the state DB, runtime manager,
	// and Redis client close — a loop must never touch a closed resource.
	var loopDones []<-chan struct{}
	if metricsLoggerDone != nil {
		loopDones = append(loopDones, metricsLoggerDone)
	}

	// Flush the registry into the state database on the fixed cadence. The
	// registry always exists, so the loop runs unconditionally: persistent stats
	// must keep flushing when METRICS_ADDR is unset (there is simply no HTTP
	// exposition alongside them). A nil state handle parks the loop until
	// shutdown (nothing to snapshot), keeping shutdown ordering uniform.
	statsDone := make(chan struct{})
	loopDones = append(loopDones, statsDone)
	go func() {
		defer close(statsDone)
		statsLoop(ctx, statsFlusher, statsFlushInterval)
	}()

	// Optional internal stream retention (cfg.StreamRetention from
	// REDIS_STREAM_RETENTION): a single goroutine periodically trims the
	// configured stream with XTRIM MINID ~ ... ACKED so entries older than the
	// window are removed only once every consumer group has acknowledged them.
	// Completely separate from ACK/retry/DLQ semantics; a malformed or
	// non-positive value is logged by config.Load and retention is disabled. On
	// a server that does not support ACKED (pre-8.2) the loop logs and disables
	// itself rather than trimming unsafely.
	if cfg.StreamRetention > 0 {
		retentionDone := make(chan struct{})
		loopDones = append(loopDones, retentionDone)
		go func() {
			defer close(retentionDone)
			retentionLoop(ctx, client, cfg.RedisStream, cfg.StreamRetention, logger)
		}()
	}

	// Join every worker-owned background loop before the resources they use
	// (state DB, runtime manager, Redis client) are closed. A barrier step: once
	// its bound expires it is cancelled and then joined for real, so a loop
	// cannot touch a closed resource even if it ignores cancellation promptly.
	shutdown.register(loopsBarrierStep(loopDones))

	// The schedule publisher atomically publishes one stream entry per logical
	// occurrence cluster-wide (publish-if-new Lua script) into the same stream the
	// consumer reads, and wires into the scheduler below.
	publisher := schedule.NewPublisher(client, cfg.RedisStream, logger, metricsInstance)

	// The cron scheduler maps each function template's schedules into jobs that
	// publish schedule occurrences through the publisher, seeded from the loaded
	// function set before Start, then converges live via the reconciler's
	// UpdateSchedules/RemoveFunction hooks.
	sched := cron.NewWithMetrics(publisher, logger, metricsInstance)
	for _, fn := range functions {
		sched.ReplaceFunction(fn.Name, fn.Template)
	}
	logger.Info("Scheduler: schedule jobs registered", "count", sched.JobCount())

	// Bounded startup catch-up: republish the latest missed occurrence per
	// schedule (within the 24h horizon) that this worker may have missed while
	// it was down. It runs once, on the initial loaded schedule set, BEFORE the
	// reconciler can converge live changes and before Start; the existing atomic
	// publish-if-new makes a catch-up that another worker already published a
	// harmless duplicate. Older misses are intentionally dropped (bounded
	// recovery, not backlog replay).
	catchUpCtx, catchUpSpan := tracing.Start(startupCtx, "schedule.catchup")
	if n := sched.CatchUp(catchUpCtx); n > 0 {
		logger.Info("Scheduler: startup catch-up published missed occurrences", "count", n)
	}
	catchUpSpan.End()

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
			// each normal Docker operation itself. Shutdown still
			// cancels the operation promptly because ctx is the signal context.
			//
			// The prepared env comes from the current registry entry (the runtime
			// plan env); a nil Prepared (unavailable) falls back to no plan env,
			// mirroring the runner's nil-safe behavior.
			UpdateServices: func(name string, tmpl *function.Template, image string) {
				enqueueLiveServices(
					services,
					manager,
					runWorker.Registry(),
					name,
					tmpl,
					image,
				)
			},
			UpdateServicesWithStatus: func(
				name string,
				tmpl *function.Template,
				image string,
				onReconcileStart func(),
				onComplete func(error),
			) {
				enqueueLiveServicesWithStatus(
					services,
					manager,
					runWorker.Registry(),
					name,
					tmpl,
					image,
					onReconcileStart,
					onComplete,
				)
			},
			UpdateServicesObservationWithStatus: func(
				name string,
				tmpl *function.Template,
				image string,
				onReconcileStart func(),
				onComplete func(error),
			) {
				enqueueLiveServiceObservationWithStatus(
					services,
					manager,
					runWorker.Registry(),
					name,
					tmpl,
					image,
					onReconcileStart,
					onComplete,
				)
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
	// Establish change detection BEFORE seeding the startup fingerprints. The
	// supplied fingerprints are the ones computed once above; a change that
	// lands after the watch is installed is observed as an fsnotify event, and a
	// change between the fingerprint scan and the watch installation is still
	// caught because the seeded value is the OLDER one, so the first reconcile
	// observes it as a rebuild. If the watcher cannot be created, the supplied
	// fingerprints must NOT be trusted: Seed is skipped entirely and Start logs
	// the error and does not run the loops, so no stale seed can suppress a
	// rebuild.
	_, reconcilerSpan := tracing.Start(startupCtx, "reconciler.start")
	if err := rec.PrepareWatch(ctx); err != nil {
		logger.Error("Reconciler: fsnotify error; not seeding startup fingerprints", "error", err)
		reconcilerSpan.RecordError(err)
	} else {
		seedStart := time.Now()
		// Seed the identity each function's image was ACTUALLY built from
		// (Prepared.Fingerprint), not the pre-prepare scan. A source edit between
		// the scan and the build's snapshot changes the built identity, and seeding
		// the built value makes the first reconcile compare against what is really
		// baked: it observes the edit as a rebuild instead of treating the mutated
		// tree as already prepared. The rule lives in the pure startupSeedFingerprints
		// helper so it is pinned by a deterministic unit test.
		seeds := startupSeedFingerprints(functions, fingerprints, prepared)
		for _, fn := range functions {
			rec.Seed(fn, seeds[fn.Name])
		}
		logger.Debug("Startup: seeded startup fingerprints",
			"count", len(functions),
			"duration", time.Since(seedStart),
		)
	}
	logger.Info("Watching functions for changes", "root", function.Dir)

	// Runs in its own goroutine and stops when ctx is cancelled. Start reuses the
	// watcher PrepareWatch already established, and now JOINS its pump/ticker/
	// eventLoop before returning; reconcilerDone is the shutdown barrier that
	// guarantees no reconcile can still be pumped into the runtime manager or
	// state DB when those are closed.
	reconcilerDone := make(chan struct{})
	go func() {
		defer close(reconcilerDone)
		rec.Start(ctx)
	}()
	shutdown.register(reconcilerBarrierStep(reconcilerDone))

	// Start the cron scheduler right after the reconciler, so jobs added here
	// (seeded before Start) fire from their first cron tick and jobs the
	// reconciler later converges schedule immediately.
	sched.Start()
	// barrier: gocron's Shutdown is bounded by WithStopTimeout (and its
	// executor can return ErrStopJobsTimedOut while a task goroutine is still
	// running), so a Relay publisher callback can outlive g.Shutdown and still
	// touch Redis. The scheduler step strictly joins both the shutdown and
	// every admitted callback before any later step (ultimately the Redis
	// close) can run.
	shutdown.register(schedulerBarrierStep(sched.Stop))
	reconcilerSpan.End()

	// The startup root ends here, at the ready-to-consume boundary immediately
	// before Consume. Everything before it is a child of the root; consumption
	// then establishes its own per-message spans.
	startupSpan.End()

	// Ready-to-consume boundary: install the live dependency probe (Redis
	// consumer health + a bounded Docker ping + NETWORKS verification) and mark
	// the worker ready. This is the LAST step before Consume, and it is reached
	// only after the external preflight, function load/prepare, the socket,
	// required listeners and loops, and the consumer/schedule/reconciler/
	// scheduler wiring are all complete. It deliberately does not wait on
	// asynchronous service convergence/housekeeping, optional tracing, SQLite,
	// or per-function success.
	workerReady.startReady(consumer, manager, cfg.Networks)
	logger.Info("Worker ready to consume")

	logger.Info("Consuming stream",
		"stream", cfg.RedisStream,
		"group", cfg.RedisGroup,
		"consumer", cfg.ConsumerName,
	)
	var consumeErr error
	if err := consumer.Consume(ctx, runWorker.Handle); err != nil {
		// The consumer is the worker's raison d'être; a Consume error means the
		// consumption loop has stopped. The deferred cleanup still drains
		// everything in order, and the error is returned after shutdown rather
		// than exiting mid-teardown. (Shutdown via a cancelled ctx returns nil, so
		// a non-nil error here is a genuine failure.)
		logger.Error("Consume failed", "error", err)
		consumeErr = fmt.Errorf("consume failed: %w", err)
	}

	// The graceful shutdown (socket, scheduler, reconciler, housekeeping,
	// services, loops, stats, servers, manager, state, tracing, Redis) is owned
	// by the deferred shutdown registry registered at the top, so a startup
	// failure and a normal shutdown converge on the exact same explicit order.
	return consumeErr
}

// Shutdown step names. Each names a single teardown step; together they are the
// explicit teardown order (shutdownStepOrder) and the structured `step` field
// on a failure log. Registration happens as resources are acquired — which is
// NOT teardown order (Redis is acquired first and released last) — so the order
// is declared here rather than inherited from registration/LIFO.
const (
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

// shutdownServices stops and removes THIS worker's persistent service
// containers during graceful shutdown. ctx is the shutdown step's fresh,
// bounded context (the registry derives it from context.Background), so a
// Docker problem or timeout must never fail the process (no os.Exit anywhere on
// this path). It returns ShutdownCleanup's error so the shutdown registry can
// surface it with the structured step name; callers may discard it. The
// detailed per-operation logs live in ShutdownCleanup, so no duplicate logging
// happens here.
func shutdownServices(
	ctx context.Context, svcCtrl *reconciler.ServiceReconciler, hostname string,
) error {
	_, err := svcCtrl.ShutdownCleanup(ctx, hostname)
	return err
}

// setupMetrics constructs the metrics components. The accounting registry is
// ALWAYS constructed — it is the single source of truth for the counters the
// persistent SQLite snapshot flushes, so it must exist even when HTTP
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
// persisted lifecycle transition onto the one-hot function_status gauge. An
// EMPTY status is the removal/prune signal and deletes the function's status
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
			metricsInstance.RemoveFunctionStatus(name)
			return
		}
		metricsInstance.SetFunctionStatus(name, status)
	})
}

// managedRuntimeBuildContext installs the function-image build observer that
// publishes the building status at the ACTUAL managed runtime image-build
// boundary. It is passed to Manager.Prepare, whose runtime fires the observer
// only when a build is really issued — after the reuse probes — so a reused
// image never flashes building, and a template that needs no runtime (Prepare is
// then a fast no-op) never fires it at all. It is nil-state-safe. Installed
// unconditionally: for a no-runtime template the observer simply never runs.
func managedRuntimeBuildContext(ctx context.Context, st *state.State, fn function.Function) context.Context {
	if st == nil {
		return ctx
	}
	return runtime.WithFunctionBuildObserver(ctx, func() {
		st.RecordReconcileBuilding(fn.Name)
	})
}

// startupFunction is one loaded function's immutable startup identity: the
// function, the content fingerprint computed for it exactly once, and — for a
// runtime-backed function — the source selection that fingerprint was computed
// from. Carrying the selection alongside the digest lets every later startup
// stage (state writes, Manager.Prepare, the reconciler seed, and the image
// keep-set) reuse one traversal instead of re-reading /functions. It is
// worker-local on purpose: the selection is a filesystem concern the state
// package must not own, so state only ever sees the narrow
// (function, fingerprint) conversion below.
//
// Fingerprint here is the PRE-PREPARE scan: it seeds the desired state. The
// identity an image is ACTUALLY built from comes back as
// runtime.Prepared.Fingerprint from the one immutable snapshot Prepare captures,
// and that returned value is what the state phase records and the reconciler is
// seeded with.
//
// Selection is nil for a no-runtime (external-image-only) function, which builds
// no function image; its Fingerprint is then template-only by construction.
type startupFunction struct {
	Function    function.Function
	Fingerprint string
	Selection   *source.Selection
}

// selectAndFingerprintFunctions computes each loaded function's fingerprint —
// and, for a runtime-backed function, resolves its source selection in the SAME
// traversal — exactly once for the whole startup. The returned records are the
// immutable startup identity reused by the state phase, Manager.Prepare, the
// reconciler seed, and the startup image keep-set. A fingerprint error is logged
// and yielded as "" with a nil selection — the same fallback the state package
// uses — so a transient read failure never blocks discovery (the empty seed then
// forces a reconcile rebuild).
func selectAndFingerprintFunctions(functions []function.Function, logger *slog.Logger) []startupFunction {
	return selectAndFingerprintFunctionsWith(functions, logger, function.SelectAndFingerprintFunction)
}

// selectAndFingerprintFunctionsWith is selectAndFingerprintFunctions with the
// identity resolver injected. Production passes
// function.SelectAndFingerprintFunction (the wrapper above); a test passes a
// counting/spying resolver so it can prove the helper resolves each loaded
// function's selection+fingerprint EXACTLY once and carries the SAME values into
// the returned records (which feed the state phase, Prepare, and the reconciler
// seed) without rehashing.
func selectAndFingerprintFunctionsWith(
	functions []function.Function,
	logger *slog.Logger,
	resolve func(dir string, tmpl *function.Template) (*source.Selection, string, error),
) []startupFunction {
	startup := make([]startupFunction, 0, len(functions))
	for _, fn := range functions {
		selection, fp, err := resolve(fn.Dir, fn.Template)
		if err != nil {
			logger.Warn("Function: fingerprint failed", "function", fn.Name, "error", err)
			selection, fp = nil, ""
		}
		startup = append(startup, startupFunction{Function: fn, Fingerprint: fp, Selection: selection})
	}
	return startup
}

// discoveredFromStartup converts the worker-local startup records to the narrow
// (function, fingerprint) pairs the state package consumes, so state never sees
// — or owns — the filesystem selection.
func discoveredFromStartup(startup []startupFunction) []state.DiscoveredFunction {
	discovered := make([]state.DiscoveredFunction, 0, len(startup))
	for _, s := range startup {
		discovered = append(discovered, state.DiscoveredFunction{Function: s.Function, Fingerprint: s.Fingerprint})
	}
	return discovered
}

// startupSeedFingerprints returns the seed value for each loaded function, keyed
// by name. The preferred seed is the identity the function's image was ACTUALLY
// built from (Prepared.Fingerprint); the pre-prepare scan (fingerprints) is only
// the fallback for a function that produced no built identity (an unavailable
// build, a no-runtime function that builds no image, or a hand-built test value).
//
// Seeding the BUILT identity is what makes the first reconcile compare the live
// tree against what is really baked: a source edit between the pre-prepare scan
// and the build's snapshot changes the built value, so the reconcile observes a
// rebuild instead of treating the mutated tree as already prepared. The rule is a
// pure helper so it is pinned by a deterministic unit test rather than inferred
// from Run's inline wiring.
func startupSeedFingerprints(
	functions []function.Function,
	fingerprints map[string]string,
	prepared []*runner.PreparedFunction,
) map[string]string {
	builtByFn := make(map[string]string, len(prepared))
	for _, pf := range prepared {
		if p := pf.Prepared(); p != nil && p.Fingerprint != "" {
			builtByFn[pf.Name()] = p.Fingerprint
		}
	}
	seeds := make(map[string]string, len(functions))
	for _, fn := range functions {
		seed := fingerprints[fn.Name]
		if built := builtByFn[fn.Name]; built != "" {
			seed = built
		}
		seeds[fn.Name] = seed
	}
	return seeds
}

// persistStartupDiscovery writes the startup state phase: it seeds a fresh
// database from the already-loaded (function, fingerprint) pairs, records each
// PRESENT-but-invalid desired definition from the loader diagnostics as invalid
// (preserving any active generation while clearing the untrustworthy desired
// snapshot), prunes rows for functions genuinely absent from dir, and records
// each valid function as discovered (preserving its active generation).
//
// Ordering matters. The invalid writes happen BEFORE the prune so a directory
// the loader observed as present but which vanished before the sweep is still
// removed by the filesystem-authoritative prune rather than resurrected as an
// invalid row — a disappearance is a removal, never an invalid desired state.
// The prune still runs BEFORE the valid discovery so a function removed while
// down is never resurrected, and the whole phase runs before
// restorePersistedStats (the caller) so a pruned function's stats row is gone
// before the fresh registry is seeded from it. Invalid functions stay out of the
// loaded set and are never passed to discovery, matching, the scheduler, or
// startup preparation — only their state view is updated. Relay-owned transient
// staging directories (function.IsReservedDir) are ignored entirely: a reserved
// issue is never recorded as an invalid desired definition, and PruneRemoved
// removes any stale reserved row a buggy prior discovery may have left, so a
// live sync's ".sync-*" directory can never create, update, or retain state.
//
// It is split out of Run as a pure seam over the state handle so the
// discovery/invalid persistence is unit-testable without Docker or Redis. A
// rebuild error is returned for the caller to surface (state stays non-fatal).
func persistStartupDiscovery(
	st *state.State,
	dir string,
	discovered []state.DiscoveredFunction,
	issues []function.LoadIssue,
	logger *slog.Logger,
) error {
	var rebuildErr error
	if err := st.RebuildFromFunctions(discovered); err != nil {
		rebuildErr = err
		logger.Warn("State: rebuild from functions failed; continuing", "error", err)
	}
	for _, issue := range issues {
		if function.IsReservedDir(issue.Name) {
			// Defense in depth: the loader already filters Relay-owned staging
			// directories, so a reserved issue should never reach here. If one
			// does (a future caller), it must not create or update state for a
			// directory that is not a function.
			continue
		}
		st.RecordInvalidDesired(issue.Name, issue.Err)
	}
	st.PruneRemoved(dir)
	for _, d := range discovered {
		st.RecordDiscoveredWithFingerprint(d.Function, d.Fingerprint)
	}
	return rebuildErr
}

// functionPreparer is the narrow view of the runtime Manager that startup
// preparation needs: the selection-aware Prepare, plus (via runner.Executor) the
// Execute used to pair each returned handle with its executor for the runner.
// *runtime.Manager satisfies it; a test spy can implement it, so the startup
// handoff (the resolved fingerprint+selection must reach Prepare, not be
// recomputed) is unit-testable without Docker.
type functionPreparer interface {
	PrepareWithFingerprintAndSelection(
		ctx context.Context,
		fn function.Function,
		fingerprint string,
		selection *source.Selection,
	) (*runtime.Prepared, error)
	runner.Executor
}

// prepareFunctions builds each function's image and returns the prepared set. A
// function whose image cannot be built is marked unavailable (the runner skips
// it) rather than failing startup; the rest carry their fresh image. It receives
// the startup records — each function with the fingerprint resolved exactly once
// and (for a runtime-backed function) the source selection that fingerprint was
// derived from — and supplies BOTH to the selection-aware Prepare. Prepare
// itself captures one immutable snapshot of the selected source and derives the
// build tag, the staged context, and the returned identity from it, so the built
// generation is coherent even if the tree mutates during the build; the startup
// scan is the input that says "a rebuild may be needed", never the label.
//
// The persisted record uses the RETURNED identity (prep.Fingerprint), so the
// state DB always describes the content the image actually serves. A source edit
// during the build is not in the image and is detected by the reconciler's
// watcher/periodic audit on the next pass, exactly as in the live reconcile path.
//
// The building status is published at the ACTUAL managed runtime image-build
// boundary via the observer installed by managedRuntimeBuildContext: a reused
// image and a no-runtime template never flash building, while a real build does.
// A successful no-service preparation reaches ready below; a function whose
// template declares services records its terminal outcome after service
// convergence (services have no separate image build, so building is
// published only for the managed-runtime image build itself), and a preparation
// failure uses the existing failure semantics (unavailable without an active
// image, degraded when a previous image is retained).
//
// ctx is the worker lifecycle context. It is passed to Prepare so a build (and
// the fast reuse probes) is cancelled on shutdown; Prepare itself roots the
// Dockerfile build in the manager lifecycle with its own 10m buildTimeout, so
// this context's lack of a short deadline is intentional and the build is never
// bounded by the 30s reconcileTimeout.
func prepareFunctions(
	ctx context.Context,
	manager functionPreparer,
	startup []startupFunction,
	st *state.State,
	logger *slog.Logger,
) []*runner.PreparedFunction {
	preparedCount := 0
	prepared := make([]*runner.PreparedFunction, 0, len(startup))
	for _, s := range startup {
		fn := s.Function
		prep, err := manager.PrepareWithFingerprintAndSelection(
			managedRuntimeBuildContext(ctx, st, fn), fn, s.Fingerprint, s.Selection,
		)
		if err != nil {
			// A build cancelled by the lifecycle is a shutdown, not a build
			// failure: it must not record a spurious reconcile failure in the
			// state DB. The function is still marked unavailable, but Run is
			// already converging on shutdown.
			if startupInterrupted(ctx, err) {
				logger.Debug("Function: prepare interrupted by shutdown", "function", fn.Name, "error", err)
			} else {
				logger.Warn("Function: prepare failed", "function", fn.Name, "error", err)
				if st != nil {
					st.RecordReconcileFailure(fn.Name, err)
				}
			}
			prepared = append(prepared, runner.NewUnavailable(fn))
			continue
		}
		if st != nil && len(fn.Template.Services) == 0 {
			// Persist the fingerprint the image was ACTUALLY built from
			// (prep.Fingerprint, derived from the one immutable source snapshot the
			// build staged), never a later live rescan. Recording a post-build scan
			// would claim the image serves content it does not: a source edit during
			// a long build is not in the image, and the reconciler's watcher/periodic
			// audit detects it by comparing the on-disk digest against this recorded
			// built identity on the next pass.
			st.RecordReconcileSuccess(fn.Name, prep.Image, prep.Fingerprint, time.Now(), fn)
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
// bounded workers converge it with the worker LIFECYCLE context. A nil Prepared
// (unavailable) entry falls back to no plan env, mirroring the runner's
// nil-safe behavior.
//
// The managed function image lease is acquired BEFORE the enqueue and handed to
// the coordinator, which holds it across pending/coalesced/running service
// reconciliation through StartService completion, so a concurrent retirement
// cannot remove the image a service pass is still converging. When the image is
// already retiring (a superseded desired state), the enqueue is skipped: a newer
// desired state will follow.
func enqueueLiveServices(
	services *reconciler.ServiceCoordinator,
	manager *runtime.Manager,
	reg *runner.Registry,
	name string,
	tmpl *function.Template,
	image string,
) {
	var preparedEnv []string
	if cur := reg.GetByName(name); cur != nil && cur.Prepared() != nil {
		preparedEnv = cur.Prepared().Env
	}
	lease := acquireServiceLease(manager, name, image)
	services.EnqueueLeased(name, tmpl, image, preparedEnv, lease)
}

func enqueueLiveServicesWithStatus(
	services *reconciler.ServiceCoordinator,
	manager *runtime.Manager,
	reg *runner.Registry,
	name string,
	tmpl *function.Template,
	image string,
	onReconcileStart func(),
	onComplete func(error),
) {
	var preparedEnv []string
	if cur := reg.GetByName(name); cur != nil && cur.Prepared() != nil {
		preparedEnv = cur.Prepared().Env
	}
	lease := acquireServiceLease(manager, name, image)
	services.EnqueueWithStatusLeased(
		name,
		tmpl,
		image,
		preparedEnv,
		lease,
		onReconcileStart,
		onComplete,
	)
}

func enqueueLiveServiceObservationWithStatus(
	services *reconciler.ServiceCoordinator,
	manager *runtime.Manager,
	reg *runner.Registry,
	name string,
	tmpl *function.Template,
	image string,
	onReconcileStart func(),
	onComplete func(error),
) {
	var preparedEnv []string
	if cur := reg.GetByName(name); cur != nil && cur.Prepared() != nil {
		preparedEnv = cur.Prepared().Env
	}
	lease := acquireServiceLease(manager, name, image)
	services.EnqueueStatusObservationLeased(
		name,
		tmpl,
		image,
		preparedEnv,
		lease,
		onReconcileStart,
		onComplete,
	)
}

// acquireServiceLease admits a managed image lease for a service pass, or nil
// when the image is not Relay-owned (an external image service) or a manager is
// absent (tests). A retirement in progress yields nil: the enqueue proceeds
// unleased, and a newer desired state will replace it. It never fails the
// caller — service convergence is best-effort and the lease only narrows a
// removal race.
func acquireServiceLease(manager *runtime.Manager, name, image string) *runtime.ImageLease {
	if manager == nil || !runtime.IsRelayImage(image) {
		return nil
	}
	lease, err := manager.AcquireImageLease(image)
	if err != nil {
		manager.Logger().Debug("Service: image retiring; enqueue unleased",
			"function", name, "image", image)
		return nil
	}
	return lease
}

// enqueueStartupServices publishes each prepared function's initial desired
// service state and returns immediately; the coordinator converges them in the
// background, so startup never blocks on service convergence (e.g. an external
// image pull). The
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
func enqueueStartupServices(prepared []*runner.PreparedFunction, services *reconciler.ServiceCoordinator, logger *slog.Logger) {
	enqueueStartupServicesWithState(prepared, services, nil, logger)
}

func enqueueStartupServicesWithState(
	prepared []*runner.PreparedFunction,
	services *reconciler.ServiceCoordinator,
	st *state.State,
	logger *slog.Logger,
) {
	for _, pf := range prepared {
		fn := pf.Function()
		if prep := pf.Prepared(); prep != nil {
			// Share the function's publication lease into the startup service
			// request, so a concurrent retirement cannot remove the image while
			// the initial service convergence still needs it. The share is
			// admitted from the SAME lease the registry will publish, and is
			// released when the request concludes. A no-runtime function has no
			// lease and enqueues unleased.
			lease := pf.SharePublication()
			if st != nil && len(fn.Template.Services) > 0 {
				services.EnqueueWithStatusLeased(fn.Name, fn.Template, prep.Image, prep.Env, lease,
					func() { st.RecordReconciling(fn.Name) },
					func(err error) {
						if err != nil {
							st.RecordServiceFailure(fn.Name, err)
							return
						}
						// Persist the fingerprint the image was ACTUALLY built
						// from, never a later live rescan: a post-convergence scan
						// could record content the active image does not serve, and
						// the next reconcile/audit detects any drift by comparing the
						// on-disk digest against this built identity.
						st.RecordReconcileSuccess(fn.Name, prep.Image, prep.Fingerprint, time.Now(), fn)
					})
			} else {
				services.EnqueueLeased(fn.Name, fn.Template, prep.Image, prep.Env, lease)
			}
			continue
		}
		if len(fn.Template.Services) == 0 {
			services.EnqueueRemove(fn.Name)
		} else {
			logger.Warn("Service: function unavailable; skipping service reconcile", "function", fn.Name)
		}
	}
}

// startupHousekeeper groups the background startup cleanup passes so their
// ordering and lifecycle behavior are deterministic to test without Docker.
// exclusive is the coordinator's housekeeping seam (see
// ServiceCoordinator.RunExclusive): it atomically waits for current service work
// to settle, PAUSES scheduling of new requests for the duration of the callback,
// and resumes/schedules the latest coalesced desired states afterward. The three
// passes run inside that exclusive window in the safe order — orphan container
// sweep, then image sweep (whose keep-set must observe the settled service
// containers), then dependency GC (which prunes dependency images the image
// sweep orphaned) — so no live UpdateServices can race them.
type startupHousekeeper struct {
	exclusive func(context.Context, func(context.Context)) error
	sweep     func(context.Context)
	images    func(context.Context)
	deps      func(context.Context)
}

// startStartupHousekeeping launches the lifecycle-aware startup housekeeping in a
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
func startStartupHousekeeping(
	lifecycle context.Context,
	logger *slog.Logger,
	h startupHousekeeper,
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
// unchanged) and services are converged. The keep-set holds (a) the exact image
// each function was actually prepared with this boot (Prepared.Image, not a
// re-derived expected tag), (b) images the running service containers
// reference. It also keeps (c) every last-active image recorded in state — the
// crash guard for a swap that started but whose RecordReconcileSuccess never
// landed, where the recorded image may still be the one serving.
//
// The recorded-image gather is the full state listing rather than only the
// loaded set: a function whose desired definition is PRESENT but INVALID is not
// part of the loaded set (it is never prepared or registered), yet it may still
// have a previously-serving active image that must not be swept. PruneRemoved
// has already dropped rows for genuinely absent functions, so every remaining
// row corresponds to a function present on disk (or one whose stat transiently
// failed, which is conservatively kept); keeping its recorded image is the safe
// choice. This affects GC only — never execution, which uses the runtime
// registry of valid functions.
//
// Keeping Prepared.Image is deliberate: the image each function is actually
// serving this boot is the authoritative answer, and it needs no tree scan — a
// function whose build failed (unavailable) contributes no prepared image, and
// its previous serving image is covered by the recorded state image. This
// replaces a re-hash of every function tree purely to reconstruct an expected
// tag, so a build that reused an image or a no-runtime function costs nothing
// here.
//
// When state is nil (DB failed to open) the recorded images are unavailable, so
// the sweep below is skipped entirely: we cannot distinguish a removed function
// from a live one, and orphan removal must never run against an unknown world.
// Conservative: nothing that might still serve is ever removed.
func sweepStartupImages(
	lifecycle context.Context,
	manager *runtime.Manager,
	prepared []*runner.PreparedFunction,
	st *state.State,
	logger *slog.Logger,
) {
	// The exact image each prepared function is serving this boot. This is the
	// authoritative keep input: no fingerprint re-scan, no expected-tag
	// reconstruction.
	preparedImages := make([]string, 0, len(prepared))
	for _, pf := range prepared {
		if p := pf.Prepared(); p != nil && p.Image != "" {
			preparedImages = append(preparedImages, p.Image)
		}
	}

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
		logStartupCleanupFailure(lifecycle, logger, "Service: keep-set list failed; continuing without", err)
	}
	// The state keep-set is only armed when the DB was available. When st is
	// nil recordedImages stays empty, so the keep-set falls back to the prepared
	// and service images alone (and the sweep below is skipped entirely).
	var recordedImages []string
	if st != nil {
		for _, img := range st.ActiveImages() {
			if img != "" {
				recordedImages = append(recordedImages, img)
			}
		}
	}

	keep := startupImageKeepSet(preparedImages, serviceImages, recordedImages)
	if st != nil {
		// The image removal pass is a Docker listing plus one removal per
		// orphaned image; bound it with its own fresh reconcileTimeout so a slow
		// daemon cannot make the exclusive housekeeping window — and every
		// service update coalesced behind it — wait unbounded.
		removeCtx, cancel := context.WithTimeout(lifecycle, reconcileTimeout)
		_, err := manager.RemoveImagesExcept(removeCtx, keep)
		cancel()
		if err != nil {
			logStartupCleanupFailure(lifecycle, logger, "Image cleanup: startup sweep failed", err)
		}
	}
}

// sweepStartupServiceOrphans runs the startup service-orphan sweep each under
// its own fresh reconcileTimeout rooted in the housekeeping lifecycle, so a slow
// daemon cannot make the exclusive housekeeping window (and every service update
// coalesced behind it) wait unbounded. It preserves the ServiceReconciler's
// existing behavior (list then stop stale containers); the only change is the
// bound, matching the per-function Applys.
func sweepStartupServiceOrphans(
	hctx context.Context, svcCtrl *reconciler.ServiceReconciler, liveNames map[string]bool,
) {
	sweepCtx, cancel := context.WithTimeout(hctx, reconcileTimeout)
	defer cancel()
	svcCtrl.SweepOrphans(sweepCtx, liveNames)
}

// networkVerifier is the narrow view of the runtime Manager that the startup
// NETWORKS pre-flight needs. *runtime.Manager satisfies it; a test fake can
// implement it, so the verification error semantics are unit-testable without a
// Docker daemon (matching the preflight seam's shape).
type networkVerifier interface {
	VerifyNetworks(ctx context.Context, networks []string) (string, bool, error)
}

// verifyConfiguredNetworks verifies every network in the worker-global NETWORKS
// set exists on the Docker daemon before any function is prepared or any
// container created. A missing network, or a verify error (a broken daemon), is
// a fatal startup failure: Relay never creates networks, and an execution
// container silently created on the wrong network would be a latent runtime
// fault. It is a no-op when networks is empty, so an unset NETWORKS keeps the
// default bridge behavior. The check is bounded by the worker lifecycle and a
// reconcileTimeout so a hung daemon cannot stall startup forever.
func verifyConfiguredNetworks(ctx context.Context, manager networkVerifier, networks []string) error {
	if len(networks) == 0 {
		return nil
	}
	verifyCtx, cancel := context.WithTimeout(ctx, reconcileTimeout)
	defer cancel()
	missing, ok, err := manager.VerifyNetworks(verifyCtx, networks)
	if err != nil {
		return fmt.Errorf("verify NETWORKS: %w", err)
	}
	if !ok {
		return fmt.Errorf("NETWORKS network %q does not exist (relay never creates networks)", missing)
	}
	return nil
}

// startupImageKeepSet computes the set of image references the startup sweep
// must keep: (a) the exact image each function was actually prepared with this
// boot (Prepared.Image), (b) every image a running service container
// references, and (c) every last-active image recorded for a function still on
// disk — the crash guard for a swap that started but whose
// RecordReconcileSuccess never landed, where the recorded image may still be the
// one serving.
//
// It takes the ALREADY-RESOLVED prepared image references rather than
// re-hashing each function tree to reconstruct an expected tag: the prepared
// refs are the authoritative "what is live right now" and cost no filesystem
// work, whereas a fingerprint re-scan would duplicate the startup hash and could
// even disagree with what was actually built. A no-runtime function has no
// prepared image and is naturally absent; any image a previous (runtime) version
// built is covered by its recorded last-active image. It is a pure function so
// the keep-set policy is unit testable without Docker or a state DB; blank image
// entries are ignored.
func startupImageKeepSet(preparedImages, serviceImages, recordedImages []string) map[string]bool {
	keep := make(map[string]bool)
	for _, img := range preparedImages {
		if img != "" {
			keep[img] = true
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
	// A fresh reconcileTimeout bound per pass, rooted in the housekeeping
	// lifecycle: like the orphan/image sweeps it runs inside the exclusive
	// window, so an unbounded dependency GC would hold every coalesced service
	// update behind it. It is still rooted in the lifecycle, so shutdown cancels
	// it promptly as well.
	depCtx, cancel := context.WithTimeout(lifecycle, reconcileTimeout)
	defer cancel()
	if _, err := manager.CleanupUnusedDependencies(depCtx); err != nil {
		logStartupCleanupFailure(lifecycle, logger, "Dependency image cleanup failed", err)
	}
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
	// metrics is the accounting registry the worker always constructs; it is
	// nil only when a caller deliberately passes nil (tests), in which case the
	// snapshot mappers yield zero values. Production always supplies the
	// registry, so a flush never clobbers the persisted cumulative totals.
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

// finalStatsFlush performs a final flush of the registry into SQLite on
// graceful shutdown, so the last interval of telemetry is not lost. ctx is the
// shutdown step's fresh, bounded context (the registry derives it from
// context.Background), so a wedged SQLite cannot hang shutdown; on timeout or
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

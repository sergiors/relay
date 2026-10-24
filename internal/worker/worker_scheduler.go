package worker

import (
	"context"
	"io"
	"log/slog"

	"github.com/redis/go-redis/v9"

	"relay/internal/app"
	"relay/internal/config"
	"relay/internal/cron"
	"relay/internal/observability/metrics"
	"relay/internal/observability/tracing"
	"relay/internal/schedule"
	"relay/internal/state"
	"relay/internal/stream"
)

// newWorkerScheduler constructs and wires the cron scheduler, then returns it
// for the reconciler hooks and Run's later Start. It does NOT start the
// scheduler: Run starts it after the reconciler so jobs seeded here fire from
// their first tick and jobs converged live are scheduled immediately.
//
// The shutdown barrier is registered IMMEDIATELY after construction and BEFORE
// starting anything that can touch Redis or the state DB (the durable retry
// worker, catch-up, jobs). gocron's Shutdown is bounded by WithStopTimeout (and
// its executor can return ErrStopJobsTimedOut while a task goroutine is still
// running), so a Relay publisher callback (or the durable retry worker) can
// outlive g.Shutdown and still touch Redis. This barrier strictly joins both
// the gocron shutdown, every admitted publisher callback, the durable retry
// worker, AND (when storage was unavailable at startup) the scheduler-owned
// storage-bootstrap loop, before closing any scheduler-owned state handle.
// Registering it here means an early startup failure after this point still
// joins them. Stop is idempotent and also guards against a scheduler that was
// never Started, so an early-return shutdown is safe.
func newWorkerScheduler(
	lifecycle context.Context,
	startupCtx context.Context,
	logger *slog.Logger,
	cfg config.Config,
	client *redis.Client,
	st *state.State,
	metricsInstance *metrics.Registry,
	consumer *stream.Consumer,
	apps []app.App,
	shutdown *shutdownRegistry,
) *cron.Scheduler {
	// The schedule publisher atomically publishes one stream entry per logical
	// occurrence cluster-wide (publish-if-new Lua script) into the same stream
	// the consumer reads, and wires into the scheduler below.
	publisher := schedule.NewPublisher(
		client, cfg.RedisStream, logger, metricsInstance,
	)

	// The cron scheduler maps each app template's schedules into jobs that
	// publish schedule occurrences through the publisher, seeded from the loaded
	// app set before Start, then converges live via the reconciler's
	// UpdateSchedules/RemoveApp hooks.
	sched := cron.NewWithMetrics(publisher, logger, metricsInstance)
	shutdown.register(schedulerBarrierStep(sched.Stop))

	// The scheduler is a metrics.GaugeSource: register it with the consumer's
	// single periodic gauge refresher so its durable-outbox depth/age gauges are
	// sampled on the existing ~15s pass, with no separate ticker or goroutine.
	// This is safe before Consume starts the refresher; the source reads the
	// scheduler's CURRENT outbox at each tick, so a bootstrap-recovered
	// scheduler-owned handle is observed too.
	consumer.AddGaugeSource(sched)

	// Schedule firing REQUIRES a usable durable outbox: every due occurrence is
	// persisted to it BEFORE it is published (intent-first durability), so a tick
	// that cannot be persisted must not fire. The outbox is the PostgreSQL state
	// database. The shared handle opened at startup (st) is always available
	// here (its open is fatal), so it is wired directly; the scheduler-owned
	// bootstrap opener remains as a defensive recovery seam for a future where
	// the shared handle could be unavailable. PostgreSQL is required for
	// scheduler correctness, but it is NOT a global worker dependency:
	// consumers, functions, services, metrics, logging, and tracing stay
	// operational either way, and worker readiness deliberately excludes it.
	wireSchedulerStorage(
		lifecycle, sched, st,
		schedulerStorageOpener(cfg.DatabaseURL, logger, metricsInstance),
	)

	for _, fn := range apps {
		sched.ReplaceApp(fn.Name, fn.Template)
	}
	logger.Info("Scheduler: schedule jobs registered", "count", sched.JobCount())

	// Bounded startup catch-up: republish the latest missed occurrence per
	// schedule (within the 24h horizon) that this worker may have missed while
	// it was down. It runs once, on the initial loaded schedule set, BEFORE the
	// reconciler can converge live changes and before Start; the existing atomic
	// publish-if-new makes a catch-up that another worker already published a
	// harmless duplicate. Older misses are intentionally dropped (bounded
	// recovery, not backlog replay). When storage is unavailable this is a no-op
	// and does NOT consume the once-only flag: the storage-bootstrap recovery
	// re-runs the same bounded scan once the outbox is usable.
	catchUpCtx, catchUpSpan := tracing.Start(startupCtx, "schedule.catchup")
	if n := sched.CatchUp(catchUpCtx); n > 0 {
		logger.Info(
			"Scheduler: startup catch-up published missed occurrences",
			"count", n,
		)
	}
	catchUpSpan.End()

	return sched
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

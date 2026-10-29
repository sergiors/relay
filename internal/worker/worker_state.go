package worker

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel/codes"

	"relay/internal/config"
	"relay/internal/observability/metrics"
	"relay/internal/observability/tracing"
	"relay/internal/state"
)

// openWorkerState opens the worker's REQUIRED PostgreSQL state handle and
// installs the status observer immediately after a successful open, before any
// discovery or status write, so no persisted lifecycle transition is missed.
// The open takes an exclusive per-NODE_ID session advisory lock, which the
// ownership monitor re-verifies on the pinned connection.
//
// It is the first fallible resource step Run performs: the state handle is
// opened before the Redis/Docker preflight and before any app is loaded or
// fingerprinted. A genuine failure is returned wrapped for the CLI. A failure
// that only reflects a cancelled lifecycle is returned as
// errStartupInterrupted with a nil handle, so Run must apply startupResult to
// turn it into a graceful nil return.
func openWorkerState(
	startupCtx context.Context,
	logger *slog.Logger,
	cfg config.Config,
	metricsInstance *metrics.Registry,
) (*state.State, error) {
	stateSpanCtx, stateSpan := tracing.Start(startupCtx, "state.initialize")
	st, err := state.Open(stateSpanCtx, state.Options{
		DSN:             cfg.DatabaseURL,
		NodeID:          cfg.NodeID,
		Instance:        cfg.InstanceID,
		Logger:          logger,
		MaxOpenConns:    config.DefaultDatabaseMaxOpenConns,
		MaxIdleConns:    config.DefaultDatabaseMaxIdleConns,
		ConnMaxLifetime: config.DefaultDatabaseConnMaxLifetime,
		ConnMaxIdleTime: config.DefaultDatabaseConnMaxIdleTime,
		ConnectTimeout:  config.DefaultDatabaseConnectTimeout,
	})
	if err != nil {
		stateSpan.RecordError(err)
		stateSpan.SetStatus(codes.Error, err.Error())
		stateSpan.End()
		if startupInterrupted(startupCtx, err) {
			logger.Info(
				"Startup: state database initialization interrupted by shutdown",
				"error", err,
			)
			return nil, errStartupInterrupted
		}
		return nil, fmt.Errorf("state: open failed: %w", err)
	}
	stateSpan.End()
	// Keep the one-hot app_status gauge in sync with every persisted status
	// transition, centrally, so startup discovery, the reconciler, and the
	// service callbacks all flow through one seam. Metrics stay decoupled from
	// state (state imports nothing observability-related; the worker owns the
	// closure).
	wireStatusObserver(st, metricsInstance)
	return st, nil
}

// startOwnershipTracking starts the periodic ownership monitor now that the
// NODE_ID lock is held (state.Open took it) and registers its quiescence
// barrier, so an early startup failure after this point still joins the monitor
// (which reads the pinned lock connection) before the state handle closes in a
// later step.
//
// The once-only loss side effects and the fail-closed shutdown proof share ONE
// action (ownershipLossAction) and ONE proof seam, so a loss detected by the
// monitor, the final barrier verification, or an identity-cleanup guard latches
// exactly once and skips identity-scoped work the same way.
func startOwnershipTracking(
	ctx context.Context,
	st *state.State,
	loss *ownershipLoss,
	onLost func(),
	logger *slog.Logger,
	shutdown *shutdownRegistry,
) ownershipProof {
	proof := ownershipProof{
		loss:   loss,
		verify: st.VerifyOwnerLock,
		onLost: onLost,
	}
	ownershipDone := startOwnershipMonitor(ctx, onLost, ownershipMonitorConfig{
		verify:   st.VerifyOwnerLock,
		interval: state.OwnerLockCheckInterval,
		timeout:  state.OwnerLockCheckTimeout,
	})
	shutdown.register(ownershipBarrierStep(ownershipDone, proof, logger))
	return proof
}

// dlqDepthGauge is the periodic PostgreSQL DLQ depth sampler. It replaces the
// former Redis XLEN observation: the durable DLQ is a PostgreSQL table shared by
// every worker configured with the same database, so the gauge reports an
// approximate row count. It deliberately uses pg_class.reltuples, an inexpensive
// planner estimate, rather than a hot-path COUNT(*) scan. A read failure skips
// the tick (preserving the previous value) and is logged at Debug; it never
// fails the consumer.
type dlqDepthGauge struct {
	st      *state.State
	metrics *metrics.Registry
	log     *slog.Logger
}

// Refresh implements metrics.GaugeSource.
func (g *dlqDepthGauge) Refresh(ctx context.Context) {
	if g == nil || g.st == nil || g.metrics == nil {
		return
	}
	depth, err := g.st.ApproxDLQDepth(ctx)
	if err != nil {
		g.log.Debug("Metrics: DLQ depth estimate failed", "error", err)
		return
	}
	g.metrics.SetGauge(metrics.MetricDLQDepth, float64(depth))
}

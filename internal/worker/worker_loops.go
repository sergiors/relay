package worker

import (
	"context"
	"log/slog"

	"github.com/redis/go-redis/v9"

	"relay/internal/config"
	"relay/internal/state"
	"relay/internal/stream"
)

// startBackgroundLoops launches the worker-owned background loops and registers
// the loops shutdown barrier. Every loop is tracked by a done channel so the
// barrier can join it before the state DB, runtime manager, and Redis client
// close — a loop must never touch a closed resource.
//
// metricsLoggerDone is the metrics snapshot logger's done channel from
// startMetricsServer; it is nil when METRICS_ADDR is unset. The stats flush
// loop always runs because the registry always exists: persistent stats must
// keep flushing when HTTP exposition is disabled. Source-stream retention runs
// only when its configured window is positive; durable DLQ settlement
// reconciliation always runs, and durable DLQ retention runs only when its
// window is positive.
func startBackgroundLoops(
	ctx context.Context,
	logger *slog.Logger,
	cfg config.Config,
	client *redis.Client,
	st *state.State,
	statsFlusher *statsFlusher,
	metricsLoggerDone <-chan struct{},
	shutdown *shutdownRegistry,
) {
	var loopDones []<-chan struct{}
	if metricsLoggerDone != nil {
		loopDones = append(loopDones, metricsLoggerDone)
	}

	// Flush the registry into the state database on the fixed cadence. A nil
	// state handle parks the loop until shutdown (nothing to snapshot), keeping
	// shutdown ordering uniform.
	statsDone := make(chan struct{})
	loopDones = append(loopDones, statsDone)
	go func() {
		defer close(statsDone)
		statsLoop(ctx, statsFlusher, statsFlushInterval)
	}()

	// Optional internal stream retention (cfg.StreamRetention from
	// REDIS_STREAM_RETENTION, default 24h; unset/empty/zero/negative behavior is
	// resolved by config.Load): a single goroutine periodically trims the
	// configured stream with XTRIM MINID ~ ... ACKED so entries older than the
	// window are removed only once every consumer group has acknowledged them.
	// Completely separate from ACK/retry/DLQ semantics. On a server that does not
	// support ACKED (pre-8.2) the loop logs and disables itself rather than
	// trimming unsafely.
	if cfg.StreamRetention > 0 {
		retentionDone := make(chan struct{})
		loopDones = append(loopDones, retentionDone)
		go func() {
			defer close(retentionDone)
			retentionLoop(ctx, client, cfg.RedisStream, cfg.StreamRetention, logger)
		}()
	}

	// Bounded ACK/settlement reconciliation always runs: a crash between a
	// successful Redis ACK and the PostgreSQL settlement update leaves otherwise
	// undeletable rows behind, so this cannot be gated on DLQ_RETENTION.
	if st != nil && client != nil {
		settlementDone := make(chan struct{})
		loopDones = append(loopDones, settlementDone)
		go func() {
			defer close(settlementDone)
			dlqSettlementLoop(ctx, st, stream.NewDeliveryInspector(client), logger)
		}()
	}

	// Optional durable DLQ retention (cfg.DLQRetention from DLQ_RETENTION,
	// default 7 days; unset/empty/zero/negative behavior is resolved by
	// config.Load): a separate goroutine deletes DLQ rows older than the window
	// from PostgreSQL in bounded batches. It never touches the source stream and
	// never affects ACK-before-DLQ ordering.
	if cfg.DLQRetention > 0 && st != nil {
		dlqRetentionDone := make(chan struct{})
		loopDones = append(loopDones, dlqRetentionDone)
		go func() {
			defer close(dlqRetentionDone)
			dlqRetentionLoop(ctx, st, cfg.DLQRetention, logger)
		}()
	}

	// Join every worker-owned background loop before the resources they use
	// (state DB, runtime manager, Redis client) are closed. A barrier step: once
	// its bound expires it is cancelled and then joined for real, so a loop
	// cannot touch a closed resource even if it ignores cancellation promptly.
	shutdown.register(loopsBarrierStep(loopDones))
}

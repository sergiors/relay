package cron

import (
	"context"

	"relay/internal/observability/metrics"
	"relay/internal/state"
)

// outboxStatsReader is the READ-ONLY aggregate view of the durable outbox that
// the scheduler's periodic metrics source samples. *state.State satisfies it; a
// test fake can too. It is deliberately separate from Outbox so the durable
// store contract is unchanged and existing fakes need not implement an
// observability-only query.
type outboxStatsReader interface {
	PendingOutboxStats(ctx context.Context) (state.OutboxStats, error)
}

// The production durable outbox (the PostgreSQL state handle) must satisfy the
// read-only aggregate seam.
var _ outboxStatsReader = (*state.State)(nil)

// The Scheduler is a metrics.GaugeSource: the worker registers it with the
// stream consumer's periodic gauge refresher (Consumer.AddGaugeSource), so the
// outbox gauges are sampled on the ONE existing periodic observation pass with
// no separate ticker or goroutine.
var _ metrics.GaugeSource = (*Scheduler)(nil)

// Refresh samples the currently installed durable outbox's read-only aggregate
// and records the outbox depth and oldest-age gauges. It satisfies
// metrics.GaugeSource and is driven by the worker's existing periodic gauge
// refresher, not by a scheduler-owned loop.
//
// It reads through the Scheduler's CURRENT outbox (currentOutbox), so a
// scheduler-OWNED handle installed by the storage bootstrap — the fallback store
// opened when the worker's shared state handle was unavailable at startup — is
// observed exactly like the shared one. It deliberately does NOT reach for a
// shared/global state handle, which could be nil in that case.
//
// It is best-effort and READ-ONLY: it never mutates the outbox, never publishes,
// and never drives the scheduler degraded. A query failure is logged and leaves
// the gauges at their prior values (fail open, no false zero). With no outbox
// installed the outbox is UNKNOWN, not empty, so the gauges are likewise left
// stale rather than reported as zero. A nil receiver is a no-op, and every
// metric call is nil-safe.
func (s *Scheduler) Refresh(ctx context.Context) {
	if s == nil {
		return
	}
	ob := s.currentOutbox()
	if ob == nil {
		return
	}
	reader, ok := ob.(outboxStatsReader)
	if !ok {
		// An outbox implementation without the read-only aggregate (a test fake)
		// leaves the gauges at their prior values.
		return
	}
	stats, err := reader.PendingOutboxStats(ctx)
	if err != nil {
		s.log.Debug("Metrics: pending outbox stats failed", "error", err)
		return
	}
	s.metrics.SetGauge(metrics.MetricScheduleOutboxDepth, float64(stats.Depth))
	s.metrics.SetGauge(metrics.MetricScheduleOutboxOldestAge, stats.OldestAge.Seconds())
}

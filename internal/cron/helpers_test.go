package cron

import (
	"log/slog"

	"relay/internal/observability/metrics"
)

// testScheduler constructs a Scheduler with an installed deterministic outbox so
// schedule publication is enabled, matching the pre-storage-gate test behavior.
// Production always installs a real outbox or marks the scheduler unavailable;
// this helper exists only so standalone scheduling tests need not repeat the
// wiring. Tests that exercise the no-outbox or unavailable paths construct the
// scheduler with New/NewWithMetrics directly instead.
func testScheduler(pub Publisher, logger *slog.Logger) *Scheduler {
	s := New(pub, logger)
	s.SetOutbox(newFakeOutbox())
	return s
}

// testSchedulerWithMetrics is the metrics-recording counterpart of testScheduler.
func testSchedulerWithMetrics(pub Publisher, logger *slog.Logger, m *metrics.Registry) *Scheduler {
	s := NewWithMetrics(pub, logger, m)
	s.SetOutbox(newFakeOutbox())
	return s
}

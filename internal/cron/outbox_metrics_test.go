package cron

import (
	"context"
	"errors"
	"testing"
	"time"

	"relay/internal/observability/metrics"
	"relay/internal/state"
)

// TestSchedulerRefreshSamplesOutboxGauges pins that the scheduler's periodic
// metrics source records the installed outbox's depth/age and, on a query
// failure, preserves the prior values (fail open, no false zero) without
// degrading the scheduler.
func TestSchedulerRefreshSamplesOutboxGauges(t *testing.T) {
	ctx := context.Background()
	m := metrics.New()
	s := NewWithMetrics(newFakePublisher(1), testLogger(), m)
	ob := newFakeOutbox()
	ob.statsAgg = state.OutboxStats{Depth: 4, OldestAge: 3 * time.Hour}
	s.SetOutbox(ob)
	defer func() { _ = s.Stop(context.Background()) }()

	if got := m.GaugeLabels(metrics.MetricSchedulerState,
		[]metrics.Label{{Name: "state", Value: metrics.SchedulerStateRunning}}); got != 1 {
		t.Fatalf("setup: scheduler_state running = %v, want 1", got)
	}

	s.Refresh(ctx)

	if got := m.Gauge(metrics.MetricScheduleOutboxDepth); got != 4 {
		t.Fatalf("schedule_outbox_depth = %v, want 4", got)
	}
	if got := m.Gauge(metrics.MetricScheduleOutboxOldestAge); got != (3 * time.Hour).Seconds() {
		t.Fatalf("schedule_outbox_oldest_age_seconds = %v, want %v", got, (3 * time.Hour).Seconds())
	}

	// A failed aggregate query leaves both gauges at their prior values and must
	// never drive the scheduler degraded.
	ob.statsErr = errors.New("database is closed")
	s.Refresh(ctx)

	if got := m.Gauge(metrics.MetricScheduleOutboxDepth); got != 4 {
		t.Fatalf("schedule_outbox_depth after query error = %v, want 4 (preserved)", got)
	}
	if got := m.Gauge(metrics.MetricScheduleOutboxOldestAge); got != (3 * time.Hour).Seconds() {
		t.Fatalf("schedule_outbox_oldest_age_seconds after query error = %v, want preserved", got)
	}
	if s.Degraded() {
		t.Fatal("a metrics refresh failure must never degrade the scheduler")
	}
}

// TestSchedulerRefreshNoOutboxLeavesGaugesStale pins that with no outbox
// installed the outbox depth is UNKNOWN, so the gauges keep their prior values
// instead of being reported as zero.
func TestSchedulerRefreshNoOutboxLeavesGaugesStale(t *testing.T) {
	m := metrics.New()
	m.SetGauge(metrics.MetricScheduleOutboxDepth, 7)
	m.SetGauge(metrics.MetricScheduleOutboxOldestAge, 11)
	s := NewWithMetrics(newFakePublisher(1), testLogger(), m)
	defer func() { _ = s.Stop(context.Background()) }()

	s.Refresh(context.Background())

	if got := m.Gauge(metrics.MetricScheduleOutboxDepth); got != 7 {
		t.Fatalf("schedule_outbox_depth with no outbox = %v, want 7 (stale, never zero)", got)
	}
	if got := m.Gauge(metrics.MetricScheduleOutboxOldestAge); got != 11 {
		t.Fatalf("schedule_outbox_oldest_age_seconds with no outbox = %v, want 11 (stale)", got)
	}
}

// readerlessOutbox exposes the Outbox contract without the read-only aggregate
// seam, so the source's type-assert miss path is exercised.
type readerlessOutbox struct{ Outbox }

// TestSchedulerRefreshWithoutAggregateSeamLeavesGaugesStale pins that an outbox
// implementation lacking the read-only aggregate leaves the gauges untouched
// rather than panicking or inventing a value.
func TestSchedulerRefreshWithoutAggregateSeamLeavesGaugesStale(t *testing.T) {
	m := metrics.New()
	m.SetGauge(metrics.MetricScheduleOutboxDepth, 2)
	s := NewWithMetrics(newFakePublisher(1), testLogger(), m)
	s.SetOutbox(readerlessOutbox{newFakeOutbox()})
	defer func() { _ = s.Stop(context.Background()) }()

	s.Refresh(context.Background())

	if got := m.Gauge(metrics.MetricScheduleOutboxDepth); got != 2 {
		t.Fatalf("schedule_outbox_depth = %v, want 2 (unchanged without the seam)", got)
	}
}

// TestCatchUpCountsSkippedOccurrences pins the skipped-occurrence counter: the
// bounded latest-only catch-up counts, per schedule, the older missed
// occurrences it intentionally discards (everything in the recovery horizon
// strictly older than the single latest occurrence). A schedule with only its
// latest occurrence in the window contributes zero, and the counter is
// independent of the published latest occurrence.
func TestCatchUpCountsSkippedOccurrences(t *testing.T) {
	now := time.Date(2026, 7, 2, 10, 2, 0, 0, time.UTC)
	m := metrics.New()
	fp := newFakePublisher(8)
	s := testSchedulerWithMetrics(fp, testLogger(), m)
	s.now = func() time.Time { return now }
	s.wait = noWait
	defer func() { _ = s.Stop(context.Background()) }()

	// Daily 03:00: only the latest (today 03:00) is inside the window -> 0.
	s.ReplaceApp("fn", schedTemplate("jobs.a", "0 3 * * *", "", ""))
	// Every 5 minutes: 288 occurrences in the window, latest-only publishes the
	// newest, so 287 are discarded.
	s.ReplaceApp("fn2", schedTemplate("jobs.b", "*/5 * * * *", "", ""))
	// Daily 23:00: only yesterday 23:00 is inside the window -> 0.
	s.ReplaceApp("fn3", schedTemplate("jobs.c", "0 23 * * *", "", ""))

	if n := s.CatchUp(context.Background()); n != 3 {
		t.Fatalf("catch-up published = %d, want 3", n)
	}
	if got := m.Counter(metrics.MetricScheduleCatchUp); got != 3 {
		t.Fatalf("catch-up counter = %d, want 3", got)
	}
	if got := m.Counter(metrics.MetricScheduleCatchupSkipped); got != 287 {
		t.Fatalf("catch-up skipped counter = %d, want 287", got)
	}
}

// TestCatchUpSkippedCountedPerScan pins that each scan counts the instants IT
// discards: a recovery scan re-running the bounded latest-only policy counts its
// skips again (a local per-scan count, not a cluster-wide de-duplicated total).
func TestCatchUpSkippedCountedPerScan(t *testing.T) {
	now := time.Date(2026, 7, 2, 10, 2, 0, 0, time.UTC)
	m := metrics.New()
	fp := newFakePublisher(8)
	s := testSchedulerWithMetrics(fp, testLogger(), m)
	s.now = func() time.Time { return now }
	s.wait = noWait
	defer func() { _ = s.Stop(context.Background()) }()

	s.ReplaceApp("fn", schedTemplate("jobs.b", "*/5 * * * *", "", ""))

	s.recoveryCatchUp(context.Background())
	if got := m.Counter(metrics.MetricScheduleCatchupSkipped); got != 287 {
		t.Fatalf("after one recovery scan: skipped = %d, want 287", got)
	}
	s.recoveryCatchUp(context.Background())
	if got := m.Counter(metrics.MetricScheduleCatchupSkipped); got != 574 {
		t.Fatalf("after two recovery scans: skipped = %d, want 574", got)
	}
}

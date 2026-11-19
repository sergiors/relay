package worker

import (
	"context"
	"testing"

	"relay/internal/observability/metrics"
	"relay/internal/state"
)

// TestStatsFlusherResetWithoutStateErrors verifies a worker whose state handle
// is unavailable reports an error (so the socket answers stats_reset_failed and
// the CLI surfaces it instead of masking it) AND leaves the effective baseline
// untouched: no reset happened, so the persisted snapshot must keep reporting
// the original counters rather than a reset point that was never committed.
func TestStatsFlusherResetWithoutStateErrors(t *testing.T) {
	m := metrics.New()
	m.Add(metrics.MetricEventsMatched, 42)
	f := newStatsFlusher(nil, m)

	if err := f.ResetStats(); err == nil {
		t.Fatal("ResetStats without a state handle: err = nil, want error")
	}
	// Prometheus stays monotonic and the baseline was NOT captured: the
	// baselined snapshot still reports the full raw counter.
	if got := m.Counter(metrics.MetricEventsMatched); got != 42 {
		t.Fatalf("Prometheus events after failed reset = %d, want 42 (monotonic)", got)
	}
	if got := f.baseline.counter(metrics.MetricEventsMatched, m.Counter(metrics.MetricEventsMatched)); got != 42 {
		t.Fatalf("baselined events after failed reset = %d, want 42 (baseline untouched)", got)
	}
	// The failed reset must not adopt any epoch either.
	if f.generation != state.StatsGenerationUnknown || f.nodeGeneration != state.StatsGenerationUnknown {
		t.Fatalf("epochs after failed reset = (%d, %d), want both unknown",
			f.generation, f.nodeGeneration)
	}
}

// TestRelayBaselineSuppressesPreResetTimestamps pins the timestamp half of the
// worker-owned reset baseline: timestamps are last-observed, not cumulative, so
// a value not advanced since the reset must be reported as "never observed",
// while a post-reset execution (a newer timestamp) resumes reporting. Unlike a
// cumulative counter, subtraction cannot express this.
func TestRelayBaselineSuppressesPreResetTimestamps(t *testing.T) {
	m := metrics.New()
	m.IncLabels(metrics.MetricAppEventsMatched, []metrics.Label{{Name: "app", Value: "alpha"}})
	m.SetAppTimestamp("alpha", metrics.AppTimestampExecution, 1700000000)
	base := captureRelayBaseline(m)

	// The pre-reset timestamp is suppressed to zero.
	got := snapshotAppStats(m, &base)
	if len(got) != 1 || got[0].LastExecutionAt != "" {
		t.Fatalf("pre-reset timestamp not suppressed: %+v", got)
	}

	// A post-reset execution advances the timestamp past the baseline and is
	// reported again (as RFC3339), together with the post-reset counter value.
	m.IncLabels(metrics.MetricAppEventsMatched, []metrics.Label{{Name: "app", Value: "alpha"}})
	m.SetAppTimestamp("alpha", metrics.AppTimestampExecution, 1700000100)
	got = snapshotAppStats(m, &base)
	if len(got) != 1 || got[0].EventsMatchedTotal != 1 {
		t.Fatalf("post-reset counters = %+v, want events 1", got)
	}
	if got[0].LastExecutionAt != unixSecToRFC3339(1700000100) {
		t.Fatalf("post-reset timestamp = %q, want %q", got[0].LastExecutionAt, unixSecToRFC3339(1700000100))
	}
}

// TestStatsFlusherDropAppBaseline verifies the removal hook: after a reset,
// dropping an app's baseline entry lets a re-added app report its fresh
// series value instead of a negative (fresh minus stale pre-removal total).
func TestStatsFlusherDropAppBaseline(t *testing.T) {
	m := metrics.New()
	m.AddLabels(metrics.MetricAppEventsMatched, []metrics.Label{{Name: "app", Value: "alpha"}}, 10)
	f := newStatsFlusher(nil, m)
	// Install the reset baseline directly: this unit test has no state DB, and
	// the baseline only ever lands after a SUCCESSFUL state reset (see
	// ResetStats), which the nil handle can never provide.
	f.baseline = captureRelayBaseline(m)

	// A fresh series (simulating the registry's removal + re-add) at 1 must be
	// offset by the stale baseline (10) until the hook drops it.
	m.RemoveApp("alpha")
	m.IncLabels(metrics.MetricAppEventsMatched, []metrics.Label{{Name: "app", Value: "alpha"}})
	if got := snapshotAppStats(m, &f.baseline); got[0].EventsMatchedTotal != -9 {
		t.Fatalf("without the removal hook, events = %d, want -9 (stale baseline)", got[0].EventsMatchedTotal)
	}

	f.dropAppBaseline("alpha")
	if got := snapshotAppStats(m, &f.baseline); got[0].EventsMatchedTotal != 1 {
		t.Fatalf("after the removal hook, events = %d, want 1", got[0].EventsMatchedTotal)
	}
}

// TestStatsFlusherNilSafe verifies a nil flusher never panics.
func TestStatsFlusherNilSafe(t *testing.T) {
	var f *statsFlusher
	f.flush(context.Background())
	_ = f.ResetStats()
}

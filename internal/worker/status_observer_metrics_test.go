//go:build integration

package worker

import (
	"context"
	"testing"
	"time"

	"relay/internal/observability/metrics"
	"relay/internal/state"
)

// TestWireStatusObserverProjectsTransitions pins the worker's state→metrics
// bridge: discovery moves the one-hot status to preparing, a success to ready,
// and a removal deletes the app's status series.
func TestWireStatusObserverProjectsTransitions(t *testing.T) {
	st := openTempState(t)
	reg := metrics.New()
	wireStatusObserver(st, reg)

	fn := stateApp("alpha", t.TempDir())
	fencingToken := seedFencingToken(t, st, fn)

	// Discovery -> preparing (one-hot: preparing 1, others 0).
	if got := reg.GaugeLabels(metrics.MetricAppStatus, []metrics.Label{{Name: "app", Value: "alpha"}, {Name: "status", Value: state.StatusPreparing}}); got != 1 {
		t.Fatalf("preparing = %v, want 1", got)
	}
	if got := reg.GaugeLabels(metrics.MetricAppStatus, []metrics.Label{{Name: "app", Value: "alpha"}, {Name: "status", Value: state.StatusReady}}); got != 0 {
		t.Fatalf("ready = %v, want 0", got)
	}

	// Success -> ready.
	if _, err := st.RecordReconcileSuccess(context.Background(), "alpha", "img", "fp", time.Now(), fn, fencingToken); err != nil {
		t.Errorf("state write error: %v", err)
	}
	if got := reg.GaugeLabels(metrics.MetricAppStatus, []metrics.Label{{Name: "app", Value: "alpha"}, {Name: "status", Value: state.StatusReady}}); got != 1 {
		t.Fatalf("ready after success = %v, want 1", got)
	}

	// Removal -> all status series deleted. Check the snapshot BEFORE any
	// GaugeLabels read: the read-back creates a series as a side effect.
	if _, err := st.RecordRemoved(context.Background(), "alpha", fencingToken); err != nil {
		t.Errorf("state write error: %v", err)
	}
	series := reg.Snapshot()
	for _, s := range []string{"preparing", "building", "reconciling", "ready", "degraded", "unavailable"} {
		if containsLabel(series, "app_status{app=alpha,status="+s+"}") {
			t.Fatalf("alpha status %q must be deleted:\n%s", s, series)
		}
	}
}

// TestWireStatusObserverNilSafe pins that no state handle (or nil registry) is a
// silent no-op rather than a panic.
func TestWireStatusObserverNilSafe(t *testing.T) {
	wireStatusObserver(nil, metrics.New())

	st := openTempState(t)
	wireStatusObserver(st, nil)
	fn := stateApp("beta", t.TempDir())
	if _, err := st.RecordDiscoveredWithFingerprint(context.Background(), fn, "fp"); err != nil {
		t.Errorf("state write error: %v", err)
	} // must not panic
}

// TestStatsResetDoesNotTouchAppStatus pins that `reset stats` (the
// worker-owned subtraction baseline) leaves the one-hot status gauge untouched:
// status is a current-state gauge, not a cumulative total, so it must never be
// reset or baselined. The Prometheus counters stay monotonic too.
func TestStatsResetDoesNotTouchAppStatus(t *testing.T) {
	st := openTempState(t)
	reg := metrics.New()
	wireStatusObserver(st, reg)
	fn := stateApp("alpha", t.TempDir())
	fencingToken := seedFencingToken(t, st, fn)
	if _, err := st.RecordReconcileSuccess(context.Background(), "alpha", "img", "fp", time.Now(), fn, fencingToken); err != nil {
		t.Errorf("state write error: %v", err)
	}

	reg.Add(metrics.MetricEventsMatched, 100)
	f := newStatsFlusher(st, reg)
	if err := f.ResetStats(); err != nil {
		t.Fatalf("ResetStats: %v", err)
	}

	if got := reg.GaugeLabels(metrics.MetricAppStatus, []metrics.Label{{Name: "app", Value: "alpha"}, {Name: "status", Value: state.StatusReady}}); got != 1 {
		t.Fatalf("app_status ready after reset = %v, want 1 (reset must not touch status)", got)
	}
	if got := reg.Counter(metrics.MetricEventsMatched); got != 100 {
		t.Fatalf("events after reset = %d, want 100 (Prometheus monotonic)", got)
	}
}

// containsLabel avoids importing strings for a single substring check.
func containsLabel(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

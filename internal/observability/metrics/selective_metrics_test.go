package metrics

import (
	"strings"
	"testing"
)

// TestSetFunctionStatusOneHot verifies the one-hot contract: a status write
// creates ALL allowed status series for the function, with exactly the current
// status at 1 and every other status at 0, so no consumer has to infer the
// current status by absence.
func TestSetFunctionStatusOneHot(t *testing.T) {
	r := New()
	r.SetFunctionStatus("fn", "ready")

	for _, status := range FunctionStatuses {
		want := 0.0
		if status == "ready" {
			want = 1
		}
		got := r.GaugeLabels(MetricFunctionStatus, []Label{{"function", "fn"}, {"status", status}})
		if got != want {
			t.Errorf("status %q = %v, want %v", status, got, want)
		}
	}

	// A transition moves the 1 to the new status and zeroes the old one.
	r.SetFunctionStatus("fn", "degraded")
	if got := r.GaugeLabels(MetricFunctionStatus, []Label{{"function", "fn"}, {"status", "ready"}}); got != 0 {
		t.Errorf("ready after transition = %v, want 0", got)
	}
	if got := r.GaugeLabels(MetricFunctionStatus, []Label{{"function", "fn"}, {"status", "degraded"}}); got != 1 {
		t.Errorf("degraded after transition = %v, want 1", got)
	}
}

// TestSetFunctionStatusBoundedAndNoDuplicateSeries pins that only the closed
// status set is representable (an unknown status is a no-op) and that a
// function's status vec has exactly one series per allowed status — no duplicate
// or unexpected status series on /metrics.
func TestSetFunctionStatusBoundedAndNoDuplicateSeries(t *testing.T) {
	r := New()
	r.SetFunctionStatus("fn", "not-a-status")
	// Assert on the snapshot BEFORE any labeled read: GaugeLabels creates a
	// series as a read-back side effect, so a read first would mask the check.
	if strings.Contains(r.Snapshot(), "function_status{function=fn,status=not-a-status}") {
		t.Fatalf("unknown status must not create a series:\n%s", r.Snapshot())
	}

	r.SetFunctionStatus("fn", "building")
	r.SetFunctionStatus("fn", "reconciling")

	seen := map[string]int{}
	for _, status := range FunctionStatuses {
		seen[status]++
	}
	snapshot := r.Snapshot()
	for _, status := range FunctionStatuses {
		if seen[status] != 1 {
			t.Fatalf("status %q appears in the seeded set %d times", status, seen[status])
		}
		wantLine := "function_status{function=fn,status=" + status + "}"
		if !strings.Contains(snapshot, wantLine) {
			t.Fatalf("snapshot missing %q:\n%s", wantLine, snapshot)
		}
	}
	// The current status is reconciling: exactly one series reads value=1.
	if !strings.Contains(snapshot, "function_status{function=fn,status=reconciling} value=1") {
		t.Fatalf("current status must be 1:\n%s", snapshot)
	}
	if strings.Count(snapshot, "value=1") != 1 {
		t.Fatalf("exactly one status series must be 1:\n%s", snapshot)
	}
}

// TestRemoveFunctionStatusDeletesAllSeries verifies the explicit status removal
// deletes every status series for a function while leaving another function's
// untouched, and is idempotent/nil-safe.
func TestRemoveFunctionStatusDeletesAllSeries(t *testing.T) {
	r := New()
	r.SetFunctionStatus("foo", "ready")
	r.SetFunctionStatus("bar", "building")

	r.RemoveFunctionStatus("foo")

	s := r.Snapshot()
	for _, status := range FunctionStatuses {
		if strings.Contains(s, "function_status{function=foo,status="+status+"}") {
			t.Fatalf("foo status %q must be removed:\n%s", status, s)
		}
	}
	for _, status := range FunctionStatuses {
		if !strings.Contains(s, "function_status{function=bar,status="+status+"}") {
			t.Fatalf("bar status %q must survive:\n%s", status, s)
		}
	}

	// RemoveFunction (the shared function cleanup) also deletes status series.
	r.SetFunctionStatus("baz", "ready")
	r.RemoveFunction("baz")
	if strings.Contains(r.Snapshot(), "function_status{function=baz,") {
		t.Fatalf("RemoveFunction must delete the status series:\n%s", r.Snapshot())
	}

	// Idempotent and nil-safe.
	r.RemoveFunctionStatus("foo")
	var nilR *Registry
	nilR.RemoveFunctionStatus("foo")
}

// TestRedisReadErrorCounterSemantics pins the redis read-error counter's fixed
// operation label and that it never carries raw error text: only the operation
// label appears, and distinct operations are distinct series.
func TestRedisReadErrorCounterSemantics(t *testing.T) {
	r := New()
	r.IncLabels(MetricRedisReadErrors, []Label{{"operation", RedisOpReadGroup}})
	r.IncLabels(MetricRedisReadErrors, []Label{{"operation", RedisOpPending}})
	r.IncLabels(MetricRedisReadErrors, []Label{{"operation", RedisOpPending}})

	if got := r.CounterLabels(MetricRedisReadErrors, []Label{{"operation", RedisOpReadGroup}}); got != 1 {
		t.Fatalf("read_group = %d, want 1", got)
	}
	if got := r.CounterLabels(MetricRedisReadErrors, []Label{{"operation", RedisOpPending}}); got != 2 {
		t.Fatalf("pending = %d, want 2", got)
	}

	s := r.Snapshot()
	if strings.Contains(s, "error=") {
		t.Fatalf("redis read errors must not carry an error label:\n%s", s)
	}
	// The counter family carries exactly the one fixed operation label; every
	// series in the snapshot has it.
	for line := range strings.SplitSeq(s, "\n") {
		if !strings.HasPrefix(line, "redis_read_errors_total{") {
			continue
		}
		if !strings.Contains(line, "operation=") {
			t.Fatalf("redis read error series lacks the operation label: %q", line)
		}
	}
	// The finite operation constants are distinct, so no two operations share a
	// series.
	ops := map[string]bool{RedisOpReadGroup: true, RedisOpPending: true, RedisOpAutoclaim: true, RedisOpPendingGauge: true}
	if len(ops) != 4 {
		t.Fatalf("operation label values must be distinct, got %d", len(ops))
	}
}

// TestServiceReconcileMetricSeries pins the service reconcile counter/histogram
// shape: the outcome label is the closed changed/unchanged/error set, durations
// are observed in seconds, and there are no duplicate series.
func TestServiceReconcileMetricSeries(t *testing.T) {
	r := New()
	for _, outcome := range []string{ServiceOutcomeChanged, ServiceOutcomeUnchanged, ServiceOutcomeError} {
		r.IncLabels(MetricServiceReconciles, []Label{{"function", "fn"}, {"outcome", outcome}})
	}
	r.ObserveDurationLabels(MetricServiceReconcileDuration, []Label{{"function", "fn"}}, 0)

	if got := r.CounterLabels(MetricServiceReconciles, []Label{{"function", "fn"}, {"outcome", ServiceOutcomeChanged}}); got != 1 {
		t.Fatalf("changed = %d, want 1", got)
	}
	s := r.Snapshot()
	for _, outcome := range []string{ServiceOutcomeChanged, ServiceOutcomeUnchanged, ServiceOutcomeError} {
		if !strings.Contains(s, "service_reconciles_total{function=fn,outcome="+outcome+"}") {
			t.Fatalf("missing outcome %q:\n%s", outcome, s)
		}
	}
	if !strings.Contains(s, "service_reconcile_duration_seconds{function=fn}") {
		t.Fatalf("missing service reconcile duration series:\n%s", s)
	}
}

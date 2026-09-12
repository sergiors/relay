package metrics

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// seedFunction increments every function-carrying vec for name, so a function
// has a series in each of the nine functionMetrics collectors. handler duration
// and function build are observed as histograms; handler_invocations_total gets
// both success and failure outcomes.
func seedFunction(r *Registry, name string) {
	r.IncLabels("handler_invocations_total", []Label{{"outcome", "success"}, {"function", name}, {"handler", "x"}})
	r.IncLabels("handler_invocations_total", []Label{{"outcome", "failure"}, {"function", name}, {"handler", "x"}})
	r.IncLabels("build_failures_total", []Label{{"function", name}})
	r.IncLabels("function_events_total", []Label{{"function", name}})
	r.IncLabels("function_handler_success_total", []Label{{"function", name}})
	r.IncLabels("function_handler_failure_total", []Label{{"function", name}})
	r.IncLabels("function_retries_total", []Label{{"function", name}})
	r.IncLabels("function_dlq_total", []Label{{"function", name}})
	r.ObserveDurationLabels("handler_duration_seconds", []Label{{"function", name}, {"handler", "x"}}, time.Millisecond)
	r.ObserveDurationLabels("function_build_seconds", []Label{{"function", name}}, time.Millisecond)
}

// seriesPresent reports whether the snapshot contains a series whose rendered
// name carries the given metric name and the function label value. sampleName
// sorts labels by name, so the function label may be followed by another label
// (",other=..."), by the closing brace ("}>"), or be the entire label set
// ("} count=...") — the check matches the function label token with any of those
// following contexts.
func seriesPresent(t *testing.T, snapshot, metric, name string) bool {
	t.Helper()
	token := "function=" + name
	for line := range strings.SplitSeq(snapshot, "\n") {
		if !strings.HasPrefix(line, metric+"{") {
			continue
		}
		idx := strings.Index(line, token)
		if idx < 0 {
			continue
		}
		rest := line[idx+len(token):]
		// The token must be a full label value, not a prefix of another value:
		// it is followed by ',' (more labels), '}' (end of labels), or nothing.
		if strings.HasPrefix(rest, ",") || strings.HasPrefix(rest, "}") || rest == "" {
			return true
		}
	}
	return false
}

// assertNoFunctionSeries asserts no series across any of the nine
// functionMetrics vecs carries function=name.
func assertNoFunctionSeries(t *testing.T, r *Registry, name string) {
	t.Helper()
	s := r.Snapshot()
	for _, m := range functionMetrics {
		if seriesPresent(t, s, m, name) {
			t.Fatalf("expected no %s series for %q, but found one in:\n%s", m, name, s)
		}
	}
}

// assertFunctionSeries asserts every one of the nine vecs has a series for name.
func assertFunctionSeries(t *testing.T, r *Registry, name string) {
	t.Helper()
	s := r.Snapshot()
	for _, m := range functionMetrics {
		if !seriesPresent(t, s, m, name) {
			t.Fatalf("expected %s series for %q, but missing in:\n%s", m, name, s)
		}
	}
}

// seedGlobals sets distinctive global counters/gauges and returns their total
// values so a test can assert they never change under function cleanup.
func seedGlobals(r *Registry) map[string]int64 {
	r.Inc("events_processed_total")
	r.SeedCounter("events_processed_total", 4)
	r.Inc("dlq_entries_total")
	r.Inc("dlq_entries_total")
	r.Inc("retries_total")
	r.Inc("handler_success_total")
	r.Inc("handler_success_total")
	r.Inc("handler_success_total")
	r.Inc("handler_failure_total")
	r.SetGauge("pending_entries", 9)
	return map[string]int64{
		"events_processed_total": 5, // 1 + 4 via SeedCounter
		"dlq_entries_total":      2,
		"retries_total":          1,
		"handler_success_total":  3,
		"handler_failure_total":  1,
	}
}

func assertGlobalTotals(t *testing.T, r *Registry, want map[string]int64) {
	t.Helper()
	for name, wantVal := range want {
		if got := r.Counter(name); got != wantVal {
			t.Fatalf("global %s = %d, want %d", name, got, wantVal)
		}
	}
	if got := r.Gauge("pending_entries"); got != 9 {
		t.Fatalf("global pending_entries = %v, want 9", got)
	}
}

// Series exist for a function across all nine function-carrying vecs while the
// function exists, with a second function's series isolated alongside.
func TestFunctionSeriesExistWhileFunctionExists(t *testing.T) {
	r := New()
	seedFunction(r, "foo")
	seedFunction(r, "bar")
	assertFunctionSeries(t, r, "foo")
	assertFunctionSeries(t, r, "bar")
}

// RemoveFunction removes exactly foo's series across all nine vecs, leaves
// bar's series untouched in each, and leaves every global metric unchanged.
func TestRemoveFunctionDeletesOnlyFunctionSeries(t *testing.T) {
	r := New()
	seedFunction(r, "foo")
	seedFunction(r, "bar")
	globals := seedGlobals(r)

	r.RemoveFunction("foo")

	assertGlobalTotals(t, r, globals)
	assertNoFunctionSeries(t, r, "foo")
	// bar's series survive in every vec.
	assertFunctionSeries(t, r, "bar")
}

// RemoveFunction on handler_invocations_total must delete BOTH outcome series
// for foo (DeletePartialMatch path) while leaving bar's intact.
func TestRemoveFunctionDeletesBothHandlerInvocationsOutcomes(t *testing.T) {
	r := New()
	r.IncLabels("handler_invocations_total", []Label{{"outcome", "success"}, {"function", "foo"}, {"handler", "x"}})
	r.IncLabels("handler_invocations_total", []Label{{"outcome", "failure"}, {"function", "foo"}, {"handler", "x"}})
	r.IncLabels("handler_invocations_total", []Label{{"outcome", "success"}, {"function", "bar"}, {"handler", "x"}})
	r.IncLabels("handler_invocations_total", []Label{{"outcome", "failure"}, {"function", "bar"}, {"handler", "x"}})

	r.RemoveFunction("foo")

	s := r.Snapshot()
	if seriesPresent(t, s, "handler_invocations_total", "foo") {
		t.Fatalf("foo handler_invocations_total must be removed:\n%s", s)
	}
	if !seriesPresent(t, s, "handler_invocations_total", "bar") {
		t.Fatalf("bar handler_invocations_total must survive:\n%s", s)
	}
	if strings.Contains(s, "outcome=success} count=2") || strings.Contains(s, "outcome=failure} count=2") {
		t.Fatalf("foo's outcome counts must not linger:\n%s", s)
	}
}

// RemoveFunction on handler_duration_seconds must delete foo's multi-label
// histogram series (DeleteLabelValues path) while leaving bar's intact.
func TestRemoveFunctionDeletesHandlerDurationMultiLabel(t *testing.T) {
	r := New()
	r.ObserveDurationLabels("handler_duration_seconds", []Label{{"function", "foo"}, {"handler", "x"}}, time.Millisecond)
	r.ObserveDurationLabels("handler_duration_seconds", []Label{{"function", "bar"}, {"handler", "x"}}, time.Millisecond)

	r.RemoveFunction("foo")

	s := r.Snapshot()
	if seriesPresent(t, s, "handler_duration_seconds", "foo") {
		t.Fatalf("foo handler_duration_seconds must be removed:\n%s", s)
	}
	if !seriesPresent(t, s, "handler_duration_seconds", "bar") {
		t.Fatalf("bar handler_duration_seconds must survive:\n%s", s)
	}
}

// After removal, re-incrementing foo creates a FRESH series at the new value,
// not the pre-removal one.
func TestRemoveFunctionReAddStartsFreshCount(t *testing.T) {
	r := New()
	r.IncLabels("function_events_total", []Label{{"function", "foo"}})
	r.IncLabels("function_events_total", []Label{{"function", "foo"}})
	r.RemoveFunction("foo")

	r.IncLabels("function_events_total", []Label{{"function", "foo"}})
	s := r.Snapshot()
	if !strings.Contains(s, "function_events_total{function=foo} count=1") {
		t.Fatalf("re-added foo must count 1, not the stale value; got:\n%s", s)
	}
}

// Nil-receiver RemoveFunction and SweepFunctionMetrics must not panic.
func TestFunctionCleanupNilReceiverNoPanic(t *testing.T) {
	var r *Registry
	r.RemoveFunction("foo")
	r.SweepFunctionMetrics(map[string]bool{"bar": true})
}

// SweepFunctionMetrics deletes series for functions absent from the live set,
// keeps live ones, leaves globals untouched, and a nil/empty map removes all
// function series but never globals.
func TestSweepFunctionMetrics(t *testing.T) {
	r := New()
	seedFunction(r, "keep")
	seedFunction(r, "remove")
	globals := seedGlobals(r)

	r.SweepFunctionMetrics(map[string]bool{"keep": true})

	assertGlobalTotals(t, r, globals)
	assertNoFunctionSeries(t, r, "remove")
	if !seriesPresent(t, r.Snapshot(), "function_events_total", "keep") {
		t.Fatal("keep's series must survive the sweep")
	}

	// A nil/empty live map removes every function series but never globals.
	r.SweepFunctionMetrics(map[string]bool{})
	assertNoFunctionSeries(t, r, "keep")
	assertGlobalTotals(t, r, globals)
}

// Race: writers increment foo's function_* vecs while deleters call
// RemoveFunction and SweepFunctionMetrics, concurrent with Snapshot /
// FunctionStatsSnapshot readers. After writers+deleters stop, a final sweep must
// leave foo absent with no permanent stale recreation, bar intact, and globals
// exactly equal to their seeded totals.
func TestRemoveFunctionConcurrentWithWriters(t *testing.T) {
	r := New()
	seedFunction(r, "bar")
	globals := seedGlobals(r)

	const writers = 4
	const iters = 500
	var wg sync.WaitGroup

	// Writers: repeatedly create foo's function-scoped series.
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				r.IncLabels("function_events_total", []Label{{"function", "foo"}})
				r.IncLabels("function_handler_success_total", []Label{{"function", "foo"}})
				r.ObserveDurationLabels("handler_duration_seconds", []Label{{"function", "foo"}, {"handler", "x"}}, time.Millisecond)
			}
		}()
	}

	// Deleters: keep removing foo and sweeping (foo not live, bar live).
	stop := make(chan struct{})
	var delWG sync.WaitGroup
	for i := 0; i < 2; i++ {
		delWG.Add(1)
		go func() {
			defer delWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
					r.RemoveFunction("foo")
					r.SweepFunctionMetrics(map[string]bool{"bar": true})
					r.Snapshot()
					r.FunctionStatsSnapshot()
				}
			}
		}()
	}

	wg.Wait()
	close(stop)
	delWG.Wait()

	// Convergence: after all writers stop, sweep repeatedly (bounded) until foo
	// no longer appears in the function_stats view.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r.SweepFunctionMetrics(map[string]bool{"bar": true})
		if !anyFunctionStat(r, "foo") {
			break
		}
		time.Sleep(time.Millisecond)
	}
	// One final sweep removes everything remaining.
	r.SweepFunctionMetrics(map[string]bool{"bar": true})

	if anyFunctionStat(r, "foo") {
		t.Fatalf("foo must be absent from FunctionStatsSnapshot after convergence:\n%+v", r.FunctionStatsSnapshot())
	}
	assertNoFunctionSeries(t, r, "foo")
	if !seriesPresent(t, r.Snapshot(), "function_events_total", "bar") {
		t.Fatal("bar's series must survive the concurrent cleanup")
	}
	// Globals equal their exact seeded totals.
	assertGlobalTotals(t, r, globals)
}

// anyFunctionStat reports whether snapshot has a FunctionStat for name.
func anyFunctionStat(r *Registry, name string) bool {
	for _, fs := range r.FunctionStatsSnapshot() {
		if fs.Function == name {
			return true
		}
	}
	return false
}

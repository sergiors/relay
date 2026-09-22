package metrics

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// seedFunction increments every function-carrying vec for name, so a function
// has a series in each of the functionMetrics collectors. handler duration and
// function build are observed as histograms; handler_invocations_total gets
// both success and failure outcomes; the runtime pool vecs get one series each.
func seedFunction(r *Registry, name string) {
	r.IncLabels(MetricHandlerInvocations, []Label{{"outcome", "success"}, {"function", name}, {"handler", "x"}})
	r.IncLabels(MetricHandlerInvocations, []Label{{"outcome", "failure"}, {"function", name}, {"handler", "x"}})
	r.IncLabels(MetricBuildFailures, []Label{{"function", name}})
	r.IncLabels(MetricFunctionEventsMatched, []Label{{"function", name}})
	r.IncLabels(MetricFunctionHandlerSuccess, []Label{{"function", name}})
	r.IncLabels(MetricFunctionHandlerFailure, []Label{{"function", name}})
	r.IncLabels(MetricFunctionRetries, []Label{{"function", name}})
	r.IncLabels(MetricFunctionDLQ, []Label{{"function", name}})
	r.ObserveDurationLabels(MetricHandlerDuration, []Label{{"function", name}, {"handler", "x"}}, time.Millisecond)
	r.ObserveDurationLabels(MetricFunctionBuild, []Label{{"function", name}}, time.Millisecond)
	r.SetGaugeLabels(MetricRuntimeContainers, []Label{{"function", name}, {"state", RuntimeStateIdle}}, 1)
	r.SetGaugeLabels(MetricRuntimePoolCapacity, []Label{{"function", name}}, 2)
	r.IncLabels(MetricRuntimeContainerAcquires, []Label{{"function", name}, {"outcome", RuntimeOutcomeCold}})
	r.IncLabels(MetricRuntimeContainerDiscards, []Label{{"function", name}, {"reason", "shutdown"}})
	r.ObserveDurationLabels(MetricRuntimeContainerAcquireDuration, []Label{{"function", name}}, time.Millisecond)
	r.IncLabels(MetricRuntimeContainerWaits, []Label{{"function", name}})
}

// seriesPresent reports whether the snapshot contains a series whose rendered
// name carries the given metric name and the function label value. The metric
// argument is the canonical (relay_-prefixed) name; the snapshot renders
// display names (prefix stripped, see sampleName), so the prefix is trimmed
// before prefixing the line. sampleName sorts labels by name, so the function
// label may be followed by another label (",other=..."), by the closing brace
// ("}>"), or be the entire label set ("} count=...") — the check matches the
// function label token with any of those following contexts.
func seriesPresent(t *testing.T, snapshot, metric, name string) bool {
	t.Helper()
	token := "function=" + name
	for line := range strings.SplitSeq(snapshot, "\n") {
		if !strings.HasPrefix(line, metricDisplayName(metric)+"{") {
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

// assertNoFunctionSeries asserts no series across any of the functionMetrics
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

// assertFunctionSeries asserts every one of the functionMetrics vecs has a series for name.
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
	r.Inc(MetricEventsMatched)
	r.SeedCounter(MetricEventsMatched, 4)
	r.Inc(MetricDLQEntries)
	r.Inc(MetricDLQEntries)
	r.Inc(MetricRetries)
	r.Inc(MetricHandlerSuccess)
	r.Inc(MetricHandlerSuccess)
	r.Inc(MetricHandlerSuccess)
	r.Inc(MetricHandlerFailure)
	r.SetGauge(MetricPendingEntries, 9)
	return map[string]int64{
		MetricEventsMatched:  5, // 1 + 4 via SeedCounter
		MetricDLQEntries:     2,
		MetricRetries:        1,
		MetricHandlerSuccess: 3,
		MetricHandlerFailure: 1,
	}
}

func assertGlobalTotals(t *testing.T, r *Registry, want map[string]int64) {
	t.Helper()
	for name, wantVal := range want {
		if got := r.Counter(name); got != wantVal {
			t.Fatalf("global %s = %d, want %d", name, got, wantVal)
		}
	}
	if got := r.Gauge(MetricPendingEntries); got != 9 {
		t.Fatalf("global pending_entries = %v, want 9", got)
	}
}

// Series exist for a function across all functionMetrics vecs while the
// function exists, with a second function's series isolated alongside.
func TestFunctionSeriesExistWhileFunctionExists(t *testing.T) {
	r := New()
	seedFunction(r, "foo")
	seedFunction(r, "bar")
	assertFunctionSeries(t, r, "foo")
	assertFunctionSeries(t, r, "bar")
}

// RemoveFunction removes exactly foo's series across all functionMetrics vecs, leaves
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
	r.IncLabels(MetricHandlerInvocations, []Label{{"outcome", "success"}, {"function", "foo"}, {"handler", "x"}})
	r.IncLabels(MetricHandlerInvocations, []Label{{"outcome", "failure"}, {"function", "foo"}, {"handler", "x"}})
	r.IncLabels(MetricHandlerInvocations, []Label{{"outcome", "success"}, {"function", "bar"}, {"handler", "x"}})
	r.IncLabels(MetricHandlerInvocations, []Label{{"outcome", "failure"}, {"function", "bar"}, {"handler", "x"}})

	r.RemoveFunction("foo")

	s := r.Snapshot()
	if seriesPresent(t, s, MetricHandlerInvocations, "foo") {
		t.Fatalf("foo handler_invocations_total must be removed:\n%s", s)
	}
	if !seriesPresent(t, s, MetricHandlerInvocations, "bar") {
		t.Fatalf("bar handler_invocations_total must survive:\n%s", s)
	}
	if strings.Contains(s, "outcome=success} count=2") || strings.Contains(s, "outcome=failure} count=2") {
		t.Fatalf("foo's outcome counts must not linger:\n%s", s)
	}
}

// TestRemoveFunctionDeletesRuntimePoolMultiLabelSeries verifies function
// lifecycle cleanup deletes the warm-container pool series, which carry an
// extra variable label (state/outcome/reason) and therefore require the
// DeletePartialMatch path: foo's series in EVERY value of each extra label are
// removed while bar's survive.
func TestRemoveFunctionDeletesRuntimePoolMultiLabelSeries(t *testing.T) {
	r := New()
	for _, fn := range []string{"foo", "bar"} {
		for _, state := range []string{RuntimeStateIdle, RuntimeStateBusy, RuntimeStateStarting} {
			r.SetGaugeLabels(MetricRuntimeContainers, []Label{{"function", fn}, {"state", state}}, 1)
		}
		for _, outcome := range []string{RuntimeOutcomeWarm, RuntimeOutcomeCold} {
			r.IncLabels(MetricRuntimeContainerAcquires, []Label{{"function", fn}, {"outcome", outcome}})
		}
		for _, reason := range []string{"timeout", "image_changed", "shutdown"} {
			r.IncLabels(MetricRuntimeContainerDiscards, []Label{{"function", fn}, {"reason", reason}})
		}
		r.IncLabels(MetricRuntimeContainerWaits, []Label{{"function", fn}})
	}

	r.RemoveFunction("foo")

	s := r.Snapshot()
	for _, m := range []string{
		MetricRuntimeContainers,
		MetricRuntimeContainerAcquires,
		MetricRuntimeContainerDiscards,
		MetricRuntimeContainerWaits,
	} {
		if seriesPresent(t, s, m, "foo") {
			t.Fatalf("foo %s series must be removed:\n%s", m, s)
		}
		if !seriesPresent(t, s, m, "bar") {
			t.Fatalf("bar %s series must survive:\n%s", m, s)
		}
	}
	// Re-incrementing foo starts fresh, not from foo's pre-removal value.
	r.IncLabels(MetricRuntimeContainerAcquires, []Label{{"function", "foo"}, {"outcome", RuntimeOutcomeCold}})
	r.RemoveFunction("foo")
	if seriesPresent(t, r.Snapshot(), MetricRuntimeContainerAcquires, "foo") {
		t.Fatal("re-added foo must be removable again")
	}
}

// TestRemoveFunctionDeletesRuntimePoolSingleLabelSeries verifies the
// single-function-label pool vecs (capacity, acquire duration) are deleted by
// label value, independent of the multi-label vecs above.
func TestRemoveFunctionDeletesRuntimePoolSingleLabelSeries(t *testing.T) {
	r := New()
	for _, fn := range []string{"foo", "bar"} {
		r.SetGaugeLabels(MetricRuntimePoolCapacity, []Label{{"function", fn}}, 2)
		r.ObserveDurationLabels(MetricRuntimeContainerAcquireDuration, []Label{{"function", fn}}, time.Millisecond)
	}
	r.RemoveFunction("foo")
	s := r.Snapshot()
	for _, m := range []string{MetricRuntimePoolCapacity, MetricRuntimeContainerAcquireDuration} {
		if seriesPresent(t, s, m, "foo") {
			t.Fatalf("foo %s series must be removed:\n%s", m, s)
		}
		if !seriesPresent(t, s, m, "bar") {
			t.Fatalf("bar %s series must survive:\n%s", m, s)
		}
	}
}

// TestRemoveFunctionDeletesHandlerDurationMultiLabel verifies RemoveFunction on
// handler_duration_seconds deletes foo's multi-label histogram series
// (DeleteLabelValues path) while leaving bar's intact.
func TestRemoveFunctionDeletesHandlerDurationMultiLabel(t *testing.T) {
	r := New()
	r.ObserveDurationLabels(MetricHandlerDuration, []Label{{"function", "foo"}, {"handler", "x"}}, time.Millisecond)
	r.ObserveDurationLabels(MetricHandlerDuration, []Label{{"function", "bar"}, {"handler", "x"}}, time.Millisecond)

	r.RemoveFunction("foo")

	s := r.Snapshot()
	if seriesPresent(t, s, MetricHandlerDuration, "foo") {
		t.Fatalf("foo handler_duration_seconds must be removed:\n%s", s)
	}
	if !seriesPresent(t, s, MetricHandlerDuration, "bar") {
		t.Fatalf("bar handler_duration_seconds must survive:\n%s", s)
	}
}

// After removal, re-incrementing foo creates a FRESH series at the new value,
// not the pre-removal one.
func TestRemoveFunctionReAddStartsFreshCount(t *testing.T) {
	r := New()
	r.IncLabels(MetricFunctionEventsMatched, []Label{{"function", "foo"}})
	r.IncLabels(MetricFunctionEventsMatched, []Label{{"function", "foo"}})
	r.RemoveFunction("foo")

	r.IncLabels(MetricFunctionEventsMatched, []Label{{"function", "foo"}})
	s := r.Snapshot()
	if !strings.Contains(s, "function_events_matched_total{function=foo} count=1") {
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
	if !seriesPresent(t, r.Snapshot(), MetricFunctionEventsMatched, "keep") {
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
				r.IncLabels(MetricFunctionEventsMatched, []Label{{"function", "foo"}})
				r.IncLabels(MetricFunctionHandlerSuccess, []Label{{"function", "foo"}})
				r.ObserveDurationLabels(MetricHandlerDuration, []Label{{"function", "foo"}, {"handler", "x"}}, time.Millisecond)
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
		if byFn(r.FunctionStatsSnapshot(), "foo").Function == "" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	// One final sweep removes everything remaining.
	r.SweepFunctionMetrics(map[string]bool{"bar": true})

	if byFn(r.FunctionStatsSnapshot(), "foo").Function != "" {
		t.Fatalf("foo must be absent from FunctionStatsSnapshot after convergence:\n%+v", r.FunctionStatsSnapshot())
	}
	assertNoFunctionSeries(t, r, "foo")
	if !seriesPresent(t, r.Snapshot(), MetricFunctionEventsMatched, "bar") {
		t.Fatal("bar's series must survive the concurrent cleanup")
	}
	// Globals equal their exact seeded totals.
	assertGlobalTotals(t, r, globals)
}

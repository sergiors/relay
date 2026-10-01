package metrics

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// seedApp increments every app-carrying vec for name, so an app
// has a series in each of the appMetrics collectors. handler duration and
// app build are observed as histograms; handler_invocations_total gets
// both success and failure outcomes; the runtime pool vecs get one series each.
func seedApp(r *Registry, name string) {
	r.IncLabels(MetricHandlerInvocations, []Label{{"outcome", "success"}, {"app", name}, {"handler", "x"}})
	r.IncLabels(MetricHandlerInvocations, []Label{{"outcome", "failure"}, {"app", name}, {"handler", "x"}})
	r.IncLabels(MetricAppBuildFailures, []Label{{"app", name}})
	r.IncLabels(MetricAppEventsMatched, []Label{{"app", name}})
	r.IncLabels(MetricFunctionHandlerSuccess, []Label{{"app", name}})
	r.IncLabels(MetricFunctionHandlerFailure, []Label{{"app", name}})
	r.IncLabels(MetricFunctionRetries, []Label{{"app", name}})
	r.IncLabels(MetricFunctionDLQ, []Label{{"app", name}})
	r.ObserveDurationLabels(MetricHandlerDuration, []Label{{"app", name}, {"handler", "x"}}, time.Millisecond)
	r.ObserveDurationLabels(MetricAppBuild, []Label{{"app", name}}, time.Millisecond)
	r.SetGaugeLabels(MetricRuntimeContainers, []Label{{"app", name}, {"state", RuntimeStateIdle}}, 1)
	r.SetGaugeLabels(MetricRuntimePoolCapacity, []Label{{"app", name}}, 2)
	r.IncLabels(MetricRuntimeContainerAcquires, []Label{{"app", name}, {"outcome", RuntimeOutcomeCold}})
	r.IncLabels(MetricRuntimeContainerDiscards, []Label{{"app", name}, {"reason", "shutdown"}})
	r.ObserveDurationLabels(MetricRuntimeContainerAcquireDuration, []Label{{"app", name}}, time.Millisecond)
	r.IncLabels(MetricRuntimeContainerWaits, []Label{{"app", name}})
	r.SetGaugeLabels(MetricAppStatus, []Label{{"app", name}, {"status", "ready"}}, 1)
	r.IncLabels(MetricServiceReconciles, []Label{{"app", name}, {"outcome", ServiceOutcomeChanged}})
	r.ObserveDurationLabels(MetricServiceReconcileDuration, []Label{{"app", name}}, time.Millisecond)
}

// seriesPresent reports whether the snapshot contains a series whose rendered
// name carries the given metric name and the app label value. The metric
// argument is the canonical (relay_-prefixed) name; the snapshot renders
// display names (prefix stripped, see sampleName), so the prefix is trimmed
// before prefixing the line. sampleName sorts labels by name, so the app
// label may be followed by another label (",other=..."), by the closing brace
// ("}>"), or be the entire label set ("} count=...") — the check matches the
// app label token with any of those following contexts.
func seriesPresent(t *testing.T, snapshot, metric, name string) bool {
	t.Helper()
	token := "app=" + name
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

// assertNoAppSeries asserts no series across any of the appMetrics
// appMetrics vecs carries app=name.
func assertNoAppSeries(t *testing.T, r *Registry, name string) {
	t.Helper()
	s := r.Snapshot()
	for _, m := range appMetrics {
		if seriesPresent(t, s, m, name) {
			t.Fatalf("expected no %s series for %q, but found one in:\n%s", m, name, s)
		}
	}
}

// assertAppSeries asserts every one of the appMetrics vecs has a series for name.
func assertAppSeries(t *testing.T, r *Registry, name string) {
	t.Helper()
	s := r.Snapshot()
	for _, m := range appMetrics {
		if !seriesPresent(t, s, m, name) {
			t.Fatalf("expected %s series for %q, but missing in:\n%s", m, name, s)
		}
	}
}

// seedGlobals sets distinctive global counters/gauges and returns their total
// values so a test can assert they never change under app cleanup.
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

// Series exist for an app across all appMetrics vecs while the
// app exists, with a second app's series isolated alongside.
func TestAppSeriesExistWhileAppExists(t *testing.T) {
	r := New()
	seedApp(r, "foo")
	seedApp(r, "bar")
	assertAppSeries(t, r, "foo")
	assertAppSeries(t, r, "bar")
}

// RemoveApp removes exactly foo's series across all appMetrics vecs, leaves
// bar's series untouched in each, and leaves every global metric unchanged.
func TestRemoveAppDeletesOnlyAppSeries(t *testing.T) {
	r := New()
	seedApp(r, "foo")
	seedApp(r, "bar")
	globals := seedGlobals(r)

	r.RemoveApp("foo")

	assertGlobalTotals(t, r, globals)
	assertNoAppSeries(t, r, "foo")
	// bar's series survive in every vec.
	assertAppSeries(t, r, "bar")
}

// RemoveApp on handler_invocations_total must delete BOTH outcome series
// for foo (DeletePartialMatch path) while leaving bar's intact.
func TestRemoveAppDeletesBothHandlerInvocationsOutcomes(t *testing.T) {
	r := New()
	r.IncLabels(MetricHandlerInvocations, []Label{{"outcome", "success"}, {"app", "foo"}, {"handler", "x"}})
	r.IncLabels(MetricHandlerInvocations, []Label{{"outcome", "failure"}, {"app", "foo"}, {"handler", "x"}})
	r.IncLabels(MetricHandlerInvocations, []Label{{"outcome", "success"}, {"app", "bar"}, {"handler", "x"}})
	r.IncLabels(MetricHandlerInvocations, []Label{{"outcome", "failure"}, {"app", "bar"}, {"handler", "x"}})

	r.RemoveApp("foo")

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

// TestRemoveAppDeletesRuntimePoolMultiLabelSeries verifies app
// lifecycle cleanup deletes the warm-container pool series, which carry an
// extra variable label (state/outcome/reason) and therefore require the
// DeletePartialMatch path: foo's series in EVERY value of each extra label are
// removed while bar's survive.
func TestRemoveAppDeletesRuntimePoolMultiLabelSeries(t *testing.T) {
	r := New()
	for _, fn := range []string{"foo", "bar"} {
		for _, state := range []string{RuntimeStateIdle, RuntimeStateBusy, RuntimeStateStarting} {
			r.SetGaugeLabels(MetricRuntimeContainers, []Label{{"app", fn}, {"state", state}}, 1)
		}
		for _, outcome := range []string{RuntimeOutcomeWarm, RuntimeOutcomeCold} {
			r.IncLabels(MetricRuntimeContainerAcquires, []Label{{"app", fn}, {"outcome", outcome}})
		}
		for _, reason := range []string{"timeout", "image_changed", "shutdown"} {
			r.IncLabels(MetricRuntimeContainerDiscards, []Label{{"app", fn}, {"reason", reason}})
		}
		r.IncLabels(MetricRuntimeContainerWaits, []Label{{"app", fn}})
	}

	r.RemoveApp("foo")

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
	r.IncLabels(MetricRuntimeContainerAcquires, []Label{{"app", "foo"}, {"outcome", RuntimeOutcomeCold}})
	r.RemoveApp("foo")
	if seriesPresent(t, r.Snapshot(), MetricRuntimeContainerAcquires, "foo") {
		t.Fatal("re-added foo must be removable again")
	}
}

// TestRemoveAppDeletesRuntimePoolSingleLabelSeries verifies the
// single-app-label pool vecs (capacity, acquire duration) are deleted by
// label value, independent of the multi-label vecs above.
func TestRemoveAppDeletesRuntimePoolSingleLabelSeries(t *testing.T) {
	r := New()
	for _, fn := range []string{"foo", "bar"} {
		r.SetGaugeLabels(MetricRuntimePoolCapacity, []Label{{"app", fn}}, 2)
		r.ObserveDurationLabels(MetricRuntimeContainerAcquireDuration, []Label{{"app", fn}}, time.Millisecond)
	}
	r.RemoveApp("foo")
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

// TestRemoveAppDeletesHandlerDurationMultiLabel verifies RemoveApp on
// handler_duration_seconds deletes foo's multi-label histogram series
// (DeleteLabelValues path) while leaving bar's intact.
func TestRemoveAppDeletesHandlerDurationMultiLabel(t *testing.T) {
	r := New()
	r.ObserveDurationLabels(MetricHandlerDuration, []Label{{"app", "foo"}, {"handler", "x"}}, time.Millisecond)
	r.ObserveDurationLabels(MetricHandlerDuration, []Label{{"app", "bar"}, {"handler", "x"}}, time.Millisecond)

	r.RemoveApp("foo")

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
func TestRemoveAppReAddStartsFreshCount(t *testing.T) {
	r := New()
	r.IncLabels(MetricAppEventsMatched, []Label{{"app", "foo"}})
	r.IncLabels(MetricAppEventsMatched, []Label{{"app", "foo"}})
	r.RemoveApp("foo")

	r.IncLabels(MetricAppEventsMatched, []Label{{"app", "foo"}})
	s := r.Snapshot()
	if !strings.Contains(s, "app_events_matched_total{app=foo} count=1") {
		t.Fatalf("re-added foo must count 1, not the stale value; got:\n%s", s)
	}
}

// Nil-receiver RemoveApp and SweepAppMetrics must not panic.
func TestAppCleanupNilReceiverNoPanic(t *testing.T) {
	var r *Registry
	r.RemoveApp("foo")
	r.SweepAppMetrics(map[string]bool{"bar": true})
}

// SweepAppMetrics deletes series for apps absent from the live set,
// keeps live ones, leaves globals untouched, and a nil/empty map removes all
// app series but never globals.
func TestSweepAppMetrics(t *testing.T) {
	r := New()
	seedApp(r, "keep")
	seedApp(r, "remove")
	globals := seedGlobals(r)

	r.SweepAppMetrics(map[string]bool{"keep": true})

	assertGlobalTotals(t, r, globals)
	assertNoAppSeries(t, r, "remove")
	if !seriesPresent(t, r.Snapshot(), MetricAppEventsMatched, "keep") {
		t.Fatal("keep's series must survive the sweep")
	}

	// A nil/empty live map removes every app series but never globals.
	r.SweepAppMetrics(map[string]bool{})
	assertNoAppSeries(t, r, "keep")
	assertGlobalTotals(t, r, globals)
}

// Race: writers increment foo's app_* vecs while deleters call
// RemoveApp and SweepAppMetrics, concurrent with Snapshot /
// AppStatsSnapshot readers. After writers+deleters stop, a final sweep must
// leave foo absent with no permanent stale recreation, bar intact, and globals
// exactly equal to their seeded totals.
func TestRemoveAppConcurrentWithWriters(t *testing.T) {
	r := New()
	seedApp(r, "bar")
	globals := seedGlobals(r)

	const writers = 4
	const iters = 500
	var wg sync.WaitGroup

	// Writers: repeatedly create foo's app-scoped series.
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				r.IncLabels(MetricAppEventsMatched, []Label{{"app", "foo"}})
				r.IncLabels(MetricFunctionHandlerSuccess, []Label{{"app", "foo"}})
				r.ObserveDurationLabels(MetricHandlerDuration, []Label{{"app", "foo"}, {"handler", "x"}}, time.Millisecond)
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
					r.RemoveApp("foo")
					r.SweepAppMetrics(map[string]bool{"bar": true})
					r.Snapshot()
					r.AppStatsSnapshot()
				}
			}
		}()
	}

	wg.Wait()
	close(stop)
	delWG.Wait()

	// Convergence: after all writers stop, sweep repeatedly (bounded) until foo
	// no longer appears in the app_stats view.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r.SweepAppMetrics(map[string]bool{"bar": true})
		if byFn(r.AppStatsSnapshot(), "foo").App == "" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	// One final sweep removes everything remaining.
	r.SweepAppMetrics(map[string]bool{"bar": true})

	if byFn(r.AppStatsSnapshot(), "foo").App != "" {
		t.Fatalf("foo must be absent from AppStatsSnapshot after convergence:\n%+v", r.AppStatsSnapshot())
	}
	assertNoAppSeries(t, r, "foo")
	if !seriesPresent(t, r.Snapshot(), MetricAppEventsMatched, "bar") {
		t.Fatal("bar's series must survive the concurrent cleanup")
	}
	// Globals equal their exact seeded totals.
	assertGlobalTotals(t, r, globals)
}

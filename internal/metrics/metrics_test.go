package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestCounterAccumulation(t *testing.T) {
	r := New()
	r.Inc("events_processed_total")
	r.Inc("events_processed_total")
	r.Add("events_processed_total", 3)
	r.Inc("retries_total")
	got := r.Snapshot()
	// Pre-registered counters and gauges render their current (zero) value too;
	// assert the accumulated counters and that the required zero gauges are
	// present rather than an exact full-string match (the gauge set grows).
	for _, want := range []string{
		"events_processed_total count=5",
		"retries_total count=1",
		"pending_entries value=0",
		"pending_oldest_age_seconds value=0",
		"buffered_events value=0",
		"in_flight_invocations value=0",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("snapshot missing %q; got %q", want, got)
		}
	}
}

func TestLabeledCounterIsolation(t *testing.T) {
	r := New()
	r.IncLabels("handler_invocations_total", []Label{{"outcome", "success"}, {"function", "a"}, {"handler", "x"}})
	// Reversed label order must resolve to the same metric.
	r.IncLabels("handler_invocations_total", []Label{{"handler", "x"}, {"outcome", "success"}, {"function", "a"}})
	r.IncLabels("handler_invocations_total", []Label{{"outcome", "success"}, {"function", "b"}, {"handler", "x"}})
	got := r.Snapshot()
	if !strings.Contains(got, "handler_invocations_total{function=a,handler=x,outcome=success} count=2") {
		t.Fatalf("snapshot missing first series:\n%s", got)
	}
	if !strings.Contains(got, "handler_invocations_total{function=b,handler=x,outcome=success} count=1") {
		t.Fatalf("snapshot missing second series:\n%s", got)
	}
	if strings.Contains(got, "outcome=failure") {
		t.Fatalf("snapshot unexpectedly contains failure series:\n%s", got)
	}
}

func TestDurationCountSum(t *testing.T) {
	r := New()
	obs := func(d time.Duration) {
		r.ObserveDurationLabels("handler_duration_seconds", []Label{{"function", "a"}, {"handler", "x"}}, d)
	}
	obs(1 * time.Second)
	obs(3 * time.Second)
	obs(2 * time.Second)
	got := r.Snapshot()
	if !strings.Contains(got, "handler_duration_seconds{function=a,handler=x} count=3 sum=6.000") {
		t.Fatalf("snapshot = %q, want it to contain %q", got, "handler_duration_seconds{function=a,handler=x} count=3 sum=6.000")
	}
}

func TestCounterGetter(t *testing.T) {
	r := New()
	r.Inc("events_processed_total")
	r.Add("events_processed_total", 3)
	r.Inc("retries_total")
	if got := r.Counter("events_processed_total"); got != 4 {
		t.Fatalf("Counter(events_processed_total) = %d, want 4", got)
	}
	if got := r.Counter("retries_total"); got != 1 {
		t.Fatalf("Counter(retries_total) = %d, want 1", got)
	}
	// Absent and labeled-only names read as 0.
	if got := r.Counter("missing"); got != 0 {
		t.Fatalf("Counter(missing) = %d, want 0", got)
	}
	r.IncLabels("handler_invocations_total", []Label{{"outcome", "success"}, {"function", "a"}, {"handler", "x"}})
	if got := r.Counter("handler_invocations_total"); got != 0 {
		t.Fatalf("Counter(handler_invocations_total) = %d, want 0 (labeled only)", got)
	}
}

func TestGaugeGetter(t *testing.T) {
	r := New()
	r.SetGauge("pending_entries", 1.5)
	r.SetGauge("pending_entries", 2.25)
	if got := r.Gauge("pending_entries"); got != 2.25 {
		t.Fatalf("Gauge(pending_entries) = %v, want 2.25", got)
	}
	if got := r.Gauge("missing"); got != 0 {
		t.Fatalf("Gauge(missing) = %v, want 0", got)
	}
}

func TestGettersNilReceiver(t *testing.T) {
	var r *Registry
	if got := r.Counter("a"); got != 0 {
		t.Fatalf("nil Counter = %d, want 0", got)
	}
	if got := r.Gauge("g"); got != 0 {
		t.Fatalf("nil Gauge = %v, want 0", got)
	}
}

func TestGaugeSet(t *testing.T) {
	r := New()
	r.SetGauge("pending_entries", 1.5)
	r.SetGauge("pending_entries", 2.25)
	// There are no labeled gauges: a labeled set is a no-op.
	r.SetGaugeLabels("pending_entries", []Label{{"l", "x"}}, 9)
	got := r.Snapshot()
	if !strings.Contains(got, "pending_entries value=2.25") {
		t.Fatalf("snapshot missing pending_entries value:\n%s", got)
	}
	if strings.Contains(got, `pending_entries{l=x}`) {
		t.Fatalf("snapshot unexpectedly contains labeled gauge:\n%s", got)
	}
}

func TestSnapshotDeterminism(t *testing.T) {
	r := New()
	r.Inc("dlq_entries_total")
	r.Inc("events_processed_total")
	r.ObserveDurationLabels("function_build_seconds", []Label{{"function", "a"}}, time.Second)
	r.SetGauge("pending_entries", 1)
	r.IncLabels("build_failures_total", []Label{{"function", "a"}})
	first := r.Snapshot()
	for i := 0; i < 100; i++ {
		if got := r.Snapshot(); got != first {
			t.Fatalf("snapshot not deterministic:\nfirst=%q\ngot=%q", first, got)
		}
	}
}

func TestNilReceiverNoop(t *testing.T) {
	var r *Registry
	// None of these may panic.
	r.Inc("a")
	r.Add("a", 1)
	r.IncLabels("h", []Label{{"a", "1"}})
	r.ObserveDuration("d", time.Second)
	r.ObserveDurationLabels("d", []Label{{"a", "1"}}, time.Second)
	r.SetGauge("g", 1)
	r.SetGaugeLabels("g", []Label{{"a", "1"}}, 1)
	if got := r.Snapshot(); got != "" {
		t.Fatalf("nil snapshot = %q, want empty", got)
	}
}

func TestFunctionStatsSnapshot(t *testing.T) {
	r := New()
	// Two functions with distinct per-function counters.
	r.IncLabels("function_events_total", []Label{{"function", "a"}})
	r.IncLabels("function_events_total", []Label{{"function", "a"}})
	r.IncLabels("function_events_total", []Label{{"function", "b"}})
	r.IncLabels("function_handler_success_total", []Label{{"function", "a"}})
	r.IncLabels("function_handler_failure_total", []Label{{"function", "b"}})
	r.IncLabels("function_retries_total", []Label{{"function", "b"}})
	r.IncLabels("function_dlq_total", []Label{{"function", "b"}})

	got := r.FunctionStatsSnapshot()
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2: %+v", len(got), got)
	}
	// Sorted by function name.
	if got[0].Function != "a" || got[1].Function != "b" {
		t.Fatalf("unexpected order: %+v", got)
	}
	if got[0].Events != 2 || got[0].HandlerSuccessTotal != 1 || got[0].HandlerFailureTotal != 0 {
		t.Fatalf("function a stats = %+v", got[0])
	}
	if got[1].Events != 1 || got[1].HandlerFailureTotal != 1 || got[1].RetriesTotal != 1 || got[1].DLQTotal != 1 {
		t.Fatalf("function b stats = %+v", got[1])
	}
}

func TestFunctionStatsSnapshotEmptyAndNil(t *testing.T) {
	r := New()
	if got := r.FunctionStatsSnapshot(); got != nil {
		t.Fatalf("empty snapshot = %+v, want nil", got)
	}
	var nilR *Registry
	if got := nilR.FunctionStatsSnapshot(); got != nil {
		t.Fatalf("nil snapshot = %+v, want nil", got)
	}
}

func TestTestutilBacking(t *testing.T) {
	r := New()
	r.Inc("events_received_total")
	c := testutil.ToFloat64(r.counters["events_received_total"])
	if c != 1 {
		t.Fatalf("testutil.ToFloat64 = %v, want 1", c)
	}
	if n := testutil.CollectAndCount(r.reg, "events_received_total"); n != 1 {
		t.Fatalf("CollectAndCount = %d, want 1", n)
	}
}

// TestHandlerNilRegistryServesEmpty verifies a nil-receiver Handler returns a
// valid handler serving an empty 200 body (callers never get a nil http.Handler).
func TestHandlerNilRegistryServesEmpty(t *testing.T) {
	var r *Registry
	h := r.Handler()
	if h == nil {
		t.Fatal("nil registry Handler() returned nil http.Handler")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("nil registry body = %q, want empty", rec.Body.String())
	}
}

// TestSetFunctionTimestampSnapshot verifies SetFunctionTimestamp stores the
// latest per-kind values and FunctionStatsSnapshot surfaces them, including the
// counter-without-timestamp (all-zero timestamps) and timestamp-without-counter
// shapes.
func TestSetFunctionTimestampSnapshot(t *testing.T) {
	r := New()
	// "a": counters + all four timestamps.
	r.IncLabels("function_events_total", []Label{{"function", "a"}})
	ts := int64(1700000000)
	r.SetFunctionTimestamp("a", FunctionTimestampExecution, ts)
	r.SetFunctionTimestamp("a", FunctionTimestampSuccess, ts+1)
	r.SetFunctionTimestamp("a", FunctionTimestampFailure, ts+2)
	r.SetFunctionTimestamp("a", FunctionTimestampDLQ, ts+3)

	// SetTimestamp entries survive repeated overwrites (latest wins).
	r.SetFunctionTimestamp("a", FunctionTimestampExecution, ts+10)

	got := r.FunctionStatsSnapshot()
	byName := map[string]FunctionStat{}
	for _, f := range got {
		byName[f.Function] = f
	}
	a, ok := byName["a"]
	if !ok {
		t.Fatalf("a missing from snapshot: %+v", got)
	}
	if a.LastExecution != ts+10 || a.LastSuccess != ts+1 || a.LastFailure != ts+2 || a.LastDLQ != ts+3 {
		t.Fatalf("a timestamps = %+v", a)
	}

	// A function with counters but NO timestamp entry reads zeros (counters-only shape).
	r.IncLabels("function_events_total", []Label{{"function", "c"}})
	got = r.FunctionStatsSnapshot()
	c := byFn(got, "c")
	if c.Events != 1 || c.LastExecution != 0 || c.LastSuccess != 0 || c.LastFailure != 0 || c.LastDLQ != 0 {
		t.Fatalf("c (counters-only) = %+v", c)
	}

	// Nil-safe: a nil receiver is a no-op.
	var nilR *Registry
	nilR.SetFunctionTimestamp("x", FunctionTimestampExecution, ts)
	// An unknown kind is ignored.
	r.SetFunctionTimestamp("a", FunctionTimestampKind(99), ts+20)
	if a := byFn(r.FunctionStatsSnapshot(), "a"); a.LastExecution != ts+10 {
		t.Fatalf("unknown kind must not write: %+v", a)
	}
}

// byFn finds the FunctionStat for name in a snapshot, or a zero value.
func byFn(fs []FunctionStat, name string) FunctionStat {
	for _, f := range fs {
		if f.Function == name {
			return f
		}
	}
	return FunctionStat{}
}

// TestSeedFunctionStatTimestamps verifies SeedFunctionStat restores persisted
// timestamps into the snapshot and SKIPS zeros (a persisted zero never
// materializes as an entry that could later look newer than nothing).
func TestSeedFunctionStatTimestamps(t *testing.T) {
	r := New()
	ts := int64(1700000000)
	r.SeedFunctionStat(FunctionStat{Function: "alpha", Events: 1, LastExecution: ts, LastSuccess: ts + 1})
	// beta has counters but only zero timestamps: nothing is stored.
	r.SeedFunctionStat(FunctionStat{Function: "beta", Events: 1})

	got := r.FunctionStatsSnapshot()
	a := byFn(got, "alpha")
	if a.LastExecution != ts || a.LastSuccess != ts+1 || a.LastFailure != 0 || a.LastDLQ != 0 {
		t.Fatalf("alpha = %+v", a)
	}
	if b := byFn(got, "beta"); b.LastExecution != 0 || b.LastSuccess != 0 {
		t.Fatalf("beta must have no timestamp entries: %+v", b)
	}

	// A partial overwrite preserves the kinds that were not seeded.
	r.SeedFunctionStat(FunctionStat{Function: "alpha", Events: 2, LastFailure: ts + 5})
	a = byFn(r.FunctionStatsSnapshot(), "alpha")
	if a.Events != 3 || a.LastExecution != ts || a.LastSuccess != ts+1 || a.LastFailure != ts+5 {
		t.Fatalf("alpha after partial seed = %+v", a)
	}
}

// TestRemoveFunctionClearsTimestamps verifies RemoveFunction deletes the
// Relay-side timestamp entry alongside the Prometheus series.
func TestRemoveFunctionClearsTimestamps(t *testing.T) {
	r := New()
	ts := int64(1700000000)
	r.IncLabels("function_events_total", []Label{{"function", "a"}})
	r.SetFunctionTimestamp("a", FunctionTimestampExecution, ts)
	r.IncLabels("function_events_total", []Label{{"function", "b"}})
	r.SetFunctionTimestamp("b", FunctionTimestampExecution, ts)

	r.RemoveFunction("a")
	got := r.FunctionStatsSnapshot()
	if byFn(got, "a").Function != "" {
		t.Fatalf("a (series+timestamps) must be fully removed: %+v", got)
	}
	if byFn(got, "b").LastExecution != ts {
		t.Fatalf("b must survive: %+v", got)
	}

	// Sweep does the same with the live set: with live={"a"} (no series at all
	// after the removal, so a is absent from the snapshot entirely) b's
	// timestamps and series are swept; with live={"b"} b survives fully.
	r.SweepFunctionMetrics(map[string]bool{"b": true})
	if a := byFn(r.FunctionStatsSnapshot(), "b"); a.LastExecution != ts {
		t.Fatalf("sweep must keep live b's timestamps:\n%+v", r.FunctionStatsSnapshot())
	}
	r.SweepFunctionMetrics(map[string]bool{"a": true})
	if fs := r.FunctionStatsSnapshot(); len(fs) != 0 {
		t.Fatalf("sweep must clear non-live b's timestamps too:\n%+v", fs)
	}

	// Nil-safety: none of these cleanup paths panic on a nil receiver.
	var nilR *Registry
	nilR.RemoveFunction("a")
	nilR.SweepFunctionMetrics(map[string]bool{})
}

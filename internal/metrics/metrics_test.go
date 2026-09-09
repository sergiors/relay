package metrics

import (
	"context"
	"fmt"
	"strings"
	"sync"
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
	// Pre-registered gauges render their current (zero) value too.
	want := "events_processed_total count=5\npending_entries value=0\npending_oldest_age_seconds value=0\nretries_total count=1"
	if got != want {
		t.Fatalf("snapshot = %q, want %q", got, want)
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
	if !strings.HasPrefix(got, "handler_duration_seconds{function=a,handler=x} count=3 sum=6.000") {
		t.Fatalf("snapshot = %q, want prefix %q", got, "handler_duration_seconds{function=a,handler=x} count=3 sum=6.000")
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

func TestLogLoopTicksAndStopsOnCancel(t *testing.T) {
	r := New()
	r.Inc("events_received_total")

	var mu sync.Mutex
	var lines []string
	logf := func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, fmt.Sprintf(format, args...))
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.LogLoop(ctx, 10*time.Millisecond, logf)
	}()

	// Wait for at least one tick to emit.
	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		n := len(lines)
		mu.Unlock()
		if n > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("LogLoop never logged")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("LogLoop did not stop on cancel")
	}

	mu.Lock()
	defer mu.Unlock()
	if !strings.Contains(lines[0], "metrics events_received_total count=1") {
		t.Fatalf("unexpected first log line: %q", lines[0])
	}
}

func TestLogLoopNilRegistryExitsOnCancel(t *testing.T) {
	var r *Registry
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.LogLoop(ctx, time.Hour, func(string, ...any) { t.Error("must not log on nil registry") })
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("nil LogLoop did not stop on cancel")
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

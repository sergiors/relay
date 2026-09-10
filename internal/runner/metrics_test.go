package runner

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/metrics"
	"relay/internal/runtime"
	"relay/internal/stream"
)

// silentLogger returns a logger that discards output (nil *log.Logger writes to
// a nil io.Writer, which would panic).
func silentLogger() *log.Logger {
	return log.New(io.Discard, "", 0)
}

// alwaysMatch returns a prepared function whose single rule matches any event
// (empty pattern). Its handler invocations record a short, observable duration.
// The rule carries the default retry count so failing invocations are retried
// (matching production template defaults).
func alwaysMatchFn(t *testing.T, name string, executor Executor) *PreparedFunction {
	t.Helper()
	return NewPrepared(
		function.Function{
			Name: name,
			Template: &function.Template{
				Runtime: "node24",
				Rules:   []function.Rule{{Handler: "index.run", Pattern: function.Pattern{}, Timeout: time.Second, Retries: function.DefaultRetries}},
			},
		},
		&runtime.Prepared{Name: name, Image: "x"},
		executor,
	)
}

// fixedExecutor succeeds or fails on demand with a configurable duration.
type fixedExecutor struct {
	err bool
	ops int
}

func (f *fixedExecutor) Execute(ctx context.Context, prepared *runtime.Prepared, handler string, _ []byte, _ []string) error {
	if f.err {
		return fmt.Errorf("boom")
	}
	return nil
}

func TestHandleRecordsSuccessMetrics(t *testing.T) {
	m := metrics.New()
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", &fixedExecutor{})}, silentLogger(), m)

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}

	got := m.Snapshot()
	for _, want := range []string{
		"events_received_total count=1",
		"handler_invocations_total{function=user-events,handler=index.run,outcome=success} count=1",
		"handler_success_total count=1",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("snapshot missing %q; got:\n%s", want, got)
		}
	}
	if strings.Contains(got, "outcome=failure") {
		t.Errorf("unexpected failure metric; got:\n%s", got)
	}
	if strings.Contains(got, "handler_failure_total") {
		t.Errorf("unexpected failure total; got:\n%s", got)
	}
	// The duration observation must be recorded (count 1, non-zero sum/max).
	if !containsDuration(got, "handler_duration_seconds{function=user-events,handler=index.run} count=1 sum=") {
		t.Errorf("expected handler_duration_seconds observation; got:\n%s", got)
	}
}

func TestHandleRecordsFailureMetrics(t *testing.T) {
	m := metrics.New()
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", &fixedExecutor{err: true})}, silentLogger(), m)

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err == nil {
		t.Fatal("expected handle to fail")
	}

	got := m.Snapshot()
	if !strings.Contains(got, "handler_invocations_total{function=user-events,handler=index.run,outcome=failure} count=1") {
		t.Errorf("expected failure counter; got:\n%s", got)
	}
	if !strings.Contains(got, "handler_failure_total count=1") {
		t.Errorf("expected failure total; got:\n%s", got)
	}
	if strings.Contains(got, "outcome=success") {
		t.Errorf("unexpected success metric; got:\n%s", got)
	}
	if strings.Contains(got, "handler_success_total") {
		t.Errorf("unexpected success total; got:\n%s", got)
	}
}

// containsDuration asserts a duration metric line with a prefix and the given
// count appears (without pinning the precise sum, which varies by timing).
func containsDuration(snapshot, prefix string) bool {
	for _, line := range strings.Split(snapshot, "\n") {
		if strings.HasPrefix(line, prefix) && strings.Contains(line, " sum=") {
			return true
		}
	}
	return false
}

func TestHandleFunctionLevelCounters(t *testing.T) {
	m := metrics.New()
	// Two functions, each with a single always-matching rule.
	r := NewWithMetrics([]*PreparedFunction{
		alwaysMatchFn(t, "a", &fixedExecutor{}),
		alwaysMatchFn(t, "b", &fixedExecutor{}),
	}, silentLogger(), m)

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}

	// One event matching two functions: global message-level counter is 1, but
	// each function is engaged once.
	if got := m.Counter("events_received_total"); got != 1 {
		t.Fatalf("events_received_total = %d, want 1", got)
	}
	fs := m.FunctionStatsSnapshot()
	if len(fs) != 2 {
		t.Fatalf("function stats len = %d, want 2: %+v", len(fs), fs)
	}
	for _, f := range fs {
		if f.Events != 1 {
			t.Fatalf("function %s events = %d, want 1", f.Function, f.Events)
		}
		if f.HandlerSuccessTotal != 1 {
			t.Fatalf("function %s success = %d, want 1", f.Function, f.HandlerSuccessTotal)
		}
		if f.HandlerFailureTotal != 0 {
			t.Fatalf("function %s failure = %d, want 0", f.Function, f.HandlerFailureTotal)
		}
	}
}

func TestHandleFunctionFailureRetryAndDLQ(t *testing.T) {
	// Without invocation state, a failure counts a retry but never a DLQ: the
	// DLQ decision needs a Redis-backed attempt count to know exhaustion.
	m := metrics.New()
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "a", &fixedExecutor{err: true})}, silentLogger(), m)
	ctx := stream.WithDeliveryAttempt(context.Background(), 2)
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err == nil {
		t.Fatal("expected handle to fail")
	}
	fs := m.FunctionStatsSnapshot()
	if len(fs) != 1 {
		t.Fatalf("function stats len = %d, want 1: %+v", len(fs), fs)
	}
	if fs[0].RetriesTotal != 1 {
		t.Fatalf("retries = %d, want 1", fs[0].RetriesTotal)
	}
	if fs[0].DLQTotal != 0 {
		t.Fatalf("dlq = %d, want 0 (no invocation state, no exhaustion decision)", fs[0].DLQTotal)
	}
	if fs[0].HandlerFailureTotal != 1 {
		t.Fatalf("failure = %d, want 1", fs[0].HandlerFailureTotal)
	}
}

func TestHandleNilMetricsSafe(t *testing.T) {
	// NewWithMetrics(nil registry) must not panic when handling succeeds/fails.
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", &fixedExecutor{})}, silentLogger(), nil)
	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}

	rf := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", &fixedExecutor{err: true})}, silentLogger(), nil)
	if err := rf.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err == nil {
		t.Fatal("expected handle to fail")
	}
}

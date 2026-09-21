package runner

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/metrics"
	"relay/internal/runtime"
	"relay/internal/stream"
	"relay/internal/testutil"
)

// panicExecutor panics inside Execute to exercise the release-on-panic and
// panic-boundary paths.
type panicExecutor struct{}

func (panicExecutor) Execute(context.Context, *runtime.Prepared, string, []byte, []string) error {
	panic("executor boom")
}

// TestHandleExecutorPanicTreatedAsFailedAttempt verifies that an executor panic
// is converted into a normal failed attempt: with retries:0 the invocation is
// exhausted after the single panicking attempt (IsTerminal), the failure metrics
// are counted, and Handle returns an error wrapping the panic instead of letting
// it escape. This mirrors TestHandleRetriesZeroExhaustsAfterOneAttempt but with
// a panicking executor.
func TestHandleExecutorPanicTreatedAsFailedAttempt(t *testing.T) {
	m := metrics.New()
	r := NewWithMetrics([]*PreparedFunction{fnWithRetries(t, "user-events", 0, panicExecutor{})}, testutil.DiscardLogger(), m)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	if !errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("handle error = %v, want ErrInvocationExhausted", err)
	}
	if !strings.Contains(err.Error(), "executor panic") {
		t.Fatalf("error = %v, want it to mention the executor panic", err)
	}
	if !prog.IsTerminal("user-events/index.run") {
		t.Fatalf("invocation should be terminal (exhausted) after a panicking attempt")
	}
	if len(prog.failures) != 0 {
		t.Fatalf("failures = %v, want none (exhausted, not retried)", prog.failures)
	}
	// The panic must be counted as a failed invocation, not a success.
	got := m.Snapshot()
	if !strings.Contains(got, "handler_invocations_total{function=user-events,handler=index.run,outcome=failure} count=1") {
		t.Errorf("expected one failure invocation; got:\n%s", got)
	}
	if strings.Contains(got, "outcome=success") {
		t.Errorf("unexpected success metric; got:\n%s", got)
	}
}

// TestHandleExecutorPanicSchedulesRetry verifies that a panicking execution is
// retryable: with the default retry count, the first panic records a retry
// backoff (RecordFailure with retryBackoff(1)=1m) and Handle returns a plain
// (non-exhausted) error; after advancing the clock, a second panicking attempt
// records the 2m backoff. This proves a panic flows through the same
// retry/exhaustion machinery as any other failure.
func TestHandleExecutorPanicSchedulesRetry(t *testing.T) {
	r := NewWithMetrics([]*PreparedFunction{fnWithRetries(t, "user-events", function.DefaultRetries, panicExecutor{})}, testutil.DiscardLogger(), nil)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	// Attempt 1: panics → retryable, records a 1m backoff.
	err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	if err == nil || errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("attempt 1 error = %v, want a plain (retryable) error", err)
	}
	if len(prog.failures) != 1 || prog.failures[0] != time.Minute {
		t.Fatalf("failures after attempt 1 = %v, want [1m]", prog.failures)
	}
	if prog.IsTerminal("user-events/index.run") {
		t.Fatalf("invocation should NOT be terminal after a retryable panic")
	}

	// Advance past the 1m backoff; attempt 2 panics → 2m backoff.
	prog.advance(retryBackoff(1))
	err = r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	if err == nil || errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("attempt 2 error = %v, want a plain (retryable) error", err)
	}
	if len(prog.failures) != 2 || prog.failures[1] != 2*time.Minute {
		t.Fatalf("failures after attempt 2 = %v, want [1m 2m]", prog.failures)
	}
}

// TestHandlePanicLogsStack verifies that a panicking execution emits a dedicated
// panic log line containing the panic value and a stack frame marker, so the
// bug is visible and attributable.
func TestHandlePanicLogsStack(t *testing.T) {
	logger, buf := bufferLogger()
	r := NewWithMetrics([]*PreparedFunction{fnWithRetries(t, "user-events", 0, panicExecutor{})}, logger, nil)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err == nil {
		t.Fatal("expected Handle to return an error for the panicking execution")
	}
	out := buf.String()
	if !strings.Contains(out, "PANICKED") {
		t.Fatalf("log does not contain the PANICKED marker:\n%s", out)
	}
	if !strings.Contains(out, "executor boom") {
		t.Fatalf("log does not contain the panic value:\n%s", out)
	}
	// The stack must be present (a goroutine/panic frame marker).
	if !strings.Contains(out, "goroutine") && !strings.Contains(out, "runtime/panic") {
		t.Fatalf("log does not contain a stack frame marker:\n%s", out)
	}
}

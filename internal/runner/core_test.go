package runner

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"relay/internal/metrics"
	"relay/internal/stream"
	"relay/internal/testutil"
)

// TestHandleSkipsCompletedInvocation verifies that when invocation state is
// present in ctx and an invocation already completed, Handle skips it: the
// executor is not called and no success/failure metrics are recorded for it.
func TestHandleSkipsCompletedInvocation(t *testing.T) {
	m := metrics.New()
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", exec)}, testutil.DiscardLogger(), m)

	// First delivery: no invocation state, so the handler runs and records success.
	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls = %d, want 1", exec.count())
	}

	// Second delivery with invocation state marking the invocation already
	// complete: the handler must be skipped (not executed) and not counted as a
	// success.
	prog := newFakeInvocationState()
	prog.done["user-events/index.run"] = true
	ctx := stream.WithInvocationState(context.Background(), prog)
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls = %d, want 1 (skipped)", exec.count())
	}

	got := m.Snapshot()
	// The skip must not add a second success invocation.
	if !strings.Contains(got, "handler_invocations_total{function=user-events,handler=index.run,outcome=success} count=1") {
		t.Errorf("expected exactly one success invocation; got:\n%s", got)
	}
	if strings.Contains(got, "outcome=failure") {
		t.Errorf("unexpected failure metric; got:\n%s", got)
	}
}

// TestHandleMarksCompleteOnExecution verifies that a successful execution
// records the invocation via MarkComplete so a later redelivery can skip it.
func TestHandleMarksCompleteOnExecution(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", exec)}, testutil.DiscardLogger(), nil)

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(prog.marks) != 1 || prog.marks[0] != "user-events/index.run" {
		t.Fatalf("marks = %v, want [user-events/index.run]", prog.marks)
	}
	if !prog.IsComplete("user-events/index.run") {
		t.Fatalf("invocation should be marked complete")
	}
}

// TestHandleWithoutInvocationStateStillExecutes verifies that Handle without
// invocation state in ctx runs every matching handler (nil-safe, no state
// bookkeeping).
func TestHandleWithoutInvocationStateStillExecutes(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", exec)}, testutil.DiscardLogger(), nil)
	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls = %d, want 1", exec.count())
	}
}

// TestSetMaxHandlerTimeoutCapsRuleTimeout verifies that a configured max
// handler timeout caps a rule's (larger) timeout: the executor observes the
// capped deadline and Handle returns a deadline error.
func TestSetMaxHandlerTimeoutCapsRuleTimeout(t *testing.T) {
	r := NewWithMetrics([]*PreparedFunction{fnWithTimeout(t, "user-events", time.Hour, ctxAwareExecutor{})}, testutil.DiscardLogger(), nil)
	r.SetMaxHandlerTimeout(50 * time.Millisecond)

	start := time.Now()
	err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected a deadline error from the capped handler")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	// The handler must have been cut off near the cap, not the 1h rule timeout.
	if elapsed > 2*time.Second {
		t.Fatalf("handler ran %s, expected it to be capped near 50ms", elapsed)
	}
}

// TestSetMaxHandlerTimeoutUncappedByDefault verifies that with no cap set, the
// rule's own (small) timeout is honored and not shortened to some default: the
// executor observes a deadline at least as long as the rule timeout.
func TestSetMaxHandlerTimeoutUncappedByDefault(t *testing.T) {
	r := NewWithMetrics([]*PreparedFunction{fnWithTimeout(t, "user-events", 200*time.Millisecond, ctxAwareExecutor{})}, testutil.DiscardLogger(), nil)
	// SetMaxHandlerTimeout deliberately NOT called (cap stays 0 = uncapped).

	start := time.Now()
	err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected a deadline error from the rule timeout")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	// The observed deadline must be the rule's own 200ms, not shortened to 50ms.
	if elapsed < 150*time.Millisecond {
		t.Fatalf("handler returned after %s, expected it to run the full 200ms rule timeout", elapsed)
	}
}

// TestHandleTryStartGuardsInFlightInvocation verifies that when an invocation is
// protected by an active running deadline (a future deadline), Handle skips the
// executor: another replica may be executing it, so it must not run concurrently.
// Because nothing executed and the invocation is protected, Handle returns
// ErrInvocationNotEligible so the stream layer leaves the message pending (the
// cross-replica ACK-hazard fix).
func TestHandleTryStartGuardsInFlightInvocation(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", exec)}, testutil.DiscardLogger(), nil)

	prog := newFakeInvocationState()
	// Freeze the clock and mark the invocation running with a future deadline.
	now := time.Now()
	prog.setClock(func() time.Time { return now })
	prog.running["user-events/index.run"] = now.Add(time.Hour)
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	if !errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("handle error = %v, want ErrInvocationNotEligible", err)
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls = %d, want 0 (in-flight invocation skipped)", exec.count())
	}
}

// TestHandleExecutesAfterDeadlineExpires verifies that once an invocation's
// running deadline has passed, Handle executes it again (the marker is stale).
func TestHandleExecutesAfterDeadlineExpires(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", exec)}, testutil.DiscardLogger(), nil)

	prog := newFakeInvocationState()
	now := time.Now()
	prog.setClock(func() time.Time { return now })
	// Mark running with a deadline that has already passed.
	prog.running["user-events/index.run"] = now.Add(-time.Second)
	ctx := stream.WithInvocationState(context.Background(), prog)

	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls = %d, want 1 (expired deadline is eligible)", exec.count())
	}
}

// TestHandleFailureSchedulesRetry verifies that a failing invocation records a
// retry backoff (RecordFailure) instead of clearing the marker: the invocation
// is gated until the backoff elapses, so an immediate second delivery is skipped
// (not eligible) rather than re-run.
func TestHandleFailureSchedulesRetry(t *testing.T) {
	exec := &countingExecutor{fail: true}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", exec)}, testutil.DiscardLogger(), nil)

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	// First delivery fails: a retry backoff is recorded (not an endRunning).
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err == nil {
		t.Fatal("expected failure")
	}
	if _, ok := prog.runningDeadline("user-events/index.run"); ok {
		t.Fatalf("running marker should be cleared after failure")
	}
	if len(prog.failures) != 1 {
		t.Fatalf("failures = %v, want one RecordFailure", prog.failures)
	}
	if prog.failures[0] != time.Minute {
		t.Fatalf("first retry backoff = %s, want 1m", prog.failures[0])
	}

	// Second delivery (immediate, no clock advance): the invocation is gated by
	// the retry backoff, so it is not eligible and Handle returns
	// ErrInvocationNotEligible.
	exec.setFail(false)
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); !errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("second delivery error = %v, want ErrInvocationNotEligible (backoff gating)", err)
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls = %d, want 1 (backoff-gated retry skipped)", exec.count())
	}
}

// TestHandleTimeoutSchedulesRetry verifies that a handler that times out
// (executor returns ctx.Err) causes Handle to error and RecordFailure to be
// called, scheduling a retry backoff.
func TestHandleTimeoutSchedulesRetry(t *testing.T) {
	r := NewWithMetrics([]*PreparedFunction{fnWithTimeout(t, "user-events", 50*time.Millisecond, ctxAwareExecutor{})}, testutil.DiscardLogger(), nil)

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	if err == nil {
		t.Fatal("expected a deadline error from the timed-out handler")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if _, ok := prog.runningDeadline("user-events/index.run"); ok {
		t.Fatalf("running marker should be cleared after timeout")
	}
	if len(prog.failures) != 1 {
		t.Fatalf("failures = %v, want one RecordFailure after timeout", prog.failures)
	}
}

// TestHandleMarshalErrorSchedulesRetry verifies that when json.Marshal fails
// after TryStart has already claimed the invocation, Handle records a retry
// backoff (RecordFailure) so the deterministic marshal failure is treated as a
// failed attempt. The event carries a value json.Marshal cannot encode (a chan),
// which errors with "unsupported type".
func TestHandleMarshalErrorSchedulesRetry(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", exec)}, testutil.DiscardLogger(), nil)

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	// json.Marshal of a map containing a chan value errors ("unsupported type"),
	// so Handle fails after TryStart has claimed the invocation.
	event := map[string]any{"bad": make(chan int)}
	if err := r.Handle(ctx, "1757-0", event); err == nil {
		t.Fatal("expected a marshal error")
	}
	// The running marker must be cleared and a retry backoff recorded.
	if _, ok := prog.runningDeadline("user-events/index.run"); ok {
		t.Fatalf("running marker should be cleared after marshal error")
	}
	if len(prog.failures) != 1 {
		t.Fatalf("failures = %v, want one RecordFailure after marshal error", prog.failures)
	}
	// The executor must never have been reached (marshal failed before handoff).
	if exec.count() != 0 {
		t.Fatalf("executor calls = %d, want 0 (marshal failed before execution)", exec.count())
	}
}

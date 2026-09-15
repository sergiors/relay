package runner

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/runtime"
	"relay/internal/stream"
)

// stateProbeExecutor records whether the invocation was protected by an active
// running marker at the moment it executed, and how many times it ran. It lets
// tests assert that TryStart persisted the running deadline BEFORE execution.
type stateProbeExecutor struct {
	mu        sync.Mutex
	calls     int
	runningAt bool
	probe     func() (bool, bool) // returns (running, ok) for the invocation
}

func (e *stateProbeExecutor) Execute(ctx context.Context, prepared *runtime.Prepared, handler string, _ []byte, _ []string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	if e.probe != nil {
		running, ok := e.probe()
		if ok {
			e.runningAt = running
		}
	}
	return nil
}

func (e *stateProbeExecutor) got() (int, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls, e.runningAt
}

// schedFnRetries returns a prepared function with a single schedule entry for
// handler "index.run" carrying the given timeout and retry count.
func schedFnRetries(t *testing.T, name string, executor Executor, scheduleTimeout time.Duration, retries int) *PreparedFunction {
	t.Helper()
	return NewPrepared(
		function.Function{
			Name: name,
			Template: &function.Template{
				Runtime: "node24",
				Rules:   []function.Rule{{Handler: "index.run", Pattern: function.Pattern{}, Timeout: scheduleTimeout, Retries: function.DefaultRetries}},
				Schedules: []function.Schedule{{
					Handler:  "index.run",
					Cron:     "0 3 * * *",
					Location: time.UTC,
					Timeout:  scheduleTimeout,
					Retries:  retries,
				}},
			},
		},
		&runtime.Prepared{Name: name, Image: "x"},
		executor,
	)
}

// TestInvokeHandlerTryStartWritesRunningBeforeExecution verifies that with
// invocation state present, TryStart persists a running marker (attempt 1) for
// the "<function>/<handler>" invocation BEFORE the executor runs.
func TestInvokeHandlerTryStartWritesRunningBeforeExecution(t *testing.T) {
	prog := newFakeInvocationState()
	exec := &stateProbeExecutor{probe: func() (bool, bool) {
		_, ok := prog.runningDeadline("fn/index.run")
		return ok, ok
	}}
	r := NewWithMetrics([]*PreparedFunction{schedFnRetries(t, "fn", exec, function.DefaultTimeout, function.DefaultRetries)}, silentLogger(), nil)
	ctx := stream.WithInvocationState(context.Background(), prog)

	if err := r.InvokeHandler(ctx, "fn", "index.run", []byte(`{}`)); err != nil {
		t.Fatalf("InvokeHandler: %v", err)
	}
	calls, runningAt := exec.got()
	if calls != 1 {
		t.Fatalf("executor calls = %d, want 1", calls)
	}
	if !runningAt {
		t.Fatalf("running marker was not persisted before the executor ran")
	}
	// Attempt 1 was started.
	if got := prog.attempts["fn/index.run"]; got != 1 {
		t.Fatalf("attempt = %d, want 1", got)
	}
	if !prog.IsComplete("fn/index.run") {
		t.Fatalf("invocation should be marked complete after success")
	}
}

// TestInvokeHandlerSuccessMarksComplete verifies a successful execution marks
// the invocation complete and returns nil.
func TestInvokeHandlerSuccessMarksComplete(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{schedFnRetries(t, "fn", exec, function.DefaultTimeout, function.DefaultRetries)}, silentLogger(), nil)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	if err := r.InvokeHandler(ctx, "fn", "index.run", []byte(`{}`)); err != nil {
		t.Fatalf("InvokeHandler: %v", err)
	}
	if !prog.IsComplete("fn/index.run") {
		t.Fatalf("invocation should be marked complete after success")
	}
	if len(prog.marks) != 1 || prog.marks[0] != "fn/index.run" {
		t.Fatalf("marks = %v, want [fn/index.run]", prog.marks)
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls = %d, want 1", exec.count())
	}
}

// TestInvokeHandlerFailureRecordsRetryBackoff verifies a failed attempt (with a
// retryable budget left) records the attempt-1 backoff (1m) and returns a plain
// retryable error, not an exhausted or not-eligible one.
func TestInvokeHandlerFailureRecordsRetryBackoff(t *testing.T) {
	exec := &fixedExecutor{err: true}
	r := NewWithMetrics([]*PreparedFunction{schedFnRetries(t, "fn", exec, function.DefaultTimeout, function.DefaultRetries)}, silentLogger(), nil)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.InvokeHandler(ctx, "fn", "index.run", []byte(`{}`))
	if err == nil {
		t.Fatal("expected InvokeHandler to fail")
	}
	if errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("err = %v, must NOT be ErrInvocationExhausted", err)
	}
	if errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("err = %v, must NOT be ErrInvocationNotEligible", err)
	}
	if len(prog.failures) != 1 {
		t.Fatalf("failures = %v, want one RecordFailure", prog.failures)
	}
	if prog.failures[0] != time.Minute {
		t.Fatalf("failure backoff = %s, want 1m", prog.failures[0])
	}
	if prog.IsComplete("fn/index.run") {
		t.Fatalf("invocation must not be marked after failure")
	}
	if prog.IsTerminal("fn/index.run") {
		t.Fatalf("invocation must not be terminal (retryable) after attempt 1")
	}
}

// TestInvokeHandlerExhaustedAfterRetries verifies that a failure at attempt >=
// 1+retries marks the invocation exhausted and returns an error wrapping
// stream.ErrInvocationExhausted (the stream routes the schedule message to the
// DLQ).
func TestInvokeHandlerExhaustedAfterRetries(t *testing.T) {
	exec := &fixedExecutor{err: true}
	r := NewWithMetrics([]*PreparedFunction{schedFnRetries(t, "fn", exec, function.DefaultTimeout, 0)}, silentLogger(), nil)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.InvokeHandler(ctx, "fn", "index.run", []byte(`{}`))
	if !errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("err = %v, want ErrInvocationExhausted", err)
	}
	if !prog.IsTerminal("fn/index.run") {
		t.Fatalf("invocation should be terminal (exhausted) after retries:0 failure")
	}
	if len(prog.failures) != 0 {
		t.Fatalf("failures = %v, want none (exhausted, not retried)", prog.failures)
	}
	if got := prog.exhausted["fn/index.run"]; got != 1 {
		t.Fatalf("exhausted attempts = %d, want 1", got)
	}
}

// TestInvokeHandlerSkipsCompletedOnRedelivery verifies a pre-marked "ok"
// invocation is not executed and InvokeHandler returns nil (the stream ACKs and
// clears state).
func TestInvokeHandlerSkipsCompletedOnRedelivery(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{schedFnRetries(t, "fn", exec, function.DefaultTimeout, function.DefaultRetries)}, silentLogger(), nil)
	prog := newFakeInvocationState()
	prog.done["fn/index.run"] = true
	ctx := stream.WithInvocationState(context.Background(), prog)

	if err := r.InvokeHandler(ctx, "fn", "index.run", []byte(`{}`)); err != nil {
		t.Fatalf("InvokeHandler: %v (want nil so the stream ACKs)", err)
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls = %d, want 0 (skipped)", exec.count())
	}
}

// TestInvokeHandlerProtectedRunningNotEligible verifies that when TryStart
// reports a protected invocation (wait > 0), the executor is not called and the
// returned error wraps stream.ErrInvocationNotEligible (the stream leaves the
// message pending).
func TestInvokeHandlerProtectedRunningNotEligible(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{schedFnRetries(t, "fn", exec, function.DefaultTimeout, function.DefaultRetries)}, silentLogger(), nil)
	prog := newFakeInvocationState()
	now := time.Now()
	prog.setClock(func() time.Time { return now })
	// Mark the invocation protected by a future retry backoff so TryStart reports
	// not started with wait > 0.
	prog.nextAt["fn/index.run"] = now.Add(time.Hour)
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.InvokeHandler(ctx, "fn", "index.run", []byte(`{}`))
	if !errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("err = %v, want ErrInvocationNotEligible", err)
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls = %d, want 0 (protected invocation skipped)", exec.count())
	}
}

// TestInvokeHandlerSlotTimeoutLeavesPending verifies that when the per-function
// concurrency slot is held and no slot frees within the (test-overridden)
// slotWait, InvokeHandler returns ErrInvocationNotEligible and writes NO state
// (no TryStart, no RecordFailure, no MarkComplete) — a slot timeout is never an
// attempt.
func TestInvokeHandlerSlotTimeoutLeavesPending(t *testing.T) {
	release := make(chan struct{})
	holding := newHoldingExecutor(release)
	r := NewWithMetrics([]*PreparedFunction{schedFnRetries(t, "fn", holding, function.DefaultTimeout, function.DefaultRetries)}, silentLogger(), nil)
	r.SetMaxConcurrency(1)
	r.slotWait = 50 * time.Millisecond

	// First invocation acquires the single per-function slot and blocks.
	firstDone := make(chan struct{})
	go func() {
		_ = r.InvokeHandler(context.Background(), "fn", "index.run", []byte(`{}`))
		close(firstDone)
	}()
	holding.waitEntered()

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)
	err := r.InvokeHandler(ctx, "fn", "index.run", []byte(`{}`))
	if !errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("err = %v, want ErrInvocationNotEligible (slot timeout)", err)
	}
	// No state written: no attempts started, no marks, no failures.
	if len(prog.attempts) != 0 {
		t.Fatalf("attempts = %v, want none (slot timeout is not an attempt)", prog.attempts)
	}
	if len(prog.marks) != 0 || len(prog.failures) != 0 {
		t.Fatalf("marks=%v failures=%v, want none after slot timeout", prog.marks, prog.failures)
	}

	close(release)
	<-firstDone
}

// TestInvokeHandlerNoStateUnchanged verifies that with no invocation state in
// ctx, the legacy behavior is preserved: the executor runs, a success returns
// nil, and no state methods are touched.
func TestInvokeHandlerNoStateUnchanged(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{schedFnRetries(t, "fn", exec, function.DefaultTimeout, function.DefaultRetries)}, silentLogger(), nil)

	if err := r.InvokeHandler(context.Background(), "fn", "index.run", []byte(`{}`)); err != nil {
		t.Fatalf("InvokeHandler: %v", err)
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls = %d, want 1", exec.count())
	}

	// A failing no-state invocation returns a plain error.
	rfail := NewWithMetrics([]*PreparedFunction{schedFnRetries(t, "fn", &fixedExecutor{err: true}, function.DefaultTimeout, function.DefaultRetries)}, silentLogger(), nil)
	err := rfail.InvokeHandler(context.Background(), "fn", "index.run", []byte(`{}`))
	if err == nil {
		t.Fatal("expected no-state failure to return an error")
	}
	if errors.Is(err, stream.ErrInvocationExhausted) || errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("no-state failure err = %v, want plain", err)
	}
}

// TestInvokeHandlerRespectsScheduleRetries pins that the exhaustion decision
// comes from the template's schedule Retries: a schedule with retries:0 fails
// once and exhausts on attempt 1 (no RecordFailure), while a schedule with
// retries:4 marks the first failure retryable (RecordFailure).
func TestInvokeHandlerRespectsScheduleRetries(t *testing.T) {
	// retries: 0 → a single failure exhausts immediately.
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)
	r0 := NewWithMetrics([]*PreparedFunction{schedFnRetries(t, "fn", &fixedExecutor{err: true}, function.DefaultTimeout, 0)}, silentLogger(), nil)
	if err := r0.InvokeHandler(ctx, "fn", "index.run", []byte(`{}`)); !errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("retries:0 err = %v, want ErrInvocationExhausted", err)
	}
	if len(prog.failures) != 0 || !prog.IsTerminal("fn/index.run") {
		t.Fatalf("retries:0 should exhaust (failures=%v terminal=%v)", prog.failures, prog.IsTerminal("fn/index.run"))
	}

	// retries: 4 → a single failure is retryable (recorded backoff), not terminal.
	prog2 := newFakeInvocationState()
	ctx2 := stream.WithInvocationState(context.Background(), prog2)
	r4 := NewWithMetrics([]*PreparedFunction{schedFnRetries(t, "fn", &fixedExecutor{err: true}, function.DefaultTimeout, 4)}, silentLogger(), nil)
	if err := r4.InvokeHandler(ctx2, "fn", "index.run", []byte(`{}`)); err == nil || errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("retries:4 first-failure err = %v, want plain retryable", err)
	}
	if len(prog2.failures) != 1 || prog2.IsTerminal("fn/index.run") {
		t.Fatalf("retries:4 should be retryable (failures=%v terminal=%v)", prog2.failures, prog2.IsTerminal("fn/index.run"))
	}
}

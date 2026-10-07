package runner

import (
	"context"
	"errors"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/runtime"
	"relay/internal/stream"
	"relay/internal/testutil"
)

// warmAdmitterExecutor is a countingExecutor that also implements
// runtime.WarmAdmitter, so the runner's pre-TryStart warm-budget admission seam
// is exercisable without a Docker daemon. admitErr simulates a saturated budget
// (every warm container busy); a nil admitErr admits unconditionally (a nil
// permit, so Execute acquires its own). executeErr, when set, is returned by
// Execute (to exercise the create-time saturation sentinel mapping).
type warmAdmitterExecutor struct {
	countingExecutor
	admitErr   error
	executeErr error
	// admitBlock, when non-nil, makes AcquireWarmPermit block until it is closed
	// or the passed context is done (to exercise the runner's slotWait bound).
	admitBlock chan struct{}
	// admits counts AcquireWarmPermit calls.
	admits int
}

func (e *warmAdmitterExecutor) AcquireWarmPermit(ctx context.Context) (*runtime.WarmPermit, error) {
	e.admits++
	if e.admitBlock != nil {
		select {
		case <-e.admitBlock:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if e.admitErr != nil {
		return nil, e.admitErr
	}
	return nil, nil
}

func (e *warmAdmitterExecutor) Execute(ctx context.Context, prepared *runtime.Prepared, handler string, eventJSON []byte, env []string) error {
	if e.executeErr != nil {
		e.mu.Lock()
		e.calls++
		e.mu.Unlock()
		return e.executeErr
	}
	return e.countingExecutor.Execute(ctx, prepared, handler, eventJSON, env)
}

// TestRunnerWarmBudgetSaturationLeavesPendingWithoutAttempt pins the core
// guarantee: when the worker-global warm budget is saturated, Handle leaves the
// invocation pending (ErrInvocationNotEligible) WITHOUT claiming a handler
// attempt and WITHOUT charging a retry or exhaustion. The executor is never
// called.
func TestRunnerWarmBudgetSaturationLeavesPendingWithoutAttempt(t *testing.T) {
	exec := &warmAdmitterExecutor{admitErr: errors.New("warm budget saturated")}
	app := buildFn(fnSpec{name: "fn", rules: []app.EventRule{alwaysMatchRule(time.Second)}}, exec)
	r := NewWithMetrics([]*PreparedApp{app}, testutil.DiscardLogger(), nil)
	r.SetMaxConcurrentInvocations(2)

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.Handle(ctx, "m-1", map[string]any{"status": "ok"})
	if !errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("Handle under warm saturation = %v, want ErrInvocationNotEligible", err)
	}
	if exec.calls != 0 {
		t.Fatalf("executor called %d times under saturation, want 0", exec.calls)
	}
	prog.mu.Lock()
	defer prog.mu.Unlock()
	if len(prog.attempts) != 0 {
		t.Fatalf("handler attempts claimed = %v, want none (no retry charge)", prog.attempts)
	}
	if len(prog.failures) != 0 || len(prog.exhausted) != 0 {
		t.Fatalf("failures=%v exhausted=%v, want none under saturation", prog.failures, prog.exhausted)
	}
}

// TestRunnerWarmBudgetAdmissionSuccessExecutes pins that an admitted invocation
// executes normally: the admitter is consulted, the handler runs, and the
// invocation is marked complete.
func TestRunnerWarmBudgetAdmissionSuccessExecutes(t *testing.T) {
	exec := &warmAdmitterExecutor{}
	app := buildFn(fnSpec{name: "fn", rules: []app.EventRule{alwaysMatchRule(time.Second)}}, exec)
	r := NewWithMetrics([]*PreparedApp{app}, testutil.DiscardLogger(), nil)
	r.SetMaxConcurrentInvocations(2)

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	if err := r.Handle(ctx, "m-1", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("Handle = %v, want nil", err)
	}
	if exec.calls != 1 {
		t.Fatalf("executor calls = %d, want 1", exec.calls)
	}
	if exec.admits != 1 {
		t.Fatalf("warm admits = %d, want 1", exec.admits)
	}
	if !prog.IsComplete("fn/index.run") {
		t.Fatal("invocation was not marked complete")
	}
}

// TestRunnerWarmBudgetScheduleSaturationLeavesPending pins the schedule path:
// a saturated warm budget returns ErrInvocationNotEligible with invocation state
// (message stays pending, no attempt claimed).
func TestRunnerWarmBudgetScheduleSaturationLeavesPending(t *testing.T) {
	exec := &warmAdmitterExecutor{admitErr: errors.New("warm budget saturated")}
	app := schedFn(t, "fn", exec, time.Second)
	r := NewWithMetrics([]*PreparedApp{app}, testutil.DiscardLogger(), nil)
	r.SetMaxConcurrentInvocations(2)

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.InvokeHandler(ctx, "m-1", "fn", "sched", "index.run", []byte(`{}`))
	if !errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("InvokeHandler under saturation = %v, want ErrInvocationNotEligible", err)
	}
	if exec.calls != 0 {
		t.Fatalf("executor called %d times under saturation, want 0", exec.calls)
	}
	prog.mu.Lock()
	defer prog.mu.Unlock()
	if len(prog.attempts) != 0 {
		t.Fatalf("handler attempts claimed = %v, want none", prog.attempts)
	}
}

// TestRunnerWarmBudgetAdmissionWaitBoundedBySlotWait pins that a blocking
// admission is bounded by the runner's slotWait and surfaces as a pending skip
// rather than hanging: the fake admitter blocks, slotWait is short, and Handle
// returns ErrInvocationNotEligible without executing.
func TestRunnerWarmBudgetAdmissionWaitBoundedBySlotWait(t *testing.T) {
	exec := &warmAdmitterExecutor{admitBlock: make(chan struct{})}
	app := buildFn(fnSpec{name: "fn", rules: []app.EventRule{alwaysMatchRule(time.Second)}}, exec)
	r := NewWithMetrics([]*PreparedApp{app}, testutil.DiscardLogger(), nil)
	r.SetMaxConcurrentInvocations(2)
	r.slotWait = 40 * time.Millisecond
	r.warmWait = 40 * time.Millisecond

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	start := time.Now()
	err := r.Handle(ctx, "m-1", map[string]any{"status": "ok"})
	if !errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("Handle with blocked admission = %v, want ErrInvocationNotEligible", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("blocked admission took %v, want ~slotWait", elapsed)
	}
	if exec.calls != 0 {
		t.Fatalf("executor called %d times, want 0", exec.calls)
	}
}

// TestRunnerWarmBudgetCreateSaturationLeavesPendingWithoutCharge pins the
// create-time saturation race: admission succeeded (an idle container existed)
// but the container create then reported ErrWarmBudgetSaturated. Handle must
// leave the invocation pending (ErrInvocationNotEligible) with NO handler
// failure/retry/exhaustion accounting and no DLQ.
func TestRunnerWarmBudgetCreateSaturationLeavesPendingWithoutCharge(t *testing.T) {
	exec := &warmAdmitterExecutor{executeErr: runtime.ErrWarmBudgetSaturated}
	app := buildFn(fnSpec{name: "fn", rules: []app.EventRule{alwaysMatchRule(time.Second)}}, exec)
	r := NewWithMetrics([]*PreparedApp{app}, testutil.DiscardLogger(), nil)
	r.SetMaxConcurrentInvocations(2)

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.Handle(ctx, "m-1", map[string]any{"status": "ok"})
	if !errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("Handle on create-time saturation = %v, want ErrInvocationNotEligible", err)
	}
	prog.mu.Lock()
	defer prog.mu.Unlock()
	if len(prog.failures) != 0 || len(prog.exhausted) != 0 {
		t.Fatalf("failures=%v exhausted=%v, want none (backpressure, not failure)", prog.failures, prog.exhausted)
	}
}

// TestRunnerWarmBudgetScheduleCreateSaturationLeavesPending pins the same
// create-time saturation mapping on the schedule path.
func TestRunnerWarmBudgetScheduleCreateSaturationLeavesPending(t *testing.T) {
	exec := &warmAdmitterExecutor{executeErr: runtime.ErrWarmBudgetSaturated}
	app := schedFn(t, "fn", exec, time.Second)
	r := NewWithMetrics([]*PreparedApp{app}, testutil.DiscardLogger(), nil)
	r.SetMaxConcurrentInvocations(2)

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.InvokeHandler(ctx, "m-1", "fn", "sched", "index.run", []byte(`{}`))
	if !errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("InvokeHandler on create-time saturation = %v, want ErrInvocationNotEligible", err)
	}
	prog.mu.Lock()
	defer prog.mu.Unlock()
	if len(prog.failures) != 0 || len(prog.exhausted) != 0 {
		t.Fatalf("failures=%v exhausted=%v, want none", prog.failures, prog.exhausted)
	}
}

// TestRunnerWarmBudgetNilAdmitterIsUnconditional pins that an executor without
// the warm-admitter capability (every test fake, in-process executors) admits
// unconditionally: the seam is opt-in and never blocks the existing paths.
func TestRunnerWarmBudgetNilAdmitterIsUnconditional(t *testing.T) {
	exec := &countingExecutor{}
	app := buildFn(fnSpec{name: "fn", rules: []app.EventRule{alwaysMatchRule(time.Second)}}, exec)
	r := NewWithMetrics([]*PreparedApp{app}, testutil.DiscardLogger(), nil)
	r.SetMaxConcurrentInvocations(2)

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)
	if err := r.Handle(ctx, "m-1", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("Handle = %v, want nil", err)
	}
	if exec.calls != 1 {
		t.Fatalf("executor calls = %d, want 1", exec.calls)
	}
}

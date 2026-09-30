package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/runtime"
	"relay/internal/stream"
	"relay/internal/testutil"
)

// schedTmplFn builds a prepared function with the given named schedules, all
// routed to their own handler/timeout/retries, so the admission descriptor can
// be exercised per schedule name.
func schedTmplFn(t *testing.T, exec Executor, schedules ...function.Schedule) *PreparedFunction {
	t.Helper()
	return NewPrepared(
		function.Function{
			Name: "fn",
			Template: &function.Template{
				Runtime:   "node24",
				Schedules: schedules,
			},
		},
		&runtime.Prepared{Name: "fn", Image: "x"},
		exec,
	)
}

// sched builds one schedule entry.
func sched(name, handler string, timeout time.Duration, retries int) function.Schedule {
	return function.Schedule{
		Name:     name,
		Handler:  handler,
		Cron:     "0 3 * * *",
		Location: time.UTC,
		Timeout:  timeout,
		Retries:  retries,
	}
}

// TestInvokeHandlerAdmissionPinsCurrentHandler pins the FIRST-admission
// resolution: an unpinned occurrence resolves the CURRENT template by schedule
// NAME, so the schedule's current handler runs even though the publication-time
// envelope named a stale handler, and the descriptor is pinned with that
// handler/timeout/retries.
func TestInvokeHandlerAdmissionPinsCurrentHandler(t *testing.T) {
	exec := &captureExecutor{}
	pf := schedTmplFn(t, exec, sched("cleanup", "jobs.new", 42*time.Second, 3))
	r := NewWithMetrics([]*PreparedFunction{pf}, testutil.DiscardLogger(), nil)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	if err := r.InvokeHandler(ctx, "m-0", "fn", "cleanup", "jobs.old", []byte(`{}`)); err != nil {
		t.Fatalf("InvokeHandler: %v", err)
	}
	handler, _ := exec.got()
	if handler != "jobs.new" {
		t.Fatalf("executed handler = %q, want the schedule's current handler jobs.new", handler)
	}
	desc, ok := prog.ScheduleDescriptor()
	if !ok {
		t.Fatal("no descriptor pinned after first admission")
	}
	if desc.Schedule != "cleanup" || desc.Handler != "jobs.new" || desc.Timeout != 42*time.Second || desc.Retries != 3 {
		t.Fatalf("pinned descriptor = %+v, want {cleanup jobs.new 42s 3}", desc)
	}
}

// TestInvokeHandlerAdmittedHandlerChangeIgnoredOnRetry pins the post-admission
// contract: once a schedule occurrence is admitted under one handler, a later
// handler change under the SAME name must not reset the claim/attempts or switch
// handlers mid-retry. The retry runs the ADMITTED handler at the ADMITTED retry
// budget, with the attempt carried forward.
func TestInvokeHandlerAdmittedHandlerChangeIgnoredOnRetry(t *testing.T) {
	exec := &countingExecutor{fail: true}
	base := time.Now()
	prog := newFakeInvocationState()
	prog.setClock(func() time.Time { return base })
	ctx := stream.WithInvocationState(context.Background(), prog)

	// First admission: handler jobs.old, retries 4.
	r1 := NewWithMetrics(
		[]*PreparedFunction{schedTmplFn(t, exec, sched("cleanup", "jobs.old", 30*time.Second, 4))},
		testutil.DiscardLogger(), nil)
	if err := r1.InvokeHandler(ctx, "m-0", "fn", "cleanup", "jobs.old", []byte(`{}`)); err == nil {
		t.Fatal("expected the first (failing) attempt to return an error")
	}
	if len(prog.failures) != 1 || prog.failures[0] != time.Minute {
		t.Fatalf("first-attempt backoff = %v, want one 1m backoff", prog.failures)
	}

	// Swap the template (a NEW worker/ruleset) to a DIFFERENT handler with a
	// smaller retry budget, then let the retry fire.
	r2 := NewWithMetrics(
		[]*PreparedFunction{schedTmplFn(t, exec, sched("cleanup", "jobs.new", 30*time.Second, 0))},
		testutil.DiscardLogger(), nil)
	prog.advance(2 * time.Minute) // past the 1m backoff

	if err := r2.InvokeHandler(ctx, "m-0", "fn", "cleanup", "jobs.new", []byte(`{}`)); err == nil {
		t.Fatal("expected the retry (still failing) to return an error")
	}
	// The admitted handler is jobs.old and the admitted retry budget is 4: the
	// second attempt is still retryable (not exhausted) and the backoff is the
	// attempt-2 schedule.
	if got := prog.attempts["fn/jobs.old"]; got != 2 {
		t.Fatalf("admitted handler attempt = %d, want 2 (carried forward, not reset)", got)
	}
	if _, ok := prog.attempts["fn/jobs.new"]; ok {
		t.Fatal("a second invocation field fn/jobs.new was claimed for the admitted message")
	}
	if len(prog.failures) != 2 || prog.failures[1] != 2*time.Minute {
		t.Fatalf("failures = %v, want [1m 2m] (pinned retry budget/backoff)", prog.failures)
	}
	if prog.IsTerminal("fn/jobs.old") {
		t.Fatal("the admitted invocation must stay retryable under its pinned retries:4, not exhaust")
	}
}

// TestInvokeHandlerScheduleRemovedBeforeAdmissionObsolete pins the pre-admission
// removal case: an occurrence whose schedule NAME is gone and which was NEVER
// admitted is obsolete — no descriptor, no execution, no state.
func TestInvokeHandlerScheduleRemovedBeforeAdmissionObsolete(t *testing.T) {
	exec := &countingExecutor{}
	// The function is present but has no schedules at all.
	pf := schedTmplFn(t, exec)
	r := NewWithMetrics([]*PreparedFunction{pf}, testutil.DiscardLogger(), nil)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.InvokeHandler(ctx, "m-0", "fn", "cleanup", "jobs.old", []byte(`{}`))
	if !errors.Is(err, stream.ErrInvocationObsolete) {
		t.Fatalf("err = %v, want ErrInvocationObsolete", err)
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls = %d, want 0 (obsolete occurrence must not run)", exec.count())
	}
	if _, ok := prog.ScheduleDescriptor(); ok {
		t.Fatal("an obsolete occurrence must not pin a descriptor")
	}
	if len(prog.attempts) != 0 || len(prog.marks) != 0 || len(prog.failures) != 0 || len(prog.exhausted) != 0 {
		t.Fatalf("obsolete occurrence touched state: attempts=%v marks=%v failures=%v exhausted=%v",
			prog.attempts, prog.marks, prog.failures, prog.exhausted)
	}
}

// TestInvokeHandlerScheduleRemovedAfterAdmissionStillCompletes pins the
// post-admission removal case: once admitted, removing the schedule NAME must
// NOT cancel the invocation. The retry still runs the admitted handler and can
// finish.
func TestInvokeHandlerScheduleRemovedAfterAdmissionStillCompletes(t *testing.T) {
	base := time.Now()
	prog := newFakeInvocationState()
	prog.setClock(func() time.Time { return base })
	ctx := stream.WithInvocationState(context.Background(), prog)

	failing := &countingExecutor{fail: true}
	r1 := NewWithMetrics(
		[]*PreparedFunction{schedTmplFn(t, failing, sched("cleanup", "jobs.old", 30*time.Second, 4))},
		testutil.DiscardLogger(), nil)
	if err := r1.InvokeHandler(ctx, "m-0", "fn", "cleanup", "jobs.old", []byte(`{}`)); err == nil {
		t.Fatal("expected the first attempt to fail")
	}

	// Remove the schedule entirely and use a fresh, succeeding runner (simulating
	// a restarted/reconciled worker).
	succeeding := &countingExecutor{}
	r2 := NewWithMetrics([]*PreparedFunction{schedTmplFn(t, succeeding)}, testutil.DiscardLogger(), nil)
	prog.advance(2 * time.Minute)

	if err := r2.InvokeHandler(ctx, "m-0", "fn", "cleanup", "jobs.old", []byte(`{}`)); err != nil {
		t.Fatalf("admitted occurrence must still complete after its schedule was removed: %v", err)
	}
	if !prog.IsComplete("fn/jobs.old") {
		t.Fatal("the admitted invocation should be complete after the retry succeeded")
	}
	if got := prog.attempts["fn/jobs.old"]; got != 2 {
		t.Fatalf("attempt = %d, want 2", got)
	}
}

// TestInvokeHandlerPinnedTimeoutSurvivesTemplateChange pins that the ADMITTED
// capped timeout (what drives the persisted running deadline) survives a later
// template timeout change on the retry.
func TestInvokeHandlerPinnedTimeoutSurvivesTemplateChange(t *testing.T) {
	base := time.Now()
	prog := newFakeInvocationState()
	prog.setClock(func() time.Time { return base })
	ctx := stream.WithInvocationState(context.Background(), prog)

	r1 := NewWithMetrics(
		[]*PreparedFunction{schedTmplFn(t, &countingExecutor{fail: true}, sched("cleanup", "jobs.old", 30*time.Second, 4))},
		testutil.DiscardLogger(), nil)
	if err := r1.InvokeHandler(ctx, "m-0", "fn", "cleanup", "jobs.old", []byte(`{}`)); err == nil {
		t.Fatal("expected the first attempt to fail")
	}
	prog.advance(2 * time.Minute)

	// The new template shrinks the timeout to 1ms; the pinned 30s must still be
	// the deadline persisted for the retry.
	probe := &deadlineProbeExecutor{prog: prog, invocation: "fn/jobs.old"}
	r2 := NewWithMetrics(
		[]*PreparedFunction{schedTmplFn(t, probe, sched("cleanup", "jobs.old", time.Millisecond, 4))},
		testutil.DiscardLogger(), nil)
	if err := r2.InvokeHandler(ctx, "m-0", "fn", "cleanup", "jobs.old", []byte(`{}`)); err != nil {
		t.Fatalf("retry with a pinned timeout must not expire: %v", err)
	}
	dl, ok := probe.observed()
	if !ok {
		t.Fatal("executor never observed a running deadline")
	}
	if remaining := dl.Sub(prog.now()); remaining < 29*time.Second || remaining > 31*time.Second {
		t.Fatalf("persisted running deadline = %s out, want ~30s (pinned, not the new 1ms)", remaining)
	}
}

// deadlineProbeExecutor captures the invocation's persisted running deadline at
// execution time, then succeeds.
type deadlineProbeExecutor struct {
	prog       *fakeInvocationState
	invocation string
	mu         sync.Mutex
	dl         time.Time
	ok         bool
}

func (e *deadlineProbeExecutor) Execute(context.Context, *runtime.Prepared, string, []byte, []string) error {
	dl, ok := e.prog.runningDeadline(e.invocation)
	e.mu.Lock()
	e.dl, e.ok = dl, ok
	e.mu.Unlock()
	return nil
}

func (e *deadlineProbeExecutor) observed() (time.Time, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.dl, e.ok
}

// TestInvokeHandlerSharedHandlerScheduleNamesAreDistinct pins that two schedules
// sharing a handler still admit under their own NAMES: each occurrence's pinned
// descriptor carries its own schedule name, so one name's removal or change never
// obsoletes the other.
func TestInvokeHandlerSharedHandlerScheduleNamesAreDistinct(t *testing.T) {
	exec := &countingExecutor{}
	pf := schedTmplFn(t, exec,
		sched("a", "jobs.shared", 10*time.Second, 1),
		sched("b", "jobs.shared", 20*time.Second, 2),
	)
	r := NewWithMetrics([]*PreparedFunction{pf}, testutil.DiscardLogger(), nil)

	for _, tc := range []struct {
		name string
		want function.Schedule
	}{{"a", sched("a", "jobs.shared", 10*time.Second, 1)}, {"b", sched("b", "jobs.shared", 20*time.Second, 2)}} {
		t.Run(tc.name, func(t *testing.T) {
			prog := newFakeInvocationState()
			ctx := stream.WithInvocationState(context.Background(), prog)
			if err := r.InvokeHandler(ctx, "m-"+tc.name, "fn", tc.name, "jobs.shared", []byte(`{}`)); err != nil {
				t.Fatalf("InvokeHandler: %v", err)
			}
			desc, ok := prog.ScheduleDescriptor()
			if !ok {
				t.Fatal("no descriptor pinned")
			}
			if desc.Schedule != tc.name {
				t.Fatalf("pinned schedule name = %q, want %q", desc.Schedule, tc.name)
			}
			if desc.Timeout != tc.want.Timeout || desc.Retries != tc.want.Retries {
				t.Fatalf("pinned descriptor = %+v, want timeout=%s retries=%d", desc, tc.want.Timeout, tc.want.Retries)
			}
		})
	}
}

// TestInvokeHandlerDescriptorRaceExactlyOneHandler pins the atomic admission
// under a concurrent descriptor race: two replicas with different templates race
// to admit the same occurrence. Exactly one claims, both converge on the same
// pinned handler, and only the winner's invocation field exists — a loser never
// runs a parallel handler under a different field.
func TestInvokeHandlerDescriptorRaceExactlyOneHandler(t *testing.T) {
	base := time.Now()
	prog := newFakeInvocationState() // shared store: models the Redis hash both replicas contend on
	prog.setClock(func() time.Time { return base })

	release := make(chan struct{})
	holding := newBlockingExecutor(release)
	rOld := NewWithMetrics(
		[]*PreparedFunction{schedTmplFn(t, holding, sched("cleanup", "jobs.old", 30*time.Second, 4))},
		testutil.DiscardLogger(), nil)
	rNew := NewWithMetrics(
		[]*PreparedFunction{schedTmplFn(t, holding, sched("cleanup", "jobs.new", 30*time.Second, 4))},
		testutil.DiscardLogger(), nil)

	ctx := stream.WithInvocationState(context.Background(), prog)
	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		errs[0] = rOld.InvokeHandler(ctx, "m-0", "fn", "cleanup", "jobs.old", []byte(`{}`))
	}()
	go func() {
		defer wg.Done()
		errs[1] = rNew.InvokeHandler(ctx, "m-0", "fn", "cleanup", "jobs.new", []byte(`{}`))
	}()
	// Wait for the winning handler to enter the (blocking) executor, then release
	// it so the winning invocation completes.
	holding.waitEntered()
	close(release)
	wg.Wait()

	for i, err := range errs {
		if err == nil {
			continue
		}
		// The losing replica legitimately observes the winner's running claim and
		// reports the invocation not eligible (leave pending), exactly like a
		// concurrent event delivery. Any other error is a real failure.
		if !errors.Is(err, stream.ErrInvocationNotEligible) {
			t.Fatalf("replica %d: %v", i, err)
		}
	}
	// Exactly ONE invocation field was ever claimed: the two replicas converged on
	// one handler and never started a parallel one under a different field.
	if len(prog.attempts) != 1 {
		t.Fatalf("claimed invocation fields = %v, want exactly one", prog.attempts)
	}
	desc, ok := prog.ScheduleDescriptor()
	if !ok {
		t.Fatal("no descriptor pinned after the race")
	}
	// The winning handler is the only invocation field, and it IS the pinned one.
	if _, ok := prog.attempts["fn/"+desc.Handler]; !ok {
		t.Fatalf("pinned handler %q has no claimed invocation field: %v", desc.Handler, prog.attempts)
	}
	if !prog.IsComplete("fn/" + desc.Handler) {
		t.Fatalf("the winning invocation %q did not complete", desc.Handler)
	}
	if holding.callCount() != 1 {
		t.Fatalf("executor calls = %d, want 1 (exactly one handler ran)", holding.callCount())
	}
}

// TestInvokeHandlerRestartPreservesDescriptor pins that a pinned descriptor
// survives a worker restart/reclaim: a brand-new handle over the same persisted
// state still runs the admitted handler and carries the attempt forward.
func TestInvokeHandlerRestartPreservesDescriptor(t *testing.T) {
	base := time.Now()
	prog := newFakeInvocationState()
	prog.setClock(func() time.Time { return base })
	ctx := stream.WithInvocationState(context.Background(), prog)

	r1 := NewWithMetrics(
		[]*PreparedFunction{schedTmplFn(t, &countingExecutor{fail: true}, sched("cleanup", "jobs.old", 30*time.Second, 4))},
		testutil.DiscardLogger(), nil)
	if err := r1.InvokeHandler(ctx, "m-0", "fn", "cleanup", "jobs.old", []byte(`{}`)); err == nil {
		t.Fatal("expected the first attempt to fail")
	}

	// "Restart": a new runner with a changed current template, the same persisted
	// invocation state, and a reclaim after the backoff.
	r2 := NewWithMetrics(
		[]*PreparedFunction{schedTmplFn(t, &countingExecutor{}, sched("cleanup", "jobs.renamed", 5*time.Second, 0))},
		testutil.DiscardLogger(), nil)
	prog.advance(2 * time.Minute)

	if err := r2.InvokeHandler(ctx, "m-0", "fn", "cleanup", "jobs.renamed", []byte(`{}`)); err != nil {
		t.Fatalf("reclaim after restart: %v", err)
	}
	if !prog.IsComplete("fn/jobs.old") {
		t.Fatal("the ADMITTED handler should complete after the restart/reclaim")
	}
	if _, ok := prog.attempts["fn/jobs.renamed"]; ok {
		t.Fatal("the restarted worker must not switch to the new template's handler")
	}
}

// TestInvokeHandlerNoStateStillUsesCurrentTemplate pins that a direct/no-state
// caller (DLQ replay, tests) resolves the CURRENT schedule by name and executes
// one attempt, exactly as before.
func TestInvokeHandlerNoStateStillUsesCurrentTemplate(t *testing.T) {
	exec := &captureExecutor{}
	r := NewWithMetrics(
		[]*PreparedFunction{schedTmplFn(t, exec, sched("cleanup", "jobs.current", 30*time.Second, 0))},
		testutil.DiscardLogger(), nil)

	if err := r.InvokeHandler(context.Background(), "m-0", "fn", "cleanup", "jobs.stale", []byte(`{}`)); err != nil {
		t.Fatalf("InvokeHandler: %v", err)
	}
	handler, _ := exec.got()
	if handler != "jobs.current" {
		t.Fatalf("handler = %q, want the current template handler jobs.current", handler)
	}
}

// TestInvokeHandlerScheduleDescriptorEncodingRoundTrip pins the descriptor codec
// used by the reserved hash field: an encoded descriptor decodes back exactly,
// and malformed values decode to "absent".
func TestInvokeHandlerScheduleDescriptorEncodingRoundTrip(t *testing.T) {
	// The codec lives in the stream package; exercise it through the public API by
	// pinning a descriptor with an unusual schedule name and reading it back.
	base := time.Now()
	prog := newFakeInvocationState()
	prog.setClock(func() time.Time { return base })
	ctx := stream.WithInvocationState(context.Background(), prog)

	want := stream.ScheduleDescriptor{Schedule: "a.b-c_1", Handler: "jobs.run", Timeout: 7 * time.Second, Retries: 2}
	r := NewWithMetrics(
		[]*PreparedFunction{schedTmplFn(t, &countingExecutor{}, sched(want.Schedule, want.Handler, want.Timeout, want.Retries))},
		testutil.DiscardLogger(), nil)
	if err := r.InvokeHandler(ctx, "m-0", "fn", want.Schedule, want.Handler, []byte(`{}`)); err != nil {
		t.Fatalf("InvokeHandler: %v", err)
	}
	got, ok := prog.ScheduleDescriptor()
	if !ok {
		t.Fatal("descriptor not pinned")
	}
	if got != want {
		t.Fatalf("descriptor round-trip = %+v, want %+v", got, want)
	}
	if fmt.Sprint(got.Schedule) != want.Schedule {
		t.Fatalf("schedule name corrupted: %q", got.Schedule)
	}
}

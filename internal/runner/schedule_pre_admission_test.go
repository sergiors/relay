package runner

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"relay/internal/function"
	"relay/internal/runtime"
	"relay/internal/stream"
	"relay/internal/testutil"
)

// This file pins the PRE-ADMISSION config-refresh boundary: an UNADMITTED
// schedule occurrence (scheduleName != "" and no pinned descriptor) may block
// arbitrarily long in reserveSlots waiting for a concurrency slot, during which
// a reload can hot-swap or remove the function. The schedule/config snapshot
// that becomes the atomic first claim's proposal MUST therefore be re-read from
// the registry AFTER the slot is admitted, so a completed reload is observed
// (or the occurrence is obsoleted). A descriptor already pinned by an earlier
// first admission is authoritative and is never re-resolved.
//
// The tests use testing/synctest so "the occurrence is parked in the slot wait"
// is a deterministic, sleep-free state: the bubble's fake clock only advances
// once every goroutine is durably blocked, so the reload always lands strictly
// before the slot is released with no time-based flakiness.

// schedLeaseExecutor is a capture executor whose publications carry REAL
// admitted image leases from a runtime.Manager, so the pin-swap on the
// pre-admission refresh can be asserted against the authoritative lease gate
// (no Docker). It records the handler it was asked to run.
type schedLeaseExecutor struct {
	*leaseExecutor
	mu      sync.Mutex
	handler string
	calls   int
}

func newSchedLeaseExecutor() *schedLeaseExecutor {
	return &schedLeaseExecutor{leaseExecutor: newLeaseExecutor()}
}

func (e *schedLeaseExecutor) Execute(_ context.Context, _ *runtime.Prepared, handler string, _ []byte, _ []string) error {
	e.mu.Lock()
	e.handler = handler
	e.calls++
	e.mu.Unlock()
	return nil
}

func (e *schedLeaseExecutor) got() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.handler
}

func (e *schedLeaseExecutor) callCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

// schedLeaseFn builds a schedule-carrying PreparedFunction whose publication
// carries a real admitted lease on image (mirroring the production
// Prepare→NewPrepared transfer), with effective concurrency 1 so a single
// held global slot parks the next InvokeHandler.
func schedLeaseFn(t *testing.T, name, image string, exec *schedLeaseExecutor, schedules ...function.Schedule) *PreparedFunction {
	t.Helper()
	lease, err := exec.mgr.AcquireImageLease(image)
	if err != nil {
		t.Fatalf("acquire lease for %s: %v", image, err)
	}
	pf := NewPrepared(function.Function{
		Name: name,
		Template: &function.Template{
			Runtime:     "node24",
			Concurrency: 1,
			Schedules:   schedules,
		},
	}, &runtime.Prepared{Name: name, Image: image}, exec)
	pf.lease = lease
	return pf
}

// holdGlobalSlot fills the runner's sole global concurrency slot so the next
// InvokeHandler parks in reserveSlots until releaseGlobalSlot runs. It is safe
// to call synchronously because the slot is free at that point.
func holdGlobalSlot(r *Runner) {
	r.globalSem.Load().slots <- struct{}{}
}

func releaseGlobalSlot(r *Runner) {
	<-r.globalSem.Load().slots
}

// TestInvokeHandlerPreAdmissionRefreshUsesReloadedSchedule blocks an occurrence
// in the slot wait, hot-swaps the schedule under the SAME name to a new
// handler/timeout/retries, then releases the slot. The pre-admission refresh
// must observe the reload: the NEW handler executes and the NEW config is what
// the atomic first admission pins.
func TestInvokeHandlerPreAdmissionRefreshUsesReloadedSchedule(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		exec := newSchedLeaseExecutor()
		old := schedLeaseFn(t, "fn", "relay-fn-fn:v1", exec,
			sched("cleanup", "jobs.old", 30*time.Second, 4))
		r := NewWithMetrics([]*PreparedFunction{old}, testutil.DiscardLogger(), nil)
		r.SetMaxConcurrency(1)
		holdGlobalSlot(r)

		prog := newFakeInvocationState()
		ctx := stream.WithInvocationState(t.Context(), prog)

		done := make(chan error, 1)
		go func() {
			done <- r.InvokeHandler(ctx, "m-0", "fn", "cleanup", "jobs.old", []byte(`{}`))
		}()
		// The occurrence is now durably parked waiting for the sole slot.
		synctest.Wait()

		// Reload while it waits: same schedule NAME, NEW handler/timeout/retries.
		fresh := schedLeaseFn(t, "fn", "relay-fn-fn:v2", exec,
			sched("cleanup", "jobs.new", 7*time.Second, 1))
		r.Registry().Replace("fn", fresh)

		releaseGlobalSlot(r)
		if err := <-done; err != nil {
			t.Fatalf("InvokeHandler: %v", err)
		}

		if got := exec.got(); got != "jobs.new" {
			t.Fatalf("executed handler = %q, want the reloaded schedule's handler jobs.new", got)
		}
		desc, ok := prog.ScheduleDescriptor()
		if !ok {
			t.Fatal("no descriptor pinned after first admission")
		}
		if desc.Schedule != "cleanup" || desc.Handler != "jobs.new" || desc.Timeout != 7*time.Second || desc.Retries != 1 {
			t.Fatalf("pinned descriptor = %+v, want {cleanup jobs.new 7s 1} (the reloaded config)", desc)
		}

		// Lease hygiene: the stale lookup pin was dropped on the swap and the
		// stale publication was released by the Replace, so the stale image is
		// fully drained; the fresh image is held only by its publication.
		if got := exec.mgr.LeaseCount("relay-fn-fn:v1"); got != 0 {
			t.Fatalf("stale image lease count = %d, want 0 (lookup pin released on swap)", got)
		}
		if got := exec.mgr.LeaseCount("relay-fn-fn:v2"); got != 1 {
			t.Fatalf("fresh image lease count = %d, want 1 (publication only, run pin released)", got)
		}
		// Dropping the function releases the fresh publication too: no pin leaked.
		r.Registry().Replace("fn", nil)
		if got := exec.mgr.LeaseCount("relay-fn-fn:v2"); got != 0 {
			t.Fatalf("fresh image lease count after removal = %d, want 0 (no stranded pin)", got)
		}
	})
}

// TestInvokeHandlerPreAdmissionRefreshFunctionRemovedObsolete blocks an
// occurrence in the slot wait, removes the FUNCTION, then releases the slot.
// The pre-admission refresh observes the removal, so the occurrence is
// obsolete: no execution, no descriptor, no state.
func TestInvokeHandlerPreAdmissionRefreshFunctionRemovedObsolete(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		exec := newSchedLeaseExecutor()
		pf := schedLeaseFn(t, "fn", "relay-fn-fn:v1", exec,
			sched("cleanup", "jobs.old", 30*time.Second, 4))
		r := NewWithMetrics([]*PreparedFunction{pf}, testutil.DiscardLogger(), nil)
		r.SetMaxConcurrency(1)
		holdGlobalSlot(r)

		prog := newFakeInvocationState()
		ctx := stream.WithInvocationState(t.Context(), prog)

		done := make(chan error, 1)
		go func() {
			done <- r.InvokeHandler(ctx, "m-0", "fn", "cleanup", "jobs.old", []byte(`{}`))
		}()
		synctest.Wait()

		// The function is removed while the occurrence waits for its slot.
		r.Registry().Replace("fn", nil)
		releaseGlobalSlot(r)

		err := <-done
		if !errors.Is(err, stream.ErrInvocationObsolete) {
			t.Fatalf("err = %v, want wrapped ErrInvocationObsolete", err)
		}
		if got := exec.callCount(); got != 0 {
			t.Fatalf("executor calls = %d, want 0 (removed function must not run)", got)
		}
		if _, ok := prog.ScheduleDescriptor(); ok {
			t.Fatal("an obsolete occurrence must not pin a descriptor")
		}
		if len(prog.attempts) != 0 || len(prog.marks) != 0 || len(prog.failures) != 0 || len(prog.exhausted) != 0 {
			t.Fatalf("obsolete occurrence touched state: attempts=%v marks=%v failures=%v exhausted=%v",
				prog.attempts, prog.marks, prog.failures, prog.exhausted)
		}
		if got := exec.mgr.LeaseCount("relay-fn-fn:v1"); got != 0 {
			t.Fatalf("image lease count = %d, want 0 (lookup pin released, publication removed)", got)
		}
	})
}

// TestInvokeHandlerPreAdmissionRefreshScheduleRemovedObsolete blocks an
// occurrence in the slot wait, reloads the function WITHOUT the schedule NAME,
// then releases the slot. The pre-admission refresh no longer finds the NAME,
// so it proposes nothing and the atomic admission reports the never-admitted
// occurrence obsolete: no execution, no descriptor, no state.
func TestInvokeHandlerPreAdmissionRefreshScheduleRemovedObsolete(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		exec := newSchedLeaseExecutor()
		old := schedLeaseFn(t, "fn", "relay-fn-fn:v1", exec,
			sched("cleanup", "jobs.old", 30*time.Second, 4))
		r := NewWithMetrics([]*PreparedFunction{old}, testutil.DiscardLogger(), nil)
		r.SetMaxConcurrency(1)
		holdGlobalSlot(r)

		prog := newFakeInvocationState()
		ctx := stream.WithInvocationState(t.Context(), prog)

		done := make(chan error, 1)
		go func() {
			done <- r.InvokeHandler(ctx, "m-0", "fn", "cleanup", "jobs.old", []byte(`{}`))
		}()
		synctest.Wait()

		// The reload keeps the function but drops the "cleanup" NAME (a
		// DIFFERENT schedule remains, so the function is still configured).
		fresh := schedLeaseFn(t, "fn", "relay-fn-fn:v2", exec,
			sched("other", "jobs.other", 5*time.Second, 0))
		r.Registry().Replace("fn", fresh)
		releaseGlobalSlot(r)

		err := <-done
		if !errors.Is(err, stream.ErrInvocationObsolete) {
			t.Fatalf("err = %v, want wrapped ErrInvocationObsolete", err)
		}
		if got := exec.callCount(); got != 0 {
			t.Fatalf("executor calls = %d, want 0 (removed schedule must not run)", got)
		}
		if _, ok := prog.ScheduleDescriptor(); ok {
			t.Fatal("an obsolete occurrence must not pin a descriptor")
		}
		if len(prog.attempts) != 0 {
			t.Fatalf("attempts = %v, want none (obsolete occurrence must not claim)", prog.attempts)
		}
		// The refreshed lookup's pin is released on the obsolete return; the
		// removed name's stale publication was released by the Replace.
		if got := exec.mgr.LeaseCount("relay-fn-fn:v1"); got != 0 {
			t.Fatalf("stale image lease count = %d, want 0 (lookup pin released)", got)
		}
		if got := exec.mgr.LeaseCount("relay-fn-fn:v2"); got != 1 {
			t.Fatalf("fresh image lease count = %d, want 1 (publication only)", got)
		}
	})
}

package reconciler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/routing"
	"relay/internal/testutil"
)

// scheduledDeadline returns the wake deadline currently installed for name.
func scheduledDeadline(c *ServiceCoordinator, name string) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if state := c.states[name]; state != nil {
		return state.deadline
	}
	return time.Time{}
}

// newPendingCoordinator builds a started coordinator over the pending service
// fake (first StartService pending, later calls terminal) with a bounded
// teardown.
func newPendingCoordinator(
	t *testing.T, timeout time.Duration,
) (*ServiceCoordinator, *pendingServiceDocker, context.CancelFunc) {
	t.Helper()
	docker := newPendingServiceDocker()
	services := NewServiceReconciler(docker, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), timeout)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	coordinator.Start(lifecycle)
	t.Cleanup(func() {
		cancel()
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer joinCancel()
		if err := coordinator.Join(joinCtx); err != nil {
			t.Errorf("join: %v", err)
		}
	})
	return coordinator, docker, cancel
}

// waitCoordinator blocks until the coordinator has no active backend work.
func waitCoordinator(t *testing.T, c *ServiceCoordinator) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Wait(ctx); err != nil {
		t.Fatalf("wait: %v", err)
	}
}

// TestServiceCoordinatorScheduleFiresToResumePendingRequest pins the timer
// seam: an externally pending logical request with a future installed deadline
// resumes exactly once when the deadline fires — no explicit Notify needed —
// concludes terminally, and clears the deadline with its conclusion.
func TestServiceCoordinatorScheduleFiresToResumePendingRequest(t *testing.T) {
	coordinator, docker, _ := newPendingCoordinator(t, 0)

	tmpl := serviceTemplate("", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
	var completeCount atomic.Int32
	completed := make(chan error, 1)
	done := enqueueMeaningfulWithHooks(coordinator, "alpha", tmpl, "img-1", nil,
		func(err error) {
			completeCount.Add(1)
			select {
			case completed <- err:
			default:
			}
		}, nil,
	)

	// given the first pass ended pending with no wake installed
	select {
	case <-docker.pendingSignal:
	case <-time.After(2 * time.Second):
		t.Fatal("the first pass never reached StartService")
	}
	waitCoordinator(t, coordinator)

	// when a future deadline is installed for the pending request
	coordinator.Schedule("alpha", time.Now().Add(120*time.Millisecond))

	// then the timer itself resumes the request to a terminal conclusion
	select {
	case err := <-completed:
		if err != nil {
			t.Fatalf("terminal completion err = %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the scheduled deadline never resumed the pending request")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("done did not close after the resumed pass concluded")
	}
	waitCoordinator(t, coordinator)

	if got := docker.startCalls.Load(); got != 2 {
		t.Fatalf("StartService calls = %d, want 2 (the timer fired exactly once)", got)
	}
	if got := completeCount.Load(); got != 1 {
		t.Fatalf("completion calls = %d, want exactly 1", got)
	}
	if got := scheduledDeadline(coordinator, "alpha"); !got.IsZero() {
		t.Fatalf("deadline after conclusion = %v, want cleared with the concluded request", got)
	}
}

// TestServiceCoordinatorCancelSchedulePreventsWake pins the cancel side of the
// timer seam: a deadline cancelled right after installation never wakes the
// pending request within its window, while an explicit Notify still resumes it
// afterwards — cancellation drops the wake, not the logical request.
func TestServiceCoordinatorCancelSchedulePreventsWake(t *testing.T) {
	coordinator, docker, _ := newPendingCoordinator(t, 0)

	tmpl := serviceTemplate("", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
	completed := make(chan error, 1)
	done := enqueueMeaningfulWithHooks(coordinator, "alpha", tmpl, "img-1", nil,
		func(err error) {
			select {
			case completed <- err:
			default:
			}
		}, nil,
	)

	select {
	case <-docker.pendingSignal:
	case <-time.After(2 * time.Second):
		t.Fatal("the first pass never reached StartService")
	}
	waitCoordinator(t, coordinator)

	// when the installed deadline is cancelled
	coordinator.Schedule("alpha", time.Now().Add(120*time.Millisecond))
	coordinator.CancelSchedule("alpha")
	if got := scheduledDeadline(coordinator, "alpha"); !got.IsZero() {
		t.Fatalf("deadline after CancelSchedule = %v, want cleared", got)
	}

	// then the cancelled deadline must not wake the pending request
	select {
	case <-completed:
		t.Fatal("a cancelled deadline resumed the pending request")
	case <-time.After(400 * time.Millisecond):
	}
	if got := docker.startCalls.Load(); got != 1 {
		t.Fatalf("StartService calls after cancellation = %d, want 1 (no timer wake)", got)
	}

	// and an explicit wake still resumes the retained request
	coordinator.Notify("alpha")
	select {
	case err := <-completed:
		if err != nil {
			t.Fatalf("terminal completion err = %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Notify did not resume the pending request after CancelSchedule")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("done did not close after the resumed pass concluded")
	}
	waitCoordinator(t, coordinator)
	if got := docker.startCalls.Load(); got != 2 {
		t.Fatalf("StartService calls = %d, want exactly 2 (one pending pass plus one explicit wake)", got)
	}
}

// TestServiceCoordinatorNewDesiredStateSupersedesScheduledDeadline pins
// deadline ownership: a wake deadline belongs to the request that installed
// it, so a newer desired state concludes the pending request AND clears its
// future deadline in the same enqueue — the concluded request's timer can
// never fire again, and the replacement runs on its own.
func TestServiceCoordinatorNewDesiredStateSupersedesScheduledDeadline(t *testing.T) {
	coordinator, docker, _ := newPendingCoordinator(t, 0)

	tmpl := serviceTemplate("", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
	var complete1, settle1 atomic.Int32
	done1 := enqueueMeaningfulWithHooks(coordinator, "alpha", tmpl, "img-1", nil,
		func(error) { complete1.Add(1) },
		func() { settle1.Add(1) },
	)

	select {
	case <-docker.pendingSignal:
	case <-time.After(2 * time.Second):
		t.Fatal("the first pass never reached StartService")
	}
	waitCoordinator(t, coordinator)

	// given the pending request holds a far-future wake deadline
	deadline := time.Now().Add(time.Hour)
	coordinator.Schedule("alpha", deadline)
	if got := scheduledDeadline(coordinator, "alpha"); !got.Equal(deadline) {
		t.Fatalf("scheduled deadline = %v, want %v", got, deadline)
	}

	// when a newer desired state arrives
	coordinator.Enqueue("alpha", tmpl, "img-2", nil)

	// then the old request concludes exactly once without its completion, and
	// its deadline is cleared with it
	select {
	case <-done1:
	default:
		t.Fatal("supersession did not conclude the pending request's done channel")
	}
	if got := complete1.Load(); got != 0 {
		t.Fatalf("superseded completion calls = %d, want 0", got)
	}
	if got := settle1.Load(); got != 1 {
		t.Fatalf("superseded settlement calls = %d, want exactly 1", got)
	}
	if got := scheduledDeadline(coordinator, "alpha"); !got.IsZero() {
		t.Fatalf("deadline after supersession = %v, want cleared (the wake belonged to the concluded request)", got)
	}

	// and the replacement converges on its own without any timer wake
	waitCoordinator(t, coordinator)
	if got := docker.startCalls.Load(); got != 2 {
		t.Fatalf("StartService calls = %d, want 2 (pending pass plus the replacement)", got)
	}
}

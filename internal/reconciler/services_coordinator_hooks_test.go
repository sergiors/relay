package reconciler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/testutil"
)

// TestServiceCoordinatorWaitAndExclusiveBlockedThroughTerminalHooks pins the
// logical-busy boundary: an app stays busy through its terminal completion
// callback AND its settlement hook, so Wait and RunExclusive never observe it
// idle early, a same-app follow-up pass never overlaps the hooks, and the done
// waiter releases only after both hooks finished — while an unrelated app keeps
// progressing on the free worker the whole time.
func TestServiceCoordinatorWaitAndExclusiveBlockedThroughTerminalHooks(t *testing.T) {
	docker := &coordinatorDocker{entered: make(chan struct{}, 8), release: make(chan struct{})}
	services := NewServiceReconciler(docker, nil, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	coordinator.Start(lifecycle)

	completeEntered := make(chan struct{})
	completeGate := make(chan struct{})
	settleEntered := make(chan struct{})
	settleGate := make(chan struct{})
	exclusiveEntered := make(chan struct{})
	releaseExclusive := make(chan struct{})

	var dockerOnce, completeOnce, settleOnce, exclusiveOnce sync.Once
	unblockDocker := func() { dockerOnce.Do(func() { close(docker.release) }) }
	unblockComplete := func() { completeOnce.Do(func() { close(completeGate) }) }
	unblockSettle := func() { settleOnce.Do(func() { close(settleGate) }) }
	unblockExclusive := func() { exclusiveOnce.Do(func() { close(releaseExclusive) }) }
	defer func() {
		unblockDocker()
		unblockComplete()
		unblockSettle()
		unblockExclusive()
		cancel()
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer joinCancel()
		if err := coordinator.Join(joinCtx); err != nil {
			t.Errorf("join: %v", err)
		}
	}()

	tmpl := serviceTemplate("", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})

	// given a meaningful request whose terminal callbacks park inside the hooks
	done := enqueueMeaningfulWithHooks(coordinator, "alpha", tmpl, "img-1", nil,
		func(error) {
			close(completeEntered)
			<-completeGate
		},
		func() {
			close(settleEntered)
			<-settleGate
		},
	)
	select {
	case <-docker.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the first pass never entered")
	}
	unblockDocker() // the pass runs to its terminal conclusion, then parks in OnComplete
	select {
	case <-completeEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("the terminal completion callback never ran")
	}

	// when the completion callback is still blocked
	// then Wait observes the app as busy for its whole window
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	err := coordinator.Wait(waitCtx)
	waitCancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait during the terminal callback = %v, want context.DeadlineExceeded (busy)", err)
	}

	// and RunExclusive's housekeeping barrier cannot clear either
	exclusiveDone := make(chan error, 1)
	go func() {
		exclusiveDone <- coordinator.RunExclusive(context.Background(), func(context.Context) {
			close(exclusiveEntered)
			<-releaseExclusive
		})
	}()

	// and a same-app follow-up pass cannot start while the hooks run
	coordinator.Enqueue("alpha", tmpl, "img-2", nil)

	// and an unrelated app still progresses on the free worker
	coordinator.Enqueue("beta", tmpl, "img-beta", nil)
	select {
	case <-docker.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the unrelated app never progressed while alpha's hooks were blocked")
	}
	select {
	case <-docker.entered:
		t.Fatal("a same-app pass started while the completion hook was blocked")
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case <-done:
		t.Fatal("done closed while the completion hook was still running")
	default:
	}

	// when the completion hook returns and the settlement hook blocks
	unblockComplete()
	select {
	case <-settleEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("the settlement hook never ran")
	}
	// then the app is still busy: Wait blocked, housekeeping blocked, and the
	// same-app follow-up still not started
	waitCtx, waitCancel = context.WithTimeout(context.Background(), 150*time.Millisecond)
	err = coordinator.Wait(waitCtx)
	waitCancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait during the settlement hook = %v, want context.DeadlineExceeded (busy)", err)
	}
	select {
	case <-exclusiveEntered:
		t.Fatal("housekeeping entered while the settlement hook was blocked")
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case <-docker.entered:
		t.Fatal("a same-app pass started while the settlement hook was blocked")
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case <-done:
		t.Fatal("done closed before the settlement hook finished")
	default:
	}

	// when both hooks are released the request concludes: done closes first,
	// then housekeeping may enter, then the same-app follow-up runs
	unblockSettle()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("done did not close after both hooks finished")
	}
	select {
	case <-exclusiveEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("housekeeping never entered after the hooks finished")
	}
	unblockExclusive()
	select {
	case err := <-exclusiveDone:
		if err != nil {
			t.Fatalf("RunExclusive: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunExclusive did not return after its window closed")
	}
	select {
	case <-docker.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the same-app follow-up never ran after the hooks finished")
	}
	docker.mu.Lock()
	resolves := append([]string(nil), docker.resolves...)
	docker.mu.Unlock()
	if !containsString(resolves, "alpha=img-2") {
		t.Fatalf("resolves = %v, want the follow-up alpha=img-2", resolves)
	}

	finalCtx, finalCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer finalCancel()
	if err := coordinator.Wait(finalCtx); err != nil {
		t.Fatalf("final wait: %v", err)
	}
}

// TestServiceCoordinatorCallbackTriggeredSupersessionAndNotifyAreSafe pins
// that coordinator callbacks run outside the lock: an OnReconcileStart hook may
// enqueue a newer desired state (superseding its own executing request) and
// Notify existing and unknown apps without deadlocking. The superseded
// request concludes without its completion callback, and the replacement
// enqueued from the callback runs to a terminal result.
func TestServiceCoordinatorCallbackTriggeredSupersessionAndNotifyAreSafe(t *testing.T) {
	docker := &coordinatorDocker{entered: make(chan struct{}, 8), release: make(chan struct{})}
	close(docker.release) // resolution never parks; the callback work is the subject
	services := NewServiceReconciler(docker, nil, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	coordinator.Start(lifecycle)
	defer func() {
		cancel()
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer joinCancel()
		if err := coordinator.Join(joinCtx); err != nil {
			t.Errorf("join: %v", err)
		}
	}()

	tmpl := serviceTemplate("", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
	var startCount, complete1, settle1 atomic.Int32
	completed2 := make(chan error, 1)
	var enqueueOnce sync.Once

	done1 := coordinator.enqueue(&serviceRequest{
		name: "alpha", tmpl: cloneServiceTemplate(tmpl), image: "img-1",
		onReconcileStart: func() {
			startCount.Add(1)
			enqueueOnce.Do(func() {
				coordinator.EnqueueWithStatus("alpha", tmpl, "img-2", nil, nil, func(err error) {
					select {
					case completed2 <- err:
					default:
					}
				})
				coordinator.Notify("alpha")
				coordinator.Notify("never-enqueued")
			})
		},
		onComplete: func(error) { complete1.Add(1) },
		onSettled:  func() { settle1.Add(1) },
	})

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()
	if err := coordinator.Wait(waitCtx); err != nil {
		t.Fatalf("wait: %v", err)
	}
	select {
	case err := <-completed2:
		if err != nil {
			t.Fatalf("replacement completion err = %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the callback-enqueued replacement never completed")
	}

	select {
	case <-done1:
	case <-time.After(2 * time.Second):
		t.Fatal("superseded request's done channel was not closed")
	}
	if got := startCount.Load(); got != 1 {
		t.Fatalf("OnReconcileStart calls = %d, want exactly 1", got)
	}
	if got := complete1.Load(); got != 0 {
		t.Fatalf("superseded completion calls = %d, want 0 (fenced by the callback's own enqueue)", got)
	}
	if got := settle1.Load(); got != 1 {
		t.Fatalf("superseded settlement calls = %d, want exactly 1", got)
	}

	docker.mu.Lock()
	resolves := append([]string(nil), docker.resolves...)
	docker.mu.Unlock()
	if !containsString(resolves, "alpha=img-1") || !containsString(resolves, "alpha=img-2") {
		t.Fatalf("resolves = %v, want both the original pass and the callback-enqueued replacement", resolves)
	}
}

// TestServiceCoordinatorShutdownDuringSettlementSettlesOnce pins shutdown
// cleanup racing an in-flight settlement hook: cancelling the lifecycle while
// the hook is parked neither releases Join early nor settles the request a
// second time; releasing the hook concludes the request exactly once and Join
// then drains cleanly.
func TestServiceCoordinatorShutdownDuringSettlementSettlesOnce(t *testing.T) {
	docker := &coordinatorDocker{entered: make(chan struct{}, 8), release: make(chan struct{})}
	close(docker.release)
	services := NewServiceReconciler(docker, nil, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	coordinator.Start(lifecycle)

	settleEntered := make(chan struct{})
	settleGate := make(chan struct{})
	var settleOnce sync.Once
	unblockSettle := func() { settleOnce.Do(func() { close(settleGate) }) }
	defer func() {
		unblockSettle()
		cancel()
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer joinCancel()
		if err := coordinator.Join(joinCtx); err != nil {
			t.Errorf("join: %v", err)
		}
	}()

	tmpl := serviceTemplate("", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
	var completeCount, settleCount atomic.Int32
	done := enqueueMeaningfulWithHooks(coordinator, "alpha", tmpl, "img-1", nil,
		func(error) { completeCount.Add(1) },
		func() {
			close(settleEntered)
			<-settleGate
			settleCount.Add(1)
		},
	)
	select {
	case <-settleEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("the settlement hook never ran")
	}

	// when shutdown lands while the settlement hook is still blocked
	cancel()
	joinDone := make(chan error, 1)
	go func() {
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer joinCancel()
		joinDone <- coordinator.Join(joinCtx)
	}()
	select {
	case err := <-joinDone:
		t.Fatalf("Join returned while the settlement hook was still blocked: %v", err)
	case <-time.After(150 * time.Millisecond):
	}

	// then releasing the hook concludes the request exactly once and Join drains
	unblockSettle()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("done did not close after the settlement hook finished")
	}
	select {
	case err := <-joinDone:
		if err != nil {
			t.Fatalf("join: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Join did not drain after the settlement hook was released")
	}
	if got := completeCount.Load(); got != 1 {
		t.Fatalf("completion calls = %d, want exactly 1", got)
	}
	if got := settleCount.Load(); got != 1 {
		t.Fatalf("settlement calls = %d, want exactly 1 (shutdown must not settle again)", got)
	}
}

// TestServiceCoordinatorCoalescedObservationCompletionPreserved pins the
// startup-progress observer seam: an observation that coalesces into an
// ACTIVE pass keeps its terminal callback composed onto the executing logical
// request instead of running early or being dropped. The coalesced wake reruns
// the same request, and at its terminal conclusion both the meaningful
// completion and the composed observer callback fire exactly once.
func TestServiceCoordinatorCoalescedObservationCompletionPreserved(t *testing.T) {
	docker := &coordinatorDocker{entered: make(chan struct{}, 8), release: make(chan struct{})}
	services := NewServiceReconciler(docker, nil, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	coordinator.Start(lifecycle)
	defer func() {
		cancel()
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer joinCancel()
		if err := coordinator.Join(joinCtx); err != nil {
			t.Errorf("join: %v", err)
		}
	}()

	tmpl := serviceTemplate("", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
	var completeCount, observationCount, settleCount atomic.Int32
	completed := make(chan error, 1)
	observationDone := make(chan error, 1)

	done := enqueueMeaningfulWithHooks(coordinator, "alpha", tmpl, "img-1", nil,
		func(err error) {
			completeCount.Add(1)
			select {
			case completed <- err:
			default:
			}
		},
		func() { settleCount.Add(1) },
	)
	select {
	case <-docker.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the first pass never entered")
	}

	// when an observation arrives while the pass is executing it coalesces as a
	// dirty wake and composes its terminal callback onto the active request
	coordinator.EnqueueStatusObservation("alpha", tmpl, "img-1", nil, nil, func(err error) {
		observationCount.Add(1)
		select {
		case observationDone <- err:
		default:
		}
	})
	// when the release lands, the coalesced wake reruns the same request and
	// the terminal conclusion reports both callbacks exactly once
	close(docker.release)

	select {
	case err := <-completed:
		if err != nil {
			t.Fatalf("meaningful completion err = %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the meaningful request never concluded terminally")
	}
	select {
	case err := <-observationDone:
		if err != nil {
			t.Fatalf("observation completion err = %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the coalesced observation's callback was lost")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("done did not close after the composed callbacks")
	}

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	if err := coordinator.Wait(waitCtx); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if got := completeCount.Load(); got != 1 {
		t.Fatalf("meaningful completion calls = %d, want exactly 1", got)
	}
	if got := observationCount.Load(); got != 1 {
		t.Fatalf("observation completion calls = %d, want exactly 1", got)
	}
	if got := settleCount.Load(); got != 1 {
		t.Fatalf("settlement calls = %d, want exactly 1", got)
	}

	// the coalesced wake reran the SAME request exactly once instead of
	// starting a separate observation pass: two resolves, not three
	docker.mu.Lock()
	var alpha int
	for _, resolve := range docker.resolves {
		if resolve == "alpha=img-1" {
			alpha++
		}
	}
	docker.mu.Unlock()
	if alpha != 2 {
		t.Fatalf("alpha resolves = %d, want 2 (initial pass plus the coalesced rerun)", alpha)
	}
}

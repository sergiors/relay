package reconciler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"

	"relay/internal/app"
	"relay/internal/testutil"
)

// healthServiceTemplate builds the one-replica service template used by the
// coordinator health tests.
func healthServiceTemplate(hc *app.ServiceHealthcheck) *app.Template {
	return serviceTemplate("", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Healthcheck: hc,
	})
}

// A healthcheck-driven pending pass installs its SOFT startup wake by itself:
// the worker is released while the request stays pending, the scheduled
// deadline is the candidate's durable startup deadline, and an observer hint
// (Notify) may resume the request EARLY — well before that deadline — which
// concludes it exactly once.
func TestServiceCoordinatorHealthStartupWakeAndNotifyResumesEarly(t *testing.T) {
	docker := newPendingServiceDocker()
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

	hc := testServiceHealthcheck(time.Second)
	tmpl := healthServiceTemplate(hc)
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

	// The first pass ends pending on StartService and must release its worker
	// while the logical request and its callbacks stay retained.
	select {
	case <-docker.pendingSignal:
	case <-time.After(2 * time.Second):
		t.Fatal("the first pass never reached StartService")
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := coordinator.Wait(waitCtx); err != nil {
		waitCancel()
		t.Fatalf("Wait did not return while the healthcheck pass is pending: %v", err)
	}
	waitCancel()
	select {
	case <-done:
		t.Fatal("the pending request's done channel was closed")
	default:
	}
	if got := completeCount.Load(); got != 0 {
		t.Fatalf("completion calls while pending = %d, want 0", got)
	}

	// The pending pass installed its own soft startup wake: a future deadline
	// with a starting-candidate observation and NO hard retry eligibility.
	deadline := scheduledDeadline(coordinator, "alpha")
	now := time.Now()
	if deadline.IsZero() || !deadline.After(now) ||
		deadline.After(now.Add(hc.Interval+500*time.Millisecond)) {
		t.Fatalf("scheduled deadline = %v, want the durable startup wake within %s of %v",
			deadline, hc.Interval, now)
	}
	coordinator.mu.Lock()
	state := coordinator.states["alpha"]
	retryNotBefore, observationDeadline := state.retryNotBefore, state.observationDeadline
	coordinator.mu.Unlock()
	if !retryNotBefore.IsZero() {
		t.Fatalf("retryNotBefore = %v, want zero: a starting candidate has no hard retry", retryNotBefore)
	}
	if observationDeadline.IsZero() {
		t.Fatal("observationDeadline is zero, want the soft startup observation installed")
	}

	// The observer hint arrives BEFORE the deadline: the soft wake is not a
	// floor, so the pass resumes immediately and concludes terminally while
	// there is still clear time before the scheduled deadline would fire.
	budget := time.Until(deadline) - 300*time.Millisecond
	if budget <= 0 {
		t.Fatalf("deadline %v leaves no budget to distinguish an early Notify wake", deadline)
	}
	coordinator.Notify("alpha")
	select {
	case err := <-completed:
		if err != nil {
			t.Fatalf("terminal completion err = %v, want nil", err)
		}
	case <-time.After(budget):
		t.Fatalf("Notify did not resume the pending request before the soft deadline %v", deadline)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("done did not close after the resumed pass concluded")
	}
	waitCoordinator(t, coordinator)

	if got := docker.startCalls.Load(); got != 2 {
		t.Fatalf("StartService calls = %d, want exactly 2 (pending pass plus one early wake)", got)
	}
	if got := completeCount.Load(); got != 1 {
		t.Fatalf("completion calls = %d, want exactly 1", got)
	}
	if got := scheduledDeadline(coordinator, "alpha"); !got.IsZero() {
		t.Fatalf("deadline after conclusion = %v, want cleared with the request", got)
	}
}

// Without any observer hint the installed startup deadline itself wakes the
// scheduler: the pending request resumes exactly once at its due time and
// concludes, proving the wake comes from the pass's own scheduled deadline.
func TestServiceCoordinatorHealthStartupDeadlineWakesWithoutNotify(t *testing.T) {
	docker := newPendingServiceDocker()
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

	hc := testServiceHealthcheck(time.Second)
	tmpl := healthServiceTemplate(hc)
	var completeCount atomic.Int32
	completed := make(chan error, 1)
	enqueueMeaningfulWithHooks(coordinator, "alpha", tmpl, "img-1", nil,
		func(err error) {
			completeCount.Add(1)
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
	deadline := scheduledDeadline(coordinator, "alpha")
	if deadline.IsZero() {
		t.Fatal("no wake deadline was installed for the pending healthcheck pass")
	}

	// No Notify: the scheduler alone must resume the request at the deadline
	// (interval plus slack), never earlier by accident and never not at all.
	select {
	case err := <-completed:
		if err != nil {
			t.Fatalf("terminal completion err = %v, want nil", err)
		}
	case <-time.After(hc.Interval + 3*time.Second):
		t.Fatalf("the scheduled startup deadline %v never resumed the pending request", deadline)
	}
	waitCoordinator(t, coordinator)
	if got := docker.startCalls.Load(); got != 2 {
		t.Fatalf("StartService calls = %d, want exactly 2 (one scheduled wake)", got)
	}
	if got := completeCount.Load(); got != 1 {
		t.Fatalf("completion calls = %d, want exactly 1", got)
	}
	if got := scheduledDeadline(coordinator, "alpha"); !got.IsZero() {
		t.Fatalf("deadline after conclusion = %v, want cleared", got)
	}
}

// A failed candidate's retry eligibility is HARD: observer hints (repeated
// Notify calls) cannot schedule a pass before it, no container work happens in
// the window, and when the due deadline finally fires the retry runs with the
// next durable attempt, commits, and stops the failed generation in order.
func TestServiceCoordinatorHardRetryHoldsAgainstNotifyUntilEligible(t *testing.T) {
	f := newFakeDocker()
	md := newMarkerStoreDocker(f)
	hc := testServiceHealthcheck(time.Second)
	seedConvergedHealthService(f, "old-cand", "fn", "service.js", "img-1",
		80, 0, hc, container.Unhealthy, 1)
	services := NewServiceReconciler(md, nil, testutil.DiscardLogger(), testReconcileTimeout)
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

	tmpl := healthServiceTemplate(hc)
	var completeCount atomic.Int32
	completed := make(chan error, 1)
	done := enqueueMeaningfulWithHooks(coordinator, "fn", tmpl, "img-1", nil,
		func(err error) {
			completeCount.Add(1)
			select {
			case completed <- err:
			default:
			}
		}, nil,
	)

	// The first pass observes the failure, records durable eligibility, and
	// ends pending without touching any container.
	waitCoordinator(t, coordinator)
	if got := f.resolveCount(); got != 1 {
		t.Fatalf("resolved passes = %d, want 1", got)
	}
	if events := f.order(); len(events) != 0 {
		t.Fatalf("events = %v, want none inside the retry window", events)
	}
	deadline := scheduledDeadline(coordinator, "fn")
	if deadline.IsZero() || !deadline.After(time.Now()) ||
		deadline.After(time.Now().Add(2*time.Second)) {
		t.Fatalf("scheduled deadline = %v, want the hard retry eligibility", deadline)
	}
	marker, ok := md.markerState("fn", "service.js", 0)
	if !ok || marker.CommittedContainerID != "" ||
		marker.CandidateContainerID != "old-cand" || marker.CandidateAttempt != 1 ||
		!marker.RetryNotBefore.Equal(deadline) {
		t.Fatalf("failure marker = (%+v, %v), want the recorded eligibility matching the wake",
			marker, ok)
	}

	// Repeated observer hints cannot bypass the hard deadline: no pass runs,
	// no container work happens, no callback fires.
	for i := 0; i < 3; i++ {
		coordinator.Notify("fn")
		time.Sleep(100 * time.Millisecond)
	}
	if got := f.resolveCount(); got != 1 {
		t.Fatalf("resolved passes after Notify = %d, want 1 (hard retry not bypassed)", got)
	}
	if events := f.order(); len(events) != 0 {
		t.Fatalf("events after Notify = %v, want none before eligibility", events)
	}
	if got := completeCount.Load(); got != 0 {
		t.Fatalf("completion calls before eligibility = %d, want 0", got)
	}
	select {
	case <-done:
		t.Fatal("the pending request's done channel closed before its retry ran")
	default:
	}

	// When the deadline fires the scheduler resumes the request: the retry
	// starts the next durable attempt, commits it, and only then stops the
	// failed generation.
	select {
	case err := <-completed:
		if err != nil {
			t.Fatalf("terminal completion err = %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("the hard retry deadline %v never resumed the pending request", deadline)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("done did not close after the retry concluded")
	}
	waitCoordinator(t, coordinator)

	replacement := f.lastStartedFor("fn", "service.js")
	if replacement == nil || replacement.id == "old-cand" {
		t.Fatalf("replacement = %+v, want a fresh candidate container", replacement)
	}
	if replacement.healthcheckAttempt != 2 {
		t.Fatalf("attempt = %d, want 2 (the durable attempt advanced)", replacement.healthcheckAttempt)
	}
	start, stop := f.indexOfEvent("start:0"), f.indexOfEvent("stop:old-cand")
	if start < 0 || stop < 0 || start > stop {
		t.Fatalf("order = %v, want the retry's start before the failed generation's stop", f.order())
	}
	if state, ok := md.markerState("fn", "service.js", 0); !ok ||
		state.CommittedContainerID != replacement.id {
		t.Fatalf("committed marker = (%+v, %v), want %q", state, ok, replacement.id)
	}
	if got := completeCount.Load(); got != 1 {
		t.Fatalf("completion calls = %d, want exactly 1", got)
	}
	if got := scheduledDeadline(coordinator, "fn"); !got.IsZero() {
		t.Fatalf("deadline after conclusion = %v, want cleared", got)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want exactly the committed replacement", got)
	}
}

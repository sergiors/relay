package reconciler

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/routing"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// mixedPendingDocker models one pass that converges nothing everywhere: the
// probe-declared service's first start is accepted-but-pending (reporting a
// real candidate container), while an UNRELATED plain service fails its start
// on every pending pass — so the pass ends pending WITH an independent error
// from another service. Flipping terminal makes both services succeed, which
// concludes the retained request normally.
type mixedPendingDocker struct {
	*fakeDocker
	mu            sync.Mutex
	terminal      bool
	pendingStarts atomic.Int32
	apiFailures   atomic.Int32
	pendingSignal chan struct{}
}

func newMixedPendingDocker() *mixedPendingDocker {
	return &mixedPendingDocker{
		fakeDocker:    newFakeDocker(),
		pendingSignal: make(chan struct{}, 1),
	}
}

func (d *mixedPendingDocker) setTerminal() {
	d.mu.Lock()
	d.terminal = true
	d.mu.Unlock()
}

func (d *mixedPendingDocker) isTerminal() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.terminal
}

func (d *mixedPendingDocker) StartService(
	ctx context.Context, spec runtime.ServiceSpec, replica int,
) (string, error) {
	if !d.isTerminal() {
		switch spec.Name {
		case "service.js":
			d.pendingStarts.Add(1)
			select {
			case d.pendingSignal <- struct{}{}:
			default:
			}
			return "cand-web", ErrServicePending
		case "api.js":
			d.apiFailures.Add(1)
			return "", fmt.Errorf("create failed for %q", spec.Name)
		}
	}
	return d.fakeDocker.StartService(ctx, spec, replica)
}

// mixedTemplate declares the probe service plus the unrelated plain service
// whose start fails while the docker is pending.
func mixedTemplate(hc *app.ServiceHealthcheck) *app.Template {
	return serviceTemplate("nodejs24.x",
		app.Service{
			Name: "service.js", Entrypoint: "service.js", Port: 80,
			Replicas: 1, Healthcheck: hc,
		},
		app.Service{Name: "api.js", Entrypoint: "api.js", Port: 81, Replicas: 1},
	)
}

// A pending result CAN carry an independent failure: convergence is explicitly
// incomplete (pending governs), the unrelated service's start failure stays on
// the result, and the pending candidate plus its soft wake deadline are kept so
// the coordinator can retain and resume the request.
func TestApplyResultPendingCarriesIndependentErrorAndKeepsCandidates(t *testing.T) {
	docker := newMixedPendingDocker()
	c := NewServiceReconciler(docker, nil, routing.TraefikConfig{},
		testutil.DiscardLogger(), 0)
	hc := testServiceHealthcheck(time.Second)

	before := time.Now()
	result := c.ApplyResult(context.Background(), "alpha", mixedTemplate(hc),
		"img-1", nil, nil)

	if !result.Pending() {
		t.Fatalf("result = %+v, want pending (incomplete convergence governs)", result)
	}
	if result.Err == nil ||
		!strings.Contains(result.Err.Error(), "api.js") {
		t.Fatalf("Err = %v, want the independent failure of the unrelated service",
			result.Err)
	}
	if len(result.Candidates) != 1 || result.Candidates[0].ContainerID != "cand-web" ||
		result.Candidates[0].App != "alpha" {
		t.Fatalf("candidates = %+v, want the retained [alpha/cand-web]", result.Candidates)
	}
	if !result.RetryNotBefore.IsZero() {
		t.Fatalf("RetryNotBefore = %v, want zero (a starting candidate has no hard retry)",
			result.RetryNotBefore)
	}
	softLatest := before.Add(hc.Interval + 500*time.Millisecond)
	if result.StartupDeadline.Before(before) ||
		result.StartupDeadline.After(softLatest) {
		t.Fatalf("StartupDeadline = %v, want a soft wake after %v and within %v",
			result.StartupDeadline, before, softLatest)
	}
	if got := docker.apiFailures.Load(); got != 1 {
		t.Fatalf("unrelated start failures = %d, want 1", got)
	}
}

// The coordinator treats pending-with-error exactly like a clean pending pass:
// it retains the observer candidates and both deadlines, releases its worker,
// and NEVER authorizes the terminal completion callback — repeated woken
// passes stay pending until convergence actually completes, and only then does
// the request conclude exactly once.
func TestServiceCoordinatorPendingWithErrorRetainsRequestAndNeverCompletes(t *testing.T) {
	docker := newMixedPendingDocker()
	services := NewServiceReconciler(docker, nil, routing.TraefikConfig{},
		testutil.DiscardLogger(), 0)
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
	var completeCount, settleCount atomic.Int32
	completed := make(chan error, 1)
	done := enqueueMeaningfulWithHooks(coordinator, "alpha", mixedTemplate(hc),
		"img-1", nil,
		func(err error) {
			completeCount.Add(1)
			select {
			case completed <- err:
			default:
			}
		},
		func() { settleCount.Add(1) },
	)

	// The first pass ends pending with the independent error and must release
	// its worker while the request, callbacks, candidates, and deadlines stay
	// retained.
	select {
	case <-docker.pendingSignal:
	case <-time.After(2 * time.Second):
		t.Fatal("the first pass never reached StartService")
	}
	waitCoordinator(t, coordinator)

	coordinator.mu.Lock()
	state := coordinator.states["alpha"]
	candidates := append([]runtime.ServiceHealthCandidate(nil),
		state.pendingCandidates...)
	observationDeadline, retryNotBefore := state.observationDeadline,
		state.retryNotBefore
	deadline := state.deadline
	coordinator.mu.Unlock()
	if len(candidates) != 1 || candidates[0].ContainerID != "cand-web" {
		t.Fatalf("retained candidates = %+v, want [cand-web]", candidates)
	}
	if observationDeadline.IsZero() {
		t.Fatal("observationDeadline = zero, want the soft startup wake retained")
	}
	if !retryNotBefore.IsZero() {
		t.Fatalf("retryNotBefore = %v, want zero for a starting candidate", retryNotBefore)
	}
	if !deadline.Equal(observationDeadline) {
		t.Fatalf("installed deadline = %v, want the retained startup wake %v",
			deadline, observationDeadline)
	}
	if completeCount.Load() != 0 || settleCount.Load() != 0 {
		t.Fatalf("callbacks while pending with an error = (complete %d, settle %d), "+
			"want 0: the error must not authorize terminal completion",
			completeCount.Load(), settleCount.Load())
	}
	select {
	case <-done:
		t.Fatal("the pending request's done channel was closed")
	default:
	}

	// An explicit wake runs a second pending-with-error pass: still retained,
	// still never completed.
	coordinator.Notify("alpha")
	waitCoordinator(t, coordinator)
	if got := docker.pendingStarts.Load(); got != 2 {
		t.Fatalf("pending starts = %d, want 2 (the wake ran exactly one more pass)", got)
	}
	if completeCount.Load() != 0 || settleCount.Load() != 0 {
		t.Fatalf("callbacks after the second pending pass = (complete %d, settle %d), "+
			"want 0", completeCount.Load(), settleCount.Load())
	}
	coordinator.mu.Lock()
	candidates = append([]runtime.ServiceHealthCandidate(nil),
		coordinator.states["alpha"].pendingCandidates...)
	coordinator.mu.Unlock()
	if len(candidates) != 1 || candidates[0].ContainerID != "cand-web" {
		t.Fatalf("retained candidates after the wake = %+v, want [cand-web]", candidates)
	}
	select {
	case <-done:
		t.Fatal("done closed while still pending")
	default:
	}

	// Convergence finally completes: the SAME request concludes exactly once,
	// with a nil terminal error, its settlement, and a cleared wake.
	docker.setTerminal()
	coordinator.Notify("alpha")
	select {
	case err := <-completed:
		if err != nil {
			t.Fatalf("terminal completion err = %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the terminal pass never concluded the retained request")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("done did not close after the terminal conclusion")
	}
	waitCoordinator(t, coordinator)
	if completeCount.Load() != 1 || settleCount.Load() != 1 {
		t.Fatalf("callbacks after conclusion = (complete %d, settle %d), want exactly 1 each",
			completeCount.Load(), settleCount.Load())
	}
	if got := scheduledDeadline(coordinator, "alpha"); !got.IsZero() {
		t.Fatalf("deadline after conclusion = %v, want cleared with the request", got)
	}
	coordinator.mu.Lock()
	remaining := coordinator.states["alpha"]
	coordinator.mu.Unlock()
	if remaining != nil && remaining.desired != nil {
		t.Fatal("the concluded request was not dropped from the app slot")
	}
}

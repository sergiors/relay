package reconciler

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"

	"relay/internal/app"
	"relay/internal/testutil"
)

// setReady updates the backend readiness observation for a seeded container.
func (f *fakeDocker) setReady(id string, ready bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.ctrs[id]; ok {
		value := ready
		c.ready = &value
	}
}

// resolveCount returns how many source resolutions the fake observed; each
// worker pass resolves once per service, so it counts executed passes.
func (f *fakeDocker) resolveCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.resolveCalls)
}

// TestServiceCoordinatorUnreadyCandidateStaysPendingUntilReady pins the
// backend-readiness revalidation seam end to end: a running, config-matching
// replacement the backend observes as NOT ready keeps every wake pending — no
// duplicate start, no stop of the candidate or the old generation, no
// completion — even when a stale Notify re-evaluates it, and the first wake
// after readiness flips true converges exactly once: the stale old generation
// retired, the candidate kept, one terminal completion, no starts ever issued.
func TestServiceCoordinatorUnreadyCandidateStaysPendingUntilReady(t *testing.T) {
	f := newFakeDocker()
	// given an old generation still serving and a config-matching replacement
	// the backend accepted but reports not ready
	seedOldService(f, "old-1", "alpha", "service.js", "img-old", 80, 0)
	notReady := false
	f.ctrs["cand-1"] = &fakeContainer{
		id: "cand-1", appName: "alpha", entrypoint: "service.js",
		image: "img-new", port: 80, replica: 0, state: container.StateRunning,
		environmentFingerprint: serviceEnvironmentFingerprint(80),
		resources:              serviceResources(),
		ready:                  &notReady,
	}
	services := NewServiceReconciler(f, nil, testutil.DiscardLogger(), time.Minute)
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

	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
	})
	var completeCount, settleCount atomic.Int32
	completed := make(chan error, 1)
	done := enqueueMeaningfulWithHooks(coordinator, "alpha", tmpl, "img-new", nil,
		func(err error) {
			completeCount.Add(1)
			select {
			case completed <- err:
			default:
			}
		},
		func() { settleCount.Add(1) },
	)

	assertPending := func(stage string, wantResolves int) {
		t.Helper()
		if got := completeCount.Load(); got != 0 {
			t.Fatalf("%s: completion calls = %d, want 0 (unready is not terminal)", stage, got)
		}
		if got := settleCount.Load(); got != 0 {
			t.Fatalf("%s: settlement calls = %d, want 0 (the request stays pending)", stage, got)
		}
		select {
		case <-done:
			t.Fatalf("%s: done closed while the request is pending", stage)
		default:
		}
		events := f.order()
		for _, ev := range events {
			if strings.HasPrefix(ev, "start:") {
				t.Fatalf("%s: events = %v, want no duplicate start while the candidate is unready", stage, events)
			}
		}
		f.mu.Lock()
		stops := append([]string(nil), f.stops...)
		f.mu.Unlock()
		if len(stops) != 0 {
			t.Fatalf("%s: stops = %v, want none (neither the candidate nor the old generation may be stopped)", stage, stops)
		}
		for _, id := range []string{"old-1", "cand-1"} {
			ctr, ok := f.containerByID(id)
			if !ok || ctr.state != container.StateRunning {
				t.Fatalf("%s: container %s = (%+v, %v), want still running", stage, id, ctr, ok)
			}
		}
		if got := f.resolveCount(); got != wantResolves {
			t.Fatalf("%s: resolved passes = %d, want %d (the wake must actually re-evaluate)", stage, got, wantResolves)
		}
	}

	// when the desired state is published, the pass must end pending: the
	// worker is released but the logical request is retained
	waitCoordinator(t, coordinator)
	assertPending("first pass", 1)

	// when a stale Notify wakes it while readiness is still false it re-evaluates
	// and ends pending again — still no duplicate start, no stop, no callback
	coordinator.Notify("alpha")
	waitCoordinator(t, coordinator)
	assertPending("stale Notify while unready", 2)

	// when the backend reports the candidate ready, the next wake converges
	// exactly once
	f.setReady("cand-1", true)
	coordinator.Notify("alpha")
	select {
	case err := <-completed:
		if err != nil {
			t.Fatalf("terminal completion err = %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the pass never converged after the backend reported the candidate ready")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("done did not close after the terminal completion")
	}
	waitCoordinator(t, coordinator)

	if got := completeCount.Load(); got != 1 {
		t.Fatalf("completion calls = %d, want exactly 1", got)
	}
	if got := settleCount.Load(); got != 1 {
		t.Fatalf("settlement calls = %d, want exactly 1", got)
	}
	events := f.order()
	if len(events) != 1 || events[0] != "stop:old-1" {
		t.Fatalf("events = %v, want exactly [stop:old-1] (the stale generation retired once, no starts)", events)
	}
	if got := f.resolveCount(); got != 3 {
		t.Fatalf("resolved passes = %d, want exactly 3 (two pending plus the converging wake)", got)
	}
	if _, ok := f.containerByID("old-1"); ok {
		t.Fatal("the stale old generation is still present after convergence")
	}
	candidate, ok := f.containerByID("cand-1")
	if !ok || candidate.state != container.StateRunning {
		t.Fatalf("candidate = (%+v, %v), want the ready replacement kept running", candidate, ok)
	}
	if got := f.runningCount("alpha", "service.js"); got != 1 {
		t.Fatalf("running replicas = %d, want exactly the converged candidate", got)
	}
}

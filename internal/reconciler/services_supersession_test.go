package reconciler

import (
	"context"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/routing"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// TestServiceCoordinatorSupersededPassPreservesOldGeneration covers gap 2: a
// request B that has already started its replacement but not yet committed must
// notice that a newer desired state C was enqueued during its pass, and abandon
// the commit. Concretely B must stop neither A (the running old generation it
// was about to supersede) nor leave its own provisional replacement behind; it
// removes the provisional container it created and lets pending C converge.
//
// The ordering is deterministic: the fake parks B's StartService at the exact
// post-start/pre-commit boundary (startGate), C is enqueued while B is parked,
// and C is itself parked before its commit so the assertions observe the state
// between "B abandoned" and "C accepted".
func TestServiceCoordinatorSupersededPassPreservesOldGeneration(t *testing.T) {
	f := newFakeDocker()
	services := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	coordinator.Start(lifecycle)

	// base is the same image-source service with a distinct desired env, so A/B/C
	// are the SAME SourceRef identity and therefore the SAME replica slot: B and C
	// both replace A in place.
	base := func(env string) *function.Template {
		tmpl := serviceTemplate("", function.Service{Image: "ghcr.io/acme/api:1.2", Port: 8080, Replicas: 1})
		tmpl.Env = map[string]string{"MODE": env}
		return tmpl
	}

	// Phase 1: converge MODE=a, creating generation A.
	coordinator.Enqueue("fn", base("a"), "", nil)
	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("wait initial: %v", err)
	}
	a := f.lastStartedFor("fn", "ghcr.io/acme/api:1.2")
	if a == nil {
		t.Fatal("no generation A was started")
	}
	f.mu.Lock()
	f.events = nil
	f.mu.Unlock()

	// Phase 2: B starts its replacement and parks before committing; C is
	// enqueued while B is parked, superseding it. The gate parks the FIRST call
	// (B) and the SECOND call (C) on separate channels so the test can observe
	// each boundary.
	bEntered := make(chan string, 1)
	bRelease := make(chan struct{})
	cEntered := make(chan string, 1)
	cRelease := make(chan struct{})
	var starts int
	f.mu.Lock()
	f.startGate = func(id string, _ runtime.ServiceSpec, _ int) {
		starts++
		switch starts {
		case 1:
			bEntered <- id
			<-bRelease
		case 2:
			cEntered <- id
			<-cRelease
		}
	}
	f.mu.Unlock()

	bComplete := make(chan error, 1)
	cComplete := make(chan error, 1)
	coordinator.EnqueueWithStatus("fn", base("b"), "", nil, nil, func(err error) { bComplete <- err })
	bID := <-bEntered

	coordinator.EnqueueWithStatus("fn", base("c"), "", nil, nil, func(err error) { cComplete <- err })

	// Phase 3: release B. It must detect the supersession at its commit boundary:
	// remove its own provisional replacement and keep A. Its worker then picks
	// pending C, whose replacement parks (cEntered) before committing.
	close(bRelease)
	<-cEntered

	// B is fully done and C has not committed yet. A must still be running, B's
	// provisional replacement must be gone, and B's stale completion must not have
	// fired (C owns status authority).
	if _, ok := f.containerByID(a.id); !ok {
		t.Fatal("generation A must remain running while C has not committed")
	}
	if _, ok := f.containerByID(bID); ok {
		t.Fatal("B's provisional replacement must be cleaned up on supersession")
	}
	select {
	case err := <-bComplete:
		t.Fatalf("superseded B completion must not fire (newer status authority), got %v", err)
	default:
	}

	// Phase 4: release C; it converges to the latest desired state and only then
	// replaces A (start-before-stop).
	close(cRelease)
	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("wait final: %v", err)
	}
	select {
	case err := <-cComplete:
		if err != nil {
			t.Fatalf("current C completion must fire with nil, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("current C completion did not fire")
	}
	if got := f.runningCount("fn", "ghcr.io/acme/api:1.2"); got != 1 {
		t.Fatalf("running = %d, want 1", got)
	}
	final := f.lastStartedFor("fn", "ghcr.io/acme/api:1.2")
	if final == nil || final.envHash != runtime.EnvHash([]string{"MODE=c", "PORT=8080"}) {
		t.Fatalf("final = %+v, want the latest MODE=c effective env", final)
	}
	// A was preserved through B's abandoned pass and stopped only by C's commit.
	if si, ai := f.indexOfEvent("start:0"), f.indexOfEvent("stop:"+a.id); si < 0 || ai < 0 || si > ai {
		t.Fatalf("order = %v, want C's start before A's stop", f.order())
	}

	cancel()
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer joinCancel()
	if err := coordinator.Join(joinCtx); err != nil {
		t.Fatalf("join: %v", err)
	}
}

// TestServiceCoordinatorSupersededPassPreservesRemovedSourceRef pins the
// supersession interaction with an identity change: while a pass converging a
// NEW SourceRef B is parked before commit, a newer desired state C (back to the
// original SourceRef A) arrives. B must abandon before it stops A's containers,
// which are "removed" under B's desired set, so A survives and C re-converges
// to it. This is the removal path of gap 1 combined with gap 2's authority
// guard.
func TestServiceCoordinatorSupersededPassPreservesRemovedSourceRef(t *testing.T) {
	f := newFakeDocker()
	services := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	coordinator.Start(lifecycle)

	tmplA := serviceTemplate("", function.Service{Image: "ghcr.io/acme/api:1", Port: 80, Replicas: 1})
	tmplB := serviceTemplate("", function.Service{Image: "ghcr.io/acme/api:2", Port: 80, Replicas: 1})
	// C returns to A's SourceRef but with a changed env, so C has corrective work
	// (a replacement for the same slot) and can be parked before its commit.
	tmplC := serviceTemplate("", function.Service{Image: "ghcr.io/acme/api:1", Port: 80, Replicas: 1})
	tmplC.Env = map[string]string{"MODE": "c"}

	// Phase 1: converge generation A.
	coordinator.Enqueue("fn", tmplA, "", nil)
	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("wait initial: %v", err)
	}
	a := f.lastStartedFor("fn", "ghcr.io/acme/api:1")
	if a == nil {
		t.Fatal("no generation A was started")
	}
	f.mu.Lock()
	f.events = nil
	f.mu.Unlock()

	// Phase 2: B parks after starting its replacement; enqueue C (= A's
	// SourceRef, env c) while B is parked.
	bEntered := make(chan string, 1)
	bRelease := make(chan struct{})
	cEntered := make(chan string, 1)
	cRelease := make(chan struct{})
	var starts int
	f.mu.Lock()
	f.startGate = func(id string, _ runtime.ServiceSpec, _ int) {
		starts++
		switch starts {
		case 1:
			bEntered <- id
			<-bRelease
		case 2:
			cEntered <- id
			<-cRelease
		}
	}
	f.mu.Unlock()

	coordinator.EnqueueWithStatus("fn", tmplB, "", nil, nil, func(error) {})
	<-bEntered
	coordinator.EnqueueWithStatus("fn", tmplC, "", nil, nil, func(error) {})

	close(bRelease)
	<-cEntered

	// A's containers were "removed" under B's desired set, but B was superseded
	// before its removed-service cleanup: A must survive. C (which does desire
	// A's SourceRef) has started its replacement but not committed yet.
	if _, ok := f.containerByID(a.id); !ok {
		t.Fatal("A (removed under B) must be preserved while B is superseded and C has not committed")
	}

	close(cRelease)
	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("wait final: %v", err)
	}
	if got := f.runningCount("fn", "ghcr.io/acme/api:1"); got != 1 {
		t.Fatalf("A's SourceRef running = %d, want 1 after C converged", got)
	}
	if got := f.runningCount("fn", "ghcr.io/acme/api:2"); got != 0 {
		t.Fatalf("B running = %d, want 0", got)
	}
	// A was preserved through B's abandoned pass and stopped only by C's commit.
	if si, ai := f.indexOfEvent("start:0"), f.indexOfEvent("stop:"+a.id); si < 0 || ai < 0 || si > ai {
		t.Fatalf("order = %v, want C's start before A's stop", f.order())
	}

	cancel()
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer joinCancel()
	if err := coordinator.Join(joinCtx); err != nil {
		t.Fatalf("join: %v", err)
	}
}

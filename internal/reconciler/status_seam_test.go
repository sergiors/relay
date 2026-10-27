package reconciler

import (
	"context"
	"testing"

	"github.com/moby/moby/api/types/container"

	"relay/internal/app"
	"relay/internal/routing"
	"relay/internal/testutil"
)

// TestReconcileWithStatusCallbackOrdering pins the focused status-callback
// contract the reconciler wires for an app that declares services:
//
//	onReconcileStart -> reconciling (source resolved / convergence beginning)
//	onComplete(nil)  -> ready (only after the full generation converged)
//
// There is no per-source build boundary at the service level any more: the two
// sources (entrypoint, image) both resolve without a Relay-owned Dockerfile
// build, so onReconcileStart is the only boundary callback.
func TestReconcileWithStatusCallbackOrdering(t *testing.T) {
	t.Run("entrypoint service fires reconciling on corrective work", func(t *testing.T) {
		f := newFakeDocker()
		entryTmpl := serviceTemplate("node24", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})

		var order []string
		c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)
		if err := c.ApplyWithStatus(context.Background(), "fn", entryTmpl, "img", nil,
			func() { order = append(order, "reconciling") },
		); err != nil {
			t.Fatalf("apply: %v", err)
		}
		if len(order) != 1 || order[0] != "reconciling" {
			t.Fatalf("callback order = %v, want [reconciling]", order)
		}
	})

	t.Run("image service fires reconciling on corrective work", func(t *testing.T) {
		f := newFakeDocker()
		imgTmpl := serviceTemplate("", app.Service{Name: "nginx:1.27", Image: "nginx:1.27", Port: 80, Replicas: 1})

		var order []string
		c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)
		if err := c.ApplyWithStatus(context.Background(), "fn", imgTmpl, "img", nil,
			func() { order = append(order, "reconciling") },
		); err != nil {
			t.Fatalf("apply: %v", err)
		}
		if len(order) != 1 || order[0] != "reconciling" {
			t.Fatalf("callback order = %v, want [reconciling]", order)
		}
	})
}

// TestReconcileWithStatusNoOpVerificationDoesNotNotifyReconciling pins the
// focused correction: a fully-converged service pass is a VERIFICATION, not
// convergence work. It must not fire the reconciling callback, so a periodic
// no-op tick can never publish a spurious reconciling status for an already-ready
// app.
func TestReconcileWithStatusNoOpVerificationDoesNotNotifyReconciling(t *testing.T) {
	f := newFakeDocker()
	f.ctrs["id-1"] = &fakeContainer{
		id: "id-1", appName: "fn", entrypoint: "service.js",
		image: "img-1", port: 80, replica: 0, state: container.StateRunning,
		envHash: serviceEnvHash(80), resources: serviceResources(),
	}
	tmpl := serviceTemplate("node24", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})

	var reconciling int
	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)
	if err := c.ApplyWithStatus(context.Background(), "fn", tmpl, "img-1", nil,
		func() { reconciling++ },
	); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if reconciling != 0 {
		t.Fatalf("reconciling fired %d times on a no-op verification, want 0", reconciling)
	}
}

// TestReconcileWithStatusCorrectiveStartNotifiesReconciling pins the other side:
// a pass that actually starts a missing replica performs corrective work, so it
// fires reconciling exactly once before converging the container.
func TestReconcileWithStatusCorrectiveStartNotifiesReconciling(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})

	var reconciling int
	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)
	if err := c.ApplyWithStatus(context.Background(), "fn", tmpl, "img-1", nil,
		func() { reconciling++ },
	); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if reconciling != 1 {
		t.Fatalf("reconciling fired %d times on a corrective pass, want 1", reconciling)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want 1", got)
	}
}

// TestReconcileWithStatusRemovedServiceNotifiesReconciling pins that stopping a
// removed service's leftover container is corrective work: the pass must publish
// reconciling (there is no per-service loop and no build in this template).
func TestReconcileWithStatusRemovedServiceNotifiesReconciling(t *testing.T) {
	f := newFakeDocker()
	f.ctrs["old-1"] = &fakeContainer{
		id: "old-1", appName: "fn", entrypoint: "old.js",
		image: "img-1", port: 80, replica: 0, state: container.StateRunning,
	}
	tmpl := serviceTemplate("node24") // old.js is no longer desired

	var reconciling int
	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)
	if err := c.ApplyWithStatus(context.Background(), "fn", tmpl, "img-1", nil,
		func() { reconciling++ },
	); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if reconciling != 1 {
		t.Fatalf("reconciling fired %d times when stopping a removed service, want 1", reconciling)
	}
	if len(f.stops) != 1 || f.stops[0] != "old-1" {
		t.Fatalf("stops = %v, want [old-1]", f.stops)
	}
}

// errServiceConverge is the sentinel service-convergence failure used above.
var errServiceConverge = &boomError{"service converge failed"}

type boomError struct{ msg string }

func (e *boomError) Error() string { return e.msg }

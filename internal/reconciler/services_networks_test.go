package reconciler

import (
	"context"
	"testing"

	"relay/internal/app"
	"relay/internal/routing"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// serviceNetworks returns the canonical relay.networks label a started
// container carries, for asserting the desired set.
func serviceNetworks(networks ...string) string { return runtime.NetworksLabel(networks...) }

// A service container with no global networks and no routing joins nothing and
// sends no NetworkingConfig (relay.networks is absent).
func TestReconcileServiceNoNetworksUnrouted(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
	if _, err := reconcileNetworks(t, f, "fn", tmpl, "img-1", nil, routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no started container")
	}
	if len(c.networks) != 0 {
		t.Fatalf("networks = %v, want none", c.networks)
	}
}

// An unrouted service joins exactly the worker-global NETWORKS set, and the
// canonical relay.networks label records it.
func TestReconcileServiceGlobalNetworksUnrouted(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
	if _, err := reconcileNetworks(t, f, "fn", tmpl, "img-1", []string{"backend", "frontend"}, routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(f.networkLookups) != 0 {
		t.Fatalf("NetworkExists called %d times for an unrouted service, want 0", len(f.networkLookups))
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no started container")
	}
	if got := len(c.networks); got != 2 {
		t.Fatalf("networks = %v, want the global set", c.networks)
	}
}

// A routed service joins the global set PLUS its routing network, de-duplicated
// deterministically when TRAEFIK_NETWORK is also a global network.
func TestReconcileServiceGlobalAndRoutingNetworksMerge(t *testing.T) {
	t.Run("distinct routing network", func(t *testing.T) {
		f := newFakeDocker()
		tmpl := serviceTemplate("node24", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1, Host: "a.test"})
		if _, err := reconcileNetworks(t, f, "fn", tmpl, "img-1", []string{"backend"}, routing.TraefikConfig{Network: "proxy"}); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		c := f.lastStartedFor("fn", "service.js")
		if c == nil {
			t.Fatal("no started container")
		}
		if got := serviceNetworks(c.networks...); got != "backend,proxy" {
			t.Fatalf("relay.networks = %q, want backend,proxy", got)
		}
		// Only the routing network is verified per reconcile; the global set is
		// startup configuration verified once at worker startup.
		if len(f.networkLookups) != 1 || f.networkLookups[0] != "proxy" {
			t.Fatalf("networkLookups = %v, want [proxy]", f.networkLookups)
		}
	})

	t.Run("routing network duplicated in global set joins once", func(t *testing.T) {
		f := newFakeDocker()
		tmpl := serviceTemplate("node24", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1, Host: "a.test"})
		if _, err := reconcileNetworks(t, f, "fn", tmpl, "img-1", []string{"proxy", "backend"}, routing.TraefikConfig{Network: "proxy"}); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		c := f.lastStartedFor("fn", "service.js")
		if c == nil {
			t.Fatal("no started container")
		}
		if got := serviceNetworks(c.networks...); got != "backend,proxy" {
			t.Fatalf("relay.networks = %q, want backend,proxy (proxy once)", got)
		}
		if len(c.networks) != 2 {
			t.Fatalf("spec networks = %v, want exactly two", c.networks)
		}
	})
}

// A single global network (no routing) is applied exactly once.
func TestReconcileServiceSingleGlobalNetwork(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
	if _, err := reconcileNetworks(t, f, "fn", tmpl, "img-1", []string{"backend"}, routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no started container")
	}
	if serviceNetworks(c.networks...) != "backend" {
		t.Fatalf("relay.networks = %q, want backend", serviceNetworks(c.networks...))
	}
	if len(c.networks) != 1 {
		t.Fatalf("networks = %v, want [backend]", c.networks)
	}
}

// Adding a global network replaces the running container (new desired network
// set), and the replacement carries the new canonical label.
func TestReconcileServiceGlobalNetworkAddedReplaces(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
	if _, err := reconcileNetworks(t, f, "fn", tmpl, "img-1", []string{"backend"}, routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile backend: %v", err)
	}
	if _, err := reconcileNetworks(t, f, "fn", tmpl, "img-1", []string{"backend", "frontend"}, routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile backend+frontend: %v", err)
	}
	if len(f.stops) != 1 {
		t.Fatalf("stops = %v, want one replaced container", f.stops)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil || serviceNetworks(c.networks...) != "backend,frontend" {
		t.Fatalf("replacement networks = %v, want backend,frontend", c)
	}
}

// Removing a global network replaces the container with the shrunken set.
func TestReconcileServiceGlobalNetworkRemovedReplaces(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
	if _, err := reconcileNetworks(t, f, "fn", tmpl, "img-1", []string{"backend", "frontend"}, routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile both: %v", err)
	}
	if _, err := reconcileNetworks(t, f, "fn", tmpl, "img-1", []string{"backend"}, routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile backend: %v", err)
	}
	if len(f.stops) != 1 {
		t.Fatalf("stops = %v, want one replaced container", f.stops)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil || serviceNetworks(c.networks...) != "backend" {
		t.Fatalf("replacement networks = %v, want backend", c)
	}
}

// Switching one global network for another replaces the container.
func TestReconcileServiceGlobalNetworkSwitchReplaces(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
	if _, err := reconcileNetworks(t, f, "fn", tmpl, "img-1", []string{"backend"}, routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile backend: %v", err)
	}
	if _, err := reconcileNetworks(t, f, "fn", tmpl, "img-1", []string{"other"}, routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile other: %v", err)
	}
	if len(f.stops) != 1 {
		t.Fatalf("stops = %v, want one replaced container", f.stops)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil || serviceNetworks(c.networks...) != "other" {
		t.Fatalf("replacement networks = %v, want other", c)
	}
}

// A reordered global network set is NOT a change: the canonical label makes the
// comparison order-invariant, so the running container is preserved.
func TestReconcileServiceGlobalNetworkOrderNoReplacement(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
	if _, err := reconcileNetworks(t, f, "fn", tmpl, "img-1", []string{"backend", "frontend"}, routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	stopsSoFar := len(f.stops)
	changed, err := reconcileNetworks(t, f, "fn", tmpl, "img-1", []string{"frontend", "backend"}, routing.TraefikConfig{})
	if err != nil {
		t.Fatalf("reconcile reordered: %v", err)
	}
	if changed {
		t.Fatal("a reordered global network set must be a no-op (set-based comparison)")
	}
	if len(f.stops) != stopsSoFar {
		t.Fatalf("stops grew from %d to %d on a reordered pass", stopsSoFar, len(f.stops))
	}
}

// A converged service with global + routing networks is a no-op on the next
// pass.
func TestReconcileServiceNetworksConvergedNoOp(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1, Host: "a.test"})
	if _, err := reconcileNetworks(t, f, "fn", tmpl, "img-1", []string{"backend"}, routing.TraefikConfig{Network: "proxy"}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	stopsSoFar := len(f.stops)
	changed, err := reconcileNetworks(t, f, "fn", tmpl, "img-1", []string{"backend"}, routing.TraefikConfig{Network: "proxy"})
	if err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if changed {
		t.Fatal("converged network state must be a no-op")
	}
	if len(f.stops) != stopsSoFar {
		t.Fatalf("stops grew from %d to %d on a converged pass", stopsSoFar, len(f.stops))
	}
}

// ServiceReconciler.WithNetworks wires the global set through Apply (the
// production shape), so the reconciler — not the caller — owns the network set.
func TestServiceReconcilerWithNetworksAppliesGlobals(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout,
		WithNetworks([]string{"backend", "frontend"}))
	if err := c.Apply(context.Background(), "fn", tmpl, "img-1", nil); err != nil {
		t.Fatalf("apply: %v", err)
	}
	started := f.lastStartedFor("fn", "service.js")
	if started == nil || serviceNetworks(started.networks...) != "backend,frontend" {
		t.Fatalf("started networks = %v, want backend,frontend", started)
	}
}

// WithNetworks copies its argument, so a caller mutating the slice after
// construction cannot change the reconciler's configuration.
func TestServiceReconcilerWithNetworksCopies(t *testing.T) {
	configured := []string{"backend"}
	c := NewServiceReconciler(newFakeDocker(), nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout,
		WithNetworks(configured))
	configured[0] = "mutated"
	if len(c.networks) != 1 || c.networks[0] != "backend" {
		t.Fatalf("networks = %v, want the copied [backend]", c.networks)
	}
}

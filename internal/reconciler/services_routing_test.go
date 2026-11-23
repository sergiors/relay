package reconciler

import (
	"strings"
	"testing"

	"github.com/moby/moby/api/types/container"

	"relay/internal/app"
)

// lastStartedFor returns the most recently started fakeContainer for fn/entry.
func (f *fakeDocker) lastStartedFor(fn, entrypoint string) *fakeContainer {
	f.mu.Lock()
	defer f.mu.Unlock()
	var found *fakeContainer
	for _, c := range f.ctrs {
		if c.appName == fn && c.entrypoint == entrypoint {
			found = c
		}
	}
	return found
}

// hasTraefikKey reports whether the label map carries any traefik.* key.
func hasTraefikKey(labels map[string]string) bool {
	for k := range labels {
		if strings.HasPrefix(k, "traefik.") {
			return true
		}
	}
	return false
}

// An unrouted service reconciles normally: no routing-network lookup and no
// provider labels on the started container.
func TestReconcileUnroutedNoRoutingActivity(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("nodejs24.x", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
	if _, err := reconcile(t, f, "fn", tmpl, "img-1"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(f.networkLookups) != 0 {
		t.Fatalf("NetworkExists called %d times for an unrouted service, want 0", len(f.networkLookups))
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no started container")
	}
	if hasTraefikKey(c.labels) {
		t.Fatalf("unrouted container labels contain traefik keys: %v", c.labels)
	}
	if len(c.networks) != 0 {
		t.Fatalf("unrouted container networks = %v, want none", c.networks)
	}
}

// A routed service no longer needs a provider network or provider-specific
// configuration: it starts without routing labels, and its route is published
// separately by the registry/discovery path.
func TestReconcileRoutedStartsWithoutProviderLabels(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Host: "service.test", Path: "/v2",
	})
	if _, err := reconcile(t, f, "fn", tmpl, "img-1"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(f.networkLookups) != 0 {
		t.Fatalf("NetworkExists called %d times, want 0 (no provider network)", len(f.networkLookups))
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no started container")
	}
	if hasTraefikKey(c.labels) {
		t.Fatalf("routed container labels contain traefik keys: %v", c.labels)
	}
	if len(c.networks) != 0 {
		t.Fatalf("routed container networks = %v, want none", c.networks)
	}
}

// A changed route (host or path) is served from the current template and must
// not replace an otherwise-converged container.
func TestReconcileRouteChangeDoesNotReplace(t *testing.T) {
	f := newFakeDocker()
	start := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Host: "old.example.com", Path: "/v1",
	})
	if _, err := reconcile(t, f, "fn", start, "img-1"); err != nil {
		t.Fatalf("reconcile start: %v", err)
	}
	before := f.lastStartedFor("fn", "service.js")
	if before == nil {
		t.Fatal("no started container")
	}

	moved := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Host: "new.example.com", Path: "/v2",
	})
	changed, err := reconcile(t, f, "fn", moved, "img-1")
	if err != nil {
		t.Fatalf("reconcile moved: %v", err)
	}
	if changed {
		t.Fatal("a routing host/path change must not replace the container")
	}
	after := f.lastStartedFor("fn", "service.js")
	if after == nil || after.id != before.id {
		t.Fatalf("container replaced: before=%v after=%v", before, after)
	}
}

// An invalid ROUTING_HOST_OVERRIDE is refused before any container action.
func TestReconcileHostOverrideInvalidRefused(t *testing.T) {
	f := newFakeDocker()
	f.mu.Lock()
	f.ctrs["keep-1"] = &fakeContainer{
		id: "keep-1", appName: "fn", entrypoint: "service.js",
		image: "img-1", port: 80, replica: 0, state: container.StateRunning,
	}
	f.mu.Unlock()
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Host: "service.test",
	})

	_, err := reconcileOverride(t, f, "fn", tmpl, "img-1", "-bad")
	if err == nil {
		t.Fatal("expected a routing-validation error")
	}
	if !strings.Contains(err.Error(), `service "service.js"`) ||
		!strings.Contains(err.Error(), "ROUTING_HOST_OVERRIDE") {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.stops) != 0 {
		t.Fatalf("stops = %v, want none (existing containers left alone)", f.stops)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want 1 (nothing replaced)", got)
	}
}

// A host-override change is operator routing configuration only: it must not
// replace a converged container.
func TestReconcileHostOverrideChangeDoesNotReplace(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Host: "issuer.example.com",
	})
	if _, err := reconcileOverride(t, f, "fn", tmpl, "img-1", "one.test"); err != nil {
		t.Fatalf("reconcile start: %v", err)
	}
	before := f.lastStartedFor("fn", "service.js")
	if before == nil {
		t.Fatal("no started container")
	}

	changed, err := reconcileOverride(t, f, "fn", tmpl, "img-1", "two.test")
	if err != nil {
		t.Fatalf("reconcile override change: %v", err)
	}
	if changed {
		t.Fatal("a host-override change must not replace the container")
	}
	after := f.lastStartedFor("fn", "service.js")
	if after == nil || after.id != before.id {
		t.Fatalf("container replaced: before=%v after=%v", before, after)
	}
}

// A routed (host + path) service converges like any other: a second pass over
// an unchanged routed template is a no-op — no stops, no starts, the same
// container kept.
func TestReconcileRoutedConvergedIsNoOp(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Host: "service.test", Path: "/v2",
	})
	if _, err := reconcile(t, f, "fn", tmpl, "img-1"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	first := f.lastStartedFor("fn", "service.js")
	if first == nil {
		t.Fatal("no started container")
	}
	stopsSoFar := len(f.stops)

	changed, err := reconcile(t, f, "fn", tmpl, "img-1")
	if err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if changed {
		t.Fatal("converged routed state must be a no-op (changed = false)")
	}
	if len(f.stops) != stopsSoFar {
		t.Fatalf("stops grew from %d to %d on a converged routed pass",
			stopsSoFar, len(f.stops))
	}
	second := f.lastStartedFor("fn", "service.js")
	if second == nil || second.id != first.id {
		t.Fatalf("container replaced: first=%v second=%v", first, second)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want 1 (untouched)", got)
	}
}

// An overlong ROUTING_HOST_OVERRIDE fails the hostname contract before any
// container or network work, naming both the variable and the length limit.
func TestReconcileHostOverrideOverlongValueRefused(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Host: "service.test",
	})
	overlong := strings.Repeat("a", 254)

	_, err := reconcileOverride(t, f, "fn", tmpl, "img-1", overlong)
	if err == nil {
		t.Fatal("expected an overlong-override routing error")
	}
	if !strings.Contains(err.Error(), "ROUTING_HOST_OVERRIDE") ||
		!strings.Contains(err.Error(), "253-character hostname limit") {
		t.Fatalf("error %v must name ROUTING_HOST_OVERRIDE and the length limit", err)
	}
	if len(f.networkLookups) != 0 || len(f.ctrs) != 0 || len(f.stops) != 0 {
		t.Fatalf("overlong override performed container/network work: "+
			"lookups=%v ctrs=%d stops=%v", f.networkLookups, len(f.ctrs), f.stops)
	}
}

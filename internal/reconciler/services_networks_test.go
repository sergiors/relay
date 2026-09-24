package reconciler

import (
	"strings"
	"testing"

	"github.com/moby/moby/api/types/container"

	"relay/internal/function"
	"relay/internal/routing"
	"relay/internal/runtime"
)

// templateWithNetworks builds a services-only template with the given top-level
// networks and the given services.
func templateWithNetworks(networks []string, services ...function.Service) *function.Template {
	t := serviceTemplate("node24", services...)
	t.Networks = networks
	return t
}

// An unrouted service with template networks joins exactly those networks (and
// no routing network). A converged second pass is a no-op.
func TestReconcileTemplateNetworksUnrouted(t *testing.T) {
	f := newFakeDocker()
	tmpl := templateWithNetworks(
		[]string{"backend", "frontend"},
		function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1},
	)
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no started container")
	}
	if len(c.networks) != 2 || c.networks[0] != "backend" || c.networks[1] != "frontend" {
		t.Fatalf("spec.Networks = %v, want [backend frontend]", c.networks)
	}
	// An unrouted service never joins the routing network.
	if c.network != "" {
		t.Fatalf("routing network = %q, want \"\"", c.network)
	}
	if len(f.networkLookups) == 0 {
		t.Fatal("expected the template networks to be verified before create")
	}

	changed, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{})
	if err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if changed {
		t.Fatal("converged template-networks state must be a no-op")
	}
}

// A routed service joins the union of the routing network and the template
// networks, exactly once each (the routing network is deduped when it also
// appears in the template list).
func TestReconcileTemplateNetworksRoutedUnion(t *testing.T) {
	f := newFakeDocker()
	tmpl := templateWithNetworks(
		[]string{"backend", "proxy"},
		function.Service{Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "a.test"},
	)
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{Network: "proxy"}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no started container")
	}
	if c.network != "proxy" {
		t.Fatalf("routing network = %q, want proxy", c.network)
	}
	if len(c.networks) != 2 {
		t.Fatalf("spec.Networks = %v, want [backend proxy]", c.networks)
	}
	// The discovered canonical label is the sorted union, each once.
	sc := f.containerFor("fn", "service.js")
	if sc == nil {
		t.Fatal("no discovered container")
	}
	if got := runtime.NetworksLabel(sc.network, sc.networks); got != "backend,proxy" {
		t.Fatalf("relay.networks = %q, want backend,proxy", got)
	}
	if c.labels[routingNetworkKey] != "proxy" {
		t.Fatalf("traefik.docker.network = %q, want proxy (labels preserved)", c.labels[routingNetworkKey])
	}
}

// Adding a template network replaces the running container WITHOUT an image
// change: the image reference is unchanged across passes.
func TestReconcileTemplateNetworkChangeReplacesWithoutImageChange(t *testing.T) {
	f := newFakeDocker()
	before := templateWithNetworks(
		[]string{"backend"},
		function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1},
	)
	if _, err := reconcile(t, f, "fn", before, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile before: %v", err)
	}
	after := templateWithNetworks(
		[]string{"backend", "frontend"},
		function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1},
	)
	if _, err := reconcile(t, f, "fn", after, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile after: %v", err)
	}
	if len(f.stops) != 1 {
		t.Fatalf("stops = %v, want the network-stale container replaced", f.stops)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no replacement container")
	}
	// The SAME image: a runtime-only network change never rebuilds.
	if c.image != "img-1" {
		t.Fatalf("replacement image = %q, want img-1 (no rebuild on network change)", c.image)
	}
	if len(c.networks) != 2 {
		t.Fatalf("replacement networks = %v, want [backend frontend]", c.networks)
	}
}

// A converged second pass is a no-op when the network set is unchanged, even
// when the template order differs (the normalization sorts).
func TestReconcileTemplateNetworkOrderConverged(t *testing.T) {
	f := newFakeDocker()
	first := templateWithNetworks(
		[]string{"backend", "frontend"},
		function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1},
	)
	if _, err := reconcile(t, f, "fn", first, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	reordered := templateWithNetworks(
		[]string{"frontend", "backend"},
		function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1},
	)
	changed, err := reconcile(t, f, "fn", reordered, "img-1", routing.TraefikConfig{})
	if err != nil {
		t.Fatalf("reconcile reordered: %v", err)
	}
	if changed {
		t.Fatal("the same network set in a different order must be a no-op")
	}
	if len(f.stops) != 0 {
		t.Fatalf("stops = %v, want none", f.stops)
	}
}

// A missing template network preserves the healthy current container and
// reports the missing name; it never tears the container down and never creates
// a network.
func TestReconcileTemplateNetworkMissingKeepsHealthy(t *testing.T) {
	f := newFakeDocker()
	tmpl := templateWithNetworks(
		[]string{"backend"},
		function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1},
	)
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want 1", got)
	}
	stopsSoFar := len(f.stops)

	// The network disappears: the next pass must preserve the healthy container.
	f.missingNetworks["backend"] = true
	_, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{})
	if err == nil || !strings.Contains(err.Error(), `network "backend" does not exist`) {
		t.Fatalf("err = %v, want a missing-network error", err)
	}
	if len(f.stops) != stopsSoFar {
		t.Fatalf("stops grew %d -> %d; a missing network must not tear down a healthy container",
			stopsSoFar, len(f.stops))
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want the healthy container preserved", got)
	}
	// No new container was started either: convergence is skipped entirely.
	if got := f.countForFunction("fn"); got != 1 {
		t.Fatalf("containers = %d, want 1 (no churn)", got)
	}
}

// A missing template network preserves the healthy desired services but STILL
// removes a service that was removed from the template: removed-service cleanup
// does not depend on network availability, so it must not be skipped by the
// network pre-flight. This pins the ordering fix (cleanup before pre-flight).
func TestReconcileTemplateNetworkMissingStillRemovesRemovedService(t *testing.T) {
	f := newFakeDocker()
	// A leftover container for a service the template no longer declares, plus a
	// healthy wanted service.
	before := templateWithNetworks(
		[]string{"backend"},
		function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1},
		function.Service{Entrypoint: "old.js", Port: 80, Replicas: 1},
	)
	if _, err := reconcile(t, f, "fn", before, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile before: %v", err)
	}
	if got := f.runningCount("fn", "old.js"); got != 1 {
		t.Fatalf("old.js running = %d, want 1", got)
	}
	stopsSoFar := len(f.stops)

	// Remove old.js from the template AND make the network missing in the same
	// pass. The removed container must still be stopped, while the healthy
	// desired service's container is preserved.
	after := templateWithNetworks(
		[]string{"backend"},
		function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1},
	)
	f.missingNetworks["backend"] = true
	_, err := reconcile(t, f, "fn", after, "img-1", routing.TraefikConfig{})
	if err == nil || !strings.Contains(err.Error(), `network "backend" does not exist`) {
		t.Fatalf("err = %v, want a missing-network error", err)
	}
	if got := f.runningCount("fn", "old.js"); got != 0 {
		t.Fatalf("old.js running = %d, want 0 (removed-service cleanup must not be skipped)", got)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("service.js running = %d, want the healthy desired container preserved", got)
	}
	if len(f.stops) != stopsSoFar+1 {
		t.Fatalf("stops grew by %d, want exactly 1 (the removed service)", len(f.stops)-stopsSoFar)
	}
}

// A missing template network on a template that declares NO services still
// performs no start work and no spurious network verification (the pre-flight is
// skipped), while leftover containers are still cleaned up.
func TestReconcileTemplateNetworkMissingServicesLessStillCleansUp(t *testing.T) {
	f := newFakeDocker()
	f.ctrs["leftover-1"] = &fakeContainer{
		id: "leftover-1", function: "fn", entrypoint: "service.js",
		image: "img-old", port: 80, replica: 0, state: container.StateRunning,
	}
	tmpl := templateWithNetworks([]string{"backend"}) // no services
	f.missingNetworks["backend"] = true

	changed, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !changed {
		t.Fatal("changed = false, want true (the leftover container was removed)")
	}
	if got := f.countForFunction("fn"); got != 0 {
		t.Fatalf("leftover containers = %d, want 0", got)
	}
	if len(f.networkLookups) != 0 {
		t.Fatalf("network lookups = %v, want none for a services-less template", f.networkLookups)
	}
}

// A missing template network ONLY affects the function whose template declares
// it: a reconcile of another function still converges normally.
func TestReconcileTemplateNetworkMissingIsFunctionScoped(t *testing.T) {
	f := newFakeDocker()
	f.missingNetworks["broken"] = true

	broken := templateWithNetworks(
		[]string{"broken"},
		function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1},
	)
	if _, err := reconcile(t, f, "fn-a", broken, "img-1", routing.TraefikConfig{}); err == nil {
		t.Fatal("expected a missing-network error for fn-a")
	}
	if got := f.countForFunction("fn-a"); got != 0 {
		t.Fatalf("fn-a containers = %d, want 0", got)
	}

	healthy := templateWithNetworks(
		[]string{"ok"},
		function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1},
	)
	if _, err := reconcile(t, f, "fn-b", healthy, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("fn-b must converge despite fn-a's missing network: %v", err)
	}
	if got := f.runningCount("fn-b", "service.js"); got != 1 {
		t.Fatalf("fn-b running = %d, want 1", got)
	}
}

// containerFor returns the fakeContainer discovered for fn/service (whatever its
// state), or nil.
func (f *fakeDocker) containerFor(fn, service string) *fakeContainer {
	f.mu.Lock()
	defer f.mu.Unlock()
	var found *fakeContainer
	for _, c := range f.ctrs {
		if c.function == fn && c.entrypoint == service {
			found = c
		}
	}
	return found
}

// TestReconcileNetworksLabelLegacyContainerReplaced pins that a container
// created before relay.networks existed (label absent) is replaced once.
func TestReconcileNetworksLabelLegacyContainerReplaced(t *testing.T) {
	f := newFakeDocker()
	f.ctrs["legacy-1"] = &fakeContainer{
		id: "legacy-1", function: "fn", entrypoint: "service.js",
		image: "img-1", port: 80, replica: 0, state: container.StateRunning,
		envHash: serviceEnvHash(80),
	}
	tmpl := templateWithNetworks(
		[]string{"backend"},
		function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1},
	)
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(f.stops) != 1 {
		t.Fatalf("stops = %v, want the legacy (unlabeled networks) container replaced", f.stops)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil || len(c.networks) != 1 || c.networks[0] != "backend" {
		t.Fatalf("replacement = %+v, want network backend", c)
	}
}

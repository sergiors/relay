package reconciler

import (
	"strings"
	"testing"

	"github.com/moby/moby/api/types/container"

	"relay/internal/function"
	"relay/internal/routing"
)

// Traefik-label helpers used by the routing tests below.
const (
	routingEnableKey     = "traefik.enable"
	routingNetworkKey    = "traefik.docker.network"
	routingRouterPrefix  = "traefik.http.routers."
	routingServicePrefix = "traefik.http.services."
)

// lastStartedFor returns the most recently started fakeContainer for fn/entry.
func (f *fakeDocker) lastStartedFor(fn, entrypoint string) *fakeContainer {
	f.mu.Lock()
	defer f.mu.Unlock()
	var found *fakeContainer
	for _, c := range f.ctrs {
		if c.function == fn && c.entrypoint == entrypoint {
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

// An unrouted service with a zero TraefikConfig reconciles normally: no
// NetworkExists lookup, no traefik.* labels on the started container.
func TestReconcileUnroutedNoRoutingActivity(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{}); err != nil {
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
	if c.network != "" {
		t.Fatalf("unrouted container network = %q, want \"\"", c.network)
	}
}

// A routed service with an empty TraefikConfig fails validation and performs
// no container action: no stops, no starts, existing containers left alone.
func TestReconcileRoutedMissingTraefikConfig(t *testing.T) {
	f := newFakeDocker()
	f.mu.Lock()
	f.ctrs["keep-1"] = &fakeContainer{id: "keep-1", function: "fn", entrypoint: "service.js", image: "img-1", port: 80, replica: 0, state: container.StateRunning}
	f.mu.Unlock()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1, Host: "service.test"})

	_, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{})
	if err == nil {
		t.Fatal("expected a routing-validation error")
	}
	if !strings.Contains(err.Error(), `service "service.js": TRAEFIK_NETWORK is required when Traefik routing is configured`) {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.networkLookups) != 0 {
		t.Fatalf("NetworkExists called, want 0 (validate fails first)")
	}
	if len(f.stops) != 0 {
		t.Fatalf("stops = %v, want none (existing containers left alone)", f.stops)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want 1 (nothing replaced)", got)
	}
}

// A routed service whose configured network does not exist is refused; Relay
// does NOT create the network (zero StartService calls). The error wraps the
// routing package's MissingNetwork sentinel so callers can classify it.
func TestReconcileRoutedMissingNetwork(t *testing.T) {
	f := newFakeDocker()
	f.missingNetworks["proxy"] = true
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1, Host: "service.test"})

	_, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{Network: "proxy"})
	if err == nil {
		t.Fatal("expected a missing-network error")
	}
	if !strings.Contains(err.Error(), routing.MissingNetwork("proxy").Error()) {
		t.Fatalf("error %v does not carry the MissingNetwork message", err)
	}
	if len(f.networkLookups) != 1 || f.networkLookups[0] != "proxy" {
		t.Fatalf("networkLookups = %v, want a single [proxy] lookup", f.networkLookups)
	}
	if len(f.ctrs) != 0 {
		t.Fatalf("StartService was called for a missing routing network; want none, ctrs = %v", f.ctrs)
	}
	if len(f.stops) != 0 {
		t.Fatalf("stops = %v, want none", f.stops)
	}
}

// A routed service happy path: labels and network reach the started container.
func TestReconcileRoutedHappyPath(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "service.test"})
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{Network: "proxy"}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no started container")
	}
	if c.labels[routingEnableKey] != "true" {
		t.Fatalf("traefik.enable = %q, want true", c.labels[routingEnableKey])
	}
	if c.labels[routingNetworkKey] != "proxy" {
		t.Fatalf("traefik.docker.network = %q, want proxy", c.labels[routingNetworkKey])
	}
	id := "relay-fn-service-js" // relay-<fn>-<entrypoint>
	wantRule := "Host(`service.test`)"
	if c.labels[routingRouterPrefix+id+".rule"] != wantRule {
		t.Fatalf("router rule = %q, want %q", c.labels[routingRouterPrefix+id+".rule"], wantRule)
	}
	if _, ok := c.labels[routingServicePrefix+id+".loadbalancer.server.port"]; !ok {
		t.Fatalf("loadbalancer port label missing in %v", c.labels)
	}
	if c.network != "proxy" {
		t.Fatalf("started network = %q, want proxy", c.network)
	}
	if len(f.networkLookups) != 1 || f.networkLookups[0] != "proxy" {
		t.Fatalf("networkLookups = %v, want [proxy]", f.networkLookups)
	}
	// Network-only config: no optional HTTPS labels at all.
	for k := range c.labels {
		if strings.Contains(k, ".entrypoints") || strings.HasSuffix(k, ".tls") ||
			strings.Contains(k, "certresolver") || strings.Contains(k, ".priority") {
			t.Fatalf("network-only routed container has optional label %s: %v", k, c.labels)
		}
	}
}

// A routed service under a full HTTPS Traefik config: the started container
// carries the four base labels PLUS all four optional ones on the same id.
func TestReconcileRoutedFullHTTPSConfig(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "service.test"})
	cfg := routing.TraefikConfig{Network: "proxy", EntryPoints: "websecure", CertResolver: "letsencrypt", Priority: intPtr(100)}
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", cfg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no started container")
	}
	if c.network != "proxy" {
		t.Fatalf("network = %q, want proxy", c.network)
	}
	id := "relay-fn-service-js"
	want := map[string]string{
		routingEnableKey:                   "true",
		routingNetworkKey:                  "proxy",
		routingRouterPrefix + id + ".rule": "Host(`service.test`)",
		"traefik.http.services." + id + ".loadbalancer.server.port": "3000",
		routingRouterPrefix + id + ".entrypoints":                   "websecure",
		routingRouterPrefix + id + ".tls":                           "true",
		routingRouterPrefix + id + ".tls.certresolver":              "letsencrypt",
		routingRouterPrefix + id + ".priority":                      "100",
	}
	if len(c.labels) != len(want) {
		t.Fatalf("labels = %v (%d), want %d keys", c.labels, len(c.labels), len(want))
	}
	for k, v := range want {
		if c.labels[k] != v {
			t.Fatalf("label %q = %q, want %q (all: %v)", k, c.labels[k], v, c.labels)
		}
	}
}

// Changing any routing option replaces the routed container. Here the
// certresolver (and thus both tls labels) is cleared: the old container is
// stopped and the replacement carries NO tls/tls.certresolver labels.
func TestReconcileCertResolverClearedReplacesWithoutTLS(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "a.test"})
	start := routing.TraefikConfig{Network: "proxy", EntryPoints: "websecure", CertResolver: "letsencrypt", Priority: intPtr(100)}
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", start); err != nil {
		t.Fatalf("reconcile https: %v", err)
	}

	cleared := routing.TraefikConfig{Network: "proxy", EntryPoints: "websecure", Priority: intPtr(100)}
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", cleared); err != nil {
		t.Fatalf("reconcile cleared: %v", err)
	}
	if len(f.stops) != 1 {
		t.Fatalf("stops = %v, want one replaced container", f.stops)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no replacement container")
	}
	id := "relay-fn-service-js"
	for _, k := range []string{routingRouterPrefix + id + ".tls", routingRouterPrefix + id + ".tls.certresolver"} {
		if _, ok := c.labels[k]; ok {
			t.Fatalf("replacement label %s present after clearing certresolver: %v", k, c.labels)
		}
	}
	if c.labels[routingRouterPrefix+id+".entrypoints"] != "websecure" {
		t.Fatalf("replacement entrypoints = %q, want websecure (still set)", c.labels[routingRouterPrefix+id+".entrypoints"])
	}
	if c.labels[routingRouterPrefix+id+".priority"] != "100" {
		t.Fatalf("replacement priority = %q, want 100 (still set)", c.labels[routingRouterPrefix+id+".priority"])
	}
}

// Clearing the priority (nil) also replaces the routed container: the
// replacement carries NO priority label while entrypoints stays.
func TestReconcilePriorityClearedReplacesWithoutPriority(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "a.test"})
	start := routing.TraefikConfig{Network: "proxy", EntryPoints: "websecure", Priority: intPtr(42)}
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", start); err != nil {
		t.Fatalf("reconcile priority: %v", err)
	}

	cleared := routing.TraefikConfig{Network: "proxy", EntryPoints: "websecure"}
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", cleared); err != nil {
		t.Fatalf("reconcile cleared: %v", err)
	}
	if len(f.stops) != 1 {
		t.Fatalf("stops = %v, want one replaced container", f.stops)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no replacement container")
	}
	if _, ok := c.labels[routingRouterPrefix+"relay-fn-service-js.priority"]; ok {
		t.Fatalf("replacement priority label present after clearing priority: %v", c.labels)
	}
	if c.labels[routingRouterPrefix+"relay-fn-service-js.entrypoints"] != "websecure" {
		t.Fatalf("replacement entrypoints = %q, want websecure", c.labels[routingRouterPrefix+"relay-fn-service-js.entrypoints"])
	}
}

// Unrouted service with all HTTPS config values set: still no traefik labels
// and no network lookup.
func TestReconcileUnroutedWithHTTPSConfigNoRouting(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})
	cfg := routing.TraefikConfig{Network: "proxy", EntryPoints: "websecure", CertResolver: "letsencrypt", Priority: intPtr(100)}
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", cfg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(f.networkLookups) != 0 {
		t.Fatalf("NetworkExists called for an unrouted service, want 0")
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no started container")
	}
	if hasTraefikKey(c.labels) {
		t.Fatalf("unrouted container labels contain traefik keys: %v", c.labels)
	}
	if c.network != "" {
		t.Fatalf("network = %q, want \"\"", c.network)
	}
}

func intPtr(i int) *int { return &i }

// A host change (a.test -> b.test) replaces the existing routed container: the
// old one is stopped and the replacement carries the new rule.
func TestReconcileHostChangeReplaces(t *testing.T) {
	f := newFakeDocker()
	start := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "a.test"})
	if _, err := reconcile(t, f, "fn", start, "img-1", routing.TraefikConfig{Network: "proxy"}); err != nil {
		t.Fatalf("reconcile a.test: %v", err)
	}

	changed := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "b.test"})
	if _, err := reconcile(t, f, "fn", changed, "img-1", routing.TraefikConfig{Network: "proxy"}); err != nil {
		t.Fatalf("reconcile b.test: %v", err)
	}
	if len(f.stops) != 1 {
		t.Fatalf("stops = %v, want one replaced container", f.stops)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no replacement container")
	}
	id := "relay-fn-service-js"
	if c.labels[routingRouterPrefix+id+".rule"] != "Host(`b.test`)" {
		t.Fatalf("replacement rule = %q, want Host(`b.test`)", c.labels[routingRouterPrefix+id+".rule"])
	}
}

// A TRAEFIK_NETWORK change also replaces the routed container.
func TestReconcileNetworkChangeReplaces(t *testing.T) {
	f := newFakeDocker()
	start := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "a.test"})
	if _, err := reconcile(t, f, "fn", start, "img-1", routing.TraefikConfig{Network: "proxy"}); err != nil {
		t.Fatalf("reconcile proxy: %v", err)
	}
	if _, err := reconcile(t, f, "fn", start, "img-1", routing.TraefikConfig{Network: "proxy2"}); err != nil {
		t.Fatalf("reconcile proxy2: %v", err)
	}
	if len(f.stops) != 1 {
		t.Fatalf("stops = %v, want one replaced container", f.stops)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil || c.network != "proxy2" || c.labels[routingNetworkKey] != "proxy2" {
		t.Fatalf("replacement container = %+v, want network/labels proxy2", c)
	}
}

// A HOST removed from a routed template makes the previously routed container
// stale: it is replaced with an unlabeled (no traefik.*) container and it no
// longer joins the routing network.
func TestReconcileHostRemovedReplacesUnrouted(t *testing.T) {
	f := newFakeDocker()
	routed := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "a.test"})
	if _, err := reconcile(t, f, "fn", routed, "img-1", routing.TraefikConfig{Network: "proxy"}); err != nil {
		t.Fatalf("reconcile routed: %v", err)
	}

	unrouted := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 3000, Replicas: 1})
	if _, err := reconcile(t, f, "fn", unrouted, "img-1", routing.TraefikConfig{Network: "proxy"}); err != nil {
		t.Fatalf("reconcile unrouted: %v", err)
	}
	if len(f.stops) != 1 {
		t.Fatalf("stops = %v, want the routed container replaced", f.stops)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no replacement container")
	}
	if hasTraefikKey(c.labels) {
		t.Fatalf("replacement labels contain traefik keys: %v", c.labels)
	}
	if c.network != "" {
		t.Fatalf("replacement network = %q, want \"\"", c.network)
	}
}

// A converged ROUTED state (running container with matching labels) is a no-op
// pass: changed == false, no stops, no starts.
func TestReconcileRoutedConvergedNoOp(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "a.test"})
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{Network: "proxy"}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	stopsSoFar := len(f.stops)
	changed, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{Network: "proxy"})
	if err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if changed {
		t.Fatal("converged routed state must be a no-op (changed = false)")
	}
	if len(f.stops) != stopsSoFar {
		t.Fatalf("stops grew from %d to %d on a converged pass", stopsSoFar, len(f.stops))
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want 1 (untouched)", got)
	}
}

// routingLabelsMatch unit tests pin the nil-vs-nil and extra-traefik-key rules.
func TestRoutingLabelsMatch(t *testing.T) {
	if !routingLabelsMatch(nil, nil) {
		t.Error("nil-vs-nil must match (unrouted, unlabeled)")
	}
	desired := map[string]string{"traefik.enable": "true", "traefik.docker.network": "proxy"}
	if !routingLabelsMatch(desired, map[string]string{
		"relay.type":             "service",
		"relay.function":         "fn",
		"traefik.enable":         "true",
		"traefik.docker.network": "proxy",
	}) {
		t.Error("actual with all desired keys plus non-traefik extras must match")
	}
	if routingLabelsMatch(desired, map[string]string{"traefik.enable": "false"}) {
		t.Error("wrong desired value must not match")
	}
	if routingLabelsMatch(nil, map[string]string{"traefik.enable": "true"}) {
		t.Error("a container with traefik labels must never match an unrouted service")
	}
	if routingLabelsMatch(desired, map[string]string{"traefik.enable": "true", "traefik.docker.network": "proxy", "traefik.http.routers.old.rule": "Host(`old.test`)"}) {
		t.Error("a stale extra traefik key must not match")
	}
	// The desired nil actual case: routed service, container without the labels.
	if routingLabelsMatch(map[string]string{"traefik.enable": "true"}, nil) {
		t.Error("routed desired vs unlabeled container must not match")
	}
}

// A routed service with a path reaches the started container with the exact
// PathPrefix rule and the StripPrefix middleware referenced by the router.
func TestReconcileRoutedPathLabels(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{
		Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "service.test", Path: "/v2",
	})
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{Network: "proxy"}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no started container")
	}
	id := "relay-fn-service-js"
	mw := "relay-fn-service-js-path"
	if got := c.labels[routingRouterPrefix+id+".rule"]; got != "Host(`service.test`) && PathPrefix(`/v2`)" {
		t.Fatalf("rule = %q", got)
	}
	if got := c.labels[routingRouterPrefix+id+".middlewares"]; got != mw {
		t.Fatalf("middlewares = %q, want %q", got, mw)
	}
	if got := c.labels["traefik.http.middlewares."+mw+".stripprefix.prefixes"]; got != "/v2" {
		t.Fatalf("stripprefix.prefixes = %q, want /v2", got)
	}
}

// Two services on the same host with different paths start with distinct router
// and middleware labels (no collision).
func TestReconcileSameHostDifferentPathsDistinct(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24",
		function.Service{Entrypoint: "v1.js", Port: 3000, Replicas: 1, Host: "same.test", Path: "/v1"},
		function.Service{Entrypoint: "v2.js", Port: 3000, Replicas: 1, Host: "same.test", Path: "/v2"},
	)
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{Network: "proxy"}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	v1 := f.lastStartedFor("fn", "v1.js")
	v2 := f.lastStartedFor("fn", "v2.js")
	if v1 == nil || v2 == nil {
		t.Fatal("expected both containers started")
	}
	if v1.labels[routingRouterPrefix+"relay-fn-v1-js.rule"] != "Host(`same.test`) && PathPrefix(`/v1`)" {
		t.Fatalf("v1 rule = %q", v1.labels[routingRouterPrefix+"relay-fn-v1-js.rule"])
	}
	if v2.labels[routingRouterPrefix+"relay-fn-v2-js.rule"] != "Host(`same.test`) && PathPrefix(`/v2`)" {
		t.Fatalf("v2 rule = %q", v2.labels[routingRouterPrefix+"relay-fn-v2-js.rule"])
	}
	v1mw := v1.labels[routingRouterPrefix+"relay-fn-v1-js.middlewares"]
	v2mw := v2.labels[routingRouterPrefix+"relay-fn-v2-js.middlewares"]
	if v1mw == "" || v1mw == v2mw {
		t.Fatalf("same-host services must not share a middleware name: %q vs %q", v1mw, v2mw)
	}
}

// Changing a service path makes the running container stale: it is stopped and
// replaced with one carrying the new PathPrefix rule and middleware.
func TestReconcilePathChangeReplaces(t *testing.T) {
	f := newFakeDocker()
	start := serviceTemplate("node24", function.Service{
		Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "a.test", Path: "/v1",
	})
	if _, err := reconcile(t, f, "fn", start, "img-1", routing.TraefikConfig{Network: "proxy"}); err != nil {
		t.Fatalf("reconcile v1: %v", err)
	}

	changed := serviceTemplate("node24", function.Service{
		Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "a.test", Path: "/v2",
	})
	if _, err := reconcile(t, f, "fn", changed, "img-1", routing.TraefikConfig{Network: "proxy"}); err != nil {
		t.Fatalf("reconcile v2: %v", err)
	}
	if len(f.stops) != 1 {
		t.Fatalf("stops = %v, want one replaced container", f.stops)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no replacement container")
	}
	id := "relay-fn-service-js"
	if got := c.labels[routingRouterPrefix+id+".rule"]; got != "Host(`a.test`) && PathPrefix(`/v2`)" {
		t.Fatalf("replacement rule = %q", got)
	}
	if got := c.labels["traefik.http.middlewares."+id+"-path.stripprefix.prefixes"]; got != "/v2" {
		t.Fatalf("replacement stripprefix = %q", got)
	}
}

// Removing a path (back to host-only) makes the path-routed container stale: it
// is replaced with the legacy host-only label set and no middleware labels.
func TestReconcilePathRemovedReplacesWithoutMiddleware(t *testing.T) {
	f := newFakeDocker()
	routed := serviceTemplate("node24", function.Service{
		Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "a.test", Path: "/v2",
	})
	if _, err := reconcile(t, f, "fn", routed, "img-1", routing.TraefikConfig{Network: "proxy"}); err != nil {
		t.Fatalf("reconcile path: %v", err)
	}

	hostOnly := serviceTemplate("node24", function.Service{
		Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "a.test",
	})
	if _, err := reconcile(t, f, "fn", hostOnly, "img-1", routing.TraefikConfig{Network: "proxy"}); err != nil {
		t.Fatalf("reconcile host-only: %v", err)
	}
	if len(f.stops) != 1 {
		t.Fatalf("stops = %v, want the path-routed container replaced", f.stops)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no replacement container")
	}
	id := "relay-fn-service-js"
	if got := c.labels[routingRouterPrefix+id+".rule"]; got != "Host(`a.test`)" {
		t.Fatalf("replacement host-only rule = %q", got)
	}
	for k := range c.labels {
		if strings.Contains(k, "middlewares") || strings.Contains(k, "stripprefix") {
			t.Fatalf("host-only replacement must carry no middleware labels: %v", c.labels)
		}
	}
}

// A converged path-routed state is a no-op: identical path labels keep the
// container (no churn).
func TestReconcilePathConvergedNoOp(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{
		Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "a.test", Path: "/v2",
	})
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{Network: "proxy"}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	stopsSoFar := len(f.stops)
	changed, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{Network: "proxy"})
	if err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if changed {
		t.Fatal("converged path-routed state must be a no-op (changed = false)")
	}
	if len(f.stops) != stopsSoFar {
		t.Fatalf("stops grew from %d to %d on a converged pass", stopsSoFar, len(f.stops))
	}
}

// A host override maps the declared host in the started container's rule while
// the network, id, and (for a path service) the StripPrefix labels are
// unchanged. The template's declared host is never mutated.
func TestReconcileHostOverrideMapsRule(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{
		Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "issuer.example.com", Path: "/v2",
	})
	cfg := routing.TraefikConfig{Network: "proxy", HostOverride: "localhost"}
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", cfg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no started container")
	}
	id := "relay-fn-service-js"
	if got := c.labels[routingRouterPrefix+id+".rule"]; got != "Host(`issuer.localhost`) && PathPrefix(`/v2`)" {
		t.Fatalf("rule = %q, want Host(`issuer.localhost`) && PathPrefix(`/v2`)", got)
	}
	if got := c.labels["traefik.http.middlewares."+id+"-path.stripprefix.prefixes"]; got != "/v2" {
		t.Fatalf("stripprefix = %q, want /v2 (unchanged)", got)
	}
	if c.labels[routingNetworkKey] != "proxy" || c.network != "proxy" {
		t.Fatalf("network = labels %q / spec %q, want proxy", c.labels[routingNetworkKey], c.network)
	}
	// The template host is untouched.
	if tmpl.Services[0].Host != "issuer.example.com" {
		t.Fatalf("template host mutated to %q", tmpl.Services[0].Host)
	}
}

// Two services on distinct subdomains of the same domain stay distinct after
// the override.
func TestReconcileHostOverrideDistinctSubdomains(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24",
		function.Service{Entrypoint: "issuer.js", Port: 3000, Replicas: 1, Host: "issuer.example.com"},
		function.Service{Entrypoint: "admin.js", Port: 3000, Replicas: 1, Host: "admin.example.com"},
	)
	cfg := routing.TraefikConfig{Network: "proxy", HostOverride: "localhost"}
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", cfg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	issuer := f.lastStartedFor("fn", "issuer.js")
	admin := f.lastStartedFor("fn", "admin.js")
	if issuer == nil || admin == nil {
		t.Fatal("expected both containers started")
	}
	if got := issuer.labels[routingRouterPrefix+"relay-fn-issuer-js.rule"]; got != "Host(`issuer.localhost`)" {
		t.Fatalf("issuer rule = %q", got)
	}
	if got := admin.labels[routingRouterPrefix+"relay-fn-admin-js.rule"]; got != "Host(`admin.localhost`)" {
		t.Fatalf("admin rule = %q", got)
	}
}

// Changing only the override replaces the routed container: the old labels
// point at the wrong host, so the container is stale.
func TestReconcileHostOverrideChangeReplaces(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "a.test"})
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{Network: "proxy"}); err != nil {
		t.Fatalf("reconcile no override: %v", err)
	}
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{Network: "proxy", HostOverride: "localhost"}); err != nil {
		t.Fatalf("reconcile override: %v", err)
	}
	if len(f.stops) != 1 {
		t.Fatalf("stops = %v, want one replaced container", f.stops)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no replacement container")
	}
	if got := c.labels[routingRouterPrefix+"relay-fn-service-js.rule"]; got != "Host(`a.localhost`)" {
		t.Fatalf("replacement rule = %q, want Host(`a.localhost`)", got)
	}
}

// An invalid override fails routing validation and performs no container
// action, matching the missing-network path.
func TestReconcileHostOverrideInvalidRefused(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1, Host: "a.test"})
	_, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{Network: "proxy", HostOverride: "-bad"})
	if err == nil {
		t.Fatal("expected a routing-validation error")
	}
	if !strings.Contains(err.Error(), "TRAEFIK_HOST_OVERRIDE") {
		t.Fatalf("error %v does not name TRAEFIK_HOST_OVERRIDE", err)
	}
	if len(f.networkLookups) != 0 || len(f.ctrs) != 0 || len(f.stops) != 0 {
		t.Fatalf("invalid override performed container/network work: lookups=%v ctrs=%d stops=%v", f.networkLookups, len(f.ctrs), f.stops)
	}
}

// An override that is itself a valid hostname but derives an OVERLONG host
// from a long declared left label is refused per service, before any network
// or container work — no stops, no starts, NetworkExists never called.
func TestReconcileHostOverrideOverlongDerivedHostRefused(t *testing.T) {
	f := newFakeDocker()
	// 63-char left label + "." + a 201-char valid override = 265 derived chars.
	longOverride := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." +
		strings.Repeat("c", 63) + "." + strings.Repeat("d", 9)
	tmpl := serviceTemplate("node24", function.Service{
		Entrypoint: "service.js", Port: 80, Replicas: 1,
		Host: strings.Repeat("z", 63) + ".example.com",
	})
	_, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{Network: "proxy", HostOverride: longOverride})
	if err == nil {
		t.Fatal("expected an overlong-derived-host routing error")
	}
	if !strings.Contains(err.Error(), "253-character hostname limit") {
		t.Fatalf("error %v does not carry the 253-character message", err)
	}
	if len(f.networkLookups) != 0 || len(f.ctrs) != 0 || len(f.stops) != 0 {
		t.Fatalf("overlong derived host performed network/container work: lookups=%v ctrs=%d stops=%v", f.networkLookups, len(f.ctrs), f.stops)
	}
}

// A valid overridden case that stays within the limit still reconciles: a long
// left label plus a short override maps to a valid host and starts normally.
func TestReconcileHostOverrideLongLabelShortDomainValid(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{
		Entrypoint: "service.js", Port: 3000, Replicas: 1,
		Host: strings.Repeat("z", 63) + ".example.com",
	})
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{Network: "proxy", HostOverride: "localhost"}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no started container")
	}
	wantRule := "Host(`" + strings.Repeat("z", 63) + ".localhost`)"
	if got := c.labels[routingRouterPrefix+"relay-fn-service-js.rule"]; got != wantRule {
		t.Fatalf("rule = %q, want %q", got, wantRule)
	}
	if len(f.networkLookups) != 1 || f.networkLookups[0] != "proxy" {
		t.Fatalf("networkLookups = %v, want [proxy]", f.networkLookups)
	}
}

// An absent override (empty) reconciles exactly as before: the declared host
// verbatim.
func TestReconcileAbsentOverrideUsesDeclaredHost(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "issuer.example.com"})
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{Network: "proxy", HostOverride: ""}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no started container")
	}
	if got := c.labels[routingRouterPrefix+"relay-fn-service-js.rule"]; got != "Host(`issuer.example.com`)" {
		t.Fatalf("rule = %q, want the declared host verbatim", got)
	}
}

// A converged override state is a no-op: identical mapped labels keep the
// container (no churn).
func TestReconcileHostOverrideConvergedNoOp(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "a.test"})
	cfg := routing.TraefikConfig{Network: "proxy", HostOverride: "localhost"}
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", cfg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	stopsSoFar := len(f.stops)
	changed, err := reconcile(t, f, "fn", tmpl, "img-1", cfg)
	if err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if changed {
		t.Fatal("converged override state must be a no-op (changed = false)")
	}
	if len(f.stops) != stopsSoFar {
		t.Fatalf("stops grew from %d to %d on a converged pass", stopsSoFar, len(f.stops))
	}
}

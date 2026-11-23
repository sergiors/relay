package discovery

import (
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// routable builds a proxy-neutral discovery endpoint fixture; persistence
// details do not exist at this boundary.
func routable(app, service, host, path, addr string, hostPort int) routableEndpoint {
	return routableEndpoint{
		app: app, service: service, host: host, path: path,
		advertiseAddr: addr, hostPort: hostPort,
	}
}

// onlyRouter returns the sole router and its identifier.
func onlyRouter(t *testing.T, cfg traefikConfig) (string, traefikRouter) {
	t.Helper()
	if len(cfg.HTTP.Routers) != 1 {
		t.Fatalf("routers = %v, want exactly one", cfg.HTTP.Routers)
	}
	for id, router := range cfg.HTTP.Routers {
		return id, router
	}
	t.Fatal("unreachable: no routers but one was iterated")
	return "", traefikRouter{}
}

// routerForRule returns the unique router carrying the exact rule.
func routerForRule(t *testing.T, cfg traefikConfig, rule string) traefikRouter {
	t.Helper()
	var found traefikRouter
	matches := 0
	for _, router := range cfg.HTTP.Routers {
		if router.Rule == rule {
			found = router
			matches++
		}
	}
	if matches != 1 {
		t.Fatalf("routers with rule %q = %d, want exactly 1 (have %v)",
			rule, matches, cfg.HTTP.Routers)
	}
	return found
}

func serverURLs(svc traefikService) []string {
	urls := make([]string, len(svc.LoadBalancer.Servers))
	for i, server := range svc.LoadBalancer.Servers {
		urls[i] = server.URL
	}
	return urls
}

// conflictSummaries flattens conflicts to comparable strings preserving the
// deterministic order (host, then path, then sorted identities).
func conflictSummaries(conflicts []routeConflict) []string {
	out := make([]string, len(conflicts))
	for i, c := range conflicts {
		out[i] = c.host + "\x00" + c.path + "\x00" + strings.Join(c.serviceNames(), ",")
	}
	return out
}

// Given one service replica set spanning several nodes with IPv4 and IPv6
// advertised addresses, when the configuration is built, then a single router
// serves every backend exactly once, deduplicated by URL and sorted into a
// deterministic order.
func TestBuildTraefikConfigGroupsReplicasAcrossNodes(t *testing.T) {
	cfg, conflicts := buildTraefikConfig([]routableEndpoint{
		routable("alpha", "web", "api.example.com", "", "192.0.2.10", 8080),
		routable("alpha", "web", "api.example.com", "", "192.0.2.11", 8081),
		routable("alpha", "web", "api.example.com", "", "192.0.2.11", 8081), // dup
		routable("alpha", "web", "api.example.com", "", "2001:db8::1", 8082),
	})
	if len(conflicts) != 0 {
		t.Fatalf("conflicts = %+v, want none for a single identity", conflicts)
	}
	if len(cfg.HTTP.Routers) != 1 || len(cfg.HTTP.Services) != 1 {
		t.Fatalf("routers = %d services = %d, want one of each",
			len(cfg.HTTP.Routers), len(cfg.HTTP.Services))
	}
	if len(cfg.HTTP.Middlewares) != 0 {
		t.Fatalf("middlewares = %v, want none for a host-only route",
			cfg.HTTP.Middlewares)
	}

	_, router := onlyRouter(t, cfg)
	if router.Rule != "Host(`api.example.com`)" {
		t.Fatalf("rule = %q", router.Rule)
	}
	if router.Priority != 1 {
		t.Fatalf("priority = %d, want 1 (explicit host-only priority)", router.Priority)
	}
	service, ok := cfg.HTTP.Services[router.Service]
	if !ok {
		t.Fatalf("router service %q missing from services", router.Service)
	}
	want := []string{
		"http://192.0.2.10:8080",
		"http://192.0.2.11:8081",
		"http://[2001:db8::1]:8082",
	}
	if got := serverURLs(service); !slices.Equal(got, want) {
		t.Fatalf("servers = %v, want %v (deduplicated, sorted, IPv6 bracketed)",
			got, want)
	}
}

// Given distinct logical identities claiming the same effective route, when the
// configuration is built, then every claim is reported as a conflict and NO
// route is emitted for it — including when the hosts differ only by case or
// surrounding whitespace.
func TestBuildTraefikConfigDistinctIdentitiesClaimingSameRouteConflict(t *testing.T) {
	for _, tc := range []struct {
		name      string
		endpoints []routableEndpoint
		wantHost  string
		wantNames []string
	}{
		{
			name: "different app",
			endpoints: []routableEndpoint{
				routable("alpha", "web", "api.test", "", "192.0.2.10", 8080),
				routable("beta", "web", "api.test", "", "192.0.2.11", 8081),
			},
			wantHost:  "api.test",
			wantNames: []string{"alpha/web", "beta/web"},
		},
		{
			name: "different service in one app",
			endpoints: []routableEndpoint{
				routable("alpha", "web", "api.test", "", "192.0.2.10", 8080),
				routable("alpha", "api", "api.test", "", "192.0.2.11", 8081),
			},
			wantHost:  "api.test",
			wantNames: []string{"alpha/api", "alpha/web"},
		},
		{
			name: "host case and padding are not a distinction",
			endpoints: []routableEndpoint{
				routable("alpha", "web", "API.test", "", "192.0.2.10", 8080),
				routable("beta", "web", " api.test ", "", "192.0.2.11", 8081),
			},
			wantHost:  "api.test",
			wantNames: []string{"alpha/web", "beta/web"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, conflicts := buildTraefikConfig(tc.endpoints)
			if len(cfg.HTTP.Routers) != 0 || len(cfg.HTTP.Services) != 0 ||
				len(cfg.HTTP.Middlewares) != 0 {
				t.Fatalf("ambiguous route was emitted: %+v", cfg.HTTP)
			}
			if len(conflicts) != 1 {
				t.Fatalf("conflicts = %+v, want exactly one", conflicts)
			}
			if conflicts[0].host != tc.wantHost || conflicts[0].path != "" {
				t.Fatalf("conflict = %q/%q, want %q/\"\" (normalized)",
					conflicts[0].host, conflicts[0].path, tc.wantHost)
			}
			if got := conflicts[0].serviceNames(); !slices.Equal(got, tc.wantNames) {
				t.Fatalf("conflict services = %v, want %v (sorted by app then service)",
					got, tc.wantNames)
			}
		})
	}
}

// Given one ambiguous route among several, when the configuration is built,
// then every route at the conflicted host/path is omitted while distinct paths
// on the same host and unrelated hosts remain served.
func TestBuildTraefikConfigConflictOmitsAllAndRetainsUnrelatedRoutes(t *testing.T) {
	cfg, conflicts := buildTraefikConfig([]routableEndpoint{
		routable("alpha", "web", "shared.test", "", "192.0.2.10", 8080),
		routable("beta", "web", "shared.test", "", "192.0.2.11", 8081),
		routable("alpha", "web", "shared.test", "/v2", "192.0.2.10", 8080),
		routable("beta", "api", "other.test", "", "192.0.2.12", 8082),
	})

	if len(conflicts) != 1 || conflicts[0].host != "shared.test" ||
		conflicts[0].path != "" {
		t.Fatalf("conflicts = %+v, want the single shared.test host-only key", conflicts)
	}
	if len(cfg.HTTP.Routers) != 2 || len(cfg.HTTP.Services) != 2 ||
		len(cfg.HTTP.Middlewares) != 1 {
		t.Fatalf("routers = %d services = %d middlewares = %d, want 2/2/1 "+
			"(only the unrelated routes survive)",
			len(cfg.HTTP.Routers), len(cfg.HTTP.Services), len(cfg.HTTP.Middlewares))
	}
	for _, router := range cfg.HTTP.Routers {
		if router.Rule == "Host(`shared.test`)" {
			t.Fatal("the conflicted host-only route was emitted; it must be omitted")
		}
	}
	routerForRule(t, cfg, "Host(`shared.test`) && PathPrefix(`/v2`)")
	routerForRule(t, cfg, "Host(`other.test`)")
}

// Given two services collapsed onto one effective hostname by the worker-wide
// host override, when the configuration is built, then the colliding host-only
// route is omitted as a conflict while a distinct path on the same overridden
// hostname remains served.
func TestBuildTraefikConfigHostOverrideCollision(t *testing.T) {
	cfg, conflicts := buildTraefikConfig([]routableEndpoint{
		routable("alpha", "web", "portal.internal.test", "", "192.0.2.10", 8080),
		routable("beta", "web", "portal.internal.test", "", "192.0.2.11", 8081),
		routable("alpha", "admin", "portal.internal.test", "/admin",
			"192.0.2.12", 8082),
	})

	if len(conflicts) != 1 || conflicts[0].host != "portal.internal.test" {
		t.Fatalf("conflicts = %+v, want the override collision on portal.internal.test",
			conflicts)
	}
	if got := conflicts[0].serviceNames(); !slices.Equal(got,
		[]string{"alpha/web", "beta/web"}) {
		t.Fatalf("collision members = %v", got)
	}
	if len(cfg.HTTP.Routers) != 1 {
		t.Fatalf("routers = %v, want only the distinct /admin path", cfg.HTTP.Routers)
	}
	routerForRule(t, cfg,
		"Host(`portal.internal.test`) && PathPrefix(`/admin`)")
}

// An empty path and the root "/" claim the SAME route for conflict purposes:
// two identities spelling the route differently still collide, and every route
// at that key is omitted.
func TestBuildTraefikConfigHostOnlyAndRootEquatedForConflict(t *testing.T) {
	cfg, conflicts := buildTraefikConfig([]routableEndpoint{
		routable("alpha", "web", "api.test", "", "192.0.2.10", 8080),
		routable("beta", "web", "api.test", "/", "192.0.2.11", 8081),
	})

	if len(cfg.HTTP.Routers) != 0 || len(cfg.HTTP.Services) != 0 {
		t.Fatalf("host-only and root claims did not collide: %+v", cfg.HTTP)
	}
	if len(conflicts) != 1 {
		t.Fatalf("conflicts = %+v, want exactly one", conflicts)
	}
	if conflicts[0].host != "api.test" || conflicts[0].path != "" {
		t.Fatalf("conflict key = %q/%q, want api.test/\"\" (equated)",
			conflicts[0].host, conflicts[0].path)
	}
	if got := displayRoutePath(conflicts[0].path); got != "/" {
		t.Fatalf("display path = %q, want / (host-only routes display as root)", got)
	}
	if got := conflicts[0].serviceNames(); !slices.Equal(got,
		[]string{"alpha/web", "beta/web"}) {
		t.Fatalf("conflict services = %v", got)
	}
}

// Comparison normalization must not change rendered semantics: a single
// explicitly configured "/" route keeps PathPrefix(`/`) with its StripPrefix
// middleware, while an empty path stays host-only — and replicas that disagree
// between the two spellings keep the explicit root declaration.
func TestBuildTraefikConfigSingleRootRouteKeepsPathPrefixAndStripPrefix(t *testing.T) {
	t.Run("explicit root renders PathPrefix and StripPrefix", func(t *testing.T) {
		cfg, conflicts := buildTraefikConfig([]routableEndpoint{
			routable("rooter", "web", "root.test", "/", "192.0.2.10", 8080),
		})
		if len(conflicts) != 0 {
			t.Fatalf("conflicts = %+v, want none", conflicts)
		}
		id, router := onlyRouter(t, cfg)
		if router.Rule != "Host(`root.test`) && PathPrefix(`/`)" {
			t.Fatalf("rule = %q, want the explicit root prefix preserved",
				router.Rule)
		}
		if router.Priority != 2 {
			t.Fatalf("priority = %d, want 2 (explicit root outranks host-only)",
				router.Priority)
		}
		if len(router.Middlewares) != 1 {
			t.Fatalf("middlewares = %v, want the StripPrefix middleware", router.Middlewares)
		}
		mw, ok := cfg.HTTP.Middlewares[router.Middlewares[0]]
		if !ok || mw.StripPrefix == nil {
			t.Fatalf("middleware %q missing or not a StripPrefix: %+v",
				router.Middlewares[0], cfg.HTTP.Middlewares)
		}
		if !slices.Equal(mw.StripPrefix.Prefixes, []string{"/"}) {
			t.Fatalf("strip prefixes = %v, want [/]", mw.StripPrefix.Prefixes)
		}
		if router.Middlewares[0] == id {
			t.Fatal("middleware name must be distinct from the router/service id")
		}
		if router.Service != id {
			t.Fatalf("router service = %q, want the shared id %q",
				router.Service, id)
		}
	})

	t.Run("empty path stays host-only without middleware", func(t *testing.T) {
		cfg, conflicts := buildTraefikConfig([]routableEndpoint{
			routable("plain", "web", "plain.test", "", "192.0.2.10", 8080),
		})
		if len(conflicts) != 0 {
			t.Fatalf("conflicts = %+v, want none", conflicts)
		}
		_, router := onlyRouter(t, cfg)
		if router.Rule != "Host(`plain.test`)" {
			t.Fatalf("rule = %q, want the host-only form", router.Rule)
		}
		if router.Priority != 1 {
			t.Fatalf("priority = %d, want 1 (host-only is the lowest)", router.Priority)
		}
		if len(router.Middlewares) != 0 || len(cfg.HTTP.Middlewares) != 0 {
			t.Fatalf("host-only route emitted middleware: router %v map %v",
				router.Middlewares, cfg.HTTP.Middlewares)
		}
	})

	t.Run("replicas disagreeing on the spelling keep the explicit root",
		func(t *testing.T) {
			cfg, conflicts := buildTraefikConfig([]routableEndpoint{
				routable("alpha", "web", "disagree.test", "", "192.0.2.10", 8080),
				routable("alpha", "web", "disagree.test", "/", "192.0.2.11", 8081),
			})
			if len(conflicts) != 0 {
				t.Fatalf("conflicts = %+v, want none (one identity)", conflicts)
			}
			_, router := onlyRouter(t, cfg)
			if router.Rule != "Host(`disagree.test`) && PathPrefix(`/`)" {
				t.Fatalf("rule = %q, want the explicit root kept across nodes",
					router.Rule)
			}
			service, ok := cfg.HTTP.Services[router.Service]
			if !ok {
				t.Fatalf("service %q missing", router.Service)
			}
			want := []string{
				"http://192.0.2.10:8080",
				"http://192.0.2.11:8081",
			}
			if got := serverURLs(service); !slices.Equal(got, want) {
				t.Fatalf("servers = %v, want both replicas grouped", got)
			}
		})
}

// Given overlapping path prefixes on one host, when the configuration is
// built, then every router carries an explicit priority that strictly
// increases with path length, so the longer prefix deterministically wins the
// requests it overlaps and Traefik's own rule-length default never decides.
func TestBuildTraefikConfigOverlapPriorities(t *testing.T) {
	cfg, conflicts := buildTraefikConfig([]routableEndpoint{
		routable("alpha", "admin", "api.test", "/api/admin", "192.0.2.10", 8080),
		routable("alpha", "api", "api.test", "/api", "192.0.2.11", 8081),
		routable("alpha", "root", "api.test", "/", "192.0.2.12", 8082),
		routable("beta", "web", "plain.test", "", "192.0.2.13", 8083),
	})
	if len(conflicts) != 0 {
		t.Fatalf("conflicts = %+v, want none (distinct paths)", conflicts)
	}
	if len(cfg.HTTP.Routers) != 4 {
		t.Fatalf("routers = %v, want four", cfg.HTTP.Routers)
	}

	admin := routerForRule(t, cfg, "Host(`api.test`) && PathPrefix(`/api/admin`)")
	api := routerForRule(t, cfg, "Host(`api.test`) && PathPrefix(`/api`)")
	root := routerForRule(t, cfg, "Host(`api.test`) && PathPrefix(`/`)")
	hostOnly := routerForRule(t, cfg, "Host(`plain.test`)")

	for _, tc := range []struct {
		name string
		got  int
		want int
	}{
		{"longest prefix", admin.Priority, 11},
		{"overlapped prefix", api.Priority, 5},
		{"explicit root", root.Priority, 2},
		{"host-only", hostOnly.Priority, 1},
	} {
		if tc.got != tc.want {
			t.Errorf("%s priority = %d, want %d (explicit and deterministic)",
				tc.name, tc.got, tc.want)
		}
	}
	// /api/admin overlaps /api, which overlaps the root route: each longer
	// prefix must strictly outrank the shorter one it covers.
	if !(admin.Priority > api.Priority && api.Priority > root.Priority &&
		root.Priority > hostOnly.Priority) {
		t.Fatalf("priorities admin=%d api=%d root=%d host-only=%d, want strict "+
			"longest-prefix ordering", admin.Priority, api.Priority,
			root.Priority, hostOnly.Priority)
	}
}

// A path route renders the PathPrefix constraint and its StripPrefix
// middleware: the prefix reaches the provider exactly as configured, and the
// middleware name lives in a namespace distinct from the router/service id.
func TestBuildTraefikConfigPathPrefixAndStripPrefix(t *testing.T) {
	cfg, conflicts := buildTraefikConfig([]routableEndpoint{
		routable("alpha", "web", "api.example.com", "/v2", "192.0.2.10", 8080),
	})
	if len(conflicts) != 0 {
		t.Fatalf("conflicts = %+v, want none", conflicts)
	}
	id, router := onlyRouter(t, cfg)
	if router.Rule != "Host(`api.example.com`) && PathPrefix(`/v2`)" {
		t.Fatalf("rule = %q", router.Rule)
	}
	if router.Priority != 4 {
		t.Fatalf("priority = %d, want 4 (1 + len of /v2)", router.Priority)
	}
	if len(router.Middlewares) != 1 {
		t.Fatalf("middlewares = %v, want exactly the StripPrefix middleware",
			router.Middlewares)
	}
	mwID := router.Middlewares[0]
	mw, ok := cfg.HTTP.Middlewares[mwID]
	if !ok || mw.StripPrefix == nil {
		t.Fatalf("middleware %q missing or not a StripPrefix: %+v",
			mwID, cfg.HTTP.Middlewares)
	}
	if !slices.Equal(mw.StripPrefix.Prefixes, []string{"/v2"}) {
		t.Fatalf("strip prefixes = %v, want [/v2]", mw.StripPrefix.Prefixes)
	}
	if mwID == id {
		t.Fatalf("middleware name %q must not collide with the router id", mwID)
	}
	if router.Service != id {
		t.Fatalf("router service = %q, want the shared id %q", router.Service, id)
	}
	if _, ok := cfg.HTTP.Services[id]; !ok {
		t.Fatalf("service %q missing from services %v", id, cfg.HTTP.Services)
	}
}

// The same registry snapshot must produce byte-identical JSON and conflict
// reports regardless of endpoint order, identifiers must be stable and
// collision-resistant across routes, and long identities must stay within the
// provider's identifier cap.
func TestBuildTraefikConfigDeterministicAcrossEndpointOrder(t *testing.T) {
	endpoints := []routableEndpoint{
		routable("alpha", "web", "b.test", "", "192.0.2.20", 8080),
		routable("beta", "web", "b.test", "", "192.0.2.21", 8081),
		routable("alpha", "web", "a.test", "/v1", "192.0.2.10", 8080),
		routable("alpha", "admin", "a.test", "", "192.0.2.11", 8081),
		routable("beta", "web", "a.test", "/v1", "192.0.2.12", 8082),
	}
	reversed := make([]routableEndpoint, len(endpoints))
	for i, e := range endpoints {
		reversed[len(endpoints)-1-i] = e
	}

	cfg1, conflicts1 := buildTraefikConfig(endpoints)
	cfg2, conflicts2 := buildTraefikConfig(reversed)

	json1, err := json.Marshal(cfg1)
	if err != nil {
		t.Fatalf("marshal first build: %v", err)
	}
	json2, err := json.Marshal(cfg2)
	if err != nil {
		t.Fatalf("marshal second build: %v", err)
	}
	if string(json1) != string(json2) {
		t.Fatalf("JSON differs across endpoint order:\n%s\nvs\n%s", json1, json2)
	}
	if !slices.Equal(conflictSummaries(conflicts1), conflictSummaries(conflicts2)) {
		t.Fatalf("conflicts differ across endpoint order: %+v vs %+v",
			conflicts1, conflicts2)
	}
	// Conflicts are ordered by host then path: a.test sorts before b.test.
	summaries := conflictSummaries(conflicts1)
	if len(summaries) != 2 || !strings.HasPrefix(summaries[0], "a.test\x00") ||
		!strings.HasPrefix(summaries[1], "b.test\x00") {
		t.Fatalf("conflict order = %v, want host-sorted [a.test b.test]", summaries)
	}

	// Identifiers: `relay-<app>-<service>-<16 hex>`, stable for one route and
	// distinct across routes of the same service.
	idOf := func(host, path string) string {
		cfg, _ := buildTraefikConfig([]routableEndpoint{
			routable("alpha", "web", host, path, "192.0.2.10", 8080),
		})
		id, _ := onlyRouter(t, cfg)
		return id
	}
	const prefix = "relay-alpha-web-"
	id := idOf("a.test", "")
	if !strings.HasPrefix(id, prefix) {
		t.Fatalf("id = %q, want the %q prefix", id, prefix)
	}
	suffix := strings.TrimPrefix(id, prefix)
	if len(suffix) != providerIDHashLen {
		t.Fatalf("hash suffix = %q, want %d hex characters", suffix, providerIDHashLen)
	}
	if _, err := strconv.ParseUint(suffix, 16, 64); err != nil {
		t.Fatalf("hash suffix %q is not hex: %v", suffix, err)
	}
	if idOf("a.test", "") != id {
		t.Fatalf("id changed across identical inputs: %q vs %q", idOf("a.test", ""), id)
	}
	for name, other := range map[string]string{
		"different host": idOf("b.test", ""),
		"different path": idOf("a.test", "/v1"),
	} {
		if other == id {
			t.Errorf("%s must get a distinct id, both are %q", name, id)
		}
	}

	longIDOf := func(host string) string {
		cfg, _ := buildTraefikConfig([]routableEndpoint{
			routable(strings.Repeat("a", 120), strings.Repeat("b", 120),
				host, "", "192.0.2.10", 8080),
		})
		rid, _ := onlyRouter(t, cfg)
		return rid
	}
	longA, longB := longIDOf("a.test"), longIDOf("b.test")
	if longA == longB {
		t.Fatalf("truncated long identities collided: %q", longA)
	}
	if len(longA) > providerIDMaxLen {
		t.Fatalf("long id length = %d, want at most %d",
			len(longA), providerIDMaxLen)
	}
}

// An empty registry is a valid, deterministic, empty configuration.
func TestBuildTraefikConfigEmptyRegistry(t *testing.T) {
	for name, endpoints := range map[string][]routableEndpoint{
		"nil":   nil,
		"empty": {},
	} {
		t.Run(name, func(t *testing.T) {
			cfg, conflicts := buildTraefikConfig(endpoints)
			if len(conflicts) != 0 {
				t.Fatalf("conflicts = %+v, want none", conflicts)
			}
			payload, err := json.Marshal(cfg)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if string(payload) != `{"http":{}}` {
				t.Fatalf("JSON = %s, want an empty http object", payload)
			}
		})
	}
}

// Endpoints missing a routing fact — host, identity, advertised address, or a
// positive host port — are unusable: they are skipped entirely and can never
// claim a route or contribute to a conflict.
func TestBuildTraefikConfigSkipsIncompleteEndpoints(t *testing.T) {
	unusable := []routableEndpoint{
		routable("alpha", "web", "", "", "192.0.2.10", 8080),      // no host
		routable("", "web", "api.test", "", "192.0.2.10", 8080),   // no app
		routable("alpha", "", "api.test", "", "192.0.2.10", 8080), // no service
		routable("alpha", "web", "api.test", "", "", 8080),        // no address
		routable("alpha", "web", "api.test", "", "192.0.2.10", 0), // no port
	}

	cfg, conflicts := buildTraefikConfig(unusable)
	if len(cfg.HTTP.Routers) != 0 || len(conflicts) != 0 {
		t.Fatalf("unusable endpoints emitted routers=%v conflicts=%v, want none",
			cfg.HTTP.Routers, conflicts)
	}

	valid := routable("gamma", "web", "api.test", "", "192.0.2.30", 8080)
	cfg, conflicts = buildTraefikConfig(append(unusable, valid))
	if len(cfg.HTTP.Routers) != 1 {
		t.Fatalf("routers = %v, want only the valid endpoint", cfg.HTTP.Routers)
	}
	if len(conflicts) != 0 {
		t.Fatalf("conflicts = %+v, want none (skipped endpoints never claim)", conflicts)
	}
}

// Host casing/whitespace and a trailing path slash are cosmetic: replicas
// publishing different spellings of one route group into a single deterministic
// router with the normalized rule.
func TestBuildTraefikConfigNormalizesHostCaseAndPathStyle(t *testing.T) {
	cfg, conflicts := buildTraefikConfig([]routableEndpoint{
		routable("alpha", "web", " API.Example.com ", "/v2/", "192.0.2.10", 8080),
		routable("alpha", "web", "api.example.com", "/v2", "192.0.2.11", 8081),
	})
	if len(conflicts) != 0 {
		t.Fatalf("conflicts = %+v, want none (cosmetic differences only)", conflicts)
	}
	_, router := onlyRouter(t, cfg)
	if router.Rule != "Host(`api.example.com`) && PathPrefix(`/v2`)" {
		t.Fatalf("rule = %q, want the normalized host and canonical path",
			router.Rule)
	}
	service, ok := cfg.HTTP.Services[router.Service]
	if !ok {
		t.Fatalf("service %q missing", router.Service)
	}
	if got := len(service.LoadBalancer.Servers); got != 2 {
		t.Fatalf("servers = %d, want both replicas grouped under one route", got)
	}
}

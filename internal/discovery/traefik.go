package discovery

import (
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// providerIDMaxLen caps a generated router/service/middleware identifier. It is
// well below provider limits and keeps generated keys short.
const providerIDMaxLen = 100

// providerIDHashLen is the hex length of the collision-resistant suffix. 16
// hex characters (64 bits) match Relay's short-handle convention for
// content-addressed image tags; the suffix is derived from the full,
// pre-sanitization route identity so sanitized or truncated bases stay distinct.
const providerIDHashLen = 16

// traefikConfig is the dynamic configuration served to Traefik's HTTP provider.
// Maps are encoded with sorted keys by encoding/json, and every list is built in
// deterministic order.
type traefikConfig struct {
	HTTP traefikHTTP `json:"http"`
}

type traefikHTTP struct {
	Routers     map[string]traefikRouter     `json:"routers,omitempty"`
	Services    map[string]traefikService    `json:"services,omitempty"`
	Middlewares map[string]traefikMiddleware `json:"middlewares,omitempty"`
}

type traefikRouter struct {
	Rule        string   `json:"rule"`
	Service     string   `json:"service"`
	Middlewares []string `json:"middlewares,omitempty"`
	Priority    int      `json:"priority"`
}

type traefikService struct {
	LoadBalancer traefikLoadBalancer `json:"loadBalancer"`
}

type traefikLoadBalancer struct {
	Servers []traefikServer `json:"servers"`
}

type traefikServer struct {
	URL string `json:"url"`
}

type traefikMiddleware struct {
	StripPrefix *traefikStripPrefix `json:"stripPrefix,omitempty"`
}

type traefikStripPrefix struct {
	Prefixes []string `json:"prefixes"`
}

type routeIdentity struct {
	app     string
	service string
}

func (i routeIdentity) name() string { return i.app + "/" + i.service }

type routeKey struct {
	host string
	path string
}

type endpointRouteKey struct {
	identity routeIdentity
	route    routeKey
}

// routePlan is one emitted route. route is the canonical conflict/grouping key
// (host-only and "/" both use an empty path there), while path is the declared
// path used for the generated rule and StripPrefix middleware: "" stays
// host-only and "/" stays an explicit root prefix.
type routePlan struct {
	identity routeIdentity
	route    routeKey
	path     string
	urls     map[string]struct{}
}

// routeConflict records one effective host/path claimed by more than one
// logical service identity. Every route at that key is omitted from the output.
type routeConflict struct {
	host       string
	path       string
	identities []routeIdentity
}

func (c routeConflict) key() string { return c.host + "\x00" + c.path }

func (c routeConflict) serviceNames() []string {
	names := make([]string, len(c.identities))
	for i, identity := range c.identities {
		names[i] = identity.name()
	}
	return names
}

func (c routeConflict) same(other routeConflict) bool {
	if c.host != other.host || c.path != other.path ||
		len(c.identities) != len(other.identities) {
		return false
	}
	for i := range c.identities {
		if c.identities[i] != other.identities[i] {
			return false
		}
	}
	return true
}

// buildTraefikConfig turns the eligible endpoint snapshot into a Traefik HTTP
// provider configuration plus the route conflicts it found.
//
// A route's conflict/grouping key is (normalized host, normalized path), where
// an empty path and "/" are equivalent. A route claimed by one logical
// (app, service) identity gets one router and one load-balancer service whose
// servers are every verified replica across nodes, deduplicated by backend URL.
// A route claimed by more than one identity is ambiguous: every route at that
// key is omitted, while distinct paths on the same host remain valid.
//
// The emitted rule and StripPrefix use the DECLARED path, not the conflict key:
// a configured root path "/" keeps PathPrefix(`/`) and StripPrefix(`/`), while
// an omitted/empty path stays host-only with no middleware. Overlapping
// prefixes are resolved by explicit deterministic priorities derived from path
// length (a longer prefix wins; same-length distinct prefixes cannot overlap).
func buildTraefikConfig(
	endpoints []routableEndpoint,
) (traefikConfig, []routeConflict) {
	claims := make(map[routeKey]map[routeIdentity]struct{})
	plans := make(map[endpointRouteKey]*routePlan)

	for _, endpoint := range endpoints {
		host := normalizeRouteHost(endpoint.host)
		if host == "" || endpoint.app == "" || endpoint.service == "" ||
			endpoint.advertiseAddr == "" || endpoint.hostPort < 1 {
			continue
		}
		path := normalizeRoutePath(endpoint.path)
		key := routeKey{host: host, path: routeConflictPath(path)}
		identity := routeIdentity{app: endpoint.app, service: endpoint.service}

		if claims[key] == nil {
			claims[key] = make(map[routeIdentity]struct{})
		}
		claims[key][identity] = struct{}{}

		planKey := endpointRouteKey{identity: identity, route: key}
		plan := plans[planKey]
		if plan == nil {
			plan = &routePlan{
				identity: identity,
				route:    key,
				path:     path,
				urls:     make(map[string]struct{}),
			}
			plans[planKey] = plan
		} else if path == "/" {
			// If nodes disagree between host-only and explicit root for the same
			// identity, keep the explicit root declaration so a configured root
			// route never silently loses its PathPrefix/StripPrefix semantics.
			plan.path = "/"
		}
		plan.urls[backendURL(endpoint.advertiseAddr, endpoint.hostPort)] = struct{}{}
	}

	conflicts := collectConflicts(claims)
	conflicting := make(map[routeKey]struct{}, len(conflicts))
	for _, conflict := range conflicts {
		conflicting[routeKey{host: conflict.host, path: conflict.path}] = struct{}{}
	}

	var active []*routePlan
	for _, plan := range plans {
		if _, ambiguous := conflicting[plan.route]; ambiguous {
			continue
		}
		active = append(active, plan)
	}

	config := traefikConfig{HTTP: traefikHTTP{
		Routers:     make(map[string]traefikRouter),
		Services:    make(map[string]traefikService),
		Middlewares: make(map[string]traefikMiddleware),
	}}

	for _, plan := range active {
		id := routeProviderID(
			plan.identity.app, plan.identity.service, plan.route.host, plan.path)
		router := traefikRouter{
			Rule:     routeRule(plan.route.host, plan.path),
			Service:  id,
			Priority: routePriority(plan.path),
		}
		if plan.path != "" {
			middleware := routeMiddlewareID(
				plan.identity.app, plan.identity.service, plan.route.host, plan.path)
			router.Middlewares = []string{middleware}
			config.HTTP.Middlewares[middleware] = traefikMiddleware{
				StripPrefix: &traefikStripPrefix{Prefixes: []string{plan.path}},
			}
		}
		config.HTTP.Routers[id] = router
		config.HTTP.Services[id] = traefikService{
			LoadBalancer: traefikLoadBalancer{Servers: sortedServers(plan.urls)},
		}
	}
	return config, conflicts
}

// collectConflicts returns every ambiguous route in deterministic order.
func collectConflicts(claims map[routeKey]map[routeIdentity]struct{}) []routeConflict {
	var conflicts []routeConflict
	for key, identities := range claims {
		if len(identities) < 2 {
			continue
		}
		conflicts = append(conflicts, routeConflict{
			host:       key.host,
			path:       key.path,
			identities: sortedIdentities(identities),
		})
	}
	sort.Slice(conflicts, func(i, j int) bool {
		if conflicts[i].host != conflicts[j].host {
			return conflicts[i].host < conflicts[j].host
		}
		return conflicts[i].path < conflicts[j].path
	})
	return conflicts
}

func sortedIdentities(set map[routeIdentity]struct{}) []routeIdentity {
	identities := make([]routeIdentity, 0, len(set))
	for identity := range set {
		identities = append(identities, identity)
	}
	sort.Slice(identities, func(i, j int) bool {
		if identities[i].app != identities[j].app {
			return identities[i].app < identities[j].app
		}
		return identities[i].service < identities[j].service
	})
	return identities
}

func sortedServers(urls map[string]struct{}) []traefikServer {
	sorted := make([]string, 0, len(urls))
	for url := range urls {
		sorted = append(sorted, url)
	}
	sort.Strings(sorted)
	servers := make([]traefikServer, len(sorted))
	for i, url := range sorted {
		servers[i] = traefikServer{URL: url}
	}
	return servers
}

// normalizeRouteHost lowercases a hostname for comparison. Host names are
// case-insensitive, and the normalized value is also what the router rule uses
// so two nodes publishing different casing converge on one deterministic route.
func normalizeRouteHost(host string) string {
	return strings.ToLower(strings.TrimSpace(host))
}

// normalizeRoutePath returns the declared canonical path used for generated
// rules and middleware. Empty stays host-only; "/" is an explicit root prefix
// and is preserved so it keeps PathPrefix(`/`) + StripPrefix(`/`). A trailing
// slash is dropped from an already-canonical non-root path.
func normalizeRoutePath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	if path == "/" {
		return "/"
	}
	return strings.TrimRight(path, "/")
}

// routeConflictPath canonicalizes only the conflict/grouping key so host-only
// and explicit root-path declarations claim the same route.
func routeConflictPath(path string) string {
	if path == "/" {
		return ""
	}
	return path
}

func displayRoutePath(path string) string {
	if path == "" {
		return "/"
	}
	return path
}

func routeRule(host, path string) string {
	rule := "Host(`" + host + "`)"
	if path != "" {
		rule += " && PathPrefix(`" + path + "`)"
	}
	return rule
}

// routePriority gives every router an explicit, deterministic priority. A
// longer path prefix must outrank a shorter one on the same host, so
// PathPrefix(`/api/admin`) wins over PathPrefix(`/api`), which wins over a
// PathPrefix(`/`) root route. Distinct same-length prefixes cannot overlap: a
// proper prefix is strictly shorter, so length alone is a deterministic total
// order for overlapping routes.
func routePriority(path string) int {
	return 1 + len(path)
}

func backendURL(advertiseAddr string, hostPort int) string {
	return "http://" + net.JoinHostPort(advertiseAddr, strconv.Itoa(hostPort))
}

// routeProviderID is the deterministic router and service name for one route:
// `relay-<app>-<service>-<hash>`. The hash covers the full logical identity AND
// the effective host/path, so the same service on two routes gets distinct
// names while a restart on the same route keeps its name.
func routeProviderID(app, service, host, path string) string {
	return routeProviderIDWithInfix(app, service, host, path, "")
}

// routeMiddlewareID is the deterministic StripPrefix middleware name for a path
// route. The "strip" infix keeps it in a namespace distinct from the router and
// service names on the same route.
func routeMiddlewareID(app, service, host, path string) string {
	return routeProviderIDWithInfix(app, service, host, path, "strip")
}

func routeProviderIDWithInfix(app, service, host, path, infix string) string {
	base := strings.Trim("relay-"+safePart(app)+"-"+safePart(service), "-")
	if base == "" {
		base = "relay"
	}
	suffix := "-" + routeHash(app, service, host, path)
	if infix != "" {
		suffix = "-" + infix + suffix
	}
	maxBase := max(0, providerIDMaxLen-len(suffix))
	if len(base) > maxBase {
		base = strings.TrimRight(base[:maxBase], "-")
	}
	return base + suffix
}

// routeHash hashes the FULL, unmodified identity and route with NUL separators,
// so different values can never run together across the join.
func routeHash(app, service, host, path string) string {
	sum := sha256.Sum256([]byte(
		app + "\x00" + service + "\x00" + host + "\x00" + path))
	return hex.EncodeToString(sum[:])[:providerIDHashLen]
}

// safePart lowercases s, replaces every character outside [a-z0-9-] with "-",
// collapses consecutive "-", and trims leading/trailing "-".
func safePart(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	clean := b.String()
	for strings.Contains(clean, "--") {
		clean = strings.ReplaceAll(clean, "--", "-")
	}
	return strings.Trim(clean, "-")
}

// conflictTracker owns the per-process conflict log state. It is safe for
// concurrent requests: a conflict is logged once when it appears or changes,
// and a resolution is logged once when it disappears. A registry read failure
// never calls observe, so an error cannot clear or re-emit the active set.
type conflictTracker struct {
	mu     sync.Mutex
	active map[string]routeConflict
}

func newConflictTracker() *conflictTracker {
	return &conflictTracker{active: make(map[string]routeConflict)}
}

func (t *conflictTracker) observe(conflicts []routeConflict, logger *slog.Logger) {
	if t == nil {
		return
	}

	current := make(map[string]routeConflict, len(conflicts))
	var added, changed []routeConflict

	t.mu.Lock()
	if t.active == nil {
		t.active = make(map[string]routeConflict)
	}
	for _, conflict := range conflicts {
		key := conflict.key()
		current[key] = conflict
		previous, existed := t.active[key]
		switch {
		case !existed:
			added = append(added, conflict)
		case !previous.same(conflict):
			changed = append(changed, conflict)
		}
	}

	var resolved []routeConflict
	for key, previous := range t.active {
		if _, active := current[key]; !active {
			resolved = append(resolved, previous)
		}
	}

	t.active = current
	t.mu.Unlock()

	if logger == nil {
		return
	}
	sortConflicts(added)
	sortConflicts(changed)
	sortConflicts(resolved)
	for _, conflict := range added {
		logger.Error("Discovery: duplicate route conflict",
			append(conflictAttrs(conflict), "state", "new")...)
	}
	for _, conflict := range changed {
		logger.Error("Discovery: duplicate route conflict",
			append(conflictAttrs(conflict), "state", "changed")...)
	}
	for _, conflict := range resolved {
		logger.Info("Discovery: duplicate route conflict resolved",
			"host", conflict.host,
			"path", displayRoutePath(conflict.path),
			"services", conflict.serviceNames(),
		)
	}
}

func sortConflicts(conflicts []routeConflict) {
	sort.Slice(conflicts, func(i, j int) bool {
		if conflicts[i].host != conflicts[j].host {
			return conflicts[i].host < conflicts[j].host
		}
		return conflicts[i].path < conflicts[j].path
	})
}

func conflictAttrs(conflict routeConflict) []any {
	return []any{
		"host", conflict.host,
		"path", displayRoutePath(conflict.path),
		"services", conflict.serviceNames(),
		"reason", "distinct services claim the same effective host and path",
		"action", "all routes for this host and path are omitted from the discovery response",
	}
}

package routing

import (
	"regexp"
	"strings"
	"testing"
)

const (
	enableKey     = "traefik.enable"
	networkKey    = "traefik.docker.network"
	routerPrefix  = "traefik.http.routers."
	servicePrefix = "traefik.http.services."
	ruleSuffix    = ".rule"
	portSuffix    = ".loadbalancer.server.port"
	tlsSuffix     = ".tls"
)

// An unrouted service (empty host) gets NO Traefik labels at all.
func TestTraefikLabelsUnroutedNil(t *testing.T) {
	configs := []TraefikConfig{
		{},
		{Network: "proxy"},
		{Network: "proxy", EntryPoints: "websecure", CertResolver: "letsencrypt", Priority: ptr(100)},
	}
	for _, cfg := range configs {
		labels := TraefikLabels("fn", "app/main.py", "", "", 8000, cfg)
		if labels != nil {
			t.Fatalf("unrouted service labels = %v, want nil", labels)
		}
	}
}

func ptr(i int) *int { return &i }

// A routed service gets exactly the four expected keys with the expected
// values, with router id == service id.
func TestTraefikLabelsRouted(t *testing.T) {
	labels := TraefikLabels("fastapi-service", "app/main.py", "api.example.com", "", 8000, TraefikConfig{Network: "proxy"})
	want := []string{
		enableKey,
		"traefik.http.routers.relay-fastapi-service-app-main-py.rule",
		"traefik.http.services.relay-fastapi-service-app-main-py.loadbalancer.server.port",
		networkKey,
	}
	if len(labels) != 4 {
		t.Fatalf("labels = %v (%d keys), want exactly 4: %v", labels, len(labels), want)
	}
	for _, k := range want {
		if _, ok := labels[k]; !ok {
			t.Fatalf("missing key %q in %v", k, labels)
		}
	}
	if labels[enableKey] != "true" {
		t.Fatalf("enable = %q, want true", labels[enableKey])
	}
	if got := labels["traefik.http.routers.relay-fastapi-service-app-main-py.rule"]; got != "Host(`api.example.com`)" {
		t.Fatalf("rule = %q, want Host(`api.example.com`)", got)
	}
	if got := labels["traefik.http.services.relay-fastapi-service-app-main-py.loadbalancer.server.port"]; got != "8000" {
		t.Fatalf("lb port = %q, want 8000", got)
	}
	if labels[networkKey] != "proxy" {
		t.Fatalf("docker.network = %q, want proxy", labels[networkKey])
	}
	// No optional labels with a network-only config.
	for _, k := range []string{"entrypoints", "tls", "tls.certresolver", "priority"} {
		if v, ok := labels[routerPrefix+"x"+k]; ok {
			t.Fatalf("unexpected optional label %s=%q in network-only config %v", k, v, labels)
		}
	}
	for k := range labels {
		if strings.Contains(k, ".entrypoints") || strings.HasSuffix(k, tlsSuffix) ||
			strings.Contains(k, "certresolver") || strings.Contains(k, ".priority") {
			t.Fatalf("optional label %s present in network-only config: %v", k, labels)
		}
	}
	// Router id == service id (single deterministic id for both).
	routerRuleKey, lbPortKey := "", ""
	for k := range labels {
		if strings.HasPrefix(k, routerPrefix) && strings.HasSuffix(k, ruleSuffix) {
			routerRuleKey = k
		}
		if strings.HasPrefix(k, servicePrefix) && strings.HasSuffix(k, portSuffix) {
			lbPortKey = k
		}
	}
	routerID := strings.TrimSuffix(strings.TrimPrefix(routerRuleKey, routerPrefix), ruleSuffix)
	serviceID := strings.TrimSuffix(strings.TrimPrefix(lbPortKey, servicePrefix), portSuffix)
	if routerID == "" || serviceID == "" {
		t.Fatalf("could not extract router/service ids from %v", labels)
	}
	if routerID != serviceID {
		t.Fatalf("router id %q != service id %q", routerID, serviceID)
	}
}

// Without a network the docker.network label is omitted (three labels total).
func TestTraefikLabelsNoNetworkOmitted(t *testing.T) {
	labels := TraefikLabels("fn", "svc.js", "a.test", "", 80, TraefikConfig{})
	if _, ok := labels[networkKey]; ok {
		t.Fatalf("docker.network label present without a configured network: %v", labels)
	}
	if len(labels) != 3 {
		t.Fatalf("labels = %v, want 3 keys without network", labels)
	}
}

// Each optional config value alone adds exactly its label(s) to the base set.
func TestTraefikLabelsOptionalIndividual(t *testing.T) {
	id := "relay-fn-svc-js"
	t.Run("entrypoint", func(t *testing.T) {
		labels := TraefikLabels("fn", "svc.js", "a.test", "", 80, TraefikConfig{Network: "proxy", EntryPoints: "websecure"})
		if got := labels[routerPrefix+id+".entrypoints"]; got != "websecure" {
			t.Fatalf("entrypoints = %q, want websecure; labels = %v", got, labels)
		}
		if len(labels) != 5 {
			t.Fatalf("labels = %v, want 5 (4 base + entrypoints)", labels)
		}
		if _, ok := labels[routerPrefix+id+tlsSuffix]; ok {
			t.Fatalf("unexpected tls label: %v", labels)
		}
	})
	t.Run("certresolver", func(t *testing.T) {
		labels := TraefikLabels("fn", "svc.js", "a.test", "", 80,
			TraefikConfig{Network: "proxy", CertResolver: "letsencrypt"})
		if got := labels[routerPrefix+id+tlsSuffix]; got != "true" {
			t.Fatalf("tls = %q, want true; labels = %v", got, labels)
		}
		if got := labels[routerPrefix+id+".tls.certresolver"]; got != "letsencrypt" {
			t.Fatalf("tls.certresolver = %q, want letsencrypt; labels = %v", got, labels)
		}
		if len(labels) != 6 {
			t.Fatalf("labels = %v, want 6 (4 base + tls + certresolver)", labels)
		}
		if _, ok := labels[routerPrefix+id+".entrypoints"]; ok {
			t.Fatalf("unexpected entrypoints label: %v", labels)
		}
	})
	t.Run("priority", func(t *testing.T) {
		labels := TraefikLabels("fn", "svc.js", "a.test", "", 80, TraefikConfig{Network: "proxy", Priority: ptr(50)})
		if got := labels[routerPrefix+id+".priority"]; got != "50" {
			t.Fatalf("priority = %q, want 50; labels = %v", got, labels)
		}
		if len(labels) != 5 {
			t.Fatalf("labels = %v, want 5 (4 base + priority)", labels)
		}
	})
}

// A comma-separated entrypoint value is passed through verbatim into the
// label: no trimming, splitting, or reordering (Traefik accepts the raw
// comma-separated string in its label grammar).
func TestTraefikLabelsEntryPointsCommaSeparatedVerbatim(t *testing.T) {
	id := "relay-fn-svc-js"
	labels := TraefikLabels("fn", "svc.js", "a.test", "", 80,
		TraefikConfig{Network: "proxy", EntryPoints: "web,websecure"})
	if got := labels[routerPrefix+id+".entrypoints"]; got != "web,websecure" {
		t.Fatalf("entrypoints = %q, want web,websecure verbatim; labels = %v", got, labels)
	}
	if len(labels) != 5 {
		t.Fatalf("labels = %v, want 5 (4 base + entrypoints)", labels)
	}
}

// All three optional values set together: the full 8-label set on the same id.
func TestTraefikLabelsFullHTTPS(t *testing.T) {
	labels := TraefikLabels("fastapi-service", "app/main.py", "api.example.com", "", 8000, TraefikConfig{
		Network:      "proxy",
		EntryPoints:  "websecure",
		CertResolver: "letsencrypt",
		Priority:     ptr(100),
	})
	id := "relay-fastapi-service-app-main-py"
	want := map[string]string{
		enableKey:                               "true",
		networkKey:                              "proxy",
		routerPrefix + id + ruleSuffix:          "Host(`api.example.com`)",
		servicePrefix + id + portSuffix:         "8000",
		routerPrefix + id + ".entrypoints":      "websecure",
		routerPrefix + id + tlsSuffix:           "true",
		routerPrefix + id + ".tls.certresolver": "letsencrypt",
		routerPrefix + id + ".priority":         "100",
	}
	if len(labels) != len(want) {
		t.Fatalf("labels = %v (%d), want %d keys", labels, len(labels), len(want))
	}
	for k, v := range want {
		if labels[k] != v {
			t.Fatalf("label %q = %q, want %q (all: %v)", k, labels[k], v, labels)
		}
	}
}

// Empty-string optional values are treated exactly as unset (nil priority).
func TestTraefikLabelsEmptyOptionalsUnset(t *testing.T) {
	labels := TraefikLabels("fn", "svc.js", "a.test", "", 80, TraefikConfig{
		Network:      "proxy",
		EntryPoints:  "",
		CertResolver: "",
		Priority:     nil,
	})
	if len(labels) != 4 {
		t.Fatalf("labels = %v, want exactly 4 base keys", labels)
	}
}

// Deterministic: two calls produce identical maps; the fastapi example id is
// relay-fastapi-service-app-main-py over the safe charset.
func TestServiceProviderIDDeterministicAndSafe(t *testing.T) {
	cfg := TraefikConfig{Network: "proxy"}
	a := TraefikLabels("fastapi-service", "app/main.py", "api.example.com", "", 8000, cfg)
	b := TraefikLabels("fastapi-service", "app/main.py", "api.example.com", "", 8000, cfg)
	if len(a) != len(b) {
		t.Fatalf("label counts differ: %v vs %v", a, b)
	}
	for k, v := range a {
		if b[k] != v {
			t.Fatalf("labels differ at %q: %q vs %q", k, v, b[k])
		}
	}

	id := ServiceProviderID("fastapi-service", "app/main.py")
	if id != "relay-fastapi-service-app-main-py" {
		t.Fatalf("id = %q, want relay-fastapi-service-app-main-py", id)
	}
	idPattern := regexp.MustCompile(`^relay-fastapi-service-app-main-py$`)
	if !idPattern.MatchString(id) {
		t.Fatalf("id %q does not match the expected pattern", id)
	}
	if !regexp.MustCompile(`^[a-z0-9-]+$`).MatchString(id) {
		t.Fatalf("id %q contains unsafe characters", id)
	}
	if len(id) > 100 {
		t.Fatalf("id length %d exceeds the 100-char cap", len(id))
	}
}

// TestServiceProviderIDTruncatesOverlong pins the >100-char truncation branch: a
// long function name plus a long entrypoint produces an id capped at exactly 100
// characters, and the truncation still yields a Traefik-safe identifier.
func TestServiceProviderIDTruncatesOverlong(t *testing.T) {
	longName := strings.Repeat("verylongfunction", 5) // 80 chars
	longEntry := strings.Repeat("deep/nested/path", 5) + ".py"
	id := ServiceProviderID(longName, longEntry)
	if len(id) != 100 {
		t.Fatalf("overlong id length = %d, want exactly 100 (truncated)", len(id))
	}
	if !strings.HasPrefix(id, "relay-") {
		t.Fatalf("truncated id %q lost the relay- prefix", id)
	}
	if !regexp.MustCompile(`^[a-z0-9-]+$`).MatchString(id) {
		t.Fatalf("truncated id %q is not Traefik-safe", id)
	}
}

// Odd characters in the function name or entrypoint still produce a safe,
// deterministic id.
func TestServiceProviderIDSanitizes(t *testing.T) {
	for _, tc := range []struct {
		fn, entrypoint string
	}{
		{"Fn.X", "app/Main v2.py"},
		{"UPPER_function", "SVC.js"},
		{"a-b", "x  y/z.js"},
	} {
		id := ServiceProviderID(tc.fn, tc.entrypoint)
		if !regexp.MustCompile(`^[a-z0-9-]+$`).MatchString(id) {
			t.Errorf("id %q (fn=%q ep=%q) is not Traefik-safe", id, tc.fn, tc.entrypoint)
		}
		if !strings.HasPrefix(id, "relay-") {
			t.Errorf("id %q missing the relay- prefix", id)
		}
		if strings.Contains(id, "--") || strings.HasPrefix(id[6:], "-") || id == "relay-" {
			t.Errorf("id %q has leading/trailing/double hyphens", id)
		}
		if a, b := ServiceProviderID(tc.fn, tc.entrypoint), id; a != b {
			t.Errorf("id not deterministic: %q vs %q", a, b)
		}
	}
	// 'Fn.X' + 'app/Main v2.py' → relay-fn-x-app-main-v2-py
	if got := ServiceProviderID("Fn.X", "app/Main v2.py"); got != "relay-fn-x-app-main-v2-py" {
		t.Fatalf("sanitized id = %q, want relay-fn-x-app-main-v2-py", got)
	}
}

// IsTraefikLabel detects Traefik-owned keys and nothing else.
func TestIsTraefikLabel(t *testing.T) {
	for _, tc := range []struct {
		key  string
		want bool
	}{
		{"traefik.enable", true},
		{"traefik.docker.network", true},
		{"traefik.http.routers.x.rule", true},
		{"traefik", false},
		{"traefikk.enable", false},
		{"relay.function", false},
		{"", false},
	} {
		if got := IsTraefikLabel(tc.key); got != tc.want {
			t.Errorf("IsTraefikLabel(%q) = %v, want %v", tc.key, got, tc.want)
		}
	}
}

// Validate's exact error message.
func TestTraefikConfigValidate(t *testing.T) {
	err := (TraefikConfig{}).Validate()
	if err == nil || err.Error() != "TRAEFIK_NETWORK is required when Traefik routing is configured" {
		t.Fatalf("Validate() = %v, want the exact error message", err)
	}
	if err := (TraefikConfig{Network: "proxy"}).Validate(); err != nil {
		t.Fatalf("Validate with network = %v, want nil", err)
	}
}

// A set HostOverride is validated with the same hostname rules as a service
// host: valid hostnames pass, malformed ones fail with an error naming the
// variable, and a missing network still fails first.
func TestTraefikConfigValidateHostOverride(t *testing.T) {
	for _, valid := range []string{"localhost", "a.test", "123.io", "sub.domain.example.com"} {
		if err := (TraefikConfig{Network: "proxy", HostOverride: valid}).Validate(); err != nil {
			t.Errorf("Validate(override=%q) = %v, want nil", valid, err)
		}
	}
	for _, invalid := range []string{"-bad", "bad-", "a..b", "a_b", "a b", "bad/name", "a.", ".a"} {
		err := (TraefikConfig{Network: "proxy", HostOverride: invalid}).Validate()
		if err == nil {
			t.Errorf("Validate(override=%q) = nil, want an error", invalid)
			continue
		}
		if !strings.Contains(err.Error(), "TRAEFIK_HOST_OVERRIDE") {
			t.Errorf("Validate(override=%q) error %v does not name TRAEFIK_HOST_OVERRIDE", invalid, err)
		}
	}
	// Unset is valid and never validated (no override, prior behavior).
	if err := (TraefikConfig{Network: "proxy", HostOverride: ""}).Validate(); err != nil {
		t.Fatalf("Validate with empty override = %v, want nil", err)
	}
	// The network requirement still wins over an invalid override.
	err := (TraefikConfig{HostOverride: "-bad"}).Validate()
	if err == nil || err.Error() != "TRAEFIK_NETWORK is required when Traefik routing is configured" {
		t.Fatalf("Validate with no network = %v, want the network error", err)
	}
}

// ValidateHost validates the EFFECTIVE per-service host: the declared host
// unchanged with no override, or its override mapping with one. The combined
// length is exactly why this is per-service — an override that is itself a
// valid hostname can still derive an overlong host from a long left label, and
// a global conservative cap in Validate would reject valid short-host cases.
func TestTraefikConfigValidateHost(t *testing.T) {
	// A 201-char valid override (four labels, each <= 63): accepted by Validate
	// on its own, but 63-char label + "." + 201 = 265 derived chars.
	longOverride := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." +
		strings.Repeat("c", 63) + "." + strings.Repeat("d", 9)
	if len(longOverride) != 201 {
		t.Fatalf("test override length = %d, want 201", len(longOverride))
	}
	if err := (TraefikConfig{Network: "proxy", HostOverride: longOverride}).Validate(); err != nil {
		t.Fatalf("Validate(long override) = %v, want nil (201 <= 253)", err)
	}

	tests := []struct {
		name     string
		override string
		host     string
		wantErr  bool
	}{
		{"no override valid host", "", "issuer.example.com", false},
		{"no override empty host is unrouted", "", "", false},
		{"short override short host", "localhost", "issuer", false},
		{"override maps long label to short domain", "localhost", strings.Repeat("z", 63) + ".example.com", false},
		{"combined exactly 253", longOverride, strings.Repeat("z", 51), false},
		{"combined one over 253", longOverride, strings.Repeat("z", 52), true},
		{"overlong derived host", longOverride, strings.Repeat("z", 63), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := TraefikConfig{HostOverride: tt.override}
			err := cfg.ValidateHost(tt.host)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ValidateHost(%q) with override %q = nil, want an error", tt.host, tt.override)
				}
				if !strings.Contains(err.Error(), "253-character hostname limit") {
					t.Fatalf("ValidateHost(%q) = %v, want the 253-character error", tt.host, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ValidateHost(%q) with override %q = %v, want nil", tt.host, tt.override, err)
			}
		})
	}
}

// OverrideHost keeps the left-most label and replaces the remaining domain.
func TestTraefikConfigOverrideHost(t *testing.T) {
	tests := []struct {
		name     string
		override string
		host     string
		want     string
	}{
		{"absent override keeps host verbatim", "", "issuer.example.com", "issuer.example.com"},
		{"multi-label domain replaced", "localhost", "issuer.example.com", "issuer.localhost"},
		{"single label gets override domain", "localhost", "issuer", "issuer.localhost"},
		{"deeper subdomain collapses to first label", "localhost", "a.b.c.d", "a.localhost"},
		{"two-part override suffix", "local.test", "issuer.example.com", "issuer.local.test"},
		{"hyphenated left label preserved", "localhost", "my-app.example.com", "my-app.localhost"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := TraefikConfig{HostOverride: tt.override}
			if got := cfg.OverrideHost(tt.host); got != tt.want {
				t.Fatalf("OverrideHost(%q) with override %q = %q, want %q", tt.host, tt.override, got, tt.want)
			}
		})
	}
}

// Distinct subdomains of the same domain stay distinct under an override.
func TestTraefikLabelsOverrideDistinctSubdomains(t *testing.T) {
	cfg := TraefikConfig{Network: "proxy", HostOverride: "localhost"}
	a := TraefikLabels("fn", "a.js", "issuer.example.com", "", 80, cfg)
	b := TraefikLabels("fn", "b.js", "admin.example.com", "", 80, cfg)
	if got := a["traefik.http.routers.relay-fn-a-js.rule"]; got != "Host(`issuer.localhost`)" {
		t.Fatalf("a rule = %q, want Host(`issuer.localhost`)", got)
	}
	if got := b["traefik.http.routers.relay-fn-b-js.rule"]; got != "Host(`admin.localhost`)" {
		t.Fatalf("b rule = %q, want Host(`admin.localhost`)", got)
	}
}

// Under an override, a host+path service keeps the exact PathPrefix and
// StripPrefix labels; only the Host term changes.
func TestTraefikLabelsOverrideHostWithPath(t *testing.T) {
	cfg := TraefikConfig{Network: "proxy", HostOverride: "localhost"}
	labels := TraefikLabels("fn", "svc.js", "issuer.example.com", "/v2", 80, cfg)
	id := "relay-fn-svc-js"
	mw := "relay-fn-svc-js-path"
	want := map[string]string{
		enableKey:                          "true",
		networkKey:                         "proxy",
		routerPrefix + id + ruleSuffix:     "Host(`issuer.localhost`) && PathPrefix(`/v2`)",
		servicePrefix + id + portSuffix:    "80",
		routerPrefix + id + ".middlewares": mw,
		"traefik.http.middlewares." + mw + ".stripprefix.prefixes": "/v2",
	}
	if len(labels) != len(want) {
		t.Fatalf("override+path labels = %v (%d), want %d keys", labels, len(labels), len(want))
	}
	for k, v := range want {
		if labels[k] != v {
			t.Fatalf("label %q = %q, want %q (all: %v)", k, labels[k], v, labels)
		}
	}
}

// An override leaves the network, entrypoints, certresolver, priority labels,
// and the router/service naming exactly as they are without one.
func TestTraefikLabelsOverridePreservesOtherValues(t *testing.T) {
	base := TraefikConfig{Network: "proxy", EntryPoints: "websecure", CertResolver: "letsencrypt", Priority: ptr(100)}
	overridden := base
	overridden.HostOverride = "localhost"
	id := "relay-fn-svc-js"
	withoutOverride := TraefikLabels("fn", "svc.js", "issuer.example.com", "/v2", 80, base)
	withOverride := TraefikLabels("fn", "svc.js", "issuer.example.com", "/v2", 80, overridden)
	for k, v := range withoutOverride {
		if strings.HasSuffix(k, ruleSuffix) {
			continue // the one rule Host term is the intended difference
		}
		if withOverride[k] != v {
			t.Fatalf("override changed label %q: %q -> %q", k, v, withOverride[k])
		}
	}
	if len(withoutOverride) != len(withOverride) {
		t.Fatalf("override changed the label count: %d -> %d", len(withoutOverride), len(withOverride))
	}
	if got := withOverride[routerPrefix+id+ruleSuffix]; got != "Host(`issuer.localhost`) && PathPrefix(`/v2`)" {
		t.Fatalf("override rule = %q", got)
	}
}

// Host-only routing under an override (no path) is the legacy label set with
// only the Host term mapped.
func TestTraefikLabelsOverrideHostOnly(t *testing.T) {
	labels := TraefikLabels("fn", "svc.js", "issuer.example.com", "", 80, TraefikConfig{Network: "proxy", HostOverride: "localhost"})
	id := "relay-fn-svc-js"
	want := map[string]string{
		enableKey:                       "true",
		networkKey:                      "proxy",
		routerPrefix + id + ruleSuffix:  "Host(`issuer.localhost`)",
		servicePrefix + id + portSuffix: "80",
	}
	if len(labels) != len(want) {
		t.Fatalf("labels = %v (%d), want %d", labels, len(labels), len(want))
	}
	for k, v := range want {
		if labels[k] != v {
			t.Fatalf("label %q = %q, want %q (all: %v)", k, labels[k], v, labels)
		}
	}
	for k := range labels {
		if strings.Contains(k, "middlewares") || strings.Contains(k, "stripprefix") {
			t.Fatalf("host-only override must carry no middleware labels: %v", labels)
		}
	}
}

// Absent override is byte-for-byte the pre-override behavior.
func TestTraefikLabelsAbsentOverrideUnchanged(t *testing.T) {
	without := TraefikLabels("fn", "svc.js", "issuer.example.com", "/v2", 80, TraefikConfig{Network: "proxy"})
	explicitEmpty := TraefikLabels("fn", "svc.js", "issuer.example.com", "/v2", 80, TraefikConfig{Network: "proxy", HostOverride: ""})
	if len(without) != len(explicitEmpty) {
		t.Fatalf("label counts differ: %v vs %v", without, explicitEmpty)
	}
	for k, v := range without {
		if explicitEmpty[k] != v {
			t.Fatalf("labels differ at %q: %q vs %q", k, v, explicitEmpty[k])
		}
	}
	if got := without["traefik.http.routers.relay-fn-svc-js.rule"]; got != "Host(`issuer.example.com`) && PathPrefix(`/v2`)" {
		t.Fatalf("absent-override rule = %q, want the declared host verbatim", got)
	}
}

// The declared host string passed in is never mutated by label generation: the
// caller's original template value is preserved (strings are values, and
// TraefikLabels only reads host).
func TestTraefikLabelsDoesNotMutateHost(t *testing.T) {
	original := "issuer.example.com"
	host := original
	_ = TraefikLabels("fn", "svc.js", host, "/v2", 80, TraefikConfig{Network: "proxy", HostOverride: "localhost"})
	if host != original {
		t.Fatalf("host mutated to %q, want %q", host, original)
	}
}

// An unrouted service stays unrouted under an override: still a NIL map.
func TestTraefikLabelsOverrideUnroutedNil(t *testing.T) {
	if labels := TraefikLabels("fn", "svc.js", "", "", 80, TraefikConfig{Network: "proxy", HostOverride: "localhost"}); labels != nil {
		t.Fatalf("unrouted labels under override = %v, want nil", labels)
	}
}

// MissingNetwork's exact message.
func TestMissingNetworkMessage(t *testing.T) {
	err := MissingNetwork("proxy")
	if err == nil || err.Error() != `Traefik network "proxy" does not exist` {
		t.Fatalf("MissingNetwork = %v, want the exact message", err)
	}
}

// Host-only routing (empty path) is byte-for-byte the legacy label set: the rule
// has no PathPrefix and there are no middleware labels at all.
func TestTraefikLabelsHostOnlyNoMiddleware(t *testing.T) {
	labels := TraefikLabels("fn", "svc.js", "a.test", "", 80, TraefikConfig{Network: "proxy"})
	id := "relay-fn-svc-js"
	want := map[string]string{
		enableKey:                       "true",
		networkKey:                      "proxy",
		routerPrefix + id + ruleSuffix:  "Host(`a.test`)",
		servicePrefix + id + portSuffix: "80",
	}
	if len(labels) != len(want) {
		t.Fatalf("host-only labels = %v (%d), want %d legacy keys", labels, len(labels), len(want))
	}
	for k, v := range want {
		if labels[k] != v {
			t.Fatalf("label %q = %q, want %q (all: %v)", k, labels[k], v, labels)
		}
	}
	for k := range labels {
		if strings.Contains(k, "middlewares") || strings.Contains(k, "stripprefix") {
			t.Fatalf("host-only routing must carry no middleware labels: %v", labels)
		}
	}
}

// A configured path yields the exact PathPrefix rule and the StripPrefix
// middleware, with the router referencing that middleware by the deterministic
// name (service id + "-path").
func TestTraefikLabelsPathAddsRuleAndMiddleware(t *testing.T) {
	labels := TraefikLabels("fn", "svc.js", "a.test", "/v2", 80, TraefikConfig{Network: "proxy"})
	id := "relay-fn-svc-js"
	mw := "relay-fn-svc-js-path"
	want := map[string]string{
		enableKey:                          "true",
		networkKey:                         "proxy",
		routerPrefix + id + ruleSuffix:     "Host(`a.test`) && PathPrefix(`/v2`)",
		servicePrefix + id + portSuffix:    "80",
		routerPrefix + id + ".middlewares": mw,
		"traefik.http.middlewares." + mw + ".stripprefix.prefixes": "/v2",
	}
	if len(labels) != len(want) {
		t.Fatalf("path labels = %v (%d), want %d keys", labels, len(labels), len(want))
	}
	for k, v := range want {
		if labels[k] != v {
			t.Fatalf("label %q = %q, want %q (all: %v)", k, labels[k], v, labels)
		}
	}
}

// The root path is a configured path: it gets PathPrefix(`/`) and a StripPrefix
// middleware, distinct from host-only routing.
func TestTraefikLabelsRootPath(t *testing.T) {
	labels := TraefikLabels("fn", "svc.js", "a.test", "/", 80, TraefikConfig{Network: "proxy"})
	id := "relay-fn-svc-js"
	mw := "relay-fn-svc-js-path"
	if got := labels[routerPrefix+id+ruleSuffix]; got != "Host(`a.test`) && PathPrefix(`/`)" {
		t.Fatalf("root rule = %q", got)
	}
	if labels[routerPrefix+id+".middlewares"] != mw {
		t.Fatalf("root middlewares = %q, want %q", labels[routerPrefix+id+".middlewares"], mw)
	}
	if labels["traefik.http.middlewares."+mw+".stripprefix.prefixes"] != "/" {
		t.Fatalf("root stripprefix = %q, want /", labels["traefik.http.middlewares."+mw+".stripprefix.prefixes"])
	}
}

// The middleware name is deterministic across calls and distinct from the
// router/service id.
func TestPathMiddlewareIDDeterministicDistinct(t *testing.T) {
	id := ServiceProviderID("fn", "svc.js")
	mw := PathMiddlewareID("fn", "svc.js")
	if mw != id+"-path" {
		t.Fatalf("middleware id = %q, want %q", mw, id+"-path")
	}
	if mw == id {
		t.Fatal("middleware id must differ from the provider id")
	}
	if a, b := PathMiddlewareID("fn", "svc.js"), mw; a != b {
		t.Fatalf("middleware id not deterministic: %q vs %q", a, b)
	}
	if !regexp.MustCompile(`^[a-z0-9-]+$`).MatchString(mw) {
		t.Fatalf("middleware id %q contains unsafe characters", mw)
	}
}

// Two services on the SAME host but different paths get distinct router and
// middleware names, so they do not collide in Traefik.
func TestTraefikLabelsSameHostDifferentPathsDistinct(t *testing.T) {
	cfg := TraefikConfig{Network: "proxy"}
	a := TraefikLabels("fn", "a.js", "same.test", "/v1", 80, cfg)
	b := TraefikLabels("fn", "b.js", "same.test", "/v2", 80, cfg)
	idA, idB := "relay-fn-a-js", "relay-fn-b-js"
	mwA, mwB := "relay-fn-a-js-path", "relay-fn-b-js-path"
	if !strings.Contains(a[routerPrefix+idA+ruleSuffix], "PathPrefix(`/v1`)") {
		t.Fatalf("service a rule = %q", a[routerPrefix+idA+ruleSuffix])
	}
	if !strings.Contains(b[routerPrefix+idB+ruleSuffix], "PathPrefix(`/v2`)") {
		t.Fatalf("service b rule = %q", b[routerPrefix+idB+ruleSuffix])
	}
	if mwA == mwB {
		t.Fatalf("same-host services share middleware name %q", mwA)
	}
	if a[routerPrefix+idA+".middlewares"] != mwA || b[routerPrefix+idB+".middlewares"] != mwB {
		t.Fatalf("router middleware references mismatch: %v vs %v", a, b)
	}
}

// A path coexists with every optional router value: all labels land on the same
// id, and the middleware stays on its own deterministic name.
func TestTraefikLabelsPathWithFullHTTPS(t *testing.T) {
	labels := TraefikLabels("fn", "svc.js", "a.test", "/v2", 80, TraefikConfig{
		Network:      "proxy",
		EntryPoints:  "websecure",
		CertResolver: "letsencrypt",
		Priority:     ptr(100),
	})
	id := "relay-fn-svc-js"
	want := map[string]string{
		routerPrefix + id + ruleSuffix:                                       "Host(`a.test`) && PathPrefix(`/v2`)",
		routerPrefix + id + ".middlewares":                                   "relay-fn-svc-js-path",
		routerPrefix + id + ".entrypoints":                                   "websecure",
		routerPrefix + id + tlsSuffix:                                        "true",
		routerPrefix + id + ".tls.certresolver":                              "letsencrypt",
		routerPrefix + id + ".priority":                                      "100",
		"traefik.http.middlewares.relay-fn-svc-js-path.stripprefix.prefixes": "/v2",
	}
	for k, v := range want {
		if labels[k] != v {
			t.Fatalf("label %q = %q, want %q (all: %v)", k, labels[k], v, labels)
		}
	}
}

// A very long service id still yields a middleware name that keeps the "-path"
// suffix and stays within the 100-char cap and the safe charset.
func TestPathMiddlewareIDOverlongKeepsSuffix(t *testing.T) {
	longName := strings.Repeat("verylongfunction", 5)
	longEntry := strings.Repeat("deep/nested/path", 5) + ".py"
	mw := PathMiddlewareID(longName, longEntry)
	if len(mw) > 100 {
		t.Fatalf("middleware id length = %d, want <= 100", len(mw))
	}
	if !strings.HasSuffix(mw, "-path") {
		t.Fatalf("middleware id %q lost the -path suffix", mw)
	}
	if !regexp.MustCompile(`^[a-z0-9-]+$`).MatchString(mw) {
		t.Fatalf("middleware id %q is not Traefik-safe", mw)
	}
}

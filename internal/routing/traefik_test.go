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
	id := ServiceProviderID("fastapi-service", "app/main.py")
	want := []string{
		enableKey,
		routerPrefix + id + ruleSuffix,
		servicePrefix + id + portSuffix,
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
	if got := labels[routerPrefix+id+ruleSuffix]; got != "Host(`api.example.com`)" {
		t.Fatalf("rule = %q, want Host(`api.example.com`)", got)
	}
	if got := labels[servicePrefix+id+portSuffix]; got != "8000" {
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
	id := ServiceProviderID("fn", "svc.js")
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
	id := ServiceProviderID("fn", "svc.js")
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
	id := ServiceProviderID("fastapi-service", "app/main.py")
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

// Deterministic: two calls produce identical maps; the fastapi example id keeps
// its readable base over the safe charset and ends in the collision-resistant
// hash suffix.
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
	if !strings.HasPrefix(id, "relay-fastapi-service-app-main-py-") {
		t.Fatalf("id = %q, want the relay-fastapi-service-app-main-py base", id)
	}
	if !strings.HasSuffix(id, identityHash("fastapi-service", "app/main.py")) {
		t.Fatalf("id = %q, want the identity hash suffix", id)
	}
	if !regexp.MustCompile(`^[a-z0-9-]+$`).MatchString(id) {
		t.Fatalf("id %q contains unsafe characters", id)
	}
	if len(id) > serviceProviderIDMaxLen {
		t.Fatalf("id length %d exceeds the %d-char cap", len(id), serviceProviderIDMaxLen)
	}
}

// TestServiceProviderIDCollisionResistant pins the core reviewer finding:
// distinct valid service identities that sanitize to the same readable base must
// still get distinct ids. "ghcr.io/acme/a/b:1" and "ghcr.io/acme/a-b:1" both
// sanitize to "ghcr-io-acme-a-b-1".
func TestServiceProviderIDCollisionResistantForSanitizedIdentities(t *testing.T) {
	a := ServiceProviderID("fn", "ghcr.io/acme/a/b:1")
	b := ServiceProviderID("fn", "ghcr.io/acme/a-b:1")
	if a == b {
		t.Fatalf("distinct identities collided: %q", a)
	}
	if !strings.HasPrefix(a, "relay-fn-ghcr-io-acme-a-b-1-") {
		t.Fatalf("a = %q, want the shared readable base", a)
	}
	// The two ids share the readable prefix but differ in the hash suffix.
	if strings.TrimPrefix(a, "relay-fn-ghcr-io-acme-a-b-1-") == strings.TrimPrefix(b, "relay-fn-ghcr-io-acme-a-b-1-") {
		t.Fatalf("hash suffixes must differ: %q vs %q", a, b)
	}
}

// TestServiceProviderIDCollisionResistantWhenTruncated pins the second half of
// the reviewer finding: two long identities whose readable bases are identical
// up to the length cap still get distinct ids because the suffix is derived from
// the full, untruncated identity and preserved at the cap.
func TestServiceProviderIDCollisionResistantWhenTruncated(t *testing.T) {
	prefix := strings.Repeat("deep/nested/path/", 15) // ~255 chars, well over the cap
	a := ServiceProviderID("fn", prefix+"one.js")
	b := ServiceProviderID("fn", prefix+"two.js")
	if a == b {
		t.Fatalf("truncated identities collided: %q", a)
	}
	if len(a) > serviceProviderIDMaxLen || len(b) > serviceProviderIDMaxLen {
		t.Fatalf("ids exceed the %d-char cap: %d / %d", serviceProviderIDMaxLen, len(a), len(b))
	}
	// Even though the readable bases coincide under the cap, the hash suffix
	// (derived from the full identity) distinguishes them.
	if !strings.HasSuffix(a, identityHash("fn", prefix+"one.js")) ||
		!strings.HasSuffix(b, identityHash("fn", prefix+"two.js")) {
		t.Fatalf("truncated ids lost the full-identity hash suffix: %q / %q", a, b)
	}
}

// TestPathMiddlewareIDCollisionResistant mirrors the provider-id collision
// guarantee for the middleware namespace: colliding readable bases still yield
// distinct middleware names, and the "-path" infix is preserved.
func TestPathMiddlewareIDCollisionResistant(t *testing.T) {
	a := PathMiddlewareID("fn", "ghcr.io/acme/a/b:1")
	b := PathMiddlewareID("fn", "ghcr.io/acme/a-b:1")
	if a == b {
		t.Fatalf("distinct identities collided for the middleware: %q", a)
	}
	if !strings.Contains(a, "-path-") {
		t.Fatalf("middleware id %q lost the -path infix", a)
	}
	if !strings.HasSuffix(a, identityHash("fn", "ghcr.io/acme/a/b:1")) ||
		!strings.HasSuffix(b, identityHash("fn", "ghcr.io/acme/a-b:1")) {
		t.Fatalf("middleware ids lost the full-identity hash suffix: %q / %q", a, b)
	}
	if !regexp.MustCompile(`^[a-z0-9-]+$`).MatchString(a) {
		t.Fatalf("middleware id %q contains unsafe characters", a)
	}
}

// TestServiceProviderIDTruncatesOverlong pins the truncation branch: a long
// app name plus a long identity produces an id capped at exactly the max
// length, with the hash suffix still present and the charset safe.
func TestServiceProviderIDTruncatesOverlong(t *testing.T) {
	longName := strings.Repeat("verylongfunction", 5) // 80 chars
	longEntry := strings.Repeat("deep/nested/path", 5) + ".py"
	id := ServiceProviderID(longName, longEntry)
	if len(id) > serviceProviderIDMaxLen {
		t.Fatalf("overlong id length = %d, want <= %d", len(id), serviceProviderIDMaxLen)
	}
	if !strings.HasPrefix(id, "relay-") {
		t.Fatalf("truncated id %q lost the relay- prefix", id)
	}
	if !strings.HasSuffix(id, identityHash(longName, longEntry)) {
		t.Fatalf("truncated id %q lost the hash suffix", id)
	}
	if !regexp.MustCompile(`^[a-z0-9-]+$`).MatchString(id) {
		t.Fatalf("truncated id %q is not Traefik-safe", id)
	}
}

// Odd characters in the app name or identity still produce a safe,
// deterministic id whose hash suffix reflects the full inputs.
func TestServiceProviderIDSanitizes(t *testing.T) {
	for _, tc := range []struct {
		fn, identity string
	}{
		{"Fn.X", "app/Main v2.py"},
		{"UPPER_function", "SVC.js"},
		{"a-b", "x  y/z.js"},
	} {
		id := ServiceProviderID(tc.fn, tc.identity)
		if !regexp.MustCompile(`^[a-z0-9-]+$`).MatchString(id) {
			t.Errorf("id %q (fn=%q id=%q) is not Traefik-safe", id, tc.fn, tc.identity)
		}
		if !strings.HasPrefix(id, "relay-") {
			t.Errorf("id %q missing the relay- prefix", id)
		}
		if strings.Contains(id, "--") || strings.HasPrefix(id[6:], "-") || id == "relay-" {
			t.Errorf("id %q has leading/trailing/double hyphens", id)
		}
		if !strings.HasSuffix(id, identityHash(tc.fn, tc.identity)) {
			t.Errorf("id %q missing the identity hash suffix", id)
		}
		if a, b := ServiceProviderID(tc.fn, tc.identity), id; a != b {
			t.Errorf("id not deterministic: %q vs %q", a, b)
		}
	}
	// 'Fn.X' + 'app/Main v2.py' → relay-fn-x-app-main-v2-py-<hash>
	if got := ServiceProviderID("Fn.X", "app/Main v2.py"); !strings.HasPrefix(got, "relay-fn-x-app-main-v2-py-") {
		t.Fatalf("sanitized id = %q, want the relay-fn-x-app-main-v2-py base", got)
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
		{"relay.app", false},
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

// ValidateHostOverride accepts valid hostnames, rejects malformed ones with an
// error naming ROUTING_HOST_OVERRIDE, and treats unset/whitespace-only as "no
// override" (valid by omission). The value is used VERBATIM as the effective
// route host, so the full hostname contract applies to the value itself.
func TestValidateHostOverride(t *testing.T) {
	for _, valid := range []string{"localhost", "a.test", "123.io", "sub.domain.example.com"} {
		if err := ValidateHostOverride(valid); err != nil {
			t.Errorf("ValidateHostOverride(%q) = %v, want nil", valid, err)
		}
	}
	for _, invalid := range []string{"-bad", "bad-", "a..b", "a_b", "a b", "bad/name", "a.", ".a"} {
		err := ValidateHostOverride(invalid)
		if err == nil {
			t.Errorf("ValidateHostOverride(%q) = nil, want an error", invalid)
			continue
		}
		if !strings.Contains(err.Error(), "ROUTING_HOST_OVERRIDE") {
			t.Errorf("ValidateHostOverride(%q) error %v does not name ROUTING_HOST_OVERRIDE",
				invalid, err)
		}
	}
	// Unset and whitespace-only both mean "no override" and are valid.
	if err := ValidateHostOverride(""); err != nil {
		t.Fatalf("ValidateHostOverride(\"\") = %v, want nil", err)
	}
	if err := ValidateHostOverride("   \t"); err != nil {
		t.Fatalf("ValidateHostOverride(whitespace) = %v, want nil", err)
	}
	// The override itself must fit the 253-character hostname limit.
	err := ValidateHostOverride(strings.Repeat("a", 254))
	if err == nil {
		t.Fatal("ValidateHostOverride(254 chars) = nil, want the length error")
	}
	if !strings.Contains(err.Error(), "253-character hostname limit") ||
		!strings.Contains(err.Error(), "ROUTING_HOST_OVERRIDE") {
		t.Fatalf("long-override error = %v, want the 253-character error naming "+
			"ROUTING_HOST_OVERRIDE", err)
	}
}

// HostOverrideConfigured: unset and whitespace-only mean no override.
func TestHostOverrideConfigured(t *testing.T) {
	for _, unset := range []string{"", "   ", "\t\n "} {
		if HostOverrideConfigured(unset) {
			t.Errorf("HostOverrideConfigured(%q) = true, want false", unset)
		}
	}
	for _, set := range []string{"localhost", "x", " portal.test "} {
		if !HostOverrideConfigured(set) {
			t.Errorf("HostOverrideConfigured(%q) = false, want true", set)
		}
	}
}

// EffectiveHost is an EXACT full-host replacement: the declared host verbatim
// when the override is unset or whitespace-only, otherwise the override value
// verbatim — never a suffix mapping or a left-label join.
func TestEffectiveHost(t *testing.T) {
	for _, tt := range []struct {
		name     string
		host     string
		override string
		want     string
	}{
		{"unset keeps the declared host", "issuer.example.com", "",
			"issuer.example.com"},
		{"whitespace-only keeps the declared host", "issuer.example.com", "   ",
			"issuer.example.com"},
		{"exact replacement is never a suffix mapping",
			"issuer.example.com", "localhost", "localhost"},
		{"multi-label override replaces wholesale",
			"issuer.example.com", "portal.internal.test", "portal.internal.test"},
		{"single-label declared host replaced", "issuer", "local.test", "local.test"},
		{"declared host is passed by value, never mutated",
			"my-app.example.com", "my-app.example.com", "my-app.example.com"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := EffectiveHost(tt.host, tt.override); got != tt.want {
				t.Fatalf("EffectiveHost(%q, %q) = %q, want %q",
					tt.host, tt.override, got, tt.want)
			}
		})
	}
}

// The caller supplies the EFFECTIVE host explicitly (see EffectiveHost): every
// routed service's rule names that exact hostname while the router/service ids
// stay keyed on app+service name, so distinct services stay distinct.
func TestTraefikLabelsExplicitEffectiveHost(t *testing.T) {
	cfg := TraefikConfig{Network: "proxy"}
	effective := EffectiveHost("issuer.example.com", "portal.internal.test")
	if effective != "portal.internal.test" {
		t.Fatalf("fixture: EffectiveHost = %q, want the exact override", effective)
	}
	a := TraefikLabels("fn", "a.js", effective, "", 80, cfg)
	b := TraefikLabels("fn", "b.js", effective, "", 80, cfg)
	if got := a[routerPrefix+ServiceProviderID("fn", "a.js")+ruleSuffix]; got != "Host(`portal.internal.test`)" {
		t.Fatalf("a rule = %q, want Host(`portal.internal.test`)", got)
	}
	if got := b[routerPrefix+ServiceProviderID("fn", "b.js")+ruleSuffix]; got != "Host(`portal.internal.test`)" {
		t.Fatalf("b rule = %q, want Host(`portal.internal.test`)", got)
	}
	idA := ServiceProviderID("fn", "a.js")
	idB := ServiceProviderID("fn", "b.js")
	if idA == idB {
		t.Fatalf("distinct services share the router id %q", idA)
	}
	if _, ok := a[routerPrefix+idA+ruleSuffix]; !ok {
		t.Fatalf("a's rule key missing: %v", a)
	}
	if _, ok := b[routerPrefix+idB+ruleSuffix]; !ok {
		t.Fatalf("b's rule key missing: %v", b)
	}
}

// Under an effective override host, a host+path service keeps the exact
// PathPrefix and StripPrefix labels; only the Host term differs.
func TestTraefikLabelsEffectiveHostWithPath(t *testing.T) {
	cfg := TraefikConfig{Network: "proxy"}
	effective := EffectiveHost("issuer.example.com", "localhost")
	labels := TraefikLabels("fn", "svc.js", effective, "/v2", 80, cfg)
	id := ServiceProviderID("fn", "svc.js")
	mw := PathMiddlewareID("fn", "svc.js")
	want := map[string]string{
		enableKey:                          "true",
		networkKey:                         "proxy",
		routerPrefix + id + ruleSuffix:     "Host(`localhost`) && PathPrefix(`/v2`)",
		servicePrefix + id + portSuffix:    "80",
		routerPrefix + id + ".middlewares": mw,
		"traefik.http.middlewares." + mw + ".stripprefix.prefixes": "/v2",
	}
	if len(labels) != len(want) {
		t.Fatalf("effective-host+path labels = %v (%d), want %d keys", labels, len(labels), len(want))
	}
	for k, v := range want {
		if labels[k] != v {
			t.Fatalf("label %q = %q, want %q (all: %v)", k, labels[k], v, labels)
		}
	}
}

// An effective-host change touches ONLY the rule's Host term: network,
// entrypoints, certresolver, priority, ids, port, and middleware labels are
// byte-for-byte identical.
func TestTraefikLabelsEffectiveHostPreservesOtherValues(t *testing.T) {
	base := TraefikConfig{Network: "proxy", EntryPoints: "websecure", CertResolver: "letsencrypt", Priority: ptr(100)}
	id := ServiceProviderID("fn", "svc.js")
	declared := TraefikLabels("fn", "svc.js", "issuer.example.com", "/v2", 80, base)
	overridden := TraefikLabels("fn", "svc.js",
		EffectiveHost("issuer.example.com", "localhost"), "/v2", 80, base)
	for k, v := range declared {
		if strings.HasSuffix(k, ruleSuffix) {
			continue // the one rule Host term is the intended difference
		}
		if overridden[k] != v {
			t.Fatalf("effective host changed label %q: %q -> %q", k, v, overridden[k])
		}
	}
	if len(declared) != len(overridden) {
		t.Fatalf("effective host changed the label count: %d -> %d",
			len(declared), len(overridden))
	}
	if got := overridden[routerPrefix+id+ruleSuffix]; got != "Host(`localhost`) && PathPrefix(`/v2`)" {
		t.Fatalf("effective-host rule = %q", got)
	}
}

// Host-only routing under an effective override host (no path) is the legacy
// label set with only the Host term changed.
func TestTraefikLabelsEffectiveHostOnly(t *testing.T) {
	labels := TraefikLabels("fn", "svc.js", "localhost", "", 80, TraefikConfig{Network: "proxy"})
	id := ServiceProviderID("fn", "svc.js")
	want := map[string]string{
		enableKey:                       "true",
		networkKey:                      "proxy",
		routerPrefix + id + ruleSuffix:  "Host(`localhost`)",
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
			t.Fatalf("host-only labels must carry no middleware keys: %v", labels)
		}
	}
}

// Without an override EffectiveHost is the identity function, so routing
// through it produces byte-for-byte the plain declared-host label set, and the
// rule keeps the declared host verbatim.
func TestTraefikLabelsAbsentOverrideUnchanged(t *testing.T) {
	cfg := TraefikConfig{Network: "proxy"}
	declared := "issuer.example.com"
	plain := TraefikLabels("fn", "svc.js", declared, "/v2", 80, cfg)
	viaHelper := TraefikLabels("fn", "svc.js",
		EffectiveHost(declared, ""), "/v2", 80, cfg)
	if len(plain) != len(viaHelper) {
		t.Fatalf("label counts differ: %v vs %v", plain, viaHelper)
	}
	for k, v := range plain {
		if viaHelper[k] != v {
			t.Fatalf("labels differ at %q: %q vs %q", k, v, viaHelper[k])
		}
	}
	if got := plain[routerPrefix+ServiceProviderID("fn", "svc.js")+ruleSuffix]; got != "Host(`issuer.example.com`) && PathPrefix(`/v2`)" {
		t.Fatalf("absent-override rule = %q, want the declared host verbatim", got)
	}
}

// The host string passed in is never mutated by label generation: the
// caller's original value is preserved (strings are values, and TraefikLabels
// only reads host).
func TestTraefikLabelsDoesNotMutateHost(t *testing.T) {
	original := "issuer.example.com"
	host := original
	_ = TraefikLabels("fn", "svc.js", host, "/v2", 80, TraefikConfig{Network: "proxy"})
	if host != original {
		t.Fatalf("host mutated to %q, want %q", host, original)
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
	id := ServiceProviderID("fn", "svc.js")
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
	id := ServiceProviderID("fn", "svc.js")
	mw := PathMiddlewareID("fn", "svc.js")
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
	id := ServiceProviderID("fn", "svc.js")
	mw := PathMiddlewareID("fn", "svc.js")
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
// router/service id, carrying the -path infix and the same identity hash.
func TestPathMiddlewareIDDeterministicDistinct(t *testing.T) {
	id := ServiceProviderID("fn", "svc.js")
	mw := PathMiddlewareID("fn", "svc.js")
	if mw == id {
		t.Fatal("middleware id must differ from the provider id")
	}
	if !strings.Contains(mw, "-path-") {
		t.Fatalf("middleware id = %q, want the -path infix", mw)
	}
	if !strings.HasSuffix(mw, identityHash("fn", "svc.js")) {
		t.Fatalf("middleware id = %q, want the identity hash suffix", mw)
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
	idA, idB := ServiceProviderID("fn", "a.js"), ServiceProviderID("fn", "b.js")
	mwA, mwB := PathMiddlewareID("fn", "a.js"), PathMiddlewareID("fn", "b.js")
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
	id := ServiceProviderID("fn", "svc.js")
	mw := PathMiddlewareID("fn", "svc.js")
	want := map[string]string{
		routerPrefix + id + ruleSuffix:                             "Host(`a.test`) && PathPrefix(`/v2`)",
		routerPrefix + id + ".middlewares":                         mw,
		routerPrefix + id + ".entrypoints":                         "websecure",
		routerPrefix + id + tlsSuffix:                              "true",
		routerPrefix + id + ".tls.certresolver":                    "letsencrypt",
		routerPrefix + id + ".priority":                            "100",
		"traefik.http.middlewares." + mw + ".stripprefix.prefixes": "/v2",
	}
	for k, v := range want {
		if labels[k] != v {
			t.Fatalf("label %q = %q, want %q (all: %v)", k, labels[k], v, labels)
		}
	}
}

// A very long service identity still yields a middleware name that keeps the
// "-path" infix and the identity hash, and stays within the cap and safe
// charset.
func TestPathMiddlewareIDOverlongKeepsSuffix(t *testing.T) {
	longName := strings.Repeat("verylongfunction", 5)
	longEntry := strings.Repeat("deep/nested/path", 5) + ".py"
	mw := PathMiddlewareID(longName, longEntry)
	if len(mw) > serviceProviderIDMaxLen {
		t.Fatalf("middleware id length = %d, want <= %d", len(mw), serviceProviderIDMaxLen)
	}
	if !strings.Contains(mw, "-path-") {
		t.Fatalf("middleware id %q lost the -path infix", mw)
	}
	if !strings.HasSuffix(mw, identityHash(longName, longEntry)) {
		t.Fatalf("middleware id %q lost the identity hash suffix", mw)
	}
	if !regexp.MustCompile(`^[a-z0-9-]+$`).MatchString(mw) {
		t.Fatalf("middleware id %q is not Traefik-safe", mw)
	}
}

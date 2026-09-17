package routing

import (
	"regexp"
	"strings"
	"testing"
)

// traefikKeyPrefixes used in tests.
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
	for _, cfg := range []TraefikConfig{{}, {Network: "proxy"}, {Network: "proxy", EntryPoints: "websecure", CertResolver: "letsencrypt", Priority: ptr(100)}} {
		labels := TraefikLabels("fn", "app/main.py", "", 8000, cfg)
		if labels != nil {
			t.Fatalf("unrouted service labels = %v, want nil", labels)
		}
	}
}

func ptr(i int) *int { return &i }

// A routed service gets exactly the four expected keys with the expected
// values, with router id == service id.
func TestTraefikLabelsRouted(t *testing.T) {
	labels := TraefikLabels("fastapi-service", "app/main.py", "api.example.com", 8000, TraefikConfig{Network: "proxy"})
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
	labels := TraefikLabels("fn", "svc.js", "a.test", 80, TraefikConfig{})
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
		labels := TraefikLabels("fn", "svc.js", "a.test", 80, TraefikConfig{Network: "proxy", EntryPoints: "websecure"})
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
		labels := TraefikLabels("fn", "svc.js", "a.test", 80, TraefikConfig{Network: "proxy", CertResolver: "letsencrypt"})
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
		labels := TraefikLabels("fn", "svc.js", "a.test", 80, TraefikConfig{Network: "proxy", Priority: ptr(50)})
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
	labels := TraefikLabels("fn", "svc.js", "a.test", 80, TraefikConfig{Network: "proxy", EntryPoints: "web,websecure"})
	if got := labels[routerPrefix+id+".entrypoints"]; got != "web,websecure" {
		t.Fatalf("entrypoints = %q, want web,websecure verbatim; labels = %v", got, labels)
	}
	if len(labels) != 5 {
		t.Fatalf("labels = %v, want 5 (4 base + entrypoints)", labels)
	}
}

// All three optional values set together: the full 8-label set on the same id.
func TestTraefikLabelsFullHTTPS(t *testing.T) {
	labels := TraefikLabels("fastapi-service", "app/main.py", "api.example.com", 8000, TraefikConfig{
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
	labels := TraefikLabels("fn", "svc.js", "a.test", 80, TraefikConfig{
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
	a := TraefikLabels("fastapi-service", "app/main.py", "api.example.com", 8000, cfg)
	b := TraefikLabels("fastapi-service", "app/main.py", "api.example.com", 8000, cfg)
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

// MissingNetwork's exact message.
func TestMissingNetworkMessage(t *testing.T) {
	err := MissingNetwork("proxy")
	if err == nil || err.Error() != `Traefik network "proxy" does not exist` {
		t.Fatalf("MissingNetwork = %v, want the exact message", err)
	}
}

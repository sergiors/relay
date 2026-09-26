package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/moby/moby/api/types/container"

	"relay/internal/testutil"
)

func TestServiceLabelsCarriesServiceIdentityAndOwnership(t *testing.T) {
	got := serviceLabels(ServiceSpec{
		Function: "user-events",
		Identity: "service.js",
		Port:     3000,
		Image:    "relay-fn-user-events:632aca75fa306911",
	}, "worker-1", 2)

	want := map[string]string{
		labelType:     ContainerTypeService,
		labelFunction: "user-events",
		labelIdentity: "service.js",
		labelImage:    "relay-fn-user-events:632aca75fa306911",
		labelHostname: "worker-1",
		labelPort:     "3000",
		labelReplica:  "2",
		// No spec.Env -> the hash of an empty env slice (a container created
		// with no effective env still carries its content hash so discovery can
		// compare it).
		labelEnvHash: EnvHash(nil),
	}
	if len(got) != len(want) {
		t.Fatalf("label count = %d, want %d (%v)", len(got), len(want), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("label %q = %q, want %q", k, got[k], v)
		}
	}
	for k := range got {
		if k == labelType && got[k] != ContainerTypeService {
			t.Errorf("relay.type must be %q, got %q", ContainerTypeService, got[k])
		}
	}
	// Service containers carry relay.identity and NO relay.handler and NO
	// relay.service label.
	if _, ok := got[labelHandler]; ok {
		t.Errorf("service labels must NOT carry relay.handler, got %v", got)
	}
	if _, ok := got["relay.service"]; ok {
		t.Errorf("service labels must NOT carry relay.service, got %v", got)
	}
}

// TestServiceLabelsEnvHashPinsEffectiveEnv: the relay.env_hash label is the
// content hash of the EXACT effective env slice (so discovery can tell an
// env/secret change happened), never a raw value; distinct env produces a
// distinct hash, and the same env reproduces it. It also pins order sensitivity:
// the same entries in a different order are a different effective env.
func TestServiceLabelsEnvHashPinsEffectiveEnv(t *testing.T) {
	spec := func(env []string) ServiceSpec {
		return ServiceSpec{Function: "fn", Identity: "svc.js", Port: 80, Image: "img", Env: env}
	}
	base := serviceLabels(spec([]string{"A=1", "B=2"}), "h", 0)
	if base[labelEnvHash] != EnvHash([]string{"A=1", "B=2"}) {
		t.Errorf("env hash = %q, want the hash of the effective env", base[labelEnvHash])
	}
	if base[labelEnvHash] == EnvHash([]string{"A=1", "B=3"}) {
		t.Error("a changed env value must change the env hash")
	}
	if base[labelEnvHash] == EnvHash([]string{"B=2", "A=1"}) {
		t.Error("a different env order must change the env hash (order-sensitive)")
	}
	if serviceLabels(spec([]string{"A=1", "B=2"}), "h", 0)[labelEnvHash] != base[labelEnvHash] {
		t.Error("the same env must reproduce the same hash")
	}
}

// TestEnvHashEmptyIsStableAndValueFree pins the two properties the reconciler
// relies on: the empty env hashes to one fixed value, and the hash is a fixed
// 16-hex-char digest (never the env itself).
func TestEnvHashEmptyIsStableAndValueFree(t *testing.T) {
	if EnvHash(nil) != EnvHash([]string{}) {
		t.Error("nil and empty env must hash identically")
	}
	got := EnvHash([]string{"TOKEN=super-secret-value"})
	if len(got) != serviceIdentityHashLen {
		t.Fatalf("env hash length = %d, want %d", len(got), serviceIdentityHashLen)
	}
	if strings.Contains(got, "super-secret-value") || strings.Contains(got, "TOKEN") {
		t.Errorf("env hash %q leaks env content", got)
	}
}

func TestServiceContainerNameSanitizesAndCaps(t *testing.T) {
	for _, tc := range []struct {
		name       string
		function   string
		entrypoint string
		replica    int
		wantPrefix string
	}{
		{"plain", "user-events", "service.js", 0, "relay-svc-user-events-service.js-"},
		{"nested", "fn", "app/service.js", 0, "relay-svc-fn-app-service.js-"},
		{"sanitize entrypoint", "fn", "my service@v1", 1, "relay-svc-fn-my-service-v1-"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := serviceContainerName(tc.function, tc.entrypoint, tc.replica)
			if len(got) > serviceContainerNameLenCap {
				t.Fatalf("name length %d exceeds cap %d", len(got), serviceContainerNameLenCap)
			}
			if !strings.HasPrefix(got, tc.wantPrefix) {
				t.Errorf("name = %q, want prefix %q", got, tc.wantPrefix)
			}
			if !strings.HasSuffix(got, "-"+strconv.Itoa(tc.replica)) {
				t.Errorf("name = %q, want the -%d replica suffix", got, tc.replica)
			}
			if !strings.Contains(got, serviceIdentityHash(tc.function, tc.entrypoint)) {
				t.Errorf("name = %q, want the identity hash %q", got, serviceIdentityHash(tc.function, tc.entrypoint))
			}
		})
	}
	// A long identity still yields a name within the cap, keeps the replica
	// suffix, and retains the identity hash.
	got := serviceContainerName("averylongfunctionname", strings.Repeat("x", 200), 99)
	if len(got) > serviceContainerNameLenCap {
		t.Fatalf("long name length %d exceeds cap %d", len(got), serviceContainerNameLenCap)
	}
	if !strings.HasSuffix(got, "-99") {
		t.Fatalf("long name = %q, want the -99 replica suffix preserved", got)
	}
	if !strings.Contains(got, serviceIdentityHash("averylongfunctionname", strings.Repeat("x", 200))) {
		t.Fatalf("long name = %q lost the identity hash", got)
	}
}

// TestServiceContainerNameCollisionResistant pins the reviewer finding for the
// generated container name: distinct identities that sanitize to the same
// readable base (or that truncate to the same prefix) must still get distinct
// names.
func TestServiceContainerNameCollisionResistant(t *testing.T) {
	a := serviceContainerName("fn", "ghcr.io/acme/a/b:1", 0)
	b := serviceContainerName("fn", "ghcr.io/acme/a-b:1", 0)
	if a == b {
		t.Fatalf("distinct identities collided in the container name: %q", a)
	}
	if !strings.Contains(a, serviceIdentityHash("fn", "ghcr.io/acme/a/b:1")) ||
		!strings.Contains(b, serviceIdentityHash("fn", "ghcr.io/acme/a-b:1")) {
		t.Fatalf("container names lack the full-identity hash: %q / %q", a, b)
	}
	// Long identities truncated to the same readable prefix stay distinct.
	long := strings.Repeat("deep/nested/path/", 15)
	x := serviceContainerName("fn", long+"one.js", 0)
	y := serviceContainerName("fn", long+"two.js", 0)
	if x == y {
		t.Fatalf("truncated identities collided in the container name: %q", x)
	}
	if len(x) > serviceContainerNameLenCap || len(y) > serviceContainerNameLenCap {
		t.Fatalf("names exceed the cap: %d / %d", len(x), len(y))
	}
}

func TestServiceContainerNameDeterministic(t *testing.T) {
	a := serviceContainerName("fn", "service.js", 3)
	b := serviceContainerName("fn", "service.js", 3)
	if a != b {
		t.Fatalf("name not deterministic: %q vs %q", a, b)
	}
}

func TestValidateServiceEntrypoint(t *testing.T) {
	for _, tc := range []struct {
		entrypoint string
		wantErr    string
	}{
		{"service.js", ""},
		{"api.js", ""},
		{"app/service.js", ""},
		{"app/main.py", ""},
		{"a/b/c.js", ""},
		{"", "empty"},
		{"a b.js", "whitespace"},
		{"app service.js", "whitespace"},
		{"/etc/passwd", "relative path"},
		{"../escape.js", "must not contain"},
		{"..hidden.js", "path elements must not"},
		{".hidden.js", "path elements must not"},
		{"a//b.js", "empty path elements"},
		{"a/./b.js", "path elements must not"},
		{"back\\slash.js", "backslashes"},
		{"a/..hidden/b.js", "path elements must not"},
	} {
		t.Run(fmt.Sprintf("%q", tc.entrypoint), func(t *testing.T) {
			err := validateServiceEntrypoint(tc.entrypoint)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != "" && err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not mention %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestServiceEntryTranslatesPerRuntime(t *testing.T) {
	for _, tc := range []struct {
		runtime    string
		entrypoint string
		want       []string
		wantErr    string // substring; empty means success
	}{
		{"node24", "service.js", []string{"node", "/app/service.js"}, ""},
		{"python3.14", "service.py", []string{"python", "-m", "service"}, ""},
		{"node24", "app/service.js", []string{"node", "/app/app/service.js"}, ""},
		{"python3.14", "app/main.py", []string{"python", "-m", "app.main"}, ""},
		{"python3.14", "app/http/server.py", []string{"python", "-m", "app.http.server"}, ""},
		{"node24", "a/b/c.js", []string{"node", "/app/a/b/c.js"}, ""},
		// Python entrypoints that are valid files but not importable modules fail
		// with a python-specific error (the conversion runs after validation).
		{"python3.14", "app/main.js", nil, "require a .py module file"},
		{"python3.14", "app/my-file.py", nil, "not a valid Python module name"},
		{"unknown", "service.js", nil, "unsupported runtime"},
		// The generic path rules reject these for BOTH runtimes, before any
		// runtime-specific translation.
		{"node24", "../../etc/passwd", nil, "must not contain"},
		{"python3.14", "../main.py", nil, "must not contain"},
		{"python3.14", "/app/main.py", nil, "relative"},
		{"python3.14", "a//b.py", nil, "empty path elements"},
		{"python3.14", ".hidden.py", nil, "path elements must not"},
	} {
		t.Run(tc.runtime+"/"+tc.entrypoint, func(t *testing.T) {
			got, err := ServiceEntry(tc.runtime, tc.entrypoint)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, got)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not mention %q", err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("entry = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("entry[%d] = %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

// TestSweepSkipsServiceContainers verifies sweepSkips (the decision helper
// SweepOrphanContainers uses for every container) excludes service containers
// even though they carry relay.function + relay.hostname and would otherwise be
// Relay-owned — a service must never be swept as an orphan.
func TestSweepSkipsServiceContainers(t *testing.T) {
	svc := serviceLabels(ServiceSpec{Function: "f", Identity: "svc", Image: "img"}, "test-host", 0)
	if !sweepSkips(svc) {
		t.Error("sweepSkips(service labels) = false, want true (services are persistent, reconciler-owned)")
	}
	// A typed invocation container (relay.type=event) must NOT be skipped.
	exec := runLabels(RunMeta{Type: ContainerTypeEvent, Function: "f", Hostname: "test-host"})
	if sweepSkips(exec) {
		t.Error("sweepSkips(event invocation labels) = true, want false")
	}
	// A non-Relay container (no known relay.type, no relay.function) is skipped.
	if !sweepSkips(map[string]string{"app": "x"}) {
		t.Error("sweepSkips(non-relay) = false, want true")
	}
	// A container carrying relay.function but NO relay.type is strictly NOT
	// Relay-owned under the new model and must be skipped (backward compat is
	// not required).
	if !sweepSkips(map[string]string{labelFunction: "f", labelHostname: "h"}) {
		t.Error("sweepSkips(relay.function but no relay.type) = false, want true")
	}
}

// TestServiceContainerListParsing verifies the discovery mapping over a scripted
// daemon: only relay.type=service containers are returned, replica and port are
// parsed from their labels, a missing/non-numeric replica defaults to -1, and a
// non-service container (including one with no labels at all) is excluded safely.
// The client-call path is exercised without a real Docker daemon.
func TestServiceContainerListParsing(t *testing.T) {
	c1Labels := `{"relay.type":"service","relay.function":"fn-a","relay.identity":"svc.js",` +
		`"relay.image":"img-a","relay.hostname":"h1","relay.port":"3000","relay.replica":"2",` +
		`"relay.env_hash":"0123456789abcdef"}`
	c2Labels := `{"relay.type":"service","relay.function":"fn-a","relay.identity":"svc.js",` +
		`"relay.image":"img-a","relay.hostname":"h1","relay.port":"notaport"}`
	body := `[{"Id":"c1","Labels":` + c1Labels + `},` +
		`{"Id":"c2","Labels":` + c2Labels + `},` +
		`{"Id":"c3","Labels":{"relay.type":"event","relay.function":"fn-a"}},` +
		`{"Id":"c4"}]`
	cli := newScriptedDockerClient(t, dockerRoute{method: http.MethodGet, path: "/containers/json", body: body})
	m := &Manager{cli: cli, log: testutil.DiscardLogger()}

	list, err := m.ServiceContainerList(context.Background())
	if err != nil {
		t.Fatalf("ServiceContainerList: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("list = %+v, want the two service containers (event excluded)", list)
	}
	byID := map[string]ServiceContainer{}
	for _, c := range list {
		byID[c.ID] = c
	}

	c1 := byID["c1"]
	if c1.Function != "fn-a" || c1.Identity != "svc.js" || c1.Image != "img-a" || c1.Hostname != "h1" {
		t.Errorf("c1 identity = %+v, want the label-derived identity", c1)
	}
	if c1.Replica != 2 {
		t.Errorf("c1 replica = %d, want the parsed 2", c1.Replica)
	}
	if c1.Port != 3000 {
		t.Errorf("c1 port = %d, want the parsed 3000", c1.Port)
	}
	if c1.EnvHash != "0123456789abcdef" {
		t.Errorf("c1 env hash = %q, want the parsed relay.env_hash", c1.EnvHash)
	}

	// c2 has no relay.replica and a non-numeric relay.port: replica defaults to
	// -1 (always-stale to Reconcile) and port to 0. Its env hash is empty
	// (missing label) — a legacy/unlabeled container that never matches a
	// desired hash, so it is replaced once.
	c2 := byID["c2"]
	if c2.Replica != -1 {
		t.Errorf("c2 replica = %d, want -1 default for a missing label", c2.Replica)
	}
	if c2.Port != 0 {
		t.Errorf("c2 port = %d, want 0 default for a non-numeric label", c2.Port)
	}
	if c2.EnvHash != "" {
		t.Errorf("c2 env hash = %q, want empty for a missing label", c2.EnvHash)
	}

	// The full label set is copied, so the reconciler can compare foreign labels.
	if c1.Labels["relay.port"] != "3000" {
		t.Errorf("c1 labels copy = %v, want the full label set", c1.Labels)
	}
}

// TestServiceLabelsSpecMergedOwnershipWins pins that serviceLabels merges the
// spec's extra labels onto the relay ownership set, and that Relay ownership
// ALWAYS wins: a spec label colliding with a relay.* key is overwritten by the
// authoritative relay value.
func TestServiceLabelsSpecMergedOwnershipWins(t *testing.T) {
	spec := ServiceSpec{
		Function: "user-events",
		Identity: "service.js",
		Port:     3000,
		Image:    "relay-fn-user-events:deadbeef",
		Labels: map[string]string{
			"traefik.enable": "true",
			// Attempted spoof of Relay ownership keys.
			labelType:     ContainerTypeEvent,
			labelFunction: "spoofed",
			labelPort:     "9999",
			labelEnvHash:  "spoofedhash",
			"custom.key":  "custom-value",
		},
	}
	got := serviceLabels(spec, "worker-1", 0)

	if got[labelType] != ContainerTypeService {
		t.Fatalf("relay.type = %q, want %q (ownership must win)", got[labelType], ContainerTypeService)
	}
	if got[labelFunction] != "user-events" {
		t.Fatalf("relay.function = %q, want user-events (ownership must win)", got[labelFunction])
	}
	if got[labelPort] != "3000" {
		t.Fatalf("relay.port = %q, want 3000 (ownership must win)", got[labelPort])
	}
	if got[labelEnvHash] != EnvHash(nil) {
		t.Fatalf("relay.env_hash = %q, want the authoritative env hash (ownership must win)", got[labelEnvHash])
	}
	if got["traefik.enable"] != "true" {
		t.Fatalf("extra routing label missing: %v", got)
	}
	if got["custom.key"] != "custom-value" {
		t.Fatalf("custom label missing: %v", got)
	}
	if got[labelIdentity] != "service.js" || got[labelHostname] != "worker-1" || got[labelReplica] != "0" {
		t.Fatalf("ownership labels corrupted: %v", got)
	}
}

// captureServiceCreate drives the real StartService client path against the
// scripted daemon and returns the JSON body of the /containers/create request,
// so a test can assert the container's Docker Config.Env and labels without a
// daemon. Start is scripted so the create+start sequence completes.
func captureServiceCreate(t *testing.T, spec ServiceSpec) []byte {
	t.Helper()
	var createBody []byte
	cli := newScriptedDockerClient(t,
		dockerRoute{
			method: http.MethodPost, path: "/containers/create",
			body:   `{"Id":"cid-1"}`,
			onBody: func(b []byte) { createBody = append([]byte(nil), b...) },
		},
		dockerRoute{method: http.MethodPost, path: "/start", body: `{}`},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), hostname: "test-host"}
	if _, err := m.StartService(context.Background(), spec, 0); err != nil {
		t.Fatalf("StartService: %v", err)
	}
	if len(createBody) == 0 {
		t.Fatal("no /containers/create body captured")
	}
	return createBody
}

// decodeCreateConfig decodes the captured create request body into its Config,
// so a test can assert the exact Docker Config.Env/Labels StartService sends.
func decodeCreateConfig(t *testing.T, body []byte) *container.Config {
	t.Helper()
	return decodeCreateRequest(t, body).Config
}

// decodeCreateRequest decodes the full create request body, so a test can assert
// its NetworkingConfig in addition to Config/HostConfig.
func decodeCreateRequest(t *testing.T, body []byte) *container.CreateRequest {
	t.Helper()
	var req container.CreateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		t.Fatalf("decode create request: %v\n%s", err, body)
	}
	if req.Config == nil {
		t.Fatalf("create request has no Config:\n%s", body)
	}
	return &req
}

// TestStartServiceWritesEffectiveEnvToConfigEnvForAllSources pins the invariant
// that BOTH source kinds write the caller-assembled effective environment
// verbatim into Docker Config.Env — entrypoint (with an entry override) and
// image (preserving the image ENTRYPOINT). The env slice is exactly what the
// reconciler's BuildEnv produced (prepared env + template env + resolved secrets
// + PORT), so a service actually receives its configured env/secrets. The
// relay.env_hash label is the digest of that same slice.
func TestStartServiceWritesEffectiveEnvToConfigEnvForAllSources(t *testing.T) {
	env := []string{"PREPARED=1", "GREETING=hello", "SECRET=s3cr3t", "PORT=3000"}
	for _, tc := range []struct {
		name string
		spec ServiceSpec
	}{
		{
			name: "entrypoint",
			spec: ServiceSpec{
				Function: "fn", Identity: "service.js", Port: 3000,
				Image: "relay-fn-fn:tag", Entry: []string{"node", "/app/service.js"}, Env: env,
			},
		},
		{
			name: "image",
			spec: ServiceSpec{
				Function: "fn", Identity: "ghcr.io/acme/api:1.2", Port: 3000,
				Image: "ghcr.io/acme/api:1.2", ImageID: "sha256:cafe", Env: env,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := decodeCreateConfig(t, captureServiceCreate(t, tc.spec))
			if len(cfg.Env) != len(env) {
				t.Fatalf("Config.Env = %v, want the effective env %v", cfg.Env, env)
			}
			for i := range env {
				if cfg.Env[i] != env[i] {
					t.Fatalf("Config.Env[%d] = %q, want %q (full: %v)", i, cfg.Env[i], env[i], cfg.Env)
				}
			}
			if cfg.Labels[labelEnvHash] != EnvHash(env) {
				t.Fatalf("relay.env_hash = %q, want the effective env hash %q", cfg.Labels[labelEnvHash], EnvHash(env))
			}
			// No secret VALUE is exposed anywhere in the labels; only the digest.
			for k, v := range cfg.Labels {
				if strings.Contains(v, "s3cr3t") {
					t.Fatalf("secret value leaked into label %q=%q", k, v)
				}
			}
		})
	}
}

// TestStartServiceEntryOnlyForEntrypointSource pins that the entry override
// reaches Config.Entrypoint only for an entrypoint source; an image source
// leaves it empty so the image's own ENTRYPOINT/CMD is preserved.
func TestStartServiceEntryOnlyForEntrypointSource(t *testing.T) {
	entry := decodeCreateConfig(t, captureServiceCreate(t, ServiceSpec{
		Function: "fn", Identity: "service.js", Port: 3000,
		Image: "img", Entry: []string{"node", "/app/service.js"}, Env: []string{"PORT=3000"},
	}))
	if len(entry.Entrypoint) != 2 || entry.Entrypoint[0] != "node" || entry.Entrypoint[1] != "/app/service.js" {
		t.Fatalf("entrypoint-source Entrypoint = %v, want [node /app/service.js]", entry.Entrypoint)
	}

	image := decodeCreateConfig(t, captureServiceCreate(t, ServiceSpec{
		Function: "fn", Identity: "ghcr.io/acme/api:1.2", Port: 3000, Image: "ghcr.io/acme/api:1.2", Env: []string{"PORT=3000"},
	}))
	if len(image.Entrypoint) != 0 {
		t.Fatalf("image-source Entrypoint = %v, want none (preserve image ENTRYPOINT)", image.Entrypoint)
	}
	if image.Labels[labelHostname] != "test-host" {
		t.Fatalf("relay.hostname = %q, want test-host", image.Labels[labelHostname])
	}
}

// TestStartServiceNetworkingConfig pins that the routing network reaches
// Docker's NetworkingConfig EndpointsConfig and that the canonical
// relay.networks label matches. A service with no routing network sends no
// NetworkingConfig at all (default bridge/network behavior). Service networking
// is independent of the worker-global NETWORKS set applied to execution
// containers.
func TestStartServiceNetworkingConfig(t *testing.T) {
	t.Run("routing network joined", func(t *testing.T) {
		req := decodeCreateRequest(t, captureServiceCreate(t, ServiceSpec{
			Function: "fn", Identity: "service.js", Port: 3000, Image: "img",
			Env: []string{"PORT=3000"}, Network: "proxy",
		}))
		if req.NetworkingConfig == nil {
			t.Fatal("expected a NetworkingConfig for a routed service")
		}
		got := req.NetworkingConfig.EndpointsConfig
		if len(got) != 1 {
			t.Fatalf("endpoints = %v, want only proxy", got)
		}
		if _, ok := got["proxy"]; !ok {
			t.Fatalf("missing proxy endpoint: %v", got)
		}
		if req.Config.Labels[labelNetworks] != "proxy" {
			t.Fatalf("relay.networks = %q, want proxy", req.Config.Labels[labelNetworks])
		}
	})

	t.Run("no network sends no NetworkingConfig", func(t *testing.T) {
		req := decodeCreateRequest(t, captureServiceCreate(t, ServiceSpec{
			Function: "fn", Identity: "service.js", Port: 3000, Image: "img",
			Env: []string{"PORT=3000"},
		}))
		if req.NetworkingConfig != nil {
			t.Fatalf("expected no NetworkingConfig, got %v", req.NetworkingConfig)
		}
		if _, ok := req.Config.Labels[labelNetworks]; ok {
			t.Fatalf("relay.networks must be omitted, got %q", req.Config.Labels[labelNetworks])
		}
	})
}

// TestServiceLabelsNetworks pins the canonical relay.networks label: sorted,
// deduped, and omitted when empty. It also pins that a caller-supplied label can
// never spoof it.
func TestServiceLabelsNetworks(t *testing.T) {
	base := serviceLabels(ServiceSpec{Function: "fn", Identity: "svc", Image: "img"}, "h", 0)
	if _, ok := base[labelNetworks]; ok {
		t.Fatalf("no networks must omit relay.networks, got %q", base[labelNetworks])
	}

	got := serviceLabels(ServiceSpec{
		Function: "fn", Identity: "svc", Image: "img", Network: "proxy",
	}, "h", 0)
	if got[labelNetworks] != "proxy" {
		t.Fatalf("relay.networks = %q, want proxy", got[labelNetworks])
	}

	spoof := serviceLabels(ServiceSpec{
		Function: "fn", Identity: "svc", Image: "img", Network: "real",
		Labels: map[string]string{labelNetworks: "spoofed"},
	}, "h", 0)
	if spoof[labelNetworks] != "real" {
		t.Fatalf("relay.networks = %q; caller must not spoof it", spoof[labelNetworks])
	}
}

// TestNetworksLabelCanonical pins the exported canonical helper used by the
// service reconciler to compare a desired network set to a discovered label.
func TestNetworksLabelCanonical(t *testing.T) {
	if got := NetworksLabel(); got != "" {
		t.Fatalf("empty = %q, want empty", got)
	}
	if got := NetworksLabel(""); got != "" {
		t.Fatalf("empty name = %q, want empty", got)
	}
	if got := NetworksLabel("proxy"); got != "proxy" {
		t.Fatalf("routing only = %q, want proxy", got)
	}
	if got := NetworksLabel("b", "a", "proxy", ""); got != "a,b,proxy" {
		t.Fatalf("union = %q, want a,b,proxy", got)
	}
}

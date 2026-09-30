package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"

	"relay/internal/function"
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
		// No spec.Resources -> the default limits' fingerprint (a container
		// created without explicit resources still carries the effective
		// default config's hash so discovery can compare it).
		labelResources: function.DefaultResourceLimits().Fingerprint(),
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

// TestEnvHashDeterministicAcrossProcesses pins the cross-restart determinism the
// service reconciler depends on: the same effective env must hash to the SAME
// value every time and every build. The digest is a pure, UNSALTED function of
// the env alone (nothing per-boot is mixed in), so a container labeled before a
// worker restart still compares equal after it. A salted digest with no
// persisted key would break that and spuriously replace every service container
// on each restart; this test guards against introducing a process-random salt.
// Only the digest is asserted, never a raw env value.
func TestEnvHashDeterministicAcrossProcesses(t *testing.T) {
	env := []string{"A=1", "TOKEN=secret-value", "PORT=8080"}
	first := EnvHash(env)
	second := EnvHash(append([]string(nil), env...))
	if first != second {
		t.Fatalf("EnvHash is not deterministic: %q vs %q", first, second)
	}
	// The exact digest pins the UNSALTED construction across builds.
	const want = "f913ec6c9980f339"
	if first != want {
		t.Fatalf("EnvHash = %q, want the unsalted construction %q", first, want)
	}
	if strings.Contains(first, "secret-value") {
		t.Fatalf("EnvHash leaked env content: %q", first)
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

// TestServiceContainerNameForStartUniqueAndBounded pins the physical-name
// contract the zero-downtime replacement relies on: the logical name stays
// deterministic (serviceContainerName), while the per-start physical name keeps
// that logical name as a greppable prefix and appends a token, so two starts for
// the SAME logical slot can coexist (Docker rejects duplicate names). The
// physical name always stays under the Docker-safe cap.
func TestServiceContainerNameForStartUniqueAndBounded(t *testing.T) {
	logical := serviceContainerName("fn", "service.js", 0)
	first := serviceContainerNameForStart("fn", "service.js", 0)
	second := serviceContainerNameForStart("fn", "service.js", 0)
	if first == second {
		t.Fatalf("two starts for one slot produced the same physical name %q; a replacement could not coexist", first)
	}
	if !strings.HasPrefix(first, logical+"-") || !strings.HasPrefix(second, logical+"-") {
		t.Fatalf("physical names %q/%q must keep the logical prefix %q", first, second, logical)
	}
	for _, n := range []string{first, second} {
		if len(n) > serviceContainerNamePhysicalLenCap {
			t.Fatalf("physical name %q length %d exceeds cap %d", n, len(n), serviceContainerNamePhysicalLenCap)
		}
	}
	// A long identity stays bounded for the physical name too.
	long := serviceContainerNameForStart("fn", strings.Repeat("x", 300), 7)
	if len(long) > serviceContainerNamePhysicalLenCap {
		t.Fatalf("long physical name length %d exceeds cap %d", len(long), serviceContainerNamePhysicalLenCap)
	}
}

// TestStartServiceConfirmsRunningBeforeReturn pins the running gate: StartService
// inspects the started container and only returns success when Docker reports it
// running.
func TestStartServiceConfirmsRunningBeforeReturn(t *testing.T) {
	inspected := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodPost, path: "/containers/create", body: `{"Id":"cid-1"}`,
			onBody: func([]byte) {}},
		dockerRoute{method: http.MethodPost, path: "/start", body: `{}`},
		dockerRoute{method: http.MethodGet, path: "/containers/cid-1/json",
			body:    `{"Id":"cid-1","State":{"Running":true}}`,
			onMatch: func() { inspected++ }},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), hostname: "h"}
	id, err := m.StartService(context.Background(), ServiceSpec{
		Function: "fn", Identity: "service.js", Port: 80, Image: "img", Env: []string{"PORT=80"},
	}, 0)
	if err != nil {
		t.Fatalf("StartService: %v", err)
	}
	if id != "cid-1" {
		t.Fatalf("id = %q, want cid-1", id)
	}
	if inspected != 1 {
		t.Fatalf("inspect calls = %d, want exactly 1 (no polling/sleep)", inspected)
	}
}

// TestStartServiceNonRunningInspectDiscardsAndFails pins the failure side of the
// running gate: a container that started but inspects as not running is removed
// (best-effort) and StartService errors, so the caller never stops the old
// generation for a dead replacement.
func TestStartServiceNonRunningInspectDiscardsAndFails(t *testing.T) {
	removed := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodPost, path: "/containers/create", body: `{"Id":"cid-1"}`},
		dockerRoute{method: http.MethodPost, path: "/start", body: `{}`},
		dockerRoute{method: http.MethodGet, path: "/containers/cid-1/json",
			body: `{"Id":"cid-1","State":{"Running":false,"Status":"exited"}}`},
		dockerRoute{method: http.MethodDelete, path: "/containers/cid-1", body: `{}`,
			onMatch: func() { removed++ }},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), hostname: "h"}
	id, err := m.StartService(context.Background(), ServiceSpec{
		Function: "fn", Identity: "service.js", Port: 80, Image: "img", Env: []string{"PORT=80"},
	}, 0)
	if err == nil {
		t.Fatal("a non-running started container must fail StartService")
	}
	if id != "" {
		t.Fatalf("id = %q, want empty on failure", id)
	}
	if !strings.Contains(err.Error(), "not running") {
		t.Fatalf("error = %v, want it to mention the running gate", err)
	}
	if removed != 1 {
		t.Fatalf("discard removals = %d, want 1", removed)
	}
}

// TestStartServiceInspectErrorDiscardsAndFails pins that an inspect failure
// (daemon blip) is also fatal to the start: the container is discarded rather
// than reported as a viable replacement.
func TestStartServiceInspectErrorDiscardsAndFails(t *testing.T) {
	removed := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodPost, path: "/containers/create", body: `{"Id":"cid-1"}`},
		dockerRoute{method: http.MethodPost, path: "/start", body: `{}`},
		dockerRoute{method: http.MethodGet, path: "/containers/cid-1/json",
			status: http.StatusInternalServerError, body: `{"message":"daemon blip"}`},
		dockerRoute{method: http.MethodDelete, path: "/containers/cid-1", body: `{}`,
			onMatch: func() { removed++ }},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), hostname: "h"}
	if _, err := m.StartService(context.Background(), ServiceSpec{
		Function: "fn", Identity: "service.js", Port: 80, Image: "img", Env: []string{"PORT=80"},
	}, 0); err == nil {
		t.Fatal("an inspect failure must fail StartService")
	}
	if removed != 1 {
		t.Fatalf("discard removals = %d, want 1", removed)
	}
}

// TestStartServiceCreateSendsUniquePhysicalName pins that the name sent to
// /containers/create is the unique physical name (logical name + token), so two
// starts never collide. The container name travels as the create request's
// `name` query parameter, not in the body.
func TestStartServiceCreateSendsUniquePhysicalName(t *testing.T) {
	var names []string
	route := dockerRoute{method: http.MethodPost, path: "/containers/create", body: `{"Id":"cid-1"}`,
		onRequest: func(req *http.Request) {
			names = append(names, req.URL.Query().Get("name"))
		}}
	cli := newScriptedDockerClient(t,
		route,
		dockerRoute{method: http.MethodPost, path: "/start", body: `{}`},
		dockerRoute{method: http.MethodGet, path: "/containers/cid-1/json", body: `{"Id":"cid-1","State":{"Running":true}}`},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), hostname: "h"}
	for i := 0; i < 2; i++ {
		if _, err := m.StartService(context.Background(), ServiceSpec{
			Function: "fn", Identity: "service.js", Port: 80, Image: "img", Env: []string{"PORT=80"},
		}, 0); err != nil {
			t.Fatalf("StartService %d: %v", i, err)
		}
	}
	if len(names) != 2 {
		t.Fatalf("captured %d create names, want 2", len(names))
	}
	if names[0] == names[1] {
		t.Fatalf("both creates used the same name %q; a replacement could not coexist", names[0])
	}
	logical := serviceContainerName("fn", "service.js", 0)
	for _, n := range names {
		if !strings.HasPrefix(n, logical+"-") {
			t.Fatalf("create name %q does not keep the logical prefix %q", n, logical)
		}
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

// TestStartServiceAppliesResourceLimitsForAllSources pins that BOTH source kinds
// (entrypoint and external image) map the spec's effective resource limits onto
// the Docker HostConfig, and stamp the same limits' fingerprint as
// relay.resources so the reconciler can detect a resource-only change. It drives
// the real StartService client path against the scripted daemon.
func TestStartServiceAppliesResourceLimitsForAllSources(t *testing.T) {
	limits := function.ResourceLimits{MemoryBytes: 512 << 20, NanoCPUs: 250_000_000, PidsLimit: 48}
	for _, tc := range []struct {
		name string
		spec ServiceSpec
	}{
		{
			name: "entrypoint",
			spec: ServiceSpec{
				Function: "fn", Identity: "service.js", Port: 3000, Image: "relay-fn-fn:tag",
				Entry: []string{"node", "/app/service.js"}, Env: []string{"PORT=3000"}, Resources: limits,
			},
		},
		{
			name: "image",
			spec: ServiceSpec{
				Function: "fn", Identity: "ghcr.io/acme/api:1.2", Port: 3000,
				Image: "ghcr.io/acme/api:1.2", ImageID: "sha256:cafe", Env: []string{"PORT=3000"}, Resources: limits,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := decodeCreateRequest(t, captureServiceCreate(t, tc.spec))
			hc := req.HostConfig
			if hc == nil {
				t.Fatal("create request has no HostConfig")
			}
			if hc.Memory != limits.MemoryBytes || hc.NanoCPUs != limits.NanoCPUs ||
				hc.PidsLimit == nil || *hc.PidsLimit != limits.PidsLimit {
				t.Fatalf("HostConfig resources = mem %d nano %d pids %v; want %+v",
					hc.Memory, hc.NanoCPUs, hc.PidsLimit, limits)
			}
			// Hardening baseline preserved.
			if !hc.ReadonlyRootfs || len(hc.CapDrop) != 1 || hc.CapDrop[0] != "ALL" {
				t.Fatalf("hardening baseline lost: %+v", hc)
			}
			if hc.AutoRemove {
				t.Fatal("service container must not AutoRemove")
			}
			if req.Config.Labels[labelResources] != limits.Fingerprint() {
				t.Fatalf("relay.resources = %q, want %q", req.Config.Labels[labelResources], limits.Fingerprint())
			}
		})
	}
}

// TestServiceLabelsResourcesFingerprint pins the relay.resources label's
// contract: it is value-free (fixed hex length, no raw byte/cpu numbers), it
// changes when any resource field changes, and a zero value fingerprints as the
// defaults so a legacy/unlabeled container is replaced once.
func TestServiceLabelsResourcesFingerprint(t *testing.T) {
	spec := func(r function.ResourceLimits) ServiceSpec {
		return ServiceSpec{Function: "fn", Identity: "svc", Image: "img", Port: 80, Resources: r}
	}
	a := serviceLabels(spec(function.DefaultResourceLimits()), "h", 0)[labelResources]
	b := serviceLabels(spec(function.ResourceLimits{MemoryBytes: 64 << 20, NanoCPUs: 1_000_000_000, PidsLimit: 128}), "h", 0)[labelResources]
	if a == "" || len(a) != serviceIdentityHashLen {
		t.Fatalf("relay.resources = %q, want %d hex chars", a, serviceIdentityHashLen)
	}
	if a == b {
		t.Fatal("a changed memory limit must change relay.resources")
	}
	if serviceLabels(spec(function.ResourceLimits{}), "h", 0)[labelResources] != a {
		t.Fatal("a zero value must fingerprint as the defaults")
	}
}

// captureServiceCreate drives the real StartService client path against the
// scripted daemon and returns the JSON body of the /containers/create request,
// so a test can assert the container's Docker Config.Env and labels without a
// daemon. Create, start, and the post-start running INSPECT are scripted so the
// full create+start+confirm sequence completes.
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
		// The running gate inspects the started container and requires Running.
		dockerRoute{method: http.MethodGet, path: "/containers/cid-1/json", body: `{"Id":"cid-1","State":{"Running":true}}`},
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

// TestStartServiceNetworkingConfig drives the real StartService client path
// against the scripted daemon and inspects the ACTUAL serialized
// /containers/create request (via captureServiceCreate + decodeCreateRequest),
// so the service's final network set is proven to reach Docker's
// NetworkingConfig.EndpointsConfig exactly as the service reconciler intends.
//
// The reconciler merges the worker-global NETWORKS set with the routing network
// (TRAEFIK_NETWORK) for a routed service, de-duplicated and sorted, and passes
// that FINAL set as spec.Networks. These cases assert that final set as the
// Docker request layer sees it: the exact endpoint key set (order-independent;
// EndpointsConfig is a map) and the canonical relay.networks label the
// reconciler later compares to decide whether a container is stale.
func TestStartServiceNetworkingConfig(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec []string // spec.Networks: the reconciler's final desired set
		want []string // exact expected endpoints / relay.networks members
	}{
		{
			// Unrouted service: exactly the worker-global backend +
			// observability set, no routing network.
			name: "unrouted global backend and observability",
			spec: []string{"backend", "observability"},
			want: []string{"backend", "observability"},
		},
		{
			// Routed service: the global backend + observability set plus the
			// routing proxy, each joined exactly once.
			name: "routed global backend observability and proxy",
			spec: []string{"backend", "observability", "proxy"},
			want: []string{"backend", "observability", "proxy"},
		},
		{
			// The routing proxy also appears in the global set: the merged set
			// contains proxy once, so the endpoints are exactly backend+proxy.
			name: "global backend and proxy overlapping routing",
			spec: []string{"backend", "proxy"},
			want: []string{"backend", "proxy"},
		},
		{
			// A single global network is applied exactly once.
			name: "single global backend",
			spec: []string{"backend"},
			want: []string{"backend"},
		},
		{
			// Repeated global values collapse to the distinct set (and the
			// canonical label), never a duplicated endpoint.
			name: "repeated global values deduped",
			spec: []string{"backend", "backend", "observability", "observability"},
			want: []string{"backend", "observability"},
		},
		{
			// A service with no networks sends no NetworkingConfig at all
			// (default bridge/network behavior) and omits relay.networks.
			name: "no network sends no NetworkingConfig",
			spec: nil,
			want: nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := decodeCreateRequest(t, captureServiceCreate(t, ServiceSpec{
				Function: "fn", Identity: "service.js", Port: 3000, Image: "img",
				Env: []string{"PORT=3000"}, Networks: tc.spec,
			}))
			assertServiceEndpoints(t, req, tc.want...)
		})
	}
}

// assertServiceEndpoints asserts a decoded create request's ACTUAL
// NetworkingConfig.EndpointsConfig is exactly the expected network set — same
// count and same keys, order-independent because EndpointsConfig is a map — and
// that the canonical relay.networks label matches that same canonical set. An
// empty want asserts no NetworkingConfig and no relay.networks label at all.
func assertServiceEndpoints(t *testing.T, req *container.CreateRequest, want ...string) {
	t.Helper()
	wantSet := NetworkSet(want)
	if len(wantSet) == 0 {
		if req.NetworkingConfig != nil {
			t.Fatalf("expected no NetworkingConfig, got %v", req.NetworkingConfig)
		}
		if _, ok := req.Config.Labels[labelNetworks]; ok {
			t.Fatalf("relay.networks must be omitted, got %q", req.Config.Labels[labelNetworks])
		}
		return
	}
	if req.NetworkingConfig == nil {
		t.Fatalf("expected a NetworkingConfig with %v", wantSet)
	}
	got := req.NetworkingConfig.EndpointsConfig
	if len(got) != len(wantSet) {
		t.Fatalf("endpoint count = %d (%v), want %d (%v)", len(got), endpointKeys(got), len(wantSet), wantSet)
	}
	for _, name := range wantSet {
		if _, ok := got[name]; !ok {
			t.Fatalf("missing %q endpoint: %v", name, endpointKeys(got))
		}
	}
	wantLabel := strings.Join(wantSet, ",")
	if gotLabel := req.Config.Labels[labelNetworks]; gotLabel != wantLabel {
		t.Fatalf("relay.networks = %q, want the canonical %q", gotLabel, wantLabel)
	}
}

// endpointKeys returns the sorted network names of an EndpointsConfig, so a
// failure message is deterministic regardless of map iteration order.
func endpointKeys(endpoints map[string]*network.EndpointSettings) []string {
	keys := make([]string, 0, len(endpoints))
	for name := range endpoints {
		keys = append(keys, name)
	}
	sort.Strings(keys)
	return keys
}

// TestStartServiceDeletedNetworkCreateFailure pins the post-start network-deleted
// behavior for services: a network that vanished after startup verification
// surfaces as a container-create error, and Relay NEVER issues a network create
// to fix it. The scripted daemon fails the test on any unhandled route, so a
// /networks/create would surface structurally as well as via the counter.
func TestStartServiceDeletedNetworkCreateFailure(t *testing.T) {
	networkCreates := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{
			method: http.MethodPost, path: "/containers/create",
			status: http.StatusNotFound,
			body:   `{"message":"network backend not found"}`,
		},
		dockerRoute{
			method: http.MethodPost, path: "/networks/create", body: `{"Id":"nid"}`,
			onMatch: func() { networkCreates++ },
		},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), hostname: "test-host"}

	_, err := m.StartService(context.Background(), ServiceSpec{
		Function: "fn", Identity: "service.js", Port: 3000, Image: "img",
		Env: []string{"PORT=3000"}, Networks: []string{"backend"},
	}, 0)
	if err == nil {
		t.Fatal("a missing network must surface as a service container create error")
	}
	if networkCreates != 0 {
		t.Fatalf("Relay issued %d network create(s); it must never create networks", networkCreates)
	}
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
		Function: "fn", Identity: "svc", Image: "img", Networks: []string{"proxy"},
	}, "h", 0)
	if got[labelNetworks] != "proxy" {
		t.Fatalf("relay.networks = %q, want proxy", got[labelNetworks])
	}

	spoof := serviceLabels(ServiceSpec{
		Function: "fn", Identity: "svc", Image: "img", Networks: []string{"real"},
		Labels: map[string]string{labelNetworks: "spoofed"},
	}, "h", 0)
	if spoof[labelNetworks] != "real" {
		t.Fatalf("relay.networks = %q; caller must not spoof it", spoof[labelNetworks])
	}
}

// TestNetworksLabelCanonical pins the exported canonical helper used by the
// reconciler to compare a desired network set to a discovered label.
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
	if got := NetworksLabel("b", "a", "b", "proxy"); got != "a,b,proxy" {
		t.Fatalf("union with duplicates = %q, want a,b,proxy", got)
	}
}

// TestNetworkSet pins the shared normalization used by both the Docker
// NetworkingConfig and the relay.networks label: non-empty entries, de-duped,
// sorted; nil for empty/all-empty.
func TestNetworkSet(t *testing.T) {
	if got := NetworkSet(nil); got != nil {
		t.Fatalf("NetworkSet(nil) = %v, want nil", got)
	}
	if got := NetworkSet([]string{"", ""}); got != nil {
		t.Fatalf("NetworkSet(empties) = %v, want nil", got)
	}
	got := NetworkSet([]string{"proxy", "backend", "proxy", "frontend"})
	want := []string{"backend", "frontend", "proxy"}
	if len(got) != len(want) {
		t.Fatalf("NetworkSet = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("NetworkSet[%d] = %q, want %q (full %v)", i, got[i], want[i], got)
		}
	}
}

package runtime

// Unit tests for the persistent internal control network
// (control_network.go): deterministic name/label derivation, strict
// ownership/configuration validation, provision-or-reuse semantics against the
// scripted daemon (including the NetworkCreate conflict race and the
// created-ID/name ambiguity guard), verified self-container attachment
// (idempotent NetworkConnect, fail-closed identity checks), the direct-route
// proof, execution-network composition, and the pre-create control
// revalidation. No real daemon, no published ports, no NETWORKS fallback: every
// fixture drives the production client path through newScriptedDockerClient,
// whose unhandled-route failure makes any unexpected Docker call (an adoption,
// a modification, a removal, a container inspect by NODE_ID) surface as a test
// failure.
//
// Direct-route (non-containerized) SUCCESS is proven against THIS host's real
// interfaces through the pre-attached-endpoint proof: the fixture advertises an
// endpoint carrying the exact address/prefix/MAC of a real up local interface,
// which is precisely the custom-hostname-container case production accepts. On
// a host with no MAC-bearing addressed interface that proof cannot be
// constructed, and only those cases skip (with the reason) rather than
// weakening the proof.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/moby/moby/api/types/network"

	"relay/internal/app"
	"relay/internal/testutil"
)

// unitControlNetwork is the deterministic control-network name used by
// fixtures whose Manager carries an explicit control identity without going
// through EnsureControlNetwork.
const unitControlNetwork = "relay-control-unit-node"

// newControlManager builds a directly constructed Manager wired to the
// scripted daemon for control-network tests. selfHostname stays empty unless
// the containerized self-attach path is under test.
func newControlManager(t *testing.T, nodeID string, routes ...dockerRoute) *Manager {
	t.Helper()
	return newControlManagerWithSelf(t, nodeID, "", routes...)
}

func newControlManagerWithSelf(t *testing.T, nodeID, selfHostname string, routes ...dockerRoute) *Manager {
	t.Helper()
	return &Manager{
		cli:          newScriptedDockerClient(t, routes...),
		log:          testutil.DiscardLogger(),
		nodeID:       nodeID,
		selfHostname: selfHostname,
	}
}

// controlNetFixture shapes one scripted control-network inspect body. The
// defaults render the network Relay itself would create; a test mutates
// exactly the field whose rejection it pins. An empty subnet omits IPAM
// entirely (the "no IPAM subnet/gateway" failure). containersJSON, when set,
// is a pre-rendered "Containers" object advertising endpoints on the network
// (the pre-attached direct-route proof reads them).
type controlNetFixture struct {
	name           string
	id             string
	scope          string
	driver         string
	internal       bool
	configOnly     bool
	ingress        bool
	labels         map[string]string
	options        map[string]string
	subnet         string
	containersJSON string
}

// defaultControlNetFixture renders the network Relay itself would create for
// nodeID: bridge/local/not-internal, the three ownership labels, and the given
// IPAM subnet.
func defaultControlNetFixture(name, nodeID, subnet string) controlNetFixture {
	return controlNetFixture{
		name:    name,
		id:      "nid-ctrl",
		scope:   "local",
		driver:  "bridge",
		labels:  controlNetworkLabels(nodeID),
		options: map[string]string{},
		subnet:  subnet,
	}
}

func (f controlNetFixture) json(t *testing.T) string {
	t.Helper()
	labels, err := json.Marshal(f.labels)
	if err != nil {
		t.Fatalf("marshal fixture labels: %v", err)
	}
	options, err := json.Marshal(f.options)
	if err != nil {
		t.Fatalf("marshal fixture options: %v", err)
	}
	ipam := ""
	if f.subnet != "" {
		ipam = fmt.Sprintf(`,"IPAM":{"Config":[{"Subnet":%q}]}`, f.subnet)
	}
	containers := ""
	if f.containersJSON != "" {
		containers = `,"Containers":` + f.containersJSON
	}
	return fmt.Sprintf(
		`{"Name":%q,"Id":%q,"Scope":%q,"Driver":%q,"Internal":%t,"ConfigOnly":%t,"Ingress":%t,"Labels":%s,"Options":%s%s%s}`,
		f.name, f.id, f.scope, f.driver, f.internal, f.configOnly, f.ingress,
		labels, options, ipam, containers,
	)
}

// withLocalEndpoint advertises an endpoint on the fixture that exactly
// matches a real up local interface (address, prefix, MAC) — the
// pre-attached custom-hostname-container proof verifyLocalControlNetworkRoute
// accepts. It returns false when this host has no such interface (every
// addressed interface lacks a MAC, e.g. loopback-only), in which case the
// caller must skip the direct-route success case instead of fabricating a
// weaker proof.
func (f *controlNetFixture) withLocalEndpoint() (string, bool) {
	local, err := localInterfaceAddresses()
	if err != nil {
		return "", false
	}
	for _, candidate := range local {
		if !candidate.up || len(candidate.hardware) == 0 || !candidate.addr.IsValid() {
			continue
		}
		prefix := netipPrefix(candidate.addr, candidate.prefix)
		f.containersJSON = fmt.Sprintf(
			`{"relay-self": {"Name":"relay-self","IPv4Address":%q,"MacAddress":%q}}`,
			prefix, candidate.hardware.String(),
		)
		return fmt.Sprintf("%s on %s", prefix, candidate.hardware.String()), true
	}
	return "", false
}

// netipPrefix renders addr/bits the way Docker reports an endpoint address.
func netipPrefix(addr interface{ String() string }, bits int) string {
	return fmt.Sprintf("%s/%d", addr.String(), bits)
}

// directRouteSkip explains why a direct-route success assertion cannot be
// constructed on this host.
const directRouteSkip = "no up local interface with an address and MAC: the pre-attached direct-route proof cannot be constructed on this host"

// validControlNetInspect is the in-Go shape of a valid control network for the
// pure validateControlNetwork table (no JSON involved).
func validControlNetInspect(name, nodeID string) network.Inspect {
	return network.Inspect{Network: network.Network{
		Name:    name,
		ID:      "nid-ctrl",
		Scope:   "local",
		Driver:  "bridge",
		Labels:  controlNetworkLabels(nodeID),
		Options: map[string]string{},
	}}
}

// networkInspectRoute serves one network inspect body (static) for GET.
func networkInspectRoute(path, body string) dockerRoute {
	return dockerRoute{method: http.MethodGet, path: path, body: body}
}

// notFoundNetwork is the daemon's network-not-found answer.
const notFoundNetwork = `{"message":"network not found"}`

// routeProofFailure is the substring of the fail-closed direct-route error.
const routeProofFailure = "cannot prove a route"

// TestControlNetworkNameDerivation pins the deterministic per-node name: the
// fixed prefix plus the exact NODE_ID, so a restarted Relay looks up exactly
// the network its previous incarnation created, and two nodes never collide.
func TestControlNetworkNameDerivation(t *testing.T) {
	if got, want := controlNetworkName("node-a"), "relay-control-node-a"; got != want {
		t.Fatalf("controlNetworkName = %q, want %q", got, want)
	}
	if got := controlNetworkName("node-a"); !strings.HasPrefix(got, controlNetworkNamePrefix) {
		t.Fatalf("name %q must carry the fixed prefix %q", got, controlNetworkNamePrefix)
	}
	if controlNetworkName("node-a") == controlNetworkName("node-b") {
		t.Fatal("distinct node identities must derive distinct control-network names")
	}
}

// TestControlNetworkLabels pins the exact label contract: the expected map is
// exactly relay.network.role=execution-control plus relay.managed=true and
// relay.node_id=<id> — three keys total, so a reuse check can never silently
// accept extra or missing labels (in particular no legacy marker).
func TestControlNetworkLabels(t *testing.T) {
	labels := controlNetworkLabels("node-a")
	if len(labels) != 3 {
		t.Fatalf("labels = %v, want exactly the three role/ownership labels", labels)
	}

	// Role contract, asserted independently of ownership.
	if got := labels["relay.network.role"]; got != "execution-control" {
		t.Errorf("relay.network.role = %q, want %q", got, "execution-control")
	}
	// Ownership contract, asserted independently of the role.
	if got := labels["relay.managed"]; got != "true" {
		t.Errorf("relay.managed = %q, want %q", got, "true")
	}
	if got := labels["relay.node_id"]; got != "node-a" {
		t.Errorf("relay.node_id = %q, want %q", got, "node-a")
	}
}

// TestValidateControlNodeID pins the defensive NODE_ID bound: an empty or
// oversized identity and any character that could form an ambiguous Docker
// network name are refused before any daemon call; ordinary identities pass.
func TestValidateControlNodeID(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want string // "" means accepted
	}{
		{"empty", "", "empty"},
		{"oversized", strings.Repeat("a", maxControlNodeIDLen+1), "exceeds"},
		{"path separator", "node/other", "unsupported character"},
		{"space", "node other", "unsupported character"},
		{"at sign", "node@x", "unsupported character"},
		{"dotted", "node.prod_1-a", ""},
		{"upper", "NodeA", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateControlNodeID(tc.in)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("validateControlNodeID(%q) = %v, want nil", tc.in, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

// TestValidateControlNetworkStrictness pins every reuse rejection: the exact
// relay.network.role=execution-control role, ownership (node identity, managed
// marker), and configuration (driver, scope, internal, config-only/ingress,
// ICC, masquerade) must all match, the daemon must return the requested name,
// and only the exact valid shape is accepted. The role match is exact: a
// missing or different role — including a legacy same-name network that
// predates the role contract — is never adopted. Only the managed marker
// tolerates case. The table drives the SAME JSON shape EnsureControlNetwork
// inspects.
func TestValidateControlNetworkStrictness(t *testing.T) {
	const name, nodeID = "relay-control-strict", "strict-node"
	for _, tc := range []struct {
		name   string
		mutate func(*controlNetFixture)
		want   string // "" means accepted
	}{
		{"valid shape accepted", func(*controlNetFixture) {}, ""},
		{
			// The exact new label set is accepted when built literally,
			// independently of controlNetworkLabels.
			"exact execution-control labels accepted",
			func(f *controlNetFixture) {
				f.labels = map[string]string{
					"relay.network.role": "execution-control",
					"relay.managed":      "true",
					"relay.node_id":      nodeID,
				}
			},
			"",
		},
		{"managed marker is case-insensitive", func(f *controlNetFixture) {
			f.labels[controlNetworkManagedLabel] = "TRUE"
		}, ""},
		{
			"daemon returned a different name",
			func(f *controlNetFixture) { f.name = "someone-elses-network" },
			"refusing to reuse it",
		},
		{
			// A legacy same-name network predating the role contract carries
			// no role label at all and must never be adopted.
			"legacy same-name network without the execution-control role",
			func(f *controlNetFixture) { delete(f.labels, networkRoleLabel) },
			"refusing to adopt or modify it",
		},
		{
			"wrong execution-control role",
			func(f *controlNetFixture) { f.labels[networkRoleLabel] = "data" },
			"refusing to adopt or modify it",
		},
		{
			"owned by another node",
			func(f *controlNetFixture) { f.labels[labelNodeID] = "other-node" },
			"owned by a different relay node",
		},
		{
			"missing node identity",
			func(f *controlNetFixture) { delete(f.labels, labelNodeID) },
			"owned by a different relay node",
		},
		{
			"missing managed marker",
			func(f *controlNetFixture) { delete(f.labels, controlNetworkManagedLabel) },
			"missing the Relay-managed marker",
		},
		{
			"managed marker false",
			func(f *controlNetFixture) { f.labels[controlNetworkManagedLabel] = "false" },
			"missing the Relay-managed marker",
		},
		{
			"non-bridge driver",
			func(f *controlNetFixture) { f.driver = "overlay" },
			`uses driver "overlay"`,
		},
		{
			"swarm scope",
			func(f *controlNetFixture) { f.scope = "swarm" },
			`has scope "swarm"`,
		},
		{
			"internal network",
			func(f *controlNetFixture) { f.internal = true },
			"is internal",
		},
		{
			"config-only network",
			func(f *controlNetFixture) { f.configOnly = true },
			"config-only or ingress",
		},
		{
			"ingress network",
			func(f *controlNetFixture) { f.ingress = true },
			"config-only or ingress",
		},
		{
			"icc disabled",
			func(f *controlNetFixture) { f.options[controlNetworkICCOption] = "false" },
			"disables inter-container communication",
		},
		{
			"masquerade disabled",
			func(f *controlNetFixture) { f.options[controlNetworkMasqueradeOption] = "false" },
			"disables IP masquerade",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := defaultControlNetFixture(name, nodeID, "172.30.0.0/16")
			tc.mutate(&f)
			var parsed network.Inspect
			if err := json.Unmarshal([]byte(f.json(t)), &parsed); err != nil {
				t.Fatalf("decode fixture: %v", err)
			}
			err := validateControlNetwork(parsed, name, nodeID)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("validateControlNetwork = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

// TestEnsureControlNetworkCreatesWhenAbsent pins the provision path: a missing
// network is created once with the bridge driver and the exact ownership
// labels, then re-inspected BY THE CREATED ID and by name (both must resolve to
// the same network) and validated before the manager records it. The direct
// route is proven through the pre-attached endpoint on this host's real
// interface; no container call happens for a host/local process (no self
// hostname), and no network is ever removed.
func TestEnsureControlNetworkCreatesWhenAbsent(t *testing.T) {
	const nodeID = "cnode"
	name := controlNetworkName(nodeID)

	fixture := defaultControlNetFixture(name, nodeID, "172.30.0.0/16")
	if _, ok := fixture.withLocalEndpoint(); !ok {
		t.Skip(directRouteSkip)
	}
	body := fixture.json(t)

	created := false
	creates := 0
	removes := 0
	var createBody []byte
	cli := newScriptedDockerClient(t,
		// The post-create lookup is by the created ID; the final lookup is by
		// name. Both must resolve to the same network.
		dockerRoute{
			method: http.MethodGet, path: "/networks/nid-ctrl",
			serve: func(*http.Request) (int, string) {
				if !created {
					return http.StatusNotFound, notFoundNetwork
				}
				return http.StatusOK, body
			},
		},
		dockerRoute{
			method: http.MethodGet, path: "/networks/" + name,
			serve: func(*http.Request) (int, string) {
				if !created {
					return http.StatusNotFound, notFoundNetwork
				}
				return http.StatusOK, body
			},
		},
		dockerRoute{
			method: http.MethodPost, path: "/networks/create",
			onMatch: func() {
				creates++
				created = true
			},
			onBody: func(b []byte) { createBody = append([]byte(nil), b...) },
			body:   `{"Id":"nid-ctrl"}`,
		},
		dockerRoute{
			method: http.MethodDelete, path: "/networks/",
			onMatch: func() { removes++ },
			body:    `{}`,
		},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: nodeID}

	if err := m.EnsureControlNetwork(context.Background()); err != nil {
		t.Fatalf("EnsureControlNetwork: %v", err)
	}
	if m.controlNetwork != name {
		t.Fatalf("controlNetwork = %q, want %q", m.controlNetwork, name)
	}
	if m.controlSelfID != "" {
		t.Fatalf("controlSelfID = %q, want empty (a host process attaches no container)", m.controlSelfID)
	}
	if creates != 1 {
		t.Fatalf("network creates = %d, want exactly 1", creates)
	}
	if removes != 0 {
		t.Fatalf("network removes = %d, want 0 (the control network is persistent)", removes)
	}

	var req struct {
		Name   string
		Driver string
		Labels map[string]string
	}
	if err := json.Unmarshal(createBody, &req); err != nil {
		t.Fatalf("decode create body: %v\n%s", err, createBody)
	}
	if req.Name != name {
		t.Errorf("create Name = %q, want the derived %q", req.Name, name)
	}
	if req.Driver != controlNetworkBridgeDriver {
		t.Errorf("create Driver = %q, want %q", req.Driver, controlNetworkBridgeDriver)
	}
	wantLabels := controlNetworkLabels(nodeID)
	if len(req.Labels) != len(wantLabels) {
		t.Errorf("create Labels = %v, want exactly %v", req.Labels, wantLabels)
	}
	for k, v := range wantLabels {
		if req.Labels[k] != v {
			t.Errorf("create Labels[%s] = %q, want %q", k, req.Labels[k], v)
		}
	}
}

// TestEnsureControlNetworkCreateIdentity pins the created-network identity
// guards: a create response without an id is refused (Relay will not reuse by
// name blindly), and a name lookup that resolves to a different network than
// the one just created is refused as ambiguous.
func TestEnsureControlNetworkCreateIdentity(t *testing.T) {
	const nodeID = "createid"
	name := controlNetworkName(nodeID)
	fixture := defaultControlNetFixture(name, nodeID, "172.30.0.0/16")
	if _, ok := fixture.withLocalEndpoint(); !ok {
		t.Skip(directRouteSkip)
	}

	t.Run("create response without an id is refused", func(t *testing.T) {
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/networks/" + name,
				body: notFoundNetwork, status: http.StatusNotFound},
			dockerRoute{method: http.MethodPost, path: "/networks/create",
				body: `{"Id":""}`},
		)
		m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: nodeID}
		err := m.EnsureControlNetwork(context.Background())
		if err == nil || !strings.Contains(err.Error(), "create returned no id") {
			t.Fatalf("error = %v, want the missing-created-id refusal", err)
		}
	})

	t.Run("name resolving to a different network than the created one is refused", func(t *testing.T) {
		created := false
		createdBody := fixture.json(t)
		ambiguity := fixture
		ambiguity.id = "some-other-network-id"
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/networks/nid-ctrl",
				serve: func(*http.Request) (int, string) {
					if !created {
						return http.StatusNotFound, notFoundNetwork
					}
					return http.StatusOK, createdBody
				}},
			dockerRoute{method: http.MethodGet, path: "/networks/" + name,
				serve: func(*http.Request) (int, string) {
					if !created {
						return http.StatusNotFound, notFoundNetwork
					}
					return http.StatusOK, ambiguity.json(t)
				}},
			dockerRoute{method: http.MethodPost, path: "/networks/create",
				onMatch: func() { created = true },
				body:    `{"Id":"nid-ctrl"}`},
		)
		m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: nodeID}
		err := m.EnsureControlNetwork(context.Background())
		if err == nil || !strings.Contains(err.Error(), "resolves to a different network") {
			t.Fatalf("error = %v, want the ambiguous-reuse refusal", err)
		}
		if m.controlNetwork != "" {
			t.Fatalf("controlNetwork = %q, want empty after an ambiguous create", m.controlNetwork)
		}
	})
}

// TestEnsureControlNetworkReusesExisting pins reuse without mutation: an
// existing valid network is accepted as-is, never re-created, never removed,
// and a second Ensure is a pure re-validation (the restart path). No container
// route is registered, so any self-container call would fail the test.
func TestEnsureControlNetworkReusesExisting(t *testing.T) {
	const nodeID = "reuse-node"
	name := controlNetworkName(nodeID)
	fixture := defaultControlNetFixture(name, nodeID, "172.30.0.0/16")
	if _, ok := fixture.withLocalEndpoint(); !ok {
		t.Skip(directRouteSkip)
	}

	inspects := 0
	creates := 0
	removes := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{
			method: http.MethodGet, path: "/networks/" + name,
			body:    fixture.json(t),
			onMatch: func() { inspects++ },
		},
		dockerRoute{method: http.MethodPost, path: "/networks/create",
			onMatch: func() { creates++ }, body: `{"Id":"nid-ctrl"}`},
		dockerRoute{method: http.MethodDelete, path: "/networks/",
			onMatch: func() { removes++ }, body: `{}`},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: nodeID}

	if err := m.EnsureControlNetwork(context.Background()); err != nil {
		t.Fatalf("first EnsureControlNetwork: %v", err)
	}
	if err := m.EnsureControlNetwork(context.Background()); err != nil {
		t.Fatalf("second EnsureControlNetwork (restart reuse): %v", err)
	}
	if creates != 0 || removes != 0 {
		t.Fatalf("creates=%d removes=%d, want 0 and 0 (adopt-in-place, never modify or delete)", creates, removes)
	}
	if inspects < 2 {
		t.Fatalf("inspects = %d, want at least 2 (each Ensure revalidates)", inspects)
	}
	if m.controlNetwork != name {
		t.Fatalf("controlNetwork = %q, want %q", m.controlNetwork, name)
	}
}

// TestEnsureControlNetworkRejectsConflictingExistingNetwork pins fail-closed
// reuse: a same-name network missing the execution-control role or any
// ownership label, owned by another node, misconfigured, or renamed by the
// daemon is rejected with an actionable
// error and is NEVER created, adopted, modified, or removed — controlNetwork
// stays empty so execution cannot proceed on it.
func TestEnsureControlNetworkRejectsConflictingExistingNetwork(t *testing.T) {
	const nodeID = "conflict-node"
	name := controlNetworkName(nodeID)

	for _, tc := range []struct {
		name   string
		mutate func(*controlNetFixture)
		want   string
	}{
		{"wrong node owner", func(f *controlNetFixture) { f.labels[labelNodeID] = "another-node" }, "different relay node"},
		{"missing execution-control role", func(f *controlNetFixture) { delete(f.labels, networkRoleLabel) }, "refusing to adopt or modify"},
		{"missing managed marker", func(f *controlNetFixture) { delete(f.labels, controlNetworkManagedLabel) }, "missing the Relay-managed marker"},
		{"wrong driver", func(f *controlNetFixture) { f.driver = "host" }, `requires "bridge"`},
		{"renamed network", func(f *controlNetFixture) { f.name = "not-our-network" }, "refusing to reuse it"},
		{"internal network", func(f *controlNetFixture) { f.internal = true }, "is internal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := defaultControlNetFixture(name, nodeID, "172.30.0.0/16")
			tc.mutate(&f)
			creates := 0
			removes := 0
			cli := newScriptedDockerClient(t,
				dockerRoute{method: http.MethodGet, path: "/networks/" + name, body: f.json(t)},
				dockerRoute{method: http.MethodPost, path: "/networks/create",
					onMatch: func() { creates++ }, body: `{"Id":"nid-ctrl"}`},
				dockerRoute{method: http.MethodDelete, path: "/networks/",
					onMatch: func() { removes++ }, body: `{}`},
			)
			m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: nodeID}

			err := m.EnsureControlNetwork(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.want)
			}
			if creates != 0 || removes != 0 {
				t.Fatalf("creates=%d removes=%d, want 0 and 0 (a conflict is never resolved by modification)", creates, removes)
			}
			if m.controlNetwork != "" {
				t.Fatalf("controlNetwork = %q, want empty after a rejected reuse", m.controlNetwork)
			}
		})
	}
}

// TestEnsureControlNetworkCreateConflictReinspects pins the create race: a
// 409 conflict (another Relay sharing the daemon won the create) is not an
// error — the winner is looked up by name and must pass the SAME strict
// validation plus the direct-route proof. A valid winner is reused; a foreign
// winner fails closed; a conflict with nothing inspectable is an actionable
// error. Non-conflict create/inspect failures surface as genuine errors.
func TestEnsureControlNetworkCreateConflictReinspects(t *testing.T) {
	const nodeID = "race-node"
	name := controlNetworkName(nodeID)
	fixture := defaultControlNetFixture(name, nodeID, "172.30.0.0/16")
	if _, ok := fixture.withLocalEndpoint(); !ok {
		t.Skip(directRouteSkip)
	}
	validBody := fixture.json(t)

	t.Run("conflict with our own valid winner is reused", func(t *testing.T) {
		created := false
		creates := 0
		cli := newScriptedDockerClient(t,
			dockerRoute{
				method: http.MethodGet, path: "/networks/" + name,
				serve: func(*http.Request) (int, string) {
					if !created {
						return http.StatusNotFound, notFoundNetwork
					}
					return http.StatusOK, validBody
				},
			},
			dockerRoute{
				method: http.MethodPost, path: "/networks/create",
				status: http.StatusConflict,
				body:   `{"message":"network with name ` + name + ` already exists"}`,
				onMatch: func() {
					creates++
					// The twin created it first; the re-inspect must see it.
					created = true
				},
			},
		)
		m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: nodeID}
		if err := m.EnsureControlNetwork(context.Background()); err != nil {
			t.Fatalf("EnsureControlNetwork after conflict: %v", err)
		}
		if creates != 1 {
			t.Fatalf("creates = %d, want 1", creates)
		}
		if m.controlNetwork != name {
			t.Fatalf("controlNetwork = %q, want %q", m.controlNetwork, name)
		}
	})

	t.Run("conflict with a foreign winner fails closed without adoption", func(t *testing.T) {
		created := false
		cli := newScriptedDockerClient(t,
			dockerRoute{
				method: http.MethodGet, path: "/networks/" + name,
				serve: func(*http.Request) (int, string) {
					if !created {
						return http.StatusNotFound, notFoundNetwork
					}
					f := defaultControlNetFixture(name, nodeID, "172.30.0.0/16")
					f.labels[labelNodeID] = "someone-else"
					return http.StatusOK, f.json(t)
				},
			},
			dockerRoute{
				method: http.MethodPost, path: "/networks/create",
				status:  http.StatusConflict,
				body:    `{"message":"network with name ` + name + ` already exists"}`,
				onMatch: func() { created = true },
			},
		)
		m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: nodeID}
		err := m.EnsureControlNetwork(context.Background())
		if err == nil || !strings.Contains(err.Error(), "different relay node") {
			t.Fatalf("error = %v, want the ownership-conflict rejection", err)
		}
		if m.controlNetwork != "" {
			t.Fatalf("controlNetwork = %q, want empty (no adoption)", m.controlNetwork)
		}
	})

	t.Run("conflict with nothing inspectable is actionable", func(t *testing.T) {
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/networks/" + name,
				body: notFoundNetwork, status: http.StatusNotFound},
			dockerRoute{method: http.MethodPost, path: "/networks/create",
				status: http.StatusConflict, body: `{"message":"already exists"}`},
		)
		m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: nodeID}
		err := m.EnsureControlNetwork(context.Background())
		if err == nil || !strings.Contains(err.Error(), "not inspectable") {
			t.Fatalf("error = %v, want the not-inspectable failure", err)
		}
	})

	t.Run("create failure surfaces as a create error", func(t *testing.T) {
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/networks/" + name,
				body: notFoundNetwork, status: http.StatusNotFound},
			dockerRoute{method: http.MethodPost, path: "/networks/create",
				status: http.StatusInternalServerError, body: `{"message":"daemon exploded"}`},
		)
		m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: nodeID}
		err := m.EnsureControlNetwork(context.Background())
		if err == nil || !strings.Contains(err.Error(), "create:") {
			t.Fatalf("error = %v, want the create-step wrapper", err)
		}
	})

	t.Run("inspect failure is never mistaken for absence", func(t *testing.T) {
		creates := 0
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/networks/" + name,
				body: `{"message":"daemon exploded"}`, status: http.StatusInternalServerError},
			dockerRoute{method: http.MethodPost, path: "/networks/create",
				onMatch: func() { creates++ }, body: `{"Id":"nid-ctrl"}`},
		)
		m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: nodeID}
		err := m.EnsureControlNetwork(context.Background())
		if err == nil || !strings.Contains(err.Error(), "inspect:") {
			t.Fatalf("error = %v, want the inspect-step wrapper", err)
		}
		if creates != 0 {
			t.Fatalf("creates = %d, want 0 (an unreadable network is not a missing network)", creates)
		}
	})
}

// TestEnsureControlNetworkRejectsInvalidNodeID pins that a NODE_ID unfit for a
// network name fails before any daemon call: no route is registered, so any
// request would fail the test through the harness.
func TestEnsureControlNetworkRejectsInvalidNodeID(t *testing.T) {
	for _, nodeID := range []string{"", "bad/node", "bad node", strings.Repeat("a", maxControlNodeIDLen+1)} {
		m := newControlManager(t, nodeID)
		err := m.EnsureControlNetwork(context.Background())
		if err == nil {
			t.Fatalf("nodeID %q must be rejected", nodeID)
		}
		if m.controlNetwork != "" {
			t.Fatalf("controlNetwork = %q after rejected nodeID, want empty", m.controlNetwork)
		}
	}
}

// TestEnsureControlNetworkLifecycle pins the operation lifecycle: a nil manager
// or missing client is an actionable error, a cancelled caller context is
// refused before any daemon call, and a closed Manager refuses provisioning
// through the same operation gate as every other Docker operation — no request
// is issued after close. Nothing is ever removed (the harness would fail on any
// unregistered DELETE).
func TestEnsureControlNetworkLifecycle(t *testing.T) {
	t.Run("nil manager and missing client fail closed", func(t *testing.T) {
		var nilManager *Manager
		if err := nilManager.EnsureControlNetwork(context.Background()); err == nil ||
			!strings.Contains(err.Error(), "docker client unavailable") {
			t.Fatalf("nil manager error = %v, want the client-unavailable failure", err)
		}
		withoutClient := &Manager{log: testutil.DiscardLogger(), nodeID: "n1"}
		if err := withoutClient.EnsureControlNetwork(context.Background()); err == nil ||
			!strings.Contains(err.Error(), "docker client unavailable") {
			t.Fatalf("clientless manager error = %v, want the client-unavailable failure", err)
		}
	})

	t.Run("cancelled context issues no daemon call", func(t *testing.T) {
		const nodeID = "cancel-node"
		name := controlNetworkName(nodeID)
		requests := 0
		m := newControlManager(t, nodeID, dockerRoute{
			method: http.MethodGet, path: "/networks/" + name,
			body:    defaultControlNetFixture(name, nodeID, "172.30.0.0/16").json(t),
			onMatch: func() { requests++ },
		})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := m.EnsureControlNetwork(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
		if requests != 0 {
			t.Fatalf("daemon requests = %d, want 0 for a cancelled caller", requests)
		}
	})

	t.Run("closed manager refuses provisioning with no request", func(t *testing.T) {
		const nodeID = "closed-node"
		name := controlNetworkName(nodeID)
		requests := 0
		m := newControlManager(t, nodeID, dockerRoute{
			method: http.MethodGet, path: "/networks/" + name,
			body:    defaultControlNetFixture(name, nodeID, "172.30.0.0/16").json(t),
			onMatch: func() { requests++ },
		})
		if err := m.CloseContext(context.Background()); err != nil {
			t.Fatalf("CloseContext: %v", err)
		}
		err := m.EnsureControlNetwork(context.Background())
		if !errors.Is(err, ErrManagerShuttingDown) {
			t.Fatalf("error = %v, want ErrManagerShuttingDown", err)
		}
		if requests != 0 {
			t.Fatalf("daemon requests = %d, want 0 after close", requests)
		}
	})
}

// TestEnsureControlNetworkAttachesSelfContainer pins the containerized Relay
// path against the scripted daemon: only the verified Docker-default
// container-ID hostname is inspected, NetworkConnect targets the VALIDATED
// network id (a name-based connect would hit an unregistered route and fail
// the test) and the full container id, attachment is idempotent when the
// endpoint already exists, a benign connect race is resolved by re-inspection,
// and every identity failure (foreign hostname, id-prefix mismatch, missing
// container, no id) fails closed with zero connects. NODE_ID is never used to
// resolve a container: no route exists for inspecting it as an id.
func TestEnsureControlNetworkAttachesSelfContainer(t *testing.T) {
	const nodeID = "cself"
	name := controlNetworkName(nodeID)
	const self = "abcdef012345"
	fullID := self + strings.Repeat("f", 52)

	type state struct {
		attached       bool
		hostname       string // served Config.Hostname
		containerID    string // served Id
		missing        bool   // serve 404 instead of the container
		endpointNetID  string // NetworkID served on the endpoint once attached
		noAddress      bool   // attached but without a usable address
		inspectCalls   int
		connectCalls   int
		connectStatus  int  // 0 means 200
		keepUnattached bool // a successful connect still leaves no endpoint
	}
	newState := func() *state {
		return &state{
			hostname:      self,
			containerID:   fullID,
			endpointNetID: "nid-ctrl",
		}
	}
	// routes wires the scripted daemon around st. The connect route exists ONLY
	// at the validated network id, so a name-based NetworkConnect would be an
	// unhandled request and fail the test.
	routes := func(t *testing.T, st *state, creates *int) []dockerRoute {
		t.Helper()
		return []dockerRoute{
			dockerRoute{method: http.MethodGet, path: "/networks/" + name,
				body: defaultControlNetFixture(name, nodeID, "172.30.0.0/16").json(t)},
			dockerRoute{method: http.MethodPost, path: "/networks/create",
				onMatch: func() { *creates++ }, body: `{"Id":"nid-ctrl"}`},
			dockerRoute{
				method: http.MethodGet, path: "/containers/",
				serve: func(*http.Request) (int, string) {
					st.inspectCalls++
					if st.missing {
						return http.StatusNotFound, `{"message":"no such container"}`
					}
					endpoints := "{}"
					if st.attached {
						addr := `"IPAddress":"172.30.0.5"`
						if st.noAddress {
							addr = `"IPAddress":""`
						}
						endpoints = fmt.Sprintf(
							`{%q: {%s, "NetworkID": %q}}`, name, addr, st.endpointNetID)
					}
					return http.StatusOK, fmt.Sprintf(
						`{"Id":%q,"Config":{"Hostname":%q},"NetworkSettings":{"Networks":%s}}`,
						st.containerID, st.hostname, endpoints)
				},
			},
			dockerRoute{
				method: http.MethodPost, path: "/networks/nid-ctrl/connect",
				onMatch: func() { st.connectCalls++ },
				serve: func(req *http.Request) (int, string) {
					// A successful attach lands the endpoint on OUR network id
					// with a daemon-assigned address; a failing connect only
					// lands it when the benign race already did so.
					setAttached := func() {
						st.attached = true
						st.endpointNetID = "nid-ctrl"
						st.noAddress = false
					}
					if st.connectStatus != 0 {
						if !st.keepUnattached {
							// Benign race: the endpoint landed despite our
							// connect failing.
							setAttached()
						}
						return st.connectStatus, `{"message":"scripted connect failure"}`
					}
					if !st.keepUnattached {
						setAttached()
					}
					return http.StatusOK, "{}"
				},
			},
		}
	}
	// newSelf builds the manager in containerized mode (verified default
	// container-ID hostname), which is the ONLY way any container route may be
	// reached.
	newSelf := func(t *testing.T, st *state, creates *int) *Manager {
		t.Helper()
		return newControlManagerWithSelf(t, nodeID, self, routes(t, st, creates)...)
	}

	t.Run("attaches a missing self endpoint by network id", func(t *testing.T) {
		st := newState()
		creates := 0
		m := newSelf(t, st, &creates)
		if err := m.EnsureControlNetwork(context.Background()); err != nil {
			t.Fatalf("EnsureControlNetwork: %v", err)
		}
		if st.connectCalls != 1 {
			t.Fatalf("connects = %d, want exactly 1", st.connectCalls)
		}
		if creates != 0 {
			t.Fatalf("creates = %d, want 0 (the network already exists)", creates)
		}
		if m.controlNetwork != name {
			t.Fatalf("controlNetwork = %q, want %q", m.controlNetwork, name)
		}
		if m.controlSelfID != fullID {
			t.Fatalf("controlSelfID = %q, want the full inspected id %q", m.controlSelfID, fullID)
		}
	})

	t.Run("already attached is idempotent", func(t *testing.T) {
		st := newState()
		st.attached = true
		creates := 0
		m := newSelf(t, st, &creates)
		if err := m.EnsureControlNetwork(context.Background()); err != nil {
			t.Fatalf("EnsureControlNetwork: %v", err)
		}
		if st.connectCalls != 0 {
			t.Fatalf("connects = %d, want 0 (already a member)", st.connectCalls)
		}
		if st.inspectCalls != 1 {
			t.Fatalf("self inspects = %d, want 1 (no re-inspect needed once attached)", st.inspectCalls)
		}
	})

	t.Run("endpoint on a replaced network is re-attached by id", func(t *testing.T) {
		st := newState()
		st.attached = true
		st.endpointNetID = "stale-network-id"
		creates := 0
		m := newSelf(t, st, &creates)
		if err := m.EnsureControlNetwork(context.Background()); err != nil {
			t.Fatalf("EnsureControlNetwork: %v", err)
		}
		if st.connectCalls != 1 {
			t.Fatalf("connects = %d, want 1 (a same-named stale endpoint is not trusted)", st.connectCalls)
		}
	})

	t.Run("connect error resolved by re-inspection succeeds", func(t *testing.T) {
		st := newState()
		st.connectStatus = http.StatusConflict // e.g. already-attached race
		creates := 0
		m := newSelf(t, st, &creates)
		if err := m.EnsureControlNetwork(context.Background()); err != nil {
			t.Fatalf("EnsureControlNetwork must trust the proven endpoint: %v", err)
		}
		if st.connectCalls != 1 {
			t.Fatalf("connects = %d, want 1", st.connectCalls)
		}
	})

	t.Run("connect error with no endpoint surfaces the attach failure", func(t *testing.T) {
		st := newState()
		st.connectStatus = http.StatusInternalServerError
		st.keepUnattached = true
		creates := 0
		m := newSelf(t, st, &creates)
		err := m.EnsureControlNetwork(context.Background())
		if err == nil || !strings.Contains(err.Error(), "attach Relay's own container") {
			t.Fatalf("error = %v, want the attach failure", err)
		}
		if m.controlNetwork != "" {
			t.Fatalf("controlNetwork = %q, want empty after a failed attach", m.controlNetwork)
		}
	})

	t.Run("connect succeeded but endpoint still unusable is actionable", func(t *testing.T) {
		st := newState()
		st.keepUnattached = true
		creates := 0
		m := newSelf(t, st, &creates)
		err := m.EnsureControlNetwork(context.Background())
		if err == nil || !strings.Contains(err.Error(), "no usable address after attach") {
			t.Fatalf("error = %v, want the post-attach address failure", err)
		}
	})

	t.Run("attached endpoint without a usable address is re-attached", func(t *testing.T) {
		st := newState()
		st.attached = true
		st.noAddress = true
		st.endpointNetID = ""
		creates := 0
		m := newSelf(t, st, &creates)
		if err := m.EnsureControlNetwork(context.Background()); err != nil {
			t.Fatalf("EnsureControlNetwork: %v", err)
		}
		if st.connectCalls != 1 {
			t.Fatalf("connects = %d, want 1 (an address-less endpoint is not a member)", st.connectCalls)
		}
	})

	t.Run("foreign container is never attached", func(t *testing.T) {
		st := newState()
		st.hostname = "intruder-container"
		creates := 0
		m := newSelf(t, st, &creates)
		err := m.EnsureControlNetwork(context.Background())
		if err == nil || !strings.Contains(err.Error(), "not Relay; refusing to attach it") {
			t.Fatalf("error = %v, want the foreign-container refusal", err)
		}
		if st.connectCalls != 0 {
			t.Fatalf("connects = %d, want 0 (never attach a container that is not Relay)", st.connectCalls)
		}
	})

	t.Run("id-prefix mismatch is never attached", func(t *testing.T) {
		st := newState()
		st.containerID = strings.Repeat("0", 64) // hostname does not prefix it
		creates := 0
		m := newSelf(t, st, &creates)
		err := m.EnsureControlNetwork(context.Background())
		if err == nil || !strings.Contains(err.Error(), "not Relay; refusing to attach it") {
			t.Fatalf("error = %v, want the identity refusal", err)
		}
		if st.connectCalls != 0 {
			t.Fatalf("connects = %d, want 0", st.connectCalls)
		}
	})

	t.Run("missing self container fails closed without connect", func(t *testing.T) {
		st := newState()
		st.missing = true
		creates := 0
		m := newSelf(t, st, &creates)
		err := m.EnsureControlNetwork(context.Background())
		if err == nil || !strings.Contains(err.Error(), "cannot inspect Relay's own container") {
			t.Fatalf("error = %v, want the self-inspect failure", err)
		}
		if st.connectCalls != 0 {
			t.Fatalf("connects = %d, want 0", st.connectCalls)
		}
	})

	t.Run("self container without an id fails closed", func(t *testing.T) {
		st := newState()
		st.containerID = ""
		st.hostname = "relaycustom"   // non-ID hostname: only reachable via the
		creates := 0                  // test-only containerDetect override below,
		m := newSelf(t, st, &creates) // past the default ID-like precondition
		m.selfHostname = "relaycustom"
		m.containerDetect = func() bool { return true }
		err := m.EnsureControlNetwork(context.Background())
		if err == nil || !strings.Contains(err.Error(), "has no id") {
			t.Fatalf("error = %v, want the missing-id failure", err)
		}
		if st.connectCalls != 0 {
			t.Fatalf("connects = %d, want 0", st.connectCalls)
		}
	})
}

// TestEnsureControlNetworkCustomHostnameFailsClosed pins the
// non-container-ID-hostname path: Relay never inspects or connects ANY
// container (no container route is registered, so any call would fail the test
// through the harness), and provisioning succeeds only with an EXACT direct
// route proof — here the pre-attached endpoint matching this host's real
// interface. A network whose endpoints match nothing local (the Docker Desktop
// host-process / remote Engine case) fails closed with actionable guidance, and
// a network without an IPAM subnet cannot be proven at all.
func TestEnsureControlNetworkCustomHostnameFailsClosed(t *testing.T) {
	const nodeID = "custom-host-node"
	name := controlNetworkName(nodeID)

	t.Run("network with no matching local endpoint fails closed with guidance", func(t *testing.T) {
		m := newControlManagerWithSelf(t, nodeID, "relay-custom-hostname", networkInspectRoute(
			"/networks/"+name,
			defaultControlNetFixture(name, nodeID, "172.30.0.0/16").json(t)))
		err := m.EnsureControlNetwork(context.Background())
		if err == nil || !strings.Contains(err.Error(), routeProofFailure) {
			t.Fatalf("error = %v, want the unprovable-route failure", err)
		}
		for _, want := range []string{"Docker Desktop host processes", "Linux host"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not guide the operator with %q", err, want)
			}
		}
		if m.controlNetwork != "" {
			t.Fatalf("controlNetwork = %q, want empty (fail closed)", m.controlNetwork)
		}
	})

	t.Run("pre-attached endpoint proves the direct route without container calls", func(t *testing.T) {
		f := defaultControlNetFixture(name, nodeID, "172.30.0.0/16")
		matched, ok := f.withLocalEndpoint()
		if !ok {
			t.Skip(directRouteSkip)
		}
		m := newControlManagerWithSelf(t, nodeID, "relay-custom-hostname",
			networkInspectRoute("/networks/"+name, f.json(t)))
		if err := m.EnsureControlNetwork(context.Background()); err != nil {
			t.Fatalf("EnsureControlNetwork (direct-route mode): %v", err)
		}
		if m.controlNetwork != name {
			t.Fatalf("controlNetwork = %q, want %q", m.controlNetwork, name)
		}
		if m.controlSelfID != "" {
			t.Fatalf("controlSelfID = %q, want empty in direct-route mode", m.controlSelfID)
		}
		t.Logf("direct route proven via the pre-attached endpoint %s", matched)
	})

	t.Run("network without an IPAM subnet cannot be proven", func(t *testing.T) {
		m := newControlManagerWithSelf(t, nodeID, "relay-custom-hostname",
			networkInspectRoute("/networks/"+name, defaultControlNetFixture(name, nodeID, "").json(t)))
		err := m.EnsureControlNetwork(context.Background())
		if err == nil || !strings.Contains(err.Error(), "no IPAM subnet") {
			t.Fatalf("error = %v, want the missing-subnet failure", err)
		}
	})
}

// TestEnsureControlNetworkNeverInspectsNodeIdentity pins the cross-identity
// guard: even when NODE_ID itself looks exactly like a container ID, it is
// only a network-name key. With no verified OCI hostname no container route
// exists, so any inspect/connect by the node identity would fail the test —
// whether provisioning ends in a proven direct route or a fail-closed proof.
func TestEnsureControlNetworkNeverInspectsNodeIdentity(t *testing.T) {
	const nodeID = "abcdef012345" // ID-looking ownership identity
	name := controlNetworkName(nodeID)

	for _, selfHostname := range []string{"", "my-laptop"} {
		f := defaultControlNetFixture(name, nodeID, "172.30.0.0/16")
		f.withLocalEndpoint() // best effort: the proof below holds either way
		m := newControlManagerWithSelf(t, nodeID, selfHostname,
			networkInspectRoute("/networks/"+name, f.json(t)))
		// Either success (proven route) or the fail-closed route proof is
		// acceptable here; what must NEVER happen is a container request —
		// and no container route is registered, so the harness enforces it.
		_ = m.EnsureControlNetwork(context.Background())
		if m.controlSelfID != "" {
			t.Fatalf("selfHostname %q: controlSelfID = %q, want empty (node identity is never a container)", selfHostname, m.controlSelfID)
		}
		if m.controlNetwork != "" && m.controlNetwork != name {
			t.Fatalf("controlNetwork = %q, want empty or %q", m.controlNetwork, name)
		}
	}
}

// TestVerifyControlNetwork pins the steady-state readiness check: an
// unprovisioned manager fails without any daemon call; a provisioned one
// re-validates ownership/configuration, re-proves the exact direct route, and —
// in containerized mode — re-proves the recorded self endpoint without ever
// re-attaching or creating. Deletion, reconfiguration, a lost route, or a
// detached self container each stop verification, and a closed manager is
// refused through the operation gate.
func TestVerifyControlNetwork(t *testing.T) {
	t.Run("unprovisioned fails with no daemon call", func(t *testing.T) {
		m := newControlManager(t, "no-prov") // no routes at all
		err := m.VerifyControlNetwork(context.Background())
		if err == nil || !strings.Contains(err.Error(), "not provisioned") {
			t.Fatalf("error = %v, want the not-provisioned failure", err)
		}
	})

	t.Run("direct-route mode re-proves the exact local route", func(t *testing.T) {
		const nodeID = "verify-direct"
		name := controlNetworkName(nodeID)
		f := defaultControlNetFixture(name, nodeID, "172.30.0.0/16")
		if _, ok := f.withLocalEndpoint(); !ok {
			t.Skip(directRouteSkip)
		}
		matchingBody := f.json(t)
		stripped := defaultControlNetFixture(name, nodeID, "172.30.0.0/16") // no endpoints
		matching := true
		cli := newScriptedDockerClient(t, dockerRoute{
			method: http.MethodGet, path: "/networks/" + name,
			serve: func(*http.Request) (int, string) {
				if matching {
					return http.StatusOK, matchingBody
				}
				return http.StatusOK, stripped.json(t)
			},
		})
		m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: nodeID}
		if err := m.EnsureControlNetwork(context.Background()); err != nil {
			t.Fatalf("EnsureControlNetwork: %v", err)
		}
		if err := m.VerifyControlNetwork(context.Background()); err != nil {
			t.Fatalf("VerifyControlNetwork with the pre-attached endpoint: %v", err)
		}
		// The route proof disappears (endpoint gone: container detached, or
		// the network moved to an unreachable engine).
		matching = false
		err := m.VerifyControlNetwork(context.Background())
		if err == nil || !strings.Contains(err.Error(), routeProofFailure) {
			t.Fatalf("error = %v, want the lost-route failure", err)
		}
	})

	t.Run("deleted network stops verification", func(t *testing.T) {
		const nodeID = "verify-gone"
		name := controlNetworkName(nodeID)
		f := defaultControlNetFixture(name, nodeID, "172.30.0.0/16")
		if _, ok := f.withLocalEndpoint(); !ok {
			t.Skip(directRouteSkip)
		}
		exists := true
		cli := newScriptedDockerClient(t, dockerRoute{
			method: http.MethodGet, path: "/networks/" + name,
			serve: func(*http.Request) (int, string) {
				if !exists {
					return http.StatusNotFound, notFoundNetwork
				}
				return http.StatusOK, f.json(t)
			},
		})
		m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: nodeID}
		if err := m.EnsureControlNetwork(context.Background()); err != nil {
			t.Fatalf("EnsureControlNetwork: %v", err)
		}
		exists = false
		err := m.VerifyControlNetwork(context.Background())
		if err == nil || !strings.Contains(err.Error(), "no longer exists") {
			t.Fatalf("error = %v, want the deletion failure", err)
		}
	})

	t.Run("reconfigured network stops verification", func(t *testing.T) {
		const nodeID = "verify-reconfig"
		name := controlNetworkName(nodeID)
		f := defaultControlNetFixture(name, nodeID, "172.30.0.0/16")
		if _, ok := f.withLocalEndpoint(); !ok {
			t.Skip(directRouteSkip)
		}
		owner := nodeID
		cli := newScriptedDockerClient(t, dockerRoute{
			method: http.MethodGet, path: "/networks/" + name,
			serve: func(*http.Request) (int, string) {
				current := defaultControlNetFixture(name, owner, "172.30.0.0/16")
				current.containersJSON = f.containersJSON
				return http.StatusOK, current.json(t)
			},
		})
		m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: nodeID}
		if err := m.EnsureControlNetwork(context.Background()); err != nil {
			t.Fatalf("EnsureControlNetwork: %v", err)
		}
		owner = "another-node"
		err := m.VerifyControlNetwork(context.Background())
		if err == nil || !strings.Contains(err.Error(), "different relay node") {
			t.Fatalf("error = %v, want the ownership-revalidation failure", err)
		}
	})

	t.Run("containerized mode re-proves the self endpoint read-only", func(t *testing.T) {
		const nodeID = "verify-self"
		name := controlNetworkName(nodeID)
		const self = "abcdef012345"
		fullID := self + strings.Repeat("f", 52)
		attached := true
		hostname := self
		creates := 0
		// Readiness must never re-attach or create: NO connect route and a
		// create route that only counts are registered, so any connect fails
		// the test through the harness and creates must stay 0.
		m := newControlManagerWithSelf(t, nodeID, self,
			dockerRoute{method: http.MethodGet, path: "/networks/" + name,
				body: defaultControlNetFixture(name, nodeID, "172.30.0.0/16").json(t)},
			dockerRoute{method: http.MethodPost, path: "/networks/create",
				onMatch: func() { creates++ }, body: `{"Id":"nid-ctrl"}`},
			dockerRoute{
				method: http.MethodGet, path: "/containers/",
				serve: func(*http.Request) (int, string) {
					endpoints := "{}"
					if attached {
						endpoints = fmt.Sprintf(
							`{%q: {"IPAddress":"172.30.0.5","NetworkID":"nid-ctrl"}}`, name)
					}
					return http.StatusOK, fmt.Sprintf(
						`{"Id":%q,"Config":{"Hostname":%q},"NetworkSettings":{"Networks":%s}}`,
						fullID, hostname, endpoints)
				},
			},
		)
		if err := m.EnsureControlNetwork(context.Background()); err != nil {
			t.Fatalf("EnsureControlNetwork: %v", err)
		}
		if err := m.VerifyControlNetwork(context.Background()); err != nil {
			t.Fatalf("VerifyControlNetwork while attached: %v", err)
		}
		if creates != 0 {
			t.Fatalf("network creates = %d, want 0 (Verify is read-only)", creates)
		}
		attached = false
		err := m.VerifyControlNetwork(context.Background())
		if err == nil || !strings.Contains(err.Error(), "no longer attached with a usable address") {
			t.Fatalf("error = %v, want the detachment failure", err)
		}
		attached = true
		hostname = "replaced-container"
		err = m.VerifyControlNetwork(context.Background())
		if err == nil || !strings.Contains(err.Error(), "no longer matches this process") {
			t.Fatalf("error = %v, want the replaced-identity failure", err)
		}
	})

	t.Run("closed manager refuses verification with no request", func(t *testing.T) {
		const nodeID = "verify-closed"
		name := controlNetworkName(nodeID)
		f := defaultControlNetFixture(name, nodeID, "172.30.0.0/16")
		if _, ok := f.withLocalEndpoint(); !ok {
			t.Skip(directRouteSkip)
		}
		requests := 0
		m := newControlManager(t, nodeID, dockerRoute{
			method: http.MethodGet, path: "/networks/" + name,
			body:    f.json(t),
			onMatch: func() { requests++ },
		})
		if err := m.EnsureControlNetwork(context.Background()); err != nil {
			t.Fatalf("EnsureControlNetwork: %v", err)
		}
		baseline := requests
		if err := m.CloseContext(context.Background()); err != nil {
			t.Fatalf("CloseContext: %v", err)
		}
		err := m.VerifyControlNetwork(context.Background())
		if !errors.Is(err, ErrManagerShuttingDown) {
			t.Fatalf("error = %v, want ErrManagerShuttingDown", err)
		}
		if requests != baseline {
			t.Fatalf("daemon requests grew from %d to %d after close", baseline, requests)
		}
	})
}

// TestExecutionNetworksComposition pins the execution join set: the control
// network is ALWAYS present (even with NETWORKS unset), the optional app set is
// merged and canonicalized, a caller-supplied duplicate of the control name
// collapses to one endpoint, and an unprovisioned control identity fails
// instead of silently falling back to NETWORKS.
func TestExecutionNetworksComposition(t *testing.T) {
	t.Run("unprovisioned control fails instead of falling back", func(t *testing.T) {
		m := &Manager{log: testutil.DiscardLogger(), networks: []string{"backend"}}
		if _, err := m.executionNetworks(); err == nil ||
			!strings.Contains(err.Error(), "not provisioned") {
			t.Fatalf("executionNetworks error = %v, want the not-provisioned failure", err)
		}
	})

	t.Run("control only when NETWORKS unset", func(t *testing.T) {
		m := &Manager{log: testutil.DiscardLogger(), controlNetwork: unitControlNetwork}
		got, err := m.executionNetworks()
		if err != nil {
			t.Fatalf("executionNetworks: %v", err)
		}
		if len(got) != 1 || got[0] != unitControlNetwork {
			t.Fatalf("networks = %v, want exactly [%s]", got, unitControlNetwork)
		}
	})

	t.Run("control merged with the optional app set", func(t *testing.T) {
		m := &Manager{
			log:            testutil.DiscardLogger(),
			controlNetwork: unitControlNetwork,
			networks:       []string{"backend", "frontend"},
		}
		got, err := m.executionNetworks()
		if err != nil {
			t.Fatalf("executionNetworks: %v", err)
		}
		want := []string{"backend", "frontend", unitControlNetwork}
		if len(got) != len(want) {
			t.Fatalf("networks = %v, want the canonical %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("networks = %v, want the canonical %v", got, want)
			}
		}
	})

	t.Run("app set listing the control network joins it once", func(t *testing.T) {
		m := &Manager{
			log:            testutil.DiscardLogger(),
			controlNetwork: unitControlNetwork,
			networks:       []string{unitControlNetwork, "backend"},
		}
		got, err := m.executionNetworks()
		if err != nil {
			t.Fatalf("executionNetworks: %v", err)
		}
		count := 0
		for _, n := range got {
			if n == unitControlNetwork {
				count++
			}
		}
		if count != 1 || len(got) != 2 {
			t.Fatalf("networks = %v, want the control network exactly once beside backend", got)
		}
	})
}

// TestValidateControlForExecution pins the pre-create revalidation: an
// unprovisioned control identity refuses execution (no NETWORKS fallback), a
// deleted network refuses with the explicit never-create message before any
// container create, an altered network refuses, and a healthy one passes. An
// inspect failure is a genuine error, never mistaken for absence.
func TestValidateControlForExecution(t *testing.T) {
	t.Run("unprovisioned refuses with no daemon call", func(t *testing.T) {
		m := newControlManager(t, "no-prov-exec") // no routes
		m.networks = []string{"backend"}          // must NOT act as a fallback
		err := m.validateControlForExecution(context.Background())
		if err == nil || !strings.Contains(err.Error(), "not provisioned") {
			t.Fatalf("error = %v, want the not-provisioned failure", err)
		}
	})

	for _, tc := range []struct {
		name       string
		serve      func(*http.Request) (int, string)
		want       string
		wantAbsent bool
	}{
		{
			name:  "healthy network passes",
			serve: func(*http.Request) (int, string) { return 0, "" }, // static fixture fallback
			want:  "",
		},
		{
			name: "deleted network refuses before create",
			serve: func(*http.Request) (int, string) {
				return http.StatusNotFound, notFoundNetwork
			},
			want:       "no longer exists; refusing to create an execution container",
			wantAbsent: true,
		},
		{
			name: "altered network refuses",
			serve: func(*http.Request) (int, string) {
				f := defaultControlNetFixture(unitControlNetwork, "other-node", "172.30.0.0/16")
				return http.StatusOK, f.json(t)
			},
			want:       "different relay node",
			wantAbsent: true,
		},
		{
			name: "inspect failure is a genuine error",
			serve: func(*http.Request) (int, string) {
				return http.StatusInternalServerError, `{"message":"daemon exploded"}`
			},
			want:       "inspect:",
			wantAbsent: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			creates := 0
			cli := newScriptedDockerClient(t, dockerRoute{
				method: http.MethodGet, path: "/networks/" + unitControlNetwork,
				serve: tc.serve,
				body:  defaultControlNetFixture(unitControlNetwork, "unit-node", "172.30.0.0/16").json(t),
			}, dockerRoute{
				method: http.MethodPost, path: "/containers/create",
				onMatch: func() { creates++ }, body: `{"Id":"cid-1"}`,
			})
			m := &Manager{
				cli:            cli,
				log:            testutil.DiscardLogger(),
				nodeID:         "unit-node",
				controlNetwork: unitControlNetwork,
			}
			err := m.validateControlForExecution(context.Background())
			if tc.want == "" {
				if err != nil {
					t.Fatalf("validateControlForExecution = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.want)
			}
			if tc.wantAbsent && creates != 0 {
				t.Fatalf("container creates = %d, want 0", creates)
			}
		})
	}
}

// TestStartContainerJoinsControlNetworkPlusNetworks drives the REAL Manager
// execution create path (workload-network guard + control revalidation +
// composed endpoint set) against the scripted daemon and inspects the actual
// serialized /containers/create request. The control network is ALWAYS an
// endpoint — even with NETWORKS unset — and the optional app networks join
// beside it. A deleted control network or an unprovisioned manager refuses
// before any container create, and a user-selected network carrying only the
// execution-control role in NETWORKS is still rejected even when this
// manager's own control network is healthy. No network is ever created by the
// create path.
func TestStartContainerJoinsControlNetworkPlusNetworks(t *testing.T) {
	const nodeID = "unit-node"
	name := controlNetworkName(nodeID)

	// controlRoute serves this manager's healthy control network.
	controlRoute := func() dockerRoute {
		return networkInspectRoute("/networks/"+name,
			defaultControlNetFixture(name, nodeID, "172.30.0.0/16").json(t))
	}
	newSUT := func(t *testing.T, routes ...dockerRoute) (*Manager, *int) {
		t.Helper()
		networkCreates := 0
		routes = append(routes, dockerRoute{
			method: http.MethodPost, path: "/networks/create",
			onMatch: func() { networkCreates++ }, body: `{"Id":"nid"}`,
		})
		m := &Manager{
			cli:            newScriptedDockerClient(t, routes...),
			log:            testutil.DiscardLogger(),
			nodeID:         nodeID,
			controlNetwork: name,
		}
		return m, &networkCreates
	}
	// failingCreate scripts image inspect + a create failure so the test can
	// capture the exact request body without starting anything. The create path
	// pins the immutable ID before create, so the pinned-ID re-inspect is
	// modeled too; the default ImageSave route validates the baseline /tmp.
	failingCreate := func(body *[]byte) []dockerRoute {
		return []dockerRoute{
			imageWithoutDeclaredVolumesRoute("img"),
			imageWithoutDeclaredVolumesRoute("sha256:clean"),
			dockerRoute{
				method: http.MethodPost, path: "/containers/create",
				status: http.StatusInternalServerError,
				body:   `{"message":"scripted create failure"}`,
				onBody: func(b []byte) { *body = append([]byte(nil), b...) },
			},
		}
	}

	t.Run("NETWORKS unset still joins the control network", func(t *testing.T) {
		var createBody []byte
		m, networkCreates := newSUT(t, append(failingCreate(&createBody), controlRoute())...)
		_, err := m.startContainer(context.Background(), "fn",
			resolvedImage{ref: "img"}, nil, app.ResourceLimits{}, RunMeta{})
		if err == nil {
			t.Fatal("expected the scripted create failure")
		}
		if len(createBody) == 0 {
			t.Fatal("no /containers/create body captured")
		}
		req := decodeCreateRequest(t, createBody)
		if req.NetworkingConfig == nil {
			t.Fatal("execution create has no NetworkingConfig; the control network must always join")
		}
		got := endpointKeys(req.NetworkingConfig.EndpointsConfig)
		if len(got) != 1 || got[0] != name {
			t.Fatalf("endpoints = %v, want exactly [%s]", got, name)
		}
		if *networkCreates != 0 {
			t.Fatalf("network creates = %d, want 0", *networkCreates)
		}
	})

	t.Run("control plus the optional NETWORKS set", func(t *testing.T) {
		var createBody []byte
		m, networkCreates := newSUT(t,
			append(failingCreate(&createBody), controlRoute(),
				networkInspectRoute("/networks/backend", `{"Name":"backend","Labels":{}}`),
				networkInspectRoute("/networks/frontend", `{"Name":"frontend","Labels":{}}`))...)
		m.networks = []string{"backend", "frontend"}
		_, err := m.startContainer(context.Background(), "fn",
			resolvedImage{ref: "img"}, nil, app.ResourceLimits{}, RunMeta{})
		if err == nil {
			t.Fatal("expected the scripted create failure")
		}
		req := decodeCreateRequest(t, createBody)
		got := endpointKeys(req.NetworkingConfig.EndpointsConfig)
		want := []string{"backend", "frontend", name}
		if len(got) != len(want) {
			t.Fatalf("endpoints = %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("endpoints = %v, want %v", got, want)
			}
		}
		if *networkCreates != 0 {
			t.Fatalf("network creates = %d, want 0", *networkCreates)
		}
	})

	t.Run("deleted control network refuses before any container create", func(t *testing.T) {
		creates := 0
		m, networkCreates := newSUT(t,
			dockerRoute{method: http.MethodGet, path: "/networks/" + name,
				status: http.StatusNotFound, body: notFoundNetwork},
			dockerRoute{method: http.MethodPost, path: "/containers/create",
				onMatch: func() { creates++ }, body: `{"Id":"cid-1"}`},
		)
		_, err := m.startContainer(context.Background(), "fn",
			resolvedImage{ref: "img"}, nil, app.ResourceLimits{}, RunMeta{})
		if err == nil || !strings.Contains(err.Error(), "no longer exists; refusing to create an execution container") {
			t.Fatalf("error = %v, want the fail-closed control deletion refusal", err)
		}
		if creates != 0 {
			t.Fatalf("container creates = %d, want 0", creates)
		}
		if *networkCreates != 0 {
			t.Fatalf("network creates = %d, want 0 (Relay never recreates the control network ad hoc)", *networkCreates)
		}
	})

	t.Run("unprovisioned manager refuses execution without a fallback", func(t *testing.T) {
		creates := 0
		cli := newScriptedDockerClient(t,
			networkInspectRoute("/networks/backend", `{"Name":"backend","Labels":{}}`),
			dockerRoute{method: http.MethodPost, path: "/containers/create",
				onMatch: func() { creates++ }, body: `{"Id":"cid-1"}`},
		)
		m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: nodeID,
			networks: []string{"backend"}} // no controlNetwork: must not fall back
		_, err := m.startContainer(context.Background(), "fn",
			resolvedImage{ref: "img"}, nil, app.ResourceLimits{}, RunMeta{})
		if err == nil || !strings.Contains(err.Error(), "internal control network is not provisioned") {
			t.Fatalf("error = %v, want the not-provisioned refusal", err)
		}
		if creates != 0 {
			t.Fatalf("container creates = %d, want 0", creates)
		}
	})

	t.Run("user-selected execution-control-role network still rejected", func(t *testing.T) {
		creates := 0
		m, _ := newSUT(t,
			controlRoute(),
			networkInspectRoute("/networks/user-ctl",
				`{"Name":"user-ctl","Driver":"bridge","Labels":{"relay.network.role":"execution-control"}}`),
			dockerRoute{method: http.MethodPost, path: "/containers/create",
				onMatch: func() { creates++ }, body: `{"Id":"cid-1"}`},
		)
		m.networks = []string{"user-ctl"}
		_, err := m.startContainer(context.Background(), "fn",
			resolvedImage{ref: "img"}, nil, app.ResourceLimits{}, RunMeta{})
		if err == nil || !strings.Contains(err.Error(), "control network") {
			t.Fatalf("error = %v, want the control-network workload rejection", err)
		}
		if creates != 0 {
			t.Fatalf("container creates = %d, want 0", creates)
		}
	})
}

// TestServiceNeverJoinsControlNetwork pins service non-attachment: even with a
// provisioned control network on the same Manager, a service joins exactly its
// spec networks — none when NETWORKS is unset (no automatic control endpoint,
// no relay.networks label), its configured set when set — and an attempt to
// select the control network by name is rejected before create.
func TestServiceNeverJoinsControlNetwork(t *testing.T) {
	capture := func(t *testing.T, networks []string) ([]byte, int, *Manager) {
		t.Helper()
		var createBody []byte
		creates := 0
		cli := newScriptedDockerClient(t,
			imageWithoutDeclaredVolumesRoute("img"),
			networkInspectRoute("/networks/"+unitControlNetwork,
				defaultControlNetFixture(unitControlNetwork, "unit-node", "172.30.0.0/16").json(t)),
			networkInspectRoute("/networks/backend", `{"Name":"backend","Labels":{}}`),
			dockerRoute{
				method: http.MethodPost, path: "/containers/create",
				body:    `{"Id":"cid-1"}`,
				onMatch: func() { creates++ },
				onBody:  func(b []byte) { createBody = append([]byte(nil), b...) },
			},
			dockerRoute{method: http.MethodPost, path: "/start", body: `{}`},
			dockerRoute{method: http.MethodGet, path: "/containers/cid-1/json",
				body: `{"Id":"cid-1","State":{"Running":true}}`},
		)
		m := &Manager{
			cli:            cli,
			log:            testutil.DiscardLogger(),
			nodeID:         "unit-node",
			controlNetwork: unitControlNetwork,
		}
		if _, err := m.StartService(context.Background(), ServiceSpec{
			App: "fn", Name: "svc", SourceRef: "service.js", Port: 3000,
			Image: "img", Env: []string{"PORT=3000"}, Networks: networks,
		}, 0); err != nil {
			t.Fatalf("StartService: %v", err)
		}
		return createBody, creates, m
	}

	t.Run("no NETWORKS joins nothing and omits the label", func(t *testing.T) {
		body, _, _ := capture(t, nil)
		assertServiceEndpoints(t, decodeCreateRequest(t, body)) // empty want
	})

	t.Run("NETWORKS set joins exactly the app networks", func(t *testing.T) {
		body, _, _ := capture(t, []string{"backend"})
		req := decodeCreateRequest(t, body)
		assertServiceEndpoints(t, req, "backend")
		if _, ok := req.NetworkingConfig.EndpointsConfig[unitControlNetwork]; ok {
			t.Fatalf("service endpoint set contains the control network: %v",
				endpointKeys(req.NetworkingConfig.EndpointsConfig))
		}
	})

	t.Run("selecting the control network by name is rejected", func(t *testing.T) {
		creates := 0
		cli := newScriptedDockerClient(t,
			imageWithoutDeclaredVolumesRoute("img"),
			networkInspectRoute("/networks/"+unitControlNetwork,
				defaultControlNetFixture(unitControlNetwork, "unit-node", "172.30.0.0/16").json(t)),
			dockerRoute{method: http.MethodPost, path: "/containers/create",
				onMatch: func() { creates++ }, body: `{"Id":"cid-1"}`},
		)
		m := &Manager{
			cli:            cli,
			log:            testutil.DiscardLogger(),
			nodeID:         "unit-node",
			controlNetwork: unitControlNetwork,
		}
		_, err := m.StartService(context.Background(), ServiceSpec{
			App: "fn", Name: "svc", SourceRef: "service.js", Port: 3000,
			Image: "img", Env: []string{"PORT=3000"},
			Networks: []string{unitControlNetwork},
		}, 0)
		if err == nil || !strings.Contains(err.Error(), "control network") {
			t.Fatalf("error = %v, want the control-network workload rejection", err)
		}
		if creates != 0 {
			t.Fatalf("container creates = %d, want 0", creates)
		}
	})
}

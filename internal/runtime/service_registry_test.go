package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"relay/internal/testutil"
)

// Unit tests for Manager.RegistryServiceEndpoints over the scripted daemon:
// the eligibility contract (committed marker, node scope, running, Relay
// healthcheck readiness, verified host binding on the expected address) runs
// through the production client call path without a real Docker daemon.

const registryTestNodeID = "registry-test-node"

// listedService is one service container in a scripted GET /containers/json
// response. The zero value of optional fields omits the corresponding label or
// summary field, which is how the tests express unbound, unlabeled, and
// health-absent containers.
type listedService struct {
	ID       string
	App      string
	Service  string
	Replica  int
	NodeID   string
	BindAddr string // relay.bind_addr; empty omits the label (no publication)
	Probe    bool   // carries Relay's relay.healthcheck label
	State    string // Docker's list-level state; empty defaults to "running"
	Port     int    // relay.port; zero defaults to 8080
	Health   string // Docker's list-level health summary; empty omits it
}

func listedServiceJSON(t *testing.T, c listedService) string {
	t.Helper()
	if c.State == "" {
		c.State = "running"
	}
	if c.Port == 0 {
		c.Port = 8080
	}
	labels := map[string]string{
		labelType:    ContainerTypeService,
		labelApp:     c.App,
		labelService: c.Service,
		labelNodeID:  c.NodeID,
		labelPort:    strconv.Itoa(c.Port),
		labelReplica: strconv.Itoa(c.Replica),
	}
	if c.BindAddr != "" {
		labels[labelBindAddr] = c.BindAddr
	}
	if c.Probe {
		labels[labelHealthcheck] = "probe-v1"
	}
	item := map[string]any{
		"Id":     c.ID,
		"State":  c.State,
		"Labels": labels,
	}
	if c.Health != "" {
		item["Health"] = map[string]string{"Status": c.Health}
	}
	return marshalRegistryJSON(t, item)
}

func serviceListJSON(items ...string) string {
	return "[" + strings.Join(items, ",") + "]"
}

// registryInspectJSON renders one GET /containers/<id>/json body. An empty
// hostIP reports NO published binding for the service port.
func registryInspectJSON(
	t *testing.T, id, status, health string, port int, hostIP, hostPort string,
) string {
	t.Helper()
	state := map[string]any{"Status": status, "Running": status == "running"}
	if health != "" {
		state["Health"] = map[string]string{"Status": health}
	}
	portKey := fmt.Sprintf("%d/tcp", port)
	published := []map[string]string{}
	if hostIP != "" {
		published = append(published,
			map[string]string{"HostIp": hostIP, "HostPort": hostPort})
	}
	return marshalRegistryJSON(t, map[string]any{
		"Id":    id,
		"State": state,
		"Config": map[string]any{
			"ExposedPorts": map[string]any{portKey: map[string]any{}},
		},
		"NetworkSettings": map[string]any{
			"Ports": map[string]any{portKey: published},
		},
	})
}

func marshalRegistryJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("render scripted daemon response: %v", err)
	}
	return string(data)
}

// committedMarkerVolume renders the committed/candidate marker volume one
// replica slot carries on the scripted daemon.
func committedMarkerVolume(
	appName, serviceName, nodeID string, replica int,
	containerID, generation string,
) scriptedVolume {
	return scriptedVolume{
		Name: fmt.Sprintf("relay-state-%s-%s-%d", appName, serviceName, replica),
		Labels: withLabels(
			strictStateLabels(appName, serviceName, nodeID, replica),
			map[string]string{
				labelCommitted:  containerID,
				labelGeneration: generation,
				labelUpdatedAt:  "2026-01-02T03:04:05.000000000Z",
			}),
	}
}

// Given one daemon carrying every eligibility combination, when the registry
// collects endpoints, then ONLY the committed, running, correctly bound
// replicas of THIS node are returned — ready under Relay's healthcheck, or
// simply running when Relay applied none (an image-declared HEALTHCHECK never
// changes the decision) — while candidate, missing-marker, unhealthy,
// stopped, unbound, wrong-bind, foreign-node, empty-generation, and
// stale-after-list containers are all omitted; the batch is sorted by
// (app, service, replica, container).
func TestRegistryServiceEndpointsFiltersToCommittedHealthyBoundReplicas(t *testing.T) {
	markers := []scriptedVolume{
		committedMarkerVolume("alpha", "web", registryTestNodeID, 0, "c1", "gen-c1"),
		committedMarkerVolume("alpha", "web", registryTestNodeID, 1, "c2", "gen-c2"),
		// c3's slot is committed to a DIFFERENT container: c3 is a candidate.
		committedMarkerVolume("alpha", "web", registryTestNodeID, 2, "c-other", "gen-x"),
		// A service Relay gave no healthcheck: running-is-ready.
		committedMarkerVolume("alpha", "static", registryTestNodeID, 0, "c13", "gen-c13"),
		committedMarkerVolume("alpha", "db", registryTestNodeID, 0, "c5", "gen-c5"),
		committedMarkerVolume("alpha", "files", registryTestNodeID, 0, "c6", "gen-c6"),
		committedMarkerVolume("alpha", "dns", registryTestNodeID, 0, "c7", "gen-c7"),
		committedMarkerVolume("alpha", "mail", registryTestNodeID, 0, "c8", "gen-c8"),
		// c10's marker names it but carries no generation identity.
		committedMarkerVolume("alpha", "cache", registryTestNodeID, 0, "c10", ""),
		committedMarkerVolume("alpha", "queue", registryTestNodeID, 0, "c11", "gen-c11"),
		committedMarkerVolume("alpha", "rpc", registryTestNodeID, 0, "c12", "gen-c12"),
		// c4's slot (alpha/web/3) has no marker at all.
	}

	// The eligible higher replica is listed FIRST to prove the batch is
	// sorted, not echoing list order.
	listed := []listedService{
		{ID: "c2", App: "alpha", Service: "web", Replica: 1,
			NodeID: registryTestNodeID, BindAddr: "127.0.0.1",
			Probe: true, Health: "healthy"},
		{ID: "c1", App: "alpha", Service: "web", Replica: 0,
			NodeID: registryTestNodeID, BindAddr: "127.0.0.1",
			Probe: true, Health: "healthy"},
		// No Relay healthcheck and an image-declared HEALTHCHECK reporting
		// unhealthy: running-is-ready, so this replica is still eligible.
		{ID: "c13", App: "alpha", Service: "static", Replica: 0,
			NodeID: registryTestNodeID, BindAddr: "127.0.0.1",
			Health: "unhealthy"},
		// Candidate: committed marker names another container.
		{ID: "c3", App: "alpha", Service: "web", Replica: 2,
			NodeID: registryTestNodeID, BindAddr: "127.0.0.1",
			Probe: true, Health: "healthy"},
		// No marker for this slot.
		{ID: "c4", App: "alpha", Service: "web", Replica: 3,
			NodeID: registryTestNodeID, BindAddr: "127.0.0.1",
			Probe: true, Health: "healthy"},
		// Relay's probe reports unhealthy.
		{ID: "c5", App: "alpha", Service: "db", Replica: 0,
			NodeID: registryTestNodeID, BindAddr: "127.0.0.1",
			Probe: true, Health: "unhealthy"},
		// Not running.
		{ID: "c6", App: "alpha", Service: "files", Replica: 0,
			NodeID: registryTestNodeID, BindAddr: "127.0.0.1",
			Probe: true, State: "exited"},
		// No host publication label at all.
		{ID: "c7", App: "alpha", Service: "dns", Replica: 0,
			NodeID: registryTestNodeID, Probe: true, Health: "healthy"},
		// Bound, but its inspect will show a different host IP.
		{ID: "c8", App: "alpha", Service: "mail", Replica: 0,
			NodeID: registryTestNodeID, BindAddr: "127.0.0.1",
			Probe: true, Health: "healthy"},
		// Another node's container.
		{ID: "c9", App: "alpha", Service: "foreign", Replica: 0,
			NodeID: "another-node", BindAddr: "127.0.0.1",
			Probe: true, Health: "healthy"},
		// Committed to itself but with an empty generation identity.
		{ID: "c10", App: "alpha", Service: "cache", Replica: 0,
			NodeID: registryTestNodeID, BindAddr: "127.0.0.1",
			Probe: true, Health: "healthy"},
		// List says healthy; its inspect will report the container exited.
		{ID: "c11", App: "alpha", Service: "queue", Replica: 0,
			NodeID: registryTestNodeID, BindAddr: "127.0.0.1",
			Probe: true, Health: "healthy"},
		// List says healthy; its inspect will report the probe turned unhealthy.
		{ID: "c12", App: "alpha", Service: "rpc", Replica: 0,
			NodeID: registryTestNodeID, BindAddr: "127.0.0.1",
			Probe: true, Health: "healthy"},
	}
	items := make([]string, 0, len(listed))
	for _, ctr := range listed {
		items = append(items, listedServiceJSON(t, ctr))
	}

	routes := []dockerRoute{
		{method: http.MethodGet, path: "/containers/json",
			body: serviceListJSON(items...)},
		{method: http.MethodGet, path: "/volumes",
			body: volumeListJSON(t, markers)},
		{method: http.MethodGet, path: "/containers/c1/json",
			body: registryInspectJSON(
				t, "c1", "running", "healthy", 8080, "127.0.0.1", "49155")},
		{method: http.MethodGet, path: "/containers/c13/json",
			body: registryInspectJSON(
				t, "c13", "running", "unhealthy", 8080, "127.0.0.1", "49157")},
		{method: http.MethodGet, path: "/containers/c2/json",
			body: registryInspectJSON(
				t, "c2", "running", "healthy", 8080, "127.0.0.1", "49156")},
		// Bound on an address other than the expected bind address.
		{method: http.MethodGet, path: "/containers/c8/json",
			body: registryInspectJSON(
				t, "c8", "running", "healthy", 8080, "10.0.0.9", "49160")},
		// The list snapshot was stale: the container exited since listing.
		{method: http.MethodGet, path: "/containers/c11/json",
			body: registryInspectJSON(t, "c11", "exited", "", 8080, "", "")},
		// The probe turned unhealthy between listing and inspection.
		{method: http.MethodGet, path: "/containers/c12/json",
			body: registryInspectJSON(
				t, "c12", "running", "unhealthy", 8080, "127.0.0.1", "49162")},
	}
	m := &Manager{
		cli:    newScriptedDockerClient(t, routes...),
		log:    testutil.DiscardLogger(),
		nodeID: registryTestNodeID,
	}

	got, err := m.RegistryServiceEndpoints(context.Background(), "127.0.0.1")
	if err != nil {
		t.Fatalf("RegistryServiceEndpoints: %v", err)
	}
	// Sorted by (app, service, replica, container): "static" precedes "web".
	if len(got) != 3 || got[0].ContainerID != "c13" ||
		got[1].ContainerID != "c1" || got[2].ContainerID != "c2" {
		ids := make([]string, len(got))
		for i, endpoint := range got {
			ids[i] = endpoint.ContainerID
		}
		t.Fatalf("endpoints = %v, want [c13 c1 c2] (eligible only, sorted by "+
			"app/service/replica/container)", ids)
	}

	noProbe := got[0]
	if noProbe.Service != "static" || noProbe.Generation != "gen-c13" ||
		noProbe.HostIP != "127.0.0.1" || noProbe.HostPort != 49157 {
		t.Fatalf("c13 endpoint = %+v, want the verified bound facts of the "+
			"probe-less running replica", noProbe)
	}

	first := got[1]
	if first.App != "alpha" || first.Service != "web" || first.Replica != 0 ||
		first.InternalPort != 8080 || first.Generation != "gen-c1" ||
		first.HostIP != "127.0.0.1" || first.HostPort != 49155 {
		t.Fatalf("c1 endpoint = %+v, want the verified bound facts", first)
	}
	if first.Labels == nil || first.Labels[labelNodeID] != registryTestNodeID ||
		first.Labels[labelBindAddr] != "127.0.0.1" {
		t.Fatalf("c1 labels = %v, want the container's full label set", first.Labels)
	}
	if second := got[2]; second.HostPort != 49156 ||
		second.Generation != "gen-c2" || second.Replica != 1 {
		t.Fatalf("c2 endpoint = %+v, want its own verified facts", second)
	}
}

// Given SERVICE_BIND_ADDR unset (empty expected bind address), when the
// registry collects, then it disables itself with no result and never reaches
// the Docker daemon.
func TestRegistryServiceEndpointsDisabledWithoutBind(t *testing.T) {
	var listCalls, volumeCalls int
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/containers/json",
			body: "[]", onMatch: func() { listCalls++ }},
		dockerRoute{method: http.MethodGet, path: "/volumes",
			body: `{"Volumes":[]}`, onMatch: func() { volumeCalls++ }},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: registryTestNodeID}

	got, err := m.RegistryServiceEndpoints(context.Background(), "")
	if err != nil {
		t.Fatalf("RegistryServiceEndpoints with no bind address: %v", err)
	}
	if got != nil {
		t.Fatalf("endpoints = %v, want nil while publication is disabled", got)
	}
	if listCalls != 0 || volumeCalls != 0 {
		t.Fatalf("docker calls = (%d container lists, %d volume lists), want none "+
			"(a disabled registry must not touch the daemon)",
			listCalls, volumeCalls)
	}
}

// Given an expected bind address that is not a usable unicast literal, when the
// registry collects, then it fails closed before any Docker call.
func TestRegistryServiceEndpointsRejectsInvalidBindAddr(t *testing.T) {
	m := &Manager{log: testutil.DiscardLogger()} // no client: rejected before use
	for _, value := range []string{"0.0.0.0", "example.com", "127.0.0.1:8080"} {
		got, err := m.RegistryServiceEndpoints(context.Background(), value)
		if err == nil || !strings.Contains(err.Error(), "invalid bind address") {
			t.Errorf("RegistryServiceEndpoints(%q) = (%v, %v), want an "+
				"invalid-bind-address error", value, got, err)
		}
	}
}

// Given a daemon failure while discovering containers or markers, when the
// registry collects, then it returns NO result (nil, error) so the caller
// keeps its previously published snapshot instead of mistaking a failure for
// "nothing is serving".
func TestRegistryServiceEndpointsDiscoveryFailuresReturnNoResult(t *testing.T) {
	t.Run("container list fails", func(t *testing.T) {
		cli := newScriptedDockerClient(t, dockerRoute{
			method: http.MethodGet, path: "/containers/json",
			status: http.StatusInternalServerError,
			body:   `{"message":"daemon wedge"}`,
		})
		m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: registryTestNodeID}
		got, err := m.RegistryServiceEndpoints(context.Background(), "127.0.0.1")
		if err == nil {
			t.Fatal("RegistryServiceEndpoints succeeded, want the list error")
		}
		if got != nil {
			t.Fatalf("endpoints = %v, want nil alongside a discovery error", got)
		}
	})

	t.Run("marker list fails", func(t *testing.T) {
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/containers/json",
				body: "[]"},
			dockerRoute{method: http.MethodGet, path: "/volumes",
				status: http.StatusInternalServerError,
				body:   `{"message":"volume api down"}`},
		)
		m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: registryTestNodeID}
		got, err := m.RegistryServiceEndpoints(context.Background(), "127.0.0.1")
		if err == nil {
			t.Fatal("RegistryServiceEndpoints succeeded, want the marker error")
		}
		if got != nil {
			t.Fatalf("endpoints = %v, want nil alongside a discovery error", got)
		}
	})
}

// Given three verified candidates where one inspect fails with a daemon error
// and another container vanished (not found), when the registry collects, then
// the successfully verified endpoint is still returned ALONGSIDE the first
// inspect error — one unreadable container must not suppress every healthy
// endpoint — while the vanished container is omitted without an error.
func TestRegistryServiceEndpointsPartialInspectFailureReturnsVerifiedBatch(t *testing.T) {
	markers := []scriptedVolume{
		committedMarkerVolume("alpha", "web", registryTestNodeID, 0, "c-ok", "gen-ok"),
		committedMarkerVolume("alpha", "web", registryTestNodeID, 1, "c-broken", "gen-broken"),
		committedMarkerVolume("alpha", "web", registryTestNodeID, 2, "c-gone", "gen-gone"),
	}
	listed := serviceListJSON(
		listedServiceJSON(t, listedService{ID: "c-ok", App: "alpha",
			Service: "web", Replica: 0, NodeID: registryTestNodeID,
			BindAddr: "127.0.0.1", Probe: true, Health: "healthy"}),
		listedServiceJSON(t, listedService{ID: "c-broken", App: "alpha",
			Service: "web", Replica: 1, NodeID: registryTestNodeID,
			BindAddr: "127.0.0.1", Probe: true, Health: "healthy"}),
		listedServiceJSON(t, listedService{ID: "c-gone", App: "alpha",
			Service: "web", Replica: 2, NodeID: registryTestNodeID,
			BindAddr: "127.0.0.1", Probe: true, Health: "healthy"}),
	)
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/containers/json", body: listed},
		dockerRoute{method: http.MethodGet, path: "/volumes",
			body: volumeListJSON(t, markers)},
		dockerRoute{method: http.MethodGet, path: "/containers/c-ok/json",
			body: registryInspectJSON(
				t, "c-ok", "running", "healthy", 8080, "127.0.0.1", "49201")},
		dockerRoute{method: http.MethodGet, path: "/containers/c-broken/json",
			status: http.StatusInternalServerError,
			body:   `{"message":"inspect failed"}`},
		dockerRoute{method: http.MethodGet, path: "/containers/c-gone/json",
			status: http.StatusNotFound,
			body:   `{"message":"no such container"}`},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: registryTestNodeID}

	got, err := m.RegistryServiceEndpoints(context.Background(), "127.0.0.1")
	if err == nil {
		t.Fatal("RegistryServiceEndpoints succeeded, want the inspect error " +
			"reported alongside the verified batch")
	}
	if !strings.Contains(err.Error(), "inspect container c-broken") {
		t.Fatalf("error %q must identify the unreadable container", err)
	}
	if strings.Contains(err.Error(), "c-gone") {
		t.Fatalf("error %q must not report a vanished container: a not-found "+
			"inspect is an ordinary omission", err)
	}
	if len(got) != 1 || got[0].ContainerID != "c-ok" ||
		got[0].HostPort != 49201 {
		t.Fatalf("endpoints = %+v, want the one verified replica c-ok", got)
	}
}

// Given marker volumes for one node, when the records are reduced, then only
// the newest committed record per replica slot survives: preparation-family
// kinds, foreign-node records, slots with no committed container, and invalid
// replicas are all excluded.
func TestCommittedServiceReplicasSelectsNewestCommittedRecord(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	states := []ServiceConvergenceState{
		{App: "alpha", Service: "web", NodeID: "n1", Replica: 0,
			CommittedContainerID: "stale", Generation: "g-stale", UpdatedAt: now},
		{App: "alpha", Service: "web", NodeID: "n1", Replica: 0,
			CommittedContainerID: "fresh", Generation: "g-fresh",
			UpdatedAt: now.Add(time.Second)},
		// Excluded families below.
		{Kind: ServiceStateKindPreparation, App: "alpha", Service: "prep",
			NodeID: "n1", Replica: 1, CommittedContainerID: "prep",
			Generation: "g-prep", UpdatedAt: now},
		{App: "alpha", Service: "web", NodeID: "other-node", Replica: 2,
			CommittedContainerID: "foreign", Generation: "g-foreign",
			UpdatedAt: now},
		{App: "alpha", Service: "web", NodeID: "n1", Replica: 3,
			CommittedContainerID: "", Generation: "g-orphan", UpdatedAt: now},
		{App: "alpha", Service: "web", NodeID: "n1", Replica: -1,
			CommittedContainerID: "invalid", Generation: "g-invalid",
			UpdatedAt: now},
	}

	got := committedServiceReplicas(states, "n1")
	if len(got) != 1 {
		t.Fatalf("committed slots = %d, want exactly 1", len(got))
	}
	record, ok := got[serviceRegistrySlot{
		app: "alpha", service: "web", replica: 0}]
	if !ok {
		t.Fatal("alpha/web/0 slot missing from the reduction")
	}
	if record.CommittedContainerID != "fresh" || record.Generation != "g-fresh" {
		t.Fatalf("record = %+v, want the NEWEST committed container and generation",
			record)
	}
}

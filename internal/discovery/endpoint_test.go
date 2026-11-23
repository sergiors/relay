package discovery

import (
	"testing"

	"relay/internal/state"
)

// Given registry rows carrying routing facts alongside persistence details,
// when they are projected onto the proxy-neutral discovery model, then all six
// routing facts — identity, effective host, path, advertised address, and host
// port — are retained verbatim in order, and no normalization happens at this
// layer (host casing/whitespace survive for the transformer to normalize).
func TestProjectRoutableEndpointsRetainsRoutingFacts(t *testing.T) {
	registry := []state.ServiceRegistryEndpoint{
		{
			NodeID:        "node-1",
			AdvertiseAddr: "192.0.2.10",
			App:           "alpha",
			Service:       "web",
			Replica:       3,
			Host:          " API.Example.com ",
			Path:          "/v2",
			HostIP:        "127.0.0.1",
			HostPort:      49152,
			InternalPort:  8080,
			ContainerID:   "ctr-1",
			Generation:    "gen-1",
		},
		{
			NodeID:        "node-2",
			AdvertiseAddr: "2001:db8::7",
			App:           "beta",
			Service:       "api",
			Host:          "api.example.com",
			Path:          "/",
			HostPort:      0,
		},
	}

	got := projectRoutableEndpoints(registry)

	if len(got) != len(registry) {
		t.Fatalf("projected %d endpoints, want %d", len(got), len(registry))
	}
	want := []routableEndpoint{
		{
			app: "alpha", service: "web", host: " API.Example.com ",
			path: "/v2", advertiseAddr: "192.0.2.10", hostPort: 49152,
		},
		{
			app: "beta", service: "api", host: "api.example.com",
			path: "/", advertiseAddr: "2001:db8::7", hostPort: 0,
		},
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("endpoint %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// An empty registry projects to an empty result, never nil-dereference bait
// for the transformer.
func TestProjectRoutableEndpointsEmptyRegistry(t *testing.T) {
	got := projectRoutableEndpoints(nil)
	if len(got) != 0 {
		t.Fatalf("projected %d endpoints from nil registry, want 0", len(got))
	}
	got = projectRoutableEndpoints([]state.ServiceRegistryEndpoint{})
	if len(got) != 0 {
		t.Fatalf("projected %d endpoints from empty registry, want 0", len(got))
	}
}

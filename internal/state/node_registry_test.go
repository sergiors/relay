package state

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

// The typed nodes.data JSONB payload is a persisted contract: its exact key
// set must round-trip, the OwnerPID is threaded from the state handle (never
// the caller), and malformed data fails loudly.
func TestNodeRegistryDataRoundTrip(t *testing.T) {
	metadata := NodeLeaseMetadata{
		BindAddr:      "10.0.0.5",
		AdvertiseAddr: "10.0.0.5:7001",
	}
	data, err := encodeNodeRegistryData(metadata, 4242)
	if err != nil {
		t.Fatalf("encode node metadata: %v", err)
	}
	const wantJSON = `{"bind_addr":"10.0.0.5","advertise_addr":"10.0.0.5:7001",` +
		`"owner_pid":4242}`
	if data != wantJSON {
		t.Fatalf("encoded metadata = %s, want %s", data, wantJSON)
	}
	payload, err := decodeNodeRegistryData(data)
	if err != nil {
		t.Fatalf("decode node metadata: %v", err)
	}
	if payload.BindAddr != "10.0.0.5" ||
		payload.AdvertiseAddr != "10.0.0.5:7001" || payload.OwnerPID != 4242 {
		t.Fatalf("round trip payload = %+v, want the encoded metadata", payload)
	}

	// A bind-less worker still persists a well-formed object.
	empty, err := encodeNodeRegistryData(NodeLeaseMetadata{}, 7)
	if err != nil {
		t.Fatalf("encode empty metadata: %v", err)
	}
	if empty != `{"bind_addr":"","advertise_addr":"","owner_pid":7}` {
		t.Fatalf("empty metadata = %s, want the zero-value object", empty)
	}
}

func TestDecodeNodeRegistryDataRejectsMalformed(t *testing.T) {
	for _, tc := range []struct {
		name string
		data string
	}{
		{"truncated object", `{"owner_pid":42`},
		{"not JSON text", `owner_pid=42`},
		{"JSON array", `[42]`},
		{"wrong scalar type", `{"owner_pid":"not-a-number"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := decodeNodeRegistryData(tc.data)
			if err == nil {
				t.Fatalf("decode(%q) succeeded, want an error", tc.data)
			}
			if !strings.Contains(err.Error(), "decode metadata") {
				t.Fatalf("error %q must identify the metadata decode failure", err)
			}
			if payload != (nodeRegistryData{}) {
				t.Fatalf("payload = %+v, want the zero value on failure", payload)
			}
		})
	}

	payload, err := decodeNodeRegistryData(`{}`)
	if err != nil {
		t.Fatalf("decode of an empty object: %v", err)
	}
	if payload != (nodeRegistryData{}) {
		t.Fatalf("empty-object payload = %+v, want the zero value", payload)
	}
}

// The endpoint payload keeps its own cohesive fact set: the exact key set is
// a persisted contract and a round trip must preserve every fact.
func TestServiceRegistryEndpointPayloadRoundTrip(t *testing.T) {
	endpoint := ServiceRegistryEndpoint{
		App:          "alpha",
		Service:      "web",
		Replica:      2,
		Host:         "api.example.com",
		Path:         "/v2",
		HostIP:       "10.0.0.7",
		HostPort:     41234,
		InternalPort: 8080,
		ContainerID:  "ctr-abc",
		Generation:   "gen-9",
	}

	data, err := encodeServiceRegistryEndpointPayload(endpoint)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	const wantJSON = `{"host_ip":"10.0.0.7","host_port":41234,` +
		`"internal_port":8080,"generation":"gen-9"}`
	if data != wantJSON {
		t.Fatalf("encoded payload = %s, want %s", data, wantJSON)
	}

	payload, err := decodeServiceRegistryEndpointPayload(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if payload.HostIP != "10.0.0.7" || payload.HostPort != 41234 ||
		payload.InternalPort != 8080 || payload.Generation != "gen-9" {
		t.Fatalf("round trip payload = %+v, want the encoded facts", payload)
	}
}

// A valid but EMPTY JSON object is a legitimate payload; every other shape
// that cannot populate the typed struct must fail loudly instead of yielding
// silently wrong endpoint facts.
func TestDecodeServiceRegistryEndpointPayloadRejectsMalformed(t *testing.T) {
	for _, tc := range []struct {
		name string
		data string
	}{
		{"truncated object", `{"host_port":41234`},
		{"not JSON text", `host_port=41234`},
		{"JSON array", `["41234"]`},
		{"JSON string", `"41234"`},
		{"wrong scalar type", `{"host_port":"not-a-number"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := decodeServiceRegistryEndpointPayload(tc.data)
			if err == nil {
				t.Fatalf("decode(%q) succeeded, want an error", tc.data)
			}
			if !strings.Contains(err.Error(), "decode endpoint") {
				t.Fatalf("error %q must identify the decode failure", err)
			}
			if payload != (serviceRegistryEndpointPayload{}) {
				t.Fatalf("payload = %+v, want the zero value on failure", payload)
			}
		})
	}

	payload, err := decodeServiceRegistryEndpointPayload(`{}`)
	if err != nil {
		t.Fatalf("decode of an empty object: %v", err)
	}
	if payload != (serviceRegistryEndpointPayload{}) {
		t.Fatalf("empty-object payload = %+v, want the zero value", payload)
	}
}

// A publish batch is a SET keyed by relational identity; a repeated identity
// must collapse to the caller's LAST view instead of reaching PostgreSQL's
// "ON CONFLICT DO UPDATE cannot affect row a second time" rejection.
func TestUniqueServiceRegistryEndpointsKeepsLastOccurrence(t *testing.T) {
	endpoints := []ServiceRegistryEndpoint{
		{App: "alpha", Service: "web", Replica: 0, ContainerID: "c1", HostPort: 41001},
		{App: "alpha", Service: "web", Replica: 1, ContainerID: "c1", HostPort: 41002},
		{App: "alpha", Service: "web", Replica: 0, ContainerID: "c1", HostPort: 41003},
		{App: "beta", Service: "web", Replica: 0, ContainerID: "c1", HostPort: 41004},
	}

	got := uniqueServiceRegistryEndpoints(endpoints)
	if len(got) != 3 {
		t.Fatalf("unique batch = %d rows, want 3 (one per relational identity)", len(got))
	}
	wantPorts := []int{41003, 41002, 41004}
	for i, want := range wantPorts {
		if got[i].HostPort != want {
			t.Errorf("row[%d] port = %d, want %d (first slot keeps its position, "+
				"last occurrence wins)", i, got[i].HostPort, want)
		}
	}

	single := []ServiceRegistryEndpoint{{App: "alpha", Replica: 0}}
	if out := uniqueServiceRegistryEndpoints(single); len(out) != 1 {
		t.Fatalf("single-endpoint batch = %d rows, want passthrough of 1", len(out))
	}
}

// Every claim requires a node-owned handle with a pinned lock backend and a
// positive TTL; each precondition is rejected before any database work.
func TestClaimNodeLeaseValidatesHandle(t *testing.T) {
	ctx := context.Background()

	if _, err := (*State)(nil).ClaimNodeLease(
		ctx, NodeLeaseMetadata{}, time.Second); err == nil ||
		!strings.Contains(err.Error(),
			"node registry: claim requires a node-owned state handle") {
		t.Fatalf("nil handle error = %v, want the node-owned rejection", err)
	}

	ownerless := &State{}
	if _, err := ownerless.ClaimNodeLease(
		ctx, NodeLeaseMetadata{}, time.Second); err == nil ||
		!strings.Contains(err.Error(),
			"node registry: claim requires a node-owned state handle") {
		t.Fatalf("ownerless handle error = %v, want the node-owned rejection", err)
	}

	noBackend := &State{nodeID: "node-a", instance: "inst-1"}
	_, err := noBackend.ClaimNodeLease(ctx, NodeLeaseMetadata{}, time.Second)
	if err == nil || !strings.Contains(err.Error(),
		"node registry: claim requires the node lock backend") {
		t.Fatalf("missing backend error = %v, want the lock-backend rejection", err)
	}

	badTTL := &State{nodeID: "node-a", instance: "inst-1", ownerPID: 42}
	_, err = badTTL.ClaimNodeLease(ctx, NodeLeaseMetadata{}, -time.Second)
	if err == nil || !strings.Contains(err.Error(),
		"node registry: lease TTL must be positive") {
		t.Fatalf("negative-TTL error = %v, want the positive-TTL rejection", err)
	}
	if errors.Is(err, ErrNodeLeaseFence) {
		t.Fatal("a configuration error must not report a fence rejection")
	}
}

// A lease carries an unexported fence that must match the handle issuing the
// write: a lease from another node/instance, a zero or mismatched incarnation,
// a foreign backend PID, or a foreign lock key are all refused with the
// recoverable fence sentinel instead of reaching the database (the handle's db
// is nil, so any unguarded statement would panic the test).
func TestNodeRegistryWritesRefuseForeignLeases(t *testing.T) {
	ctx := context.Background()
	st := &State{nodeID: "node-a", instance: "inst-1", ownerPID: 77}

	proper := func() NodeLease {
		return NodeLease{
			NodeID:      "node-a",
			Instance:    "inst-1",
			Incarnation: 5,
			BindAddr:    "10.0.0.5",
			fence: nodeLeaseFence{
				nodeID:      "node-a",
				instance:    "inst-1",
				incarnation: 5,
				ownerPID:    77,
				lockKey:     nodeLockKey("node-a"),
			},
		}
	}

	for _, tc := range []struct {
		name  string
		madge func(*NodeLease)
	}{
		{"foreign node", func(l *NodeLease) {
			l.fence.nodeID = "node-b"
		}},
		{"foreign instance", func(l *NodeLease) {
			l.fence.instance = "inst-2"
		}},
		{"zero incarnation", func(l *NodeLease) {
			l.fence.incarnation = 0
			l.Incarnation = 0
		}},
		{"incarnation disagrees with fence", func(l *NodeLease) {
			l.Incarnation = 6
		}},
		{"foreign backend PID", func(l *NodeLease) {
			l.fence.ownerPID = 78
		}},
		{"foreign lock key", func(l *NodeLease) {
			l.fence.lockKey = nodeLockKey("node-b")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lease := proper()
			tc.madge(&lease)
			endpoint := ServiceRegistryEndpoint{
				App: "alpha", Service: "web", Host: "api.example.com",
				ContainerID: "ctr-1",
			}

			if _, err := st.HeartbeatNodeLease(ctx, lease); !errors.Is(
				err, ErrNodeLeaseFence) {
				t.Errorf("heartbeat error = %v, want ErrNodeLeaseFence", err)
			}
			if err := st.PublishServiceRegistryEndpoints(
				ctx, lease, []ServiceRegistryEndpoint{endpoint}); !errors.Is(
				err, ErrNodeLeaseFence) {
				t.Errorf("publish error = %v, want ErrNodeLeaseFence", err)
			}
			if err := st.ReleaseNodeLease(ctx, lease); !errors.Is(
				err, ErrNodeLeaseFence) {
				t.Errorf("release error = %v, want ErrNodeLeaseFence", err)
			}
		})
	}

	// An ownerless handle refuses any lease with an explicit handle error.
	ownerless := &State{}
	if _, err := ownerless.HeartbeatNodeLease(ctx, proper()); err == nil ||
		strings.Contains(err.Error(), ErrNodeLeaseFence.Error()) ||
		!strings.Contains(err.Error(),
			"node registry: operation requires a node-owned state handle") {
		t.Fatalf("ownerless heartbeat error = %v, want the node-owned rejection", err)
	}
}

// Endpoint admission validates every RAW input before deduplication and before
// any write transaction opens: one invalid member rejects the whole batch —
// including an invalid DUPLICATE whose later valid twin would otherwise win
// the dedup — without persisting any row. The matrix covers identity, route
// hostname/path rules, port bounds, replica int32 bounds, and IP usability.
// Accepted forms (valid ports, IPv6/loopback/link-local, MaxInt32 replicas)
// are pinned by the integration publication tests.
func TestPublishServiceRegistryEndpointsRejectsInvalidIdentities(t *testing.T) {
	ctx := context.Background()
	st := &State{nodeID: "node-a", instance: "inst-1", ownerPID: 77}
	lease := NodeLease{
		Incarnation: 5,
		fence: nodeLeaseFence{
			nodeID:      "node-a",
			instance:    "inst-1",
			incarnation: 5,
			ownerPID:    77,
			lockKey:     nodeLockKey("node-a"),
		},
	}
	valid := ServiceRegistryEndpoint{
		App: "alpha", Service: "web", Replica: 0,
		Host: "api.example.com", Path: "/v2",
		HostIP: "10.0.0.1", HostPort: 41000, InternalPort: 8080,
		ContainerID: "ctr-1", Generation: "gen-1",
	}
	mutate := func(f func(*ServiceRegistryEndpoint)) ServiceRegistryEndpoint {
		e := valid
		f(&e)
		return e
	}

	for _, tc := range []struct {
		name     string
		endpoint ServiceRegistryEndpoint
		want     string
	}{
		{"empty app", mutate(func(e *ServiceRegistryEndpoint) {
			e.App = ""
		}), "endpoint app is empty"},
		{"empty service", mutate(func(e *ServiceRegistryEndpoint) {
			e.Service = ""
		}), "endpoint service is empty"},
		{"empty container", mutate(func(e *ServiceRegistryEndpoint) {
			e.ContainerID = ""
		}), "endpoint container is empty"},
		{"empty host", mutate(func(e *ServiceRegistryEndpoint) {
			e.Host = ""
		}), "host is empty"},
		{"malformed host", mutate(func(e *ServiceRegistryEndpoint) {
			e.Host = "bad_host"
		}), "not a valid hostname"},
		{"host with whitespace", mutate(func(e *ServiceRegistryEndpoint) {
			e.Host = "api example.com"
		}), "contains whitespace"},
		{"negative replica", mutate(func(e *ServiceRegistryEndpoint) {
			e.Replica = -1
		}), "replica must not be negative"},
		{"replica above int32 max", mutate(func(e *ServiceRegistryEndpoint) {
			e.Replica = math.MaxInt32 + 1
		}), "exceeds the maximum supported value"},
		{"host port zero", mutate(func(e *ServiceRegistryEndpoint) {
			e.HostPort = 0
		}), "host port 0 is outside 1..65535"},
		{"host port above range", mutate(func(e *ServiceRegistryEndpoint) {
			e.HostPort = 65536
		}), "host port 65536 is outside 1..65535"},
		{"internal port zero", mutate(func(e *ServiceRegistryEndpoint) {
			e.InternalPort = 0
		}), "internal port 0 is outside 1..65535"},
		{"internal port above range", mutate(func(e *ServiceRegistryEndpoint) {
			e.InternalPort = 65536
		}), "internal port 65536 is outside 1..65535"},
		{"empty host IP", mutate(func(e *ServiceRegistryEndpoint) {
			e.HostIP = ""
		}), "is not a valid IPv4 or IPv6 address"},
		{"unparseable host IP", mutate(func(e *ServiceRegistryEndpoint) {
			e.HostIP = "not-an-ip"
		}), "is not a valid IPv4 or IPv6 address"},
		{"IPv4-mapped IPv6 host IP", mutate(func(e *ServiceRegistryEndpoint) {
			e.HostIP = "::ffff:192.0.2.1"
		}), "is IPv4-mapped IPv6"},
		{"zone-scoped host IP", mutate(func(e *ServiceRegistryEndpoint) {
			e.HostIP = "fe80::1%eth0"
		}), "zone-scoped"},
		{"unspecified IPv4 host IP", mutate(func(e *ServiceRegistryEndpoint) {
			e.HostIP = "0.0.0.0"
		}), "is unspecified"},
		{"unspecified IPv6 host IP", mutate(func(e *ServiceRegistryEndpoint) {
			e.HostIP = "::"
		}), "is unspecified"},
		{"multicast IPv4 host IP", mutate(func(e *ServiceRegistryEndpoint) {
			e.HostIP = "224.0.0.1"
		}), "is multicast"},
		{"multicast IPv6 host IP", mutate(func(e *ServiceRegistryEndpoint) {
			e.HostIP = "ff02::1"
		}), "is multicast"},
		{"broadcast host IP", mutate(func(e *ServiceRegistryEndpoint) {
			e.HostIP = "255.255.255.255"
		}), "not a usable unicast host address"},
		{"missing generation", mutate(func(e *ServiceRegistryEndpoint) {
			e.Generation = ""
		}), "generation is empty"},
		{"path without leading slash", mutate(func(e *ServiceRegistryEndpoint) {
			e.Path = "v2"
		}), `must start with "/"`},
		{"non-canonical path", mutate(func(e *ServiceRegistryEndpoint) {
			e.Path = "/v2/"
		}), "not canonical"},
		{"path with empty segment", mutate(func(e *ServiceRegistryEndpoint) {
			e.Path = "/a//b"
		}), "empty path segment"},
		{"path with whitespace", mutate(func(e *ServiceRegistryEndpoint) {
			e.Path = "/a b"
		}), "contains whitespace"},
		{"path with query", mutate(func(e *ServiceRegistryEndpoint) {
			e.Path = "/a?b"
		}), "query, fragment, or backslash"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The valid member precedes the invalid one: whole-batch rejection.
			batch := []ServiceRegistryEndpoint{valid, tc.endpoint}
			err := st.PublishServiceRegistryEndpoints(ctx, lease, batch)
			if err == nil {
				t.Fatal("publish succeeded, want the endpoint rejection")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q must contain %q", err, tc.want)
			}
			if errors.Is(err, ErrNodeLeaseFence) {
				t.Fatal("a validation failure must not report a fence rejection")
			}
		})
	}

	// Validation runs on RAW inputs BEFORE deduplication: an invalid
	// duplicate first cannot hide behind its later valid twin (dedup keeps the
	// LAST row, so a post-dedup validator would wrongly accept this batch).
	t.Run("invalid duplicate precedes valid same identity", func(t *testing.T) {
		invalidDup := valid
		invalidDup.HostIP = "0.0.0.0"
		validTwin := valid // identical relational identity, usable IP
		err := st.PublishServiceRegistryEndpoints(ctx, lease,
			[]ServiceRegistryEndpoint{invalidDup, validTwin})
		if err == nil {
			t.Fatal("publish succeeded, want the raw-input rejection: an " +
				"invalid duplicate must not collapse into its valid twin")
		}
		if !strings.Contains(err.Error(), "is unspecified") {
			t.Fatalf("error %q must name the invalid duplicate's host IP", err)
		}
	})
}

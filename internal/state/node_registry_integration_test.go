//go:build integration

// PostgreSQL-backed contract tests for the global node registry and service
// endpoint publication: lease claim/heartbeat/release fencing, the
// heartbeat-owned lease boundary, bounded expired-row pruning, database-clock
// expiry, multi-node isolation, schema constraints, and real concurrency.
// These use the production connection branch (ownerful handles on an isolated
// schema) because the guarantees are enforced by transactions, pg_locks, and
// the PostgreSQL clock — an in-memory fake cannot prove them.
package state

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func mustClaimNodeLease(
	t *testing.T, st *State, metadata NodeLeaseMetadata, ttl time.Duration,
) NodeLease {
	t.Helper()
	lease, err := st.ClaimNodeLease(context.Background(), metadata, ttl)
	if err != nil {
		t.Fatalf("claim node lease: %v", err)
	}
	return lease
}

// testRegistryEndpoint returns an endpoint with distinctive defaults; each test
// overrides only the facts relevant to its scenario.
func testRegistryEndpoint(app, service, container string) ServiceRegistryEndpoint {
	return ServiceRegistryEndpoint{
		App:          app,
		Service:      service,
		Replica:      0,
		Host:         "svc.example.com",
		Path:         "",
		HostIP:       "10.0.0.1",
		HostPort:     41000,
		InternalPort: 8080,
		ContainerID:  container,
		Generation:   "gen-" + container,
	}
}

func publishRegistryBatch(
	t *testing.T, st *State, lease NodeLease,
	endpoints []ServiceRegistryEndpoint,
) {
	t.Helper()
	if err := st.PublishServiceRegistryEndpoints(
		context.Background(), lease, endpoints); err != nil {
		t.Fatalf("publish service registry endpoints: %v", err)
	}
}

func readRegistryEndpoints(
	t *testing.T, st *State, filter ServiceRegistryFilter,
) []ServiceRegistryEndpoint {
	t.Helper()
	endpoints, err := st.ReadEligibleServiceRegistryEndpoints(
		context.Background(), filter)
	if err != nil {
		t.Fatalf("read service registry endpoints: %v", err)
	}
	return endpoints
}

func registryContainerIDs(endpoints []ServiceRegistryEndpoint) []string {
	ids := make([]string, len(endpoints))
	for i, endpoint := range endpoints {
		ids[i] = endpoint.ContainerID
	}
	return ids
}

func requireRegistryContainers(
	t *testing.T, endpoints []ServiceRegistryEndpoint, want ...string,
) {
	t.Helper()
	got := registryContainerIDs(endpoints)
	if len(got) != len(want) {
		t.Fatalf("endpoints = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("endpoints = %v, want %v (deterministic order)", got, want)
		}
	}
}

func registryRowTimestamps(t *testing.T, st *State) map[string]time.Time {
	t.Helper()
	return registryEndpointColumn(t, st, "updated_at")
}

func registryEndpointExpiries(t *testing.T, st *State) map[string]time.Time {
	t.Helper()
	return registryEndpointColumn(t, st, "expires_at")
}

func registryEndpointColumn(
	t *testing.T, st *State, column string,
) map[string]time.Time {
	t.Helper()
	rows, err := st.db.QueryContext(context.Background(),
		`SELECT container_id, `+column+` FROM service_registry_endpoints
		  ORDER BY container_id`)
	if err != nil {
		t.Fatalf("read endpoint %s: %v", column, err)
	}
	defer rows.Close()
	values := map[string]time.Time{}
	for rows.Next() {
		var (
			container string
			value     time.Time
		)
		if err := rows.Scan(&container, &value); err != nil {
			t.Fatalf("scan endpoint %s: %v", column, err)
		}
		values[container] = value
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read endpoint %s: %v", column, err)
	}
	return values
}

// readNodeLeaseRow returns the heartbeat-owned lease timestamps for nodeID.
func readNodeLeaseRow(
	t *testing.T, st *State, nodeID string,
) (heartbeatAt, expiresAt time.Time) {
	t.Helper()
	if err := st.db.QueryRowContext(context.Background(),
		`SELECT heartbeat_at, lease_expires_at FROM nodes WHERE node_id = $1`,
		nodeID).Scan(&heartbeatAt, &expiresAt); err != nil {
		t.Fatalf("read node lease row: %v", err)
	}
	return heartbeatAt, expiresAt
}

func databaseNow(t *testing.T, st *State) time.Time {
	t.Helper()
	var now time.Time
	if err := st.db.QueryRowContext(context.Background(),
		`SELECT now()`).Scan(&now); err != nil {
		t.Fatalf("read database clock: %v", err)
	}
	return now
}

func execNodeRegistrySQL(t *testing.T, st *State, query string, args ...any) {
	t.Helper()
	if _, err := st.db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("exec %s: %v", strings.TrimSpace(query), err)
	}
}

func countRows(t *testing.T, st *State, query string, args ...any) int {
	t.Helper()
	var n int
	if err := st.db.QueryRowContext(context.Background(), query, args...).
		Scan(&n); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return n
}

// Given a node-owned handle, when it claims the global lease with bind/advertise
// metadata and publishes a batch, then every relational field, routing key, and
// JSONB fact survives the round trip; nodes.data carries the typed metadata with
// the pinned backend PID; endpoint rows share the node row's incarnation; and
// the lease expiry is stamped by the database clock.
func TestIntegrationNodeRegistryClaimPublishReadRoundTrip(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	st := openWithIdentity(t, dsn, uniqueNodeID(t, "node-rt"), "inst-1")
	ctx := context.Background()

	const ttl = 30 * time.Second
	metadata := NodeLeaseMetadata{
		BindAddr:      "10.0.0.5",
		AdvertiseAddr: "10.0.0.5:7001",
	}
	lease := mustClaimNodeLease(t, st, metadata, ttl)
	if lease.Instance != "inst-1" || lease.Incarnation <= 0 {
		t.Fatalf("lease identity = (%q, %d), want inst-1 with a positive incarnation",
			lease.Instance, lease.Incarnation)
	}
	if lease.BindAddr != metadata.BindAddr ||
		lease.AdvertiseAddr != metadata.AdvertiseAddr {
		t.Fatalf("lease addresses = (%q, %q), want the claimed metadata pair",
			lease.BindAddr, lease.AdvertiseAddr)
	}
	dbNow := databaseNow(t, st)
	if lease.ExpiresAt.Before(dbNow) ||
		lease.ExpiresAt.After(dbNow.Add(ttl+5*time.Second)) {
		t.Fatalf("lease expiry %s not within [%s, %s + ttl]: expiry must be "+
			"stamped by the database clock",
			lease.ExpiresAt.Format(time.RFC3339Nano),
			dbNow.Format(time.RFC3339Nano), dbNow.Format(time.RFC3339Nano))
	}

	// The typed nodes.data metadata is persisted with the claim, including the
	// handle's pinned backend PID (never a caller-supplied value).
	var rawData string
	if err := st.db.QueryRowContext(ctx,
		`SELECT data::text FROM nodes WHERE node_id = $1`,
		lease.NodeID).Scan(&rawData); err != nil {
		t.Fatalf("read nodes.data: %v", err)
	}
	nodeData, err := decodeNodeRegistryData(rawData)
	if err != nil {
		t.Fatalf("decode nodes.data %q: %v", rawData, err)
	}
	if nodeData.BindAddr != metadata.BindAddr ||
		nodeData.AdvertiseAddr != metadata.AdvertiseAddr {
		t.Fatalf("nodes.data metadata = %+v, want the claimed pair", nodeData)
	}
	if nodeData.OwnerPID != st.ownerPID {
		t.Fatalf("nodes.data owner_pid = %d, want the pinned backend %d",
			nodeData.OwnerPID, st.ownerPID)
	}

	web := testRegistryEndpoint("alpha", "web", "ctr-web")
	web.Replica = 1
	web.Host = "api.example.com"
	web.Path = "/v2"
	web.HostIP = "192.0.2.10"
	web.HostPort = 41001
	web.Generation = "gen-web-1"
	health := testRegistryEndpoint("alpha", "health", "ctr-health")
	health.Host = "health.example.com"
	health.HostPort = 41002
	publishRegistryBatch(t, st, lease,
		[]ServiceRegistryEndpoint{web, health})

	// Endpoint rows are fenced to the CURRENT nodes incarnation.
	var incarnationMatches bool
	if err := st.db.QueryRowContext(ctx,
		`SELECT bool_and(e.incarnation = n.incarnation)
		   FROM service_registry_endpoints e
		   JOIN nodes n ON n.node_id = e.node_id
		  WHERE e.node_id = $1`, lease.NodeID).Scan(&incarnationMatches); err != nil {
		t.Fatalf("compare incarnations: %v", err)
	}
	if !incarnationMatches {
		t.Fatal("endpoint rows must share the node row's incarnation")
	}

	got := readRegistryEndpoints(t, st, ServiceRegistryFilter{})
	if len(got) != 2 {
		t.Fatalf("read endpoints = %d, want 2", len(got))
	}
	byContainer := map[string]ServiceRegistryEndpoint{}
	for _, endpoint := range got {
		if endpoint.NodeID != lease.NodeID {
			t.Errorf("endpoint %s node = %q, want %q",
				endpoint.ContainerID, endpoint.NodeID, lease.NodeID)
		}
		// AdvertiseAddr is decoded from the node row's typed metadata.
		if endpoint.AdvertiseAddr != metadata.AdvertiseAddr {
			t.Errorf("endpoint %s advertise = %q, want %q",
				endpoint.ContainerID, endpoint.AdvertiseAddr, metadata.AdvertiseAddr)
		}
		byContainer[endpoint.ContainerID] = endpoint
	}
	gotWeb, ok := byContainer["ctr-web"]
	if !ok {
		t.Fatalf("ctr-web missing from %v", registryContainerIDs(got))
	}
	if gotWeb.App != "alpha" || gotWeb.Service != "web" || gotWeb.Replica != 1 ||
		gotWeb.Host != "api.example.com" || gotWeb.Path != "/v2" ||
		gotWeb.HostIP != "192.0.2.10" || gotWeb.HostPort != 41001 ||
		gotWeb.InternalPort != 8080 || gotWeb.Generation != "gen-web-1" {
		t.Fatalf("ctr-web round trip = %+v, want every published fact", gotWeb)
	}
	if _, ok := byContainer["ctr-health"]; !ok {
		t.Fatalf("ctr-health missing from %v", registryContainerIDs(got))
	}
}

// Given a worker with NO service publication configured (empty bind metadata and
// no endpoints at all), when it claims and heartbeats the global lease, then the
// registration exists with a well-formed empty-address nodes.data object, reads
// see no endpoints, and a release removes the node row: the node registry is
// global, independent of SERVICE_BIND_ADDR and of having services.
func TestIntegrationNodeRegistryClaimsGloballyWithoutServices(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	st := openWithIdentity(t, dsn, uniqueNodeID(t, "node-nobind"), "inst-1")
	ctx := context.Background()

	const ttl = 30 * time.Second
	lease := mustClaimNodeLease(t, st, NodeLeaseMetadata{}, ttl)
	if lease.BindAddr != "" || lease.AdvertiseAddr != "" {
		t.Fatalf("lease addresses = (%q, %q), want empty metadata with bind unset",
			lease.BindAddr, lease.AdvertiseAddr)
	}

	renewed, err := st.HeartbeatNodeLease(ctx, lease)
	if err != nil {
		t.Fatalf("heartbeat without services: %v", err)
	}
	dbNow := databaseNow(t, st)
	if renewed.ExpiresAt.Before(dbNow) ||
		renewed.ExpiresAt.After(dbNow.Add(ttl+5*time.Second)) {
		t.Fatalf("renewed expiry %s not within the database-clock window", renewed.ExpiresAt)
	}

	var rawData string
	if err := st.db.QueryRowContext(ctx,
		`SELECT data::text FROM nodes WHERE node_id = $1`,
		lease.NodeID).Scan(&rawData); err != nil {
		t.Fatalf("read nodes.data: %v", err)
	}
	// jsonb normalizes key order, so the contract is the DECODED typed object,
	// not the encoder's byte order.
	nodeData, err := decodeNodeRegistryData(rawData)
	if err != nil {
		t.Fatalf("decode nodes.data %q: %v", rawData, err)
	}
	if nodeData.BindAddr != "" || nodeData.AdvertiseAddr != "" ||
		nodeData.OwnerPID != st.ownerPID {
		t.Fatalf("nodes.data = %+v, want empty addresses with owner_pid %d "+
			"(typed object even when bind is unset)", nodeData, st.ownerPID)
	}
	requireRegistryContainers(t,
		readRegistryEndpoints(t, st, ServiceRegistryFilter{}))
	if n := countRows(t, st,
		`SELECT COUNT(*) FROM service_registry_endpoints`); n != 0 {
		t.Fatalf("endpoint rows = %d, want none for a service-less node", n)
	}

	if err := st.ReleaseNodeLease(ctx, lease); err != nil {
		t.Fatalf("release without services: %v", err)
	}
	if n := countRows(t, st,
		`SELECT COUNT(*) FROM nodes WHERE node_id = $1`, lease.NodeID); n != 0 {
		t.Fatalf("node rows after release = %d, want the lease invalidated", n)
	}
}

// Given a predecessor that published endpoints and then lost its pinned lock
// backend's session lock, when a successor claims the same NODE_ID with a
// fresh incarnation, then the claim removes the predecessor's rows atomically;
// every later write by the stale process (heartbeat, publish) is fenced, its
// release is an idempotent no-op that never touches the successor's rows, and
// its re-claim is refused because its backend no longer holds the node lock.
func TestIntegrationNodeRegistryRestartClaimSupersedesPredecessor(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	node := uniqueNodeID(t, "node-restart")
	ctx := context.Background()

	st1 := openWithIdentity(t, dsn, node, "inst-1")
	lease1 := mustClaimNodeLease(t, st1,
		NodeLeaseMetadata{AdvertiseAddr: "10.0.0.5:7001"}, 30*time.Second)
	staleA := testRegistryEndpoint("alpha", "web", "ctr-old-a")
	staleB := testRegistryEndpoint("alpha", "web", "ctr-old-b")
	publishRegistryBatch(t, st1, lease1,
		[]ServiceRegistryEndpoint{staleA, staleB})
	requireRegistryContainers(t,
		readRegistryEndpoints(t, st1, ServiceRegistryFilter{}),
		"ctr-old-a", "ctr-old-b")

	// The predecessor's pinned backend no longer holds the node advisory lock
	// (a crashed or partitioned process whose pool outlives the lock).
	var unlocked bool
	if err := st1.ownerConn.QueryRowContext(ctx,
		`SELECT pg_advisory_unlock($1)`, nodeLockKey(node)).Scan(&unlocked); err != nil {
		t.Fatalf("release node advisory lock: %v", err)
	}
	if !unlocked {
		t.Fatal("pg_advisory_unlock reported the lock was not held")
	}

	st2 := openWithIdentity(t, dsn, node, "inst-2")
	lease2 := mustClaimNodeLease(t, st2,
		NodeLeaseMetadata{AdvertiseAddr: "10.0.0.5:7002"}, 30*time.Second)
	if lease2.Incarnation <= lease1.Incarnation {
		t.Fatalf("successor incarnation = %d, want a fresh claim above %d",
			lease2.Incarnation, lease1.Incarnation)
	}
	requireRegistryContainers(t,
		readRegistryEndpoints(t, st2, ServiceRegistryFilter{}))

	current := testRegistryEndpoint("alpha", "web", "ctr-new")
	current.Host = "new.example.com"
	publishRegistryBatch(t, st2, lease2, []ServiceRegistryEndpoint{current})

	if _, err := st1.HeartbeatNodeLease(ctx, lease1); !errors.Is(
		err, ErrNodeLeaseFence) {
		t.Errorf("stale heartbeat = %v, want ErrNodeLeaseFence", err)
	}
	if err := st1.PublishServiceRegistryEndpoints(ctx, lease1,
		[]ServiceRegistryEndpoint{staleA}); !errors.Is(
		err, ErrNodeLeaseFence) {
		t.Errorf("stale publish = %v, want ErrNodeLeaseFence", err)
	}
	if _, err := st1.ClaimNodeLease(ctx, NodeLeaseMetadata{}, 30*time.Second); !errors.Is(
		err, ErrNodeLeaseFence) {
		t.Errorf("stale re-claim = %v, want ErrNodeLeaseFence "+
			"(its backend no longer holds the node lock)", err)
	}
	if err := st1.ReleaseNodeLease(ctx, lease1); err != nil {
		t.Fatalf("stale release = %v, want an idempotent no-op", err)
	}

	requireRegistryContainers(t,
		readRegistryEndpoints(t, st1, ServiceRegistryFilter{}), "ctr-new")
	var instance string
	if err := st1.db.QueryRowContext(ctx,
		`SELECT instance FROM nodes WHERE node_id = $1`,
		node).Scan(&instance); err != nil {
		t.Fatalf("read node row: %v", err)
	}
	if instance != "inst-2" {
		t.Fatalf("node instance = %q, want the successor's inst-2", instance)
	}
}

// Given a live lease, when the handle presents a backend PID other than the one
// pinned at Open, then every write is fenced even though the real backend still
// holds the node advisory lock: the fence pins the EXACT backend, not merely
// the presence of the key in pg_locks.
func TestIntegrationNodeRegistryFencesExactOwnerBackend(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	st := openWithIdentity(t, dsn, uniqueNodeID(t, "node-backend"), "inst-1")
	ctx := context.Background()

	lease := mustClaimNodeLease(t, st,
		NodeLeaseMetadata{AdvertiseAddr: "10.0.0.5:7001"}, 30*time.Second)
	publishRegistryBatch(t, st, lease,
		[]ServiceRegistryEndpoint{testRegistryEndpoint("alpha", "web", "ctr-1")})

	realPID := st.ownerPID
	st.ownerPID = realPID + 99999

	if _, err := st.HeartbeatNodeLease(ctx, lease); !errors.Is(
		err, ErrNodeLeaseFence) {
		t.Errorf("heartbeat with a foreign backend PID = %v, want ErrNodeLeaseFence", err)
	}
	if err := st.PublishServiceRegistryEndpoints(ctx, lease,
		[]ServiceRegistryEndpoint{
			testRegistryEndpoint("alpha", "web", "ctr-2")}); !errors.Is(
		err, ErrNodeLeaseFence) {
		t.Errorf("publish with a foreign backend PID = %v, want ErrNodeLeaseFence", err)
	}
	if _, err := st.ClaimNodeLease(ctx, NodeLeaseMetadata{},
		30*time.Second); !errors.Is(err, ErrNodeLeaseFence) {
		t.Errorf("claim with a foreign backend PID = %v, want ErrNodeLeaseFence", err)
	}
	requireRegistryContainers(t,
		readRegistryEndpoints(t, st, ServiceRegistryFilter{}), "ctr-1")

	// Restoring the true pinned backend restores every fenced operation.
	st.ownerPID = realPID
	if _, err := st.HeartbeatNodeLease(ctx, lease); err != nil {
		t.Fatalf("heartbeat after restoring the backend PID: %v", err)
	}
	publishRegistryBatch(t, st, lease,
		[]ServiceRegistryEndpoint{
			testRegistryEndpoint("alpha", "web", "ctr-1"),
			testRegistryEndpoint("alpha", "web", "ctr-2")})
	requireRegistryContainers(t,
		readRegistryEndpoints(t, st, ServiceRegistryFilter{}),
		"ctr-1", "ctr-2")
}

// Given a published batch whose node lease lapses, then the rows disappear from
// reads even while their endpoint expiry is still in the future; a heartbeat —
// the SOLE lease-renewal owner — extends the lease against the database clock
// and moves heartbeat_at and lease_expires_at together; the rows become visible
// again without re-publishing; after release the heartbeat is a fence rejection.
func TestIntegrationNodeRegistryHeartbeatExtendsLapsedLease(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	node := uniqueNodeID(t, "node-heartbeat")
	st := openWithIdentity(t, dsn, node, "inst-1")
	ctx := context.Background()

	const ttl = 30 * time.Second
	lease := mustClaimNodeLease(t, st,
		NodeLeaseMetadata{AdvertiseAddr: "10.0.0.5:7001"}, ttl)
	publishRegistryBatch(t, st, lease,
		[]ServiceRegistryEndpoint{testRegistryEndpoint("alpha", "web", "ctr-1")})
	requireRegistryContainers(t,
		readRegistryEndpoints(t, st, ServiceRegistryFilter{}), "ctr-1")

	execNodeRegistrySQL(t, st,
		`UPDATE nodes SET lease_expires_at = now() - interval '1 minute'
		  WHERE node_id = $1`, node)
	requireRegistryContainers(t,
		readRegistryEndpoints(t, st, ServiceRegistryFilter{}))

	beforeHeartbeat, beforeExpiry := readNodeLeaseRow(t, st, node)
	renewed, err := st.HeartbeatNodeLease(ctx, lease)
	if err != nil {
		t.Fatalf("heartbeat after lease expiry: %v", err)
	}
	dbNow := databaseNow(t, st)
	if renewed.ExpiresAt.Before(dbNow) ||
		renewed.ExpiresAt.After(dbNow.Add(ttl+5*time.Second)) {
		t.Fatalf("renewed expiry %s not within [%s, %s + ttl]: renewal must be "+
			"stamped by the database clock",
			renewed.ExpiresAt.Format(time.RFC3339Nano),
			dbNow.Format(time.RFC3339Nano), dbNow.Format(time.RFC3339Nano))
	}
	afterHeartbeat, afterExpiry := readNodeLeaseRow(t, st, node)
	if !afterHeartbeat.After(beforeHeartbeat) {
		t.Fatalf("heartbeat_at = %s, want it advanced from %s by the heartbeat",
			afterHeartbeat, beforeHeartbeat)
	}
	if !afterExpiry.After(beforeExpiry) {
		t.Fatalf("lease_expires_at = %s, want it advanced from %s by the heartbeat",
			afterExpiry, beforeExpiry)
	}
	requireRegistryContainers(t,
		readRegistryEndpoints(t, st, ServiceRegistryFilter{}), "ctr-1")

	if err := st.ReleaseNodeLease(ctx, lease); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, err := st.HeartbeatNodeLease(ctx, lease); !errors.Is(
		err, ErrNodeLeaseFence) {
		t.Fatalf("heartbeat after release = %v, want ErrNodeLeaseFence", err)
	}
	requireRegistryContainers(t,
		readRegistryEndpoints(t, st, ServiceRegistryFilter{}))
}

// Given a node heartbeat-owned lease, when endpoints are published (repeatedly
// and with changed facts), then nodes.heartbeat_at and nodes.lease_expires_at
// remain BYTE-IDENTICAL — publication must never renew the node lease — while
// the published endpoint's own expires_at is refreshed; a subsequent heartbeat
// alone moves the node lease timestamps (the sole-renewal contrast).
func TestIntegrationServiceRegistryPublicationDoesNotRenewNodeLease(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	node := uniqueNodeID(t, "node-solerenewal")
	st := openWithIdentity(t, dsn, node, "inst-1")

	const ttl = time.Minute
	lease := mustClaimNodeLease(t, st,
		NodeLeaseMetadata{AdvertiseAddr: "10.0.0.5:7001"}, ttl)

	endpoint := testRegistryEndpoint("alpha", "web", "ctr-1")
	publishRegistryBatch(t, st, lease, []ServiceRegistryEndpoint{endpoint})
	heartbeat0, expiry0 := readNodeLeaseRow(t, st, node)
	expiries0 := registryEndpointExpiries(t, st)

	// A repeat publish of changed facts forces an endpoint rewrite; the node
	// lease must not move at all.
	changed := endpoint
	changed.HostPort = 41999
	changed.Generation = "gen-2"
	publishRegistryBatch(t, st, lease, []ServiceRegistryEndpoint{changed})

	heartbeat1, expiry1 := readNodeLeaseRow(t, st, node)
	if !heartbeat1.Equal(heartbeat0) {
		t.Fatalf("heartbeat_at moved %s -> %s on publish: the node heartbeat "+
			"must be the SOLE lease-renewal owner", heartbeat0, heartbeat1)
	}
	if !expiry1.Equal(expiry0) {
		t.Fatalf("lease_expires_at moved %s -> %s on publish: publication must "+
			"never renew the node lease", expiry0, expiry1)
	}
	expiries1 := registryEndpointExpiries(t, st)
	if !expiries1["ctr-1"].After(expiries0["ctr-1"]) {
		t.Fatalf("endpoint expires_at = %s, want a refresh after %s "+
			"(publication renews only endpoint expiry)",
			expiries1["ctr-1"], expiries0["ctr-1"])
	}

	// The contrast: only HeartbeatNodeLease moves the node lease timestamps.
	if _, err := st.HeartbeatNodeLease(context.Background(), lease); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	heartbeat2, expiry2 := readNodeLeaseRow(t, st, node)
	if !expiry2.After(expiry1) || !heartbeat2.After(heartbeat1) {
		t.Fatalf("heartbeat moved the lease from (%s, %s) to (%s, %s), want both "+
			"timestamps advanced: the heartbeat is the sole renewal",
			heartbeat1, expiry1, heartbeat2, expiry2)
	}
}

// Given a node whose DB-time lease has expired while its fence (advisory lock
// and incarnation) is still perfectly valid, when publication is attempted,
// then it is rejected with ErrNodeLeaseFence and does NOT resurrect the lease;
// reads exclude its endpoints; only a successful HeartbeatNodeLease restores
// eligibility, after which publication succeeds again — and that publication
// still leaves the heartbeat-owned timestamps untouched.
func TestIntegrationServiceRegistryPublishRejectedAfterLeaseExpiryUntilHeartbeat(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	node := uniqueNodeID(t, "node-expiredpublish")
	st := openWithIdentity(t, dsn, node, "inst-1")
	ctx := context.Background()

	const ttl = time.Minute
	lease := mustClaimNodeLease(t, st,
		NodeLeaseMetadata{AdvertiseAddr: "10.0.0.5:7001"}, ttl)
	endpoint := testRegistryEndpoint("alpha", "web", "ctr-1")
	publishRegistryBatch(t, st, lease, []ServiceRegistryEndpoint{endpoint})

	execNodeRegistrySQL(t, st,
		`UPDATE nodes SET lease_expires_at = now() - interval '1 minute'
		  WHERE node_id = $1`, node)
	_, expiredAt := readNodeLeaseRow(t, st, node)

	// The fence alone is not enough: publication requires an UNEXPIRED lease.
	next := endpoint
	next.HostPort = 41998
	err := st.PublishServiceRegistryEndpoints(ctx, lease,
		[]ServiceRegistryEndpoint{next})
	if !errors.Is(err, ErrNodeLeaseFence) {
		t.Fatalf("publish with an expired lease = %v, want ErrNodeLeaseFence", err)
	}
	_, stillExpired := readNodeLeaseRow(t, st, node)
	if !stillExpired.Equal(expiredAt) {
		t.Fatalf("lease_expires_at = %s, want the rejected publish to leave the "+
			"expired lease untouched (%s)", stillExpired, expiredAt)
	}
	requireRegistryContainers(t,
		readRegistryEndpoints(t, st, ServiceRegistryFilter{}))

	// Only the heartbeat restores eligibility.
	renewed, err := st.HeartbeatNodeLease(ctx, lease)
	if err != nil {
		t.Fatalf("heartbeat after lease expiry: %v", err)
	}
	dbNow := databaseNow(t, st)
	if !renewed.ExpiresAt.After(dbNow) {
		t.Fatalf("renewed expiry %s must be in the future by the database clock",
			renewed.ExpiresAt)
	}
	_, heartbeatExpiry := readNodeLeaseRow(t, st, node)
	requireRegistryContainers(t,
		readRegistryEndpoints(t, st, ServiceRegistryFilter{}), "ctr-1")

	publishRegistryBatch(t, st, lease, []ServiceRegistryEndpoint{next})
	requireRegistryContainers(t,
		readRegistryEndpoints(t, st, ServiceRegistryFilter{}), "ctr-1")
	_, expiryAfterPublish := readNodeLeaseRow(t, st, node)
	if !expiryAfterPublish.Equal(heartbeatExpiry) {
		t.Fatalf("lease_expires_at = %s after the accepted publish, want the "+
			"heartbeat-owned %s (publication never renews the lease)",
			expiryAfterPublish, heartbeatExpiry)
	}
}

// Given a published batch, when the identical batch is published again, then no
// endpoint row is rewritten (a steady-state sync must not amplify writes), and
// when the SAME relational identity carries new JSONB facts, then the single
// row is updated in place rather than duplicated.
func TestIntegrationNodeRegistryPublicationWriteAmplification(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	st := openWithIdentity(t, dsn, uniqueNodeID(t, "node-rewrite"), "inst-1")

	// A long TTL keeps the bounded expiry-renewal threshold far away, so only
	// a genuine fact change can rewrite a row.
	const ttl = time.Minute
	lease := mustClaimNodeLease(t, st,
		NodeLeaseMetadata{AdvertiseAddr: "10.0.0.5:7001"}, ttl)

	alpha := testRegistryEndpoint("alpha", "web", "ctr-a")
	alpha.Host = "a.example.com"
	beta := testRegistryEndpoint("alpha", "api", "ctr-b")
	beta.Host = "b.example.com"
	beta.Replica = 1

	publishRegistryBatch(t, st, lease,
		[]ServiceRegistryEndpoint{alpha, beta})
	before := registryRowTimestamps(t, st)
	if len(before) != 2 {
		t.Fatalf("published rows = %d, want 2", len(before))
	}

	publishRegistryBatch(t, st, lease,
		[]ServiceRegistryEndpoint{alpha, beta})
	unchanged := registryRowTimestamps(t, st)
	if len(unchanged) != 2 {
		t.Fatalf("rows after identical publish = %d, want 2", len(unchanged))
	}
	for container, updatedAt := range before {
		if !unchanged[container].Equal(updatedAt) {
			t.Errorf("row %s updated_at = %s, want unchanged %s "+
				"(identical publish must not rewrite rows)",
				container, unchanged[container], updatedAt)
		}
	}

	// Same relational identity and route; only the JSONB facts changed (the
	// replica was restarted onto a new host port and generation).
	restarted := alpha
	restarted.HostIP = "10.0.0.9"
	restarted.HostPort = 41999
	restarted.Generation = "gen-a-2"
	publishRegistryBatch(t, st, lease,
		[]ServiceRegistryEndpoint{restarted, beta})

	got := readRegistryEndpoints(t, st, ServiceRegistryFilter{})
	if len(got) != 2 {
		t.Fatalf("rows after changed publish = %v, want 2 (updated in place, "+
			"never duplicated)", registryContainerIDs(got))
	}
	after := registryRowTimestamps(t, st)
	if len(after) != 2 {
		t.Fatalf("row identities = %v, want the original two", after)
	}
	if !after["ctr-a"].After(before["ctr-a"]) {
		t.Fatalf("ctr-a updated_at = %s, want a rewrite after %s (changed JSONB "+
			"facts must update the row)", after["ctr-a"], before["ctr-a"])
	}
	for _, endpoint := range got {
		if endpoint.ContainerID != "ctr-a" {
			continue
		}
		if endpoint.HostIP != "10.0.0.9" || endpoint.HostPort != 41999 ||
			endpoint.Generation != "gen-a-2" ||
			endpoint.Host != alpha.Host || endpoint.Replica != alpha.Replica {
			t.Fatalf("ctr-a after changed publish = %+v, want new facts under the "+
				"unchanged identity", endpoint)
		}
	}
}

// Given a published set, when a new batch is published, then the stored rows
// are EXACTLY that batch — additions land, departures are removed, changed
// rows are updated — and an empty batch withdraws every endpoint while the
// node lease itself stays alive.
func TestIntegrationNodeRegistryPublishReplacesExactBatch(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	st := openWithIdentity(t, dsn, uniqueNodeID(t, "node-batch"), "inst-1")
	ctx := context.Background()

	lease := mustClaimNodeLease(t, st,
		NodeLeaseMetadata{AdvertiseAddr: "10.0.0.5:7001"}, time.Minute)
	alpha := testRegistryEndpoint("alpha", "web", "ctr-a")
	alpha.Host = "a.example.com"
	beta := testRegistryEndpoint("alpha", "api", "ctr-b")
	beta.Host = "b.example.com"
	gamma := testRegistryEndpoint("beta", "web", "ctr-c")
	gamma.Host = "c.example.com"

	publishRegistryBatch(t, st, lease,
		[]ServiceRegistryEndpoint{alpha, beta})
	requireRegistryContainers(t,
		readRegistryEndpoints(t, st, ServiceRegistryFilter{}),
		"ctr-a", "ctr-b")

	// The next batch is authoritative: beta moved to a new route and
	// generation, gamma joined, alpha departed.
	moved := beta
	moved.Host = "beta.example.com"
	moved.HostPort = 42002
	moved.Generation = "gen-beta-2"
	publishRegistryBatch(t, st, lease,
		[]ServiceRegistryEndpoint{moved, gamma})

	got := readRegistryEndpoints(t, st, ServiceRegistryFilter{})
	requireRegistryContainers(t, got, "ctr-b", "ctr-c")
	for _, endpoint := range got {
		if endpoint.ContainerID == "ctr-b" &&
			(endpoint.Host != "beta.example.com" ||
				endpoint.HostPort != 42002 ||
				endpoint.Generation != "gen-beta-2") {
			t.Fatalf("ctr-b = %+v, want the moved route and new facts", endpoint)
		}
	}

	// An empty batch withdraws every endpoint but keeps the node lease alive.
	publishRegistryBatch(t, st, lease, nil)
	requireRegistryContainers(t,
		readRegistryEndpoints(t, st, ServiceRegistryFilter{}))
	if _, err := st.HeartbeatNodeLease(ctx, lease); err != nil {
		t.Fatalf("heartbeat after an empty publish = %v, want the lease intact", err)
	}
}

// Given a batch repeating one relational identity (with the caller's latest
// facts), when it is published, then it succeeds and stores exactly one row
// with the LAST occurrence's payload instead of tripping PostgreSQL's
// ON CONFLICT single-affect restriction.
func TestIntegrationNodeRegistryPublishDeduplicatesBatchIdentities(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	st := openWithIdentity(t, dsn, uniqueNodeID(t, "node-dedup"), "inst-1")

	lease := mustClaimNodeLease(t, st,
		NodeLeaseMetadata{AdvertiseAddr: "10.0.0.5:7001"}, time.Minute)
	first := testRegistryEndpoint("alpha", "web", "ctr-1")
	first.HostPort = 41001
	last := first
	last.HostPort = 41999
	last.Generation = "gen-final"

	publishRegistryBatch(t, st, lease,
		[]ServiceRegistryEndpoint{first, last})

	got := readRegistryEndpoints(t, st, ServiceRegistryFilter{})
	requireRegistryContainers(t, got, "ctr-1")
	if got[0].HostPort != 41999 || got[0].Generation != "gen-final" {
		t.Fatalf("ctr-1 = %+v, want the last occurrence's facts", got[0])
	}
}

// Given a batch with one invalid member, when it is published, then the whole
// batch is rejected before any write: the stored set stays empty.
func TestIntegrationNodeRegistryInvalidBatchPersistsNothing(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	st := openWithIdentity(t, dsn, uniqueNodeID(t, "node-invalid"), "inst-1")

	lease := mustClaimNodeLease(t, st,
		NodeLeaseMetadata{AdvertiseAddr: "10.0.0.5:7001"}, time.Minute)
	valid := testRegistryEndpoint("alpha", "web", "ctr-1")
	invalid := testRegistryEndpoint("alpha", "web", "ctr-2")
	invalid.Host = ""

	err := st.PublishServiceRegistryEndpoints(context.Background(), lease,
		[]ServiceRegistryEndpoint{valid, invalid})
	if err == nil || !strings.Contains(err.Error(), "invalid endpoint identity") {
		t.Fatalf("publish = %v, want the identity rejection for the batch", err)
	}
	if errors.Is(err, ErrNodeLeaseFence) {
		t.Fatal("a validation failure must not report a fence rejection")
	}
	requireRegistryContainers(t,
		readRegistryEndpoints(t, st, ServiceRegistryFilter{}))
}

// Given published endpoints and a live lease, when the node releases, then
// every endpoint row and the node lease row are gone in one transaction, the
// operation is idempotent — and release still works after the lease has
// EXPIRED (ownership of the fence, never expiry, is the release authority).
func TestIntegrationNodeRegistryReleaseRemovesEndpointsAndLease(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	node := uniqueNodeID(t, "node-release")
	st := openWithIdentity(t, dsn, node, "inst-1")
	ctx := context.Background()

	lease := mustClaimNodeLease(t, st,
		NodeLeaseMetadata{AdvertiseAddr: "10.0.0.5:7001"}, time.Minute)
	publishRegistryBatch(t, st, lease,
		[]ServiceRegistryEndpoint{
			testRegistryEndpoint("alpha", "web", "ctr-1"),
			testRegistryEndpoint("alpha", "api", "ctr-2")})
	requireRegistryContainers(t,
		readRegistryEndpoints(t, st, ServiceRegistryFilter{}),
		"ctr-2", "ctr-1")

	if err := st.ReleaseNodeLease(ctx, lease); err != nil {
		t.Fatalf("release: %v", err)
	}
	requireRegistryContainers(t,
		readRegistryEndpoints(t, st, ServiceRegistryFilter{}))
	if nodes := countRows(t, st,
		`SELECT COUNT(*) FROM nodes WHERE node_id = $1`, node); nodes != 0 {
		t.Fatalf("node rows after release = %d, want the lease invalidated", nodes)
	}
	if endpoints := countRows(t, st,
		`SELECT COUNT(*) FROM service_registry_endpoints
		  WHERE node_id = $1`, node); endpoints != 0 {
		t.Fatalf("endpoint rows after release = %d, want none", endpoints)
	}
	if err := st.ReleaseNodeLease(ctx, lease); err != nil {
		t.Fatalf("repeat release = %v, want an idempotent no-op", err)
	}

	// A graceful shutdown that runs AFTER a lease lapse still releases.
	relapsed := mustClaimNodeLease(t, st,
		NodeLeaseMetadata{AdvertiseAddr: "10.0.0.5:7001"}, time.Minute)
	publishRegistryBatch(t, st, relapsed,
		[]ServiceRegistryEndpoint{testRegistryEndpoint("alpha", "web", "ctr-3")})
	execNodeRegistrySQL(t, st,
		`UPDATE nodes SET lease_expires_at = now() - interval '1 minute'
		  WHERE node_id = $1`, node)
	if err := st.ReleaseNodeLease(ctx, relapsed); err != nil {
		t.Fatalf("release after lease lapse = %v, want success (fence, not "+
			"expiry, is the release authority)", err)
	}
	if n := countRows(t, st,
		`SELECT COUNT(*) FROM nodes WHERE node_id = $1`, node); n != 0 {
		t.Fatalf("node rows after lapsed release = %d, want none", n)
	}
}

// Given a published endpoint, when either expiry (the node lease or the
// endpoint snapshot) crosses the PostgreSQL clock, then the row disappears from
// reads, in every combination; restoring either expiry makes it visible again.
func TestIntegrationNodeRegistryReadFiltersOnDatabaseClockExpiry(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	node := uniqueNodeID(t, "node-expiry")
	st := openWithIdentity(t, dsn, node, "inst-1")
	lease := mustClaimNodeLease(t, st,
		NodeLeaseMetadata{AdvertiseAddr: "10.0.0.5:7001"}, time.Minute)
	publishRegistryBatch(t, st, lease,
		[]ServiceRegistryEndpoint{testRegistryEndpoint("alpha", "web", "ctr-1")})

	setEndpointExpiry := func(offset string) {
		t.Helper()
		execNodeRegistrySQL(t, st,
			`UPDATE service_registry_endpoints
			    SET expires_at = now() + $2::interval
			  WHERE node_id = $1`, node, offset)
	}
	setLeaseExpiry := func(offset string) {
		t.Helper()
		execNodeRegistrySQL(t, st,
			`UPDATE nodes SET lease_expires_at = now() + $2::interval
			  WHERE node_id = $1`, node, offset)
	}
	requireVisible := func(want bool) {
		t.Helper()
		got := readRegistryEndpoints(t, st, ServiceRegistryFilter{})
		if want {
			requireRegistryContainers(t, got, "ctr-1")
		} else {
			requireRegistryContainers(t, got)
		}
	}

	requireVisible(true)
	setEndpointExpiry("-1 hour")
	requireVisible(false) // lease future, endpoint past
	setEndpointExpiry("1 hour")
	requireVisible(true)
	setLeaseExpiry("-1 hour")
	requireVisible(false) // endpoint future, lease past
	setLeaseExpiry("1 hour")
	requireVisible(true)
	setEndpointExpiry("-1 hour")
	setLeaseExpiry("-1 hour")
	requireVisible(false) // both past
	setEndpointExpiry("1 hour")
	setLeaseExpiry("1 hour")
	requireVisible(true) // both future again
}

// Given a node that stops writing entirely (a crashed worker), when the
// database clock passes the stored lease and endpoint expiry, then its rows
// disappear from reads on their own — no writer ever cleans them up, and the
// row itself remains until the bounded prune removes it later.
func TestIntegrationNodeRegistryRowsExpireWithoutWriterCleanup(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	node := uniqueNodeID(t, "node-silence")
	st := openWithIdentity(t, dsn, node, "inst-1")
	ctx := context.Background()

	lease := mustClaimNodeLease(t, st,
		NodeLeaseMetadata{AdvertiseAddr: "10.0.0.5:7001"}, time.Minute)
	publishRegistryBatch(t, st, lease,
		[]ServiceRegistryEndpoint{testRegistryEndpoint("alpha", "web", "ctr-1")})
	requireRegistryContainers(t,
		readRegistryEndpoints(t, st, ServiceRegistryFilter{}), "ctr-1")

	// The last writer stamps both expiries a moment ahead on the database
	// clock, then goes silent: nothing updates or deletes these rows again.
	execNodeRegistrySQL(t, st,
		`UPDATE nodes SET lease_expires_at = now() + interval '1 second'
		  WHERE node_id = $1`, node)
	execNodeRegistrySQL(t, st,
		`UPDATE service_registry_endpoints
		    SET expires_at = now() + interval '1 second'
		  WHERE node_id = $1`, node)
	// Advance the DATABASE clock past both expiries; the Go clock is irrelevant.
	if _, err := st.db.ExecContext(ctx, `SELECT pg_sleep(1.5)`); err != nil {
		t.Fatalf("advance the database clock: %v", err)
	}
	requireRegistryContainers(t,
		readRegistryEndpoints(t, st, ServiceRegistryFilter{}))
	// Read-level invisibility required NO cleanup writer: both rows still exist.
	if n := countRows(t, st,
		`SELECT COUNT(*) FROM nodes WHERE node_id = $1`, node); n != 1 {
		t.Fatalf("node rows = %d, want the expired row left for the bounded prune", n)
	}
	if n := countRows(t, st,
		`SELECT COUNT(*) FROM service_registry_endpoints
		  WHERE node_id = $1`, node); n != 1 {
		t.Fatalf("endpoint rows = %d, want the expired snapshot left untouched", n)
	}
}

// Given two independent nodes sharing one database, when both publish, then a
// read sees every eligible endpoint in the deterministic documented order
// (host, path, node, app, service, replica, container), each row carries its
// owning node's identity and advertise address (decoded from nodes.data), and
// every filter dimension narrows exactly to its matches — including reads from
// an ownerless admin handle.
func TestIntegrationNodeRegistryMultiNodeFiltersAndOrdering(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	nodeA := uniqueNodeID(t, "node-multi-a")
	nodeB := uniqueNodeID(t, "node-multi-b")

	stA := openWithIdentity(t, dsn, nodeA, "inst-a")
	stB := openWithIdentity(t, dsn, nodeB, "inst-b")
	leaseA := mustClaimNodeLease(t, stA,
		NodeLeaseMetadata{BindAddr: "10.0.0.1", AdvertiseAddr: "10.0.0.1:7001"},
		time.Minute)
	leaseB := mustClaimNodeLease(t, stB,
		NodeLeaseMetadata{BindAddr: "10.0.0.2", AdvertiseAddr: "10.0.0.2:7002"},
		time.Minute)

	a0 := testRegistryEndpoint("alpha", "web", "ctr-a0")
	a0.Host = "b.example.com"
	a0.HostPort = 41001
	a1 := testRegistryEndpoint("alpha", "api", "ctr-a1")
	a1.Host = "a.example.com"
	a1.Path = "/v1"
	a1.Replica = 1
	a1.HostPort = 41002
	b0 := testRegistryEndpoint("beta", "web", "ctr-b0")
	b0.Host = "a.example.com"
	b0.HostPort = 42001
	b1 := testRegistryEndpoint("alpha", "web", "ctr-b1")
	b1.Host = "a.example.com"
	b1.HostPort = 42002

	// Publish in an order unrelated to the deterministic read order.
	publishRegistryBatch(t, stA, leaseA,
		[]ServiceRegistryEndpoint{a0, a1})
	publishRegistryBatch(t, stB, leaseB,
		[]ServiceRegistryEndpoint{b1, b0})

	admin := testOpenOrFail(t, dsn)
	all := readRegistryEndpoints(t, admin, ServiceRegistryFilter{})
	requireRegistryContainers(t, all,
		"ctr-b1", "ctr-b0", "ctr-a1", "ctr-a0")
	for _, endpoint := range all {
		wantNode, wantAdvertise := nodeB, "10.0.0.2:7002"
		if strings.HasSuffix(endpoint.ContainerID, "a0") ||
			strings.HasSuffix(endpoint.ContainerID, "a1") {
			wantNode, wantAdvertise = nodeA, "10.0.0.1:7001"
		}
		if endpoint.NodeID != wantNode {
			t.Errorf("endpoint %s node = %q, want %q",
				endpoint.ContainerID, endpoint.NodeID, wantNode)
		}
		if endpoint.AdvertiseAddr != wantAdvertise {
			t.Errorf("endpoint %s advertise = %q, want %q",
				endpoint.ContainerID, endpoint.AdvertiseAddr, wantAdvertise)
		}
	}

	for _, tc := range []struct {
		name   string
		filter ServiceRegistryFilter
		want   []string
	}{
		{"node A only", ServiceRegistryFilter{NodeID: nodeA},
			[]string{"ctr-a1", "ctr-a0"}},
		{"node B only", ServiceRegistryFilter{NodeID: nodeB},
			[]string{"ctr-b1", "ctr-b0"}},
		{"app alpha", ServiceRegistryFilter{App: "alpha"},
			[]string{"ctr-b1", "ctr-a1", "ctr-a0"}},
		{"service web", ServiceRegistryFilter{Service: "web"},
			[]string{"ctr-b1", "ctr-b0", "ctr-a0"}},
		{"host", ServiceRegistryFilter{Host: "a.example.com"},
			[]string{"ctr-b1", "ctr-b0", "ctr-a1"}},
		{"path", ServiceRegistryFilter{Path: "/v1"},
			[]string{"ctr-a1"}},
		{"host and service",
			ServiceRegistryFilter{Host: "a.example.com", Service: "web"},
			[]string{"ctr-b1", "ctr-b0"}},
		{"node, app and service",
			ServiceRegistryFilter{NodeID: nodeA, App: "alpha", Service: "web"},
			[]string{"ctr-a0"}},
		{"no match", ServiceRegistryFilter{App: "nonexistent"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := readRegistryEndpoints(t, admin, tc.filter)
			requireRegistryContainers(t, got, tc.want...)
		})
	}
}

// Given several independent nodes claiming and publishing concurrently over
// real connections, then each node observes exactly its own batch under its own
// identity, and the combined view is the disjoint union of all batches.
func TestIntegrationNodeRegistryConcurrentNodesStayIsolated(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)

	const (
		nodeCount        = 6
		endpointsPerNode = 3
	)
	nodes := make([]string, nodeCount)
	handles := make([]*State, nodeCount)
	for i := range handles {
		nodes[i] = uniqueNodeID(t, fmt.Sprintf("node-race-%d", i))
		handles[i] = openWithIdentity(t, dsn, nodes[i], "inst-1")
	}

	var wg sync.WaitGroup
	for i := range handles {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx := context.Background()
			st := handles[i]
			advertise := fmt.Sprintf("10.0.0.%d:7001", i+1)
			lease, err := st.ClaimNodeLease(ctx, NodeLeaseMetadata{
				BindAddr:      fmt.Sprintf("10.0.0.%d", i+1),
				AdvertiseAddr: advertise,
			}, time.Minute)
			if err != nil {
				t.Errorf("node %d claim: %v", i, err)
				return
			}
			batch := make([]ServiceRegistryEndpoint, 0, endpointsPerNode)
			for j := 0; j < endpointsPerNode; j++ {
				endpoint := testRegistryEndpoint(
					"alpha", fmt.Sprintf("svc-%d", j),
					fmt.Sprintf("ctr-%d-%d", i, j))
				endpoint.Host = fmt.Sprintf("node%d.example.com", i)
				endpoint.HostPort = 41000 + i*10 + j
				batch = append(batch, endpoint)
			}
			if err := st.PublishServiceRegistryEndpoints(ctx, lease, batch); err != nil {
				t.Errorf("node %d publish: %v", i, err)
				return
			}
			got, err := st.ReadEligibleServiceRegistryEndpoints(ctx,
				ServiceRegistryFilter{NodeID: nodes[i]})
			if err != nil {
				t.Errorf("node %d read: %v", i, err)
				return
			}
			if len(got) != endpointsPerNode {
				t.Errorf("node %d sees %d endpoints, want %d",
					i, len(got), endpointsPerNode)
				return
			}
			for _, endpoint := range got {
				if endpoint.NodeID != nodes[i] {
					t.Errorf("node %d sees foreign node %q", i, endpoint.NodeID)
				}
				if !strings.HasPrefix(endpoint.ContainerID,
					fmt.Sprintf("ctr-%d-", i)) {
					t.Errorf("node %d sees foreign container %q",
						i, endpoint.ContainerID)
				}
				if endpoint.AdvertiseAddr != advertise {
					t.Errorf("node %d endpoint advertise = %q, want %q",
						i, endpoint.AdvertiseAddr, advertise)
				}
			}
		}(i)
	}
	wg.Wait()

	all := readRegistryEndpoints(t, handles[0], ServiceRegistryFilter{})
	if len(all) != nodeCount*endpointsPerNode {
		t.Fatalf("combined endpoints = %d, want %d (the disjoint union)",
			len(all), nodeCount*endpointsPerNode)
	}
	seen := map[string]string{}
	for _, endpoint := range all {
		if other, dup := seen[endpoint.ContainerID]; dup {
			t.Fatalf("container %s published by both %q and %q",
				endpoint.ContainerID, other, endpoint.NodeID)
		}
		seen[endpoint.ContainerID] = endpoint.NodeID
	}
}

// Given the registry schema, when a writer violates a table constraint
// directly, then PostgreSQL rejects it: positive incarnation and an OBJECT
// typed payload on nodes, the node primary key, and on the endpoints the
// positive replica, non-empty identity columns, OBJECT payload, composite
// primary key, and the node foreign key.
func TestIntegrationNodeRegistrySchemaConstraints(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	st := openWithIdentity(t, dsn, uniqueNodeID(t, "node-schema"), "inst-1")
	ctx := context.Background()
	lease := mustClaimNodeLease(t, st,
		NodeLeaseMetadata{AdvertiseAddr: "10.0.0.5:7001"}, time.Minute)

	const endpointInsert = `INSERT INTO service_registry_endpoints
		(node_id, incarnation, app_name, service_name, replica, host, path,
		 container_id, expires_at, updated_at, data)
		VALUES ($1, $2, $3, $4, $5, $6, '', $7,
		 now() + interval '1 hour', now(), $8::jsonb)`
	for _, tc := range []struct {
		name      string
		nodeID    string
		app       string
		service   string
		replica   int
		host      string
		container string
		data      string
		want      string
	}{
		{"negative replica", lease.NodeID, "alpha", "web", -1,
			"h.example.com", "ctr-1", `{}`, "violates check constraint"},
		{"empty app name", lease.NodeID, "", "web", 0,
			"h.example.com", "ctr-1", `{}`, "violates check constraint"},
		{"empty service name", lease.NodeID, "alpha", "", 0,
			"h.example.com", "ctr-1", `{}`, "violates check constraint"},
		{"empty host", lease.NodeID, "alpha", "web", 0,
			"", "ctr-1", `{}`, "violates check constraint"},
		{"empty container", lease.NodeID, "alpha", "web", 0,
			"h.example.com", "", `{}`, "violates check constraint"},
		{"non-object payload", lease.NodeID, "alpha", "web", 0,
			"h.example.com", "ctr-1", `"scalar"`, "violates check constraint"},
		{"foreign node", "ghost-node", "alpha", "web", 0,
			"h.example.com", "ctr-1", `{}`, "violates foreign key constraint"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := st.db.ExecContext(ctx, endpointInsert,
				tc.nodeID, lease.Incarnation, tc.app, tc.service,
				tc.replica, tc.host, tc.container, tc.data)
			if err == nil {
				t.Fatalf("insert succeeded, want %q rejection", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q must report %q", err, tc.want)
			}
		})
	}

	t.Run("duplicate endpoint identity", func(t *testing.T) {
		args := []any{lease.NodeID, lease.Incarnation, "alpha", "web", 0,
			"dup.example.com", "ctr-dup", `{}`}
		if _, err := st.db.ExecContext(ctx, endpointInsert, args...); err != nil {
			t.Fatalf("first insert: %v", err)
		}
		_, err := st.db.ExecContext(ctx, endpointInsert, args...)
		if err == nil ||
			!strings.Contains(err.Error(),
				"duplicate key value violates unique constraint") {
			t.Fatalf("second insert = %v, want the primary-key rejection", err)
		}
	})

	const nodeInsert = `INSERT INTO nodes
		(node_id, instance, incarnation, heartbeat_at, lease_expires_at, data)
		VALUES ($1, 'inst-x', $2, now(),
		 now() + interval '1 hour', $3::jsonb)`
	t.Run("zero incarnation", func(t *testing.T) {
		_, err := st.db.ExecContext(ctx, nodeInsert,
			uniqueNodeID(t, "node-ghost"), int64(0), `{}`)
		if err == nil || !strings.Contains(err.Error(), "violates check constraint") {
			t.Fatalf("insert = %v, want the incarnation check rejection", err)
		}
	})
	t.Run("non-object node payload", func(t *testing.T) {
		_, err := st.db.ExecContext(ctx, nodeInsert,
			uniqueNodeID(t, "node-ghost"), int64(1), `"scalar"`)
		if err == nil || !strings.Contains(err.Error(), "violates check constraint") {
			t.Fatalf("insert = %v, want the jsonb_typeof check rejection", err)
		}
	})
	t.Run("duplicate node identity", func(t *testing.T) {
		node := uniqueNodeID(t, "node-dup")
		if _, err := st.db.ExecContext(ctx, nodeInsert, node, int64(1),
			`{}`); err != nil {
			t.Fatalf("first insert: %v", err)
		}
		_, err := st.db.ExecContext(ctx, nodeInsert, node, int64(2), `{}`)
		if err == nil ||
			!strings.Contains(err.Error(),
				"duplicate key value violates unique constraint") {
			t.Fatalf("second insert = %v, want the primary-key rejection", err)
		}
	})
}

// Given a stored endpoint payload corrupted by a direct SQL writer (the only
// way the typed decoder can be bypassed), when a read runs, then it fails
// loudly instead of returning silently wrong endpoint facts.
func TestIntegrationServiceRegistryReadSurfacesMalformedEndpointPayload(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	node := uniqueNodeID(t, "node-corrupt-endpoint")
	st := openWithIdentity(t, dsn, node, "inst-1")
	ctx := context.Background()

	lease := mustClaimNodeLease(t, st,
		NodeLeaseMetadata{AdvertiseAddr: "10.0.0.5:7001"}, time.Minute)
	publishRegistryBatch(t, st, lease,
		[]ServiceRegistryEndpoint{testRegistryEndpoint("alpha", "web", "ctr-1")})
	execNodeRegistrySQL(t, st,
		`UPDATE service_registry_endpoints
		    SET data = '{"host_port":"not-a-number"}'::jsonb
		  WHERE node_id = $1`, node)

	endpoints, err := st.ReadEligibleServiceRegistryEndpoints(ctx,
		ServiceRegistryFilter{})
	if err == nil {
		t.Fatalf("read of a malformed payload = %v, want a decode error", endpoints)
	}
	if !strings.Contains(err.Error(), "decode endpoint") {
		t.Fatalf("error %q must identify the endpoint decode failure", err)
	}
	if endpoints != nil {
		t.Fatalf("endpoints = %v, want no partial result alongside the error",
			endpoints)
	}
}

// Given nodes.data corrupted by a direct SQL writer, when a read runs (reads
// decode the node metadata for AdvertiseAddr), then it fails loudly instead of
// returning silently wrong node metadata.
func TestIntegrationNodeRegistryReadSurfacesMalformedMetadata(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	node := uniqueNodeID(t, "node-corrupt-metadata")
	st := openWithIdentity(t, dsn, node, "inst-1")
	ctx := context.Background()

	lease := mustClaimNodeLease(t, st,
		NodeLeaseMetadata{AdvertiseAddr: "10.0.0.5:7001"}, time.Minute)
	publishRegistryBatch(t, st, lease,
		[]ServiceRegistryEndpoint{testRegistryEndpoint("alpha", "web", "ctr-1")})
	execNodeRegistrySQL(t, st,
		`UPDATE nodes SET data = '{"owner_pid":"not-a-number"}'::jsonb
		  WHERE node_id = $1`, node)

	endpoints, err := st.ReadEligibleServiceRegistryEndpoints(ctx,
		ServiceRegistryFilter{})
	if err == nil {
		t.Fatalf("read with malformed node metadata = %v, want a decode error",
			endpoints)
	}
	if !strings.Contains(err.Error(), "decode metadata") {
		t.Fatalf("error %q must identify the metadata decode failure", err)
	}
	if endpoints != nil {
		t.Fatalf("endpoints = %v, want no partial result alongside the error",
			endpoints)
	}
}

// Given a mixed registry (an active node with live, stale-incarnation, and
// self-expired endpoints; an expired node holding endpoints), when one bounded
// prune runs, then it removes exactly the expired endpoint rows — under an
// ACTIVE node too — keeps the active node and its live endpoint, and DEFERS the
// expired node while one of its endpoint rows is still locked away: an expired
// node is deleted only in a pass where no endpoint row remains attached (a
// later pass removes that endpoint and then the node in the same transaction).
func TestIntegrationPruneExpiredNodeRegistryRetainsActiveRows(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	nodeA := uniqueNodeID(t, "prune-active")
	st := openWithIdentity(t, dsn, nodeA, "inst-a")
	ctx := context.Background()

	leaseA := mustClaimNodeLease(t, st,
		NodeLeaseMetadata{AdvertiseAddr: "10.0.0.1:7001"}, time.Minute)
	live := testRegistryEndpoint("alpha", "web", "ctr-live")
	stale := testRegistryEndpoint("alpha", "web", "ctr-stale")
	selfExpired := testRegistryEndpoint("alpha", "api", "ctr-expired")
	publishRegistryBatch(t, st, leaseA,
		[]ServiceRegistryEndpoint{live, stale, selfExpired})
	// A leftover row from an old incarnation, attached to the ACTIVE node.
	execNodeRegistrySQL(t, st,
		`UPDATE service_registry_endpoints
		    SET incarnation = incarnation + 999
		  WHERE node_id = $1 AND container_id = 'ctr-stale'`, nodeA)
	// An endpoint whose OWN expiry lapsed while its node stays active.
	execNodeRegistrySQL(t, st,
		`UPDATE service_registry_endpoints
		    SET expires_at = now() - interval '1 minute'
		  WHERE node_id = $1 AND container_id = 'ctr-expired'`, nodeA)

	// An expired node D whose endpoint row exists but is LOCKED elsewhere: the
	// prune must skip it (SKIP LOCKED) and must NOT delete the node while that
	// endpoint row is attached.
	nodeD := uniqueNodeID(t, "prune-deferred")
	execNodeRegistrySQL(t, st,
		`INSERT INTO nodes (node_id, instance, incarnation, heartbeat_at,
		    lease_expires_at, data)
		  VALUES ($1, 'inst-d', 7, now() - interval '1 hour',
		   now() - interval '1 hour', '{}'::jsonb)`, nodeD)
	execNodeRegistrySQL(t, st,
		`INSERT INTO service_registry_endpoints (node_id, incarnation,
		    app_name, service_name, replica, host, path, container_id,
		    expires_at, updated_at, data)
		  VALUES ($1, 7, 'alpha', 'deferred', 0, 'd.example.com', '',
		   'ctr-deferred', now() + interval '1 hour', now(), '{}'::jsonb)`,
		nodeD)

	lockTx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin lock tx: %v", err)
	}
	if _, err := lockTx.ExecContext(ctx,
		`SELECT ctid FROM service_registry_endpoints
		  WHERE node_id = $1 AND container_id = 'ctr-deferred' FOR UPDATE`,
		nodeD); err != nil {
		t.Fatalf("lock the deferred endpoint row: %v", err)
	}

	first, err := st.PruneExpiredNodeRegistry(ctx)
	if err != nil {
		t.Fatalf("prune while a row is locked: %v", err)
	}
	if first.Endpoints != 2 || first.Nodes != 0 {
		t.Fatalf("prune = %+v, want {2 endpoints, 0 nodes}: exactly the "+
			"stale-incarnation and self-expired rows under the active node, "+
			"with the locked row skipped and the deferred node retained",
			first)
	}
	if n := countRows(t, st,
		`SELECT COUNT(*) FROM nodes WHERE node_id = $1`, nodeD); n != 1 {
		t.Fatalf("deferred node rows = %d, want it retained while an endpoint "+
			"row remains attached", n)
	}
	if n := countRows(t, st,
		`SELECT COUNT(*) FROM service_registry_endpoints
		  WHERE node_id = $1`, nodeD); n != 1 {
		t.Fatalf("deferred endpoint rows = %d, want the locked row retained", n)
	}
	requireRegistryContainers(t,
		readRegistryEndpoints(t, st, ServiceRegistryFilter{}), "ctr-live")

	if err := lockTx.Commit(); err != nil {
		t.Fatalf("commit lock tx: %v", err)
	}
	second, err := st.PruneExpiredNodeRegistry(ctx)
	if err != nil {
		t.Fatalf("prune after the lock released: %v", err)
	}
	if second.Endpoints != 1 || second.Nodes != 1 {
		t.Fatalf("prune = %+v, want {1 endpoint, 1 node}: the deferred endpoint "+
			"and THEN its now-endpoint-free expired node in the same transaction",
			second)
	}
	if n := countRows(t, st,
		`SELECT COUNT(*) FROM nodes WHERE node_id = $1`, nodeD); n != 0 {
		t.Fatalf("deferred node rows = %d, want it deleted once its endpoints "+
			"were gone", n)
	}

	// The active node and its live endpoint survived every pass.
	requireRegistryContainers(t,
		readRegistryEndpoints(t, st, ServiceRegistryFilter{}), "ctr-live")
	third, err := st.PruneExpiredNodeRegistry(ctx)
	if err != nil {
		t.Fatalf("prune idempotence: %v", err)
	}
	if third.Endpoints != 0 || third.Nodes != 0 {
		t.Fatalf("final prune = %+v, want {0, 0} (idempotent)", third)
	}
}

// Given a backlog larger than the batch caps, when prune runs, then ONE
// invocation deletes at most NodeRegistryPruneEndpointBatch endpoints and
// NodeRegistryPruneNodeBatch nodes — draining gradually across invocations —
// while live nodes and live endpoints are never selected.
func TestIntegrationPruneExpiredNodeRegistryBoundedCaps(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	nodeL := uniqueNodeID(t, "prune-caps")
	st := openWithIdentity(t, dsn, nodeL, "inst-l")
	ctx := context.Background()

	leaseL := mustClaimNodeLease(t, st,
		NodeLeaseMetadata{AdvertiseAddr: "10.0.0.1:7001"}, time.Minute)
	publishRegistryBatch(t, st, leaseL,
		[]ServiceRegistryEndpoint{testRegistryEndpoint("alpha", "web", "ctr-live")})

	const expiredEndpoints = NodeRegistryPruneEndpointBatch + 1 // 1001
	execNodeRegistrySQL(t, st,
		`INSERT INTO service_registry_endpoints (node_id, incarnation,
		    app_name, service_name, replica, host, path, container_id,
		    expires_at, updated_at, data)
		  SELECT $1, $2, 'alpha', 'backlog', g::int % 5, 'h.example.com', '',
		   'ctr-' || g, now() - interval '1 minute', now(), '{}'::jsonb
		    FROM generate_series(1, $3::int) g`,
		nodeL, leaseL.Incarnation, expiredEndpoints)

	const expiredNodes = NodeRegistryPruneNodeBatch + 5 // 105
	execNodeRegistrySQL(t, st,
		`INSERT INTO nodes (node_id, instance, incarnation, heartbeat_at,
		    lease_expires_at, data)
		  SELECT 'stale-' || g, 'inst-stale', g,
		   now() - interval '1 hour', now() - interval '1 hour', '{}'::jsonb
		    FROM generate_series(1, $1::bigint) g`, int64(expiredNodes))

	first, err := st.PruneExpiredNodeRegistry(ctx)
	if err != nil {
		t.Fatalf("prune #1: %v", err)
	}
	if first.Endpoints != NodeRegistryPruneEndpointBatch ||
		first.Nodes != NodeRegistryPruneNodeBatch {
		t.Fatalf("prune #1 = %+v, want exactly the bounded caps "+
			"{%d endpoints, %d nodes} over a larger backlog",
			first, NodeRegistryPruneEndpointBatch, NodeRegistryPruneNodeBatch)
	}

	second, err := st.PruneExpiredNodeRegistry(ctx)
	if err != nil {
		t.Fatalf("prune #2: %v", err)
	}
	if second.Endpoints != expiredEndpoints-NodeRegistryPruneEndpointBatch ||
		second.Nodes != expiredNodes-NodeRegistryPruneNodeBatch {
		t.Fatalf("prune #2 = %+v, want the remainder {%d endpoints, %d nodes}",
			second, expiredEndpoints-NodeRegistryPruneEndpointBatch,
			expiredNodes-NodeRegistryPruneNodeBatch)
	}

	third, err := st.PruneExpiredNodeRegistry(ctx)
	if err != nil {
		t.Fatalf("prune #3: %v", err)
	}
	if third.Endpoints != 0 || third.Nodes != 0 {
		t.Fatalf("prune #3 = %+v, want {0, 0} once the backlog drained", third)
	}

	// The live node and its live endpoint were never selectable.
	if n := countRows(t, st,
		`SELECT COUNT(*) FROM nodes WHERE node_id = $1`, nodeL); n != 1 {
		t.Fatalf("live node rows = %d, want 1", n)
	}
	requireRegistryContainers(t,
		readRegistryEndpoints(t, st, ServiceRegistryFilter{}), "ctr-live")
	if n := countRows(t, st,
		`SELECT COUNT(*) FROM service_registry_endpoints
		  WHERE node_id = $1`, nodeL); n != 1 {
		t.Fatalf("live node endpoint rows = %d, want only the live one", n)
	}
}

// Given an expired backlog pruned by CONCURRENT invocations (while another node
// heartbeats its lease live), then the per-row totals partition exactly across
// invocations — no row deleted twice, none missed — a follow-up prune finds
// nothing (idempotence), and the live node and its endpoints are never touched.
func TestIntegrationPruneExpiredNodeRegistryConcurrentAndIdempotent(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	nodeL := uniqueNodeID(t, "prune-race-live")
	stL := openWithIdentity(t, dsn, nodeL, "inst-l")
	nodeB := uniqueNodeID(t, "prune-race-heartbeat")
	stB := openWithIdentity(t, dsn, nodeB, "inst-b")
	ctx := context.Background()

	leaseL := mustClaimNodeLease(t, stL,
		NodeLeaseMetadata{AdvertiseAddr: "10.0.0.1:7001"}, time.Minute)
	publishRegistryBatch(t, stL, leaseL,
		[]ServiceRegistryEndpoint{testRegistryEndpoint("alpha", "web", "ctr-live")})

	const expiredEndpoints = 200
	execNodeRegistrySQL(t, stL,
		`INSERT INTO service_registry_endpoints (node_id, incarnation,
		    app_name, service_name, replica, host, path, container_id,
		    expires_at, updated_at, data)
		  SELECT $1, $2, 'alpha', 'backlog', g::int % 5, 'h.example.com', '',
		   'ctr-' || g, now() - interval '1 minute', now(), '{}'::jsonb
		    FROM generate_series(1, $3::int) g`,
		nodeL, leaseL.Incarnation, expiredEndpoints)
	const expiredNodes = 30
	execNodeRegistrySQL(t, stL,
		`INSERT INTO nodes (node_id, instance, incarnation, heartbeat_at,
		    lease_expires_at, data)
		  SELECT 'stale-' || g, 'inst-stale', 1000 + g,
		   now() - interval '1 hour', now() - interval '1 hour', '{}'::jsonb
		    FROM generate_series(1, $1::bigint) g`, int64(expiredNodes))

	// A live node B keeps heartbeating and publishing while prunes run: its
	// lease is never expired by the database clock, so nothing may delete it.
	leaseB, err := stB.ClaimNodeLease(ctx, NodeLeaseMetadata{
		AdvertiseAddr: "10.0.0.2:7002"}, time.Minute)
	if err != nil {
		t.Fatalf("claim node B: %v", err)
	}
	publishRegistryBatch(t, stB, leaseB,
		[]ServiceRegistryEndpoint{testRegistryEndpoint("beta", "web", "ctr-b")})

	const (
		pruneWorkers    = 4
		prunesPerWorker = 3
	)
	var (
		wg           sync.WaitGroup
		mu           sync.Mutex
		sumEndpoints int64
		sumNodes     int64
		pruneErrs    []error
	)
	for w := 0; w < pruneWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < prunesPerWorker; i++ {
				res, err := stL.PruneExpiredNodeRegistry(ctx)
				mu.Lock()
				if err != nil {
					pruneErrs = append(pruneErrs, err)
				} else {
					sumEndpoints += res.Endpoints
					sumNodes += res.Nodes
				}
				mu.Unlock()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			if _, err := stB.HeartbeatNodeLease(ctx, leaseB); err != nil {
				t.Errorf("live heartbeat during prune: %v", err)
				return
			}
		}
	}()
	wg.Wait()

	for _, err := range pruneErrs {
		t.Errorf("concurrent prune error: %v", err)
	}
	if sumEndpoints != expiredEndpoints {
		t.Fatalf("pruned endpoints across invocations = %d, want exactly %d "+
			"(each expired row deleted once, none missed)", sumEndpoints,
			expiredEndpoints)
	}
	if sumNodes != expiredNodes {
		t.Fatalf("pruned nodes across invocations = %d, want exactly %d "+
			"(each expired node deleted once, none missed)", sumNodes, expiredNodes)
	}
	final, err := stL.PruneExpiredNodeRegistry(ctx)
	if err != nil {
		t.Fatalf("final prune: %v", err)
	}
	if final.Endpoints != 0 || final.Nodes != 0 {
		t.Fatalf("final prune = %+v, want {0, 0} (idempotent)", final)
	}

	// The concurrently-heartbeating node and both live endpoints survived.
	if n := countRows(t, stL,
		`SELECT COUNT(*) FROM nodes WHERE node_id IN ($1, $2)`,
		nodeL, nodeB); n != 2 {
		t.Fatalf("live node rows = %d, want both live nodes retained", n)
	}
	requireRegistryContainers(t,
		readRegistryEndpoints(t, stL, ServiceRegistryFilter{NodeID: nodeL}),
		"ctr-live")
	requireRegistryContainers(t,
		readRegistryEndpoints(t, stB, ServiceRegistryFilter{NodeID: nodeB}),
		"ctr-b")
}

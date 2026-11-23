package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"time"

	"relay/internal/app"
)

// Global node registry and service endpoint publication
//
// The nodes table is the PostgreSQL-backed, lease-fenced registration of every
// live Relay worker, independent of whether the worker publishes service
// endpoints. Endpoint publication consumes that same node lease; it never owns
// a second lease or heartbeat.
//
// Every node/endpoint write is fenced twice in PostgreSQL, at the operation
// boundary:
//
//   - the caller's PINNED lock backend (owner_pid, captured by Open) must still
//     hold the node's session advisory lock. The check queries pg_locks for that
//     exact backend and key; a process-local VerifyOwnerLock result is not
//     sufficient, because it proves the state at an earlier instant.
//   - the stored lease incarnation and process instance must still be the
//     caller's. A claim allocates a fresh incarnation, so a successor fences the
//     previous process out even before it publishes endpoints.
//
// The advisory-lock check closes the interval between a lost lock and a
// successor's claim: an old process's writes fail as soon as pg_locks no longer
// shows its backend. A lease expiry timestamp alone never authorizes a
// concurrent process while the advisory lock is held; only ownership of that
// lock admits a new claim.
//
// Node availability is the current incarnation plus an unexpired lease measured
// by the PostgreSQL clock. There is no status column. Crashed processes need no
// cleanup writer: readers stop seeing their endpoints when the lease expires.
// The standalone heartbeat is the ONLY lease-renewal owner; endpoint publication
// consumes the lease, requires it to be unexpired at the fenced operation
// boundary, and refreshes only each endpoint's own expiry.
// Service registry endpoints are a MATERIALIZED VIEW of the Docker lifecycle
// only; they never drive matching, builds, reconciliation, or execution.

// ErrNodeLeaseFence reports that a fenced node-registry write was refused: the
// node lease no longer belongs to this process/incarnation, the pinned backend
// no longer holds the node advisory lock, or (for endpoint publication) the
// lease has expired. It is a normal, recoverable rejection, never a database
// failure.
var ErrNodeLeaseFence = errors.New("node lease fence lost")

// Bounded registry cleanup batch sizes. One cleanup invocation deletes at most
// these many rows, so a large stale backlog is drained gradually without a long
// transaction or an unbounded lock footprint.
const (
	NodeRegistryPruneEndpointBatch = 1000
	NodeRegistryPruneNodeBatch     = 100
)

// NodeRegistryPruneResult reports how many stale rows one bounded cleanup
// invocation removed.
type NodeRegistryPruneResult struct {
	Endpoints int64
	Nodes     int64
}

// NodeLeaseMetadata is the noncritical metadata stored in nodes.data. The
// identity, incarnation fence, and lease timestamps are relational; bind and
// advertise addresses are presentation/diagnostic metadata and have no
// relational duplicate. OwnerPID is added from the state handle, not the caller.
type NodeLeaseMetadata struct {
	BindAddr      string
	AdvertiseAddr string
}

// NodeLease is one node's ownership of its registry rows. The exported fields
// are operator/logging metadata; the unexported fence is the PostgreSQL-enforced
// proof and cannot be constructed outside this package.
type NodeLease struct {
	NodeID        string
	Instance      string
	Incarnation   int64
	BindAddr      string
	AdvertiseAddr string
	ExpiresAt     time.Time

	fence nodeLeaseFence
	ttl   time.Duration
}

// ServiceRegistryEndpoint is one routable service replica published by a node.
// Host is the service's effective routing hostname: the declared template host,
// or the exact ROUTING_HOST_OVERRIDE value when configured. It intentionally
// carries only routing/metadata: no environment, secret, or handler payload ever
// enters the registry.
type ServiceRegistryEndpoint struct {
	// NodeID and AdvertiseAddr are filled by ReadEligible... and ignored by
	// Publish, which takes them from the fenced node lease.
	NodeID        string
	AdvertiseAddr string

	App          string
	Service      string
	Replica      int
	Host         string
	Path         string
	HostIP       string
	HostPort     int
	InternalPort int
	ContainerID  string
	Generation   string
}

// ServiceRegistryFilter narrows ReadEligibleServiceRegistryEndpoints. Empty
// fields are wildcards.
type ServiceRegistryFilter struct {
	NodeID  string
	App     string
	Service string
	Host    string
	Path    string
}

// nodeRegistryData is the typed JSONB payload of nodes.data. Every field is
// noncritical metadata; none duplicates a relational column.
type nodeRegistryData struct {
	BindAddr      string `json:"bind_addr"`
	AdvertiseAddr string `json:"advertise_addr"`
	OwnerPID      int    `json:"owner_pid"`
}

// serviceRegistryEndpointPayload is the cohesive JSONB attribute set of one
// endpoint. The row's identity (node/app/service/replica/container), the routing
// keys (host/path), the incarnation fence, and the expiry are relational; these
// are the endpoint facts that may grow without a schema change. No value here
// duplicates a relational column, and no environment/secret data may enter it.
type serviceRegistryEndpointPayload struct {
	HostIP       string `json:"host_ip"`
	HostPort     int    `json:"host_port"`
	InternalPort int    `json:"internal_port"`
	Generation   string `json:"generation"`
}

type nodeLeaseFence struct {
	nodeID      string
	instance    string
	incarnation int64
	ownerPID    int
	lockKey     int64
}

func encodeNodeRegistryData(metadata NodeLeaseMetadata, ownerPID int) (string, error) {
	data, err := json.Marshal(nodeRegistryData{
		BindAddr:      metadata.BindAddr,
		AdvertiseAddr: metadata.AdvertiseAddr,
		OwnerPID:      ownerPID,
	})
	if err != nil {
		return "", fmt.Errorf("node registry: encode metadata: %w", err)
	}
	return string(data), nil
}

func decodeNodeRegistryData(data string) (nodeRegistryData, error) {
	var payload nodeRegistryData
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		return nodeRegistryData{}, fmt.Errorf("node registry: decode metadata: %w", err)
	}
	return payload, nil
}

func encodeServiceRegistryEndpointPayload(
	endpoint ServiceRegistryEndpoint,
) (string, error) {
	data, err := json.Marshal(serviceRegistryEndpointPayload{
		HostIP:       endpoint.HostIP,
		HostPort:     endpoint.HostPort,
		InternalPort: endpoint.InternalPort,
		Generation:   endpoint.Generation,
	})
	if err != nil {
		return "", fmt.Errorf("service registry: encode endpoint: %w", err)
	}
	return string(data), nil
}

func decodeServiceRegistryEndpointPayload(data string) (serviceRegistryEndpointPayload, error) {
	var payload serviceRegistryEndpointPayload
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		return serviceRegistryEndpointPayload{}, fmt.Errorf(
			"service registry: decode endpoint: %w", err)
	}
	return payload, nil
}

type serviceRegistryEndpointKey struct {
	app         string
	service     string
	replica     int
	containerID string
}

// uniqueServiceRegistryEndpoints returns the batch with one row per relational
// identity, keeping the LAST occurrence (the caller's most recent view).
func uniqueServiceRegistryEndpoints(
	endpoints []ServiceRegistryEndpoint,
) []ServiceRegistryEndpoint {
	if len(endpoints) < 2 {
		return endpoints
	}
	seen := make(map[serviceRegistryEndpointKey]int, len(endpoints))
	unique := make([]ServiceRegistryEndpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		key := serviceRegistryEndpointKey{
			app:         endpoint.App,
			service:     endpoint.Service,
			replica:     endpoint.Replica,
			containerID: endpoint.ContainerID,
		}
		if index, ok := seen[key]; ok {
			unique[index] = endpoint
			continue
		}
		seen[key] = len(unique)
		unique = append(unique, endpoint)
	}
	return unique
}

// ClaimNodeLease claims (or reclaims) this node's registry lease and records a
// fresh incarnation. The claim is accepted only while the pinned lock backend
// still holds the node advisory lock. It deletes endpoint rows left by a
// previous incarnation in the same transaction, so restart recovery is atomic.
// A lease that expired is irrelevant here: ownership of the advisory lock, never
// expiry, authorizes a claim.
func (st *State) ClaimNodeLease(
	ctx context.Context,
	metadata NodeLeaseMetadata,
	ttl time.Duration,
) (NodeLease, error) {
	if st == nil || st.nodeID == "" || st.instance == "" {
		return NodeLease{}, fmt.Errorf(
			"node registry: claim requires a node-owned state handle")
	}
	if st.ownerPID <= 0 {
		return NodeLease{}, fmt.Errorf(
			"node registry: claim requires the node lock backend")
	}
	if ttl <= 0 {
		return NodeLease{}, fmt.Errorf("node registry: lease TTL must be positive")
	}
	data, err := encodeNodeRegistryData(metadata, st.ownerPID)
	if err != nil {
		return NodeLease{}, err
	}
	fence := nodeLeaseFence{
		nodeID:   st.nodeID,
		instance: st.instance,
		ownerPID: st.ownerPID,
		lockKey:  nodeLockKey(st.nodeID),
	}
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		return NodeLease{}, fmt.Errorf("node registry: begin claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var (
		incarnation int64
		expiresAt   time.Time
	)
	err = tx.QueryRowContext(ctx, claimNodeLeaseQuery,
		fence.nodeID, fence.instance, fence.ownerPID, fence.lockKey,
		ttl.Seconds(), data,
	).Scan(&incarnation)
	if errors.Is(err, sql.ErrNoRows) {
		return NodeLease{}, fmt.Errorf(
			"%w: node %q advisory lock is not held by backend %d",
			ErrNodeLeaseFence, fence.nodeID, fence.ownerPID)
	}
	if err != nil {
		return NodeLease{}, fmt.Errorf("node registry: claim lease: %w", err)
	}
	fence.incarnation = incarnation
	// The claim INSERT may have waited on a conflicting row (and, on a
	// conflict-delete, inserted values computed before that wait). Re-stamp the
	// owned lease with a fresh database instant now that the row is locked, so a
	// delayed claimant never starts with an already-shortened TTL.
	err = tx.QueryRowContext(ctx, refreshClaimedNodeLeaseQuery,
		fence.nodeID, fence.instance, fence.incarnation,
		ttl.Seconds(), fence.ownerPID, fence.lockKey,
	).Scan(&expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return NodeLease{}, fmt.Errorf(
			"%w: node %q claim could not be refreshed",
			ErrNodeLeaseFence, fence.nodeID)
	}
	if err != nil {
		return NodeLease{}, fmt.Errorf("node registry: refresh claim: %w", err)
	}
	// Startup/restart recovery: rows from a superseded incarnation are invisible
	// to readers immediately (they join on incarnation) and are removed here so
	// they cannot accumulate.
	if _, err := tx.ExecContext(ctx, deleteSupersededNodeEndpointsQuery,
		fence.nodeID, incarnation); err != nil {
		return NodeLease{}, fmt.Errorf(
			"node registry: clear superseded endpoints: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return NodeLease{}, fmt.Errorf("node registry: commit claim: %w", err)
	}
	return NodeLease{
		NodeID:        fence.nodeID,
		Instance:      fence.instance,
		Incarnation:   incarnation,
		BindAddr:      metadata.BindAddr,
		AdvertiseAddr: metadata.AdvertiseAddr,
		ExpiresAt:     expiresAt,
		fence:         fence,
		ttl:           ttl,
	}, nil
}

// HeartbeatNodeLease extends the lease only while it still belongs to this
// process/incarnation and the pinned backend still holds the node advisory
// lock. It is one bounded statement and deliberately does NOT require an
// unexpired lease: a heartbeat delayed past the TTL still renews the current
// incarnation while its lock is held, so cleanup must not have erased the row.
// A fence rejection is reported as ErrNodeLeaseFence; every other error is a
// database failure the caller may retry with the same lease.
func (st *State) HeartbeatNodeLease(
	ctx context.Context,
	lease NodeLease,
) (NodeLease, error) {
	fence, err := st.checkNodeLease(lease)
	if err != nil {
		return NodeLease{}, err
	}
	var expiresAt time.Time
	err = st.db.QueryRowContext(ctx, heartbeatNodeLeaseQuery,
		fence.nodeID, fence.instance, fence.incarnation,
		lease.ttl.Seconds(), fence.ownerPID, fence.lockKey,
	).Scan(&expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return NodeLease{}, fmt.Errorf(
			"%w: heartbeat node %q incarnation %d",
			ErrNodeLeaseFence, fence.nodeID, fence.incarnation)
	}
	if err != nil {
		return NodeLease{}, fmt.Errorf("node registry: heartbeat: %w", err)
	}
	lease.ExpiresAt = expiresAt
	return lease, nil
}

// ReleaseNodeLease withdraws ONLY this incarnation's endpoints and releases
// ONLY this incarnation's node lease in one bounded transaction. It is fenced
// exactly like a write, so a process whose ownership was already lost can never
// invalidate or delete a successor's rows. A missing row is an idempotent
// no-op. It runs even when no endpoints were ever published.
func (st *State) ReleaseNodeLease(
	ctx context.Context,
	lease NodeLease,
) error {
	fence, err := st.checkNodeLease(lease)
	if err != nil {
		return err
	}
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("node registry: begin release: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var lockedIncarnation int64
	err = tx.QueryRowContext(ctx, lockNodeLeaseQuery,
		fence.nodeID, fence.instance, fence.incarnation,
		fence.ownerPID, fence.lockKey,
	).Scan(&lockedIncarnation)
	if errors.Is(err, sql.ErrNoRows) {
		// Already released, or superseded by a successor: an idempotent no-op
		// that never touches the successor's rows.
		return nil
	}
	if err != nil {
		return fmt.Errorf("node registry: verify release fence: %w", err)
	}
	if _, err := tx.ExecContext(ctx, deleteIncarnationEndpointsQuery,
		fence.nodeID, fence.incarnation); err != nil {
		return fmt.Errorf("node registry: withdraw endpoints: %w", err)
	}
	// The node row delete cascades any endpoint row still attached to this node.
	// The fenced row lock above proved no successor owns the identity, so the
	// cascade can only remove this node's own stale rows, never a successor's.
	if _, err := tx.ExecContext(ctx, releaseNodeLeaseQuery,
		fence.nodeID, fence.instance, fence.incarnation); err != nil {
		return fmt.Errorf("node registry: release lease: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("node registry: commit release: %w", err)
	}
	return nil
}

// PruneExpiredNodeRegistry removes at most one bounded batch of stale registry
// rows in one transaction: expired endpoint rows (including endpoints belonging
// to a currently active node) first, then node rows whose DB-time lease has
// expired. Deleting an expired node row cascades any endpoints still attached to
// it. Concurrent claim/heartbeat/publication transactions are handled by
// FOR UPDATE SKIP LOCKED plus a re-check of the expiry predicate against fresh
// database time, so a row renewed concurrently is retained and a live successor
// is never removed.
//
// An expired row is retained while the node's exact exclusive advisory key is
// currently held in pg_locks. That is the "heartbeat was delayed past TTL, but
// the process is alive and still owns the identity" case: visibility already
// ends at expiry, and cleanup must not erase the registration the heartbeat is
// about to renew. Ownership is proved by the exact per-node key, never by PID or
// nodes.data: malformed metadata cannot block cleanup, and a row is reclaimed
// once the lock is released.
//
// It is a global maintenance operation owned by the node-registry lifecycle; it
// needs no process fence because every deletion is conditioned on expiry. It
// returns the number of rows removed so the caller can log only when useful.
func (st *State) PruneExpiredNodeRegistry(
	ctx context.Context,
) (NodeRegistryPruneResult, error) {
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		return NodeRegistryPruneResult{}, fmt.Errorf("node registry: begin prune: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx, pruneExpiredServiceRegistryEndpointsQuery,
		NodeRegistryPruneEndpointBatch)
	if err != nil {
		return NodeRegistryPruneResult{}, fmt.Errorf(
			"node registry: prune expired endpoints: %w", err)
	}
	endpoints, err := res.RowsAffected()
	if err != nil {
		return NodeRegistryPruneResult{}, fmt.Errorf(
			"node registry: prune expired endpoints: %w", err)
	}
	nodes, err := pruneExpiredNodeRows(ctx, tx, NodeRegistryPruneNodeBatch)
	if err != nil {
		return NodeRegistryPruneResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return NodeRegistryPruneResult{}, fmt.Errorf("node registry: commit prune: %w", err)
	}
	return NodeRegistryPruneResult{Endpoints: endpoints, Nodes: nodes}, nil
}

// expiredNodePruneCandidate is one locked expired node row. Its advisory key is
// derived from the node identity; nodes.data is deliberately not decoded, so
// malformed metadata cannot poison the cleanup transaction.
type expiredNodePruneCandidate struct {
	nodeID  string
	lockKey int64
}

// pruneExpiredNodeRows locks one bounded batch of endpoint-free expired node
// rows, excludes rows whose exact per-node exclusive advisory key is currently
// held, and deletes the rest. The row locks held by the selection serialize a
// concurrent heartbeat or successor claim for the remainder of the transaction,
// and the delete re-checks expiry against fresh database time.
func pruneExpiredNodeRows(
	ctx context.Context, tx *sql.Tx, limit int,
) (int64, error) {
	rows, err := tx.QueryContext(ctx, selectExpiredNodesForPruneQuery, limit)
	if err != nil {
		return 0, fmt.Errorf("node registry: select expired nodes: %w", err)
	}
	candidates := make([]expiredNodePruneCandidate, 0, limit)
	for rows.Next() {
		var nodeID string
		if err := rows.Scan(&nodeID); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("node registry: scan expired node: %w", err)
		}
		candidates = append(candidates, expiredNodePruneCandidate{
			nodeID:  nodeID,
			lockKey: nodeLockKey(nodeID),
		})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("node registry: select expired nodes: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("node registry: select expired nodes: %w", err)
	}
	if len(candidates) == 0 {
		return 0, nil
	}

	held, err := heldNodeLeaseLocks(ctx, tx, candidates)
	if err != nil {
		return 0, err
	}
	ids := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if _, ok := held[candidate.nodeID]; ok {
			continue
		}
		ids = append(ids, candidate.nodeID)
	}
	if len(ids) == 0 {
		return 0, nil
	}
	res, err := tx.ExecContext(ctx, deleteExpiredNodesQuery, ids)
	if err != nil {
		return 0, fmt.Errorf("node registry: prune expired nodes: %w", err)
	}
	deleted, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("node registry: prune expired nodes: %w", err)
	}
	return deleted, nil
}

// heldNodeLeaseLocks returns the candidate node IDs whose exact per-node
// exclusive advisory key is currently held. The key alone is the ownership
// evidence: no PID and no nodes.data owner_pid is consulted.
func heldNodeLeaseLocks(
	ctx context.Context, tx *sql.Tx,
	candidates []expiredNodePruneCandidate,
) (map[string]struct{}, error) {
	ids := make([]string, 0, len(candidates))
	keys := make([]int64, 0, len(candidates))
	for _, candidate := range candidates {
		ids = append(ids, candidate.nodeID)
		keys = append(keys, candidate.lockKey)
	}
	rows, err := tx.QueryContext(ctx, heldNodeLeaseLocksQuery, ids, keys)
	if err != nil {
		return nil, fmt.Errorf("node registry: check node advisory locks: %w", err)
	}
	defer rows.Close()
	held := make(map[string]struct{}, len(ids))
	for rows.Next() {
		var nodeID string
		if err := rows.Scan(&nodeID); err != nil {
			return nil, fmt.Errorf("node registry: check node advisory locks: %w", err)
		}
		held[nodeID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("node registry: check node advisory locks: %w", err)
	}
	return held, nil
}

// validateServiceRegistryEndpoint rejects an invalid endpoint row before any
// registry write begins. It is the single admission check for the published
// batch: one invalid row rejects the whole batch, and valid rows are persisted
// verbatim rather than normalized here. The host and path rules are the service
// template's own rules from internal/app, so a stored route always means what the
// reconciler would accept.
func validateServiceRegistryEndpoint(endpoint ServiceRegistryEndpoint) error {
	if endpoint.App == "" {
		return fmt.Errorf("service registry: endpoint app is empty")
	}
	if endpoint.Service == "" {
		return fmt.Errorf("service registry: endpoint service is empty")
	}
	if endpoint.ContainerID == "" {
		return fmt.Errorf("service registry: endpoint container is empty")
	}
	identity := fmt.Sprintf("app=%q service=%q replica=%d container=%q",
		endpoint.App, endpoint.Service, endpoint.Replica, endpoint.ContainerID)
	if endpoint.Host == "" {
		return fmt.Errorf("service registry: endpoint %s: host is empty", identity)
	}
	if err := app.ValidateServiceHost(endpoint.Host); err != nil {
		return fmt.Errorf("service registry: endpoint %s: host: %w", identity, err)
	}
	if endpoint.Replica < 0 {
		return fmt.Errorf(
			"service registry: endpoint %s: replica must not be negative", identity)
	}
	if endpoint.Replica > math.MaxInt32 {
		return fmt.Errorf(
			"service registry: endpoint %s: replica %d exceeds the maximum supported value %d",
			identity, endpoint.Replica, math.MaxInt32)
	}
	if endpoint.HostPort < 1 || endpoint.HostPort > 65535 {
		return fmt.Errorf(
			"service registry: endpoint %s: host port %d is outside 1..65535",
			identity, endpoint.HostPort)
	}
	if endpoint.InternalPort < 1 || endpoint.InternalPort > 65535 {
		return fmt.Errorf(
			"service registry: endpoint %s: internal port %d is outside 1..65535",
			identity, endpoint.InternalPort)
	}
	if err := validateServiceRegistryHostIP(identity, endpoint.HostIP); err != nil {
		return err
	}
	if endpoint.Generation == "" {
		return fmt.Errorf(
			"service registry: endpoint %s: generation is empty", identity)
	}
	if err := app.ValidateServicePath(endpoint.Path); err != nil {
		return fmt.Errorf("service registry: endpoint %s: path: %w", identity, err)
	}
	return nil
}

// validateServiceRegistryHostIP enforces the address shape the Docker collector
// can actually produce. servicePublishedBinding rejects invalid, unspecified,
// and multicast bindings and returns an unmapped address; the service bind
// address parser additionally rejects mapped, zone-scoped, and non-unicast
// forms. A persisted registry row must fit that same contract rather than
// admitting an address the collector could never verify. This is admission
// only: equality with the configured SERVICE_BIND_ADDR remains the runtime
// collector's responsibility.
func validateServiceRegistryHostIP(identity, hostIP string) error {
	addr, err := netip.ParseAddr(hostIP)
	if err != nil {
		return fmt.Errorf(
			"service registry: endpoint %s: host IP %q is not a valid IPv4 or IPv6 address",
			identity, hostIP)
	}
	if addr.Is4In6() {
		return fmt.Errorf(
			"service registry: endpoint %s: host IP %q is IPv4-mapped IPv6; use the IPv4 form",
			identity, hostIP)
	}
	if addr.Zone() != "" {
		return fmt.Errorf(
			"service registry: endpoint %s: host IP %q is zone-scoped",
			identity, hostIP)
	}
	if addr.IsUnspecified() {
		return fmt.Errorf(
			"service registry: endpoint %s: host IP %q is unspecified",
			identity, hostIP)
	}
	if addr.IsMulticast() {
		return fmt.Errorf(
			"service registry: endpoint %s: host IP %q is multicast",
			identity, hostIP)
	}
	if !addr.IsGlobalUnicast() && !addr.IsLoopback() && !addr.IsLinkLocalUnicast() {
		return fmt.Errorf(
			"service registry: endpoint %s: host IP %q is not a usable unicast host address",
			identity, hostIP)
	}
	return nil
}

// PublishServiceRegistryEndpoints atomically publishes the EXACT eligible
// endpoint set for this node using the global node lease. It NEVER renews that
// lease: the standalone node heartbeat is the sole lease-renewal owner. It
// requires the current node row/incarnation/lock fence AND an unexpired
// database-time lease, then upserts changed rows, removes rows no longer in the
// set, and renews each endpoint's OWN expiry. An unchanged publish rewrites no
// endpoint row (except a bounded expiry renewal), so a periodic sync does not
// amplify writes.
func (st *State) PublishServiceRegistryEndpoints(
	ctx context.Context,
	lease NodeLease,
	endpoints []ServiceRegistryEndpoint,
) error {
	fence, err := st.checkNodeLease(lease)
	if err != nil {
		return err
	}
	// Validate every RAW input first. Deduplication below keeps the LAST row for
	// a repeated identity; validating only the deduplicated set would let an
	// invalid earlier row disappear behind a later valid one. One invalid input
	// always rejects the whole batch before any encoding or transaction begins.
	for _, endpoint := range endpoints {
		if err := validateServiceRegistryEndpoint(endpoint); err != nil {
			return err
		}
	}
	// A batch is a SET keyed by the relational identity. Deduplicate here so a
	// caller that repeats a row produces the idempotent result instead of
	// PostgreSQL's "ON CONFLICT DO UPDATE cannot affect row a second time".
	endpoints = uniqueServiceRegistryEndpoints(endpoints)
	appNames := make([]string, len(endpoints))
	serviceNames := make([]string, len(endpoints))
	replicas := make([]int32, len(endpoints))
	hosts := make([]string, len(endpoints))
	paths := make([]string, len(endpoints))
	containerIDs := make([]string, len(endpoints))
	payloads := make([]string, len(endpoints))
	for i, endpoint := range endpoints {
		payload, err := encodeServiceRegistryEndpointPayload(endpoint)
		if err != nil {
			return err
		}
		appNames[i] = endpoint.App
		serviceNames[i] = endpoint.Service
		replicas[i] = int32(endpoint.Replica)
		hosts[i] = endpoint.Host
		paths[i] = endpoint.Path
		containerIDs[i] = endpoint.ContainerID
		payloads[i] = payload
	}

	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("service registry: begin publish: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Lock the active lease row and verify the fence in the same statement. The
	// row lock serializes a concurrent successor claim behind this transaction,
	// and FOR UPDATE re-reads the newest row, so a just-superseded incarnation is
	// rejected rather than publishing against a stale snapshot. Expiry is
	// checked SEPARATELY after the lock is held, using a fresh statement and
	// clock_timestamp(): a predicate evaluated in the locking statement can be
	// re-evaluated by READ COMMITTED's EvalPlanQual against a concurrently
	// updated row, but only a post-lock statement necessarily measures real
	// database time after any conflict wait. lease_expires_at must still be in
	// the future by that clock: publication consumes the heartbeat-owned lease
	// and never extends it, so a lapsed lease stops the endpoint snapshot from
	// outliving node liveness.
	var lockedIncarnation int64
	err = tx.QueryRowContext(ctx, lockActiveNodeLeaseQuery,
		fence.nodeID, fence.instance, fence.incarnation,
		fence.ownerPID, fence.lockKey,
	).Scan(&lockedIncarnation)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf(
			"%w: publish node %q incarnation %d",
			ErrNodeLeaseFence, fence.nodeID, fence.incarnation)
	}
	if err != nil {
		return fmt.Errorf("service registry: verify publish fence: %w", err)
	}
	var active bool
	err = tx.QueryRowContext(ctx, recheckActiveNodeLeaseQuery,
		fence.nodeID, fence.instance, fence.incarnation,
	).Scan(&active)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !active) {
		return fmt.Errorf(
			"%w: publish node %q incarnation %d lease expired",
			ErrNodeLeaseFence, fence.nodeID, fence.incarnation)
	}
	if err != nil {
		return fmt.Errorf("service registry: verify publish lease: %w", err)
	}
	if len(endpoints) > 0 {
		if _, err := tx.ExecContext(ctx, upsertServiceRegistryEndpointsQuery,
			fence.nodeID, fence.incarnation, lease.ttl.Seconds(),
			appNames, serviceNames, replicas, hosts, paths,
			containerIDs, payloads,
		); err != nil {
			return fmt.Errorf("service registry: upsert endpoints: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, deleteServiceRegistryEndpointsQuery,
		fence.nodeID, fence.incarnation,
		appNames, serviceNames, replicas, containerIDs,
	); err != nil {
		return fmt.Errorf("service registry: withdraw stale endpoints: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("service registry: commit publish: %w", err)
	}
	return nil
}

// ReadEligibleServiceRegistryEndpoints returns every endpoint whose node lease
// AND endpoint expiry are still in the future according to the PostgreSQL clock.
// Results are deterministic (host, path, node, app, service, replica, container)
// so callers can compare snapshots without sorting. It never mutates rows and is
// safe on an ownerless CLI/admin handle.
func (st *State) ReadEligibleServiceRegistryEndpoints(
	ctx context.Context,
	filter ServiceRegistryFilter,
) ([]ServiceRegistryEndpoint, error) {
	rows, err := st.db.QueryContext(ctx, readServiceRegistryEndpointsQuery,
		filter.NodeID, filter.App, filter.Service, filter.Host, filter.Path)
	if err != nil {
		return nil, fmt.Errorf("service registry: read endpoints: %w", err)
	}
	defer rows.Close()
	var endpoints []ServiceRegistryEndpoint
	for rows.Next() {
		var (
			endpoint    ServiceRegistryEndpoint
			replica     int32
			nodeData    string
			payloadData string
		)
		if err := rows.Scan(
			&endpoint.NodeID, &nodeData,
			&endpoint.App, &endpoint.Service, &replica,
			&endpoint.Host, &endpoint.Path, &endpoint.ContainerID, &payloadData,
		); err != nil {
			return nil, fmt.Errorf("service registry: scan endpoint: %w", err)
		}
		nodeMetadata, err := decodeNodeRegistryData(nodeData)
		if err != nil {
			return nil, err
		}
		payload, err := decodeServiceRegistryEndpointPayload(payloadData)
		if err != nil {
			return nil, err
		}
		endpoint.AdvertiseAddr = nodeMetadata.AdvertiseAddr
		endpoint.Replica = int(replica)
		endpoint.HostIP = payload.HostIP
		endpoint.HostPort = payload.HostPort
		endpoint.InternalPort = payload.InternalPort
		endpoint.Generation = payload.Generation
		endpoints = append(endpoints, endpoint)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("service registry: read endpoints: %w", err)
	}
	return endpoints, nil
}

// checkNodeLease validates that lease was issued by THIS handle's node/instance
// and still carries the fence fields the SQL statements require. It is a cheap
// local check that turns a programming error (a lease from another handle) into
// a fence rejection instead of a write against the wrong row.
func (st *State) checkNodeLease(lease NodeLease) (nodeLeaseFence, error) {
	if st == nil || st.nodeID == "" || st.instance == "" {
		return nodeLeaseFence{}, fmt.Errorf(
			"node registry: operation requires a node-owned state handle")
	}
	fence := lease.fence
	if fence.nodeID != st.nodeID || fence.instance != st.instance ||
		fence.incarnation <= 0 || fence.incarnation != lease.Incarnation ||
		fence.ownerPID <= 0 || fence.ownerPID != st.ownerPID ||
		fence.lockKey != nodeLockKey(st.nodeID) {
		return nodeLeaseFence{}, fmt.Errorf(
			"%w: lease does not belong to this state handle", ErrNodeLeaseFence)
	}
	return fence, nil
}

// claimNodeLeaseQuery claims the node's lease and allocates a fresh incarnation,
// but only while the PINNED lock backend still holds the node's session advisory
// lock. The advisory key is reconstructed from pg_locks' classid/objid exactly
// like VerifyOwnerLock; nothing is re-acquired.
//
// Timestamps use clock_timestamp(), not now(): now() is the transaction start
// time, and this INSERT may wait on a conflicting row. On conflict, the DO
// UPDATE branch evaluates its SET expressions after that wait. The separate
// refresh statement below then re-stamps the claimed row after the row lock is
// held, covering the insert path of a concurrent delete as well.
const claimNodeLeaseQuery = `
	INSERT INTO nodes
		(node_id, instance, incarnation, heartbeat_at, lease_expires_at, data)
	SELECT $1, $2, nextval('nodes_incarnation_seq'),
	       clock_timestamp(),
	       clock_timestamp() + make_interval(secs => $5::double precision), $6::jsonb
	 WHERE EXISTS (
		SELECT 1 FROM pg_locks
		 WHERE locktype = 'advisory'
		   AND pid = $3
		   AND objsubid = 1
		   AND granted
		   AND (classid::bigint << 32) | objid::bigint = $4
	 )
	ON CONFLICT (node_id) DO UPDATE SET
		instance = excluded.instance,
		incarnation = excluded.incarnation,
		heartbeat_at = clock_timestamp(),
		lease_expires_at =
			clock_timestamp() + make_interval(secs => $5::double precision),
		data = excluded.data
	RETURNING incarnation`

// refreshClaimedNodeLeaseQuery re-stamps the claimed row after the INSERT above
// has acquired its row lock. The INSERT itself may have waited on a conflicting
// row, and its INSERT path can carry timestamps computed before that wait; this
// follow-up statement measures the lease from the real DB instant and keeps the
// same owner/incarnation/lock fence.
const refreshClaimedNodeLeaseQuery = `
	UPDATE nodes
	   SET heartbeat_at = clock_timestamp(),
	       lease_expires_at =
			clock_timestamp() + make_interval(secs => $4::double precision)
	 WHERE node_id = $1
	   AND instance = $2
	   AND incarnation = $3
	   AND EXISTS (
		SELECT 1 FROM pg_locks
		 WHERE locktype = 'advisory'
		   AND pid = $5
		   AND objsubid = 1
		   AND granted
		   AND (classid::bigint << 32) | objid::bigint = $6
	 )
	RETURNING lease_expires_at`

const deleteSupersededNodeEndpointsQuery = `
	DELETE FROM service_registry_endpoints
	 WHERE node_id = $1 AND incarnation <> $2`

const heartbeatNodeLeaseQuery = `
	UPDATE nodes
	   SET heartbeat_at = clock_timestamp(),
	       lease_expires_at =
			clock_timestamp() + make_interval(secs => $4::double precision)
	 WHERE node_id = $1
	   AND instance = $2
	   AND incarnation = $3
	   AND EXISTS (
		SELECT 1 FROM pg_locks
		 WHERE locktype = 'advisory'
		   AND pid = $5
		   AND objsubid = 1
		   AND granted
		   AND (classid::bigint << 32) | objid::bigint = $6
	 )
	RETURNING lease_expires_at`

// lockNodeLeaseQuery verifies the complete fence and takes a row lock that
// serializes a concurrent successor claim behind the caller's transaction.
// FOR UPDATE re-reads the newest committed row, so a stale snapshot cannot
// authorize a superseded write. It deliberately does NOT require an unexpired
// lease: release/withdrawal must still succeed after a lease lapse (ownership of
// the advisory lock, never expiry, is the authority).
const lockNodeLeaseQuery = `
	SELECT incarnation
	  FROM nodes
	 WHERE node_id = $1
	   AND instance = $2
	   AND incarnation = $3
	   AND EXISTS (
		SELECT 1 FROM pg_locks
		 WHERE locktype = 'advisory'
		   AND pid = $4
		   AND objsubid = 1
		   AND granted
		   AND (classid::bigint << 32) | objid::bigint = $5
	 )
	 FOR UPDATE`

// lockActiveNodeLeaseQuery is the PUBLICATION fence's LOCK step. It requires the
// complete owner/incarnation/advisory-lock fence but deliberately does not test
// expiry: a row predicate evaluated by this statement can be re-checked by READ
// COMMITTED's EvalPlanQual against a concurrently updated version, and now()
// would carry the transaction timestamp across a conflict wait. Taking the row
// lock first and checking lease_expires_at with clock_timestamp() in a follow-up
// statement (recheckActiveNodeLeaseQuery) measures real DB time after the lock.
const lockActiveNodeLeaseQuery = `
	SELECT incarnation
	  FROM nodes
	 WHERE node_id = $1
	   AND instance = $2
	   AND incarnation = $3
	   AND EXISTS (
		SELECT 1 FROM pg_locks
		 WHERE locktype = 'advisory'
		   AND pid = $4
		   AND objsubid = 1
		   AND granted
		   AND (classid::bigint << 32) | objid::bigint = $5
	 )
	 FOR UPDATE`

const recheckActiveNodeLeaseQuery = `
	SELECT lease_expires_at > clock_timestamp()
	  FROM nodes
	 WHERE node_id = $1
	   AND instance = $2
	   AND incarnation = $3`

const deleteIncarnationEndpointsQuery = `
	DELETE FROM service_registry_endpoints
	 WHERE node_id = $1 AND incarnation = $2`

// pruneExpiredServiceRegistryEndpointsQuery deletes one bounded batch of
// endpoints that are expired, or whose node/incarnation is no longer valid or
// whose node lease has lapsed. An endpoint is KEPT only while its node row has
// the same incarnation and an unexpired lease AND its own endpoint lease is
// unexpired. Expired endpoints under a currently active node are included, so a
// crashed publisher's snapshot cannot linger. FOR UPDATE SKIP LOCKED avoids
// blocking a concurrent publication, and the outer predicate re-checks the same
// conditions against a concurrently renewed row.
const pruneExpiredServiceRegistryEndpointsQuery = `
	DELETE FROM service_registry_endpoints e
	 WHERE e.ctid IN (
		SELECT c.ctid
		  FROM service_registry_endpoints c
		 WHERE c.expires_at <= clock_timestamp()
		    OR NOT EXISTS (
			SELECT 1 FROM nodes n
			 WHERE n.node_id = c.node_id
			   AND n.incarnation = c.incarnation
			   AND n.lease_expires_at > clock_timestamp()
		    )
		 ORDER BY c.expires_at
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED
	 )
	 AND (
		e.expires_at <= clock_timestamp()
		OR NOT EXISTS (
			SELECT 1 FROM nodes n
			 WHERE n.node_id = e.node_id
			   AND n.incarnation = e.incarnation
			   AND n.lease_expires_at > clock_timestamp()
		)
	 )`

// selectExpiredNodesForPruneQuery locks one bounded batch of expired node rows
// that have no remaining endpoint rows. The endpoint batch above runs first, so
// this deliberately avoids relying on a cascade to remove unpruned endpoints: a
// node with remaining endpoints is deferred to a later invocation, keeping both
// batches bounded. FOR UPDATE SKIP LOCKED serializes against a concurrent
// heartbeat/claim, and READ COMMITTED re-checks the predicate against a
// concurrently renewed row before locking it. Only the node ID is read; the
// lock key is derived in Go, so malformed nodes.data cannot fail cleanup.
const selectExpiredNodesForPruneQuery = `
	SELECT n.node_id
	  FROM nodes n
	 WHERE n.lease_expires_at <= clock_timestamp()
	   AND NOT EXISTS (
		SELECT 1 FROM service_registry_endpoints e
		 WHERE e.node_id = n.node_id
	   )
	 ORDER BY n.lease_expires_at
	 LIMIT $1
	 FOR UPDATE SKIP LOCKED`

// heldNodeLeaseLocksQuery reports which candidate node IDs currently have their
// exact exclusive session advisory key held. The key is reconstructed in Go from
// the node identity; no PID or nodes.data value participates.
const heldNodeLeaseLocksQuery = `
	SELECT cand.node_id
	  FROM unnest($1::text[], $2::bigint[])
	       AS cand(node_id, lock_key)
	  JOIN pg_locks l
	    ON l.locktype = 'advisory'
	   AND l.objsubid = 1
	   AND l.granted
	   AND l.mode = 'ExclusiveLock'
	   AND (l.classid::bigint << 32) | l.objid::bigint = cand.lock_key`

// deleteExpiredNodesQuery removes the selected rows that were not excluded for a
// still-held owner lock. The rows are already row-locked by the selection, and
// expiry/no-endpoints are re-checked with fresh database time.
const deleteExpiredNodesQuery = `
	DELETE FROM nodes n
	 WHERE n.node_id = ANY($1::text[])
	   AND n.lease_expires_at <= clock_timestamp()
	   AND NOT EXISTS (
		SELECT 1 FROM service_registry_endpoints e
		 WHERE e.node_id = n.node_id
	   )`

const releaseNodeLeaseQuery = `
	DELETE FROM nodes
	 WHERE node_id = $1 AND instance = $2 AND incarnation = $3`

// upsertServiceRegistryEndpointsQuery inserts the published set and updates only
// rows whose relational routing/identity fields or cohesive JSONB attributes
// changed, or whose expiry is within the renewal threshold, so a steady-state
// sync does not rewrite every row on every pass. clock_timestamp() keeps an
// insert/update that waited on a conflicting row from carrying the transaction's
// older start time into a shorter-than-intended renewal window.
const upsertServiceRegistryEndpointsQuery = `
	INSERT INTO service_registry_endpoints
		(node_id, incarnation, app_name, service_name, replica, host, path,
		 container_id, expires_at, updated_at, data)
	SELECT $1, $2, u.app_name, u.service_name, u.replica, u.host, u.path,
	       u.container_id,
	       clock_timestamp() + make_interval(secs => $3::double precision),
	       clock_timestamp(),
	       u.data::jsonb
	  FROM unnest(
	       $4::text[], $5::text[], $6::int[], $7::text[], $8::text[],
	       $9::text[], $10::text[]
	  ) AS u(app_name, service_name, replica, host, path, container_id, data)
	ON CONFLICT (node_id, app_name, service_name, replica, container_id)
	DO UPDATE SET
		incarnation = excluded.incarnation,
		host = excluded.host,
		path = excluded.path,
		data = excluded.data,
		expires_at =
			clock_timestamp() + make_interval(secs => $3::double precision),
		updated_at = clock_timestamp()
	WHERE service_registry_endpoints.incarnation IS DISTINCT FROM excluded.incarnation
	   OR service_registry_endpoints.host IS DISTINCT FROM excluded.host
	   OR service_registry_endpoints.path IS DISTINCT FROM excluded.path
	   OR service_registry_endpoints.data IS DISTINCT FROM excluded.data
	   OR service_registry_endpoints.expires_at <
	      clock_timestamp() + make_interval(secs => $3::double precision / 2)`

const deleteServiceRegistryEndpointsQuery = `
	DELETE FROM service_registry_endpoints e
	 WHERE e.node_id = $1
	   AND (
		e.incarnation <> $2
		OR NOT EXISTS (
			SELECT 1
			  FROM unnest($3::text[], $4::text[], $5::int[], $6::text[])
			       AS x(app_name, service_name, replica, container_id)
			 WHERE x.app_name = e.app_name
			   AND x.service_name = e.service_name
			   AND x.replica = e.replica
			   AND x.container_id = e.container_id
		)
	   )`

// readServiceRegistryEndpointsQuery exposes only endpoints whose node
// incarnation matches and whose node lease and own expiry are still in the
// future at the instant of the read. Visibility is expiry-only: a held advisory
// lock never keeps an expired node or endpoint discoverable.
const readServiceRegistryEndpointsQuery = `
	SELECT e.node_id, n.data::text,
	       e.app_name, e.service_name, e.replica,
	       e.host, e.path, e.container_id, e.data::text
	  FROM service_registry_endpoints e
	  JOIN nodes n ON n.node_id = e.node_id
	 WHERE n.incarnation = e.incarnation
	   AND n.lease_expires_at > clock_timestamp()
	   AND e.expires_at > clock_timestamp()
	   AND ($1 = '' OR e.node_id = $1)
	   AND ($2 = '' OR e.app_name = $2)
	   AND ($3 = '' OR e.service_name = $3)
	   AND ($4 = '' OR e.host = $4)
	   AND ($5 = '' OR e.path = $5)
	 ORDER BY e.host, e.path, e.node_id, e.app_name, e.service_name,
	          e.replica, e.container_id`

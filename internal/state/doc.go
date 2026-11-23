// Package state persists a read-mostly operator snapshot of Relay's app
// state in a PostgreSQL database.
//
// The state database is a state VIEW, not the source of truth: /apps (the
// filesystem root the loader reads) is authoritative. It is not immutable or
// read-only — the worker writes it (the startup/discovery state phase, the
// reconcile outcomes, the periodic stats flush) and `relay stats reset` writes
// it — but it is read-mostly in that no read path mutates it and it never
// drives matching, image building, or reconciliation decisions. The database is
// initialized idempotently on open. It exists so operators can introspect what
// Relay has loaded and how the last reconcile of each app went, independent
// of Redis or Docker.
//
// State Model:
//   - status: the public lifecycle of the current generation. It is linear —
//     "preparing" (a discovered or newly desired generation is being prepared) ->
//     "building" (an actual image build is running) -> "reconciling" (persistent
//     services are being converged) -> "ready" (the full current generation is
//     converged and serving). "degraded" and "unavailable" are terminal failure
//     outcomes: degraded retains a usable previous generation's image;
//     unavailable has none. Startup discovery and live generation changes both
//     enter "preparing", replacing any stale transient lifecycle state left by a
//     process that died mid-work.
//   - last_reconcile_status: "success" | "failed"
//     (skipped periodic checks are not recorded; the snapshot reflects the last
//     MEANINGFUL reconcile — a success or a failure — so an unchanged-app
//     periodic pass never overwrites them)
//
// Desired vs active: the LAST USABLE generation (Image/Fingerprint/PreparedAt)
// is only ever replaced by a successful reconcile, so it always describes what
// the previous process was actually serving. The LATEST DESIRED content digest
// is kept separately (DesiredFingerprint vs Fingerprint), so a prepare may target
// a new digest while the old generation keeps serving.
//
// Two failure kinds are distinguished:
//   - A VALID desired generation that fails to prepare (a build error) records a
//     failure while PRESERVING both the active generation and the desired
//     fingerprint (the failed generation is still the desired one, so a later
//     pass retries it).
//   - An INVALID desired definition (RecordInvalidDesired: an invalid path, an
//     invalid/unreadable template) records a failure while preserving only the
//     active generation: the definition cannot be parsed, so there is no
//     trustworthy desired fingerprint or template-derived configuration, and
//     those fields are cleared. This applies equally to startup discovery (the
//     loader reports present-but-invalid entries) and the live reconciler. A
//     directory without template.yaml is not an app candidate at all: it is
//     never recorded as invalid, and a known app that loses its template is
//     removed (startup pruning and live removal share that rule).
//
// A DETERMINISTIC parse/validation failure of successfully read template bytes
// additionally persists the content identity (SHA-256 of those exact bytes) as
// invalid_source. While that same identity is persisted, a scan/reconcile skips
// the repeat parse, warning, state write, and fencing-token allocation: the
// persisted row is the authority, never a process-local negative cache. Changed
// bytes revalidate and record; a valid correction clears the identity through
// the normal desired/success snapshot. Operational failures (invalid path,
// unreadable file, fingerprint/source traversal, dependency errors) carry NO
// identity and stay retryable. An invalid observation snapshots the row's
// fencing token BEFORE reading the template and is applied only while that token
// is still current, so a stale observation cannot overwrite another worker's
// newer valid correction. The check runs inside the per-app write transaction,
// and a stale observation is reported as a distinct recoverable result rather
// than a PostgreSQL error. Missing or unreadable observations are refused, never
// treated as an absent row, so they cannot bypass concurrency. The identity is
// never a fencing token.
//
// In both cases a failure never reports "ready": degraded when a usable active
// generation exists, unavailable otherwise. Relational updated_at and
// selected_at timestamps use PostgreSQL TIMESTAMPTZ; JSON snapshot and public
// presentation timestamps retain their RFC3339 string form.
//
// View boundary: this database is a VIEW, never the source of truth. It
// records what was loaded/prepared/served for operators, but it never drives
// matching, image building, scheduling, or execution decisions — those are
// rebuilt from /apps. An invalid app therefore stays out of the
// runtime registry entirely; only its state row reflects the invalid desired
// state, and only the persisted active image is consulted (conservatively) by
// the startup image keep-set.
//
// App storage model:
//
//   - apps(name TEXT PRIMARY KEY, data JSONB NOT NULL, updated_at TIMESTAMPTZ NOT
//     NULL, fencing_token BIGINT NOT NULL DEFAULT 0) stores ONE whole per-app
//     snapshot as a native JSONB object in data. Only the stable name key, the
//     write timestamp, and the internal fencing token stay as columns;
//     fencing_token is allocated from apps_fencing_token_seq while the per-app
//     advisory lock is held and is never part of the JSONB payload. It fences
//     desired-state transitions across workers: a callback carrying a stale
//     token updates no row. The nested configuration — the env names (each value
//     is the fixed RedactedEnvValue marker, never the literal template value) and
//     the secret REFERENCE names (never a secret value), the event handlers (name
//   - timeout), the schedules (handler/cron/timezone/timeout/retries), and the
//     services (entrypoint/image/host/path/port/replicas) — is all part of
//     that one payload, so a template change replaces the snapshot atomically
//     and there are no per-handler/schedule/service child tables.
//   - The handler/event and schedule entries deliberately omit the template
//     parser's opaque matcher patterns: matching is rebuilt from template.yaml,
//     never from this state view, and those matcher interfaces cannot be JSON
//     round-tripped.
//   - schedule_pending(id PRIMARY KEY, data JSONB NOT NULL, attempts,
//     next_attempt_ms, lease_until_ms, expires_at_ms, claim_token) is the durable
//     schedule-PUBLICATION retry outbox, not history and not an execution
//     source. The row key is the derived occurrence ID, and the COMPLETE
//     immutable occurrence intent (app/schedule/handler/scheduled_at) is the
//     native JSONB object in data — the same JSONB-payload convention as
//     apps.data, so the schema stays stable while the intent grows. It is
//     written once (INSERT ... ON CONFLICT DO NOTHING, so it is unique and
//     idempotent), leased per-row (a claim selects with FOR UPDATE SKIP LOCKED so
//     concurrent retriers across processes cannot both claim it) and fenced by a
//     monotonic claim_token so a superseded claimant's reschedule/delete can
//     never clobber a newer claim; it is deleted only after a publication call
//     resolves with a nil error (published or a clean duplicate) OR after its
//     retention lapses. The intent (id/data) is never updated after insert; only
//     attempts/next_attempt_ms advance. The scheduling columns are integer Unix
//     milliseconds. expires_at_ms is the durable retention deadline, stamped at
//     first insert as the injected now + 7 days and never modified by a
//     reschedule or a re-save, so the retry window is bounded and stable across
//     restarts. A record is EXPIRED once now >= expires_at_ms: it is excluded
//     from claims (the retrier re-checks the deadline immediately before
//     publishing), never retried, and removed by a bounded cleanup as an
//     observable expiration. The deadline is deliberately shorter than the Redis
//     occurrence dedup key TTL (14 days), so the original key still protects
//     every retry: an ambiguous publish is resolved as a clean duplicate, and
//     after expiry no publish is attempted at all. A stored payload that
//     cannot be decoded, or whose decoded intent does not reconstruct the row's
//     occurrence ID, is surfaced to the caller and RETAINED (logged and
//     rescheduled, never published, never deleted) so a repair can still recover
//     it. This table never drives matching, building, scheduling, or execution:
//     /apps is authoritative, and the occurrence identity it stores is derived
//     exactly as on the wire.
//
// The dlq table is Relay's SOLE dead-letter persistence, admin read model, and
// retention store; Redis remains the broker and the invocation-state authority.
// Unlike the app-state view, DLQ rows are durable work records: the stream layer
// commits them before acknowledging a source delivery and never rebuilds them
// from /apps.
// A row records ONE terminal source delivery's exhaustion of ONE invocation (or
// one placeholder row for a delivery that never reached a handler). Its primary
// key is a stable ID derived from the logical source delivery identity (source,
// source group, source message ID) plus the invocation identity, never from a
// broker-assigned row ID, so re-persisting the same exhaustion after a crash, an
// ambiguous transaction, or a failed source ACK is idempotent
// (INSERT ... ON CONFLICT DO NOTHING). The row payload is a private JSONB object
// (data): event and reason are byte slices so encoding/json base64-encodes them,
// which preserves arbitrary malformed bytes (including NUL) that JSONB itself
// would reject. Only the stable ID, the source delivery identity hash
// (delivery_id), the nullable settlement marker (settled_at_ms), and
// created_at_ms are relational columns. Every row from one delivery shares
// delivery_id; the stream marks them settled only AFTER a confirmed source ACK,
// and a bounded worker reconciliation settles rows whose source delivery is no
// longer pending after a crash. Admin removal and retention delete only settled
// rows, so a delivery that may still be recoverable can never lose its dead
// letter. created_at_ms and the single (created_at_ms, id) index serve operator
// listing order and bounded settled retention cleanup; a partial
// (delivery_id, created_at_ms) index serves unsettled reconciliation. The
// consumer name and delivery count are diagnostic only. An oversized event is
// stored only as the bounded diagnostic summary produced by the stream layer,
// never as its raw payload.
//
// Runtime image selection state:
//
// runtime_image_selections(runtime_id TEXT PRIMARY KEY, source_image TEXT NOT
// NULL, digest TEXT NOT NULL, generation BIGINT NOT NULL,
// selected_at TIMESTAMPTZ NOT NULL) is a small table of runtime base-image pins,
// keyed by the template
// runtime identifier. source_image stores the EXACT configured base reference
// (e.g. "python:3.14-slim"), and digest stores a Docker multi-platform index
// digest in the form "sha256:<64 lowercase hex>" — never a child manifest
// digest. generation is monotonically increasing and selected_at is a native
// TIMESTAMPTZ. There is no history table: a pin is replaced in place. A
// MISSING ROW MEANS UNPINNED, which preserves the exact historical base-image tag
// behavior for every existing deployment.
//
// ReadRuntimeImageSelections is the complete snapshot read used both at worker
// startup and by each periodic reconciliation pass, which refreshes the runtime
// Manager's validated view. A read or validation failure leaves the worker's
// last-known-good snapshot in place. CompareAndSwapRuntimeImageSelection is the
// narrow write API: it inserts at expected generation 0 (absent/unpinned) or
// updates only when the stored generation still matches the expected positive
// generation, advancing it by one. A lost race is reported as
// ErrRuntimeImageSelectionConflict, distinct from a PostgreSQL error.
//
// Selection writes and runtime publication are fenced by one transaction-scoped
// PostgreSQL advisory lock keyed by runtime ID: CAS writes take it, and
// WithRuntimeImageSelectionLock reads the authoritative row (including the
// absent/unpinned case) and runs the caller's publication inside the same lock.
// A preparation whose captured ref@digest no longer matches the row is therefore
// never published, while the active version keeps serving until a later pass
// converges. This package does not import the runtime package and does not
// validate runtime/source-image binding; the runtime layer does.
//
// The current CLI has no selection writer for runtime pins. Runtimes without a
// row remain unpinned; a stored change converges without a worker restart.
//
// Coordinated deployment state (source-driven participation):
//
// deployment_desired(id = 1, generation, updated_at, data JSONB) is the shared
// desired revision on a coordinator-active worker. generation is a
// monotonic compare-and-swap key and data holds the immutable 40-character
// commit SHA plus the non-secret repository/ref/path, publisher, optional source
// timestamp, pin policy, and note. It is deliberately separate from apps: the
// global apps row is never repurposed as a shared desired state. Publishing is
// serialized by a dedicated transaction-scoped advisory lock and performs only
// PostgreSQL work, so no network, checkout, materialization, or build runs
// inside the transaction. Repeated identical revisions are idempotent no-ops;
// concurrent publishers serialize and exactly one advances a generation; a
// pinned revision ignores automatic webhook publications. Automatic ordering is
// anchored to a persisted branch tip: a push replaces the current revision only
// when its provider before SHA equals BranchTip (the after SHA of the last
// accepted automatic push). The first-ever automatic publication establishes the
// anchor; commit timestamps never accept a mismatched before SHA. Missing or
// mismatched chains are ignored (ambiguous/stale), and an automatic publication
// may never change an existing shared repository/ref/path identity. Manual
// publication bypasses automatic ordering, preserves the same-source metadata
// and branch-tip anchor for pin/rollback, may deliberately roll back to an older
// SHA as a NEW generation, and is the explicit recovery path (re-anchoring) when
// a chain was missed. Manual publication is also the only way to change the
// shared source identity.
//
// deployment_nodes(node_id PRIMARY KEY, generation, applied_generation,
// instance, incarnation, applied_incarnation, updated_at, data JSONB) records
// each node's INDEPENDENT progress. generation is the latest desired generation
// observed; applied_generation is the generation materialized and authorized in
// /apps; incarnation is allocated from deployment_incarnation_seq and fences a
// stale process incarnation out of every progress write. The phase/commit/error
// payload is JSONB. deployment_apps(node_id, app_name, generation, instance,
// updated_at, data JSONB) records per-app activation progress DISTINCT from Git
// materialization: pending/activating/active/degraded/failed/drifted/removed,
// plus the materialized content fingerprint and template identity. Rows for
// older generations survive until a successful applied advancement prunes them,
// so a failed newer materialization never destroys the previous generation's
// recovery authorization. App progress writes are accepted only while the
// node's observed/applied generation matches and the caller presents the current
// incarnation. No leader, executor, or global barrier participates: every node
// reads the same desired row and converges independently after the publisher's
// commit. A committed generation additionally emits a PostgreSQL NOTIFY hint on
// a dedicated session channel (see ListenDeploymentDesired); it only shortens
// convergence latency, listeners always re-read the authoritative row, and the
// coordinator's periodic read remains the safe fallback.
//
// The app publication fence is extended with Detail.SourceGeneration (the
// deployment generation whose materialized tree produced the snapshot). A
// success that would publish a NEW active fingerprint is refused once the shared
// desired generation differs from the app row's SourceGeneration, even if the
// content fingerprint is unchanged (a rollback to identical bytes). Re-stamping
// the already-active fingerprint (service convergence of the serving generation)
// remains allowed. This axis is independent of runtime_image_selections: Git
// generation ordering and runtime base-image pin CAS never substitute for one
// another.
//
// Global node registry and service endpoint publication:
//
// nodes(node_id PRIMARY KEY, instance, incarnation, heartbeat_at,
// lease_expires_at, data JSONB) is every live worker's lease-fenced
// registration, allocated from nodes_incarnation_seq. Identity, incarnation
// fencing, and lease timestamps are relational; data holds only the typed
// noncritical metadata (bind_addr, advertise_addr, owner_pid) and duplicates no
// column. A worker claims it only while its PINNED lock backend still holds the
// node's session advisory lock (proved from pg_locks at the operation boundary,
// not from a process-local check), so a successor cannot take over while the
// existing lock is held and a process that lost its lock cannot claim,
// heartbeat, publish, or release. Availability is the current incarnation plus
// an unexpired lease measured by the PostgreSQL clock; there is no status
// column.
//
// service_registry_endpoints(node_id, incarnation, app_name, service_name,
// replica, host, path, container_id, expires_at, updated_at, data JSONB) is the
// materialized set of externally routable service replicas each node verified
// on its own Docker daemon. Identity, the host/path query keys, incarnation
// fencing, and expiry are relational; host is the service's effective routing
// hostname (the declared host, or the exact ROUTING_HOST_OVERRIDE value when
// configured); data holds the typed endpoint details
// (host_ip, host_port, internal_port, generation) with no duplicate columns.
// node_id references nodes(node_id) ON DELETE CASCADE. It is a VIEW-like
// publication for discovery only: it never drives matching, builds,
// reconciliation, or execution, and it carries route/endpoint metadata (never
// environment or secret values). Rows are visible only while the node
// incarnation matches and both the node lease and endpoint expiry are in the
// future by the PostgreSQL clock. Endpoint publication consumes the node lease
// and never renews it: the standalone node heartbeat is the sole lease-renewal
// owner, and a fenced publish requires a currently valid database-time lease.
// Publishing the exact batch is atomic and idempotent; unchanged rows are not
// rewritten, removed routes are withdrawn, and each endpoint expiry is
// refreshed. A graceful shutdown releases this incarnation's endpoints and node
// row in one fenced transaction.
//
// Bounded registry cleanup is owned by the global node-registry lifecycle, not
// service publication or execution. One infrequent (minutes) invocation deletes
// at most NodeRegistryPruneEndpointBatch endpoints that are endpoint-expired or
// whose node/incarnation is absent/expired, then at most
// NodeRegistryPruneNodeBatch expired node rows that have NO remaining endpoints
// (so a cascade never outruns the endpoint batch). Selection orders by the
// indexed expires_at/lease_expires_at columns and uses FOR UPDATE SKIP LOCKED
// with an outer expiry re-check against fresh database time, so concurrent
// claim/heartbeat/publication transactions are never blocked or deleted while
// live. An expired row is retained while the node's exact exclusive advisory
// key is currently held, so a delayed heartbeat can renew its registration
// instead of losing it to cleanup; visibility still ends at expiry. Ownership
// evidence is the exact key alone, not a PID or nodes.data value, so malformed
// metadata cannot stall cleanup and the row is reclaimed after the lock is
// released. Crashed nodes therefore become invisible at lease expiry and their
// rows are eventually reclaimed without an external cleanup service.
//
// Development schema recreation (manual only; startup NEVER runs this, and it
// touches no unrelated table or data):
//
//	DROP TABLE IF EXISTS service_registry_endpoints;
//	DROP TABLE IF EXISTS service_registry_nodes;
//	DROP TABLE IF EXISTS nodes;
//	DROP SEQUENCE IF EXISTS service_registry_incarnation_seq;
//	DROP SEQUENCE IF EXISTS nodes_incarnation_seq;
//
// The FK-safe order drops the endpoint table before either node table; the
// intermediate service_registry_nodes name is included only for a developer
// database created by an earlier uncommitted revision.
//
// Secret values are never stored: only the reference names appear in the
// snapshot. Literal env values are never stored either: env entries keep the
// env-var name only, with a fixed redaction marker as the value.
//
// Design Constraint:
//   - State errors after a successful open are never fatal to the worker:
//     callers (reconciler) log them and continue; the state view degrades to "no
//     state available" on failure. Opening the database, however, IS fatal: the
//     state view, the stats snapshots, and the durable schedule outbox all live
//     in PostgreSQL, so the worker refuses to start without it (see Open).
//
// Stats persistence model:
//
// The stats and app_stats tables store the latest persisted ABSOLUTE
// snapshot of Relay's operational counters — idempotent, no deltas — PER NODE.
// Each row is keyed by the owning node's STABLE logical identity (config.NodeID,
// resolved from the required NODE_ID) so two nodes sharing one PostgreSQL
// database keep independent counter sets instead
// of last-writer-wins overwriting a single global row, and so same-host workers
// never collide (two processes must not share a NODE_ID).
// Only stable relational metadata is kept as columns: stats.node_id,
// stats.instance, stats.generation (the node-local reset epoch), and
// stats.updated_at (TIMESTAMPTZ), and app_stats.app_name, app_stats.node_id, and
// app_stats.updated_at (TIMESTAMPTZ). stats.instance is the owning process
// incarnation (config InstanceID, an always-fresh per-process token): every flush
// is conditioned on it, so a stale process for the same NodeID can never overwrite
// a newer incarnation's snapshot. The
// evolving counter/gauge/execution-history payload is a native JSONB object in the
// data column, marshalled and unmarshalled ONLY through stats_json.go; the worker,
// CLI, runtime, and metrics layers pass typed Stats/AppStats values and never
// touch the JSON. This keeps the schema stable as instrumentation grows: absent
// fields decode to zero. The typed structs are the source of truth. There is no
// migration or backward compatibility for payloads written by a different
// schema, and the internal state schema as a whole — including schedule_pending —
// carries no backward-compatibility guarantee across versions: it is
// coordination/operator state, not a durable public contract.
//
// stats_meta holds the single-row GLOBAL reset epoch (generation) and a native
// TIMESTAMPTZ updated_at. Every flush reads it FOR SHARE and `stats reset --all`
// bumps it with an exclusive UPDATE in
// the same transaction that zeroes the rows, so a delayed pre-reset snapshot from
// ANY worker is fenced out of the global reset — not only the socket-owning
// worker's. A node-scoped `stats reset` instead bumps stats.generation on that
// node's row, so it fences only that node's stale snapshots. The single lock
// ordering (stats_meta first, then stats/app_stats) makes a flush, either reset
// scope, and a retention prune deadlock-free.
//
// Reads: a worker restoring its own counters at startup reads ONLY its own rows
// (OwnerStats/OwnerAppStats), so a restart of the same identity continues its
// totals without double-counting a shared database. An operator read selects one
// node (ReadNodeStats) or AGGREGATES across every node (ReadStats/AppStats/
// AllAppStats): cumulative counters are summed, the execution-history timestamps
// take the maximum, and the shared-backlog gauges come from the complete pair
// with the newest successful observation instant. A legacy row without that
// metadata contributes no aggregate gauges (the aggregate stays zero when none
// has a valid observation), so a failed sample or a dead node's stale gauge can
// never mask the live value. A node-specific read still shows that node's own
// legacy gauges verbatim. Two workers
// deliberately configured with the SAME NODE_ID cannot coexist: Open takes an
// exclusive per-node session advisory lock held for the handle's lifetime, so
// the second concurrent worker fails to start with a clear error instead of
// silently fighting over one row (each concurrent worker must set a distinct
// NODE_ID).
//
// In addition to the event/handler counters, app_stats carries the
// CUMULATIVE warm-container pool counters (warm acquires, cold starts,
// discarded) so the standalone `relay app inspect` process can render the
// Runtime pool section without access to the worker's in-memory pool. The LIVE
// pool gauges (capacity, container counts by lease state) are deliberately NOT
// persisted: a persisted live gauge would go stale between flushes. The worker
// flushes the current in-memory metrics registry into them every 5 seconds
// (fixed, non-configurable) via RecordStatsSnapshotWithGenerations, which writes
// the worker's global row and every per-app row in one short transaction, fenced
// by the current global and node reset epochs and the worker's process
// incarnation. PostgreSQL is never
// on the event path: the runner and stream consumer update only the Prometheus
// registry, and the flush merely mirrors that store. A failed flush is retried
// next tick with the current values (absolute snapshots make this safe); a hard
// crash may lose up to ~5s of telemetry. Graceful shutdown performs a final
// bounded flush. At startup the worker restores its own persisted counters into
// the fresh registry from one atomic snapshot (restorePersistedStats, which also
// returns the reset epochs that snapshot was read under) so the first snapshot
// never resets them and a reset racing startup is still fenced. Redis
// event-processing correctness never depends on the stats tables.
//
// Stale-node compaction: a retired NODE_ID leaves its rows behind, so many
// short-lived identities can accumulate rows. After a
// successful flush the worker best-effort deletes stats/app_stats rows owned by
// OTHER nodes whose updated_at is older than STATS_RETENTION (default 7 days)
// via PruneStaleStats, throttled to one attempt per retentionTickInterval. A
// worker never prunes its OWN row, however stale it looks, and a zero/negative
// retention disables compaction. See docs/configuration.md.
//
// `relay stats reset` zeroes one node's cumulative fields IN PLACE and
// `stats reset --all` zeroes every node's: the target global rows and app_stats
// rows survive, decoded from their typed JSON payloads, while the live gauges,
// their successful-observation instant, and any known unrelated field are
// preserved. A node-scoped reset bumps only that row's generation; the global
// reset bumps the shared generation in the same transaction. Either way a
// delayed snapshot observes the advanced epoch and refuses to write it. A running
// worker performs the reset over its socket, under the same mutex as its flush,
// so its in-memory baseline is reset too (candidate captured before the state
// reset, installed only after it commits) and no captured pre-reset snapshot can
// be written afterwards; a stopped worker is
// reset directly through state.ResetNodeStats or state.ResetAllStats. Prometheus
// counters are never reset: the worker keeps a Relay-side reset baseline and
// subtracts it when snapshotting, so the persisted totals continue from zero
// while the counters stay monotonic. When a reset advances an epoch under a
// running worker that did not issue it, the worker rebases its baseline to the
// current registry values and retries once, so at most one flush interval (≤5s)
// of counters is dropped and none are resurrected.
//
// The driver is github.com/jackc/pgx/v5's database/sql adapter (pgx), a
// maintained pure-Go PostgreSQL driver (no CGO).
//
// Concurrency model:
//
//   - In-process access is a small bounded connection pool (a handful of
//     connections). Relay's state workload is short and low-frequency —
//     reconciler writes, a 5s stats flush, scheduler outbox writes, CLI reads —
//     so a small pool serves it without a write queue; PostgreSQL handles the
//     concurrency natively (MVCC, row locks).
//   - Per-app writes are serialized by a transaction-scoped advisory lock keyed
//     on the app NAME (lockAppTx/appWriteTx). The app snapshot is a single JSONB
//     payload that each write path reads, changes a subset of fields, and writes
//     whole, so MVCC alone does not protect it: two concurrent writers would
//     each read the same snapshot and the second would clobber the first's
//     fields. Keying on the name (not the row) also covers the app-row-ABSENT
//     case, where a row lock would lock nothing.
//   - Cross-process access is native: the database is shared, and the outbox's
//     FOR UPDATE SKIP LOCKED claims and claim_token fencing coordinate retriers
//     across processes without any process-global lock. Schema initialization is
//     serialized by a transaction-scoped advisory lock so concurrent openers
//     cannot race CREATE TABLE IF NOT EXISTS. A worker handle additionally holds
//     a per-node EXCLUSIVE session advisory lock on a dedicated pinned
//     connection for its whole lifetime: a second process presenting the same
//     NODE_ID fails Open with a clear error rather than silently sharing the
//     identity (see Open/Close); ownerless CLI/admin handles never take it. The
//     worker periodically re-verifies that pinned lock with VerifyOwnerLock
//     (recording the connection's backend PID at open): if the lock connection
//     is lost while the process keeps running (a terminated backend, a broken
//     link) or the lock is released, it fails closed with ErrOwnerLockLost so
//     the worker can cancel its lifecycle and skip identity-scoped cleanup
//     before a replacement owner is disturbed. Detection is interval-bounded,
//     not instantaneous (see OwnerLockCheckInterval). Every access to that one
//     pinned connection — each check and Close — is serialized by ownerMu,
//     because database/sql does not make a whole check atomic on one connection
//     and pgx fails the overlapping calls with "conn busy"; the monitor and the
//     startup/shutdown ownership proofs all share it. The lock is narrow
//     (ownerConn only), so unrelated state work remains fully concurrent.
//   - Dependency-image coordination rides the same pinned connection
//     (AcquireDependencyImageUse/TryAcquireDependencyImageGC, see
//     dep_image_lease.go): dependency layers live on a Docker daemon that
//     several Relay processes may share, so their shared-use/exclusive-GC
//     decision is made in the one database those processes share, using the
//     two-int advisory-lock namespace 2 with a stable FNV-1a hash of the full
//     layer reference. Local lease bookkeeping (and the local use-versus-GC
//     conflict check) is keyed by that advisory-lock identity rather than the
//     reference string, so a 32-bit hash collision conservatively serializes
//     colliding references on one handle — PostgreSQL grants a session a
//     conflicting lock mode on a key it already holds, so reference-keyed
//     bookkeeping would let one handle use one colliding layer while
//     garbage-collecting the other. Both forms live on a session (never a
//     transaction), because a Docker build outlives any transaction. Advisory
//     locks are scoped to one database: processes sharing a daemon must share
//     this database. Ownerless handles pin their own coordination connection for
//     the same reason. Every query on that connection (including lease Verify)
//     and its closure are serialized by depConnMu: database/sql does not make a
//     whole query-and-scan atomic, so overlapping pgx calls on one connection
//     fail with "conn busy".
//   - Transactions are short and hold no external I/O: fingerprints are computed
//     before the transaction opens (see RebuildFromFS/RecordDiscovered), so a
//     write transaction never blocks on the filesystem. Callers that already
//     hold the loaded set and a fingerprint (the worker's startup state phase)
//     use RebuildFromApps + RecordDiscoveredWithFingerprint so no state
//     write re-reads /apps.
package state

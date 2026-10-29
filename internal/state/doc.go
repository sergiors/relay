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
// generation exists, unavailable otherwise. All timestamps are RFC3339 strings.
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
//   - apps(name TEXT PRIMARY KEY, data JSONB NOT NULL, updated_at TEXT NOT
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
// snapshot of Relay's operational counters — idempotent, no deltas — PER WORKER.
// Each row is keyed by the owning worker's STABLE logical identity (config
// WorkerID, resolved from the required WORKER_ID) so two
// workers sharing one PostgreSQL database keep independent counter sets instead
// of last-writer-wins overwriting a single global row, and so same-host workers
// never collide (two processes must not share a WORKER_ID).
// Only stable relational metadata is kept as columns: stats.worker,
// stats.instance, and stats.updated_at, and app_stats.app_name,
// app_stats.worker, and app_stats.updated_at. stats.instance is the owning
// process incarnation (config InstanceID, an always-fresh per-process token):
// every flush is conditioned on it, so a stale process for the same WorkerID can
// never overwrite a newer incarnation's snapshot (see RecordStatsSnapshot). The
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
// stats_meta holds the single-row GLOBAL reset epoch (generation). Every flush
// reads it FOR SHARE and every `stats reset` bumps it with an exclusive UPDATE in
// the same transaction that zeroes the rows, so a delayed pre-reset snapshot from
// ANY worker is fenced out of the reset — not only the socket-owning worker's. The
// single lock ordering (stats_meta first, then stats/app_stats) makes a flush,
// a reset, and a retention prune deadlock-free.
//
// Reads: a worker restoring its own counters at startup reads ONLY its own rows
// (OwnerStats/OwnerAppStats), so a restart of the same identity continues its
// totals without double-counting a shared database. CLI and observability reads
// AGGREGATE across every owner (Stats/AppStats/AllAppStats): cumulative counters
// are summed, the execution-history timestamps take the maximum, and the
// shared-backlog gauges come from the FRESHEST owner snapshot (the newest
// updated_at), so a dead worker's stale gauge can never mask the live value.
// `relay stats reset` zeroes every owner's rows in place. Two workers
// deliberately configured with the SAME WORKER_ID cannot coexist: Open takes an
// exclusive per-owner session advisory lock held for the handle's lifetime, so
// the second concurrent worker fails to start with a clear error instead of
// silently fighting over one row (each concurrent worker must set a distinct
// WORKER_ID).
//
// In addition to the event/handler counters, app_stats carries the
// CUMULATIVE warm-container pool counters (warm acquires, cold starts,
// discarded) so the standalone `relay app inspect` process can render the
// Runtime pool section without access to the worker's in-memory pool. The LIVE
// pool gauges (capacity, container counts by lease state) are deliberately NOT
// persisted: a persisted live gauge would go stale between flushes. The worker
// flushes the current in-memory metrics registry into them every 5 seconds
// (fixed, non-configurable) via RecordStatsSnapshot, which writes the worker's
// global row and every per-app row in one short transaction, fenced by the
// current reset epoch and the worker's process incarnation. PostgreSQL is never
// on the event path: the runner and stream consumer update only the Prometheus
// registry, and the flush merely mirrors that store. A failed flush is retried
// next tick with the current values (absolute snapshots make this safe); a hard
// crash may lose up to ~5s of telemetry. Graceful shutdown performs a final
// bounded flush. At startup the worker restores its own persisted counters into
// the fresh registry (restorePersistedStats) so the first snapshot never resets
// them. Redis event-processing correctness never depends on the stats tables.
//
// Stale-owner compaction: a retired WORKER_ID leaves its rows behind, so many
// short-lived identities can accumulate rows. After a
// successful flush the worker best-effort deletes stats/app_stats rows owned by
// OTHER workers whose updated_at is older than STATS_RETENTION (default 7 days)
// via PruneStaleStats, throttled to one attempt per retentionTickInterval. A
// worker never prunes its OWN row, however stale it looks, and a zero/negative
// retention disables compaction. See docs/configuration.md.
//
// `relay stats reset` zeroes the cumulative fields IN PLACE: every worker's
// global row and every app_stats row survive, decoded from their typed JSON
// payloads, while the live gauges and any known unrelated field are preserved.
// The reset bumps the shared generation in the same transaction, so a delayed
// snapshot from ANY worker (not just the socket-owning one) observes the advanced
// epoch and refuses to write it. A running worker performs the reset over its
// socket, under the same mutex as its flush, so its in-memory baseline is reset
// too and no captured pre-reset snapshot can be written afterwards; a stopped
// worker is reset directly through state.ResetStats. Prometheus counters are
// never reset: the worker keeps a Relay-side reset baseline and subtracts it when
// snapshotting, so the persisted totals continue from zero while the counters stay
// monotonic. When a reset advances the epoch under a running worker that did not
// issue it, the worker rebases its baseline to the current registry values and
// retries once, so at most one flush interval (≤5s) of counters is dropped and
// none are resurrected.
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
//     a per-owner EXCLUSIVE session advisory lock on a dedicated pinned
//     connection for its whole lifetime: a second process presenting the same
//     WORKER_ID fails Open with a clear error rather than silently sharing the
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

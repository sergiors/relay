// Package state persists a read-mostly operator snapshot of Relay's app
// state in a local SQLite database.
//
// The state database is a state VIEW, not the source of truth: /apps (the
// filesystem root the loader reads) is authoritative. It is not immutable or
// read-only — the worker writes it (the startup/discovery state phase, the
// reconcile outcomes, the periodic stats flush) and `relay stats reset` writes
// it — but it is read-mostly in that no read path mutates it and it never
// drives matching, image building, or reconciliation decisions. The database is
// rebuilt automatically when empty. It exists so operators can introspect what
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
//   - An INVALID desired definition (RecordInvalidDesired: an invalid path, a
//     missing template.yaml, an invalid/unreadable template) records a failure
//     while preserving only the active generation: the definition cannot be
//     parsed, so there is no trustworthy desired fingerprint or template-derived
//     configuration, and those fields are cleared. This applies equally to
//     startup discovery (the loader reports present-but-invalid entries) and the
//     live reconciler.
//
// In both cases a failure never reports "ready": degraded when a usable active
// generation exists, unavailable otherwise. All timestamps are RFC3339 strings.
//
// SQLite-view boundary: this database is a VIEW, never the source of truth. It
// records what was loaded/prepared/served for operators, but it never drives
// matching, image building, scheduling, or execution decisions — those are
// rebuilt from /apps. An invalid app therefore stays out of the
// runtime registry entirely; only its state row reflects the invalid desired
// state, and only the persisted active image is consulted (conservatively) by
// the startup image keep-set.
//
// App storage model:
//
//   - apps(name TEXT PRIMARY KEY, data BLOB NOT NULL, updated_at TEXT NOT
//     NULL) stores ONE whole per-app snapshot as a JSON object in data,
//     written through SQLite's jsonb(?) (binary JSON / JSONB format) and read
//     back with json(data). Only the stable name key and the write timestamp
//     stay as columns. The nested configuration — the env names (each value is
//     the fixed RedactedEnvValue marker, never the literal template value) and
//     the secret REFERENCE names (never a secret value), the event handlers (name
//   - timeout), the schedules (handler/cron/timezone/timeout/retries), and the
//     services (entrypoint/image/host/path/port/replicas) — is all part of
//     that one payload, so a template change replaces the snapshot atomically
//     and there are no per-handler/schedule/service child tables.
//   - The handler/event and schedule entries deliberately omit the template
//     parser's opaque matcher patterns: matching is rebuilt from template.yaml,
//     never from this state view, and those matcher interfaces cannot be JSON
//     round-tripped.
//   - schedule_pending(id PRIMARY KEY, data BLOB NOT NULL, attempts,
//     next_attempt_ms, lease_until_ms, expires_at_ms) is the durable
//     schedule-PUBLICATION retry outbox, not history and not an execution
//     source. The row key is the derived occurrence ID, and the COMPLETE
//     immutable occurrence intent (app/schedule/handler/scheduled_at) is the
//     JSON object in data, written through jsonb(?) and read back with json(data)
//     — the same BLOB-payload convention as apps.data, so the schema stays stable
//     while the intent grows. It is written once (INSERT ... ON CONFLICT DO
//     NOTHING, so it is unique and idempotent), leased per-row so concurrent
//     retriers cannot both claim it, and deleted only after a publication call
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
// Secret values are never stored: only the reference names appear in the
// snapshot. Literal env values are never stored either: env entries keep the
// env-var name only, with a fixed redaction marker as the value.
//
// Design Constraint:
//   - State errors are never fatal. Callers (main, reconciler) log them and
//     continue; the state database degrades to "no state available" on failure.
//
// Stats persistence model:
//
// The stats and app_stats tables store the latest persisted ABSOLUTE
// snapshot of Relay's operational counters — idempotent, no deltas. Only stable
// relational metadata is kept as columns: stats.id and stats.updated_at, and
// app_stats.app_name and app_stats.updated_at. The evolving
// counter/gauge/execution-history payload is a JSON object — stored as binary
// JSON (JSONB) in the data BLOB column — marshalled and unmarshalled ONLY
// through stats_json.go; the worker, CLI, runtime, and metrics layers pass typed
// Stats/AppStats values and never touch the JSON. This keeps the schema
// stable as instrumentation grows: absent fields decode to zero. The typed
// structs are the source of truth. There is no migration or backward
// compatibility for payloads written by a different schema, and the internal
// state schema as a whole — including schedule_pending — carries no
// backward-compatibility guarantee across versions: it is local coordination
// state, not a durable public contract.
//
// In addition to the event/handler counters, app_stats carries the
// CUMULATIVE warm-container pool counters (warm acquires, cold starts,
// discarded) so the standalone `relay app inspect` process can render the
// Runtime pool section without access to the worker's in-memory pool. The LIVE
// pool gauges (capacity, container counts by lease state) are deliberately NOT
// persisted: a persisted live gauge would go stale between flushes. The worker
// flushes the current in-memory metrics registry into them every 5 seconds
// (fixed, non-configurable) via RecordStatsSnapshot, which writes the global
// row and every per-app row in one short transaction. SQLite is never on
// the event path: the runner and stream consumer update only the Prometheus
// registry, and the flush merely mirrors that store. A failed flush is retried
// next tick with the current values (absolute snapshots make this safe); a hard
// crash may lose up to ~5s of telemetry. Graceful shutdown performs a final
// bounded flush. At startup the worker restores the persisted counters into the
// fresh registry (restorePersistedStats) so the first snapshot never resets
// them. Redis event-processing correctness never depends on SQLite stats.
//
// `relay stats reset` zeroes the cumulative fields IN PLACE: the global stats
// row and every app_stats row survive, decoded from their typed JSON
// payloads, while the live gauges and any known unrelated field are preserved.
// A running worker performs the reset over its socket, under the same lock as
// the flush, so its in-memory totals are reset too and no captured pre-reset
// snapshot can be written afterwards; a stopped worker is reset directly through
// state.ResetStats. Prometheus counters are never reset: the worker keeps a
// Relay-side reset baseline and subtracts it when snapshotting, so the persisted
// totals continue from zero while the counters stay monotonic.
//
// The driver is modernc.org/sqlite (pure Go, CGO-free) so the binary stays
// static under CGO_ENABLED=0 and the CLI needs no external database
// dependencies.
//
// Concurrency model:
//
//   - In-process access is serialized through a single pooled connection
//     (SetMaxOpenConns(1) in Open). Relay's local state is a tiny,
//     low-frequency, single-file workload — reconciler writes, a 5s stats flush,
//     CLI reads — so a pool of 1 makes SQLITE_BUSY structurally impossible:
//     database/sql queues callers on the single connection instead of letting
//     SQLite reject concurrent writers. No write queue is needed because
//     database/sql itself provides the queueing.
//   - Cross-process access (a CLI process opening the same file while the
//     worker runs) is covered by WAL + busy_timeout: WAL is persistent in the DB
//     file and survives close, and each process sets its own busy_timeout on its
//     own connection. One cross-process edge remains: Open always initializes
//     the schema (CREATE TABLE IF NOT EXISTS — a brief write) even for read-only
//     CLI commands, so a CLI may wait on the worker's in-flight flush for up to
//     its busy_timeout; the worker's transactions are short (milliseconds), so
//     this surfaces at worst as a brief startup pause, never a failure.
//   - Transactions are short and hold no external I/O: fingerprints are computed
//     before the transaction opens (see RebuildFromFS/RecordDiscovered), so a
//     write transaction never blocks on the filesystem. Callers that already
//     hold the loaded set and a fingerprint (the worker's startup state phase)
//     use RebuildFromApps + RecordDiscoveredWithFingerprint so no state
//     write re-reads /apps.
package state

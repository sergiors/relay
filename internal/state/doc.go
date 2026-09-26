// Package state persists a read-only view of Relay's function state in a
// local SQLite database.
//
// The state database is a state VIEW, not the source of truth: /functions (the
// filesystem root the loader reads) is authoritative. The database is rebuilt
// automatically when empty and never drives matching, image building, or
// reconciliation decisions. It exists so operators can introspect what Relay
// has loaded and how the last reconcile of each function went, independent of
// Redis or Docker.
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
//     MEANINGFUL reconcile — a success or a failure — so an unchanged-function
//     periodic pass never overwrites them)
//
// A failed reconcile never marks a whole function unavailable: the previously
// active image/fingerprint/prepared_at are retained so the last good version
// still serves. All timestamps are RFC3339 strings.
//
// Function storage model:
//
//   - functions(name TEXT PRIMARY KEY, data BLOB NOT NULL, updated_at TEXT NOT
//     NULL) stores ONE whole per-function snapshot as a JSON object in data,
//     written through SQLite's jsonb(?) (binary JSON / JSONB format) and read
//     back with json(data). Only the stable name key and the write timestamp
//     stay as columns. The nested configuration — the env and secret MAPPINGS
//     (env-var name → literal value, and env-var name → secret REFERENCE, never
//     a secret value), the event handlers (name
//   - timeout), the schedules (handler/cron/timezone/timeout/retries), and the
//     services (entrypoint/image/host/path/port/replicas) — is all part of
//     that one payload, so a template change replaces the snapshot atomically
//     and there are no per-handler/schedule/service child tables.
//   - The handler/event and schedule entries deliberately omit the template
//     parser's opaque matcher patterns: matching is rebuilt from template.yaml,
//     never from this read-only view, and those matcher interfaces cannot be
//     JSON round-tripped.
//
// Secret values are never stored: only the reference names appear in the
// snapshot.
//
// Design Constraint:
//   - State errors are never fatal. Callers (main, reconciler) log them and
//     continue; the state database degrades to "no state available" on failure.
//
// Stats persistence model:
//
// The stats and function_stats tables store the latest persisted ABSOLUTE
// snapshot of Relay's operational counters — idempotent, no deltas. Only stable
// relational metadata is kept as columns: stats.id and stats.updated_at, and
// function_stats.function_name and function_stats.updated_at. The evolving
// counter/gauge/execution-history payload is a JSON object — stored as binary
// JSON (JSONB) in the data BLOB column — marshalled and unmarshalled ONLY
// through stats_json.go; the worker, CLI, runtime, and metrics layers pass typed
// Stats/FunctionStats values and never touch the JSON. This keeps the schema
// stable as instrumentation grows: absent fields decode to zero. The typed
// structs are the source of truth. There is no migration or backward
// compatibility for payloads written by a different schema.
//
// In addition to the event/handler counters, function_stats carries the
// CUMULATIVE warm-container pool counters (warm acquires, cold starts,
// discarded) so the standalone `relay function inspect` process can render the
// Runtime pool section without access to the worker's in-memory pool. The LIVE
// pool gauges (capacity, container counts by lease state) are deliberately NOT
// persisted: a persisted live gauge would go stale between flushes. The worker
// flushes the current in-memory metrics registry into them every 5 seconds
// (fixed, non-configurable) via RecordStatsSnapshot, which writes the global
// row and every per-function row in one short transaction. SQLite is never on
// the event path: the runner and stream consumer update only the Prometheus
// registry, and the flush merely mirrors that store. A failed flush is retried
// next tick with the current values (absolute snapshots make this safe); a hard
// crash may lose up to ~5s of telemetry. Graceful shutdown performs a final
// bounded flush. At startup the worker restores the persisted counters into the
// fresh registry (restorePersistedStats) so the first snapshot never resets
// them. Redis event-processing correctness never depends on SQLite stats.
//
// `relay stats reset` zeroes the cumulative fields IN PLACE: the global stats
// row and every function_stats row survive, decoded from their typed JSON
// payloads, while the live gauges and any known unrelated field are preserved.
// A running worker performs the reset over its socket, under the same lock as
// the flush, so its in-memory totals are reset too and no captured pre-reset
// snapshot can be written afterwards; a stopped worker is reset directly through
// state.ResetStats. Prometheus counters are never reset: the worker keeps a
// Relay-side reset baseline and subtracts it when snapshotting, so the persisted
// totals continue from zero while the counters stay monotonic.
//
// The driver is modernc.org/sqlite (pure Go, CGO-free) so the binary stays
// static under CGO_ENABLED=0 and the CLI is fully read-only with no external
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
//     use RebuildFromFunctions + RecordDiscoveredWithFingerprint so no state
//     write re-reads /functions.
package state

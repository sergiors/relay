// Package schedule owns the identity and cluster-wide publication of cron
// schedule occurrences.
//
// Every Relay worker evaluates a function's cron schedules locally (see
// internal/cron, which wraps gocron). Previously each worker executed the
// scheduled handler itself, so N workers meant N executions per occurrence.
// Now every worker still evaluates the cron locally, but instead of executing
// it publishes the occurrence to a shared Relay event stream with an atomic
//
//	publish-if-new    (one stream entry per occurrence, exactly once cluster-wide)
//
// and the existing consumer-group / PEL / recovery machinery delivers that one
// entry to exactly one worker for execution:
//
//	gocron (every worker) → atomic publish-if-new (Lua) → Redis Stream
//	→ existing consumer group → one worker → runner.InvokeHandler → container
//
// No leader election and no lock is held during execution: the deduplication is
// at publication time (the atomic SET NX + XADD script), and after the single
// entry is in the stream, delivery is Relay's normal at-least-once model.
//
// # Identity semantics
//
// An Occurrence is the deterministic identity of one logical firing: the
// function, the handler it invokes, and the scheduled instant (UTC). Its ID —
// "schedule:<function>:<handler>:<scheduled_at RFC3339 UTC>" — is derived from
// the absolute instant, never from a timezone representation: ScheduledAt is
// normalized to UTC and truncated to the second (5-field cron granularity), so
// DST offsets and timezone encoding never change the ID. The configured
// timezone affects when a schedule fires (its gocron evaluation), never the
// identity. Two workers evaluating the same cron tick produce the same
// Occurrence (and therefore the same ID), so they contend on exactly one
// publish-if-new.
//
// # Atomicity invariant
//
// The SET NX (dedup key) and the XADD (stream entry) run in one Lua script, so
// there is no window where a dedup key exists without its stream entry (a
// worker that "wins" the key always writes the entry in the same atomic step),
// and no window where an entry exists without its key (a failed script leaves
// neither). The dedup key is the history of what has been published; it expires
// only via its TTL (7 days — far longer than any realistic scheduling/recovery
// window, matching the invocation-state TTL) and is never deleted on completion,
// so a later worker evaluating the same (now-stale) tick cannot republish it.
//
// # Failure semantics
//
// One schedule occurrence is published once cluster-wide, while handler
// execution remains at-least-once. A duplicate publication (the key already
// exists) is a clean no-op: another worker published the occurrence first, and
// the single entry routes through the stream to exactly one worker. Publication
// is best-effort across the fleet because every worker evaluates the cron — a
// publish failure on one worker loses that worker's tick, but other workers'
// callbacks still publish the same occurrence. After publication the entry is a
// normal stream message and enjoys the full at-least-once delivery, retry,
// DLQ, and invocation-state semantics of any other message.
//
// # Reconciliation notes
//
// Editing a function's schedules converges its gocron jobs (future occurrences
// use the current cron/timezone/handler) but deliberately does NOT purge dedup
// keys or stream entries: an already-published entry represents an occurrence
// that was valid when published and expires via the key TTL / stream retention.
package schedule

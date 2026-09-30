// Package schedule owns the identity and cluster-wide publication of cron
// schedule occurrences.
//
// Every Relay worker evaluates schedules locally through internal/cron, but a
// logical occurrence is published to the shared Redis stream at most once
// cluster-wide:
//
//	gocron -> schedule.Publisher -> Redis stream -> consumer group -> runner
//
// Publication is deduplicated atomically before the stream entry is admitted.
// No leader election or execution lock is required; once published, the
// occurrence follows Relay's normal at-least-once delivery, retry, invocation
// state, and DLQ semantics.
//
// An Occurrence identifies one logical firing by function, SCHEDULE NAME, and
// scheduled instant; the handler is carried but is not part of the identity.
// ScheduledAt is normalized to UTC before its deterministic ID is derived, so
// workers evaluating the same logical tick produce the same identity regardless
// of timezone representation or callback timing.
//
// Deduplication and stream publication happen in one Redis Lua script. The
// dedup key and XADD therefore succeed or fail as one atomic operation: Relay
// never records an occurrence as published without also admitting its stream
// entry. Dedup keys remain for a bounded TTL so retries, other workers, and
// startup catch-up cannot republish the same occurrence during the recovery
// window.
//
// A duplicate publication is a successful no-op. The cron scheduler retries
// transient publication failures and may republish the latest missed occurrence
// during startup catch-up; both paths reuse the same occurrence identity, so an
// occurrence already published by another worker remains a harmless duplicate.
//
// Updating a function's schedules affects future occurrences only. Existing
// dedup keys and already-published stream entries are left intact and expire
// through their normal retention policies.
package schedule

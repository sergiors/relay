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
// An Occurrence identifies one logical firing by app, SCHEDULE NAME, and
// scheduled instant; the handler is carried but is not part of the identity.
// ScheduledAt is normalized to UTC before its deterministic ID is derived, so
// workers evaluating the same logical tick produce the same identity regardless
// of timezone representation or callback timing.
//
// The package also owns the shared structural and semantic validation of a
// decoded occurrence: ClassifyClaim separates an ordinary event from a
// well-formed schedule claim and from a malformed claim (which must never fall
// through to ordinary event matching). A malformed claim includes a
// scheduled_at that is unparseable or has a sub-second component, because cron
// evaluation is whole-second granularity. Whether a whole-second instant is
// actually a firing is semantic and timezone-dependent, so it is decided by
// Contains/IsFiring (the runner's validateOccurrence), never structurally here:
// historical IANA offsets can carry a seconds component, so a legitimate
// local-minute firing can have a nonzero UTC second. IsFiring/ParseCron
// expose the single cron-firing primitive that both the publisher
// (internal/cron) and the consumer (internal/runner) use, so a claim's
// scheduled_at can be checked against a
// schedule's current definition without divergent cron semantics. None of this
// is cryptographic: it establishes structural and semantic integrity, not
// provenance.
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
// Updating an app's schedules affects future occurrences only. Existing
// dedup keys and already-published stream entries are left intact and expire
// through their normal retention policies.
package schedule

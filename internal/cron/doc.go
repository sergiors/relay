// Package cron owns when app schedules fire. It maps each template schedule
// to a gocron job and hands due occurrences to internal/schedule for
// cluster-wide publication. It does not depend on Docker or Redis directly.
//
// Scheduled execution flows through:
//
//	gocron -> cron -> schedule.Publisher -> Redis stream -> consumer -> runner
//
// Every worker evaluates the same schedules locally. Publication is
// deduplicated atomically by logical occurrence, so only one stream entry is
// admitted cluster-wide and the consumer group delivers it to one worker.
// Scheduled invocations therefore reuse the same runner/runtime path as normal
// event-driven execution.
//
// New constructs the scheduler without starting it. ReplaceApp converges a
// app's jobs, RemoveApp removes them, Start begins firing, and Stop
// stops firing and strictly joins every in-flight publisher callback (including
// callbacks gocron's own bounded shutdown may have abandoned), so no callback
// can still be touching Redis once Stop returns.
//
// Publication durability is intent-first: the EXACT occurrence is written to the
// durable outbox BEFORE it is published, so a crash between persistence and
// publication recovers the occurrence from the store rather than losing it. The
// durable row is deleted only after a publication call returns a nil error
// (published or a clean duplicate) OR once its retention deadline passes without
// resolving (7 days from first insert, state.PendingRetention); an expired record
// is never published, is counted as an expiration, and is removed by a bounded
// cleanup. The Redis occurrence dedup key lives 14 days, so the original key
// still protects every retry the 7-day outbox can make.
//
// If the outbox REJECTS the intent the occurrence is NOT published: it is retained
// in the scheduler's bounded in-memory unsaved set (one slot per schedule), the
// scheduler is driven degraded, and the durable retry worker is woken to flush it
// to the outbox with the existing backoff as soon as the store answers again.
// Recovery flushes the held occurrences to the outbox BEFORE it drains rows or
// runs the bounded catch-up, so an exact held occurrence is never reconstructed
// from the current time or lost behind a newer one. A held occurrence that is
// never flushed — a crash or shutdown while every durable store still rejects the
// write — is the one remaining loss window; it cannot be closed without a third
// durable failure domain. Publication retries reuse the same logical occurrence
// and therefore the same deduplication identity; retries are bounded and stop on
// success, a clean duplicate, or lifecycle cancellation. On startup, CatchUp may
// republish the latest missed occurrence for each schedule within a bounded
// recovery horizon; older occurrences are not replayed and future occurrences are
// never synthesized.
//
// Storage gate: the scheduler may not evaluate or publish an occurrence unless a
// usable durable outbox is installed (SetOutbox, or the storage bootstrap's
// installOwnedOutbox). Every due occurrence is persisted BEFORE it is published
// (intent-first), so an outbox is required for scheduler correctness; the worker
// marks the scheduler unavailable when no outbox could be opened
// (MarkStorageUnavailable) and retries opening one (StartStorageBootstrap). A
// runtime outbox write failure pauses live publication (degraded) and holds the
// exact occurrence in memory until the durable retry worker observes that outbox
// operations work again, at which point the scheduler recovers: it flushes the
// held exact occurrences to the outbox, drains existing rows, re-runs the SAME
// bounded latest-only catch-up for schedule time that passed while paused, and
// only then re-enables live ticks. A held occurrence that no durable store ever
// accepted before a crash or shutdown is the documented fundamental loss window.
// Schedule firing is deliberately NOT a prerequisite for the event consumer,
// services, or worker readiness.
//
// gocron callbacks do not expose the scheduled due instant, so Relay derives the
// latest occurrence at or before the callback time using the same cron parsing
// semantics used for registration. This keeps delayed callbacks and startup
// catch-up aligned on the same occurrence identity. A callback with no
// occurrence within the 24h recovery horizon is dropped and logged, never
// stamped with a fabricated or minute-truncated instant.
//
// Each schedule has an effective IANA timezone. The scheduler runs in UTC and
// encodes the schedule timezone in the cron expression, leaving DST and offset
// handling to Go's time.Location and the cron parser.
//
// Relay accepts minute-precision cron schedules only. Six-field schedules and
// relative @every expressions are rejected because their occurrence identity is
// not deterministic across workers under the current scheduling model.
//
// Publisher.PublishOccurrence is the package boundary: cron decides when an
// occurrence is due; internal/schedule owns occurrence identity,
// deduplication, and publication.
package cron

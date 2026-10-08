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
// Publication retries always reuse the same logical occurrence and therefore the
// same deduplication identity. Retries are bounded and stop on success, a clean
// duplicate, or lifecycle cancellation. The FIRST failed attempt — and a
// cancellation before any attempt — persists the complete immutable occurrence
// intent to the durable outbox BEFORE the bounded in-memory retries continue, so
// a crash during the backoff window still leaves the occurrence recoverable; a
// healthy first-attempt success/duplicate never touches the outbox. A persist
// failure surfaces the occurrence as unresolved and drives the scheduler
// degraded, never substituting in-memory retries for durability. The durable
// retry worker (StartPendingRetry) republishes a persisted row across restarts
// until it resolves or expires. A row is deleted after a publication call
// returns a nil error (published or a clean duplicate) OR once its retention
// deadline passes without resolving (7 days from first insert,
// state.PendingRetention); an expired record is never published, is counted as
// an expiration, and is removed by a bounded cleanup. The Redis occurrence dedup
// key lives 14 days, so the original key still protects every retry the 7-day
// outbox can make. On startup, CatchUp may republish the latest missed
// occurrence for each schedule within a bounded recovery horizon; older
// occurrences are not replayed and future occurrences are never synthesized.
//
// Storage gate: the scheduler may not evaluate or publish an occurrence unless a
// usable durable outbox is installed (SetOutbox, or the storage bootstrap's
// installOwnedOutbox). A live tick must be able to persist its occurrence if
// Redis rejects the publish, so an outbox is required for scheduler correctness;
// the worker marks the scheduler unavailable when no outbox could be opened
// (MarkStorageUnavailable) and retries opening one (StartStorageBootstrap). A
// runtime outbox failure pauses live publication (degraded) until the durable
// retry worker observes that outbox operations work again, at which point the
// scheduler recovers: it drains existing rows, re-runs the SAME bounded
// latest-only catch-up for schedule time that passed while paused, and only then
// re-enables live ticks. Schedule firing is deliberately NOT a prerequisite for
// the event consumer, services, or worker readiness.
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

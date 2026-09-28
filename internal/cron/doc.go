// Package cron owns when function schedules fire. It maps each template schedule
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
// New constructs the scheduler without starting it. ReplaceFunction converges a
// function's jobs, RemoveFunction removes them, Start begins firing, and Stop
// performs bounded shutdown.
//
// Publication retries always reuse the same logical occurrence and therefore the
// same deduplication identity. Retries are bounded and stop on success, a clean
// duplicate, or lifecycle cancellation. On startup, CatchUp may republish the
// latest missed occurrence for each schedule within a bounded recovery horizon;
// older occurrences are not replayed and future occurrences are never
// synthesized.
//
// gocron callbacks do not expose the scheduled due instant, so Relay derives the
// latest occurrence at or before the callback time using the same cron parsing
// semantics used for registration. This keeps delayed callbacks and startup
// catch-up aligned on the same occurrence identity. If no occurrence exists
// within the recovery horizon, the callback time is normalized to minute
// precision as a bounded fallback.
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

package cron

import (
	"context"
	"time"

	robfigcron "github.com/robfig/cron/v3"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"relay/internal/observability/metrics"
	"relay/internal/observability/tracing"
	"relay/internal/schedule"
)

// occurrenceHorizon bounds the occurrence search used by both the callback path
// and the startup catch-up. It is deliberately a fixed constant, not
// user-configurable:
//
//   - Callback: the due occurrence of a fired tick is the most recent schedule
//     occurrence at or before now. gocron fires its timer at the scheduled
//     instant (never early), so the due occurrence is within the callback's
//     scheduling delay of now, far inside this horizon. Searching the horizon
//     makes the stamped occurrence robust to a callback delayed across a
//     matching boundary (which minute-truncation of the wall clock could not
//     express).
//   - Catch-up: on startup, at most the latest missed occurrence per schedule
//     within this horizon is republished; older misses are intentionally
//     dropped (this is a bounded recovery, not an unbounded backlog replay).
const occurrenceHorizon = 24 * time.Hour

// publishRetryDelays are the bounded backoff delays between publication
// attempts for one logical occurrence: four delays means five attempts total
// (the initial attempt plus four retries) over a ~7.6s backoff budget, plus the
// time each attempt spends in Redis. The occurrence identity is computed once
// by the caller and never recomputed while retrying.
//
// The budget is far below the one-minute minimum schedule interval, so the
// retries of one occurrence can never overlap the next tick (five-field
// schedules are minute-granularity). It is a package value (not a const slice)
// so tests can substitute a shorter budget.
var publishRetryDelays = []time.Duration{
	100 * time.Millisecond,
	500 * time.Millisecond,
	2 * time.Second,
	5 * time.Second,
}

// parseSchedule parses a template schedule with the SAME parser configuration
// gocron/v2 uses for a 5-field cron job (gocron's defaultCron.IsValid calls
// cron.ParseStandard with the job's CRON_TZ-prefixed spec), so Relay's
// occurrence derivation and gocron's firing agree by construction and cannot
// drift. Only the accepted schedule grammar reaches here: the 6-field (seconds)
// form and `@every` are rejected before registration (see ReplaceApp and
// internal/app.validateCron).
//
// It delegates to schedule.ParseCron, the shared primitive the consumer side
// (runner claim validation) also uses, so firing semantics have exactly one
// implementation.
func parseSchedule(expr string, loc *time.Location) (robfigcron.Schedule, error) {
	return schedule.ParseCron(expr, loc)
}

// latestOccurrence returns the latest occurrence of sch at or before now,
// searching back at most horizon. It returns ok=false when no occurrence exists
// in (now-horizon, now], in which case the caller must not fabricate one.
//
// It uses only Schedule.Next, which is monotone non-decreasing in its argument,
// so the predicate Next(t) <= now is true for early t and false for late t and
// the greatest whole second satisfying it is found by binary search. The
// occurrence is Next of that second; occurrences are second-aligned, so this is
// the last occurrence at or before now. The search is bounded (about 17
// iterations for a 24h horizon) and never iterates the occurrences in the
// window.
func latestOccurrence(sch robfigcron.Schedule, now time.Time, horizon time.Duration) (time.Time, bool) {
	now = now.UTC()
	loSec := now.Add(-horizon).Unix()
	hiSec := now.Unix()
	if hiSec < loSec {
		return time.Time{}, false
	}
	// nextAt returns the first occurrence strictly after the given whole second.
	nextAt := func(sec int64) time.Time { return sch.Next(time.Unix(sec, 0).UTC()) }
	// atOrBefore reports whether the first occurrence after sec is at or before
	// now (i.e. sec lies before the latest occurrence).
	atOrBefore := func(sec int64) bool {
		next := nextAt(sec)
		return !next.IsZero() && !next.After(now)
	}
	if atOrBefore(hiSec) {
		// A schedule implementation may produce sub-second instants; if its
		// first occurrence after floor(now) is already due, it is the latest.
		// The cron parser used here produces second-aligned instants, so this is a
		// defensive fast path for the generic helper rather than a usual branch.
		return nextAt(hiSec), true
	}
	if !atOrBefore(loSec) {
		return time.Time{}, false
	}
	// Invariant: atOrBefore(lo) is true, atOrBefore(hi) is false.
	for loSec < hiSec {
		mid := loSec + (hiSec-loSec+1)/2
		if atOrBefore(mid) {
			loSec = mid
		} else {
			hiSec = mid - 1
		}
	}
	due := nextAt(loSec)
	if due.IsZero() || due.After(now) {
		return time.Time{}, false
	}
	return due, true
}

// publishOccurrence publishes one logical occurrence through the Publisher with
// a bounded retry loop, returning whether it was newly published and whether the
// publish resolved (success OR duplicate). The occurrence — and therefore its
// ID — is computed by the caller and NEVER recomputed here, so every attempt
// targets the same dedup key.
//
// A success or a clean duplicate (published=false, err=nil) is terminal: a
// duplicate means another worker already published this occurrence, so there is
// nothing to retry. Only a genuine error (Redis unreachable, script error)
// schedules another attempt. Retries observe ctx: cancellation (a gocron job
// shutting down, or the worker lifecycle during catch-up) aborts promptly and
// resolves=false.
//
// Durability: the FIRST failed attempt (and a cancellation before any attempt)
// persists the complete immutable occurrence intent to the configured outbox, so
// a crash during the bounded backoff window still leaves the occurrence
// recoverable. The durable row is deleted only when this call resolves with a nil
// error (published or clean duplicate), so a row never outlives a resolved
// publication.
//
// A persist failure (an outbox is installed but the write errors) is NOT
// recovered by more in-memory retries: the scheduler is driven degraded and the
// occurrence is returned unresolved. With no outbox installed the loop is the
// bounded in-memory retry it always was (standalone/test schedulers); production
// never enters that mode because the scheduler is gated on an installed outbox
// and marked unavailable otherwise.
//
// One `schedule.publish` logical span wraps the WHOLE retry loop, so all
// attempts of one occurrence share a single trace. The Publisher opens its own
// `schedule.publish.attempt` child span per attempt (and injects that attempt's
// context into the stream entry), which preserves the logical trace lineage
// without creating an unrelated root per retry.
//
// Metrics: the Publisher already counts each failed attempt as a real publish
// failure and each duplicate result as a duplicate. This loop adds the
// bounded-policy counters (retries performed, and exhaustion of the budget).
func (s *Scheduler) publishOccurrence(ctx context.Context, o schedule.Occurrence, catchUp bool) (published, resolved bool) {
	return s.publish(ctx, o, catchUp, true)
}

// publish is the shared body of publishOccurrence. gated enables the storage
// gate: live ticks (and the ordinary startup catch-up) pass true, while an
// explicit recovery catch-up passes false so it can publish schedule time that
// passed while the scheduler was paused, BEFORE markRunning re-enables live
// ticks. The atomic pause gate is read without the scheduler lock.
func (s *Scheduler) publish(ctx context.Context, o schedule.Occurrence, catchUp, gated bool) (published, resolved bool) {
	if gated && s.paused.Load() {
		s.log.Debug("Schedule: publication skipped; scheduler not running",
			"app", o.App, "schedule", o.Schedule, "handler", o.Handler, "catchup", catchUp)
		return false, false
	}
	ctx, span := tracing.Start(ctx, "schedule.publish",
		trace.WithAttributes(
			attribute.String("relay.app", o.App),
			attribute.String("relay.schedule", o.Schedule),
			attribute.String("relay.handler", o.Handler),
			attribute.Bool("relay.catchup", catchUp),
		),
	)
	defer span.End()

	id := o.ID()
	log := s.log.With(
		"app", o.App,
		"schedule", o.Schedule,
		"handler", o.Handler,
		"scheduled_at", o.ScheduledAt.UTC().Format(time.RFC3339),
		"occurrence_id", id,
	)

	var queued bool
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			// Lifecycle cancelled before this attempt. The occurrence was still
			// computed (fire only calls publishOccurrence with a real due
			// instant), so it is recorded durably rather than dropped: a later
			// process recovers it. The write uses a cancellation-independent
			// context, so this succeeds even though ctx is already done.
			log.Debug("Schedule: publish cancelled before first attempt", "catchup", catchUp)
			span.SetAttributes(attribute.String("relay.outcome", "cancelled"))
			if _, perr := s.persistPending(o, log); perr != nil {
				s.markDegraded(perr)
			}
			return false, false
		}
		if attempt > 0 {
			// A retry is about to be attempted: count it now, so a performed
			// re-attempt is counted and a cancelled wait before it is not.
			s.metrics.Inc(metrics.MetricSchedulePublishRetries)
		}
		pub, err := s.pub.PublishOccurrence(ctx, o)
		if err == nil {
			if pub {
				log.Info("Schedule: occurrence published")
				span.SetAttributes(attribute.String("relay.outcome", "published"))
			} else {
				log.Debug("Schedule: occurrence already published")
				span.SetAttributes(attribute.String("relay.outcome", "duplicate"))
			}
			// A call resolved with a nil error (published or clean duplicate):
			// remove the durable record, if one was written after an earlier
			// failure. This is the ONE place a pending row is deleted, so the
			// row never outlives a resolved publication.
			if queued {
				s.resolvePending(o, log)
			}
			span.SetStatus(codes.Ok, "")
			return pub, true
		}
		if !queued {
			// The immediate publication failed: persist the complete immutable
			// occurrence intent BEFORE the in-memory retries continue, so a crash
			// during the bounded backoff window still leaves the occurrence
			// recoverable. A healthy first-attempt success/duplicate never
			// reaches here, so the local DB is untouched on the healthy path.
			var perr error
			queued, perr = s.persistPending(o, log)
			if perr != nil {
				// An outbox is installed but persisting failed. In-memory
				// retries cannot substitute for durability: the occurrence would
				// be lost on a crash. Pause live publication immediately and
				// surface this occurrence as explicitly UNRESOLVED (not durably
				// recoverable) rather than continuing to retry in memory.
				s.markDegraded(perr)
				span.RecordError(perr)
				span.SetStatus(codes.Error, perr.Error())
				span.SetAttributes(attribute.String("relay.outcome", "unresolved"))
				log.Warn("Schedule: occurrence unresolved; publish and durable persist both failed",
					"app", o.App, "schedule", o.Schedule, "handler", o.Handler,
					"occurrence_id", id, "catchup", catchUp, "reason", err)
				return false, false
			}
		}
		if ctx.Err() != nil {
			// The attempt failed because the lifecycle was cancelled (the
			// in-flight publish observed it): do not escalate or schedule an
			// in-memory retry. The durable record (if configured) already holds
			// the occurrence for recovery by a later process.
			log.Debug("Schedule: publish attempt cancelled", "attempt", attempt+1, "catchup", catchUp)
			span.SetAttributes(attribute.String("relay.outcome", "cancelled"))
			return false, false
		}

		if attempt >= len(publishRetryDelays) {
			s.metrics.Inc(metrics.MetricSchedulePublishExhausted)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.SetAttributes(attribute.String("relay.outcome", "exhausted"))
			log.Warn("Schedule: publish retries exhausted", "attempts", attempt+1, "catchup", catchUp, "reason", err)
			// The bounded in-memory budget is spent; the durable record (when
			// configured) now carries the occurrence to the retry worker, which
			// keeps trying across temporary/long outages and restarts. With no
			// outbox (standalone/test schedulers) there is no durable copy, which
			// is the documented bounded-retry-only behavior.
			return false, false
		}

		delay := publishRetryDelays[attempt]
		span.RecordError(err)
		log.Warn("Schedule: publish failed; retrying",
			"attempt", attempt+1,
			"next_retry_in", delay,
			"catchup", catchUp,
			"reason", err,
		)
		if !s.wait(ctx, delay) {
			log.Debug("Schedule: publish retries cancelled", "attempt", attempt+1, "catchup", catchUp)
			span.SetAttributes(attribute.String("relay.outcome", "cancelled"))
			return false, false
		}
	}
}

// CatchUp performs the bounded startup catch-up: for every schedule registered
// so far it republishes, through the same bounded retry routine as a live tick,
// the latest occurrence missed within occurrenceHorizon. It is called by the
// worker after seeding the startup apps and BEFORE Scheduler.Start, and
// future occurrences then converge through ReplaceApp as usual.
//
// Policy:
//   - At most ONE occurrence per schedule (the latest at or before a single
//     `now` snapshot); older misses are intentionally dropped rather than
//     replayed as an unbounded backlog.
//   - Occurrences outside the 24h horizon, and all future occurrences, are
//     excluded.
//   - Deduplication is the existing atomic publish-if-new: a catch-up every
//     worker independently performs races on one Redis key, so exactly one
//     stream entry is written and a repeated recovery (across workers or
//     restarts) is a harmless no-op. This is why duplicate catch-up is safe.
//   - It runs at most once per Scheduler lifecycle: live ReplaceApp calls must
//     never synthesize additional catch-up (a changed schedule converges FUTURE
//     occurrences only). A recovery from a paused period re-runs the SAME
//     bounded scan, which is idempotent under the atomic dedup.
//
// It returns the number of occurrences newly published by this worker (not
// counting clean duplicates another worker won). A cancelled ctx aborts the
// scan with whatever it has published so far.
func (s *Scheduler) CatchUp(ctx context.Context) int {
	// A catch-up while live publication is paused (no usable outbox, degraded,
	// or stopped) would publish nothing and must NOT consume the once-only flag:
	// the recovery path re-runs the same bounded scan once storage is usable.
	if s.paused.Load() {
		s.log.Debug("Schedule: catch-up skipped; scheduler not running")
		return 0
	}
	s.mu.Lock()
	if s.stopped || s.catchUpDone {
		s.mu.Unlock()
		return 0
	}
	s.catchUpDone = true
	s.mu.Unlock()
	return s.catchUpScan(ctx, true)
}

// catchUpScan is the shared bounded latest-only catch-up scan used by both the
// once-per-startup CatchUp and a recovery from a paused period. It republishes,
// per registered schedule, the latest occurrence at or before one `now` snapshot
// within occurrenceHorizon, through the same bounded retry routine as a live
// tick. It is idempotent under the atomic publish-if-new, so a recovery scan
// cannot double-publish. gated distinguishes the ordinary (paused-gated) startup
// catch-up from a recovery scan, which runs while the scheduler is still paused
// but with the outbox freshly installed.
func (s *Scheduler) catchUpScan(ctx context.Context, gated bool) int {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return 0
	}
	entries := make([]registeredSchedule, len(s.schedules))
	copy(entries, s.schedules)
	s.mu.Unlock()

	now := s.now().UTC()
	published := 0
	for _, e := range entries {
		due, ok := latestOccurrence(e.parsed, now, occurrenceHorizon)
		if !ok {
			continue
		}
		// Count each occurrence the scan attempts; a duplicate result (another
		// worker already published it) additionally increments the duplicate
		// counter, so the two are independent.
		s.metrics.Inc(metrics.MetricScheduleCatchUp)
		o := schedule.Occurrence{App: e.fn, Schedule: e.name, Handler: e.handler, ScheduledAt: due}
		if pub, resolved := s.publish(ctx, o, true, gated); resolved && pub {
			published++
		}
	}
	s.log.Debug("Schedule: catch-up scan complete", "schedules", len(entries), "published", published)
	return published
}

// recoveryCatchUp re-runs the bounded latest-only catch-up after the scheduler
// recovers from an unavailable or degraded period, to cover schedule time that
// passed while live publication was paused. It deliberately reuses the exact
// same 24h latest-only policy and atomic-dedup semantics as startup catch-up: it
// is a bounded recovery, never an unbounded backlog replay. It runs UNGATED
// (the scheduler is still paused while recovering) so the recovered occurrences
// can be published before live ticks are re-enabled.
func (s *Scheduler) recoveryCatchUp(ctx context.Context) {
	if s == nil {
		return
	}
	s.catchUpScan(ctx, false)
}

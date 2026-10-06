package cron

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"relay/internal/observability/metrics"
	"relay/internal/schedule"
	"relay/internal/state"
)

// Outbox is the durable schedule-publication retry store. It persists the
// COMPLETE immutable occurrence intent of a tick whose immediate publication
// did not resolve, leases due records so concurrent retriers cannot both claim
// one, and deletes a record only once a publication call has resolved with a nil
// error (published or a clean duplicate). *state.State satisfies it; the narrow
// interface keeps cron decoupled from the SQLite implementation and lets tests
// substitute a deterministic fake.
//
// Every method takes a context. The scheduler calls the write methods with a
// cancellation-independent context (see pendingWriteCtx) so an in-flight tick
// cancelled by shutdown still leaves a recoverable row.
type Outbox interface {
	// SavePendingOccurrence inserts p's immutable intent unless a row for the
	// same id already exists, reporting whether a new row was inserted. The
	// retention deadline is stamped by the store on first insert and never
	// refreshed by a re-save.
	SavePendingOccurrence(ctx context.Context, p state.PendingOccurrence) (inserted bool, err error)
	// ClaimPendingOccurrences leases up to limit due, UNEXPIRED records
	// atomically and returns them.
	ClaimPendingOccurrences(ctx context.Context, now, leaseUntil time.Time, limit int) ([]state.PendingOccurrence, error)
	// ReschedulePendingOccurrence increments a record's attempt count and sets
	// its next due instant after a failed retry; it never changes retention.
	ReschedulePendingOccurrence(ctx context.Context, id string, nextAttempt time.Time) error
	// DeletePendingOccurrence removes a resolved record. It is idempotent.
	DeletePendingOccurrence(ctx context.Context, id string) error
	// ExpirePendingOccurrences deletes up to limit records whose retention has
	// passed and returns how many were removed, so the worker can drain an
	// expired backlog in bounded batches.
	ExpirePendingOccurrences(ctx context.Context, now time.Time, limit int) (int, error)
	// NextPendingDue returns the earliest instant the retry worker must wake for
	// an unexpired record: the earlier of its next claim time and its retention
	// deadline, so a record whose backoff would outlive its 7-day expiry still
	// wakes the loop for bounded cleanup.
	NextPendingDue(ctx context.Context, now time.Time) (due time.Time, ok bool, err error)
}

// The local SQLite state store is the production durable retry outbox; this
// assertion keeps the two in lockstep without state importing cron.
var _ Outbox = (*state.State)(nil)

// Durable retry policy constants. They are fixed internal values, not
// configurable: the durable outbox is a recovery mechanism, not a tuning knob.
const (
	// pendingClaimBatch bounds how many records one scan claims, so a large
	// backlog is drained in bounded batches rather than one unbounded query.
	pendingClaimBatch = 64
	// pendingLease is how long a claimed record is protected from another
	// claim. It is short enough that a crashed/wedged retrier's work is
	// reclaimed promptly (the "claim expiry" rescan) and long enough to cover a
	// normal publication attempt.
	pendingLease = time.Minute
	// pendingWriteTimeout bounds one SQLite outbox write/delete. The outbox is
	// written off the publish path and must never hang on a wedged write.
	pendingWriteTimeout = 5 * time.Second
	// pendingCleanupBatch bounds how many EXPIRED records one cleanup pass
	// deletes, so a large backlog of unretryable rows drains in bounded
	// batches rather than one unbounded DELETE. It matches pendingClaimBatch so
	// a single scan does a bounded amount of both kinds of work.
	pendingCleanupBatch = pendingClaimBatch
	// pendingErrorDelay is the pause after a failed scan/due query, so a
	// persistent database error cannot become a busy loop.
	pendingErrorDelay = time.Second
)

// pendingRetryDelays is the durable retry backoff: a capped progression indexed
// by the persisted attempt count. Unlike the bounded in-memory publishRetryDelays
// (which recovers a brief Redis blip within seconds), it keeps retrying across
// temporary and long outages at a capped cadence. Retrying is bounded by the
// record's retention deadline (state.PendingRetention, 7 days), not by the
// attempt count: a record is removed when it expires, after which there is no
// publish attempt. It is a package value so tests can substitute a shorter
// budget.
var pendingRetryDelays = []time.Duration{
	5 * time.Second,
	15 * time.Second,
	30 * time.Second,
	2 * time.Minute,
	5 * time.Minute,
	10 * time.Minute,
}

// pendingBackoff returns the retry delay for a record that has already
// accumulated attempts failed durable retries, capped at the final delay so the
// cadence stops growing while the record remains inside its retention window.
func pendingBackoff(attempts int) time.Duration {
	if attempts < 0 {
		attempts = 0
	}
	if attempts >= len(pendingRetryDelays) {
		return pendingRetryDelays[len(pendingRetryDelays)-1]
	}
	return pendingRetryDelays[attempts]
}

// SetOutbox installs the durable publication-retry store. It must be called
// before StartPendingRetry and before any publication that might need to persist
// (i.e. before CatchUp and Start). Installing a non-nil outbox arms the
// scheduler: it transitions to running so live publication is enabled (gocron is
// not started here; that happens in Start, or in markRunning once the worker has
// requested it). A nil outbox is a no-op. A scheduler that is never given an
// outbox and never marked unavailable keeps the documented standalone behavior
// (bounded in-memory retry only); the production worker always either installs an
// outbox or marks the scheduler unavailable, so it is never unsupervised. A nil
// receiver is a no-op.
func (s *Scheduler) SetOutbox(o Outbox) {
	if s == nil || o == nil {
		return
	}
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.outbox = o
	s.mu.Unlock()
	s.markRunning(false)
}

// pendingWriteCtx returns a bounded context for one outbox write that is NOT
// cancelled when the calling publish context is. Shutdown cancels a gocron job's
// context out from under an in-flight tick; that tick must still record its
// occurrence so the retry worker can recover it, so the write deliberately
// ignores the caller's cancellation.
func pendingWriteCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), pendingWriteTimeout)
}

// wakePending signals the durable retry worker that a record was just persisted,
// so it does not sleep out its backoff before discovering new work. The send is
// non-blocking: a buffered token is enough, and a full buffer means the worker is
// already awake.
func (s *Scheduler) wakePending() {
	select {
	case s.pendingWake <- struct{}{}:
	default:
	}
}

// persistPending records o in the durable outbox for retry, returning whether
// the occurrence is now durably queued and, separately, any outbox write error.
//
// Semantics distinguish the two "not queued" cases:
//   - No outbox installed (standalone/test scheduler): (false, nil). The caller
//     falls back to the documented bounded in-memory retry.
//   - Outbox installed but the write failed: (false, err). The caller MUST NOT
//     substitute in-memory retries for persistence: it drives the scheduler
//     degraded and surfaces the occurrence as unresolved.
//
// A pre-existing row for the same ID counts as queued: the intent is immutable
// and identical, so persistence is idempotent under concurrent immediate and
// durable observations.
func (s *Scheduler) persistPending(o schedule.Occurrence, log *slog.Logger) (queued bool, err error) {
	ob := s.currentOutbox()
	if ob == nil {
		return false, nil
	}
	wctx, cancel := pendingWriteCtx()
	defer cancel()
	inserted, err := ob.SavePendingOccurrence(wctx, state.PendingOccurrence{
		ID:          o.ID(),
		App:         o.App,
		Schedule:    o.Schedule,
		Handler:     o.Handler,
		ScheduledAt: o.ScheduledAt,
		// Due immediately: the durable worker should observe this occurrence as
		// soon as it wakes.
		NextAttempt: s.now(),
	})
	if err != nil {
		log.Warn("Schedule: persist pending occurrence failed", "reason", err)
		return false, err
	}
	if inserted {
		s.metrics.Inc(metrics.MetricSchedulePendingPersisted)
	}
	s.wakePending()
	return true, nil
}

// resolvePending removes o's durable record after a publication call resolved
// with a nil error. A delete failure is logged, drives the scheduler degraded,
// and the row is left in place: it is idempotent to retry, so the durable worker
// will publish the occurrence again (a clean duplicate) and attempt the delete
// again.
func (s *Scheduler) resolvePending(o schedule.Occurrence, log *slog.Logger) {
	ob := s.currentOutbox()
	if ob == nil {
		return
	}
	wctx, cancel := pendingWriteCtx()
	defer cancel()
	if err := ob.DeletePendingOccurrence(wctx, o.ID()); err != nil {
		log.Warn("Schedule: delete pending occurrence failed; will retry", "reason", err)
		s.markDegraded(err)
	}
}

// StartPendingRetry starts the durable publication-retry worker, which reclaims
// persisted occurrences and republishes them until each resolves. The lifecycle
// context is always recorded (even when no outbox is installed yet), so a later
// storage recovery can start the worker without the worker re-supplying it. The
// worker itself starts only when an outbox is installed. It is a no-op when it
// was already started or after Stop. The worker observes ctx (the worker
// lifecycle), and Stop additionally cancels and joins it, so no retry can touch
// Redis or the state DB after the scheduler barrier returns. The worker scans
// immediately on start, so records persisted before a restart are retried as
// soon as the process is up.
func (s *Scheduler) StartPendingRetry(ctx context.Context) {
	s.mu.Lock()
	s.pendingCtx = ctx
	if s.outbox == nil || s.stopped || s.pendingDone != nil {
		s.mu.Unlock()
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.pendingCancel = cancel
	done := make(chan struct{})
	s.pendingDone = done
	s.mu.Unlock()
	go func() {
		defer close(done)
		s.pendingRetryLoop(runCtx)
	}()
}

// pendingRetryLoop is the durable retry worker: it drains due records and
// expired records in bounded batches, then sleeps until the earliest next wake
// instant, a wake signal, or lifecycle cancellation. A wake instant is the
// earlier of a record's next retry and its retention deadline, so a row whose
// backoff would outlive its 7-day expiry still wakes the loop in time for the
// bounded cleanup to remove it. It never busy-polls: an empty outbox waits only
// for a wake or cancellation, and a non-empty one waits on a timer for the next
// due record. A failed scan/due query pauses briefly rather than spinning.
//
// When the scheduler is paused (degraded), the loop does not publish live
// occurrences; instead it probes the outbox for recovery, and only once outbox
// operations work again does it drain existing rows, run the bounded latest-only
// catch-up, and re-enable live publication.
func (s *Scheduler) pendingRetryLoop(ctx context.Context) {
	log := s.log.With("component", "schedule_retry")
	for {
		if ctx.Err() != nil {
			return
		}
		if s.paused.Load() {
			if !s.attemptRecovery(ctx, log) {
				if !s.pendingSleep(ctx, pendingErrorDelay, s.pendingWake) {
					return
				}
				continue
			}
			// Recovered: fall through to a normal scan.
		}
		n, err := s.RunPendingOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Warn("Schedule: durable retry scan failed", "reason", err)
			s.markDegraded(err)
			if !s.pendingSleep(ctx, pendingErrorDelay, s.pendingWake) {
				return
			}
			continue
		}
		if n > 0 {
			// More due records may remain: scan again immediately rather than
			// sleeping.
			continue
		}
		due, ok, derr := s.NextPendingDue(ctx)
		if derr != nil {
			if ctx.Err() != nil {
				return
			}
			log.Warn("Schedule: durable retry due query failed", "reason", derr)
			s.markDegraded(derr)
			if !s.pendingSleep(ctx, pendingErrorDelay, s.pendingWake) {
				return
			}
			continue
		}
		d := time.Duration(-1)
		if ok {
			if d = due.Sub(s.now()); d < 0 {
				d = 0
			}
		}
		if !s.pendingSleep(ctx, d, s.pendingWake) {
			return
		}
	}
}

// attemptRecovery probes a paused scheduler's outbox for liveness and, once the
// outbox operations work again, drains existing durable rows, runs the bounded
// latest-only catch-up for schedule time that passed while paused, and re-enables
// live publication. It reports whether the scheduler is no longer paused. A
// still-failing probe re-affirms degraded (idempotent) and returns false so the
// loop backs off. It is a no-op returning false while no outbox is installed
// (startup-unavailable until the storage bootstrap installs one).
func (s *Scheduler) attemptRecovery(ctx context.Context, log *slog.Logger) bool {
	ob := s.currentOutbox()
	if ob == nil {
		return false
	}
	// Clear the fault flag so a failure during the recovery drain/catch-up can be
	// detected even though the scheduler is already degraded (so markDegraded
	// cannot signal a new transition).
	s.fault.Store(false)
	if _, _, err := s.NextPendingDue(ctx); err != nil {
		if ctx.Err() != nil {
			return false
		}
		log.Warn("Schedule: durable retry recovery probe failed", "reason", err)
		s.markDegraded(err)
		return false
	}
	// The outbox answers again: drain existing rows, run the bounded catch-up,
	// then re-enable live publication.
	var runCtx context.Context = ctx
	s.mu.Lock()
	if s.pendingCtx != nil {
		runCtx = s.pendingCtx
	}
	s.mu.Unlock()
	if _, err := s.RunPendingOnce(ctx); err != nil {
		if ctx.Err() != nil {
			return false
		}
		log.Warn("Schedule: durable retry recovery drain failed", "reason", err)
		s.markDegraded(err)
		return false
	}
	s.recoveryCatchUp(runCtx)
	// A failure during the drain or catch-up (e.g. a persist write on the
	// recovered outbox) leaves the scheduler degraded: live publication must stay
	// paused until a later cycle sees a clean pass.
	if s.fault.Load() {
		log.Warn("Schedule: durable retry recovery incomplete; a store operation still failed")
		return false
	}
	s.markRunning(true)
	return true
}

// NextPendingDue returns the earliest instant the retry worker must wake,
// using the installed outbox: the earlier of an unexpired record's next claim
// time and its retention deadline (so a record whose backoff would outlive its
// 7-day expiry still wakes the loop, which then removes it via bounded
// cleanup). It is a thin accessor so the recovery loop and tests share one path;
// when no outbox is installed it reports an empty outbox. Expired records are
// excluded by the store, so only expired leftovers yield ok=false.
func (s *Scheduler) NextPendingDue(ctx context.Context) (time.Time, bool, error) {
	ob := s.currentOutbox()
	if ob == nil {
		return time.Time{}, false, nil
	}
	return ob.NextPendingDue(ctx, s.now())
}

// RunPendingOnce performs one durable-retry cycle: it atomically claims up to
// pendingClaimBatch due, unexpired records and attempts each, then deletes up to
// pendingCleanupBatch EXPIRED records. It returns the number of records handled
// (claimed-and-attempted plus expired-and-removed), so a full batch makes the
// worker scan again immediately while each individual DB operation stays
// bounded. It is exported so shutdown and retry behavior can be driven
// deterministically in tests without timers.
//
// A claim that a later lifecycle cancellation interrupts leaves the claimed
// records leased; they are reclaimed after pendingLease expires, so no
// occurrence is lost when a publication is cut short at shutdown. A claim or
// cleanup error drives the scheduler degraded, so a failing outbox pauses live
// publication instead of silently dropping durable work.
func (s *Scheduler) RunPendingOnce(ctx context.Context) (int, error) {
	ob := s.currentOutbox()
	if ob == nil {
		return 0, nil
	}
	now := s.now()
	claimed, err := ob.ClaimPendingOccurrences(ctx, now, now.Add(pendingLease), pendingClaimBatch)
	if err != nil {
		s.markDegraded(err)
		return 0, err
	}
	log := s.log.With("component", "schedule_retry")
	processed := 0
	for _, p := range claimed {
		if ctx.Err() != nil {
			// Leave the remaining leases to expire; they are recoverable.
			break
		}
		s.retryPending(ctx, p, log)
		processed++
	}

	// Bounded cleanup of records past their retention deadline. Expired records
	// are never published (claims exclude them and retryPending re-checks the
	// deadline), so removing them is the only correct disposition and keeps the
	// outbox from accumulating unretryable rows. Each call deletes at most
	// pendingCleanupBatch rows, so a large backlog drains across cycles
	// rather than in one unbounded statement. A cleanup error is a store fault
	// and pauses live publication like any other outbox failure; the rows stay
	// for the next pass. It is skipped when the lifecycle is already cancelled,
	// so shutdown does not surface a spurious degraded transition.
	if ctx.Err() != nil {
		return processed, nil
	}
	expired, cerr := ob.ExpirePendingOccurrences(ctx, now, pendingCleanupBatch)
	if cerr != nil {
		s.markDegraded(cerr)
		return processed, cerr
	}
	if expired > 0 {
		s.metrics.Add(metrics.MetricSchedulePendingExpired, int64(expired))
		log.Warn("Schedule: expired pending occurrences removed",
			"count", expired, "retention", state.PendingRetention)
	}
	return processed + expired, nil
}

// retryPending attempts one claimed record. A nil-error publication (newly
// published or a clean duplicate) deletes the record; a failure reschedules it
// with the durable backoff; a cancellation leaves the record leased for
// reclaim. A record whose retention deadline has passed since the claim is
// EXPIRED: it is removed, never published and never rescheduled, and it is
// counted as expiration rather than as a publish attempt.
//
// Before any publisher call it validates the record: a row whose stored intent
// could not be decoded (p.DecodeErr), or whose decoded fields reconstruct an
// occurrence ID different from the row's key (p.ID), is NEVER published (that
// would target the wrong occurrence) and NEVER deleted (that would lose the
// pending work). It is logged for an operator and rescheduled under the same
// bounded backoff, so the durable row is retained and repairable while it does
// not spin.
func (s *Scheduler) retryPending(ctx context.Context, p state.PendingOccurrence, log *slog.Logger) {
	// Expiry guard at the claim-to-publish boundary: do not begin an attempt
	// once now >= expires_at_ms, and never reschedule an expired row. Removing
	// it here covers the window between the store's claim and this call. It is
	// expiration, not a publish retry, so it is neither counted nor logged as
	// one.
	if !p.ExpiresAt.After(s.now()) {
		s.expirePending(p, log)
		return
	}

	s.metrics.Inc(metrics.MetricSchedulePendingRetries)

	// Identity guard: the stored key is the derived occurrence ID, so the
	// decoded intent MUST reconstruct exactly that ID. A decode failure or a
	// mismatch is corrupt coordination state, not an occurrence: retain and
	// reschedule it (never publish, never delete).
	if err := s.pendingIdentityError(p); err != nil {
		log.Warn("Schedule: pending occurrence corrupt; retaining for repair",
			"occurrence_id", p.ID, "durable_attempts", p.Attempts+1, "reason", err)
		s.reschedulePending(p, log)
		return
	}

	o := schedule.Occurrence{App: p.App, Schedule: p.Schedule, Handler: p.Handler, ScheduledAt: p.ScheduledAt}

	published, err := s.pub.PublishOccurrence(ctx, o)
	if err == nil {
		if published {
			log.Info("Schedule: pending occurrence published",
				"app", o.App, "schedule", o.Schedule, "handler", o.Handler, "occurrence_id", o.ID(), "durable_attempts", p.Attempts+1)
		} else {
			log.Debug("Schedule: pending occurrence resolved as duplicate",
				"app", o.App, "schedule", o.Schedule, "handler", o.Handler, "occurrence_id", o.ID(), "durable_attempts", p.Attempts+1)
		}
		// Resolve with a cancellation-independent context: the publication
		// succeeded, so the row must be removable even if the lifecycle is
		// shutting down.
		ob := s.currentOutbox()
		if ob != nil {
			wctx, cancel := pendingWriteCtx()
			if derr := ob.DeletePendingOccurrence(wctx, p.ID); derr != nil {
				log.Warn("Schedule: delete pending occurrence failed; will retry",
					"app", o.App, "schedule", o.Schedule, "occurrence_id", p.ID, "reason", derr)
				s.markDegraded(derr)
			}
			cancel()
		}
		return
	}
	if ctx.Err() != nil {
		// The attempt was cut short by lifecycle cancellation. Leave the record
		// leased; it is reclaimed after the lease expires on the next process.
		log.Debug("Schedule: pending retry cancelled",
			"app", o.App, "schedule", o.Schedule, "occurrence_id", p.ID, "durable_attempts", p.Attempts+1)
		return
	}
	s.reschedulePending(p, log)
	log.Warn("Schedule: pending retry failed; rescheduled",
		"app", o.App, "schedule", o.Schedule, "handler", o.Handler,
		"occurrence_id", p.ID, "durable_attempts", p.Attempts+1,
		"next_retry_in", pendingBackoff(p.Attempts), "reason", err,
	)
}

// expirePending removes one record whose retention has passed. It is the ONLY
// disposition for an expired record: never publish, never reschedule. Removal
// uses a cancellation-independent context so a shutdown cannot leave an expired
// row behind, and the expiration counter increments only once the delete
// actually commits, so a failed cleanup is not double-counted and the row is
// removed by the next bounded cleanup pass. A delete failure drives the
// scheduler degraded, like any other outbox fault.
func (s *Scheduler) expirePending(p state.PendingOccurrence, log *slog.Logger) {
	ob := s.currentOutbox()
	if ob == nil {
		return
	}
	wctx, cancel := pendingWriteCtx()
	defer cancel()
	if err := ob.DeletePendingOccurrence(wctx, p.ID); err != nil {
		log.Warn("Schedule: expired pending occurrence delete failed; will retry cleanup",
			"occurrence_id", p.ID, "expires_at", p.ExpiresAt.UTC().Format(time.RFC3339), "reason", err)
		s.markDegraded(err)
		return
	}
	s.metrics.Inc(metrics.MetricSchedulePendingExpired)
	log.Warn("Schedule: pending occurrence expired; removed",
		"app", p.App, "schedule", p.Schedule, "handler", p.Handler,
		"occurrence_id", p.ID, "expires_at", p.ExpiresAt.UTC().Format(time.RFC3339))
}

// pendingIdentityError validates a claimed record before publication. It reports
// a non-nil error when the record cannot be trusted as the occurrence its key
// names: a decode failure (no intent), or decoded fields that reconstruct an
// occurrence ID different from the stored key. The reconstructed identity is
// schedule.Occurrence.ID — the SAME derivation the publisher and consumer use —
// so the check is exact rather than a re-implementation.
func (s *Scheduler) pendingIdentityError(p state.PendingOccurrence) error {
	if p.DecodeErr != nil {
		return fmt.Errorf("stored payload undecodable: %w", p.DecodeErr)
	}
	reconstructed := schedule.Occurrence{
		App: p.App, Schedule: p.Schedule, Handler: p.Handler, ScheduledAt: p.ScheduledAt,
	}
	if got := reconstructed.ID(); got != p.ID {
		return fmt.Errorf("row identity %q does not match reconstructed occurrence %q", p.ID, got)
	}
	return nil
}

// reschedulePending records one failed durable observation for p, advancing its
// attempt count and next due instant under the bounded backoff. It is the shared
// failure path for a publisher error and for a corrupt/mismatched record, and it
// NEVER deletes the row. A reschedule failure drives the scheduler degraded, so a
// failing outbox pauses live publication rather than silently leaving durable
// work uncoordinated.
func (s *Scheduler) reschedulePending(p state.PendingOccurrence, log *slog.Logger) {
	ob := s.currentOutbox()
	if ob == nil {
		return
	}
	next := s.now().Add(pendingBackoff(p.Attempts))
	wctx, cancel := pendingWriteCtx()
	defer cancel()
	if rerr := ob.ReschedulePendingOccurrence(wctx, p.ID, next); rerr != nil {
		log.Warn("Schedule: reschedule pending occurrence failed",
			"occurrence_id", p.ID, "reason", rerr)
		s.markDegraded(rerr)
	}
}

// pendingSleep pauses the durable retry worker for up to d (indefinitely when
// d < 0) via the pendingWait seam, reporting whether the worker should continue
// (true) or exit because the lifecycle was cancelled (false).
func (s *Scheduler) pendingSleep(ctx context.Context, d time.Duration, wake <-chan struct{}) bool {
	return s.pendingWait(ctx, d, wake)
}

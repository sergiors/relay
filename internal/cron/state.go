package cron

import (
	"context"
	"io"
	"time"

	"relay/internal/observability/metrics"
)

// Scheduler storage state, degraded/recovery gating.
//
// The scheduler publishes occurrences into the same Redis stream as external
// events, but a live tick is only safe when its occurrence can be durably
// persisted if Redis rejects it (see publishOccurrence and the durable outbox).
// The worker therefore wires a durable outbox (the local SQLite store) before
// enabling schedule publication and marks the scheduler unavailable when no
// outbox could be opened. A runtime outbox failure pauses live publication
// (degraded) until the durable retry worker observes that outbox operations
// work again, at which point the scheduler recovers: it drains existing rows,
// re-runs the bounded latest-only catch-up for schedule time that passed while
// paused, and only then re-enables live ticks. None of this gates the event
// consumer, services, or worker readiness: schedule firing is not a prerequisite
// for processing external events.

// storageUnavailableReason is the fixed low-cardinality reason attached to the
// unavailable log. It is deliberately constant (no raw error text) so it never
// becomes a log/metric label.
const storageUnavailableReason = "durable outbox not installed"

// storageBootstrapDelay is the pause between attempts to open the scheduler's
// durable outbox when the worker's shared state handle was unavailable at
// startup. It is fixed and moderately short: the scheduler recovers promptly
// after the local store becomes usable without busy-polling a persistent
// failure.
const storageBootstrapDelay = 30 * time.Second

// OutboxOpener opens a durable outbox and returns it with a closer that releases
// its resources. Production wires state.Open (the *state.State handle satisfies
// Outbox and io.Closer); tests substitute a deterministic fake. A non-nil closer
// is owned by the scheduler and closed only after all scheduler work has stopped.
type OutboxOpener func() (Outbox, io.Closer, error)

// MarkStorageUnavailable marks the scheduler unavailable because no usable
// durable outbox could be installed at startup. Live publication is paused and
// the deferred gocron scheduler is not started; StartStorageBootstrap may later
// install an outbox and recover. It is idempotent; a stopped scheduler ignores
// it.
func (s *Scheduler) MarkStorageUnavailable() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.stopped || s.state == stateUnavailable {
		s.mu.Unlock()
		return
	}
	s.state = stateUnavailable
	s.paused.Store(true)
	// The metric update is inside the transition critical section so concurrent
	// storage transitions cannot leave scheduler_state stale (see markRunning).
	// metrics.Registry methods never call back into the scheduler, so no lock
	// inversion is possible.
	s.metrics.SetSchedulerState(metrics.SchedulerStateUnavailable)
	s.mu.Unlock()
	s.log.Warn("Schedule: scheduler unavailable; schedule publication disabled",
		"reason", storageUnavailableReason)
}

// StartStorageBootstrap starts a scheduler-owned, cancellable retry loop that
// periodically opens a durable outbox when the worker's shared state handle was
// unavailable at startup. The loop installs the opened outbox, starts the
// durable retry worker, drains existing rows, runs the bounded latest-only
// catch-up for schedule time that passed while unavailable, and only then enables
// live publication. Stop cancels and joins the loop, so no bootstrap open or
// install can touch the store after the scheduler barrier returns. It is a
// no-op when the scheduler is stopped or a bootstrap is already running.
func (s *Scheduler) StartStorageBootstrap(ctx context.Context, open OutboxOpener) {
	if s == nil || open == nil {
		return
	}
	s.mu.Lock()
	if s.stopped || s.bootstrapDone != nil {
		s.mu.Unlock()
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.bootstrapCancel = cancel
	done := make(chan struct{})
	s.bootstrapDone = done
	s.mu.Unlock()
	go func() {
		defer close(done)
		s.storageBootstrapLoop(runCtx, open)
	}()
}

// storageBootstrapLoop periodically attempts to open the durable outbox until it
// succeeds, the scheduler stops, or the lifecycle is cancelled. A failed open
// pauses under the fixed bootstrap delay (never a busy loop); the first success
// installs the outbox and ends the loop.
func (s *Scheduler) storageBootstrapLoop(ctx context.Context, open OutboxOpener) {
	log := s.log.With("component", "schedule_storage")
	for {
		if ctx.Err() != nil {
			return
		}
		ob, closer, err := open()
		if err != nil {
			log.Warn("Schedule: durable outbox open failed; retrying", "reason", err)
			if !s.pendingSleep(ctx, storageBootstrapDelay, s.pendingWake) {
				return
			}
			continue
		}
		// The scheduler may have stopped while the store was opening: install
		// refuses and the just-opened handle is released here rather than leaked.
		if !s.installOwnedOutbox(ctx, ob, closer) {
			if closer != nil {
				_ = closer.Close()
			}
			return
		}
		return
	}
}

// installOwnedOutbox installs an outbox opened by the storage bootstrap (a
// scheduler-owned handle, when the worker's shared state handle was nil), starts
// the durable retry worker, and immediately attempts recovery (drain existing
// rows, run the bounded latest-only catch-up, re-enable live publication). If
// recovery cannot complete because the freshly opened store is not yet
// operable, the durable retry worker's recovery probe retries it on the next
// cycle rather than failing the bootstrap. It reports false when the scheduler
// has already stopped, in which case the caller releases the opened handle.
func (s *Scheduler) installOwnedOutbox(ctx context.Context, ob Outbox, closer io.Closer) bool {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return false
	}
	s.outbox = ob
	if closer != nil {
		s.ownedCloser = closer
	}
	s.mu.Unlock()

	s.log.Info("Schedule: durable outbox opened; recovering scheduler")
	// Recover before starting the durable retry worker, so the bootstrap's own
	// recovery scan cannot race the worker's recovery probe. If the immediate
	// recovery does not complete (the store answers the open but not a scan), the
	// worker started below keeps probing and recovers on a later cycle.
	s.attemptRecovery(ctx, s.log.With("component", "schedule_storage"))
	s.StartPendingRetry(ctx)
	return true
}

// markRunning enables live publication: it flips the state to running, clears
// the atomic pause gate, records the one-hot state and (on a real transition
// back from degraded/unavailable) the recovery counter, and starts the deferred
// gocron scheduler when the worker has already requested Start. It never starts
// gocron from a stopped scheduler. recovered distinguishes the recovery
// transition (counted and logged) from the initial running setup.
//
// Callers run the bounded recovery catch-up BEFORE calling markRunning so live
// ticks cannot be admitted before schedule time that passed while paused has been
// recovered.
func (s *Scheduler) markRunning(recovered bool) {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	was := s.state
	s.state = stateRunning
	s.paused.Store(false)
	s.fault.Store(false)
	startNow := s.startRequested && !s.gocronStarted && s.g != nil
	if startNow {
		s.gocronStarted = true
	}
	// The metric update is inside the transition critical section, so concurrent
	// MarkStorageUnavailable/markDegraded/markRunning calls cannot interleave a
	// state write with a gauge write and leave scheduler_state stale/incoherent.
	// metrics.Registry methods take only their own internal locks and never call
	// back into the scheduler, so holding s.mu here cannot invert locks.
	s.metrics.SetSchedulerState(metrics.SchedulerStateRunning)
	recovery := recovered && was != stateRunning
	if recovery {
		s.metrics.Inc(metrics.MetricSchedulerRecoveries)
	}
	count := 0
	if startNow {
		count = len(s.g.Jobs())
		// Call gocron's Start WHILE STILL HOLDING s.mu, serialized against Stop's
		// g.Shutdown (also initiated under s.mu): a concurrent Stop cannot slip
		// its Shutdown into the unlock/g.Start gap and leave markRunning racing or
		// restarting an already-shutdown gocron, which is not restartable after
		// Shutdown. g.Start is non-blocking. See Scheduler.Start for the same
		// handshake.
		s.startGocronNow()
	}
	s.mu.Unlock()

	if recovery {
		s.log.Info("Schedule: scheduler recovered; live schedule publication enabled",
			"previous_state", was.metricLabel())
	}
	if startNow {
		s.log.Info("Cron scheduler started", "count", count)
	}
}

// markDegraded pauses live publication because an outbox operation failed. It is
// idempotent per degraded episode: repeated failures while already degraded do
// not re-count the transition. It applies from uninitialized/unavailable too (an
// outbox that was just opened but cannot be operated is degraded, not
// unavailable), and a stopped scheduler is ignored.
func (s *Scheduler) markDegraded(reason error) {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	// Record the fault even if already degraded: the recovery path uses the flag
	// to detect a store operation failing while it is running, where markDegraded
	// cannot signal a fresh state transition.
	s.fault.Store(true)
	if s.state == stateDegraded {
		s.mu.Unlock()
		return
	}
	was := s.state
	s.state = stateDegraded
	s.paused.Store(true)
	// The metric updates are inside the transition critical section so concurrent
	// storage transitions cannot leave scheduler_state stale (see markRunning).
	// metrics.Registry methods never call back into the scheduler, so no lock
	// inversion is possible.
	s.metrics.Inc(metrics.MetricSchedulerDegraded)
	s.metrics.SetSchedulerState(metrics.SchedulerStateDegraded)
	s.mu.Unlock()

	s.log.Warn("Schedule: scheduler degraded; live schedule publication paused",
		"previous_state", was.metricLabel(), "reason", reason)
}

// currentOutbox snapshots the installed outbox under the scheduler lock, so the
// durable retry worker and the publication path never race a recovery that
// installs or replaces it.
func (s *Scheduler) currentOutbox() Outbox {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.outbox
}

// StateLabel returns the scheduler's current storage state as its metrics label.
// It is used by observability wiring and tests; a nil receiver reports
// unavailable.
func (s *Scheduler) StateLabel() string {
	if s == nil {
		return metrics.SchedulerStateUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.metricLabel()
}

// Degraded reports whether live schedule publication is currently paused because
// of a failed outbox operation (as opposed to unavailable, which means no outbox
// was ever installed). It is intended for observability wiring and tests.
func (s *Scheduler) Degraded() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state == stateDegraded
}

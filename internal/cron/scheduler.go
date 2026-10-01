package cron

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"
	robfigcron "github.com/robfig/cron/v3"

	"relay/internal/app"
	"relay/internal/observability/metrics"
	"relay/internal/schedule"
)

// Publisher publishes one schedule occurrence cluster-wide. *schedule.
// SchedulePublisher satisfies it; the cron scheduler never touches Redis or
// execution internals.
type Publisher interface {
	PublishOccurrence(ctx context.Context, o schedule.Occurrence) (published bool, err error)
}

// registeredSchedule is one app schedule Relay has registered: the
// app, the schedule's stable name, the handler it invokes, the
// registration-relevant configuration (cron expression and effective timezone)
// alongside the schedule parsed with the SAME parser gocron uses, so occurrence
// derivation (both the callback path and the startup catch-up) matches gocron's
// firing exactly. Entries are recorded by ReplaceApp (mutex-protected) and
// read by CatchUp; they never drive the live gocron jobs (those remain gocron's
// responsibility). ReplaceApp compares handler/cron/location to decide
// whether an existing job can be retained, while parsed serves catch-up.
type registeredSchedule struct {
	fn       string
	name     string
	handler  string
	cron     string
	location string
	parsed   robfigcron.Schedule
}

// callbackTracker tracks the in-flight Relay publisher callbacks — the
// Redis-facing part of a gocron task body — so shutdown can join them
// independently of gocron's own bounded stop.
//
// gocron's Shutdown is bounded by WithStopTimeout and its executor returns
// ErrStopJobsTimedOut while a task goroutine is still running, so a Relay
// callback may still be inside Publisher.PublishOccurrence (and therefore
// touching Redis) after g.Shutdown has returned. The worker must not close
// Redis until every such callback has returned; this tracker is that join.
type callbackTracker struct {
	mu      sync.Mutex
	closing bool
	wg      sync.WaitGroup
	// done is created by close and closed once every admitted callback has
	// returned. It lets Stop wait for the join under a caller context instead
	// of blocking unbounded.
	done chan struct{}
}

// begin admits one in-flight callback, returning false once shutdown has closed
// admission. closing and the Add share mu, so the join can never race an Add:
// after close returns, begin always reports false and wg is only ever
// decremented.
func (c *callbackTracker) begin() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing {
		return false
	}
	c.wg.Add(1)
	return true
}

// end reports one admitted callback as returned.
func (c *callbackTracker) end() { c.wg.Done() }

// close stops admitting new callbacks and starts the join watcher. It is
// idempotent and safe for concurrent callers. The watcher goroutine terminates
// as soon as the last admitted callback returns.
//
// The watcher is started only after closing is set under mu, so no Add can race
// it: begin observes closing and never increments the WaitGroup again.
func (c *callbackTracker) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing {
		return
	}
	c.closing = true
	c.done = make(chan struct{})
	go func() {
		c.wg.Wait()
		close(c.done)
	}()
}

// doneCh returns the join-completion channel, or nil when close has not run.
func (c *callbackTracker) doneCh() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

// Scheduler wraps a gocron scheduler to run an app's template schedules.
type Scheduler struct {
	log *slog.Logger
	pub Publisher
	g   gocron.Scheduler
	// metrics is the bounded publication-recovery counters sink; nil is
	// nil-safe (every metric call is a no-op), matching the publisher's
	// registry.
	metrics *metrics.Registry
	mu      sync.Mutex
	stopped bool
	// callbacks tracks in-flight publisher callbacks so Stop can strictly join
	// them before Redis closes (see callbackTracker). begin is taken at the top
	// of every gocron task body.
	callbacks callbackTracker
	// shutdownDone is closed once the single gocron Shutdown launched by Stop
	// returns, and shutdownErr holds its result. Repeated Stop calls (the
	// worker's barrier re-invokes Stop after its bound expires) join the SAME
	// shutdown instead of returning early.
	shutdownDone chan struct{}
	shutdownErr  error
	// schedules records the parsed schedules registered so far, used only by
	// the once-per-Scheduler startup CatchUp. It is replaced per app by
	// ReplaceApp so it converges alongside the gocron jobs.
	schedules []registeredSchedule
	// catchUpDone guards the once-only startup catch-up: live ReplaceApp
	// calls must never synthesize additional catch-up.
	catchUpDone bool
	// now provides the current time used to identify schedule occurrences.
	now func() time.Time
	// wait blocks for d or until ctx is cancelled, reporting whether the delay
	// elapsed (true) or ctx was cancelled first (false). Production uses a
	// timer; tests substitute a fake so the retry backoff is deterministic and
	// sleep-free.
	wait func(ctx context.Context, d time.Duration) bool
}

// New constructs a Scheduler over the given publisher with a nil metrics
// registry. The gocron scheduler is pinned to UTC (per-job timezones are encoded
// as CRON_TZ prefixes) and its shutdown is bounded by WithStopTimeout.
func New(pub Publisher, logger *slog.Logger) *Scheduler {
	return NewWithMetrics(pub, logger, nil)
}

// NewWithMetrics constructs a Scheduler that also records the bounded
// publication-recovery counters. It mirrors runner.NewWithMetrics: New is the
// nil-metrics convenience, and the worker wires the registry through this
// constructor. A nil registry is nil-safe.
func NewWithMetrics(pub Publisher, logger *slog.Logger, metricsRegistry *metrics.Registry) *Scheduler {
	g, err := gocron.NewScheduler(
		gocron.WithLocation(time.UTC),
		gocron.WithStopTimeout(5*time.Second),
	)
	if err != nil {
		// WithLocation(time.UTC) cannot fail with a non-nil location; a failure
		// here is a programmer/version error, so panic with a clear message
		// (mirroring the webhook provider-name panic style).
		panic(fmt.Sprintf("cron: construct gocron scheduler: %v", err))
	}
	return &Scheduler{
		log:     logger,
		pub:     pub,
		g:       g,
		metrics: metricsRegistry,
		now:     time.Now,
		wait:    waitContext,
	}
}

// waitContext blocks for d or until ctx is cancelled, reporting whether the
// delay elapsed. It is the production Scheduler.wait: a single timer per retry
// (no polling), released promptly by lifecycle cancellation.
func waitContext(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// appTag is the tag that identifies every job belonging to an app, so
// RemoveApp can remove a whole app's schedules in one RemoveByTags
// call. ReplaceApp no longer removes by this tag: it reconciles per stable
// schedule name via jobTag so unrelated schedules keep their jobs.
func appTag(name string) string { return "app:" + name }

// jobTag uniquely identifies one schedule of an app by its stable name, so
// RemoveByTags can remove a single schedule without touching another schedule
// that shares its handler.
func jobTag(fnName, scheduleName string) string {
	return "schedule:" + fnName + "/" + scheduleName
}

// ReplaceApp converges the scheduler's jobs for name to the template's
// schedules, keyed by the stable app+schedule name.
//
// Reconcile semantics (each name independent of every other name):
//   - A name declared by both the previous and the new template whose gocron-
//     relevant fields — handler, cron expression, effective timezone — are
//     unchanged keeps its existing job untouched. Timeout and retries are runner
//     configuration, not cron configuration, so changing only those does not
//     replace the job.
//   - A name whose handler/cron/timezone changed, or a newly declared name,
//     removes and re-registers only that name's job.
//   - A name the template no longer declares (removed or renamed) removes only
//     that name's job.
//
// Unrelated schedules of the same app therefore never lose their jobs on a
// single-name edit. RemoveApp is the only whole-app removal. A skipped
// tick during a single job's replacement is acceptable. Templates are validated
// at parse, so a NewJob error here is defensive and merely logged.
func (s *Scheduler) ReplaceApp(name string, tmpl *app.Template) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	// Index the app's recorded schedules by stable name so each desired
	// schedule can be compared against what is already registered, and carry
	// every other app's records through unchanged.
	existing := make(map[string]registeredSchedule)
	next := make([]registeredSchedule, 0, len(s.schedules))
	for _, e := range s.schedules {
		if e.fn == name {
			existing[e.name] = e
			continue
		}
		next = append(next, e)
	}
	for _, sch := range tmpl.Schedules {
		// Copy the loop variable into a local so the closure captures this
		// iteration's schedule, not the loop variable.
		sch := sch
		// `@every` is a relative-delay schedule anchored to each worker's own
		// job start; workers would not agree on an occurrence, so it is rejected
		// here as it is in template validation. gocron would otherwise accept it
		// even with withSeconds=false.
		if trimmed := strings.TrimSpace(sch.Cron); trimmed == "@every" || strings.HasPrefix(trimmed, "@every ") {
			s.log.Warn("Cron: register schedule failed", "app", name, "schedule", sch.Name, "handler", sch.Handler,
				"error", "`@every` relative schedules are not supported")
			continue
		}
		// Parse with gocron's own parser configuration (5-field/descriptor via
		// CRON_TZ-aware ParseStandard) so occurrence derivation matches firing.
		parsed, err := parseSchedule(sch.Cron, sch.Location)
		if err != nil {
			s.log.Warn("Cron: register schedule failed", "app", name, "schedule", sch.Name, "handler", sch.Handler, "error", err)
			continue
		}
		rec := registeredSchedule{fn: name, name: sch.Name, handler: sch.Handler, cron: sch.Cron, location: sch.Location.String(), parsed: parsed}
		// Retain the live job when this name is already registered with the same
		// handler, cron, and timezone. Only those fields reach the gocron job;
		// timeout/retries are resolved by the runner, so a change confined to
		// them must not churn the job (and its next run).
		if prev, ok := existing[sch.Name]; ok && prev.handler == sch.Handler && prev.cron == sch.Cron && prev.location == sch.Location.String() {
			delete(existing, sch.Name)
			next = append(next, rec)
			continue
		}
		// Changed or new: remove only this name's job, then re-register it, so
		// another schedule sharing this app (or even this handler) keeps its
		// job and job ID.
		s.g.RemoveByTags(jobTag(name, sch.Name))
		// Always prepend the zone (UTC included explicitly) so the job runs in
		// the schedule's effective timezone while the scheduler stays pinned to
		// UTC; NextRun returns a UTC instant regardless.
		spec := "CRON_TZ=" + sch.Location.String() + " " + sch.Cron
		// The job name is the stable schedule name (app/name), not a
		// handler+index slot: a schedule's identity survives a handler change,
		// and two schedules sharing a handler remain distinct.
		jobName := name + "/" + sch.Name
		// withSeconds=false pins registration to the 5-field minute form (plus
		// calendar descriptors), exactly matching template validation. Seconds
		// schedules are rejected at parse time because gocron's callback exposes
		// no scheduled-due instant, so a per-second occurrence could not be
		// identified deterministically across workers.
		_, err = s.g.NewJob(
			gocron.CronJob(spec, false),
			gocron.NewTask(func(ctx context.Context) {
				// Admit the callback before it can touch Redis. If shutdown has
				// already closed admission, the scheduler is stopping: do not
				// start a new publish (gocron would not have run it either, but
				// a task already dequeued can still reach here).
				if !s.callbacks.begin() {
					return
				}
				defer s.callbacks.end()
				s.fire(ctx, name, sch.Name, sch.Handler, parsed)
			}),
			gocron.WithTags(appTag(name), jobTag(name, sch.Name)),
			gocron.WithName(jobName),
			gocron.WithSingletonMode(gocron.LimitModeReschedule),
		)
		if err != nil {
			// The old job for this name was already removed and no record is
			// appended, so the name is dropped like any other unregisterable
			// schedule rather than left half-applied.
			s.log.Warn("Cron: register schedule failed", "app", name, "schedule", sch.Name, "handler", sch.Handler, "error", err)
			continue
		}
		delete(existing, sch.Name)
		next = append(next, rec)
	}
	// Remove any name the new template no longer declares (removed or renamed),
	// each by its own tag. A name whose re-registration failed also lands here
	// (it was never deleted from existing), so no stale job survives.
	for staleName := range existing {
		s.g.RemoveByTags(jobTag(name, staleName))
	}
	s.schedules = next
	s.log.Debug("Cron: registered schedules", "app", name, "count", len(tmpl.Schedules))
}

// dropAppSchedules returns schedules with every entry for fn removed,
// preserving the relative order of the rest. It is the record-side twin of
// gocron's RemoveByTags(appTag(fn)).
func dropAppSchedules(schedules []registeredSchedule, fn string) []registeredSchedule {
	kept := schedules[:0]
	for _, e := range schedules {
		if e.fn != fn {
			kept = append(kept, e)
		}
	}
	return kept
}

// RemoveApp removes every schedule job belonging to name, so stale gocron
// jobs never outlive their app.
func (s *Scheduler) RemoveApp(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	s.g.RemoveByTags(appTag(name))
	s.schedules = dropAppSchedules(s.schedules, name)
}

// Start begins firing scheduled jobs. Jobs added before Start fire from their
// first cron tick; jobs added later (via ReplaceApp after Start) schedule
// immediately. It is idempotent-ish: gocron's Start is safe to call once and
// subsequent calls are no-ops while running.
func (s *Scheduler) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	s.g.Start()
	s.log.Info("Cron scheduler started", "count", len(s.g.Jobs()))
}

// JobCount returns the current number of registered schedule jobs. It is used
// for startup logging before Start.
func (s *Scheduler) JobCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.g == nil {
		return 0
	}
	return len(s.g.Jobs())
}

// Stop performs a bounded graceful shutdown of the scheduler and then STRICTLY
// JOINS its Relay publisher callbacks.
//
// gocron cannot be restarted after Shutdown, and Shutdown itself is bounded by
// WithStopTimeout; this additionally honors ctx so a caller's deadline wins even
// if gocron's internal bound misbehaves. Crucially, gocron's executor returns
// (ErrStopJobsTimedOut) while a task goroutine may still be running, and a Relay
// task's Redis-facing publish can therefore outlive g.Shutdown. Stop closes the
// callback admission first, launches the single Shutdown, and returns only once
// BOTH that Shutdown and every admitted callback have returned — so a caller
// (the worker's barrier step) can guarantee no callback touches Redis after Stop
// returns.
//
// It is idempotent and safe for concurrent/repeated calls: the first call starts
// the one Shutdown, and every later call joins the SAME shutdown and callback
// set instead of returning early. Under an expiring ctx it returns ctx.Err()
// while the join continues in the background; a barrier re-invocation (with a
// fresh context) then waits for the real completion. A nil scheduler is a no-op.
func (s *Scheduler) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.stopped {
		s.stopped = true
		g := s.g
		// Close callback admission BEFORE requesting the gocron shutdown: no
		// callback may begin (and touch Redis) once Stop has started, and the
		// watcher below joins those already admitted.
		s.callbacks.close()
		done := make(chan struct{})
		s.shutdownDone = done
		if g == nil {
			close(done)
			s.mu.Unlock()
			return nil
		}
		go func() {
			err := g.Shutdown()
			s.mu.Lock()
			s.shutdownErr = err
			s.mu.Unlock()
			close(done)
		}()
	}
	done := s.shutdownDone
	callbacksDone := s.callbacks.doneCh()
	s.mu.Unlock()

	if err := waitShutdown(ctx, done, callbacksDone); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shutdownErr
}

// waitShutdown waits under ctx for the gocron shutdown and every admitted Relay
// callback to complete. It returns ctx.Err() while either is still outstanding,
// so a bounded caller (the worker's barrier step) can re-invoke Stop for the
// strict join.
func waitShutdown(ctx context.Context, shutdownDone, callbacksDone <-chan struct{}) error {
	select {
	case <-shutdownDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-callbacksDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// fire computes the scheduled instant for this tick and publishes the occurrence
// cluster-wide through the Publisher's bounded retry routine. It is the gocron
// task body (a func(ctx context.Context) so gocron injects and cancels the job
// context on shutdown).
//
// The due instant is derived from the schedule itself, NOT from truncating the
// callback's wall clock: gocron's callback exposes no scheduled-due instant, and
// minute-truncating "now" would stamp the WRONG occurrence if the callback were
// ever delayed across a matching boundary (e.g. a delayed 03:00 tick observed at
// 03:01). latestOccurrence finds the most recent occurrence at or before now, so
// an on-time and a delayed callback for the same tick both stamp the SAME due
// instant (and therefore the same occurrence ID), keeping cluster-wide dedup
// intact. The occurrence is computed once and never recomputed across retries
// (see publishOccurrence). A callback with no occurrence inside the horizon is
// dropped rather than stamped with a fabricated instant; that only happens for a
// tick stale by more than the bounded recovery horizon.
//
// The gocron Job's NextRun is deliberately NOT used: it requires a round trip
// to the scheduler goroutine and returns the FIRST element of a nextScheduled
// slice that the rescheduling/completion messages may not have pruned yet, so
// during a callback it can be the just-fired due (not the next future run) —
// nondeterministic across workers and racy with the scheduler loop.
//
// Retries are bounded (see publishRetryDelays) and observe ctx, so a shutdown
// aborts promptly. The schedule-path at-least-once contract after publication is
// unchanged: the stream layer drives delivery, per-invocation retry, and DLQ.
func (s *Scheduler) fire(ctx context.Context, fnName, scheduleName, handler string, parsed robfigcron.Schedule) {
	now := s.now()
	due, ok := latestOccurrence(parsed, now, occurrenceHorizon)
	if !ok {
		// No occurrence in (now-horizon, now]: this callback is stale by more
		// than the bounded recovery horizon (a callback delayed >24h, e.g. a
		// machine suspended that long), or the schedule's gap exceeds the
		// horizon. Relay never fabricates a non-occurrence: the stale tick is
		// dropped, exactly like a missed occurrence older than the startup
		// catch-up horizon. It cannot happen for a normal on-time callback.
		s.log.Warn("Schedule: tick has no occurrence within the recovery horizon; dropping",
			"app", fnName, "schedule", scheduleName, "handler", handler,
			"now", now.UTC().Format(time.RFC3339),
		)
		return
	}
	o := schedule.Occurrence{App: fnName, Schedule: scheduleName, Handler: handler, ScheduledAt: due}
	_, _ = s.publishOccurrence(ctx, o, false)
}

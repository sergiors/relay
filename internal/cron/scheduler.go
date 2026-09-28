package cron

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"
	robfigcron "github.com/robfig/cron/v3"

	"relay/internal/function"
	"relay/internal/observability/metrics"
	"relay/internal/schedule"
)

// Publisher publishes one schedule occurrence cluster-wide. *schedule.
// SchedulePublisher satisfies it; the cron scheduler never touches Redis or
// execution internals.
type Publisher interface {
	PublishOccurrence(ctx context.Context, o schedule.Occurrence) (published bool, err error)
}

// registeredSchedule is one function schedule Relay has registered: the
// function and handler it invokes plus the schedule parsed with the SAME
// parser gocron uses, so occurrence derivation (both the callback path and the
// startup catch-up) matches gocron's firing exactly. Entries are recorded by
// ReplaceFunction (mutex-protected) and read by CatchUp; they never drive the
// live gocron jobs (those remain gocron's responsibility).
type registeredSchedule struct {
	fn      string
	handler string
	parsed  robfigcron.Schedule
}

// Scheduler wraps a gocron scheduler to run a function's template schedules.
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
	// schedules records the parsed schedules registered so far, used only by
	// the once-per-Scheduler startup CatchUp. It is replaced per function by
	// ReplaceFunction so it converges alongside the gocron jobs.
	schedules []registeredSchedule
	// catchUpDone guards the once-only startup catch-up: live ReplaceFunction
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

// functionTag is the tag that identifies every job belonging to a function, so
// ReplaceFunction and RemoveFunction can converge/remove a whole function's
// schedules in one RemoveByTags call.
func functionTag(name string) string { return "function:" + name }

// jobTag uniquely identifies one schedule slot of a function. The index suffix
// distinguishes duplicate handler entries in the template.
func jobTag(fnName, handler string, i int) string {
	return "schedule:" + fnName + "/" + handler + "#" + strconv.Itoa(i)
}

// ReplaceFunction converges the scheduler's jobs for name to the template's
// schedules. It is the single reconcile entry point, called when a function is
// discovered, updated, or hot-swapped. A skipped tick during the swap is
// acceptable (a schedule is recreated atomically enough). Templates are
// validated at parse, so a NewJob error here is defensive and merely logged.
func (s *Scheduler) ReplaceFunction(name string, tmpl *function.Template) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	s.g.RemoveByTags(functionTag(name))
	// Converge the parsed-schedule record for this function alongside the
	// gocron jobs, so the startup catch-up sees exactly the schedules registered
	// before Start. Entries are dropped for skipped/invalid schedules below.
	s.schedules = dropFunctionSchedules(s.schedules, name)
	for i, sch := range tmpl.Schedules {
		// Copy the loop variable into a local so the closure captures this
		// iteration's schedule, not the loop variable.
		sch := sch
		// `@every` is a relative-delay schedule anchored to each worker's own
		// job start; workers would not agree on an occurrence, so it is rejected
		// here as it is in template validation. gocron would otherwise accept it
		// even with withSeconds=false.
		if trimmed := strings.TrimSpace(sch.Cron); trimmed == "@every" || strings.HasPrefix(trimmed, "@every ") {
			s.log.Warn("Cron: register schedule failed", "function", name, "handler", sch.Handler,
				"error", "`@every` relative schedules are not supported")
			continue
		}
		// Parse with gocron's own parser configuration (5-field/descriptor via
		// CRON_TZ-aware ParseStandard) so occurrence derivation matches firing.
		parsed, err := parseSchedule(sch.Cron, sch.Location)
		if err != nil {
			s.log.Warn("Cron: register schedule failed", "function", name, "handler", sch.Handler, "error", err)
			continue
		}
		// Always prepend the zone (UTC included explicitly) so the job runs in
		// the schedule's effective timezone while the scheduler stays pinned to
		// UTC; NextRun returns a UTC instant regardless.
		spec := "CRON_TZ=" + sch.Location.String() + " " + sch.Cron
		jobName := name + "/" + sch.Handler + "#" + strconv.Itoa(i)
		// withSeconds=false pins registration to the 5-field minute form (plus
		// calendar descriptors), exactly matching template validation. Seconds
		// schedules are rejected at parse time because gocron's callback exposes
		// no scheduled-due instant, so a per-second occurrence could not be
		// identified deterministically across workers.
		_, err = s.g.NewJob(
			gocron.CronJob(spec, false),
			gocron.NewTask(func(ctx context.Context) { s.fire(ctx, name, sch.Handler, parsed) }),
			gocron.WithTags(functionTag(name), jobTag(name, sch.Handler, i)),
			gocron.WithName(jobName),
			gocron.WithSingletonMode(gocron.LimitModeReschedule),
		)
		if err != nil {
			s.log.Warn("Cron: register schedule failed", "function", name, "handler", sch.Handler, "error", err)
			continue
		}
		s.schedules = append(s.schedules, registeredSchedule{fn: name, handler: sch.Handler, parsed: parsed})
	}
	s.log.Debug("Cron: registered schedules", "function", name, "count", len(tmpl.Schedules))
}

// dropFunctionSchedules returns schedules with every entry for fn removed,
// preserving the relative order of the rest. It is the record-side twin of
// gocron's RemoveByTags(functionTag(fn)).
func dropFunctionSchedules(schedules []registeredSchedule, fn string) []registeredSchedule {
	kept := schedules[:0]
	for _, e := range schedules {
		if e.fn != fn {
			kept = append(kept, e)
		}
	}
	return kept
}

// RemoveFunction removes every schedule job belonging to name, so stale gocron
// jobs never outlive their function.
func (s *Scheduler) RemoveFunction(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	s.g.RemoveByTags(functionTag(name))
	s.schedules = dropFunctionSchedules(s.schedules, name)
}

// Start begins firing scheduled jobs. Jobs added before Start fire from their
// first cron tick; jobs added later (via ReplaceFunction after Start) schedule
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

// Stop performs a bounded graceful shutdown of the scheduler. gocron cannot be
// restarted after Shutdown, and Shutdown itself is already bounded by
// WithStopTimeout; this additionally honors ctx so a caller's deadline wins
// even if gocron's internal bound misbehaves. It is idempotent and nil-safe: a
// second (or concurrent) call is a no-op, and a call after a prior Stop returns
// nil immediately.
func (s *Scheduler) Stop(ctx context.Context) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	g := s.g
	s.mu.Unlock()
	if g == nil {
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- g.Shutdown() }()
	select {
	case err := <-done:
		return err
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
func (s *Scheduler) fire(ctx context.Context, fnName, handler string, parsed robfigcron.Schedule) {
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
			"function", fnName, "handler", handler,
			"now", now.UTC().Format(time.RFC3339),
		)
		return
	}
	o := schedule.Occurrence{Function: fnName, Handler: handler, ScheduledAt: due}
	_, _ = s.publishOccurrence(ctx, o, false)
}

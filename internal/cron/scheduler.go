package cron

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"

	"relay/internal/function"
	"relay/internal/schedule"
)

// Publisher publishes one schedule occurrence cluster-wide. *schedule.
// SchedulePublisher satisfies it; the cron scheduler never touches Redis or
// execution internals.
type Publisher interface {
	PublishOccurrence(ctx context.Context, o schedule.Occurrence) (published bool, err error)
}

// Scheduler wraps a gocron scheduler to run a function's template schedules.
type Scheduler struct {
	log     *slog.Logger
	pub     Publisher
	g       gocron.Scheduler
	mu      sync.Mutex
	stopped bool
}

// New constructs a Scheduler over the given publisher. The gocron scheduler is
// pinned to UTC (per-job timezones are encoded as CRON_TZ prefixes) and its
// shutdown is bounded by WithStopTimeout. A nil logger falls back to a
// discarding slog logger like the runner does.
func New(pub Publisher, logger *slog.Logger) *Scheduler {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
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
	return &Scheduler{log: logger, pub: pub, g: g}
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
	for i, sch := range tmpl.Schedules {
		// Copy the loop variable into a local so the closure captures this
		// iteration's schedule, not the loop variable.
		sch := sch
		// Always prepend the zone (UTC included explicitly) so the job runs in
		// the schedule's effective timezone while the scheduler stays pinned to
		// UTC; NextRun returns a UTC instant regardless.
		spec := "CRON_TZ=" + sch.Location.String() + " " + sch.Cron
		jobName := name + "/" + sch.Handler + "#" + strconv.Itoa(i)
		// withSeconds=true so runtime execution accepts the same 5-field and
		// 6-field (seconds) expressions that template validation permits,
		// keeping the two sides exactly consistent.
		_, err := s.g.NewJob(
			gocron.CronJob(spec, true),
			gocron.NewTask(func(ctx context.Context) { s.fire(ctx, name, sch.Handler) }),
			gocron.WithTags(functionTag(name), jobTag(name, sch.Handler, i)),
			gocron.WithName(jobName),
			gocron.WithSingletonMode(gocron.LimitModeReschedule),
		)
		if err != nil {
			s.log.Warn("Cron: register schedule failed", "function", name, "handler", sch.Handler, "error", err)
			continue
		}
	}
	s.log.Debug("Cron: registered schedules", "function", name, "count", len(tmpl.Schedules))
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
// cluster-wide through the Publisher. It is the gocron task body (a
// func(ctx context.Context) so gocron injects and cancels the job context on
// shutdown). There is no local retry: after publication, the stream layer drives
// at-least-once delivery and per-invocation retry/backoff/DLQ; if publication
// itself fails, the tick is lost for THIS worker but other workers' callbacks
// still publish the same occurrence (per-worker evaluation makes publication
// best-effort across the fleet).
func (s *Scheduler) fire(ctx context.Context, fnName, handler string) {
	// The scheduled instant is minute-truncated UTC. 5-field cron granularity is
	// minutes, so every worker evaluating the same tick arrives at the same
	// minute-level instant; minute-truncation is robust to second-boundary
	// jitter between workers and is what keeps their occurrence IDs identical.
	// (The gocron Job's NextRun() is not used: under RunNow it returns the
	// FUTURE cron instant, not the due one, so it is not deterministic here.)
	// The payload's scheduled_at is thus the honest due instant to the minute.
	due := time.Now().UTC().Truncate(time.Minute)
	o := schedule.Occurrence{Function: fnName, Handler: handler, ScheduledAt: due}
	published, err := s.pub.PublishOccurrence(ctx, o)
	if err != nil {
		s.log.Warn("Schedule: publish failed", "function", fnName, "handler", handler, "scheduled_at", due.Format(time.RFC3339), "occurrence_id", o.ID(), "reason", err)
		return
	}
	if published {
		s.log.Info("Schedule: occurrence published", "function", fnName, "handler", handler, "scheduled_at", due.Format(time.RFC3339), "occurrence_id", o.ID())
		return
	}
	// Another worker already published this occurrence (clean duplicate no-op).
	s.log.Debug("Schedule: occurrence already published", "function", fnName, "handler", handler, "scheduled_at", due.Format(time.RFC3339), "occurrence_id", o.ID())
}

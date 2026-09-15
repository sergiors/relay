package cron

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/schedule"
)

// errBoom is a sentinel publish error used to exercise the non-fatal error path.
var errBoom = errors.New("boom")

// fakePublisher records the (function, handler, due instant) of each published
// occurrence along with a configurable result. When block is non-nil, a publish
// blocks until the job's ctx is cancelled OR block is closed, and then observes
// whether the ctx was cancelled.
type fakePublisher struct {
	mu        sync.Mutex
	calls     []schedule.Occurrence
	published bool
	err       error
	fired     chan struct{} // signals a completed publish
	block     chan struct{}
	cancel    chan struct{} // closed when a publish observed ctx cancellation
	cancels   int
}

func newFakePublisher(n int) *fakePublisher {
	return &fakePublisher{published: true, fired: make(chan struct{}, n), cancel: make(chan struct{}, n)}
}

func (f *fakePublisher) PublishOccurrence(ctx context.Context, o schedule.Occurrence) (bool, error) {
	if f.block != nil {
		select {
		case <-ctx.Done():
			f.mu.Lock()
			f.cancels++
			f.mu.Unlock()
			f.cancel <- struct{}{}
			return false, ctx.Err()
		case <-f.block:
		}
	}
	f.mu.Lock()
	f.calls = append(f.calls, o)
	pub, err := f.published, f.err
	f.mu.Unlock()
	f.fired <- struct{}{}
	return pub, err
}

func (f *fakePublisher) got() []schedule.Occurrence {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]schedule.Occurrence(nil), f.calls...)
}

func (f *fakePublisher) waitFired(n int) bool {
	for i := 0; i < n; i++ {
		select {
		case <-f.fired:
		case <-time.After(3 * time.Second):
			return false
		}
	}
	return true
}

func (f *fakePublisher) waitCancel(n int) bool {
	for i := 0; i < n; i++ {
		select {
		case <-f.cancel:
		case <-time.After(3 * time.Second):
			return false
		}
	}
	return true
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func schedTemplate(handler, cron, timezone, timeout string) *function.Template {
	t := &function.Template{Runtime: "node24"}
	sch := function.Schedule{Handler: handler, Cron: cron, Location: time.UTC, Timeout: function.DefaultTimeout}
	if timezone != "" {
		loc, err := time.LoadLocation(timezone)
		if err != nil {
			panic(err)
		}
		sch.Location = loc
	}
	if timeout != "" {
		d, err := time.ParseDuration(timeout)
		if err != nil {
			panic(err)
		}
		sch.Timeout = d
	}
	t.Schedules = []function.Schedule{sch}
	return t
}

func twoSchedules() *function.Template {
	return &function.Template{Runtime: "node24", Schedules: []function.Schedule{
		{Handler: "jobs.a", Cron: "0 3 * * *", Location: time.UTC, Timeout: function.DefaultTimeout},
		{Handler: "jobs.b", Cron: "0 4 * * *", Location: time.UTC, Timeout: 20 * time.Second},
	}}
}

// fireNow runs the job with the given name once (RunNow), so a test can trigger
// a specific schedule's tick deterministically regardless of the cron.
func fireNow(t *testing.T, s *Scheduler, name string) {
	t.Helper()
	for _, j := range s.g.Jobs() {
		if j.Name() == name {
			if err := j.RunNow(); err != nil {
				t.Fatalf("RunNow %q: %v", name, err)
			}
			return
		}
	}
	t.Fatalf("no job named %q in %d job(s)", name, len(s.g.Jobs()))
}

// ReplaceFunction with two schedules registers two jobs; a template without
// schedules registers none.
func TestReplaceFunctionRegistersJobs(t *testing.T) {
	fp := newFakePublisher(2)
	s := New(fp, testLogger())
	defer func() { _ = s.Stop(context.Background()) }()

	s.ReplaceFunction("fn", twoSchedules())
	if n := s.JobCount(); n != 2 {
		t.Fatalf("jobs = %d, want 2", n)
	}

	// No schedules -> replaces with zero jobs, leaving the prior ones removed.
	s.ReplaceFunction("fn", &function.Template{Runtime: "node24"})
	if n := s.JobCount(); n != 0 {
		t.Fatalf("jobs = %d, want 0 (empty template replaces)", n)
	}
}

// Firing a registered job publishes a schedule occurrence for the function and
// handler with a scheduled instant within a minute-truncation window.
func TestFireSendsPayload(t *testing.T) {
	fp := newFakePublisher(1)
	s := New(fp, testLogger())
	defer func() { _ = s.Stop(context.Background()) }()
	s.ReplaceFunction("fn", schedTemplate("jobs.a", "0 3 * * *", "", ""))
	s.Start()

	before := time.Now()
	fireNow(t, s, "fn/jobs.a#0")
	if !fp.waitFired(1) {
		t.Fatal("occurrence not published")
	}

	calls := fp.got()
	if len(calls) != 1 {
		t.Fatalf("publishes = %d, want 1", len(calls))
	}
	o := calls[0]
	if o.Function != "fn" {
		t.Fatalf("function = %q, want fn", o.Function)
	}
	if o.Handler != "jobs.a" {
		t.Fatalf("handler = %q, want jobs.a", o.Handler)
	}
	// The due instant is minute-truncated UTC (cron granularity): it must be
	// within the minute-truncated window around the fire time.
	wantMinute := before.UTC().Truncate(time.Minute)
	if !o.ScheduledAt.Equal(wantMinute) && !o.ScheduledAt.Equal(time.Now().UTC().Truncate(time.Minute)) {
		t.Fatalf("scheduled_at %v not minute-truncated near now", o.ScheduledAt)
	}
}

// Two schedules fire independently with their own handlers.
func TestMultipleSchedulesFireIndependently(t *testing.T) {
	fp := newFakePublisher(2)
	s := New(fp, testLogger())
	defer func() { _ = s.Stop(context.Background()) }()
	s.ReplaceFunction("fn", twoSchedules())
	s.Start()

	fireNow(t, s, "fn/jobs.a#0")
	fireNow(t, s, "fn/jobs.b#1")
	if !fp.waitFired(2) {
		t.Fatal("two occurrences not published")
	}

	handlers := map[string]bool{}
	for _, c := range fp.got() {
		handlers[c.Handler] = true
	}
	if !handlers["jobs.a"] || !handlers["jobs.b"] {
		t.Fatalf("handlers = %v, want jobs.a and jobs.b", handlers)
	}
}

// A publish error is logged, not fatal: the scheduler keeps running and the
// tick is simply lost for this worker (other workers still publish it).
func TestPublishErrorLoggedNotFatal(t *testing.T) {
	fp := newFakePublisher(1)
	fp.err = errBoom
	s := New(fp, testLogger())
	defer func() { _ = s.Stop(context.Background()) }()
	s.ReplaceFunction("fn", schedTemplate("jobs.a", "0 3 * * *", "", ""))
	s.Start()

	fireNow(t, s, "fn/jobs.a#0")
	// The publisher is called (once) even though it errors; the fire path must
	// not panic or otherwise fail the scheduler.
	if !fp.waitFired(1) {
		t.Fatal("occurrence not attempted")
	}
	if n := s.JobCount(); n != 1 {
		t.Fatalf("jobs after publish error = %d, want 1 (scheduler keeps running)", n)
	}
	s.Stop(context.Background())

	// A duplicate publication is a clean no-op: only one logical occurrence.
	fp2 := newFakePublisher(1)
	fp2.published = false
	s2 := New(fp2, testLogger())
	defer func() { _ = s2.Stop(context.Background()) }()
	s2.ReplaceFunction("fn", schedTemplate("jobs.a", "0 3 * * *", "", ""))
	s2.Start()
	fireNow(t, s2, "fn/jobs.a#0")
	if !fp2.waitFired(1) {
		t.Fatal("occurrence not attempted")
	}
}

// Reconciliation: adding schedules grows the job set; changing a cron changes
// the next run; removing one schedule drops its job; RemoveFunction drops all.
func TestReplaceFunctionConvergesJobs(t *testing.T) {
	fp := newFakePublisher(4)
	s := New(fp, testLogger())
	defer func() { _ = s.Stop(context.Background()) }()
	s.Start()

	// Add: one job.
	s.ReplaceFunction("fn", schedTemplate("jobs.a", "0 3 * * *", "", ""))
	if n := s.JobCount(); n != 1 {
		t.Fatalf("jobs after add = %d, want 1", n)
	}

	// Change cron: still one job, next run reflects the new hour.
	s.ReplaceFunction("fn", schedTemplate("jobs.a", "0 4 * * *", "", ""))
	if n := s.JobCount(); n != 1 {
		t.Fatalf("jobs after cron change = %d, want 1", n)
	}
	next, err := s.g.Jobs()[0].NextRun()
	if err != nil {
		t.Fatalf("NextRun: %v", err)
	}
	if next.Hour() != 4 {
		t.Fatalf("next run hour = %d, want 4", next.Hour())
	}

	// Two schedules: two jobs.
	s.ReplaceFunction("fn", twoSchedules())
	if n := s.JobCount(); n != 2 {
		t.Fatalf("jobs after add second = %d, want 2", n)
	}

	// Remove one schedule: one job remains.
	s.ReplaceFunction("fn", schedTemplate("jobs.a", "0 4 * * *", "", ""))
	if n := s.JobCount(); n != 1 {
		t.Fatalf("jobs after remove schedule = %d, want 1", n)
	}

	// RemoveFunction drops all.
	s.RemoveFunction("fn")
	if n := s.JobCount(); n != 0 {
		t.Fatalf("jobs after RemoveFunction = %d, want 0", n)
	}
}

// Changing only the timezone keeps the cron minute but shifts the UTC instant:
// with Europe/Rome (UTC+1 winter / UTC+2 CEST), 08:00 local never equals 08:00Z.
func TestTimezoneChangeShiftsUTCInstant(t *testing.T) {
	fp := newFakePublisher(2)
	s := New(fp, testLogger())
	defer func() { _ = s.Stop(context.Background()) }()
	s.Start()

	s.ReplaceFunction("fn", schedTemplate("jobs.a", "0 8 * * *", "", "")) // UTC
	n1, _ := s.g.Jobs()[0].NextRun()

	s.ReplaceFunction("fn", schedTemplate("jobs.a", "0 8 * * *", "Europe/Rome", ""))
	n2, _ := s.g.Jobs()[0].NextRun()

	if n1.Equal(n2) {
		t.Fatalf("next runs identical for different timezones: %v", n1)
	}
	// In Rome, the second schedule's instant is 08:00 local.
	rome, _ := time.LoadLocation("Europe/Rome")
	local := n2.In(rome)
	if local.Hour() != 8 || local.Minute() != 0 {
		t.Fatalf("Europe/Rome next run local = %v, want 08:00", local)
	}
}

// The Europe/Rome 08:00 schedule's next run is 08:00 local, and DST is handled
// by time.Location: over ~400 days both standard and DST offsets appear.
func TestNextRunTimezoneAndDST(t *testing.T) {
	fp := newFakePublisher(1)
	s := New(fp, testLogger())
	defer func() { _ = s.Stop(context.Background()) }()
	s.ReplaceFunction("fn", schedTemplate("jobs.a", "0 8 * * *", "Europe/Rome", ""))
	s.Start()

	nr, err := s.g.Jobs()[0].NextRuns(1)
	if err != nil {
		t.Fatalf("NextRuns: %v", err)
	}
	rome, _ := time.LoadLocation("Europe/Rome")
	if hour, min := nr[0].In(rome).Hour(), nr[0].In(rome).Minute(); hour != 8 || min != 0 {
		t.Fatalf("first next run in Rome = %02d:%02d, want 08:00", hour, min)
	}

	// Over ~400 days the instant must always be 08:00 local in Rome AND span at
	// least two distinct UTC offsets (DST is delegated to time.Location).
	runs, err := s.g.Jobs()[0].NextRuns(400)
	if err != nil {
		t.Fatalf("NextRuns(400): %v", err)
	}
	offsets := map[int]bool{}
	for _, r := range runs {
		l := r.In(rome)
		if l.Hour() != 8 || l.Minute() != 0 {
			t.Fatalf("a run landed at %02d:%02d local in Rome, want 08:00", l.Hour(), l.Minute())
		}
		_, off := l.Zone()
		offsets[off] = true
	}
	if len(offsets) < 2 {
		t.Fatalf("expected at least two UTC offsets (DST) across 400 days, got %v", offsets)
	}
}

// Stop cancels an in-flight publish's context (the publisher blocks until its
// ctx is cancelled) and returns within the bound.
func TestStopCancelsInFlightPublish(t *testing.T) {
	fp := newFakePublisher(1)
	fp.block = make(chan struct{})
	s := New(fp, testLogger())
	s.ReplaceFunction("fn", schedTemplate("jobs.a", "0 3 * * *", "", ""))
	s.Start()

	fireNow(t, s, "fn/jobs.a#0")
	// Wait for the publish to begin blocking (it must not complete on its own,
	// and the fired channel stays empty).
	select {
	case <-fp.fired:
		t.Fatal("publish should not have completed before Stop")
	case <-time.After(500 * time.Millisecond):
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	// The in-flight publish observed the job context being cancelled.
	if !fp.waitCancel(1) {
		t.Fatal("in-flight publish did not observe cancellation")
	}
}

// Stop is idempotent; ReplaceFunction/RemoveFunction/Start after Stop are safe
// no-ops (no panic, no new jobs added).
func TestStopIdempotentAndNoOpsAfterStop(t *testing.T) {
	fp := newFakePublisher(1)
	s := New(fp, testLogger())
	s.ReplaceFunction("fn", schedTemplate("jobs.a", "0 3 * * *", "", ""))
	s.Start()
	if n := s.JobCount(); n != 1 {
		t.Fatalf("jobs before Stop = %d, want 1", n)
	}

	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("first Stop: %v", err)
	}
	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
	// ReplaceFunction, RemoveFunction, and Start after Stop must not add jobs
	// (they are no-ops) and never panic. gocron's Shutdown clears the job list,
	// so the count stays at 0.
	s.ReplaceFunction("fn", schedTemplate("jobs.b", "0 4 * * *", "", ""))
	s.RemoveFunction("fn")
	s.Start()
	if got := s.JobCount(); got != 0 {
		t.Fatalf("jobs after Stop = %d, want 0 (no new jobs after stop)", got)
	}
}

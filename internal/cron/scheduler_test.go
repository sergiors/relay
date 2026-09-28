package cron

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	oteltrace "go.opentelemetry.io/otel/trace"

	"relay/internal/function"
	"relay/internal/observability/metrics"
	"relay/internal/schedule"
)

// errBoom is a sentinel publish error used to exercise the non-fatal error path.
var errBoom = errors.New("boom")

// pubResult is one scripted PublishOccurrence outcome.
type pubResult struct {
	published bool
	err       error
}

// fakePublisher records the (function, handler, due instant) of each published
// occurrence along with a scripted result. Results are consumed in order from
// script; once exhausted the last entry repeats. With an empty script the
// default published/err fields are used (the original behavior). When block is
// non-nil, a publish blocks until the job's ctx is cancelled OR block is closed,
// and then observes whether the ctx was cancelled. Every publish also signals
// fired and observes ctx cancellation while signaling, so a retry loop is
// released by cancellation rather than leaking.
type fakePublisher struct {
	mu        sync.Mutex
	calls     []schedule.Occurrence
	ids       map[string]int
	script    []pubResult
	next      int
	published bool
	err       error
	fired     chan struct{} // signals a completed publish
	block     chan struct{}
	cancel    chan struct{} // closed when a publish observed ctx cancellation
	cancels   int
	ctxSpans  []oteltrace.SpanContext // span context observed on each publish call
}

func newFakePublisher(n int) *fakePublisher {
	return &fakePublisher{published: true, fired: make(chan struct{}, n), cancel: make(chan struct{}, n), ids: map[string]int{}}
}

func (f *fakePublisher) PublishOccurrence(ctx context.Context, o schedule.Occurrence) (bool, error) {
	if f.block != nil {
		select {
		case <-ctx.Done():
			f.observeCancel()
			return false, ctx.Err()
		case <-f.block:
		}
	}
	f.mu.Lock()
	pub, err := f.published, f.err
	if len(f.script) > 0 {
		s := f.script[min(f.next, len(f.script)-1)]
		f.next++
		pub, err = s.published, s.err
	}
	f.calls = append(f.calls, o)
	if f.ids == nil {
		f.ids = map[string]int{}
	}
	f.ids[o.ID()]++
	f.ctxSpans = append(f.ctxSpans, oteltrace.SpanContextFromContext(ctx))
	f.mu.Unlock()
	select {
	case <-ctx.Done():
		f.observeCancel()
		return false, ctx.Err()
	case f.fired <- struct{}{}:
	}
	return pub, err
}

func (f *fakePublisher) observeCancel() {
	f.mu.Lock()
	f.cancels++
	f.mu.Unlock()
	select {
	case f.cancel <- struct{}{}:
	default:
	}
}

func (f *fakePublisher) got() []schedule.Occurrence {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]schedule.Occurrence(nil), f.calls...)
}

// callCount returns how many publish attempts were recorded.
func (f *fakePublisher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// callsFor returns how many attempts recorded exactly the given occurrence ID.
func (f *fakePublisher) callsFor(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ids[id]
}

// spans returns the span context observed on each publish call.
func (f *fakePublisher) spans() []oteltrace.SpanContext {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]oteltrace.SpanContext(nil), f.ctxSpans...)
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

// A malformed cron expression in one schedule logs a Warn and is skipped, while
// the valid schedules still register. (Templates are validated at parse time, so
// this is the defensive path.)
func TestReplaceFunctionSkipsUnregisterableSchedule(t *testing.T) {
	fp := newFakePublisher(1)
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	s := New(fp, logger)
	defer func() { _ = s.Stop(context.Background()) }()

	s.ReplaceFunction("fn", &function.Template{Runtime: "node24", Schedules: []function.Schedule{
		{Handler: "jobs.good", Cron: "0 3 * * *", Location: time.UTC, Timeout: function.DefaultTimeout},
		{Handler: "jobs.bad", Cron: "not a cron", Location: time.UTC, Timeout: function.DefaultTimeout},
	}})

	if n := s.JobCount(); n != 1 {
		t.Fatalf("jobs = %d, want 1 (invalid schedule skipped, valid one registered)", n)
	}
	names := map[string]bool{}
	for _, j := range s.g.Jobs() {
		names[j.Name()] = true
	}
	if !names["fn/jobs.good#0"] || names["fn/jobs.bad#1"] {
		t.Fatalf("registered jobs = %v, want only fn/jobs.good#0", names)
	}
	if !strings.Contains(logBuf.String(), "register schedule failed") {
		t.Fatalf("expected a Warn about the unregisterable schedule:\n%s", logBuf.String())
	}
}

// A hand-built template carrying a 6-field (seconds) schedule cannot register:
// template validation rejects seconds, so registration is pinned to the 5-field
// form and this defensive path skips the bad entry while keeping valid ones.
func TestReplaceFunctionSkipsSixFieldSchedule(t *testing.T) {
	fp := newFakePublisher(1)
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	s := New(fp, logger)
	defer func() { _ = s.Stop(context.Background()) }()

	s.ReplaceFunction("fn", &function.Template{Runtime: "node24", Schedules: []function.Schedule{
		{Handler: "jobs.five", Cron: "0 0 * * *", Location: time.UTC, Timeout: function.DefaultTimeout},
		{Handler: "jobs.six", Cron: "30 0 0 * * *", Location: time.UTC, Timeout: function.DefaultTimeout},
	}})
	if n := s.JobCount(); n != 1 {
		t.Fatalf("jobs = %d, want 1 (seconds schedule skipped, five-field kept)", n)
	}
	names := map[string]bool{}
	for _, j := range s.g.Jobs() {
		names[j.Name()] = true
	}
	if !names["fn/jobs.five#0"] || names["fn/jobs.six#1"] {
		t.Fatalf("registered jobs = %v, want only fn/jobs.five#0", names)
	}
	if !strings.Contains(logBuf.String(), "register schedule failed") {
		t.Fatalf("expected a Warn about the unregisterable six-field schedule:\n%s", logBuf.String())
	}
}

// A hand-built template carrying an `@every` relative schedule is likewise
// skipped defensively at registration (validation rejects it).
func TestReplaceFunctionSkipsEverySchedule(t *testing.T) {
	fp := newFakePublisher(1)
	s := New(fp, testLogger())
	defer func() { _ = s.Stop(context.Background()) }()

	s.ReplaceFunction("fn", &function.Template{Runtime: "node24", Schedules: []function.Schedule{
		{Handler: "jobs.every", Cron: "@every 30s", Location: time.UTC, Timeout: function.DefaultTimeout},
	}})
	if n := s.JobCount(); n != 0 {
		t.Fatalf("jobs = %d, want 0 (@every must not register)", n)
	}
}

// Firing a registered job publishes a schedule occurrence for the function and
// handler stamped with the schedule's most recent occurrence at or before the
// callback's clock (not the raw wall clock).
func TestFireSendsPayload(t *testing.T) {
	fp := newFakePublisher(1)
	s := New(fp, testLogger())
	defer func() { _ = s.Stop(context.Background()) }()
	// Pin the clock just past the 08:00 matching boundary so the expected due
	// instant is unambiguous regardless of when the test actually runs.
	frozen := time.Date(2026, 7, 1, 8, 0, 42, 123456789, time.UTC)
	s.now = func() time.Time { return frozen }
	s.ReplaceFunction("fn", schedTemplate("jobs.a", "0 8 * * *", "", ""))
	s.Start()

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
	// The due instant is the schedule's occurrence at or before now (the 08:00
	// boundary), NOT the callback's 08:00:42 wall clock.
	wantDue := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	if !o.ScheduledAt.Equal(wantDue) {
		t.Fatalf("scheduled_at = %v, want %v", o.ScheduledAt, wantDue)
	}
	// The occurrence identity carries that occurrence, never the callback jitter.
	wantID := "schedule:fn:jobs.a:" + wantDue.Format(time.RFC3339)
	if o.ID() != wantID {
		t.Fatalf("occurrence ID = %q, want %q", o.ID(), wantID)
	}
}

// dedupPublisher emulates the cluster-wide publish-if-new contract in-memory:
// the first occurrence ID wins, every later publish of that ID is a clean
// duplicate. It is shared by two Scheduler instances to model two Relay
// workers racing the same tick.
type dedupPublisher struct {
	mu   sync.Mutex
	seen map[string]bool
	wins int
	dups int
}

func newDedupPublisher() *dedupPublisher { return &dedupPublisher{seen: map[string]bool{}} }

func (d *dedupPublisher) PublishOccurrence(_ context.Context, o schedule.Occurrence) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.seen[o.ID()] {
		d.dups++
		return false, nil
	}
	d.seen[o.ID()] = true
	d.wins++
	return true, nil
}

// Two workers evaluating the same tick derive the same occurrence and contend
// on exactly one publish; the loser is a clean duplicate no-op. Both callbacks
// are triggered before either is awaited, so the publish-if-new contention is
// exercised concurrently under -race.
func TestTwoWorkersDedupSameTick(t *testing.T) {
	pub := newDedupPublisher()
	frozen := time.Date(2026, 7, 1, 8, 0, 30, 0, time.UTC)
	var workers []*Scheduler
	for i := 0; i < 2; i++ {
		s := New(pub, testLogger())
		s.now = func() time.Time { return frozen }
		s.ReplaceFunction("fn", schedTemplate("jobs.a", "0 3 * * *", "", ""))
		s.Start()
		workers = append(workers, s)
	}
	// Fire both workers' jobs for the same logical tick before awaiting either,
	// so they genuinely race on the shared dedup set.
	for _, s := range workers {
		fireNow(t, s, "fn/jobs.a#0")
	}
	deadline := time.Now().Add(3 * time.Second)
	for pub.total() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	for i, s := range workers {
		if err := s.Stop(context.Background()); err != nil {
			t.Fatalf("worker %d Stop: %v", i, err)
		}
	}
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if pub.wins != 1 || pub.dups != 1 {
		t.Fatalf("publish outcomes = wins %d, dups %d; want exactly 1 win and 1 duplicate", pub.wins, pub.dups)
	}
}

// total reports how many publish attempts (wins + duplicates) have completed.
func (d *dedupPublisher) total() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.wins + d.dups
}

// Callback jitter must not distort occurrence identity: two schedulers firing
// the "same" tick at different sub-minute wall-clock instants (e.g. worker-side
// scheduling latency) derive the same schedule occurrence and therefore the same
// occurrence ID.
func TestFireMinuteTruncationAbsorbsJitter(t *testing.T) {
	base := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	pins := []time.Time{
		base.Add(50 * time.Millisecond),                 // worker A fires a hair late
		base.Add(59*time.Second + 999*time.Millisecond), // worker B fires near the minute edge
	}
	var ids []string
	for i, pin := range pins {
		fp := newFakePublisher(1)
		s := New(fp, testLogger())
		s.now = func() time.Time { return pin }
		s.ReplaceFunction("fn", schedTemplate("jobs.a", "* * * * *", "", ""))
		s.Start()
		fireNow(t, s, "fn/jobs.a#0")
		if !fp.waitFired(1) {
			t.Fatalf("worker %d: occurrence not published", i)
		}
		ids = append(ids, fp.got()[0].ID())
		if err := s.Stop(context.Background()); err != nil {
			t.Fatalf("worker %d: Stop: %v", i, err)
		}
	}
	if ids[0] != ids[1] {
		t.Fatalf("jitter split one occurrence into two IDs: %q vs %q", ids[0], ids[1])
	}
}

// A five-field schedule's next run is exactly once per minute at second 0: the
// next two runs are a minute apart and both land on second 0.
func TestFiveFieldNextRunsOncePerMinute(t *testing.T) {
	fp := newFakePublisher(1)
	s := New(fp, testLogger())
	defer func() { _ = s.Stop(context.Background()) }()
	s.ReplaceFunction("fn", schedTemplate("jobs.a", "* * * * *", "", ""))
	s.Start()

	runs, err := s.g.Jobs()[0].NextRuns(2)
	if err != nil {
		t.Fatalf("NextRuns: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("runs = %d, want 2", len(runs))
	}
	if runs[0].Second() != 0 || runs[1].Second() != 0 {
		t.Fatalf("runs = %v, %v, want both at second 0", runs[0], runs[1])
	}
	if d := runs[1].Sub(runs[0]); d != time.Minute {
		t.Fatalf("gap between runs = %s, want exactly 1m", d)
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

// A publish failure is retried within the bounded budget and never fatal: the
// scheduler keeps running, and an exhausted budget is logged without failing the
// scheduler. A duplicate publication is a clean terminal no-op (no retry).
func TestPublishErrorLoggedNotFatal(t *testing.T) {
	m := metrics.New()
	fp := newFakePublisher(16)
	fp.err = errBoom
	s := NewWithMetrics(fp, testLogger(), m)
	s.wait = func(context.Context, time.Duration) bool { return true } // bounded policy, no real backoff
	defer func() { _ = s.Stop(context.Background()) }()
	s.ReplaceFunction("fn", schedTemplate("jobs.a", "0 3 * * *", "", ""))
	s.Start()

	fireNow(t, s, "fn/jobs.a#0")
	// The publisher is called with bounded retries even though every attempt
	// errors; the fire path must not panic or otherwise fail the scheduler. The
	// budget is attempt(1) + len(publishRetryDelays) retries.
	wantAttempts := 1 + len(publishRetryDelays)
	deadline := time.Now().Add(3 * time.Second)
	for fp.callCount() < wantAttempts && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := fp.callCount(); got != wantAttempts {
		t.Fatalf("publish attempts = %d, want %d (one attempt plus the bounded retries)", got, wantAttempts)
	}
	if n := s.JobCount(); n != 1 {
		t.Fatalf("jobs after publish error = %d, want 1 (scheduler keeps running)", n)
	}
	if got := m.Counter(metrics.MetricSchedulePublishExhausted); got != 1 {
		t.Fatalf("exhausted counter = %d, want 1", got)
	}
	if got := m.Counter(metrics.MetricSchedulePublishRetries); got != int64(len(publishRetryDelays)) {
		t.Fatalf("retries counter = %d, want %d", got, len(publishRetryDelays))
	}
	s.Stop(context.Background())

	// A duplicate publication is a clean terminal no-op: the first attempt
	// returns (false, nil), so no retry is scheduled.
	fp2 := newFakePublisher(4)
	fp2.published = false
	s2 := New(fp2, testLogger())
	defer func() { _ = s2.Stop(context.Background()) }()
	s2.ReplaceFunction("fn", schedTemplate("jobs.a", "0 3 * * *", "", ""))
	s2.Start()
	fireNow(t, s2, "fn/jobs.a#0")
	if !fp2.waitFired(1) {
		t.Fatal("occurrence not attempted")
	}
	// Give any (incorrect) retry a chance to be observed, then require exactly
	// one attempt.
	time.Sleep(50 * time.Millisecond)
	if got := fp2.callCount(); got != 1 {
		t.Fatalf("duplicate publish attempts = %d, want 1 (no retry on duplicate)", got)
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

package cron

import (
	"context"
	"errors"
	"testing"
	"time"

	robfigcron "github.com/robfig/cron/v3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"relay/internal/observability/metrics"
	"relay/internal/observability/tracing"
	"relay/internal/schedule"
	"relay/internal/testutil"
)

// noWait is the deterministic Scheduler.wait used by retry tests: it never
// sleeps and never cancels, so the bounded retry policy is exercised without
// wall-clock waits.
func noWait(context.Context, time.Duration) bool { return true }

// mustParse parses a schedule or fails the test.
func mustParse(t *testing.T, expr string, loc *time.Location) robfigcron.Schedule {
	t.Helper()
	s, err := parseSchedule(expr, loc)
	if err != nil {
		t.Fatalf("parseSchedule(%q): %v", expr, err)
	}
	return s
}

// TestFireRetriesSameOccurrenceThenSucceeds pins the core recovery contract: a
// failing publish is retried with the SAME logical occurrence (never a
// recomputed ID) and the first success ends the loop.
func TestFireRetriesSameOccurrenceThenSucceeds(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 42, 0, time.UTC)
	m := metrics.New()
	fp := newFakePublisher(8)
	fp.script = []pubResult{
		{published: false, err: errBoom},
		{published: false, err: errBoom},
		{published: true},
	}
	s := NewWithMetrics(fp, testLogger(), m)
	s.now = func() time.Time { return frozen }
	s.wait = noWait
	defer func() { _ = s.Stop(context.Background()) }()
	s.ReplaceFunction("fn", schedTemplate("jobs.a", "0 8 * * *", "", ""))
	s.Start()

	fireNow(t, s, "fn/jobs.a")
	deadline := time.Now().Add(3 * time.Second)
	for fp.callCount() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := fp.callCount(); got != 3 {
		t.Fatalf("publish attempts = %d, want 3 (fail, fail, success)", got)
	}
	// Every attempt targeted the same occurrence ID.
	wantDue := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	wantID := schedule.Occurrence{Function: "fn", Schedule: "jobs.a", Handler: "jobs.a", ScheduledAt: wantDue}.ID()
	if n := fp.callsFor(wantID); n != 3 {
		t.Fatalf("attempts for occurrence %q = %d, want 3 (ID must not be recomputed)", wantID, n)
	}
	if got := m.Counter(metrics.MetricSchedulePublishRetries); got != 2 {
		t.Fatalf("retries counter = %d, want 2", got)
	}
	if got := m.Counter(metrics.MetricSchedulePublishExhausted); got != 0 {
		t.Fatalf("exhausted counter = %d, want 0", got)
	}
}

// TestFireSuccessPathSingleAttempt pins that a successful publish is a single
// call with no retries.
func TestFireSuccessPathSingleAttempt(t *testing.T) {
	m := metrics.New()
	fp := newFakePublisher(4)
	fp.script = []pubResult{{published: true}}
	s := NewWithMetrics(fp, testLogger(), m)
	s.wait = noWait
	defer func() { _ = s.Stop(context.Background()) }()
	s.ReplaceFunction("fn", schedTemplate("jobs.a", "* * * * *", "", ""))
	s.Start()

	fireNow(t, s, "fn/jobs.a")
	if !fp.waitFired(1) {
		t.Fatal("occurrence not published")
	}
	time.Sleep(50 * time.Millisecond)
	if got := fp.callCount(); got != 1 {
		t.Fatalf("publish attempts = %d, want 1", got)
	}
	if got := m.Counter(metrics.MetricSchedulePublishRetries); got != 0 {
		t.Fatalf("retries counter = %d, want 0", got)
	}
}

// TestPublishCancellationStopsRetries pins that a cancelled lifecycle aborts the
// retry loop: a pre-cancelled context makes no attempt, and a cancelled wait
// after a failure stops before the next attempt.
func TestPublishCancellationStopsRetries(t *testing.T) {
	fp := newFakePublisher(8)
	fp.err = errBoom
	s := New(fp, testLogger())
	s.wait = func(context.Context, time.Duration) bool { return false } // cancelled wait
	defer func() { _ = s.Stop(context.Background()) }()

	due, ok := latestOccurrence(mustParse(t, "* * * * *", time.UTC), time.Date(2026, 7, 1, 8, 0, 30, 0, time.UTC), occurrenceHorizon)
	if !ok {
		t.Fatal("no occurrence found")
	}
	o := schedule.Occurrence{Function: "fn", Schedule: "jobs.a", Handler: "jobs.a", ScheduledAt: due}

	// A pre-cancelled context stops before the first attempt.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if pub, resolved := s.publishOccurrence(cancelled, o, false); pub || resolved {
		t.Fatalf("cancelled publish = (%v,%v), want (false,false)", pub, resolved)
	}
	if got := fp.callCount(); got != 0 {
		t.Fatalf("attempts after pre-cancelled ctx = %d, want 0", got)
	}

	// A failure followed by a cancelled wait stops retrying: exactly one
	// attempt, unresolved.
	if pub, resolved := s.publishOccurrence(context.Background(), o, false); pub || resolved {
		t.Fatalf("cancelled-wait publish = (%v,%v), want (false,false)", pub, resolved)
	}
	if got := fp.callCount(); got != 1 {
		t.Fatalf("attempts = %d, want 1 (cancelled wait stops before retry)", got)
	}
}

// TestFireRetriesShareOneTrace pins the trace-lineage contract: every retry of
// one occurrence is a `schedule.publish.attempt` child of the SAME
// `schedule.publish` logical span, so all attempts share one trace.
func TestFireRetriesShareOneTrace(t *testing.T) {
	exp, provider := withCronSpanRecorder(t)

	fp := newFakePublisher(8)
	fp.script = []pubResult{
		{published: false, err: errBoom},
		{published: false, err: errBoom},
		{published: true},
	}
	s := New(fp, testLogger())
	s.wait = noWait
	defer func() { _ = s.Stop(context.Background()) }()
	s.ReplaceFunction("fn", schedTemplate("jobs.a", "* * * * *", "", ""))
	s.Start()

	fireNow(t, s, "fn/jobs.a")
	deadline := time.Now().Add(3 * time.Second)
	for fp.callCount() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err := provider.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}
	spans := exp.GetSpans()
	var logical []tracetest.SpanStub
	for _, sp := range spans {
		if sp.Name == "schedule.publish" {
			logical = append(logical, sp)
		}
	}
	if len(logical) != 1 {
		t.Fatalf("schedule.publish logical spans = %d, want 1", len(logical))
	}
	logicalTrace := logical[0].SpanContext.TraceID()
	// Each retry's context must share the logical trace.
	attempts := fp.spans()
	if len(attempts) < 3 {
		t.Fatalf("observed attempt span contexts = %d, want 3", len(attempts))
	}
	for i, sc := range attempts {
		if !sc.IsValid() {
			t.Fatalf("attempt %d has no recording span context", i)
		}
		if sc.TraceID() != logicalTrace {
			t.Fatalf("attempt %d trace = %s, want logical %s", i, sc.TraceID(), logicalTrace)
		}
	}
}

// withCronSpanRecorder installs an SDK provider backed by an in-memory exporter
// through the production Setup seam, restoring the globals on cleanup.
func withCronSpanRecorder(t *testing.T) (*tracetest.InMemoryExporter, *tracing.Provider) {
	t.Helper()
	t.Setenv("OTEL_SDK_DISABLED", "")
	prev := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	exp := tracetest.NewInMemoryExporter()
	provider, err := tracing.Setup(context.Background(), testutil.DiscardLogger(), tracing.WithExporter(exp))
	if err != nil {
		t.Fatalf("tracing.Setup: %v", err)
	}
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(prev)
		otel.SetTextMapPropagator(prevProp)
	})
	return exp, provider
}

// TestLatestOccurrence pins the occurrence derivation, including the
// delayed-callback case that minute truncation got wrong: the latest occurrence
// at or before now is returned, a schedule with none in the horizon reports
// ok=false, and the configured timezone is honored.
func TestLatestOccurrence(t *testing.T) {
	t.Run("delayed callback uses the due boundary", func(t *testing.T) {
		sch := mustParse(t, "0 3 * * *", time.UTC)
		// A 03:00 tick whose callback ran at 03:01:30: the due occurrence is
		// 03:00, not the callback's 03:01 minute.
		got, ok := latestOccurrence(sch, time.Date(2026, 7, 1, 3, 1, 30, 0, time.UTC), occurrenceHorizon)
		if !ok {
			t.Fatal("no occurrence found")
		}
		if want := time.Date(2026, 7, 1, 3, 0, 0, 0, time.UTC); !got.Equal(want) {
			t.Fatalf("occurrence = %v, want %v", got, want)
		}
	})
	t.Run("every minute", func(t *testing.T) {
		sch := mustParse(t, "* * * * *", time.UTC)
		got, ok := latestOccurrence(sch, time.Date(2026, 7, 1, 8, 0, 30, 0, time.UTC), occurrenceHorizon)
		if !ok {
			t.Fatal("no occurrence found")
		}
		if want := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC); !got.Equal(want) {
			t.Fatalf("occurrence = %v, want %v", got, want)
		}
	})
	t.Run("timezone evaluated", func(t *testing.T) {
		rome, err := time.LoadLocation("Europe/Rome")
		if err != nil {
			t.Fatalf("load Europe/Rome: %v", err)
		}
		sch := mustParse(t, "0 8 * * *", rome)
		// 08:00 Rome (CEST, UTC+2) is 06:00Z; at 06:00:30Z the due is 06:00Z.
		got, ok := latestOccurrence(sch, time.Date(2026, 7, 1, 6, 0, 30, 0, time.UTC), occurrenceHorizon)
		if !ok {
			t.Fatal("no occurrence found")
		}
		if want := time.Date(2026, 7, 1, 6, 0, 0, 0, time.UTC); !got.Equal(want) {
			t.Fatalf("occurrence = %v, want %v", got, want)
		}
	})
	t.Run("beyond horizon", func(t *testing.T) {
		// A yearly schedule whose only occurrence is >24h before now.
		sch := mustParse(t, "0 0 1 1 *", time.UTC)
		_, ok := latestOccurrence(sch, time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), occurrenceHorizon)
		if ok {
			t.Fatal("expected no occurrence within the 24h horizon")
		}
	})
	t.Run("exact left horizon boundary is excluded", func(t *testing.T) {
		sch := mustParse(t, "0 0 * * *", time.UTC)
		// The midnight occurrence is exactly now-horizon. The documented search
		// window is open on the left, so this old occurrence is not recovered.
		now := time.Date(2026, 7, 2, 23, 59, 0, 0, time.UTC)
		_, ok := latestOccurrence(sch, now, 23*time.Hour+59*time.Minute)
		if ok {
			t.Fatal("occurrence exactly at now-horizon must be excluded")
		}
	})
	t.Run("calendar with no occurrence returns false", func(t *testing.T) {
		// Standard cron syntax permits a day-of-month that no month can have.
		// gocron/robfig eventually reports no next occurrence; catch-up must
		// drop it quietly rather than fabricate one or panic.
		sch := mustParse(t, "0 0 31 2 *", time.UTC)
		_, ok := latestOccurrence(sch, time.Date(2026, 7, 2, 10, 0, 0, 0, time.UTC), occurrenceHorizon)
		if ok {
			t.Fatal("a schedule with no occurrence must return ok=false")
		}
	})
}

// TestLatestOccurrenceCrossCheckAgainstBruteForce pins the binary-search
// derivation against a direct forward scan of the window, across several cron
// shapes and phases. The brute-force scan is the oracle: it walks every minute
// from now-horizon+1m up to now and keeps the greatest occurrence <= now; the
// search must agree exactly. (The window is kept small so the oracle is cheap.)
func TestLatestOccurrenceCrossCheckAgainstBruteForce(t *testing.T) {
	horizon := 6 * time.Hour
	exprs := []string{"* * * * *", "*/5 * * * *", "0 * * * *", "0 3 * * *", "30 2 * * *"}
	phases := []time.Time{
		time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 2, 3, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 2, 3, 0, 30, 0, time.UTC),
		time.Date(2026, 7, 2, 3, 1, 0, 0, time.UTC),
		time.Date(2026, 7, 2, 4, 17, 42, 0, time.UTC),
		time.Date(2026, 7, 3, 2, 59, 59, 0, time.UTC),
	}
	for _, expr := range exprs {
		sch := mustParse(t, expr, time.UTC)
		for _, now := range phases {
			want, ok := bruteLatest(sch, now, horizon)
			got, gotOK := latestOccurrence(sch, now, horizon)
			if gotOK != ok {
				t.Fatalf("%s @ %v: ok = %v, want %v", expr, now, gotOK, ok)
			}
			if ok && !got.Equal(want) {
				t.Fatalf("%s @ %v: occurrence = %v, want %v", expr, now, got, want)
			}
		}
	}
}

// bruteLatest is the oracle for latestOccurrence: a direct forward scan of the
// window, returning the greatest occurrence at or before now. It starts
// strictly after now-horizon to match latestOccurrence's (now-horizon, now]
// window.
func bruteLatest(sch robfigcron.Schedule, now time.Time, horizon time.Duration) (time.Time, bool) {
	now = now.UTC()
	var best time.Time
	for cur := sch.Next(now.Add(-horizon)); !cur.IsZero() && !cur.After(now); cur = sch.Next(cur) {
		best = cur
	}
	return best, !best.IsZero()
}

// TestCatchUpLatestOnlyWithinHorizon pins the bounded startup recovery: per
// schedule only the latest missed occurrence at or before the snapshot is
// republished, and a future occurrence is never synthesized.
func TestCatchUpLatestOnlyWithinHorizon(t *testing.T) {
	now := time.Date(2026, 7, 2, 10, 2, 0, 0, time.UTC)
	m := metrics.New()
	fp := newFakePublisher(8)
	s := NewWithMetrics(fp, testLogger(), m)
	s.now = func() time.Time { return now }
	s.wait = noWait
	defer func() { _ = s.Stop(context.Background()) }()

	// Daily at 03:00: the latest missed occurrence is 03:00 today (7h before
	// now). The prior day's 03:00 is >24h before now, so it is intentionally
	// dropped.
	s.ReplaceFunction("fn", schedTemplate("jobs.a", "0 3 * * *", "", ""))
	// Every 5 minutes: the latest missed occurrence is 10:00 today.
	s.ReplaceFunction("fn2", schedTemplate("jobs.b", "*/5 * * * *", "", ""))
	// Daily at 23:00: the latest occurrence is YESTERDAY 23:00 (11h before now);
	// today's 23:00 is still in the FUTURE and must not be synthesized.
	s.ReplaceFunction("fn3", schedTemplate("jobs.c", "0 23 * * *", "", ""))

	n := s.CatchUp(context.Background())
	if n != 3 {
		t.Fatalf("catch-up published = %d, want 3", n)
	}
	calls := fp.got()
	if len(calls) != 3 {
		t.Fatalf("catch-up attempts = %d, want 3", len(calls))
	}
	want := map[string]time.Time{
		"fn/jobs.a":  time.Date(2026, 7, 2, 3, 0, 0, 0, time.UTC),
		"fn2/jobs.b": time.Date(2026, 7, 2, 10, 0, 0, 0, time.UTC),
		"fn3/jobs.c": time.Date(2026, 7, 1, 23, 0, 0, 0, time.UTC),
	}
	for _, c := range calls {
		key := c.Function + "/" + c.Handler
		w, ok := want[key]
		if !ok {
			t.Fatalf("unexpected catch-up occurrence %q", key)
		}
		if !c.ScheduledAt.Equal(w) {
			t.Fatalf("catch-up %s scheduled_at = %v, want %v", key, c.ScheduledAt, w)
		}
		if c.ScheduledAt.After(now) {
			t.Fatalf("catch-up synthesized a future occurrence: %s at %v", key, c.ScheduledAt)
		}
	}
	if got := m.Counter(metrics.MetricScheduleCatchUp); got != 3 {
		t.Fatalf("catch-up counter = %d, want 3", got)
	}
}

// TestCatchUpRunsOnce pins that the startup catch-up is once-per-Scheduler:
// live ReplaceFunction converges future occurrences only and never synthesizes
// additional catch-up.
func TestCatchUpRunsOnce(t *testing.T) {
	now := time.Date(2026, 7, 2, 10, 0, 0, 0, time.UTC)
	fp := newFakePublisher(8)
	s := New(fp, testLogger())
	s.now = func() time.Time { return now }
	s.wait = noWait
	defer func() { _ = s.Stop(context.Background()) }()

	s.ReplaceFunction("fn", schedTemplate("jobs.a", "0 3 * * *", "", ""))
	if n := s.CatchUp(context.Background()); n != 1 {
		t.Fatalf("first catch-up = %d, want 1", n)
	}
	// A live change converges future occurrences; a second catch-up is a no-op.
	s.ReplaceFunction("fn", schedTemplate("jobs.b", "0 4 * * *", "", ""))
	if n := s.CatchUp(context.Background()); n != 0 {
		t.Fatalf("second catch-up = %d, want 0 (once per Scheduler)", n)
	}
	if got := fp.callCount(); got != 1 {
		t.Fatalf("total catch-up attempts = %d, want 1", got)
	}
}

// TestCatchUpRepeatedIsDeduped pins that two workers catching up the same
// occurrence rely on the atomic publish-if-new: exactly one wins and the other
// observes a clean duplicate.
func TestCatchUpRepeatedIsDeduped(t *testing.T) {
	now := time.Date(2026, 7, 2, 10, 0, 0, 0, time.UTC)
	pub := newDedupPublisher()
	for i := 0; i < 2; i++ {
		s := New(pub, testLogger())
		s.now = func() time.Time { return now }
		s.wait = noWait
		s.ReplaceFunction("fn", schedTemplate("jobs.a", "0 3 * * *", "", ""))
		n := s.CatchUp(context.Background())
		if i == 0 && n != 1 {
			t.Fatalf("first worker catch-up = %d, want 1 (winner)", n)
		}
		if i == 1 && n != 0 {
			t.Fatalf("second worker catch-up = %d, want 0 (duplicate)", n)
		}
		_ = s.Stop(context.Background())
	}
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if pub.wins != 1 || pub.dups != 1 {
		t.Fatalf("outcomes = wins %d dups %d, want exactly 1 win and 1 duplicate", pub.wins, pub.dups)
	}
}

// TestCatchUpCancellationAborts pins that a cancelled lifecycle ctx aborts the
// catch-up scan without publishing.
func TestCatchUpCancellationAborts(t *testing.T) {
	now := time.Date(2026, 7, 2, 10, 0, 0, 0, time.UTC)
	fp := newFakePublisher(4)
	s := New(fp, testLogger())
	s.now = func() time.Time { return now }
	defer func() { _ = s.Stop(context.Background()) }()
	s.ReplaceFunction("fn", schedTemplate("jobs.a", "0 3 * * *", "", ""))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if n := s.CatchUp(ctx); n != 0 {
		t.Fatalf("cancelled catch-up = %d, want 0", n)
	}
	if got := fp.callCount(); got != 0 {
		t.Fatalf("cancelled catch-up attempts = %d, want 0", got)
	}
}

// TestCatchUpExcludesFutureAndOlderOccurrences pins the horizon boundary: a
// daily 03:00 schedule looked up at and just after 03:00 chooses that instant,
// never the future next day.
func TestCatchUpExcludesFutureAndOlderOccurrences(t *testing.T) {
	fp := newFakePublisher(4)
	s := New(fp, testLogger())
	defer func() { _ = s.Stop(context.Background()) }()
	s.ReplaceFunction("fn", schedTemplate("jobs.a", "0 3 * * *", "", ""))
	s.wait = noWait

	// Exactly on the boundary.
	s.now = func() time.Time { return time.Date(2026, 7, 2, 3, 0, 0, 0, time.UTC) }
	if n := s.CatchUp(context.Background()); n != 1 {
		t.Fatalf("catch-up at boundary = %d, want 1", n)
	}
	if got := fp.got()[0].ScheduledAt; !got.Equal(time.Date(2026, 7, 2, 3, 0, 0, 0, time.UTC)) {
		t.Fatalf("boundary occurrence = %v, want 2026-07-02T03:00Z", got)
	}

	// A fresh Scheduler one minute later chooses the same 03:00, never the
	// future 2026-07-03T03:00.
	fp2 := newFakePublisher(4)
	s2 := New(fp2, testLogger())
	s2.wait = noWait
	defer func() { _ = s2.Stop(context.Background()) }()
	s2.ReplaceFunction("fn", schedTemplate("jobs.a", "0 3 * * *", "", ""))
	s2.now = func() time.Time { return time.Date(2026, 7, 2, 3, 0, 30, 0, time.UTC) }
	if n := s2.CatchUp(context.Background()); n != 1 {
		t.Fatalf("catch-up after boundary = %d, want 1", n)
	}
	if got := fp2.got()[0].ScheduledAt; !got.Equal(time.Date(2026, 7, 2, 3, 0, 0, 0, time.UTC)) {
		t.Fatalf("post-boundary occurrence = %v, want 2026-07-02T03:00Z", got)
	}
}

// TestCatchUpExhaustionCounts pins that an exhausted catch-up budget counts the
// exhaustion and resolves false (no unbounded retry beyond the policy).
func TestCatchUpExhaustionCounts(t *testing.T) {
	now := time.Date(2026, 7, 2, 10, 0, 0, 0, time.UTC)
	m := metrics.New()
	fp := newFakePublisher(16)
	fp.err = errors.New("boom")
	s := NewWithMetrics(fp, testLogger(), m)
	s.now = func() time.Time { return now }
	s.wait = noWait
	defer func() { _ = s.Stop(context.Background()) }()
	s.ReplaceFunction("fn", schedTemplate("jobs.a", "0 3 * * *", "", ""))

	if n := s.CatchUp(context.Background()); n != 0 {
		t.Fatalf("exhausted catch-up = %d, want 0", n)
	}
	if got := fp.callCount(); got != 1+len(publishRetryDelays) {
		t.Fatalf("attempts = %d, want %d", got, 1+len(publishRetryDelays))
	}
	if got := m.Counter(metrics.MetricSchedulePublishExhausted); got != 1 {
		t.Fatalf("exhausted counter = %d, want 1", got)
	}
}

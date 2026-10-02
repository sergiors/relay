package runner

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/schedule"
	"relay/internal/stream"
	"relay/internal/testutil"
)

// occApp builds a prepared app with one named schedule and the given cron,
// timezone, and handler, so InvokeOccurrence's semantic validation can be
// exercised without event rules.
func occApp(t *testing.T, name, scheduleName, handler, cron string, loc *time.Location, exec Executor) *PreparedApp {
	t.Helper()
	return buildFn(fnSpec{
		name: name,
		schedules: []app.Schedule{{
			Name:     scheduleName,
			Handler:  handler,
			Cron:     cron,
			Location: loc,
			Timeout:  time.Second,
			Retries:  0,
		}},
	}, exec)
}

// occOcc builds an occurrence for a fixed app/schedule/handler/instant.
func occOcc(appName, scheduleName, handler string, at time.Time) schedule.Occurrence {
	return schedule.Occurrence{App: appName, Schedule: scheduleName, Handler: handler, ScheduledAt: at}
}

// TestInvokeOccurrenceValidFiringExecutesAndIgnoresEnvelopeHandler pins the happy
// path: a scheduled_at that is a real firing executes the CURRENT configured
// handler, and the envelope's handler (stale/spoofed) never chooses the target.
func TestInvokeOccurrenceValidFiringExecutesAndIgnoresEnvelopeHandler(t *testing.T) {
	exec := &captureExecutor{}
	r := New([]*PreparedApp{occApp(t, "fn", "cleanup", "jobs.new", "0 3 * * *", time.UTC, exec)}, testutil.DiscardLogger())
	ctx := stream.WithInvocationState(context.Background(), newFakeInvocationState())

	firing := time.Date(2026, 7, 1, 3, 0, 0, 0, time.UTC)
	// The envelope handler is a spoofed/stale value; it must be ignored.
	err := r.InvokeOccurrence(ctx, "m-0", occOcc("fn", "cleanup", "jobs.spoofed", firing), []byte(`{"source":"relay.schedule"}`))
	if err != nil {
		t.Fatalf("InvokeOccurrence: %v", err)
	}
	handler, _ := exec.got()
	if handler != "jobs.new" {
		t.Fatalf("executed handler = %q, want the CURRENT configured jobs.new (envelope handler must be ignored)", handler)
	}
}

// TestInvokeOccurrenceNonFiringIsInvalid pins that a scheduled_at that is not a
// real firing of the current cron is a terminal ErrScheduleInvalid (the stream
// dead-letters it) and the handler never executes.
func TestInvokeOccurrenceNonFiringIsInvalid(t *testing.T) {
	exec := &captureExecutor{}
	r := New([]*PreparedApp{occApp(t, "fn", "cleanup", "jobs.run", "0 3 * * *", time.UTC, exec)}, testutil.DiscardLogger())

	for _, at := range []time.Time{
		time.Date(2026, 7, 1, 3, 1, 0, 0, time.UTC),           // wrong minute
		time.Date(2026, 7, 1, 4, 0, 0, 0, time.UTC),           // wrong hour
		time.Date(2026, 7, 1, 3, 0, 1, 0, time.UTC),           // wrong second
		time.Date(2026, 7, 1, 3, 0, 0, 500_000_000, time.UTC), // fractional second in the firing minute
		time.Date(2026, 7, 1, 3, 0, 30, 0, time.UTC),          // non-zero whole second in the firing minute
		time.Date(2026, 7, 1, 2, 59, 0, 0, time.UTC),
	} {
		ctx := stream.WithInvocationState(context.Background(), newFakeInvocationState())
		err := r.InvokeOccurrence(ctx, "m-"+at.Format("150405"), occOcc("fn", "cleanup", "jobs.run", at), []byte(`{}`))
		if !errors.Is(err, stream.ErrScheduleInvalid) {
			t.Fatalf("InvokeOccurrence(%v) err = %v, want ErrScheduleInvalid", at, err)
		}
	}
	if got, _ := exec.got(); got != "" {
		t.Fatalf("executor ran for a non-firing claim: handler = %q", got)
	}
}

// TestInvokeOccurrenceUnparseableCronIsInvalid pins the corrupt-configuration
// case distinct from a non-firing instant: the referenced schedule exists (its
// NAME resolves, so the obsolete/removal path does not apply) but its cron
// cannot be parsed at all. Because no instant can be a firing of an unparseable
// schedule, this is surfaced as terminal ErrScheduleInvalid (the stream
// dead-letters it) and the handler never executes — it is never silently
// accepted as a valid firing.
func TestInvokeOccurrenceUnparseableCronIsInvalid(t *testing.T) {
	exec := &captureExecutor{}
	r := New([]*PreparedApp{occApp(t, "fn", "cleanup", "jobs.run", "not-a-cron", time.UTC, exec)}, testutil.DiscardLogger())
	ctx := stream.WithInvocationState(context.Background(), newFakeInvocationState())

	err := r.InvokeOccurrence(ctx, "m-0", occOcc("fn", "cleanup", "jobs.run", time.Date(2026, 7, 1, 3, 0, 0, 0, time.UTC)), []byte(`{}`))
	if !errors.Is(err, stream.ErrScheduleInvalid) {
		t.Fatalf("InvokeOccurrence with an unparseable cron err = %v, want ErrScheduleInvalid", err)
	}
	if errors.Is(err, stream.ErrInvocationObsolete) {
		t.Fatal("an existing schedule with an unparseable cron must not be classified obsolete")
	}
	if got, _ := exec.got(); got != "" {
		t.Fatalf("executor ran for an unparseable-cron schedule: handler = %q", got)
	}
}

// TestInvokeOccurrenceTimezoneAndDST pins that the schedule's effective timezone
// is honored: the same absolute instant is a valid firing in the configured zone
// and invalid in UTC.
func TestInvokeOccurrenceTimezoneAndDST(t *testing.T) {
	rome, err := time.LoadLocation("Europe/Rome")
	if err != nil {
		t.Fatalf("load Europe/Rome: %v", err)
	}
	exec := &captureExecutor{}
	r := New([]*PreparedApp{occApp(t, "fn", "cleanup", "jobs.run", "0 8 * * *", rome, exec)}, testutil.DiscardLogger())
	ctx := stream.WithInvocationState(context.Background(), newFakeInvocationState())

	// 08:00 Rome (CEST, UTC+2) is 06:00Z: a valid firing.
	if err := r.InvokeOccurrence(ctx, "m-0", occOcc("fn", "cleanup", "jobs.run", time.Date(2026, 7, 1, 6, 0, 0, 0, time.UTC)), []byte(`{}`)); err != nil {
		t.Fatalf("InvokeOccurrence(06:00Z = 08:00 Rome): %v", err)
	}
	// The same 08:00 as a UTC-cron schedule would be 08:00Z; 06:00Z is not that.
	exec2 := &captureExecutor{}
	r2 := New([]*PreparedApp{occApp(t, "fn", "cleanup", "jobs.run", "0 8 * * *", time.UTC, exec2)}, testutil.DiscardLogger())
	ctx2 := stream.WithInvocationState(context.Background(), newFakeInvocationState())
	err = r2.InvokeOccurrence(ctx2, "m-1", occOcc("fn", "cleanup", "jobs.run", time.Date(2026, 7, 1, 6, 0, 0, 0, time.UTC)), []byte(`{}`))
	if !errors.Is(err, stream.ErrScheduleInvalid) {
		t.Fatalf("InvokeOccurrence(06:00Z for a UTC 08:00 cron) err = %v, want ErrScheduleInvalid", err)
	}
}

// TestInvokeOccurrenceDSTSpringForward pins DST handling: on the Europe/Rome
// spring-forward day, a 03:00 local schedule fires at 01:00Z and 00:00Z is not a
// firing.
func TestInvokeOccurrenceDSTSpringForward(t *testing.T) {
	rome, err := time.LoadLocation("Europe/Rome")
	if err != nil {
		t.Fatalf("load Europe/Rome: %v", err)
	}
	exec := &captureExecutor{}
	r := New([]*PreparedApp{occApp(t, "fn", "cleanup", "jobs.run", "0 3 * * *", rome, exec)}, testutil.DiscardLogger())

	// Valid: 01:00Z on the spring-forward day is 03:00 local.
	ctx := stream.WithInvocationState(context.Background(), newFakeInvocationState())
	if err := r.InvokeOccurrence(ctx, "m-0", occOcc("fn", "cleanup", "jobs.run", time.Date(2026, 3, 29, 1, 0, 0, 0, time.UTC)), []byte(`{}`)); err != nil {
		t.Fatalf("InvokeOccurrence(03:00 Rome, spring-forward): %v", err)
	}
	// Invalid: 00:00Z is 01:00 local.
	ctx2 := stream.WithInvocationState(context.Background(), newFakeInvocationState())
	err = r.InvokeOccurrence(ctx2, "m-1", occOcc("fn", "cleanup", "jobs.run", time.Date(2026, 3, 29, 0, 0, 0, 0, time.UTC)), []byte(`{}`))
	if !errors.Is(err, stream.ErrScheduleInvalid) {
		t.Fatalf("InvokeOccurrence(01:00 Rome, spring-forward) err = %v, want ErrScheduleInvalid", err)
	}
}

// TestInvokeOccurrenceHistoricalOffsetNonzeroUTCSecond pins that a legitimate
// local-minute firing with a nonzero UTC second (because the zone's historical
// offset carries a seconds component) is accepted by semantic validation, while
// the neighboring second is rejected. Europe/Rome LMT is UTC+00:49:56, so 12:00
// local in 1866 is 11:10:04Z.
func TestInvokeOccurrenceHistoricalOffsetNonzeroUTCSecond(t *testing.T) {
	rome, err := time.LoadLocation("Europe/Rome")
	if err != nil {
		t.Fatalf("load Europe/Rome: %v", err)
	}
	exec := &captureExecutor{}
	r := New([]*PreparedApp{occApp(t, "fn", "cleanup", "jobs.run", "0 12 * * *", rome, exec)}, testutil.DiscardLogger())

	firing := time.Date(1866, 6, 1, 11, 10, 4, 0, time.UTC)
	ctx := stream.WithInvocationState(context.Background(), newFakeInvocationState())
	if err := r.InvokeOccurrence(ctx, "m-0", occOcc("fn", "cleanup", "jobs.run", firing), []byte(`{}`)); err != nil {
		t.Fatalf("InvokeOccurrence(%v = 12:00 Rome LMT): %v", firing, err)
	}

	ctx2 := stream.WithInvocationState(context.Background(), newFakeInvocationState())
	err = r.InvokeOccurrence(ctx2, "m-1", occOcc("fn", "cleanup", "jobs.run", firing.Add(time.Second)), []byte(`{}`))
	if !errors.Is(err, stream.ErrScheduleInvalid) {
		t.Fatalf("InvokeOccurrence(%v) err = %v, want ErrScheduleInvalid", firing.Add(time.Second), err)
	}
}

// TestInvokeOccurrenceMissingScheduleObsolete pins the preserved removal
// semantics: a schedule NAME absent from the current template is NOT "invalid" —
// it is obsolete (ACKed), never dead-lettered or retried.
func TestInvokeOccurrenceMissingScheduleObsolete(t *testing.T) {
	exec := &captureExecutor{}
	r := New([]*PreparedApp{occApp(t, "fn", "cleanup", "jobs.run", "0 3 * * *", time.UTC, exec)}, testutil.DiscardLogger())
	ctx := stream.WithInvocationState(context.Background(), newFakeInvocationState())

	err := r.InvokeOccurrence(ctx, "m-0", occOcc("fn", "gone", "jobs.run", time.Date(2026, 7, 1, 3, 0, 0, 0, time.UTC)), []byte(`{}`))
	if !errors.Is(err, stream.ErrInvocationObsolete) {
		t.Fatalf("err = %v, want ErrInvocationObsolete (removed schedule is ACKed, not invalid)", err)
	}
	if errors.Is(err, stream.ErrScheduleInvalid) {
		t.Fatal("a removed schedule must not be classified invalid")
	}
	if got, _ := exec.got(); got != "" {
		t.Fatalf("executor ran for an obsolete schedule: handler = %q", got)
	}
}

// TestInvokeOccurrenceMissingAppObsolete pins that an app absent from the
// registry is obsolete (ACKed), not invalid.
func TestInvokeOccurrenceMissingAppObsolete(t *testing.T) {
	r := New([]*PreparedApp{occApp(t, "present", "cleanup", "jobs.run", "0 3 * * *", time.UTC, &captureExecutor{})}, testutil.DiscardLogger())
	ctx := stream.WithInvocationState(context.Background(), newFakeInvocationState())

	err := r.InvokeOccurrence(ctx, "m-0", occOcc("ghost", "cleanup", "jobs.run", time.Date(2026, 7, 1, 3, 0, 0, 0, time.UTC)), []byte(`{}`))
	if !errors.Is(err, stream.ErrInvocationObsolete) {
		t.Fatalf("err = %v, want ErrInvocationObsolete", err)
	}
}

// TestInvokeOccurrenceUnavailableRetryable pins that a configured but
// unavailable app is a temporary, retryable failure — never invalid and never
// obsolete.
func TestInvokeOccurrenceUnavailableRetryable(t *testing.T) {
	r := New([]*PreparedApp{NewUnavailable(app.App{
		Name: "fn",
		Template: &app.Template{
			Runtime: "node24",
			Schedules: []app.Schedule{{
				Name: "cleanup", Handler: "jobs.run", Cron: "0 3 * * *", Location: time.UTC,
			}},
		},
	})}, testutil.DiscardLogger())
	ctx := stream.WithInvocationState(context.Background(), newFakeInvocationState())

	err := r.InvokeOccurrence(ctx, "m-0", occOcc("fn", "cleanup", "jobs.run", time.Date(2026, 7, 1, 3, 0, 0, 0, time.UTC)), []byte(`{}`))
	if err == nil {
		t.Fatal("expected an unavailable error")
	}
	if errors.Is(err, stream.ErrScheduleInvalid) || errors.Is(err, stream.ErrInvocationObsolete) {
		t.Fatalf("unavailable app err = %v, want a plain retryable error", err)
	}
	if !strings.Contains(err.Error(), "not available") {
		t.Fatalf("err = %v, want a not-available error", err)
	}
}

// TestInvokeOccurrenceAdmittedSkipsValidation pins that post-admission an
// occurrence completes even if its scheduled_at no longer matches a changed
// cron: a pinned descriptor is authoritative and never re-validated.
func TestInvokeOccurrenceAdmittedSkipsValidation(t *testing.T) {
	exec := &captureExecutor{}
	r := New([]*PreparedApp{occApp(t, "fn", "cleanup", "jobs.run", "0 3 * * *", time.UTC, exec)}, testutil.DiscardLogger())

	// First delivery: a real firing at 03:00 admits the invocation and pins the
	// descriptor; the executor succeeds.
	first := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), first)
	if err := r.InvokeOccurrence(ctx, "m-0", occOcc("fn", "cleanup", "jobs.run", time.Date(2026, 7, 1, 3, 0, 0, 0, time.UTC)), []byte(`{}`)); err != nil {
		t.Fatalf("first InvokeOccurrence: %v", err)
	}
	if first.scheduleDesc == nil {
		t.Fatal("descriptor not pinned on admission")
	}

	// Second delivery (same message) now carries a NON-firing instant, but the
	// descriptor is pinned: it must not be re-validated and must complete.
	second := newFakeInvocationState()
	second.scheduleDesc = first.scheduleDesc
	ctx2 := stream.WithInvocationState(context.Background(), second)
	err := r.InvokeOccurrence(ctx2, "m-0", occOcc("fn", "cleanup", "jobs.run", time.Date(2026, 7, 1, 3, 30, 0, 0, time.UTC)), []byte(`{}`))
	if err != nil {
		t.Fatalf("admitted InvokeOccurrence err = %v, want nil (pinned descriptor is authoritative)", err)
	}
}

// TestInvokeOccurrenceNoStateDoesNotValidate pins that the state-free path (DLQ
// replay / direct callers) keeps its legacy single-attempt behavior and does not
// apply occurrence validation.
func TestInvokeOccurrenceNoStateDoesNotValidate(t *testing.T) {
	exec := &captureExecutor{}
	r := New([]*PreparedApp{occApp(t, "fn", "cleanup", "jobs.run", "0 3 * * *", time.UTC, exec)}, testutil.DiscardLogger())

	// No invocation state: a non-firing instant still executes (legacy behavior).
	err := r.InvokeOccurrence(context.Background(), "m-0", occOcc("fn", "cleanup", "jobs.run", time.Date(2026, 7, 1, 3, 30, 0, 0, time.UTC)), []byte(`{}`))
	if err != nil {
		t.Fatalf("state-free InvokeOccurrence err = %v, want nil", err)
	}
	if got, _ := exec.got(); got != "jobs.run" {
		t.Fatalf("executor handler = %q, want jobs.run", got)
	}
}

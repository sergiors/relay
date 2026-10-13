package runner

import (
	"context"
	"errors"
	"testing"
	"time"

	"relay/internal/stream"
	"relay/internal/testutil"
)

// F-030 runner-side coverage: the schedule rejection boundary records the
// terminal rejection (invalid vs obsolete) in the invocation-state hash and
// reports it to the stream, and a PREVIOUSLY reserved kind is authoritative even
// when the current template would admit (or differently reject) the occurrence.

// TestInvokeOccurrenceInvalidReservesRejection pins the invalid path: a non-firing
// scheduled_at reserves an INVALID rejection (stream dead-letters it), never pins
// a descriptor, never claims an attempt, and never executes.
func TestInvokeOccurrenceInvalidReservesRejection(t *testing.T) {
	exec := &countingExecutor{}
	r := New([]*PreparedApp{occApp(t, "fn", "cleanup", "jobs.run", "0 3 * * *", time.UTC, exec)}, testutil.DiscardLogger())
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	// 03:30 is not a firing of "0 3 * * *".
	at := time.Date(2026, 7, 1, 3, 30, 0, 0, time.UTC)
	err := r.InvokeOccurrence(ctx, "m-0", occOcc("fn", "cleanup", "jobs.run", at), []byte(`{}`))
	if !errors.Is(err, stream.ErrScheduleInvalid) {
		t.Fatalf("err = %v, want wrapped ErrScheduleInvalid", err)
	}
	if errors.Is(err, stream.ErrInvocationObsolete) {
		t.Fatal("a non-firing occurrence must not be classified obsolete")
	}
	if prog.rejection != stream.ScheduleRejectionInvalid {
		t.Fatalf("reserved rejection = %v, want invalid", prog.rejection)
	}
	if _, ok := prog.ScheduleDescriptor(); ok {
		t.Fatal("a rejected occurrence must not pin a descriptor")
	}
	if len(prog.attempts) != 0 {
		t.Fatalf("attempts = %v, want none (a rejected occurrence must not claim)", prog.attempts)
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls = %d, want 0 (invalid occurrence must not run)", exec.count())
	}
}

// TestInvokeHandlerObsoleteReservesRejection pins the obsolete path: a removed
// schedule NAME reserves an OBSOLETE rejection (stream ACKs it), never pins a
// descriptor and never executes.
func TestInvokeHandlerObsoleteReservesRejection(t *testing.T) {
	exec := &countingExecutor{}
	// App present, but the "cleanup" schedule NAME is gone.
	r := New([]*PreparedApp{schedTmplFn(t, exec)}, testutil.DiscardLogger())
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.InvokeHandler(ctx, "m-0", "fn", "cleanup", "jobs.old", []byte(`{}`))
	if !errors.Is(err, stream.ErrInvocationObsolete) {
		t.Fatalf("err = %v, want wrapped ErrInvocationObsolete", err)
	}
	if prog.rejection != stream.ScheduleRejectionObsolete {
		t.Fatalf("reserved rejection = %v, want obsolete", prog.rejection)
	}
	if _, ok := prog.ScheduleDescriptor(); ok {
		t.Fatal("an obsolete occurrence must not pin a descriptor")
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls = %d, want 0 (obsolete occurrence must not run)", exec.count())
	}
}

// TestInvokeHandlerReservedInvalidIsAuthoritative pins crash recovery: a message
// whose INVALID rejection was reserved by an earlier delivery is dead-lettered on
// the retry even though the CURRENT template would happily admit the occurrence.
func TestInvokeHandlerReservedInvalidIsAuthoritative(t *testing.T) {
	exec := &countingExecutor{}
	r := New([]*PreparedApp{schedTmplFn(t, exec, sched("cleanup", "jobs.run", time.Second, 4))}, testutil.DiscardLogger())
	prog := newFakeInvocationState()
	// Simulate the earlier delivery having reserved an invalid rejection.
	prog.rejection = stream.ScheduleRejectionInvalid
	ctx := stream.WithInvocationState(context.Background(), prog)

	// The schedule NAME exists and would be admitted, but the reservation wins.
	err := r.InvokeHandler(ctx, "m-0", "fn", "cleanup", "jobs.run", []byte(`{}`))
	if !errors.Is(err, stream.ErrScheduleInvalid) {
		t.Fatalf("err = %v, want wrapped ErrScheduleInvalid from the reserved kind", err)
	}
	if errors.Is(err, stream.ErrInvocationObsolete) {
		t.Fatal("a reserved invalid rejection must not be flipped to obsolete")
	}
	if prog.rejection != stream.ScheduleRejectionInvalid {
		t.Fatalf("reserved rejection = %v, want invalid (unchanged)", prog.rejection)
	}
	if _, ok := prog.ScheduleDescriptor(); ok {
		t.Fatal("a reserved occurrence must not pin a descriptor")
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls = %d, want 0 (reserved occurrence must not run)", exec.count())
	}
}

// TestInvokeHandlerReservedObsoleteIsAckedOnRetry pins the obsolete half: a
// reserved OBSOLETE rejection is ACKed on the retry (reported as
// ErrInvocationObsolete), never executed, even though the current template would
// admit the occurrence.
func TestInvokeHandlerReservedObsoleteIsAckedOnRetry(t *testing.T) {
	exec := &countingExecutor{}
	r := New([]*PreparedApp{schedTmplFn(t, exec, sched("cleanup", "jobs.run", time.Second, 4))}, testutil.DiscardLogger())
	prog := newFakeInvocationState()
	prog.rejection = stream.ScheduleRejectionObsolete
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.InvokeHandler(ctx, "m-0", "fn", "cleanup", "jobs.run", []byte(`{}`))
	if !errors.Is(err, stream.ErrInvocationObsolete) {
		t.Fatalf("err = %v, want wrapped ErrInvocationObsolete from the reserved kind", err)
	}
	if errors.Is(err, stream.ErrScheduleInvalid) {
		t.Fatal("a reserved obsolete rejection must not be flipped to invalid")
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls = %d, want 0 (reserved occurrence must not run)", exec.count())
	}
}

// TestInvokeHandlerAppAbsentAdmissionWinsStaysPending pins the app-absent
// admission-first branch: a replica whose registry has NO such app must not
// ACK/DLQ an occurrence another replica already ADMITTED. The reserve-only
// boundary reports the admission in flight, so the runner leaves the message
// pending (ErrInvocationNotEligible), reserves NO rejection, and never executes
// — a replica that can run the pinned invocation completes it.
func TestInvokeHandlerAppAbsentAdmissionWinsStaysPending(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics(nil, testutil.DiscardLogger(), nil)
	prog := newFakeInvocationState()
	// Another replica admitted the occurrence: a descriptor is pinned (its claim
	// is in flight).
	desc := stream.ScheduleDescriptor{Schedule: "cleanup", Handler: "jobs.run", Timeout: time.Minute, Retries: 2}
	prog.scheduleDesc = &desc
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.InvokeHandler(ctx, "m-0", "ghost", "cleanup", "jobs.run", []byte(`{}`))
	if !errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("err = %v, want ErrInvocationNotEligible (admission in flight, keep pending)", err)
	}
	if errors.Is(err, stream.ErrInvocationObsolete) || errors.Is(err, stream.ErrScheduleInvalid) {
		t.Fatalf("err = %v, an admitted occurrence must never be rejected", err)
	}
	if prog.rejection != stream.ScheduleRejectionNone {
		t.Fatalf("reserved rejection = %v, want none (an admission always beats a rejection)", prog.rejection)
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls = %d, want 0 (a missing app cannot run)", exec.count())
	}
}

// TestInvokeHandlerAppAbsentAdmittedMarker pins the defensive admitted-marker
// branch: an app-absent replica that finds an invocation marker with no pinned
// descriptor (legacy/pre-pinning state) must complete the marker's REAL
// disposition and reserve nothing:
//
//   - an ACTIVE marker (running/backoff) keeps the message pending;
//   - a TERMINAL "ok" marker ACKs (nil);
//   - a TERMINAL exhausted marker returns the typed exhaustion error (DLQ);
//   - an active marker wins over a coexisting terminal marker (still pending).
func TestInvokeHandlerAppAbsentAdmittedMarker(t *testing.T) {
	t.Run("active stays pending", func(t *testing.T) {
		exec := &countingExecutor{}
		r := NewWithMetrics(nil, testutil.DiscardLogger(), nil)
		prog := newFakeInvocationState()
		prog.running["ghost/jobs.run"] = time.Now().Add(time.Hour)
		ctx := stream.WithInvocationState(context.Background(), prog)

		err := r.InvokeHandler(ctx, "m-0", "ghost", "cleanup", "jobs.run", []byte(`{}`))
		if !errors.Is(err, stream.ErrInvocationNotEligible) {
			t.Fatalf("err = %v, want ErrInvocationNotEligible (active marker, keep pending)", err)
		}
		if prog.rejection != stream.ScheduleRejectionNone {
			t.Fatalf("reserved rejection = %v, want none", prog.rejection)
		}
		if exec.count() != 0 {
			t.Fatalf("executor calls = %d, want 0", exec.count())
		}
	})
	t.Run("active wins over terminal", func(t *testing.T) {
		r := NewWithMetrics(nil, testutil.DiscardLogger(), nil)
		prog := newFakeInvocationState()
		prog.running["ghost/jobs.run"] = time.Now().Add(time.Hour)
		prog.done["ghost/jobs.other"] = true
		ctx := stream.WithInvocationState(context.Background(), prog)

		err := r.InvokeHandler(ctx, "m-0", "ghost", "cleanup", "jobs.run", []byte(`{}`))
		if !errors.Is(err, stream.ErrInvocationNotEligible) {
			t.Fatalf("err = %v, want ErrInvocationNotEligible (active wins, keep pending)", err)
		}
		if prog.rejection != stream.ScheduleRejectionNone {
			t.Fatalf("reserved rejection = %v, want none", prog.rejection)
		}
	})
	t.Run("complete acks", func(t *testing.T) {
		exec := &countingExecutor{}
		r := NewWithMetrics(nil, testutil.DiscardLogger(), nil)
		prog := newFakeInvocationState()
		prog.done["ghost/jobs.run"] = true
		ctx := stream.WithInvocationState(context.Background(), prog)

		if err := r.InvokeHandler(ctx, "m-0", "ghost", "cleanup", "jobs.run", []byte(`{}`)); err != nil {
			t.Fatalf("err = %v, want nil (legacy ok is ACKed)", err)
		}
		if prog.rejection != stream.ScheduleRejectionNone {
			t.Fatalf("reserved rejection = %v, want none", prog.rejection)
		}
		if exec.count() != 0 {
			t.Fatalf("executor calls = %d, want 0", exec.count())
		}
	})
	t.Run("exhausted routes to DLQ", func(t *testing.T) {
		exec := &countingExecutor{}
		r := NewWithMetrics(nil, testutil.DiscardLogger(), nil)
		prog := newFakeInvocationState()
		prog.exhausted["ghost/jobs.run"] = 2
		ctx := stream.WithInvocationState(context.Background(), prog)

		err := r.InvokeHandler(ctx, "m-0", "ghost", "cleanup", "jobs.run", []byte(`{}`))
		if !errors.Is(err, stream.ErrInvocationExhausted) {
			t.Fatalf("err = %v, want wrapped ErrInvocationExhausted (legacy exhausted is DLQed)", err)
		}
		var he *stream.HandlerExhaustedError
		if !errors.As(err, &he) || len(he.Invocations) != 1 {
			t.Fatalf("err = %v, want one typed exhausted invocation", err)
		}
		got := he.Invocations[0]
		if got.App != "ghost" || got.Handler != "jobs.run" || got.Attempts != 2 {
			t.Fatalf("exhausted invocation = %+v, want ghost/jobs.run attempt 2", got)
		}
		if prog.rejection != stream.ScheduleRejectionNone {
			t.Fatalf("reserved rejection = %v, want none", prog.rejection)
		}
		if exec.count() != 0 {
			t.Fatalf("executor calls = %d, want 0", exec.count())
		}
	})
}

// TestInvokeHandlerAppAbsentHonorsReservedInvalid pins that an app-absent
// replica honors a rejection RESERVED earlier (when the app was still present):
// an invalid reservation is dead-lettered even though the local disposition
// would have been obsolete.
func TestInvokeHandlerAppAbsentHonorsReservedInvalid(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics(nil, testutil.DiscardLogger(), nil)
	prog := newFakeInvocationState()
	prog.rejection = stream.ScheduleRejectionInvalid
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.InvokeHandler(ctx, "m-0", "ghost", "cleanup", "jobs.run", []byte(`{}`))
	if !errors.Is(err, stream.ErrScheduleInvalid) {
		t.Fatalf("err = %v, want wrapped ErrScheduleInvalid from the reserved kind", err)
	}
	if errors.Is(err, stream.ErrInvocationObsolete) {
		t.Fatal("a reserved invalid rejection must not be flipped to obsolete")
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls = %d, want 0", exec.count())
	}
}

// TestInvokeHandlerAppAbsentReservationErrorStaysPending pins that an ambiguous
// reservation outcome (a store/transport error) never dispositions a rejection:
// the message stays pending (ErrInvocationNotEligible plus the unconfirmed
// sentinel) and nothing is reserved.
func TestInvokeHandlerAppAbsentReservationErrorStaysPending(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics(nil, testutil.DiscardLogger(), nil)
	prog := newFakeInvocationState()
	prog.startErr = errors.New("redis down")
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.InvokeHandler(ctx, "m-0", "ghost", "cleanup", "jobs.run", []byte(`{}`))
	if !errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("err = %v, want ErrInvocationNotEligible (ambiguous reservation, keep pending)", err)
	}
	if !errors.Is(err, stream.ErrInvocationClaimUnconfirmed) {
		t.Fatalf("err = %v, want the unconfirmed sentinel to keep the cause visible", err)
	}
	if prog.rejection != stream.ScheduleRejectionNone {
		t.Fatalf("reserved rejection = %v, want none (no rejection on an ambiguous outcome)", prog.rejection)
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls = %d, want 0", exec.count())
	}
}

// TestInvokeHandlerAppAbsentNoStateIsPlainError pins that the state-free path
// (direct callers/tests, DLQ replay) keeps the legacy plain error: it neither
// reserves a rejection nor reports obsolete.
func TestInvokeHandlerAppAbsentNoStateIsPlainError(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics(nil, testutil.DiscardLogger(), nil)

	err := r.InvokeHandler(context.Background(), "m-0", "ghost", "cleanup", "jobs.run", []byte(`{}`))
	if err == nil {
		t.Fatal("expected an error for an absent app on the state-free path")
	}
	if errors.Is(err, stream.ErrInvocationObsolete) || errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("err = %v, the state-free path must keep the legacy plain error", err)
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls = %d, want 0", exec.count())
	}
}

// TestInvokeHandlerPresentAppRemovedScheduleLegacyExhaustedRoutesDLQ pins the
// present-app rejection path's legacy terminal handling: the app is present but
// its schedule NAME was removed, and the message carries a no-descriptor legacy
// exhausted marker. The occurrence must be dead-lettered (never ACKed as
// obsolete, never left pending), so the exhausted invocation's DLQ entry is not
// lost.
func TestInvokeHandlerPresentAppRemovedScheduleLegacyExhaustedRoutesDLQ(t *testing.T) {
	exec := &countingExecutor{}
	// App present, but the "cleanup" schedule NAME is gone (no schedules).
	r := NewWithMetrics([]*PreparedApp{schedTmplFn(t, exec)}, testutil.DiscardLogger(), nil)
	prog := newFakeInvocationState()
	prog.exhausted["fn/jobs.old"] = 3
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.InvokeHandler(ctx, "m-0", "fn", "cleanup", "jobs.old", []byte(`{}`))
	if !errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("err = %v, want wrapped ErrInvocationExhausted (a legacy exhausted marker is DLQed)", err)
	}
	var he *stream.HandlerExhaustedError
	if !errors.As(err, &he) || len(he.Invocations) != 1 {
		t.Fatalf("err = %v, want one typed exhausted invocation", err)
	}
	if got := he.Invocations[0]; got.App != "fn" || got.Handler != "jobs.old" || got.Attempts != 3 {
		t.Fatalf("exhausted invocation = %+v, want fn/jobs.old attempt 3", got)
	}
	if prog.rejection != stream.ScheduleRejectionNone {
		t.Fatalf("reserved rejection = %v, want none (an admitted marker must beat a rejection)", prog.rejection)
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls = %d, want 0", exec.count())
	}
}

// TestInvokeHandlerAppAbsentPinnedMarker pins the descriptor-pinned branch of the
// app-absent path: a current-version delivery pinned + claimed, then completed or
// exhausted, but crashed before its ACK/DLQ — leaving BOTH a pinned descriptor and
// a terminal marker. The app-absent replica must complete the marker's real
// disposition (not report a blanket pending):
//
//   - pinned + active → pending;
//   - pinned + complete → ACK (nil);
//   - pinned + exhausted → the typed exhaustion error (DLQ);
//   - pinned + no marker → pending.
func TestInvokeHandlerAppAbsentPinnedMarker(t *testing.T) {
	newRunner := func(prog *fakeInvocationState) (*Runner, context.Context) {
		r := NewWithMetrics(nil, testutil.DiscardLogger(), nil)
		return r, stream.WithInvocationState(context.Background(), prog)
	}
	pinDesc := func(prog *fakeInvocationState) {
		d := stream.ScheduleDescriptor{Schedule: "cleanup", Handler: "jobs.run", Timeout: time.Minute, Retries: 2}
		prog.scheduleDesc = &d
	}

	t.Run("active pending", func(t *testing.T) {
		exec := &countingExecutor{}
		prog := newFakeInvocationState()
		pinDesc(prog)
		prog.running["ghost/jobs.run"] = time.Now().Add(time.Hour)
		r, ctx := newRunner(prog)

		err := r.InvokeHandler(ctx, "m-0", "ghost", "cleanup", "jobs.run", []byte(`{}`))
		if !errors.Is(err, stream.ErrInvocationNotEligible) {
			t.Fatalf("err = %v, want ErrInvocationNotEligible (pinned active, keep pending)", err)
		}
		if prog.rejection != stream.ScheduleRejectionNone {
			t.Fatalf("reserved rejection = %v, want none", prog.rejection)
		}
		if exec.count() != 0 {
			t.Fatalf("executor calls = %d, want 0", exec.count())
		}
	})

	t.Run("complete acks", func(t *testing.T) {
		exec := &countingExecutor{}
		prog := newFakeInvocationState()
		pinDesc(prog)
		prog.done["ghost/jobs.run"] = true
		r, ctx := newRunner(prog)

		if err := r.InvokeHandler(ctx, "m-0", "ghost", "cleanup", "jobs.run", []byte(`{}`)); err != nil {
			t.Fatalf("err = %v, want nil (pinned ok is ACKed)", err)
		}
		if prog.rejection != stream.ScheduleRejectionNone {
			t.Fatalf("reserved rejection = %v, want none", prog.rejection)
		}
		if exec.count() != 0 {
			t.Fatalf("executor calls = %d, want 0", exec.count())
		}
	})

	t.Run("exhausted routes to DLQ", func(t *testing.T) {
		exec := &countingExecutor{}
		prog := newFakeInvocationState()
		pinDesc(prog)
		prog.exhausted["ghost/jobs.run"] = 3
		r, ctx := newRunner(prog)

		err := r.InvokeHandler(ctx, "m-0", "ghost", "cleanup", "jobs.run", []byte(`{}`))
		if !errors.Is(err, stream.ErrInvocationExhausted) {
			t.Fatalf("err = %v, want wrapped ErrInvocationExhausted (pinned exhausted is DLQed)", err)
		}
		var he *stream.HandlerExhaustedError
		if !errors.As(err, &he) || len(he.Invocations) != 1 {
			t.Fatalf("err = %v, want one typed exhausted invocation", err)
		}
		if got := he.Invocations[0]; got.App != "ghost" || got.Handler != "jobs.run" || got.Attempts != 3 {
			t.Fatalf("exhausted invocation = %+v, want ghost/jobs.run attempt 3", got)
		}
		if prog.rejection != stream.ScheduleRejectionNone {
			t.Fatalf("reserved rejection = %v, want none", prog.rejection)
		}
		if exec.count() != 0 {
			t.Fatalf("executor calls = %d, want 0", exec.count())
		}
	})

	t.Run("pinned no marker pending", func(t *testing.T) {
		exec := &countingExecutor{}
		prog := newFakeInvocationState()
		pinDesc(prog)
		r, ctx := newRunner(prog)

		err := r.InvokeHandler(ctx, "m-0", "ghost", "cleanup", "jobs.run", []byte(`{}`))
		if !errors.Is(err, stream.ErrInvocationNotEligible) {
			t.Fatalf("err = %v, want ErrInvocationNotEligible (pinned, no marker)", err)
		}
		if prog.rejection != stream.ScheduleRejectionNone {
			t.Fatalf("reserved rejection = %v, want none", prog.rejection)
		}
		if exec.count() != 0 {
			t.Fatalf("executor calls = %d, want 0", exec.count())
		}
	})
}

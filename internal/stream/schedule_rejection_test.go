package stream

import (
	"context"
	"testing"
	"time"
)

// F-030: the schedule pre-admission/disposition race. A delivery that observed
// "no admission" (its schedule is gone or the occurrence is invalid) must make
// its terminal REJECTION atomic against a concurrent admission, or a later
// ACK/DLQ can terminalize an invocation another replica just admitted. These
// tests pin the reservation boundary at the in-memory store seam (the real Lua
// scripts are covered by the integration suite):
//
//   - rejection-first: the reservation is written and every later admission is
//     refused, with no descriptor and no active claim ever created;
//   - admission-first: a rejection arriving after an admission is REFUSED and
//     the pinned descriptor adopted (an active claim always beats a rejection);
//   - the reserved kind is authoritative on retry, so a changed template cannot
//     flip an invalid rejection into an obsolete ACK (or vice versa);
//   - a terminal-retained hash is inert (never re-reserved, never re-opened).

// seamInvocation is the "<app>/<handler>" identity the seam tests key on.
func seamInvocation(d ScheduleDescriptor) string { return "fn/" + d.Handler }

// TestScheduleRejectionCodecRoundTrip pins the reserved rejection-field codec:
// each kind encodes to its stable string, and an unknown/empty value does not
// decode (the caller treats an unrecognized reservation as invalid).
func TestScheduleRejectionCodecRoundTrip(t *testing.T) {
	for kind, want := range map[ScheduleRejection]string{
		ScheduleRejectionNone:     "",
		ScheduleRejectionObsolete: scheduleRejectionObsoleteValue,
		ScheduleRejectionInvalid:  scheduleRejectionInvalidValue,
	} {
		if got := encodeScheduleRejection(kind); got != want {
			t.Fatalf("encode(%v) = %q, want %q", kind, got, want)
		}
		if want == "" {
			continue
		}
		got, ok := decodeScheduleRejection(want)
		if !ok || got != kind {
			t.Fatalf("decode(%q) = (%v,%v), want (%v,true)", want, got, ok, kind)
		}
	}
	for _, bad := range []string{"", "bogus", "Obsolete", "invalid:dlq"} {
		if _, ok := decodeScheduleRejection(bad); ok {
			t.Fatalf("decode(%q) ok = true, want false", bad)
		}
	}
}

// TestScheduleAdmissionRejectionReservedRefusesLaterAdmission pins rejection-first:
// a no-proposal rejection is reserved, and a later valid proposal is refused —
// never pinning a descriptor and never claiming an invocation.
func TestScheduleAdmissionRejectionReservedRefusesLaterAdmission(t *testing.T) {
	store := newFakeInvocationStore(nil)
	p := consumerForStore(t, store)

	// A rejects (invalid) with no proposal: the reservation is written.
	adm, err := p.TryStartScheduled(ScheduleDescriptor{}, ScheduleRejectionInvalid, seamInvocation)
	if err != nil {
		t.Fatalf("TryStartScheduled (reject): %v", err)
	}
	if !adm.Rejected || adm.Rejection != ScheduleRejectionInvalid {
		t.Fatalf("admission = %+v, want rejected(invalid)", adm)
	}
	if store.rejection != ScheduleRejectionInvalid {
		t.Fatalf("reserved kind = %v, want invalid", store.rejection)
	}
	// Invariant: no descriptor pinned AND no active claim (no invocation field).
	if _, ok := store.fields[scheduleField]; ok {
		t.Fatal("a rejected occurrence must not pin a descriptor")
	}
	if len(store.fields) != 0 {
		t.Fatalf("a rejected occurrence claimed invocation state: %v", store.fields)
	}

	// B proposes a valid descriptor: refused, never claims, never pins.
	desc := ScheduleDescriptor{Schedule: "cleanup", Handler: "jobs.old", Timeout: 30 * time.Second, Retries: 4}
	admB, err := p.TryStartScheduled(desc, ScheduleRejectionNone, seamInvocation)
	if err != nil {
		t.Fatalf("TryStartScheduled (proposal): %v", err)
	}
	if !admB.Rejected || admB.Rejection != ScheduleRejectionInvalid {
		t.Fatalf("admission = %+v, want rejected(invalid) — the reservation must win", admB)
	}
	if admB.Started {
		t.Fatal("an admission after a reservation must never start")
	}
	if _, ok := store.fields[scheduleField]; ok {
		t.Fatal("a reserved occurrence must not pin a descriptor")
	}
	if len(store.fields) != 0 {
		t.Fatalf("a reserved occurrence claimed invocation state: %v", store.fields)
	}
}

// TestScheduleAdmissionRejectionObsoleteReservedIsAckedKind pins the obsolete
// kind: an obsolete reservation surfaces as rejected(obsolete) (the ACK
// disposition) and equally refuses a later admission.
func TestScheduleAdmissionRejectionObsoleteReserved(t *testing.T) {
	store := newFakeInvocationStore(nil)
	p := consumerForStore(t, store)

	adm, err := p.TryStartScheduled(ScheduleDescriptor{}, ScheduleRejectionObsolete, seamInvocation)
	if err != nil {
		t.Fatalf("TryStartScheduled: %v", err)
	}
	if !adm.Rejected || adm.Rejection != ScheduleRejectionObsolete {
		t.Fatalf("admission = %+v, want rejected(obsolete)", adm)
	}
	desc := ScheduleDescriptor{Schedule: "cleanup", Handler: "jobs.old", Timeout: time.Minute, Retries: 0}
	admB, err := p.TryStartScheduled(desc, ScheduleRejectionNone, seamInvocation)
	if err != nil {
		t.Fatalf("TryStartScheduled: %v", err)
	}
	if !admB.Rejected || admB.Rejection != ScheduleRejectionObsolete {
		t.Fatalf("admission = %+v, want rejected(obsolete)", admB)
	}
}

// TestScheduleAdmissionReservedKindAuthoritativeOnRetry pins that the reserved
// kind, not the retry's current template, decides the disposition: a message
// reserved invalid stays invalid (DLQ) even when a later delivery would call it
// obsolete, and a valid proposal is refused with the SAME reserved kind.
func TestScheduleAdmissionReservedKindAuthoritativeOnRetry(t *testing.T) {
	store := newFakeInvocationStore(nil)
	p := consumerForStore(t, store)

	if _, err := p.TryStartScheduled(ScheduleDescriptor{}, ScheduleRejectionInvalid, seamInvocation); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// A retry whose current template would call the occurrence obsolete still sees
	// the reserved INVALID kind.
	adm, err := p.TryStartScheduled(ScheduleDescriptor{}, ScheduleRejectionObsolete, seamInvocation)
	if err != nil {
		t.Fatalf("retry (obsolete proposal): %v", err)
	}
	if !adm.Rejected || adm.Rejection != ScheduleRejectionInvalid {
		t.Fatalf("retry admission = %+v, want the reserved rejected(invalid)", adm)
	}
	// A retry that could now admit (valid proposal) is refused with the same kind.
	desc := ScheduleDescriptor{Schedule: "cleanup", Handler: "jobs.old", Timeout: time.Minute, Retries: 2}
	adm2, err := p.TryStartScheduled(desc, ScheduleRejectionNone, seamInvocation)
	if err != nil {
		t.Fatalf("retry (valid proposal): %v", err)
	}
	if !adm2.Rejected || adm2.Rejection != ScheduleRejectionInvalid {
		t.Fatalf("retry admission = %+v, want the reserved rejected(invalid)", adm2)
	}
	if adm2.Started {
		t.Fatal("a retry must never start after a reservation")
	}
}

// TestScheduleAdmissionRejectionAdoptsAdmittedFirst pins admission-first: a
// rejection arriving after another delivery ADMITTED the occurrence is refused,
// the pinned descriptor adopted, and (the claim being in flight) the caller is
// told to stay pending. The admitted claim then completes normally and no
// reservation is ever recorded.
func TestScheduleAdmissionRejectionAdoptsAdmittedFirst(t *testing.T) {
	store := newFakeInvocationStore(nil)
	p := consumerForStore(t, store)

	desc := ScheduleDescriptor{Schedule: "cleanup", Handler: "jobs.old", Timeout: time.Hour, Retries: 4}
	b, err := p.TryStartScheduled(desc, ScheduleRejectionNone, seamInvocation)
	if err != nil || !b.Started {
		t.Fatalf("admission = (%+v,%v), want started", b, err)
	}

	// A rejection with no proposal arrives AFTER the admission: it must be refused
	// and the pinned descriptor adopted; the caller is pending (protected), never
	// rejected, never ACKed/DLQ'd as a rejection.
	a, err := p.TryStartScheduled(ScheduleDescriptor{}, ScheduleRejectionInvalid, seamInvocation)
	if err != nil {
		t.Fatalf("TryStartScheduled (rejection): %v", err)
	}
	if a.Rejected {
		t.Fatalf("admission = %+v, an admission-first rejection must be refused", a)
	}
	if a.Started {
		t.Fatal("the rejection must not start a parallel invocation")
	}
	if a.Wait <= 0 {
		t.Fatalf("admission = %+v, want the adopted (protected) claim, wait>0", a)
	}
	if a.Descriptor != desc {
		t.Fatalf("adopted descriptor = %+v, want the admitted %+v", a.Descriptor, desc)
	}
	if store.rejection != ScheduleRejectionNone {
		t.Fatalf("reserved kind = %v, want none (admission-first refuses the rejection)", store.rejection)
	}
	// The admitted claim still completes.
	if !p.MarkComplete("fn/jobs.old", b.Claim) {
		t.Fatal("MarkComplete = false, want true (the admitted claim must survive the rejection)")
	}
	if !p.IsComplete("fn/jobs.old") {
		t.Fatal("the admitted invocation should be complete")
	}
}

// TestReserveScheduleRejectionIdempotent pins the reservation write-once
// contract: a second reservation (a redelivery after a crash, or a concurrent
// replica) is a no-op that reports the STORED kind, so a later invalid
// reservation can never overwrite an earlier obsolete one.
func TestReserveScheduleRejectionIdempotent(t *testing.T) {
	store := newFakeInvocationStore(nil)
	ctx := context.Background()

	first, err := store.reserveScheduleRejection(ctx, "s", "g", "m-0", ScheduleRejectionInvalid)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if first.outcome != scheduleRejectionReserved || first.kind != ScheduleRejectionInvalid {
		t.Fatalf("first reserve = %+v, want reserved(invalid)", first)
	}
	second, err := store.reserveScheduleRejection(ctx, "s", "g", "m-0", ScheduleRejectionObsolete)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if second.outcome != scheduleRejectionAlreadyReserved || second.kind != ScheduleRejectionInvalid {
		t.Fatalf("second reserve = %+v, want alreadyReserved(invalid)", second)
	}
}

// TestReserveScheduleRejectionTerminalRetainedIsInert pins that a
// terminal-retained hash (its message already left the PEL) is never reserved:
// there is nothing to reject, and a stale in-memory delivery cannot re-open it.
func TestReserveScheduleRejectionTerminalRetainedIsInert(t *testing.T) {
	store := newFakeInvocationStore(map[string]string{"fn/h": "ok"})
	store.retained = true

	r, err := store.reserveScheduleRejection(context.Background(), "s", "g", "m-0", ScheduleRejectionInvalid)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if r.outcome != scheduleRejectionTerminal {
		t.Fatalf("reserve = %+v, want terminal", r)
	}
	if store.rejection != ScheduleRejectionNone {
		t.Fatalf("reserved kind = %v, want none (terminal-retained is inert)", store.rejection)
	}
	// The handle reports terminal (not rejected) so the stream ACKs idempotently.
	p := consumerForStore(t, store)
	adm, err := p.TryStartScheduled(ScheduleDescriptor{}, ScheduleRejectionInvalid, seamInvocation)
	if err != nil {
		t.Fatalf("TryStartScheduled: %v", err)
	}
	if adm.Rejected {
		t.Fatalf("admission = %+v, a terminal-retained hash must not be reported rejected", adm)
	}
}

// TestReserveScheduleRejectionRejectsNoKind pins the guard: a caller must name a
// rejection kind; an empty one is refused rather than silently reserving "".
func TestReserveScheduleRejectionRejectsNoKind(t *testing.T) {
	store := newFakeInvocationStore(nil)
	if _, err := store.reserveScheduleRejection(context.Background(), "s", "g", "m-0", ScheduleRejectionNone); err == nil {
		t.Fatal("reserve with no kind must be an error")
	}
}

// TestReserveScheduleRejectionReservesWithoutAdmission pins the reserve-only
// boundary's happy path: with nothing pinned and no active claim, the requested
// rejection is reserved exactly once, reports rejected(kind), and writes no
// descriptor and no invocation field.
func TestReserveScheduleRejectionReservesWithoutAdmission(t *testing.T) {
	store := newFakeInvocationStore(nil)
	p := consumerForStore(t, store)

	adm, err := p.ReserveScheduleRejection(ScheduleRejectionObsolete)
	if err != nil {
		t.Fatalf("ReserveScheduleRejection: %v", err)
	}
	if !adm.Rejected || adm.Rejection != ScheduleRejectionObsolete || adm.InFlight {
		t.Fatalf("admission = %+v, want rejected(obsolete)", adm)
	}
	if store.rejection != ScheduleRejectionObsolete {
		t.Fatalf("reserved kind = %v, want obsolete", store.rejection)
	}
	if len(store.fields) != 0 {
		t.Fatalf("the reservation wrote extra state: %v", store.fields)
	}
}

// TestReserveScheduleRejectionAdmittedElsewhereStaysPending pins the reserve-only
// admission-first branch: a descriptor pinned by another delivery (already
// admitted) is reported InFlight so an app-absent caller keeps the message
// pending and NEVER reserves a rejection that would ACK/DLQ the live admission.
func TestReserveScheduleRejectionAdmittedElsewhereStaysPending(t *testing.T) {
	store := newFakeInvocationStore(nil)
	p := consumerForStore(t, store)

	desc := ScheduleDescriptor{Schedule: "cleanup", Handler: "jobs.old", Timeout: time.Hour, Retries: 4}
	if started, err := p.TryStartScheduled(desc, ScheduleRejectionNone, seamInvocation); err != nil || !started.Started {
		t.Fatalf("admission = (%+v,%v), want started", started, err)
	}

	adm, err := p.ReserveScheduleRejection(ScheduleRejectionObsolete)
	if err != nil {
		t.Fatalf("ReserveScheduleRejection: %v", err)
	}
	if adm.Rejected {
		t.Fatalf("admission = %+v, an admission-first rejection must be refused", adm)
	}
	if !adm.InFlight {
		t.Fatalf("admission = %+v, want InFlight (keep pending)", adm)
	}
	if store.rejection != ScheduleRejectionNone {
		t.Fatalf("reserved kind = %v, want none", store.rejection)
	}
}

// TestReserveScheduleRejectionAdmittedMarkerNoDescriptor pins the defensive
// legacy branch: an invocation marker with no readable descriptor proves the
// occurrence was admitted, so the rejection is refused and NOTHING is written —
// the marker is never overwritten. The disposition follows the marker:
//
//   - an ACTIVE marker (running/backoff) keeps the message PENDING (InFlight);
//   - a TERMINAL "ok" marker yields a zero admission (ACK);
//   - a TERMINAL exhausted marker reports the still-owed invocation(s) so the
//     caller DLQs them;
//   - an active marker WINS over a coexisting terminal marker (still pending).
func TestReserveScheduleRejectionAdmittedMarkerNoDescriptor(t *testing.T) {
	active := runningValue(time.Now().Add(time.Hour), InvocationClaim{Attempt: 1, Token: "ab"})
	pending := nextAttemptValue(time.Now().Add(time.Hour), InvocationClaim{Attempt: 1, Token: "ab"})
	exhausted := exhaustedValue(InvocationClaim{Attempt: 2, Token: "ab"}, false)

	t.Run("active running", func(t *testing.T) {
		assertReserveInFlight(t, map[string]string{"fn/jobs.old": active})
	})
	t.Run("active next_attempt", func(t *testing.T) {
		assertReserveInFlight(t, map[string]string{"fn/jobs.old": pending})
	})
	t.Run("active wins over terminal", func(t *testing.T) {
		assertReserveInFlight(t, map[string]string{"fn/jobs.old": active, "fn/jobs.new": "ok"})
	})
	t.Run("complete", func(t *testing.T) {
		store := newFakeInvocationStore(map[string]string{"fn/jobs.old": "ok"})
		p := consumerForStore(t, store)

		adm, err := p.ReserveScheduleRejection(ScheduleRejectionObsolete)
		if err != nil {
			t.Fatalf("ReserveScheduleRejection: %v", err)
		}
		if adm.Rejected || adm.InFlight || len(adm.ExhaustedInvocations) > 0 {
			t.Fatalf("admission = %+v, want a zero admission (legacy ok is an ACK)", adm)
		}
		assertMarkerUntouched(t, store, "fn/jobs.old", "ok")
	})
	t.Run("exhausted", func(t *testing.T) {
		store := newFakeInvocationStore(map[string]string{"fn/jobs.old": exhausted})
		p := consumerForStore(t, store)

		adm, err := p.ReserveScheduleRejection(ScheduleRejectionObsolete)
		if err != nil {
			t.Fatalf("ReserveScheduleRejection: %v", err)
		}
		if adm.Rejected || adm.InFlight {
			t.Fatalf("admission = %+v, want the exhausted invocations, not a rejection/in-flight", adm)
		}
		if len(adm.ExhaustedInvocations) != 1 {
			t.Fatalf("admission = %+v, want exactly one exhausted invocation", adm)
		}
		got := adm.ExhaustedInvocations[0]
		if got.App != "fn" || got.Handler != "jobs.old" || got.Attempts != 2 {
			t.Fatalf("exhausted invocation = %+v, want fn/jobs.old attempt 2", got)
		}
		assertMarkerUntouched(t, store, "fn/jobs.old", exhausted)
	})
	t.Run("exhausted dlq persisted still reported", func(t *testing.T) {
		// The :dlq marker is still reported; the stream's persistence check skips
		// the write and ACKs, so this is idempotent, not a duplicate.
		marker := exhaustedValue(InvocationClaim{Attempt: 2, Token: "ab"}, true)
		store := newFakeInvocationStore(map[string]string{"fn/jobs.old": marker})
		p := consumerForStore(t, store)

		adm, err := p.ReserveScheduleRejection(ScheduleRejectionObsolete)
		if err != nil {
			t.Fatalf("ReserveScheduleRejection: %v", err)
		}
		if len(adm.ExhaustedInvocations) != 1 || adm.ExhaustedInvocations[0].Attempts != 2 {
			t.Fatalf("admission = %+v, want the :dlq invocation reported", adm)
		}
		assertMarkerUntouched(t, store, "fn/jobs.old", marker)
	})
}

func assertReserveInFlight(t *testing.T, fields map[string]string) {
	t.Helper()
	store := newFakeInvocationStore(fields)
	p := consumerForStore(t, store)
	adm, err := p.ReserveScheduleRejection(ScheduleRejectionObsolete)
	if err != nil {
		t.Fatalf("ReserveScheduleRejection: %v", err)
	}
	if adm.Rejected {
		t.Fatalf("admission = %+v, an admitted marker must beat a rejection", adm)
	}
	if !adm.InFlight {
		t.Fatalf("admission = %+v, want InFlight (keep pending)", adm)
	}
	if len(adm.ExhaustedInvocations) > 0 {
		t.Fatalf("admission = %+v, an active marker must win over a terminal one", adm)
	}
	for field, marker := range fields {
		assertMarkerUntouched(t, store, field, marker)
	}
}

func assertMarkerUntouched(t *testing.T, store *fakeInvocationStore, field, marker string) {
	t.Helper()
	if store.rejection != ScheduleRejectionNone {
		t.Fatalf("reserved kind = %v, want none (an admitted marker must not be overwritten)", store.rejection)
	}
	if store.fields[field] != marker {
		t.Fatalf("marker = %q, want untouched %q", store.fields[field], marker)
	}
}

// TestReserveScheduleRejectionTerminalIsInert pins that a terminal-retained hash
// is never reserved: the reserve-only boundary reports a zero admission so the
// stream ACKs idempotently and no rejection is written.
func TestReserveScheduleRejectionTerminalIsInert(t *testing.T) {
	store := newFakeInvocationStore(nil)
	store.retained = true
	p := consumerForStore(t, store)

	adm, err := p.ReserveScheduleRejection(ScheduleRejectionObsolete)
	if err != nil {
		t.Fatalf("ReserveScheduleRejection: %v", err)
	}
	if adm.Rejected || adm.InFlight {
		t.Fatalf("admission = %+v, want a zero (terminal) admission", adm)
	}
	if store.rejection != ScheduleRejectionNone {
		t.Fatalf("reserved kind = %v, want none", store.rejection)
	}
}

// TestReserveScheduleRejectionPinnedMarker pins the descriptor-pinned branch of
// the reserve-only path: a pinned descriptor (a current-version delivery that
// pinned + claimed, then completed/exhausted but crashed before its ACK/DLQ) must
// still resolve its invocation marker's disposition rather than report a
// blanket "admitted" (which an app-absent caller maps to pending forever):
//
//   - active (running) → InFlight (pending);
//   - complete ("ok") → zero admission (ACK);
//   - exhausted → the still-owed invocation (DLQ then ACK);
//   - pinned with no marker → InFlight (pending).
//
// The present-app TryStartScheduled rejection path is unaffected: it still ADOPTS
// the pinned descriptor and re-runs the normal admission.
func TestReserveScheduleRejectionPinnedMarker(t *testing.T) {
	desc := ScheduleDescriptor{Schedule: "cleanup", Handler: "jobs.old", Timeout: time.Hour, Retries: 4}

	t.Run("active pending", func(t *testing.T) {
		store := newFakeInvocationStore(nil)
		p := consumerForStore(t, store)
		if _, err := p.TryStartScheduled(desc, ScheduleRejectionNone, seamInvocation); err != nil {
			t.Fatalf("TryStartScheduled: %v", err)
		}
		adm, err := p.ReserveScheduleRejection(ScheduleRejectionObsolete)
		if err != nil {
			t.Fatalf("ReserveScheduleRejection: %v", err)
		}
		if adm.Rejected || !adm.InFlight || len(adm.ExhaustedInvocations) > 0 {
			t.Fatalf("admission = %+v, want InFlight (pinned active)", adm)
		}
		assertMarkerUntouched(t, store, "fn/jobs.old", store.fields["fn/jobs.old"])
	})

	t.Run("complete acks", func(t *testing.T) {
		store := newFakeInvocationStore(nil)
		p := consumerForStore(t, store)
		started, err := p.TryStartScheduled(desc, ScheduleRejectionNone, seamInvocation)
		if err != nil {
			t.Fatalf("TryStartScheduled: %v", err)
		}
		if !p.MarkComplete("fn/jobs.old", started.Claim) {
			t.Fatal("MarkComplete = false, want true")
		}
		adm, err := p.ReserveScheduleRejection(ScheduleRejectionObsolete)
		if err != nil {
			t.Fatalf("ReserveScheduleRejection: %v", err)
		}
		if adm.Rejected || adm.InFlight || len(adm.ExhaustedInvocations) > 0 {
			t.Fatalf("admission = %+v, want a zero admission (pinned ok is an ACK)", adm)
		}
		assertMarkerUntouched(t, store, "fn/jobs.old", "ok")
	})

	t.Run("exhausted dlq", func(t *testing.T) {
		store := newFakeInvocationStore(nil)
		p := consumerForStore(t, store)
		started, err := p.TryStartScheduled(desc, ScheduleRejectionNone, seamInvocation)
		if err != nil {
			t.Fatalf("TryStartScheduled: %v", err)
		}
		if !p.MarkExhausted("fn/jobs.old", started.Claim) {
			t.Fatal("MarkExhausted = false, want true")
		}
		adm, err := p.ReserveScheduleRejection(ScheduleRejectionObsolete)
		if err != nil {
			t.Fatalf("ReserveScheduleRejection: %v", err)
		}
		if adm.Rejected || adm.InFlight {
			t.Fatalf("admission = %+v, want the exhausted invocation", adm)
		}
		if len(adm.ExhaustedInvocations) != 1 {
			t.Fatalf("admission = %+v, want exactly one exhausted invocation", adm)
		}
		got := adm.ExhaustedInvocations[0]
		if got.App != "fn" || got.Handler != "jobs.old" || got.Attempts != 1 {
			t.Fatalf("exhausted invocation = %+v, want fn/jobs.old attempt 1", got)
		}
		assertMarkerUntouched(t, store, "fn/jobs.old", store.fields["fn/jobs.old"])
	})

	t.Run("pinned no marker pending", func(t *testing.T) {
		store := newFakeInvocationStore(nil)
		p := consumerForStore(t, store)
		if _, err := p.TryStartScheduled(desc, ScheduleRejectionNone, seamInvocation); err != nil {
			t.Fatalf("TryStartScheduled: %v", err)
		}
		// Drop the invocation marker, leaving only the pinned descriptor.
		delete(store.fields, "fn/jobs.old")
		adm, err := p.ReserveScheduleRejection(ScheduleRejectionObsolete)
		if err != nil {
			t.Fatalf("ReserveScheduleRejection: %v", err)
		}
		if adm.Rejected || !adm.InFlight || len(adm.ExhaustedInvocations) > 0 {
			t.Fatalf("admission = %+v, want InFlight (pinned, no marker)", adm)
		}
	})

	t.Run("try-start still adopts pinned", func(t *testing.T) {
		// The present-app rejection path must still adopt the pinned descriptor
		// (no new execution path): a pinned + complete message reports the adopted
		// (terminal) invocation, never a rejection.
		store := newFakeInvocationStore(nil)
		p := consumerForStore(t, store)
		started, err := p.TryStartScheduled(desc, ScheduleRejectionNone, seamInvocation)
		if err != nil {
			t.Fatalf("TryStartScheduled: %v", err)
		}
		if !p.MarkComplete("fn/jobs.old", started.Claim) {
			t.Fatal("MarkComplete = false, want true")
		}
		adm, err := p.TryStartScheduled(ScheduleDescriptor{}, ScheduleRejectionObsolete, seamInvocation)
		if err != nil {
			t.Fatalf("TryStartScheduled (rejection): %v", err)
		}
		if adm.Rejected || adm.Started {
			t.Fatalf("admission = %+v, want the adopted (terminal) invocation, never a rejection", adm)
		}
		if adm.Descriptor != desc {
			t.Fatalf("adopted descriptor = %+v, want the pinned %+v", adm.Descriptor, desc)
		}
	})
}

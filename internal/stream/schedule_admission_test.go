package stream

import (
	"context"
	"testing"
	"time"
)

// TestScheduleDescriptorCodecRoundTrip pins the reserved scheduleField codec:
// an encoded descriptor decodes back exactly, including names with separators or
// punctuation, and a malformed/foreign value decodes to "absent".
func TestScheduleDescriptorCodecRoundTrip(t *testing.T) {
	cases := []ScheduleDescriptor{
		{Schedule: "cleanup", Handler: "jobs.cleanup.handler", Timeout: 30 * time.Second, Retries: 4},
		{Schedule: "a.b-c_1", Handler: "m.f", Timeout: time.Millisecond, Retries: 0},
		{Schedule: "s:with:colons", Handler: "h", Timeout: 5 * time.Minute, Retries: 7},
		{Schedule: "", Handler: "jobs.run", Timeout: 42 * time.Second, Retries: 3},
	}
	for _, want := range cases {
		got, ok := decodeScheduleDescriptor(encodeScheduleDescriptor(want))
		if !ok {
			t.Fatalf("decode(%q) ok = false, want true", encodeScheduleDescriptor(want))
		}
		if got != want {
			t.Fatalf("round-trip = %+v, want %+v", got, want)
		}
	}

	for _, bad := range []string{
		"",
		"sd1",
		"sd1:",
		"sd1:notanumber:0:aGk:aGk",
		"sd1:01:0:aGk:aGk",      // non-canonical timeout
		"sd1:5:-1:aGk:aGk",      // negative retries
		"sd1:5:0:aGk",           // missing field
		"sd1:5:0:aGk:aGk:extra", // extra field
		"sd1:5:0:!!!:aGk",       // bad base64
		"__terminal",            // foreign reserved value
	} {
		if got, ok := decodeScheduleDescriptor(bad); ok {
			t.Fatalf("decode(%q) = %+v, ok true; want absent", bad, got)
		}
	}
}

// TestScheduleAdmissionFirstPinWinsAndAdopts pins the atomic single-winner
// semantics at the store/InvocationState seam: the first descriptor to pin wins,
// a second, different proposal adopts it, and only the winning handler's
// invocation field is ever claimed.
func TestScheduleAdmissionFirstPinWinsAndAdopts(t *testing.T) {
	store := newFakeInvocationStore(nil)
	p := consumerForStore(t, store)

	old := ScheduleDescriptor{Schedule: "cleanup", Handler: "jobs.old", Timeout: 30 * time.Second, Retries: 4}
	first, err := p.TryStartScheduled(old, ScheduleRejectionNone, func(d ScheduleDescriptor) string { return "fn/" + d.Handler })
	if err != nil {
		t.Fatalf("first TryStartScheduled: %v", err)
	}
	if !first.Started || first.Descriptor.Handler != "jobs.old" {
		t.Fatalf("first admission = %+v, want started on jobs.old", first)
	}

	// A second replica proposes a different descriptor: it adopts jobs.old and,
	// because the first attempt is still protected, is not started.
	other := ScheduleDescriptor{Schedule: "cleanup", Handler: "jobs.new", Timeout: 10 * time.Second, Retries: 0}
	second, err := p.TryStartScheduled(other, ScheduleRejectionNone, func(d ScheduleDescriptor) string { return "fn/" + d.Handler })
	if err != nil {
		t.Fatalf("second TryStartScheduled: %v", err)
	}
	if second.Started {
		t.Fatal("second admission must not start while the first is protected")
	}
	if second.Descriptor.Handler != "jobs.old" {
		t.Fatalf("adopted descriptor = %+v, want the winner jobs.old", second.Descriptor)
	}
	// Only the winner's invocation field was ever claimed.
	if _, ok := store.fields["fn/jobs.old"]; !ok {
		t.Fatalf("winning invocation field missing: %v", store.fields)
	}
	if _, ok := store.fields["fn/jobs.new"]; ok {
		t.Fatal("a parallel invocation field fn/jobs.new was claimed")
	}
}

// TestScheduleAdmissionNoDescriptorAndNoProposalIsObsolete pins the
// never-admitted-removed case: with nothing pinned and no proposal, admission
// RESERVES the obsolete rejection and writes no descriptor or invocation field.
func TestScheduleAdmissionNoDescriptorAndNoProposalIsObsolete(t *testing.T) {
	store := newFakeInvocationStore(nil)
	p := consumerForStore(t, store)

	adm, err := p.TryStartScheduled(ScheduleDescriptor{}, ScheduleRejectionObsolete, func(d ScheduleDescriptor) string { return "fn/" + d.Handler })
	if err != nil {
		t.Fatalf("TryStartScheduled: %v", err)
	}
	if !adm.Rejected || adm.Rejection != ScheduleRejectionObsolete {
		t.Fatalf("admission = %+v, want rejected(obsolete)", adm)
	}
	// The rejection is reserved (so a later admission is refused), but no
	// descriptor is pinned and no invocation field is claimed.
	if store.rejection != ScheduleRejectionObsolete {
		t.Fatalf("reserved rejection = %v, want obsolete", store.rejection)
	}
	if _, ok := store.fields[scheduleField]; ok {
		t.Fatal("an obsolete occurrence must not pin a descriptor")
	}
	if len(store.fields) != 0 {
		t.Fatalf("obsolete occurrence wrote invocation state: %v", store.fields)
	}
}

// TestScheduleAdmissionRejectionRefusedWhenAdmittedFirst pins the admission-first
// branch of the rejection boundary: a rejection arriving after the occurrence was
// ADMITTED is refused and the pinned descriptor adopted, so a rejection can never
// cancel a live admission (F-030).
func TestScheduleAdmissionRejectionRefusedWhenAdmittedFirst(t *testing.T) {
	store := newFakeInvocationStore(nil)
	p := consumerForStore(t, store)

	pinned := ScheduleDescriptor{Schedule: "cleanup", Handler: "jobs.old", Timeout: 30 * time.Second, Retries: 4}
	first, err := p.TryStartScheduled(pinned, ScheduleRejectionNone, func(d ScheduleDescriptor) string { return "fn/" + d.Handler })
	if err != nil || !first.Started {
		t.Fatalf("first admission = (%+v,%v), want started", first, err)
	}
	// The admitted invocation is complete; a rejection arriving now must NOT be
	// recorded and must report the (terminal) admitted invocation instead.
	if !p.MarkComplete("fn/jobs.old", first.Claim) {
		t.Fatal("MarkComplete = false, want true")
	}
	adm, err := p.TryStartScheduled(ScheduleDescriptor{}, ScheduleRejectionInvalid, func(d ScheduleDescriptor) string { return "fn/" + d.Handler })
	if err != nil {
		t.Fatalf("TryStartScheduled: %v", err)
	}
	if adm.Rejected {
		t.Fatalf("admission = %+v, a rejection must be refused once the occurrence was admitted", adm)
	}
	if store.rejection != ScheduleRejectionNone {
		t.Fatalf("reserved rejection = %v, want none (admission-first refuses the rejection)", store.rejection)
	}
}

// TestScheduleAdmissionPinnedDescriptorSurvivesNoProposal pins the post-admission
// removal case at the seam: once a descriptor is pinned, a delivery that can no
// longer resolve the schedule (no proposal) still adopts the pinned descriptor
// and runs its handler.
func TestScheduleAdmissionPinnedDescriptorSurvivesNoProposal(t *testing.T) {
	store := newFakeInvocationStore(nil)
	p := consumerForStore(t, store)

	pinned := ScheduleDescriptor{Schedule: "cleanup", Handler: "jobs.old", Timeout: 30 * time.Second, Retries: 4}
	first, err := p.TryStartScheduled(pinned, ScheduleRejectionNone, func(d ScheduleDescriptor) string { return "fn/" + d.Handler })
	if err != nil || !first.Started {
		t.Fatalf("first admission = (%+v,%v), want started", first, err)
	}
	// Simulate the admitted attempt's deadline elapsing (its running marker is now
	// eligible but the descriptor stays pinned), then deliver with no proposal
	// (the schedule was removed): the delivery must adopt the pinned descriptor
	// and re-claim under the SAME handler.
	store.fields["fn/jobs.old"] = runningValue(
		time.Now().Add(-time.Hour), InvocationClaim{Attempt: 1, Token: "ab"})
	adm, err := p.TryStartScheduled(ScheduleDescriptor{}, ScheduleRejectionObsolete, func(d ScheduleDescriptor) string { return "fn/" + d.Handler })
	if err != nil {
		t.Fatalf("TryStartScheduled: %v", err)
	}
	if adm.Rejected {
		t.Fatal("a pinned (admitted) occurrence must not be rejected when its schedule is removed")
	}
	if !adm.Started || adm.Descriptor.Handler != "jobs.old" {
		t.Fatalf("admission = %+v, want started on the pinned jobs.old", adm)
	}
	if adm.Claim.Attempt != 2 {
		t.Fatalf("attempt = %d, want 2 (carried forward from the elapsed admitted attempt)", adm.Claim.Attempt)
	}
	// The descriptor is unchanged (never re-pinned by the removed-schedule delivery).
	if got, ok := p.ScheduleDescriptor(); !ok || got != pinned {
		t.Fatalf("descriptor = (%+v,%v), want the pinned one", got, ok)
	}
}

// TestScheduleDescriptorLifecycleThroughTerminalRetention pins the descriptor's
// lifetime against the existing state contract: while the message is recoverable
// the pinned descriptor is readable; after a successful ACK switches the hash to
// terminal retention the descriptor is still readable (until the retention TTL
// expires with the whole hash); and a stale in-memory delivery can neither
// re-pin nor re-open it.
func TestScheduleDescriptorLifecycleThroughTerminalRetention(t *testing.T) {
	store := newFakeInvocationStore(nil)
	p := consumerForStore(t, store)

	pinned := ScheduleDescriptor{Schedule: "cleanup", Handler: "jobs.old", Timeout: 30 * time.Second, Retries: 4}
	adm, err := p.TryStartScheduled(pinned, ScheduleRejectionNone, func(d ScheduleDescriptor) string { return "fn/" + d.Handler })
	if err != nil || !adm.Started {
		t.Fatalf("admission = (%+v,%v), want started", adm, err)
	}
	if !p.MarkComplete("fn/jobs.old", adm.Claim) {
		t.Fatal("MarkComplete = false, want true")
	}
	// The pinned descriptor stays readable while the message is recoverable.
	if got, ok := p.ScheduleDescriptor(); !ok || got != pinned {
		t.Fatalf("descriptor before retention = (%+v,%v), want the pinned one", got, ok)
	}

	// The stream switches the hash to terminal retention after the ACK.
	if err := store.retainTerminal(context.Background(), "s", "g", "m-0", DefaultInvocationRetention); err != nil {
		t.Fatalf("retainTerminal: %v", err)
	}
	// The descriptor is still readable (it expires with the whole hash, and a
	// terminal-retained hash keeps every field until its TTL).
	if got, ok := p.ScheduleDescriptor(); !ok || got != pinned {
		t.Fatalf("descriptor after retention = (%+v,%v), want the pinned one", got, ok)
	}
	// A stale in-memory delivery can neither re-pin nor re-open the invocation.
	adm2, err := p.TryStartScheduled(ScheduleDescriptor{}, ScheduleRejectionObsolete, func(d ScheduleDescriptor) string { return "fn/" + d.Handler })
	if err != nil {
		t.Fatalf("TryStartScheduled after retention: %v", err)
	}
	if adm2.Rejected {
		t.Fatal("a terminal-retained occurrence must not be reported rejected")
	}
	if adm2.Started {
		t.Fatal("a terminal-retained hash must never be re-opened")
	}
	if got, ok := p.ScheduleDescriptor(); !ok || got != pinned {
		t.Fatalf("descriptor mutated by a stale delivery = (%+v,%v)", got, ok)
	}
}

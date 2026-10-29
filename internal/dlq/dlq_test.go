package dlq

import (
	"testing"
)

// TestNewDeliveryIDIsDeterministic verifies the delivery identity is a pure
// function of (source, group, message ID): repeated invocations agree, and any
// input difference produces a different identity — including message IDs that
// differ only by sequence number (the broker's only per-delivery distinction).
func TestNewDeliveryIDIsDeterministic(t *testing.T) {
	same := NewDeliveryID("events", "relay", "1760000000000-0")
	if again := NewDeliveryID("events", "relay", "1760000000000-0"); again != same {
		t.Fatalf("NewDeliveryID is not deterministic: %q vs %q", same, again)
	}
	for _, variant := range []struct{ source, group, id string }{
		{"events", "relay", "1760000000000-1"}, // different message
		{"events", "other", "1760000000000-0"}, // different group
		{"other", "relay", "1760000000000-0"},  // different source
	} {
		if got := NewDeliveryID(variant.source, variant.group, variant.id); got == same {
			t.Fatalf("NewDeliveryID(%q,%q,%q) collided with %q",
				variant.source, variant.group, variant.id, same)
		}
	}
}

// TestNewIDLengthPrefixesCannotAlias pins the anti-aliasing property of the
// length-prefixed hashing: field tuples whose naive concatenation would be
// identical ("a"+"bc" vs "ab"+"c") must produce distinct delivery identities,
// so no two distinct logical deliveries can ever share an entry ID.
func TestNewIDLengthPrefixesCannotAlias(t *testing.T) {
	a := NewDeliveryID("a", "bc", "m")
	b := NewDeliveryID("ab", "c", "m")
	if a == b {
		t.Fatalf("distinct deliveries (a|bc|m) and (ab|c|m) produced one identity %q", a)
	}
	c := NewDeliveryID("stream", "group", "1-0")
	d := NewDeliveryID("stream", "group", "10-")
	if c == d {
		t.Fatalf("distinct message IDs %q and %q produced one identity %q", "1-0", "10-", c)
	}
}

// TestNewIDForDeliveryPinsInvocationIdentity verifies the entry identity binds
// one exhaustible invocation to one delivery: the same delivery and invocation
// always map to the same stable ID (the idempotency contract), a different
// invocation on the same delivery maps to a different entry, and the empty
// invocation (the malformed-message placeholder) is a distinct identity from
// any real invocation.
func TestNewIDForDeliveryPinsInvocationIdentity(t *testing.T) {
	const delivery = "delivery-identity"
	placeholder := NewIDForDelivery(delivery, "")
	if again := NewIDForDelivery(delivery, ""); again != placeholder {
		t.Fatalf("NewIDForDelivery is not deterministic: %q vs %q", placeholder, again)
	}
	h := NewIDForDelivery(delivery, "fn/h")
	if again := NewIDForDelivery(delivery, "fn/h"); again != h {
		t.Fatalf("NewIDForDelivery is not deterministic: %q vs %q", h, again)
	}
	if h == placeholder {
		t.Fatal("a real invocation and the placeholder must be distinct entries")
	}
	if other := NewIDForDelivery(delivery, "fn/other"); other == h {
		t.Fatalf("distinct invocations fn/h and fn/other share one entry ID %q", h)
	}
	// The wrapper computes the delivery identity first, so NewID agrees with
	// NewIDForDelivery(NewDeliveryID(...), invocation).
	viaNewID := NewID("events", "relay", "1760000000000-0", "fn/h")
	viaDelivery := NewIDForDelivery(NewDeliveryID("events", "relay", "1760000000000-0"), "fn/h")
	if viaNewID != viaDelivery {
		t.Fatalf("NewID and NewIDForDelivery disagree: %q vs %q", viaNewID, viaDelivery)
	}
}

// TestEntryReplayable pins the replay predicate on the broker-neutral read
// model: an entry attributing a real app/handler invocation is replayable,
// while the NoHandler placeholder ("-" on either side) and an empty identity
// are not — `relay dlq replay` must refuse them before dialing the worker.
func TestEntryReplayable(t *testing.T) {
	real := Entry{App: "fn", Handler: "index.run"}
	if !real.Replayable() {
		t.Fatal("an entry with a real invocation must be replayable")
	}
	for _, e := range []Entry{
		{App: NoHandler, Handler: NoHandler}, // malformed-message placeholder
		{App: NoHandler, Handler: "index.run"},
		{App: "fn", Handler: NoHandler},
		{App: "", Handler: "index.run"},
		{App: "fn", Handler: ""},
	} {
		if e.Replayable() {
			t.Fatalf("entry %+v must not be replayable", e)
		}
	}
}

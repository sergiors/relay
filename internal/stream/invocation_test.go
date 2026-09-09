package stream

import (
	"context"
	"testing"
)

// TestInvocationStateKeyLayout pins the exact Redis key layout so a change to
// the namespace is a deliberate, reviewed decision. Colon-free names keep their
// readable form; colons in stream/group names are percent-encoded so two
// distinct (stream, group) pairs can never alias onto one key. The prefix
// superseded "relay:progress:" — state from older binaries is not migrated:
// pending messages simply re-run their handlers (at-least-once).
func TestInvocationStateKeyLayout(t *testing.T) {
	got := invocationStateKey("orders", "orders-group", "1757-0")
	want := "relay:invocation:orders:orders-group:1757-0"
	if got != want {
		t.Fatalf("invocationStateKey = %q, want %q", got, want)
	}

	// Injectivity: a colon in the stream vs a colon in the group must not
	// produce the same key for the same msgID.
	a := invocationStateKey("a:b", "c", "1-0")
	b := invocationStateKey("a", "b:c", "1-0")
	if a == b {
		t.Fatalf("invocationStateKey aliased distinct (stream, group) pairs: both %q", a)
	}
	if a != "relay:invocation:a%3Ab:c:1-0" {
		t.Fatalf("invocationStateKey(\"a:b\",\"c\",\"1-0\") = %q, want %q", a, "relay:invocation:a%3Ab:c:1-0")
	}
	if b != "relay:invocation:a:b%3Ac:1-0" {
		t.Fatalf("invocationStateKey(\"a\",\"b:c\",\"1-0\") = %q, want %q", b, "relay:invocation:a:b%3Ac:1-0")
	}
}

// TestInvocationStateContextRoundTrip verifies the WithInvocationState /
// InvocationStateFrom contract: a present value round-trips to the same
// instance, an absent value yields (nil, false), and a nil interface value
// stored in ctx is treated as absent (safe, no panic).
func TestInvocationStateContextRoundTrip(t *testing.T) {
	// Present: same instance comes back.
	p := &invocationState{}
	ctx := WithInvocationState(context.Background(), p)
	got, ok := InvocationStateFrom(ctx)
	if !ok {
		t.Fatal("expected invocation state present in ctx")
	}
	if got != p {
		t.Fatalf("round-trip instance mismatch: got %p, want %p", got, p)
	}

	// Absent: (nil, false).
	if p2, ok := InvocationStateFrom(context.Background()); ok || p2 != nil {
		t.Fatalf("absent ctx: got (%v, %v), want (nil, false)", p2, ok)
	}

	// Nil interface value stored in ctx: treated as absent, no panic.
	var nilP InvocationState
	ctxNil := WithInvocationState(context.Background(), nilP)
	if p3, ok := InvocationStateFrom(ctxNil); ok || p3 != nil {
		t.Fatalf("nil interface in ctx: got (%v, %v), want (nil, false)", p3, ok)
	}
}

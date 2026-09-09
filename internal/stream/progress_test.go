package stream

import (
	"context"
	"testing"
)

// TestProgressKeyLayout pins the exact Redis key layout so a change to the
// namespace is a deliberate, reviewed decision. Colon-free names keep their
// readable form; colons in stream/group names are percent-encoded so two
// distinct (stream, group) pairs can never alias onto one key.
func TestProgressKeyLayout(t *testing.T) {
	got := progressKey("orders", "orders-group", "1757-0")
	want := "relay:progress:orders:orders-group:1757-0"
	if got != want {
		t.Fatalf("progressKey = %q, want %q", got, want)
	}

	// Injectivity: a colon in the stream vs a colon in the group must not
	// produce the same key for the same msgID.
	a := progressKey("a:b", "c", "1-0")
	b := progressKey("a", "b:c", "1-0")
	if a == b {
		t.Fatalf("progressKey aliased distinct (stream, group) pairs: both %q", a)
	}
	if a != "relay:progress:a%3Ab:c:1-0" {
		t.Fatalf("progressKey(\"a:b\",\"c\",\"1-0\") = %q, want %q", a, "relay:progress:a%3Ab:c:1-0")
	}
	if b != "relay:progress:a:b%3Ac:1-0" {
		t.Fatalf("progressKey(\"a\",\"b:c\",\"1-0\") = %q, want %q", b, "relay:progress:a:b%3Ac:1-0")
	}
}

// TestInvocationProgressContextRoundTrip verifies the WithInvocationProgress /
// InvocationProgressFrom contract: a present value round-trips to the same
// instance, an absent value yields (nil, false), and a nil interface value
// stored in ctx is treated as absent (safe, no panic).
func TestInvocationProgressContextRoundTrip(t *testing.T) {
	// Present: same instance comes back.
	p := &invocationProgress{}
	ctx := WithInvocationProgress(context.Background(), p)
	got, ok := InvocationProgressFrom(ctx)
	if !ok {
		t.Fatal("expected progress present in ctx")
	}
	if got != p {
		t.Fatalf("round-trip instance mismatch: got %p, want %p", got, p)
	}

	// Absent: (nil, false).
	if p2, ok := InvocationProgressFrom(context.Background()); ok || p2 != nil {
		t.Fatalf("absent ctx: got (%v, %v), want (nil, false)", p2, ok)
	}

	// Nil interface value stored in ctx: treated as absent, no panic.
	var nilP InvocationProgress
	ctxNil := WithInvocationProgress(context.Background(), nilP)
	if p3, ok := InvocationProgressFrom(ctxNil); ok || p3 != nil {
		t.Fatalf("nil interface in ctx: got (%v, %v), want (nil, false)", p3, ok)
	}
}

package stream

import (
	"context"
	"testing"
	"time"
)

// TestInvocationStateKeyLayout pins the exact Redis key layout so a change to
// the namespace is a deliberate, reviewed decision. Colon-free names keep their
// readable form; colons in stream/group names are percent-encoded so two
// distinct (stream, group) pairs can never alias onto one key.
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

// TestRunningValueRoundTrip pins the "running:<unixnano>#<attempts>" field-value
// encoding: runningValue encodes an absolute deadline and attempt number and
// parseInvocationState decodes it back. The "running:" prefix distinguishes the
// marker from the "ok" completion sentinel; the Unix-nano suffix is the deadline
// at which the attempt is considered abandoned.
func TestRunningValueRoundTrip(t *testing.T) {
	dl := time.Unix(0, 1757000000000000000)
	v := runningValue(dl, 3)
	if v != "running:1757000000000000000#3" {
		t.Fatalf("runningValue = %q, want %q", v, "running:1757000000000000000#3")
	}
	kind, got, attempts, ok := parseInvocationState(v)
	if !ok {
		t.Fatalf("parseInvocationState(%q) ok = false, want true", v)
	}
	if kind != kindRunning {
		t.Fatalf("parseInvocationState(%q) kind = %v, want kindRunning", v, kind)
	}
	if !got.Equal(dl) {
		t.Fatalf("parseInvocationState(%q) deadline = %v, want %v", v, got, dl)
	}
	if attempts != 3 {
		t.Fatalf("parseInvocationState(%q) attempts = %d, want 3", v, attempts)
	}
}

// TestParseInvocationStateValueGrammar pins the full value grammar: ok, running,
// next_attempt_at, exhausted, and unparseable values (treated as eligible). The
// attempt count is mandatory in deadline markers: a bare "<prefix>:<deadline>"
// without "#<attempts>" does not parse.
func TestParseInvocationStateValueGrammar(t *testing.T) {
	dl := time.Unix(0, 1757000000000000000)

	// ok → complete.
	if kind, _, _, ok := parseInvocationState("ok"); !ok || kind != kindComplete {
		t.Fatalf("parseInvocationState(\"ok\") = kind %v ok %v, want kindComplete true", kind, ok)
	}

	// running with attempts.
	if kind, got, n, ok := parseInvocationState("running:1757000000000000000#2"); !ok || kind != kindRunning || !got.Equal(dl) || n != 2 {
		t.Fatalf("running#2 = kind %v dl %v n %d ok %v", kind, got, n, ok)
	}

	// next_attempt_at with attempts.
	if kind, got, n, ok := parseInvocationState("next_attempt_at:1757000000000000000#4"); !ok || kind != kindNextAttempt || !got.Equal(dl) || n != 4 {
		t.Fatalf("next_attempt_at#4 = kind %v dl %v n %d ok %v", kind, got, n, ok)
	}

	// exhausted.
	if kind, _, n, ok := parseInvocationState("exhausted:5"); !ok || kind != kindExhausted || n != 5 {
		t.Fatalf("exhausted:5 = kind %v n %d ok %v", kind, n, ok)
	}

	// Unparseable values → eligible (ok=false). A deadline marker without the
	// mandatory "#<attempts>" part also does not parse.
	for _, v := range []string{
		"",
		"running:",
		"running:notanumber",
		"running:1757000000000000000",
		"running:123#",
		"running:123#0",
		"running:123#abc",
		"next_attempt_at:",
		"next_attempt_at:notanumber",
		"next_attempt_at:1757000000000000000",
		"exhausted:",
		"exhausted:0",
		"exhausted:abc",
		"bogus",
	} {
		if _, _, _, ok := parseInvocationState(v); ok {
			t.Errorf("parseInvocationState(%q) ok = true, want false (eligible)", v)
		}
	}
}

// TestNextAttemptAndExhaustedValueRoundTrip pins the next_attempt_at and
// exhausted encodings.
func TestNextAttemptAndExhaustedValueRoundTrip(t *testing.T) {
	dl := time.Unix(0, 1757000000000000000)
	if v := nextAttemptValue(dl, 2); v != "next_attempt_at:1757000000000000000#2" {
		t.Fatalf("nextAttemptValue = %q", v)
	}
	if v := exhaustedValue(5); v != "exhausted:5" {
		t.Fatalf("exhaustedValue = %q", v)
	}
}

// TestNewInvocationStateClockOption verifies the WithClock option wires the
// injected clock into the handle, so tests can freeze/advance time without
// changing production semantics (production passes no option → time.Now).
func TestNewInvocationStateClockOption(t *testing.T) {
	frozen := time.Unix(0, 1757000000000000000)
	p := NewInvocationState(context.Background(), &invocationStore{}, "s", "g", "m", nil,
		WithClock(func() time.Time { return frozen }))
	ip, ok := p.(*invocationState)
	if !ok {
		t.Fatalf("NewInvocationState returned %T, want *invocationState", p)
	}
	if got := ip.now(); !got.Equal(frozen) {
		t.Fatalf("injected clock = %v, want %v", got, frozen)
	}
}

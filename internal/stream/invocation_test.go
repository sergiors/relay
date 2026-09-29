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

// TestRunningValueRoundTrip pins the "running:<deadline_ms>:<attempt>:<token>"
// field-value encoding: runningValue encodes an absolute millisecond deadline
// and claim and parseInvocationState decodes it back. The "running:" prefix
// distinguishes the marker from the "ok" completion sentinel; the deadline is
// the integer Unix-ms at which the attempt is considered abandoned.
func TestRunningValueRoundTrip(t *testing.T) {
	dl := time.UnixMilli(1757000000000)
	claim := InvocationClaim{Attempt: 3, Token: "0a1b2c3d"}
	v := runningValue(dl, claim)
	if v != "running:1757000000000:3:0a1b2c3d" {
		t.Fatalf("runningValue = %q, want %q", v, "running:1757000000000:3:0a1b2c3d")
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
// attempt and token parts are mandatory in every active/exhausted marker: a
// marker missing either does not parse.
func TestParseInvocationStateValueGrammar(t *testing.T) {
	dl := time.UnixMilli(1757000000000)

	// ok → complete.
	if kind, _, _, ok := parseInvocationState("ok"); !ok || kind != kindComplete {
		t.Fatalf("parseInvocationState(\"ok\") = kind %v ok %v, want kindComplete true", kind, ok)
	}

	// running with attempts and token.
	if kind, got, n, ok := parseInvocationState("running:1757000000000:2:ab12"); !ok || kind != kindRunning || !got.Equal(dl) || n != 2 {
		t.Fatalf("running#2 = kind %v dl %v n %d ok %v", kind, got, n, ok)
	}

	// next_attempt_at with attempts and token.
	if kind, got, n, ok := parseInvocationState("next_attempt_at:1757000000000:4:cd34"); !ok || kind != kindNextAttempt || !got.Equal(dl) || n != 4 {
		t.Fatalf("next_attempt_at#4 = kind %v dl %v n %d ok %v", kind, got, n, ok)
	}

	// exhausted.
	if kind, _, n, ok := parseInvocationState("exhausted:5:ef56"); !ok || kind != kindExhausted || n != 5 {
		t.Fatalf("exhausted:5 = kind %v n %d ok %v", kind, n, ok)
	}

	// exhausted with the persisted-DLQ suffix parses identically (same kind and
	// attempt count); the suffix records only DLQ persistence, not a distinct
	// lifecycle state.
	if kind, _, n, ok := parseInvocationState("exhausted:5:ef56:dlq"); !ok || kind != kindExhausted || n != 5 {
		t.Fatalf("exhausted:5:dlq = kind %v n %d ok %v", kind, n, ok)
	}
	if !isExhaustedDLQValue("exhausted:5:ef56:dlq") {
		t.Fatalf("isExhaustedDLQValue(exhausted:5:ef56:dlq) = false, want true")
	}
	if isExhaustedDLQValue("exhausted:5:ef56") {
		t.Fatalf("isExhaustedDLQValue(exhausted:5:ef56) = true, want false")
	}

	// Unparseable values → eligible (ok=false). A marker missing the mandatory
	// "<attempt>:<token>" part also does not parse.
	for _, v := range []string{
		"",
		"running:",
		"running:notanumber",
		"running:1757000000000",
		"running:1757000000000:",
		"running:1757000000000:2",
		"running:1757000000000:2:",
		"running:1757000000000:0:ab12",
		"running:1757000000000:2:NOTHEX",
		"running:#2:ab12",
		"running:01757000000000:2:ab12", // non-canonical deadline (leading zero)
		"next_attempt_at:",
		"next_attempt_at:notanumber",
		"next_attempt_at:1757000000000",
		"next_attempt_at:1757000000000:2",
		"next_attempt_at:01757000000000:2:ab12", // non-canonical deadline
		"exhausted:",
		"exhausted:0:ab12",
		"exhausted:5",
		"exhausted:abc:ab12",
		"exhausted:5:",
		"exhausted:05:ab12", // non-canonical attempt (leading zero)
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
	dl := time.UnixMilli(1757000000000)
	if v := nextAttemptValue(dl, InvocationClaim{Attempt: 2, Token: "ab12"}); v != "next_attempt_at:1757000000000:2:ab12" {
		t.Fatalf("nextAttemptValue = %q", v)
	}
	claim := InvocationClaim{Attempt: 5, Token: "ef56"}
	if v := exhaustedValue(claim, false); v != "exhausted:5:ef56" {
		t.Fatalf("exhaustedValue(5, false) = %q, want exhausted:5:ef56", v)
	}
	if v := exhaustedValue(claim, true); v != "exhausted:5:ef56:dlq" {
		t.Fatalf("exhaustedValue(5, true) = %q, want exhausted:5:ef56:dlq", v)
	}
}

// TestRetentionTTLMillis pins the terminal retention TTL conversion: the
// retention window is expressed in integer milliseconds for the PEXPIRE the
// retain script applies after a message leaves the PEL.
func TestRetentionTTLMillis(t *testing.T) {
	if got := retentionTTLMillis(); got != int64(invocationRetentionTTL/time.Millisecond) {
		t.Fatalf("retentionTTLMillis() = %d, want %d", got, int64(invocationRetentionTTL/time.Millisecond))
	}
	if invocationRetentionTTL <= 0 {
		t.Fatalf("invocationRetentionTTL = %s, want a positive retention window", invocationRetentionTTL)
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

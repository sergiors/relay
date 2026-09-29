package stream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// activeToken extracts the claim token from a well-formed active marker
// ("running:<dl>:<n>:<token>" or "next_attempt_at:<dl>:<n>:<token>"). It is the
// in-memory fake's CAS key, mirroring the scripts' token comparison. It returns
// "" for a non-active marker.
func activeToken(v string) string {
	switch {
	case strings.HasPrefix(v, "running:"):
		_, claim, ok := parseActiveValue(v[len("running:"):])
		if ok {
			return claim.Token
		}
	case strings.HasPrefix(v, "next_attempt_at:"):
		_, claim, ok := parseActiveValue(v[len("next_attempt_at:"):])
		if ok {
			return claim.Token
		}
	}
	return ""
}

// fakeInvocationStore is an in-memory invocationStateStore that models the
// documented value grammar of the Redis-backed invocationStore (invocation.go).
// It exists so the consumer's invocation-state seam (newConsumer) and the
// InvocationState wrapper can be exercised without Redis; the Redis-backed store
// itself is covered by the integration suite. It models the per-claim
// (attempt + token) compare-and-set identity, generating unique tokens, and the
// message-level terminal-retention marker (terminalField) that makes every
// lifecycle transition inert once the message left the PEL.
type fakeInvocationStore struct {
	fields   map[string]string
	readErr  error // when set, every read fails (fail-open paths)
	tokenSeq int
	// retained tracks the message-level terminal-retention state. It mirrors the
	// reserved terminalField being present in fields; a separate bool keeps the
	// "repeated retain is monotonic" assertion easy.
	retained bool
	// retainCalls counts retainTerminal invocations, so tests can assert the
	// post-ACK retention ran exactly once per message.
	retainCalls int
	// recoverableCalls counts makeRecoverable invocations for non-terminal
	// hashes, so tests can assert the legacy-TTL migration ran.
	recoverableCalls int
	// recoverableErr, when set, makes makeRecoverable fail like an unreachable
	// Redis: the delivery must then leave the message pending (no handler, no
	// ACK/DLQ) because the state hash may still carry its old TTL.
	recoverableErr error
}

// terminalRetained reports whether the message-level terminal marker is present,
// mirroring the scripts' HEXISTS guard.
func (f *fakeInvocationStore) terminalRetained() bool {
	return f.retained
}

func newFakeInvocationStore(values map[string]string) *fakeInvocationStore {
	m := make(map[string]string, len(values))
	for k, v := range values {
		m[k] = v
	}
	return &fakeInvocationStore{fields: m}
}

// nextToken returns a fresh unique token, mirroring the crypto-random hex claim
// token an EVAL would be supplied with. It is lowercase hex (not an arbitrary
// string) so it round-trips through activeToken/parseInvocationState exactly like
// a real token; a non-hex token would make every CAS parse fail.
func (f *fakeInvocationStore) nextToken() string {
	f.tokenSeq++
	return fmt.Sprintf("aabb%04d", f.tokenSeq)
}

func (f *fakeInvocationStore) completed(_ context.Context, _, _, _, invocation string) (bool, error) {
	if f.readErr != nil {
		return false, f.readErr
	}
	return f.fields[invocation] == "ok", nil
}

func (f *fakeInvocationStore) terminal(_ context.Context, _, _, _, invocation string) (bool, error) {
	if f.readErr != nil {
		return false, f.readErr
	}
	kind, _, _, ok := parseInvocationState(f.fields[invocation])
	if !ok {
		return false, nil
	}
	return kind == kindComplete || kind == kindExhausted, nil
}

// markComplete models markCompleteScript: it CASes the claim (attempt + token)
// against the current active marker, treats an already-"ok" marker as an
// idempotent success, and preserves an exhausted marker.
func (f *fakeInvocationStore) markComplete(_ context.Context, _, _, _, invocation string, claim InvocationClaim) (bool, error) {
	if f.terminalRetained() {
		return false, nil
	}
	v, ok := f.fields[invocation]
	if !ok {
		return false, nil
	}
	if v == "ok" {
		return true, nil
	}
	kind, _, active, parsed := parseInvocationState(v)
	if !parsed || (kind != kindRunning && kind != kindNextAttempt) {
		return false, nil
	}
	if active != claim.Attempt || activeToken(v) != claim.Token {
		return false, nil
	}
	f.fields[invocation] = "ok"
	return true, nil
}

func (f *fakeInvocationStore) tryStart(
	_ context.Context,
	_, _, _, invocation string,
	now, deadline time.Time,
) (bool, InvocationClaim, time.Duration, error) {
	if f.readErr != nil {
		return false, InvocationClaim{}, 0, f.readErr
	}
	if f.terminalRetained() {
		return false, InvocationClaim{}, 0, nil
	}
	attempt := 0
	if v, ok := f.fields[invocation]; ok {
		kind, dl, n, parsed := parseInvocationState(v)
		if parsed {
			switch kind {
			case kindComplete:
				return false, InvocationClaim{}, 0, nil
			case kindExhausted:
				return false, InvocationClaim{Attempt: n}, 0, nil
			case kindRunning, kindNextAttempt:
				if now.Before(dl) {
					return false, InvocationClaim{Attempt: n}, dl.Sub(now), nil
				}
				attempt = n // deadline elapsed: eligible, carry the attempt forward
			}
		}
	}
	attempt++
	token := f.nextToken()
	claim := InvocationClaim{Attempt: attempt, Token: token}
	f.fields[invocation] = runningValue(deadline, claim)
	return true, claim, 0, nil
}

func (f *fakeInvocationStore) finishFailure(
	_ context.Context,
	_, _, _, invocation string,
	claim InvocationClaim,
	backoff time.Duration,
	now time.Time,
) (bool, error) {
	if f.readErr != nil {
		return false, f.readErr
	}
	if f.terminalRetained() {
		return false, nil
	}
	v := f.fields[invocation]
	kind, _, active, ok := parseInvocationState(v)
	if !ok {
		return false, nil
	}
	if kind != kindRunning && kind != kindNextAttempt {
		// Terminal (complete/exhausted): a stale failure is refused.
		return false, nil
	}
	if active != claim.Attempt || activeToken(v) != claim.Token {
		// Superseded by a newer claim: refused.
		return false, nil
	}
	f.fields[invocation] = nextAttemptValue(now.Add(backoff), claim)
	return true, nil
}

func (f *fakeInvocationStore) markExhausted(_ context.Context, _, _, _, invocation string, claim InvocationClaim) (bool, error) {
	return f.writeExhausted(invocation, claim), nil
}

// writeExhausted models markExhaustedScript: it CASes the claim against the
// current active marker and writes "exhausted:<attempt>:<token>".
func (f *fakeInvocationStore) writeExhausted(invocation string, claim InvocationClaim) bool {
	if f.terminalRetained() {
		return false
	}
	v, ok := f.fields[invocation]
	if !ok {
		return false
	}
	kind, _, active, parsed := parseInvocationState(v)
	if !parsed || (kind != kindRunning && kind != kindNextAttempt) {
		return false
	}
	if active != claim.Attempt || activeToken(v) != claim.Token {
		return false
	}
	f.fields[invocation] = exhaustedValue(claim, false)
	return true
}

// markExhaustedDLQ models markExhaustedDLQScript: it upgrades an existing
// exhausted marker to the ":dlq" form only when its retained identity matches
// the exhausted claim; an existing ":dlq" is monotonic.
func (f *fakeInvocationStore) markExhaustedDLQ(_ context.Context, _, _, _, invocation string, claim InvocationClaim) (bool, error) {
	if f.terminalRetained() {
		return false, nil
	}
	v, ok := f.fields[invocation]
	if !ok {
		return false, nil
	}
	exClaim, dlq, parsed := parseExhaustedValue(v)
	if !parsed {
		return false, nil
	}
	if exClaim.Attempt != claim.Attempt || exClaim.Token != claim.Token {
		return false, nil
	}
	if dlq {
		return true, nil
	}
	f.fields[invocation] = exhaustedValue(exClaim, true)
	return true, nil
}

func (f *fakeInvocationStore) exhaustedState(_ context.Context, _, _, _, invocation string) (InvocationClaim, bool, bool, error) {
	if f.readErr != nil {
		return InvocationClaim{}, false, false, f.readErr
	}
	claim, dlq, ok := parseExhaustedValue(f.fields[invocation])
	return claim, dlq, ok, nil
}

// claimClassification models the Redis HSETNX claim: the first call sets the
// reserved field and returns true; every later call returns false. A read error
// is surfaced so the fail-closed classification path is exercisable. Once the
// hash is terminal-retained the claim always reports "already taken" (mirroring
// the script's HEXISTS guard), so a stale in-memory delivery cannot re-count.
func (f *fakeInvocationStore) claimClassification(_ context.Context, _, _, _ string) (bool, error) {
	if f.readErr != nil {
		return false, f.readErr
	}
	if f.terminalRetained() {
		return false, nil
	}
	if _, ok := f.fields[classificationField]; ok {
		return false, nil
	}
	f.fields[classificationField] = "1"
	return true, nil
}

// traceReference/recordTrace model the reserved sibling trace field: recording
// never disturbs the invocation's lifecycle value, and reading an absent field
// is ("", nil). A read error is surfaced so the best-effort no-link path is
// exercisable. A terminal-retained hash ignores records (mirroring the script's
// HEXISTS guard).
func (f *fakeInvocationStore) traceReference(_ context.Context, _, _, _, invocation string) (string, error) {
	if f.readErr != nil {
		return "", f.readErr
	}
	return f.fields[traceField(invocation)], nil
}

func (f *fakeInvocationStore) recordTrace(_ context.Context, _, _, _, invocation, lineage string) error {
	if lineage == "" || f.terminalRetained() {
		return nil
	}
	f.fields[traceField(invocation)] = lineage
	return nil
}

// retainTerminal models retainTerminalScript: the message-level terminal marker
// is set (monotonically) and the message is considered no longer recoverable.
// Repeated calls are no-ops; a hash that never existed is not created.
func (f *fakeInvocationStore) retainTerminal(_ context.Context, _, _, _ string) error {
	if len(f.fields) == 0 && !f.retained {
		// No hash to retain (mirrors the script's EXISTS guard).
		return nil
	}
	f.retainCalls++
	f.retained = true
	return nil
}

// makeRecoverable models makeRecoverableScript: it marks a pre-existing,
// non-terminal hash recoverable. The in-memory fake does not model TTLs, so it
// only records that the migration was requested; it never clears terminal
// retention. It is a no-op for a missing hash. When recoverableErr is set it
// fails like an unreachable Redis, so the caller's fail-closed path (leave the
// message pending, do not dispatch) can be exercised.
func (f *fakeInvocationStore) makeRecoverable(_ context.Context, _, _, _ string) error {
	if f.recoverableErr != nil {
		return f.recoverableErr
	}
	if len(f.fields) == 0 || f.retained {
		return nil
	}
	f.recoverableCalls++
	return nil
}

// consumerForStore builds a Consumer wired to store via the newConsumer seam,
// then returns the InvocationState handle exactly as the delivery path does.
func consumerForStore(t *testing.T, store invocationStateStore) InvocationState {
	t.Helper()
	c := newConsumer(ConsumerConfig{
		Stream: "s", Group: "g", Consumer: "c",
		Log: slog.New(slog.DiscardHandler),
	}, store)
	return NewInvocationState(context.Background(), c.invStateStore, "s", "g", "m-0", c.log)
}

// TestInvocationTryStartEligibilityMatrix drives each row of the tryStart
// eligibility matrix through the consumer's invocation-state seam: a complete or
// exhausted field is terminal (started=false, wait=0), an in-flight running or
// next_attempt_at marker is protected until its deadline (started=false,
// wait>0), and an absent marker, one whose deadline has elapsed, or an
// unparseable field is eligible (started=true).
func TestInvocationTryStartEligibilityMatrix(t *testing.T) {
	// The eligibility decisions below use the real clock (NewInvocationState
	// defaults now to time.Now), so deadlines are computed from time.Now.
	now := time.Now()
	timeout := time.Hour

	tests := []struct {
		name        string
		field       string
		wantStarted bool
		wantAttempt int
		wantWaitPos bool // true => wait must be > 0
	}{
		{"complete is terminal", "ok", false, 0, false},
		{"exhausted is terminal", exhaustedValue(InvocationClaim{Attempt: 5, Token: "ab"}, false), false, 5, false},
		{"exhausted+dlq is terminal", exhaustedValue(InvocationClaim{Attempt: 5, Token: "ab"}, true), false, 5, false},
		{"unparseable is eligible", "running:notanumber", true, 1, false},
		{"absent is eligible", "", true, 1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := map[string]string{}
			if tt.field != "" {
				values["fn/h"] = tt.field
			}
			p := consumerForStore(t, newFakeInvocationStore(values))
			started, claim, wait, err := p.TryStart("fn/h", timeout)
			if err != nil {
				t.Fatalf("TryStart error = %v, want nil", err)
			}
			if started != tt.wantStarted {
				t.Fatalf("started = %v, want %v", started, tt.wantStarted)
			}
			if claim.Attempt != tt.wantAttempt {
				t.Fatalf("attempt = %d, want %d", claim.Attempt, tt.wantAttempt)
			}
			if (wait > 0) != tt.wantWaitPos {
				t.Fatalf("wait = %s, want positive=%v", wait, tt.wantWaitPos)
			}
		})
	}

	t.Run("running marker protects until deadline", func(t *testing.T) {
		deadline := now.Add(30 * time.Minute)
		p := consumerForStore(t, newFakeInvocationStore(map[string]string{"fn/h": runningValue(deadline, InvocationClaim{Attempt: 2, Token: "ab"})}))
		started, claim, wait, err := p.TryStart("fn/h", timeout)
		if err != nil {
			t.Fatalf("TryStart error = %v, want nil", err)
		}
		if started {
			t.Fatal("started = true for a protected running marker, want false")
		}
		if claim.Attempt != 2 {
			t.Fatalf("attempt = %d, want 2", claim.Attempt)
		}
		if wait <= 0 {
			t.Fatalf("wait = %s, want > 0 (protected until %s)", wait, deadline)
		}
	})

	t.Run("next_attempt_at marker protects during backoff", func(t *testing.T) {
		deadline := now.Add(5 * time.Minute)
		p := consumerForStore(t, newFakeInvocationStore(map[string]string{"fn/h": nextAttemptValue(deadline, InvocationClaim{Attempt: 3, Token: "ab"})}))
		started, claim, wait, err := p.TryStart("fn/h", timeout)
		if err != nil {
			t.Fatalf("TryStart error = %v, want nil", err)
		}
		if started {
			t.Fatal("started = true while waiting out a retry backoff, want false")
		}
		if claim.Attempt != 3 {
			t.Fatalf("attempt = %d, want 3", claim.Attempt)
		}
		if wait <= 0 {
			t.Fatalf("wait = %s, want > 0", wait)
		}
	})
}

// TestInvocationTryStartElapsedDeadlineIsEligible pins the eligible half of the
// matrix: a running/next_attempt_at marker whose deadline has passed is eligible,
// and the new attempt carries the elapsed marker's attempt count forward (n+1).
func TestInvocationTryStartElapsedDeadlineIsEligible(t *testing.T) {
	now := time.Now()
	// A deadline well in the past.
	elapsed := now.Add(-time.Hour)
	for name, field := range map[string]string{
		"running":         runningValue(elapsed, InvocationClaim{Attempt: 2, Token: "ab"}),
		"next_attempt_at": nextAttemptValue(elapsed, InvocationClaim{Attempt: 4, Token: "cd"}),
	} {
		t.Run(name, func(t *testing.T) {
			store := newFakeInvocationStore(map[string]string{"fn/h": field})
			p := consumerForStore(t, store)
			started, claim, wait, err := p.TryStart("fn/h", time.Hour)
			if err != nil {
				t.Fatalf("TryStart error = %v, want nil", err)
			}
			if !started {
				t.Fatal("started = false for a marker past its deadline, want true (eligible)")
			}
			if wait != 0 {
				t.Fatalf("wait = %s, want 0 for a started invocation", wait)
			}
			// The elapsed marker's attempt count (2 or 4) advances to n+1.
			wantAttempt := 3
			if name == "next_attempt_at" {
				wantAttempt = 5
			}
			if claim.Attempt != wantAttempt {
				t.Fatalf("attempt = %d, want %d (carried from the elapsed marker)", claim.Attempt, wantAttempt)
			}
		})
	}
}

// TestInvocationRecordFailureRequiresConfirmedClaim pins that RecordFailure is
// claim-gated: a failure transition with no matching active claim (an absent
// marker, or a zero claim) writes nothing. Every active-claim-originated
// transition needs the exact confirmed claim, so a failure can never fabricate
// a marker out of nothing.
func TestInvocationRecordFailureRequiresConfirmedClaim(t *testing.T) {
	store := newFakeInvocationStore(nil)
	p := consumerForStore(t, store)

	// A zero claim against an absent marker is refused.
	if p.RecordFailure("fn/h", InvocationClaim{}, time.Minute) {
		t.Fatal("RecordFailure with a zero claim on an absent marker = true, want false")
	}
	if got := store.fields["fn/h"]; got != "" {
		t.Fatalf("marker = %q, want none (a claimless failure must not write)", got)
	}

	// A well-formed claim whose token does not own any marker is refused too.
	if p.RecordFailure("fn/h", InvocationClaim{Attempt: 1, Token: "deadbeef"}, time.Minute) {
		t.Fatal("RecordFailure with an unmatched claim = true, want false")
	}
	if got := store.fields["fn/h"]; got != "" {
		t.Fatalf("marker = %q, want none (an unmatched claim must not write)", got)
	}
}

// TestInvocationRecordFailureStaleGuard pins the claim CAS through the
// production RecordFailure wrapper: a stale owner (a token other than the one
// currently owning the active marker) does not overwrite the newer marker, while
// the current owner does.
func TestInvocationRecordFailureStaleGuard(t *testing.T) {
	// The current marker is attempt 2, owned by token "aa".
	deadline := time.Now().Add(time.Hour)
	current := InvocationClaim{Attempt: 2, Token: "aa"}
	store := newFakeInvocationStore(map[string]string{"fn/h": runningValue(deadline, current)})
	p := consumerForStore(t, store)

	// A stale attempt-2 claim with a different token is refused.
	if p.RecordFailure("fn/h", InvocationClaim{Attempt: 2, Token: "bb"}, time.Minute) {
		t.Fatal("stale RecordFailure (same attempt, different token) = true, want false")
	}
	if got := store.fields["fn/h"]; got != runningValue(deadline, current) {
		t.Fatalf("stale RecordFailure changed marker to %q, want the running marker", got)
	}
	// The current claim is recorded.
	if !p.RecordFailure("fn/h", current, time.Minute) {
		t.Fatal("current RecordFailure = false, want true")
	}
	kind, _, attempts, ok := parseInvocationState(store.fields["fn/h"])
	if !ok || kind != kindNextAttempt || attempts != 2 {
		t.Fatalf("current RecordFailure marker = %q, want next_attempt_at attempt 2", store.fields["fn/h"])
	}
}

// TestInvocationStoreTerminalGuards pins the store's terminal/monotonic
// semantics that mirror the Lua scripts: success is CASed on the claim and never
// downgrades an exhausted marker, a stale failure never displaces "ok", and the
// DLQ suffix upgrade is CASed on the exhausted identity and is monotonic.
func TestInvocationStoreTerminalGuards(t *testing.T) {
	store := newFakeInvocationStore(nil)
	ctx := context.Background()

	// A running claim, then exhaustion: success on the same claim must not
	// downgrade the exhausted marker.
	claim := InvocationClaim{Attempt: 3, Token: "aa"}
	store.fields["fn/h"] = runningValue(time.Now().Add(time.Hour), claim)
	if ok, err := store.markExhausted(ctx, "s", "g", "m", "fn/h", claim); err != nil || !ok {
		t.Fatalf("markExhausted = (%v,%v), want (true,nil)", ok, err)
	}
	if ok, err := store.markComplete(ctx, "s", "g", "m", "fn/h", claim); err != nil || ok {
		t.Fatalf("markComplete on exhausted = (%v,%v), want (false,nil)", ok, err)
	}
	if got := store.fields["fn/h"]; got != exhaustedValue(claim, false) {
		t.Fatalf("marker = %q, want exhausted (success must not downgrade)", got)
	}

	// A stale failure must not overwrite "ok".
	store2 := newFakeInvocationStore(map[string]string{"fn/h": "ok"})
	if ok, err := store2.finishFailure(ctx, "s", "g", "m", "fn/h", InvocationClaim{Attempt: 1, Token: "aa"}, time.Minute, time.Now()); err != nil || ok {
		t.Fatalf("finishFailure on ok = (%v,%v), want (false,nil)", ok, err)
	}
	if got := store2.fields["fn/h"]; got != "ok" {
		t.Fatalf("marker = %q, want ok (stale failure must not overwrite success)", got)
	}

	// DLQ suffix is monotonic and CASed on the exhausted identity.
	store3 := newFakeInvocationStore(nil)
	exClaim := InvocationClaim{Attempt: 3, Token: "aa"}
	store3.fields["fn/h"] = runningValue(time.Now().Add(time.Hour), exClaim)
	if ok, err := store3.markExhausted(ctx, "s", "g", "m", "fn/h", exClaim); err != nil || !ok {
		t.Fatalf("markExhausted = (%v,%v)", ok, err)
	}
	// A foreign identity cannot upgrade the marker.
	if ok, err := store3.markExhaustedDLQ(ctx, "s", "g", "m", "fn/h", InvocationClaim{Attempt: 3, Token: "bb"}); err != nil || ok {
		t.Fatalf("markExhaustedDLQ with a foreign identity = (%v,%v), want (false,nil)", ok, err)
	}
	if got := store3.fields["fn/h"]; got != exhaustedValue(exClaim, false) {
		t.Fatalf("marker = %q, want the un-upgraded exhausted marker", got)
	}
	// The owning identity upgrades it, and the suffix is monotonic.
	if ok, err := store3.markExhaustedDLQ(ctx, "s", "g", "m", "fn/h", exClaim); err != nil || !ok {
		t.Fatalf("markExhaustedDLQ = (%v,%v), want (true,nil)", ok, err)
	}
	if ok, err := store3.markExhausted(ctx, "s", "g", "m", "fn/h", exClaim); err != nil || ok {
		t.Fatalf("re-markExhausted = (%v,%v), want (false,nil) (already exhausted)", ok, err)
	}
	if got := store3.fields["fn/h"]; got != exhaustedValue(exClaim, true) {
		t.Fatalf("marker = %q, want exhausted...:dlq (suffix preserved)", got)
	}
}

// TestInvocationStateStoreErrorContract pins the error-handling contract under
// a store error. TryStart does NOT fail open: it returns the error (the claim
// outcome is unknown) so the runner leaves the message pending and never
// executes the handler on an ambiguous claim. IsComplete/IsTerminal fail closed
// to false, and RecordFailure is a no-op that returns false.
func TestInvocationStateStoreErrorContract(t *testing.T) {
	boom := errors.New("redis down")
	store := &fakeInvocationStore{fields: map[string]string{"fn/h": "ok"}, readErr: boom}
	p := consumerForStore(t, store)

	if started, claim, wait, err := p.TryStart("fn/h", time.Hour); err == nil || started || claim.Attempt != 0 || wait != 0 {
		t.Fatalf("TryStart on store error = (%v, %+v, %s, %v), want (false, zero, 0, err)", started, claim, wait, err)
	}
	if p.IsComplete("fn/h") {
		t.Fatal("IsComplete on read error = true, want false (fail closed)")
	}
	if p.IsTerminal("fn/h") {
		t.Fatal("IsTerminal on read error = true, want false (fail closed)")
	}
	// RecordFailure on a read error must not panic and must leave the field.
	if p.RecordFailure("fn/h", InvocationClaim{Attempt: 1, Token: "aa"}, time.Second) {
		t.Fatal("RecordFailure on a read error = true, want false")
	}
	if store.fields["fn/h"] != "ok" {
		t.Fatalf("field mutated on RecordFailure read error: %q", store.fields["fn/h"])
	}
}

// TestInvocationClaimClassificationOnce pins the once-per-message classification
// claim through the production seam: the first delivery wins (true), every later
// delivery of the same message loses (false), and the reserved field is set. It
// is the invariant that makes events_received == events_matched +
// events_unmatched hold across redeliveries.
func TestInvocationClaimClassificationOnce(t *testing.T) {
	store := newFakeInvocationStore(nil)
	p := consumerForStore(t, store)

	claimed, err := p.ClaimClassification()
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if !claimed {
		t.Fatal("first ClaimClassification = false, want true")
	}
	if store.fields[classificationField] != "1" {
		t.Fatalf("claim field = %q, want \"1\"", store.fields[classificationField])
	}

	for i := 0; i < 3; i++ {
		claimed, err := p.ClaimClassification()
		if err != nil {
			t.Fatalf("later claim %d: %v", i, err)
		}
		if claimed {
			t.Fatalf("later claim %d = true, want false (already claimed)", i)
		}
	}
}

// TestInvocationClaimClassificationFailsClosed pins that a store error is NOT
// swallowed as a claim: (false, err) is returned so the caller counts nothing,
// avoiding a double count on redelivery.
func TestInvocationClaimClassificationFailsClosed(t *testing.T) {
	boom := errors.New("redis down")
	store := &fakeInvocationStore{fields: map[string]string{}, readErr: boom}
	p := consumerForStore(t, store)

	claimed, err := p.ClaimClassification()
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("ClaimClassification error = %v, want the store error", err)
	}
	if claimed {
		t.Fatal("ClaimClassification on store error = true, want false")
	}
}

// TestInvocationTerminalRetentionGuardsStaleWrites pins the message-level
// terminal-retention guard end to end through the store seam: once the message
// left the PEL and was retained, a stale in-memory delivery's transitions are all
// inert, so retained state can neither be mutated nor re-counted.
func TestInvocationTerminalRetentionGuardsStaleWrites(t *testing.T) {
	ctx := context.Background()
	store := newFakeInvocationStore(nil)
	p := consumerForStore(t, store)

	started, claim, _, err := p.TryStart("fn/h", time.Hour)
	if err != nil || !started {
		t.Fatalf("TryStart = (%v,%+v,%v)", started, claim, err)
	}
	if !p.MarkComplete("fn/h", claim) {
		t.Fatal("MarkComplete = false, want true")
	}
	if store.fields["fn/h"] != "ok" {
		t.Fatalf("marker = %q, want ok", store.fields["fn/h"])
	}

	// The message leaves the PEL: retain.
	if err := store.retainTerminal(ctx, "s", "g", "m-0"); err != nil {
		t.Fatalf("retainTerminal: %v", err)
	}
	if !store.terminalRetained() {
		t.Fatal("store not marked terminal-retained after retain")
	}

	// Every stale transition is now inert: no mutation, no counter change.
	if s, _, _, err := p.TryStart("fn/h", time.Hour); err != nil || s {
		t.Fatalf("TryStart after retain = (%v,%v), want (false,nil)", s, err)
	}
	if p.MarkComplete("fn/h", claim) {
		t.Fatal("MarkComplete after retain = true, want false")
	}
	if p.RecordFailure("fn/h", claim, time.Minute) {
		t.Fatal("RecordFailure after retain = true, want false")
	}
	if p.MarkExhausted("fn/h", claim) {
		t.Fatal("MarkExhausted after retain = true, want false")
	}
	if got := store.fields["fn/h"]; got != "ok" {
		t.Fatalf("marker changed by a stale transition: %q, want ok", got)
	}

	// Classification cannot be re-claimed once retained (would double-count).
	if claimed, err := p.ClaimClassification(); err != nil || claimed {
		t.Fatalf("ClaimClassification after retain = (%v,%v), want (false,nil)", claimed, err)
	}
	// Trace recording is likewise inert once retained.
	p.RecordTrace("fn/h", "00-trace-span-01")
	if got := store.fields[traceField("fn/h")]; got != "" {
		t.Fatalf("trace recorded on a terminal-retained hash: %q", got)
	}
}

// TestInvocationRetentionIsMonotonicAndNoopWhenAbsent pins the retention
// contract through the store seam: retaining an absent hash is a no-op (no empty
// key is created), and a repeated retain is monotonic — it does not clear the
// terminal marker or otherwise change state.
func TestInvocationRetentionIsMonotonicAndNoopWhenAbsent(t *testing.T) {
	ctx := context.Background()
	store := &fakeInvocationStore{fields: map[string]string{}}

	// Absent hash: no-op.
	if err := store.retainTerminal(ctx, "s", "g", "missing"); err != nil {
		t.Fatalf("retainTerminal(absent): %v", err)
	}
	if store.terminalRetained() {
		t.Fatal("retainTerminal created retention for an absent hash")
	}

	// Present hash: retain, then a second retain keeps it retained.
	store.fields["fn/h"] = "ok"
	if err := store.retainTerminal(ctx, "s", "g", "m-0"); err != nil {
		t.Fatalf("retainTerminal: %v", err)
	}
	if err := store.retainTerminal(ctx, "s", "g", "m-0"); err != nil {
		t.Fatalf("second retainTerminal: %v", err)
	}
	if !store.terminalRetained() {
		t.Fatal("terminal retention was cleared by a repeated retain")
	}
	if got := store.retainCalls; got != 2 {
		t.Fatalf("retainCalls = %d, want 2", got)
	}
}

// TestInvocationMakeRecoverableMigratesNonTerminalOnly pins the legacy-TTL
// migration seam: makeRecoverable is a no-op for an absent hash and for an
// already terminal-retained hash (it must never remove terminal retention), and
// it records a migration for a live, non-terminal hash.
func TestInvocationMakeRecoverableMigratesNonTerminalOnly(t *testing.T) {
	ctx := context.Background()
	store := newFakeInvocationStore(nil)

	// Absent: no-op.
	if err := store.makeRecoverable(ctx, "s", "g", "m-0"); err != nil {
		t.Fatalf("makeRecoverable(absent): %v", err)
	}
	if store.recoverableCalls != 0 {
		t.Fatalf("recoverableCalls = %d for an absent hash, want 0", store.recoverableCalls)
	}

	// Live, non-terminal hash: migrated.
	store.fields["fn/h"] = "ok"
	if err := store.makeRecoverable(ctx, "s", "g", "m-0"); err != nil {
		t.Fatalf("makeRecoverable: %v", err)
	}
	if store.recoverableCalls != 1 {
		t.Fatalf("recoverableCalls = %d, want 1", store.recoverableCalls)
	}

	// Terminal-retained: never migrated again.
	if err := store.retainTerminal(ctx, "s", "g", "m-0"); err != nil {
		t.Fatalf("retainTerminal: %v", err)
	}
	if err := store.makeRecoverable(ctx, "s", "g", "m-0"); err != nil {
		t.Fatalf("makeRecoverable(retained): %v", err)
	}
	if store.recoverableCalls != 1 {
		t.Fatalf("recoverableCalls = %d after a retained hash, want 1 (never migrated)", store.recoverableCalls)
	}
	if !store.terminalRetained() {
		t.Fatal("makeRecoverable cleared terminal retention")
	}
}

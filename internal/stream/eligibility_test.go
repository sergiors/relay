package stream

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// fakeInvocationStore is an in-memory invocationStateStore that models the
// documented value grammar of the Redis-backed invocationStore (invocation.go).
// It exists so the consumer's invocation-state seam (newConsumer) and the
// InvocationState wrapper can be exercised without Redis; the Redis-backed store
// itself is covered by the integration suite.
type fakeInvocationStore struct {
	fields  map[string]string
	readErr error // when set, every read fails (fail-open paths)
}

func newFakeInvocationStore(values map[string]string) *fakeInvocationStore {
	m := make(map[string]string, len(values))
	for k, v := range values {
		m[k] = v
	}
	return &fakeInvocationStore{fields: m}
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

func (f *fakeInvocationStore) markComplete(_ context.Context, _, _, _, invocation string) error {
	f.fields[invocation] = "ok"
	return nil
}

func (f *fakeInvocationStore) tryStart(
	_ context.Context,
	_, _, _, invocation string,
	now, deadline time.Time,
) (bool, int, time.Duration, error) {
	if f.readErr != nil {
		return false, 0, 0, f.readErr
	}
	attempt := 0
	if v, ok := f.fields[invocation]; ok {
		kind, dl, n, parsed := parseInvocationState(v)
		if parsed {
			switch kind {
			case kindComplete:
				return false, 0, 0, nil
			case kindExhausted:
				return false, n, 0, nil
			case kindRunning, kindNextAttempt:
				if now.Add(-clockSkewTolerance).Before(dl) {
					return false, n, dl.Sub(now), nil
				}
				attempt = n // expired: eligible, carry the attempt forward
			}
		}
	}
	attempt++
	f.fields[invocation] = runningValue(deadline, attempt)
	return true, attempt, 0, nil
}

func (f *fakeInvocationStore) finishFailure(
	_ context.Context,
	_, _, _, invocation string,
	backoff time.Duration,
	now time.Time,
) (time.Time, error) {
	if f.readErr != nil {
		return time.Time{}, f.readErr
	}
	v := f.fields[invocation]
	attempts := 1
	if kind, _, n, ok := parseInvocationState(v); ok && kind != kindComplete {
		attempts = n
	}
	next := now.Add(backoff)
	f.fields[invocation] = nextAttemptValue(next, attempts)
	return next, nil
}

func (f *fakeInvocationStore) markExhausted(_ context.Context, _, _, _, invocation string, attempts int) error {
	f.fields[invocation] = exhaustedValue(attempts, false)
	return nil
}

func (f *fakeInvocationStore) markExhaustedDLQ(_ context.Context, _, _, _, invocation string, attempts int) error {
	f.fields[invocation] = exhaustedValue(attempts, true)
	return nil
}

func (f *fakeInvocationStore) exhaustedPersisted(_ context.Context, _, _, _, invocation string) (bool, error) {
	if f.readErr != nil {
		return false, f.readErr
	}
	return isExhaustedDLQValue(f.fields[invocation]), nil
}

// claimClassification models the Redis HSETNX claim: the first call sets the
// reserved field and returns true; every later call returns false. A read error
// is surfaced so the fail-closed classification path is exercisable.
func (f *fakeInvocationStore) claimClassification(_ context.Context, _, _, _ string) (bool, error) {
	if f.readErr != nil {
		return false, f.readErr
	}
	if _, ok := f.fields[classificationField]; ok {
		return false, nil
	}
	f.fields[classificationField] = "1"
	return true, nil
}

func (f *fakeInvocationStore) clear(_ context.Context, _, _, _ string) error {
	f.fields = map[string]string{}
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
// wait>0), and an absent, expired, or unparseable field is eligible
// (started=true).
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
		{"exhausted is terminal", exhaustedValue(5, false), false, 5, false},
		{"exhausted+dlq is terminal", exhaustedValue(5, true), false, 5, false},
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
			started, attempt, wait := p.TryStart("fn/h", timeout)
			if started != tt.wantStarted {
				t.Fatalf("started = %v, want %v", started, tt.wantStarted)
			}
			if attempt != tt.wantAttempt {
				t.Fatalf("attempt = %d, want %d", attempt, tt.wantAttempt)
			}
			if (wait > 0) != tt.wantWaitPos {
				t.Fatalf("wait = %s, want positive=%v", wait, tt.wantWaitPos)
			}
		})
	}

	t.Run("running marker protects until deadline", func(t *testing.T) {
		deadline := now.Add(30 * time.Minute)
		p := consumerForStore(t, newFakeInvocationStore(map[string]string{"fn/h": runningValue(deadline, 2)}))
		started, attempt, wait := p.TryStart("fn/h", timeout)
		if started {
			t.Fatal("started = true for a protected running marker, want false")
		}
		if attempt != 2 {
			t.Fatalf("attempt = %d, want 2", attempt)
		}
		if wait <= 0 {
			t.Fatalf("wait = %s, want > 0 (protected until %s)", wait, deadline)
		}
	})

	t.Run("next_attempt_at marker protects during backoff", func(t *testing.T) {
		deadline := now.Add(5 * time.Minute)
		p := consumerForStore(t, newFakeInvocationStore(map[string]string{"fn/h": nextAttemptValue(deadline, 3)}))
		started, attempt, wait := p.TryStart("fn/h", timeout)
		if started {
			t.Fatal("started = true while waiting out a retry backoff, want false")
		}
		if attempt != 3 {
			t.Fatalf("attempt = %d, want 3", attempt)
		}
		if wait <= 0 {
			t.Fatalf("wait = %s, want > 0", wait)
		}
	})
}

// TestInvocationTryStartExpiredMarkerIsEligible pins the expired half of the
// matrix: a running/next_attempt_at marker whose deadline has passed is eligible,
// and the new attempt carries the expired attempt count forward (n+1).
func TestInvocationTryStartExpiredMarkerIsEligible(t *testing.T) {
	now := time.Now()
	// A deadline well in the past (beyond the clock-skew tolerance).
	expired := now.Add(-time.Hour)
	for name, field := range map[string]string{
		"running":         runningValue(expired, 2),
		"next_attempt_at": nextAttemptValue(expired, 4),
	} {
		t.Run(name, func(t *testing.T) {
			store := newFakeInvocationStore(map[string]string{"fn/h": field})
			p := consumerForStore(t, store)
			started, attempt, wait := p.TryStart("fn/h", time.Hour)
			if !started {
				t.Fatal("started = false for an expired marker, want true (eligible)")
			}
			if wait != 0 {
				t.Fatalf("wait = %s, want 0 for a started invocation", wait)
			}
			// The expired attempt count (2 or 4) advances to n+1.
			wantAttempt := 3
			if name == "next_attempt_at" {
				wantAttempt = 5
			}
			if attempt != wantAttempt {
				t.Fatalf("attempt = %d, want %d (carried from the expired marker)", attempt, wantAttempt)
			}
		})
	}
}

// TestInvocationRecordFailureMissingFieldDefaultsToAttempt1 pins the
// missing-field race branch through the production RecordFailure wrapper: when a
// failure is recorded but the invocation field is absent, the persisted
// next_attempt_at marker reports attempt 1 (and it is written at now+backoff).
func TestInvocationRecordFailureMissingFieldDefaultsToAttempt1(t *testing.T) {
	store := newFakeInvocationStore(nil)
	p := consumerForStore(t, store)

	backoff := 90 * time.Second
	p.RecordFailure("fn/h", backoff)

	got := store.fields["fn/h"]
	if !strings.HasPrefix(got, "next_attempt_at:") {
		t.Fatalf("persisted marker = %q, want a next_attempt_at marker", got)
	}
	kind, deadline, attempts, ok := parseInvocationState(got)
	if !ok || kind != kindNextAttempt {
		t.Fatalf("persisted marker %q did not parse as kindNextAttempt (ok=%v kind=%v)", got, ok, kind)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (missing field defaults to attempt 1)", attempts)
	}
	if d := deadline.Sub(time.Now()); d < backoff-time.Minute || d > backoff+time.Minute {
		t.Fatalf("deadline = %s from now, want ~%s (now+backoff)", d, backoff)
	}
}

// TestInvocationStateFailsOpenOnStoreReadError pins at-least-once behavior: a
// store read error must not block execution. TryStart fails open (started=true),
// IsComplete/IsTerminal fail closed to false, and RecordFailure is a no-op.
func TestInvocationStateFailsOpenOnStoreReadError(t *testing.T) {
	boom := errors.New("redis down")
	store := &fakeInvocationStore{fields: map[string]string{"fn/h": "ok"}, readErr: boom}
	p := consumerForStore(t, store)

	if started, attempt, wait := p.TryStart("fn/h", time.Hour); !started || attempt != 1 || wait != 0 {
		t.Fatalf("TryStart on read error = (%v, %d, %s), want (true, 1, 0)", started, attempt, wait)
	}
	if p.IsComplete("fn/h") {
		t.Fatal("IsComplete on read error = true, want false (fail closed)")
	}
	if p.IsTerminal("fn/h") {
		t.Fatal("IsTerminal on read error = true, want false (fail closed)")
	}
	// RecordFailure on a read error must not panic and must leave the field.
	p.RecordFailure("fn/h", time.Second)
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

package stream

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/schedule"
)

// These tests pin the ACK boundary at the consumer seam: every production
// path that acknowledges a message must do so through the single atomic
// ackAndRetainTerminal store call, never the former split XAck + retainTerminal
// pair. The in-memory fake records both call counts, so a regression that
// reintroduces the split (or drops the retention) fails deterministically
// without Redis. The atomic Redis script's own byte-level behavior is covered by
// the integration suite (see ack_retain_integration_test.go).

// TestProductionAckUsesAtomicAckAndRetain drives every production ACK path
// against a recording fake store and asserts each one acknowledges through
// ackAndRetainTerminal exactly once and never through retainTerminal.
func TestProductionAckUsesAtomicAckAndRetain(t *testing.T) {
	t.Run("event success", func(t *testing.T) {
		store := newFakeInvocationStore(nil)
		c := processMessageConsumer(store)
		msg := redis.XMessage{ID: "1-0", Values: map[string]any{"event": `{"event_name":"X"}`}}
		c.processMessage(context.Background(), msg, 1, func(context.Context, string, map[string]any) error {
			return nil
		})
		assertAtomicAckOnly(t, store)
	})

	t.Run("event exhaustion via DLQ", func(t *testing.T) {
		store := newFakeInvocationStore(nil)
		c := processMessageConsumer(store)
		msg := redis.XMessage{ID: "1-0", Values: map[string]any{"event": `{"event_name":"X"}`}}
		c.processMessage(context.Background(), msg, 1, func(ctx context.Context, _ string, _ map[string]any) error {
			p, ok := InvocationStateFrom(ctx)
			if !ok {
				return errors.New("no invocation state in ctx")
			}
			started, claim, _, err := p.TryStart("fn/h", time.Hour)
			if err != nil || !started {
				return fmt.Errorf("TryStart = (%v,%v)", started, err)
			}
			if !p.MarkExhausted("fn/h", claim) {
				return errors.New("MarkExhausted refused")
			}
			return &HandlerExhaustedError{Invocations: []ExhaustedInvocation{
				{App: "fn", Handler: "h", Attempts: claim.Attempt, Err: ErrInvocationExhausted},
			}}
		})
		assertAtomicAckOnly(t, store)
		dlqStore, ok := c.dlqStore.(*fakeDLQStore)
		if !ok || len(dlqStore.entries) != 1 {
			t.Fatalf("DLQ entries = %d, want 1 (the entry must be persisted before the ack)", len(dlqStore.entries))
		}
	})

	t.Run("malformed message via DLQ", func(t *testing.T) {
		store := newFakeInvocationStore(nil)
		c := processMessageConsumer(store)
		msg := redis.XMessage{ID: "1-0", Values: map[string]any{"not_event": "x"}}
		c.processMessage(context.Background(), msg, 1, func(context.Context, string, map[string]any) error {
			return nil
		})
		assertAtomicAckOnly(t, store)
	})

	t.Run("missing payload ack", func(t *testing.T) {
		store := newFakeInvocationStore(nil)
		c := processMessageConsumer(store)
		// A body-less message takes the missing-payload branch with ack=true.
		c.processMessage(context.Background(), redis.XMessage{ID: "1-0"}, 1,
			func(context.Context, string, map[string]any) error { return nil })
		assertAtomicAckOnly(t, store)
	})

	t.Run("schedule success", func(t *testing.T) {
		store := newFakeInvocationStore(nil)
		c := processMessageConsumer(store)
		c.scheduleRunner = func(context.Context, string, schedule.Occurrence, []byte) error { return nil }
		msg := scheduleMessage(t)
		c.processMessage(context.Background(), msg, 1, func(context.Context, string, map[string]any) error {
			return nil
		})
		assertAtomicAckOnly(t, store)
	})

	t.Run("schedule obsolete", func(t *testing.T) {
		store := newFakeInvocationStore(nil)
		c := processMessageConsumer(store)
		c.scheduleRunner = func(context.Context, string, schedule.Occurrence, []byte) error {
			return fmt.Errorf("%w: schedule removed", ErrInvocationObsolete)
		}
		msg := scheduleMessage(t)
		c.processMessage(context.Background(), msg, 1, func(context.Context, string, map[string]any) error {
			return nil
		})
		assertAtomicAckOnly(t, store)
	})
}

// TestMissingPayloadPurgedReferenceUsesRetainOnly pins the one production path
// that legitimately retains WITHOUT an ACK: when XAUTOCLAIM reports a dangling
// PEL reference the server already purged, there is nothing to XACK, so the
// store is asked only to switch the hash to terminal retention
// (retainTerminal), never the atomic ack+retain call.
func TestMissingPayloadPurgedReferenceUsesRetainOnly(t *testing.T) {
	store := newFakeInvocationStore(map[string]string{"fn/h": "ok"})
	c := processMessageConsumer(store)

	c.clearMissingValueEntry(context.Background(), redis.XMessage{ID: "1-0"}, 3, false)

	if store.retainCalls != 1 {
		t.Fatalf("retainTerminal calls = %d, want 1", store.retainCalls)
	}
	if store.ackRetainCalls != 0 {
		t.Fatalf("ackAndRetainTerminal calls = %d, want 0 (nothing to ACK)", store.ackRetainCalls)
	}
	if !store.terminalRetained() {
		t.Fatal("the server-purged reference must still be switched to terminal retention")
	}
}

// assertAtomicAckOnly asserts a production ACK path used the single atomic
// ack+retain boundary exactly once and never the split retainTerminal call.
func assertAtomicAckOnly(t *testing.T, store *fakeInvocationStore) {
	t.Helper()
	if store.ackRetainCalls != 1 {
		t.Fatalf("ackAndRetainTerminal calls = %d, want 1", store.ackRetainCalls)
	}
	if store.retainCalls != 0 {
		t.Fatalf("retainTerminal calls = %d, want 0 (production must not split ack from retain)", store.retainCalls)
	}
}

// scheduleMessage builds a stream message carrying a valid schedule occurrence
// envelope, so processMessage classifies it as a schedule claim.
func scheduleMessage(t *testing.T) redis.XMessage {
	t.Helper()
	occ := schedule.Occurrence{
		App:         "courses",
		Schedule:    "cleanup",
		Handler:     "jobs.cleanup.handler",
		ScheduledAt: time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC),
	}
	envelope, err := occ.Envelope()
	if err != nil {
		t.Fatalf("Envelope: %v", err)
	}
	return redis.XMessage{ID: "9-0", Values: map[string]any{"event": string(envelope)}}
}

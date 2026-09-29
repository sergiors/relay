package stream

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/schedule"
)

// TestProcessMessageMakeRecoverableFailureLeavesPending pins the fail-closed
// contract for a legacy-TTL invocation-state hash that cannot be made
// persistent: the state may expire mid-delivery, so the handler must NOT be
// dispatched and the message must NOT be ACKed (retainTerminal only ever runs
// after a successful ACK) nor dead-lettered. It stays pending for a later
// reclaim, preserving at-least-once while a transient Redis fault self-heals.
func TestProcessMessageMakeRecoverableFailureLeavesPending(t *testing.T) {
	rec := withSpanRecorder(t)
	// A pre-existing non-terminal hash: exactly the legacy/pre-persistence shape
	// makeRecoverable is meant to migrate. The fake now fails the migration.
	store := newFakeInvocationStore(map[string]string{"fn/h": "ok"})
	store.recoverableErr = errors.New("redis down")
	c := processMessageConsumer(store)

	dispatched := false
	handler := func(context.Context, string, map[string]any) error {
		dispatched = true
		return nil
	}
	msg := redis.XMessage{ID: "1-0", Values: map[string]any{"event": `{"event_name":"X"}`}}
	c.processMessage(context.Background(), msg, 1, handler)

	if dispatched {
		t.Fatal("handler was dispatched despite a makeRecoverable failure")
	}
	if store.retainCalls != 0 {
		t.Fatalf("retainTerminal called %d times; the message must not be ACKed", store.retainCalls)
	}
	if got := attrString(spanByName(t, rec, "stream.message"), "relay.outcome"); got != "pending" {
		t.Fatalf("relay.outcome = %q, want pending (no dispatch, no ACK)", got)
	}
}

// TestProcessScheduleMessageMakeRecoverableFailureLeavesPending proves the
// schedule path shares the same fail-closed contract: a makeRecoverable failure
// returns a "pending" outcome and an error from processScheduleMessage (so the
// caller neither ACKs nor DLQs), the ScheduleRunner is never invoked, and the
// state is not retained. processMessage is driven too, to prove the schedule
// branch threads that pending outcome through to the message span.
func TestProcessScheduleMessageMakeRecoverableFailureLeavesPending(t *testing.T) {
	rec := withSpanRecorder(t)
	occ := schedule.Occurrence{
		Function:    "courses",
		Handler:     "jobs.cleanup.handler",
		ScheduledAt: time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC),
	}
	envelope, err := occ.Envelope()
	if err != nil {
		t.Fatalf("Envelope: %v", err)
	}

	store := newFakeInvocationStore(map[string]string{"courses/jobs.cleanup.handler": "ok"})
	store.recoverableErr = errors.New("redis down")
	c := processMessageConsumer(store)

	calls := 0
	c.scheduleRunner = func(context.Context, string, string, string, []byte) error {
		calls++
		return nil
	}

	// Direct call: the pending outcome and non-nil error are what stop the
	// caller from ACKing or dead-lettering.
	outcome, err := c.processScheduleMessage(context.Background(), "9-0", 1, occ)
	if outcome != "pending" {
		t.Fatalf("processScheduleMessage outcome = %q, want pending", outcome)
	}
	if err == nil {
		t.Fatal("processScheduleMessage error = nil, want a Redis-state error")
	}
	if calls != 0 {
		t.Fatalf("schedule runner calls = %d, want 0", calls)
	}

	// Through processMessage: the schedule branch records the pending outcome on
	// the message span and never ACKs (no retainTerminal).
	msg := redis.XMessage{ID: "9-0", Values: map[string]any{"event": string(envelope)}}
	c.processMessage(context.Background(), msg, 1, func(context.Context, string, map[string]any) error { return nil })
	if calls != 0 {
		t.Fatalf("schedule runner calls after processMessage = %d, want 0", calls)
	}
	if store.retainCalls != 0 {
		t.Fatalf("retainTerminal called %d times; the schedule message must not be ACKed", store.retainCalls)
	}
	if got := attrString(spanByName(t, rec, "stream.message"), "relay.outcome"); got != "pending" {
		t.Fatalf("relay.outcome = %q, want pending", got)
	}
}

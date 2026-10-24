//go:build integration

// This file pins the Redis-PEL migration-failure regression for schedule
// messages: a valid schedule occurrence dispatched to a wired ScheduleRunner
// whose invocation-state migration (makeRecoverable) fails must stay in the
// REAL pending entries list with no runner execution, no ACK, and no DLQ
// entry — the fail-closed at-least-once contract for the schedule path.
package stream

import (
	"context"
	"errors"
	"testing"

	"relay/internal/testutil"
)

// failingMigrationStore narrows the seam to exactly one method: the real
// invocationStore built by newEnv handles every other call, but
// makeRecoverable always fails (counted), simulating an unreachable Redis
// state at dispatch time.
type failingMigrationStore struct {
	invocationStateStore
	calls int
}

func (s *failingMigrationStore) makeRecoverable(
	_ context.Context, _, _, _ string,
) error {
	s.calls++
	return errors.New("redis state unavailable")
}

// TestIntegrationScheduleMigrationFailureStaysInPELWithoutRunnerCallOrDLQ
// reads a schedule message into the group's PEL, then synchronously drives
// processMessage with a store whose makeRecoverable fails. The message must
// remain pending in the PEL (a later reclaim retries it) while the schedule
// runner is never invoked and no ACK or DLQ write happens.
func TestIntegrationScheduleMigrationFailureStaysInPELWithoutRunnerCallOrDLQ(t *testing.T) {
	testutil.RequireRedis(t)
	rr := &scriptedRunner{}
	e := newEnv(t, ConsumerConfig{ScheduleRunner: rr.Run})

	store := &failingMigrationStore{invocationStateStore: e.consumer.invStateStore}
	e.consumer.invStateStore = store

	id := e.xadd(t, schEnvelope(t, schOcc()))
	// XREADGROUP (not the consumer loop) puts the entry in the real PEL so the
	// later inspection reflects exactly this delivery.
	msg := e.readOneIntoPEL(t)
	if msg.ID != id {
		t.Fatalf("read message %s, want %s", msg.ID, id)
	}

	e.consumer.processMessage(context.Background(), msg, 1,
		func(context.Context, string, map[string]any) error {
			t.Error("ordinary handler invoked for a schedule message")
			return nil
		})

	if store.calls != 1 {
		t.Fatalf("makeRecoverable calls = %d, want 1 (the schedule path's own migration attempt)", store.calls)
	}
	if got := rr.count(); got != 0 {
		t.Fatalf("schedule runner invoked %d times despite a failed migration; want 0", got)
	}
	if pending := e.pending(); pending[id] == 0 {
		t.Fatalf("message %s missing from the PEL; it must remain pending on a migration failure", id)
	}
	if entries := e.dlqFor(id); len(entries) != 0 {
		t.Fatalf("DLQ entries for %s = %d, want 0", id, len(entries))
	}
}

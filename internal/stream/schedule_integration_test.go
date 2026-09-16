//go:build integration

// This file exercises schedule-occurrence messages end to end through the stream
// consumer against a real Redis server: the ScheduleRunner seam routes them
// directly to the runner (bypassing event matching), normal events and schedule
// messages coexist, and the full delivery contract (ACK / pending / reclaim /
// DLQ / invocation-state protection) applies to schedule messages exactly like
// normal events.
package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"relay/internal/schedule"
)

// schOcc returns a schedule occurrence used by the schedule integration tests.
func schOcc() schedule.Occurrence {
	return schedule.Occurrence{
		Function:    "courses",
		Handler:     "jobs.cleanup.handler",
		ScheduledAt: time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC),
	}
}

// schEnvelope returns the stream message body (single "event" field) carrying the
// occurrence's envelope, exactly as the publisher writes it.
func schEnvelope(t *testing.T, o schedule.Occurrence) string {
	t.Helper()
	env, err := o.Envelope()
	if err != nil {
		t.Fatalf("Envelope: %v", err)
	}
	return string(env)
}

// scriptedRunner records each schedule invocation it receives and, under a
// configurable script, fails the first N calls and/or reports exhaustion.
type scriptedRunner struct {
	mu      sync.Mutex
	calls   []scheduleOccCall
	failErr error
	exhaust bool
}

type scheduleOccCall struct {
	fn      string
	handler string
	payload []byte
}

func (r *scriptedRunner) Run(ctx context.Context, msgID, fn, handler string, payload []byte) error {
	r.mu.Lock()
	r.calls = append(r.calls, scheduleOccCall{fn: fn, handler: handler, payload: append([]byte(nil), payload...)})
	failErr := r.failErr
	exhaust := r.exhaust
	r.mu.Unlock()
	if exhaust {
		return ErrInvocationExhausted
	}
	return failErr
}

func (r *scriptedRunner) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *scriptedRunner) last() scheduleOccCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.calls) == 0 {
		return scheduleOccCall{}
	}
	return r.calls[len(r.calls)-1]
}

// A schedule message routed through a ScheduleRunner is executed directly with the
// exact function/handler and payload, then ACKed (gone from the PEL). It does NOT
// flow through a normal-event handler.
func TestIntegrationScheduleRoutedToRunnerAndAcked(t *testing.T) {
	requireRedis(t)
	rr := &scriptedRunner{}
	e := newEnv(t, ConsumerConfig{ScheduleRunner: rr.Run})
	o := schOcc()

	id := e.xadd(t, schEnvelope(t, o))
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		return fmt.Errorf("normal handler invoked for a schedule message")
	})

	WaitFor(t, 8*time.Second, "schedule runner invoked", func() bool {
		return rr.count() >= 1
	})
	call := rr.last()
	if call.fn != o.Function {
		t.Fatalf("function = %q, want %q", call.fn, o.Function)
	}
	if call.handler != o.Handler {
		t.Fatalf("handler = %q, want %q", call.handler, o.Handler)
	}
	// The payload is the handler payload (source + scheduled_at only), never the
	// envelope (no function/handler/occurrence_id leaks).
	var p map[string]any
	if err := json.Unmarshal(call.payload, &p); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if len(p) != 2 || p["source"] != "relay.schedule" || p["scheduled_at"] != "2026-07-01T08:00:00Z" {
		t.Fatalf("unexpected schedule payload: %v", p)
	}

	WaitFor(t, 8*time.Second, "schedule message acked (gone from PEL)", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	e.stop(t)
}

// A schedule message routed through a ScheduleRunner is delivered and acked, and
// the normal-event handler is never invoked for it (bypass confirmed).
func TestIntegrationScheduleBypassesNormalHandler(t *testing.T) {
	requireRedis(t)
	rr := &scriptedRunner{}
	e := newEnv(t, ConsumerConfig{ScheduleRunner: rr.Run})
	o := schOcc()

	id := e.xadd(t, schEnvelope(t, o))
	normalCalls := &atomic.Int64{}
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		normalCalls.Add(1)
		return nil
	})

	WaitFor(t, 8*time.Second, "schedule runner invoked", func() bool {
		return rr.count() >= 1
	})
	WaitFor(t, 8*time.Second, "schedule message acked (gone from PEL)", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	e.stop(t)
	if got := normalCalls.Load(); got != 0 {
		t.Fatalf("normal handler invoked %d times for a schedule message; want 0 (bypass)", got)
	}
}

// After a ScheduleRunner is wired, a normal (non-schedule) event still flows
// through the matcher handler and never touches the schedule runner.
func TestIntegrationNormalEventUnchangedWithScheduleRunner(t *testing.T) {
	requireRedis(t)
	rr := &scriptedRunner{}
	e := newEnv(t, ConsumerConfig{ScheduleRunner: rr.Run})
	id := e.xadd(t, `{"a":1}`)

	acked := make(chan struct{})
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID == id {
			close(acked)
		}
		return nil
	})
	<-acked
	WaitFor(t, 8*time.Second, "normal event acked (gone from PEL)", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	e.stop(t)
	if rr.count() != 0 {
		t.Fatalf("schedule runner invoked %d times for a normal event; want 0", rr.count())
	}
}

// A failing ScheduleRunner leaves the message pending; a reclaim (fast timings)
// redelivers it with a higher attempt count, and the second attempt succeeds and
// is acked. Invocation-state protection still applies to schedule messages.
func TestIntegrationScheduleReclaimRetriesThenAcks(t *testing.T) {
	requireRedis(t)
	rr := &scriptedRunner{failErr: fmt.Errorf("boom")}
	// The runner fails only its first invocation.
	var attempts atomic.Int64
	runner := func(ctx context.Context, msgID, fn, handler string, payload []byte) error {
		n := attempts.Add(1)
		_ = n
		return rr.Run(ctx, msgID, fn, handler, payload)
	}
	e := newEnv(t, ConsumerConfig{
		ScheduleRunner:  runner,
		MinPendingIdle:  300 * time.Millisecond,
		ReclaimInterval: 200 * time.Millisecond,
	})
	o := schOcc()
	id := e.xadd(t, schEnvelope(t, o))

	e.start(func(ctx context.Context, msgID string, ev map[string]any) error { return nil })

	// First delivery: the runner fails (attempt 1), leaving the message pending.
	WaitFor(t, 8*time.Second, "schedule runner invoked (attempt 1 fails)", func() bool {
		return rr.count() >= 1
	})
	// Flip the runner to success so a reclaim retry succeeds.
	rr.mu.Lock()
	rr.failErr = nil
	rr.mu.Unlock()

	WaitFor(t, 8*time.Second, "schedule message acked (gone from PEL)", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	e.stop(t)
	if got := attempts.Load(); got < 2 {
		t.Fatalf("expected >= 2 delivery attempts (fail then reclaim retry), got %d", got)
	}
}

// A schedule message whose ScheduleRunner reports exhaustion routes to the DLQ and
// is acked, exactly like a normal exhausted event.
func TestIntegrationScheduleExhaustionRoutesToDLQ(t *testing.T) {
	requireRedis(t)
	rr := &scriptedRunner{exhaust: true}
	e := newEnv(t, ConsumerConfig{ScheduleRunner: rr.Run})
	o := schOcc()
	id := e.xadd(t, schEnvelope(t, o))

	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		return nil
	})
	WaitFor(t, 8*time.Second, "schedule runner invoked", func() bool {
		return rr.count() >= 1
	})
	// The invocation is exhausted, so the message routes to the DLQ (and is acked).
	WaitFor(t, 8*time.Second, "schedule message routed to DLQ", func() bool {
		_, ok := e.dlq()[id]
		return ok
	})
	WaitFor(t, 8*time.Second, "schedule message acked (gone from PEL)", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	e.stop(t)

	m, ok := e.dlq()[id]
	if !ok {
		t.Fatalf("expected DLQ entry for %s", id)
	}
	// The DLQ entry must carry the schedule envelope in its `event` field.
	if !strings.Contains(fmt.Sprintf("%v", m.Values["event"]), "relay.schedule") {
		t.Fatalf("DLQ event missing the schedule envelope: %v", m.Values["event"])
	}
}

// A schedule message whose ScheduleRunner reports ErrInvocationNotEligible (a protected
// invocation, e.g. running on another replica or waiting out a retry backoff) is
// left pending, never acked and never DLQ'd.
func TestIntegrationScheduleNotEligibleLeavesPending(t *testing.T) {
	requireRedis(t)
	neo := schOcc()
	env := newEnv(t, ConsumerConfig{
		ScheduleRunner: func(ctx context.Context, msgID, fn, handler string, payload []byte) error {
			return ErrInvocationNotEligible
		},
		MinPendingIdle:  300 * time.Millisecond,
		ReclaimInterval: 200 * time.Millisecond,
	})
	id := env.xadd(t, schEnvelope(t, neo))
	env.start(func(ctx context.Context, msgID string, ev map[string]any) error { return nil })

	// The message must stay pending across the reclaim grace window (never acked,
	// never DLQ'd): the runner reports the invocation not eligible, so the message
	// is left pending for a later delivery.
	WaitFor(t, 8*time.Second, "not-eligible schedule message delivered into PEL", func() bool {
		_, ok := env.pending()[id]
		return ok
	})
	waitSustained(t, "not-eligible schedule message stays pending", 500*time.Millisecond, func() bool {
		_, ok := env.pending()[id]
		return ok
	})
	env.stop(t)
}

// A schedule message whose ScheduleRunner reports ErrInvocationObsolete (the
// function or its schedule entry/handler was removed while the message was
// pending) is terminal but must be ACKed — never left pending, never routed to
// the DLQ. This is the intentional-removal case: retrying or dead-lettering an
// obsolete occurrence would be wrong, so the stream acknowledges it instead.
func TestIntegrationScheduleObsoleteIsAckedNotDLQed(t *testing.T) {
	requireRedis(t)
	env := newEnv(t, ConsumerConfig{
		ScheduleRunner: func(ctx context.Context, msgID, fn, handler string, payload []byte) error {
			return fmt.Errorf("%w: removed", ErrInvocationObsolete)
		},
		MinPendingIdle:  300 * time.Millisecond,
		ReclaimInterval: 200 * time.Millisecond,
	})
	id := env.xadd(t, schEnvelope(t, schOcc()))
	env.start(func(ctx context.Context, msgID string, ev map[string]any) error { return nil })

	// The obsolete occurrence is acknowledged: it disappears from the PEL.
	WaitFor(t, 8*time.Second, "obsolete schedule message acked (gone from PEL)", func() bool {
		_, ok := env.pending()[id]
		return !ok
	})
	// It must NOT be dead-lettered, and it must stay acked — no reclaim brings it
	// back into the PEL.
	waitSustained(t, "obsolete schedule message stays acked and un-DLQed", 500*time.Millisecond, func() bool {
		if _, ok := env.pending()[id]; ok {
			return false
		}
		_, dlqed := env.dlq()[id]
		return !dlqed
	})
	env.stop(t)
}

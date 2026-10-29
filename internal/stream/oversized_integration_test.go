//go:build integration

// This file exercises the raw-event byte cap (MAX_EVENT_BYTES) against a real
// Redis server: an oversized message is rejected before decode/match/handler and
// routed to the durable PostgreSQL DLQ with a bounded diagnostic summary (never
// the payload), the original is acked only after that write, and a failed write
// leaves the message pending. It is excluded from the default suite by the
// integration build tag and REQUIRES Redis at REDIS_TEST_ADDR (default
// localhost:6379) and PostgreSQL at TEST_DATABASE_URL.
package stream

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/dlq"
	"relay/internal/observability/metrics"
	"relay/internal/testutil"
)

// TestIntegrationOversizedEventRoutesToDLQImmediately pins the end-to-end
// oversize disposition: a message whose raw event value exceeds the configured
// cap is routed straight to the DLQ (acked without ever reaching the handler),
// the DLQ `event` field is a bounded diagnostic summary rather than the
// oversized payload, and the entry is non-replayable through the read model.
func TestIntegrationOversizedEventRoutesToDLQImmediately(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{MaxEventBytes: 64})
	// A deterministic payload larger than the cap. It is valid JSON, so only the
	// byte cap can reject it.
	payload := `{"event_name":"INSERT","blob":"` + strings.Repeat("a", 256) + `"}`
	id := e.xadd(t, payload)

	var handlerRan atomic.Bool
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		handlerRan.Store(true) // must not be reached: oversize routes to DLQ pre-handler
		return nil
	})

	testutil.WaitFor(t, 8*time.Second, "oversized message routed to DLQ", func() bool {
		_, ok := e.dlqEntry(id)
		return ok
	})
	testutil.WaitFor(t, 8*time.Second, "oversized message acked", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	e.stop(t)

	if handlerRan.Load() {
		t.Fatal("handler should not be invoked for an oversized message")
	}
	m, ok := e.dlqEntry(id)
	if !ok {
		t.Fatalf("expected DLQ entry for oversized message %s", id)
	}
	// The DLQ never stores the oversized payload.
	if strings.Contains(m.Event, strings.Repeat("a", 32)) {
		t.Fatalf("DLQ event must not contain the oversized payload: %q", m.Event)
	}
	if len(m.Event) > 256 {
		t.Fatalf("DLQ event summary is not bounded: %d bytes", len(m.Event))
	}
	if !strings.Contains(m.Event, "event_oversized") || !strings.Contains(m.Event, "event_bytes") {
		t.Fatalf("DLQ event is not the bounded summary: %q", m.Event)
	}
	if !strings.Contains(m.Reason, "event oversized") {
		t.Fatalf("DLQ reason = %q, want the stable oversize reason", m.Reason)
	}
	// No handler invocation to attribute: placeholder app/handler, 0 attempts.
	if m.App != dlq.NoHandler || m.Handler != dlq.NoHandler {
		t.Errorf("oversized DLQ app/handler = %q/%q, want %q placeholder",
			m.App, m.Handler, dlq.NoHandler)
	}
	if m.HandlerAttempts != 0 {
		t.Errorf("oversized DLQ HandlerAttempts = %d, want 0", m.HandlerAttempts)
	}
	if m.Deliveries != 1 {
		t.Errorf("oversized DLQ Deliveries = %d, want 1", m.Deliveries)
	}
	// The read model parses the entry and reports it non-replayable, so
	// `relay dlq replay` refuses it before dialing.
	if m.Replayable() {
		t.Fatalf("oversized summary entry must not be replayable: %+v", m)
	}
	// The entry became durable before the ACK, so the confirmed ACK settled it.
	if m.SettledAt == "" {
		t.Fatalf("oversized entry must be settled after the confirmed ACK")
	}
}

// TestIntegrationOversizedCounterIncrementsOnce pins the unlabeled oversize
// counter: a single oversized delivery increments it exactly once.
func TestIntegrationOversizedCounterIncrementsOnce(t *testing.T) {
	testutil.RequireRedis(t)
	m := metrics.New()
	e := newEnv(t, ConsumerConfig{MaxEventBytes: 32, Metrics: m})
	id := e.xadd(t, `{"blob":"`+strings.Repeat("a", 128)+`"}`)
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		return nil
	})
	// Wait for the DLQ entry (which requires the oversize branch to have run)
	// before asserting the counter, so the assertion cannot race processing.
	testutil.WaitFor(t, 8*time.Second, "oversized message routed to DLQ", func() bool {
		_, ok := e.dlqEntry(id)
		return ok
	})
	testutil.WaitFor(t, 8*time.Second, "oversized message acked", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	e.stop(t)
	if got := m.Counter(metrics.MetricEventsOversized); got != 1 {
		t.Fatalf("events_oversized_total = %d, want 1", got)
	}
}

// TestIntegrationOversizedDLQWriteFailureLeavesPending pins the required
// DLQ-before-XACK ordering on the oversize path: when the PostgreSQL DLQ insert
// fails, the original stays pending; once it succeeds the message is acked.
func TestIntegrationOversizedDLQWriteFailureLeavesPending(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{MaxEventBytes: 32})
	id := e.xadd(t, `{"blob":"`+strings.Repeat("a", 128)+`"}`)

	// Read the message into the PEL so XAck has an entry to remove.
	msgs, err := e.client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group:    e.group,
		Consumer: e.consumer.consumer,
		Streams:  []string{e.stream, ">"},
		Count:    1,
		Block:    time.Second,
	}).Result()
	if err != nil {
		t.Fatalf("xreadgroup: %v", err)
	}
	if len(msgs) != 1 || len(msgs[0].Messages) != 1 {
		t.Fatalf("expected one message in PEL, got %+v", msgs)
	}
	msg := msgs[0].Messages[0]

	// Force the DLQ insert to fail with an injected store wrapper.
	failing := &failingSaveDLQStore{err: context.DeadlineExceeded}
	failing.Store = e.dlqStore
	e.consumer.dlqStore = failing
	e.consumer.processMessage(context.Background(), msg, 1, func(context.Context, string, map[string]any) error {
		return nil
	})
	if _, ok := e.pending()[id]; !ok {
		t.Fatalf("oversized message %s must stay pending when the DLQ write fails", id)
	}
	if got := len(e.dlqFor(id)); got != 0 {
		t.Fatalf("DLQ rows after failed save = %d, want 0", got)
	}

	// Let the DLQ write succeed and re-process.
	failing.ok.Store(true)
	e.consumer.processMessage(context.Background(), msg, 1, func(context.Context, string, map[string]any) error {
		return nil
	})
	if _, ok := e.pending()[id]; ok {
		t.Fatalf("oversized message %s should be acked after a successful DLQ write", id)
	}
	m, ok := e.dlqEntry(id)
	if !ok {
		t.Fatalf("expected DLQ entry after the successful write")
	}
	if !strings.Contains(m.Event, "event_oversized") {
		t.Fatalf("DLQ event = %q, want the bounded oversize summary", m.Event)
	}
}

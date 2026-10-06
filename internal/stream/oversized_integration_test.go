//go:build integration

// This file exercises the raw-event byte cap (MAX_EVENT_BYTES) against a real
// Redis server: an oversized message is rejected before decode/match/handler and
// routed to the DLQ with a bounded diagnostic summary (never the payload), the
// original is acked only after that write, and a failed write leaves the message
// pending. It is excluded from the default suite by the integration build tag and
// REQUIRES Redis at REDIS_TEST_ADDR (default localhost:6379).
package stream

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

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
		_, ok := e.dlq()[id]
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
	m, ok := e.dlq()[id]
	if !ok {
		t.Fatalf("expected DLQ entry for oversized message %s", id)
	}
	event, _ := m.Values["event"].(string)
	// The DLQ never stores the oversized payload.
	if strings.Contains(event, strings.Repeat("a", 32)) {
		t.Fatalf("DLQ event must not contain the oversized payload: %q", event)
	}
	if len(event) > 256 {
		t.Fatalf("DLQ event summary is not bounded: %d bytes", len(event))
	}
	if !strings.Contains(event, "event_oversized") || !strings.Contains(event, "event_bytes") {
		t.Fatalf("DLQ event is not the bounded summary: %q", event)
	}
	reason, _ := m.Values["reason"].(string)
	if !strings.Contains(reason, "event oversized") {
		t.Fatalf("DLQ reason = %q, want the stable oversize reason", reason)
	}
	// No handler invocation to attribute: placeholder app/handler, 0 attempts.
	if m.Values["app"] != dlqNoHandler || m.Values["handler"] != dlqNoHandler {
		t.Errorf("oversized DLQ app/handler = %v/%v, want %q placeholder",
			m.Values["app"], m.Values["handler"], dlqNoHandler)
	}
	if m.Values["handler_attempts"] != "0" {
		t.Errorf("oversized DLQ handler_attempts = %v, want 0", m.Values["handler_attempts"])
	}
	if m.Values["deliveries"] != "1" {
		t.Errorf("oversized DLQ deliveries = %v, want 1", m.Values["deliveries"])
	}

	// The read model parses the entry and reports it non-replayable, so
	// `relay dlq replay` refuses it before dialing.
	entry, err := ParseDLQEntry(m.ID, m.Values)
	if err != nil {
		t.Fatalf("ParseDLQEntry: %v", err)
	}
	if entry.Replayable() {
		t.Fatalf("oversized summary entry must not be replayable: %+v", entry)
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
		_, ok := e.dlq()[id]
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
// DLQ-before-XACK ordering on the oversize path: when the DLQ XADD fails, the
// original stays pending; once it succeeds the message is acked.
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

	// Force the DLQ XADD to fail by making the DLQ stream name a wrong-type key.
	if r := e.client.Set(context.Background(), e.consumer.dlqStream, "not-a-stream", 0); r.Err() != nil {
		t.Fatalf("set wrong-type key: %v", r.Err())
	}
	e.consumer.processMessage(context.Background(), msg, 1, func(context.Context, string, map[string]any) error {
		return nil
	})
	if _, ok := e.pending()[id]; !ok {
		t.Fatalf("oversized message %s must stay pending when the DLQ write fails", id)
	}

	// Let the DLQ write succeed: delete the wrong-type key and re-process.
	if err := e.client.Del(context.Background(), e.consumer.dlqStream).Err(); err != nil {
		t.Fatalf("del wrong-type key: %v", err)
	}
	e.consumer.processMessage(context.Background(), msg, 1, func(context.Context, string, map[string]any) error {
		return nil
	})
	if _, ok := e.pending()[id]; ok {
		t.Fatalf("oversized message %s should be acked after a successful DLQ write", id)
	}
	m, ok := e.dlq()[id]
	if !ok {
		t.Fatalf("expected DLQ entry after the successful write")
	}
	if event, _ := m.Values["event"].(string); !strings.Contains(event, "event_oversized") {
		t.Fatalf("DLQ event = %q, want the bounded oversize summary", event)
	}
}

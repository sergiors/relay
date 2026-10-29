//go:build integration

// This file exercises the ATOMIC DLQ seam against a real Redis server AND a
// real PostgreSQL: routeToDLQ validates the exhausted claim against Redis
// BEFORE writing any PostgreSQL row, writes rows idempotently by their stable
// (delivery, invocation) identity, and acknowledges the source only after every
// required row is committed. It covers validation-before-write (a stale or
// missing exhausted marker must leave no row and no ACK), the placeholder path,
// and a concurrent race on the same stable identity that must produce exactly
// one row. Excluded from the default suite by the integration build tag;
// REQUIRES Redis at REDIS_TEST_ADDR (default localhost:6379) and PostgreSQL at
// TEST_DATABASE_URL, failing rather than skipping when either is absent.
package stream

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/dlq"
	"relay/internal/testutil"
)

// routeToDLQForOne runs c.routeToDLQ for msgID with the runner's typed
// exhaustion error carrying exactly one attributed invocation.
func routeToDLQForOne(t *testing.T, c *Consumer, msgID, app, handler string, attempts int) {
	t.Helper()
	c.routeToDLQ(context.Background(), redis.XMessage{ID: msgID, Values: map[string]any{"event": `{"a":1}`}},
		&HandlerExhaustedError{Invocations: []ExhaustedInvocation{
			{App: app, Handler: handler, Attempts: attempts, Err: ErrInvocationExhausted},
		}}, 1)
}

// TestIntegrationDLQRefusesStaleOrMissingClaimPinsNoRowNoACK pins the claim
// validation-before-write contract on the real transports: a stale exhausted
// marker (a retained attempt that no longer matches the runner's attribution)
// and a missing marker both refuse the write — no PostgreSQL row is committed,
// the source message is NOT acknowledged (it stays recoverable in the PEL), and
// later, with a correct claim, the same delivery writes exactly one row and
// ACKs.
func TestIntegrationDLQRefusesStaleOrMissingClaimPinsNoRowNoACK(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	msg := e.readOneIntoPEL(t)
	key := invocationStateKey(e.stream, e.group, id)

	// Missing marker: the invocation was never exhausted, so nothing may be
	// written on its behalf.
	routeToDLQForOne(t, e.consumer, id, "fn", "h", 1)
	if _, ok := e.pending()[id]; !ok {
		t.Fatalf("message %s must stay pending while the claim is missing", id)
	}
	if got := len(e.dlqFor(id)); got != 0 {
		t.Fatalf("a missing exhausted claim must write no row, got %d", got)
	}
	if ok, err := e.client.HExists(context.Background(), key, terminalField).Result(); err != nil || ok {
		t.Fatalf("state must not be terminal while the write is refused (hexists=%v err=%v)", ok, err)
	}

	// Stale marker: the runner attributes attempt 1 but the retained marker
	// carries attempt 2 (a superseded exhaustion), so the write is refused.
	p := NewInvocationState(context.Background(), e.consumer.invStateStore, e.stream, e.group, id, e.consumer.log)
	if started, _, _, err := p.TryStart("fn/h", time.Hour); err != nil || !started {
		t.Fatalf("TryStart = (%v,%v), want a started claim", started, err)
	}
	if err := e.client.HSet(context.Background(), key, "fn/h",
		exhaustedValue(InvocationClaim{Attempt: 2, Token: "bb"}, false)).Err(); err != nil {
		t.Fatalf("rewrite marker as a newer exhaustion: %v", err)
	}
	routeToDLQForOne(t, e.consumer, id, "fn", "h", 1)
	if _, ok := e.pending()[id]; !ok {
		t.Fatalf("message %s must stay pending while the claim is stale", id)
	}
	if got := len(e.dlqFor(id)); got != 0 {
		t.Fatalf("a stale exhausted claim must write no row, got %d", got)
	}
	if ok, err := e.client.HExists(context.Background(), key, terminalField).Result(); err != nil || ok {
		t.Fatalf("state must not be terminal while the write is refused (hexists=%v err=%v)", ok, err)
	}

	// A current claim writes exactly one row and ACKs the delivery. The marker
	// is already exhausted at attempt 2 (not owned by any live claim), so the
	// redelivery re-reports the retained attempt without a new execution.
	e.consumer.processMessage(context.Background(), msg, 2, func(context.Context, string, map[string]any) error {
		return &HandlerExhaustedError{Invocations: []ExhaustedInvocation{
			{App: "fn", Handler: "h", Attempts: 2, Err: ErrInvocationExhausted},
		}}
	})
	entries := e.dlqFor(id)
	if got := len(entries); got != 1 {
		t.Fatalf("DLQ rows after the valid claim = %d, want exactly 1", got)
	}
	if entries[0].HandlerAttempts != 2 {
		t.Fatalf("recorded HandlerAttempts = %d, want 2 (the current retained attempt)", entries[0].HandlerAttempts)
	}
	if _, ok := e.pending()[id]; ok {
		t.Fatalf("message %s should be acked after the valid claim's write", id)
	}
	if entries[0].SettledAt == "" {
		t.Fatalf("a confirmed ACK settles the delivery immediately")
	}
}

// TestIntegrationDLQPlaceholderRoutePersistsAndAcks pins the malformed-message
// placeholder: no claim is consulted (there is no invocation), one placeholder
// row with the explicit 0 attempt count is written, and the message is ACKed.
func TestIntegrationDLQPlaceholderRoutePersistsAndAcks(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id, err := e.client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: e.stream,
		Values: map[string]any{"not_event": "x"},
	}).Result()
	if err != nil {
		t.Fatalf("xadd malformed: %v", err)
	}
	msg := e.readOneIntoPEL(t)
	e.consumer.processMessage(context.Background(), msg, 1, func(context.Context, string, map[string]any) error {
		return nil
	})

	entries := e.dlqFor(id)
	if len(entries) != 1 {
		t.Fatalf("placeholder rows = %d, want 1", len(entries))
	}
	entry := entries[0]
	if entry.App != dlq.NoHandler || entry.Handler != dlq.NoHandler {
		t.Fatalf("placeholder attribution = %q/%q, want %q", entry.App, entry.Handler, dlq.NoHandler)
	}
	if entry.HandlerAttempts != 0 {
		t.Fatalf("placeholder attempt count = %d, want an explicit 0", entry.HandlerAttempts)
	}
	if entry.ID != dlq.NewIDForDelivery(dlq.NewDeliveryID(e.stream, e.group, id), "") {
		t.Fatalf("placeholder ID = %q, want the (delivery, empty-invocation) identity", entry.ID)
	}
	if _, ok := e.pending()[id]; ok {
		t.Fatalf("message %s should be acked after the placeholder write", id)
	}
	if entry.SettledAt == "" {
		t.Fatalf("the confirmed ACK settles the placeholder delivery")
	}
}

// TestIntegrationDLQConcurrentExactlyOneWinner races many callers persisting
// the SAME exhausted invocation through the real PostgreSQL store. The stable
// entry identity is the uniqueness boundary, so exactly one caller inserts the
// row and every other observes it as already persisted — never a duplicate, and
// the single persisted row starts unsettled (its delivery is still recoverable).
func TestIntegrationDLQConcurrentExactlyOneWinner(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	key := invocationStateKey(e.stream, e.group, id)

	if err := e.client.HSet(context.Background(), key,
		"fn/h", exhaustedValue(InvocationClaim{Attempt: 1, Token: "aa"}, false)).Err(); err != nil {
		t.Fatalf("seed exhausted marker: %v", err)
	}

	const n = 16
	deliveryID := dlq.NewDeliveryID(e.stream, e.group, id)
	entryID := dlq.NewIDForDelivery(deliveryID, "fn/h")
	makeEntry := func() dlq.Entry {
		return dlq.Entry{
			ID:              entryID,
			Source:          e.stream,
			SourceGroup:     e.group,
			SourceMessageID: id,
			DeliveryID:      deliveryID,
			Consumer:        e.consumer.consumer,
			Event:           `{"a":1}`,
			Reason:          "invocation exhausted: function \"fn\" handler \"h\" exhausted after 1 handler attempts",
			App:             "fn",
			Handler:         "h",
			Deliveries:      1,
			HandlerAttempts: 1,
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
		}
	}

	start := make(chan struct{})
	wrote := make([]bool, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			wrote[i], errs[i] = e.dlqStore.SaveDLQEntry(context.Background(), makeEntry())
		}(i)
	}
	close(start)
	wg.Wait()

	winners := 0
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("caller %d: %v", i, errs[i])
		}
		if wrote[i] {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("winners = %d, want exactly 1", winners)
	}
	if got := len(e.dlqFor(id)); got != 1 {
		t.Fatalf("row count = %d, want exactly 1", got)
	}
	entry, ok := e.dlqEntry(id)
	if !ok || entry.App != "fn" || entry.Handler != "h" || entry.ID != entryID {
		t.Fatalf("persisted entry = %+v (ok %v)", entry, ok)
	}
	if entry.SettledAt != "" {
		t.Fatalf("a concurrently inserted row starts unsettled: %+v", entry)
	}
}

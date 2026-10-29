//go:build integration

// This file drives the PostgreSQL durable-DLQ contract through the REAL
// routeToDLQ/processMessage flow against a real Redis server and a real
// PostgreSQL: the stable identity/uniqueness seam, the claim-validation seam,
// and the persist-BEFORE-ack ordering are exercised on the real transports
// rather than seeded.
//
//   - The source XACK that follows a genuinely-persisted DLQ row is forced to
//     fail deterministically, and a redelivery completes the message without a
//     second row (row + delivery unsettled until the ACK succeeds).
//   - A placeholder (malformed message) behaves identically.
//   - PostgreSQL unavailability leaves the source pending and the exhausted
//     handler un-executed on the redelivery (the invocation state still fences
//     a re-run); once PostgreSQL returns the redelivery resumes with no
//     duplicate and the ACK lands.
//   - The delivery is settled as soon as the ACK is confirmed, and stays
//     unsettled exactly while the source remains recoverable.
//
// Excluded from the default suite by the integration build tag; REQUIRES Redis
// at REDIS_TEST_ADDR (default localhost:6379) and PostgreSQL at
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

// failAckAfterTraceReadStore decorates the real Redis-backed invocation store.
// After the FIRST successful traceReference read (routeToDLQ reads the exhausted
// invocation's lineage right before committing its DLQ row, after the claim
// validation) it runs afterRead, which the test uses to close the consumer's
// Redis client: the XACK routeToDLQ performs after the PostgreSQL row is
// committed is then deterministically refused (redis.ErrClosed) while the DLQ
// row is already durable. Every other method is delegated to the wrapped store
// unchanged, so this is a narrow failure seam, not a reimplementation.
type failAckAfterTraceReadStore struct {
	invocationStateStore
	afterRead func()
	once      sync.Once
}

func (s *failAckAfterTraceReadStore) traceReference(ctx context.Context, stream, group, msgID, invocation string) (string, error) {
	lineage, err := s.invocationStateStore.traceReference(ctx, stream, group, msgID, invocation)
	if err == nil {
		s.once.Do(s.afterRead)
	}
	return lineage, err
}

// pendingForClient reports whether id is in the group's PEL, using cli.
func pendingForClient(t *testing.T, cli *redis.Client, stream, group, id string) bool {
	t.Helper()
	entries, err := cli.XPendingExt(context.Background(), &redis.XPendingExtArgs{
		Stream: stream, Group: group, Start: "-", End: "+", Count: 100,
	}).Result()
	if err != nil {
		t.Fatalf("xpending: %v", err)
	}
	for _, pe := range entries {
		if pe.ID == id {
			return true
		}
	}
	return false
}

// readPendingForConsumer reads the consumer's still-pending PEL entry with
// wantID (XREADGROUP with "0" reads that consumer's history), so a test can
// drive a redelivery through processMessage after the original XACK failed.
func readPendingForConsumer(t *testing.T, cli *redis.Client, stream, group, consumer, wantID string) redis.XMessage {
	t.Helper()
	msgs, err := cli.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, "0"},
		Count:    100,
	}).Result()
	if err != nil {
		t.Fatalf("xreadgroup pending: %v", err)
	}
	for _, s := range msgs {
		for _, m := range s.Messages {
			if m.ID == wantID {
				return m
			}
		}
	}
	t.Fatalf("pending message %s not found for consumer %s", wantID, consumer)
	return redis.XMessage{}
}

// waitRetainedForClient is the client-explicit form of testEnv.waitRetained,
// used after the consumer's own client was closed: the invocation-state hash
// must carry the terminal marker and a positive retention TTL.
func waitRetainedForClient(t *testing.T, cli *redis.Client, key string) {
	t.Helper()
	testutil.WaitFor(t, 8*time.Second, "invocation state terminal-retained", func() bool {
		v, err := cli.HGet(context.Background(), key, terminalField).Result()
		if err != nil || v != "1" {
			return false
		}
		d, err := cli.PTTL(context.Background(), key).Result()
		return err == nil && d > 0
	})
}

// exhaustion returns a handler that claims "fn/h", records its trace, marks it
// exhausted, and returns the runner's typed exhaustion error. On redelivery the
// claim is refused (already terminal), so it re-reports exhaustion from the
// retained attempt without a new execution.
func exhaustion() func(ctx context.Context, msgID string, ev map[string]any) error {
	return func(ctx context.Context, _ string, _ map[string]any) error {
		p, ok := InvocationStateFrom(ctx)
		if !ok {
			return &HandlerExhaustedError{Invocations: []ExhaustedInvocation{
				{App: "fn", Handler: "h", Attempts: 1},
			}}
		}
		started, claim, _, err := p.TryStart("fn/h", time.Hour)
		if err != nil || !started {
			// Already terminal (or protected): the runner would report it
			// without a new execution.
			return &HandlerExhaustedError{Invocations: []ExhaustedInvocation{
				{App: "fn", Handler: "h", Attempts: claim.Attempt},
			}}
		}
		p.RecordTrace("fn/h", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
		p.MarkExhausted("fn/h", claim)
		return &HandlerExhaustedError{Invocations: []ExhaustedInvocation{
			{App: "fn", Handler: "h", Attempts: claim.Attempt, Err: ErrInvocationExhausted},
		}}
	}
}

// TestIntegrationDLQPersistedBeforeACKSettledAfterACK pins the write/settle
// ordering at the seam between the two transports: processMessage commits the
// DLQ rows BEFORE the source ACK and marks the delivery settled only after the
// ACK resolves. With the real PostgreSQL and real Redis wired, a successful
// delivery leaves exactly one settled row; a forced ACK failure leaves the row
// unsettled with the message recoverable; and a redelivery through a fresh
// consumer completes it without a duplicate.
func TestIntegrationDLQPersistedBeforeACKSettledAfterACK(t *testing.T) {
	inspect := testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	key := invocationStateKey(e.stream, e.group, id)
	// The test closes e.client to force the XACK failure; newEnv's cleanup uses
	// e.client, so clean up through the live inspection client instead.
	// Registered after newEnv so it runs first (LIFO) while inspect is open.
	t.Cleanup(func() {
		ctx := context.Background()
		_ = inspect.Del(ctx, key, e.stream).Err()
	})

	msg := e.readOneIntoPEL(t)

	// Close the consumer's client from inside the trace read (which happens
	// after the claim validation and right before the row write): the PostgreSQL
	// row is committed (a live DB), then the XACK refused.
	e.consumer.invStateStore = &failAckAfterTraceReadStore{
		invocationStateStore: e.consumer.invStateStore,
		afterRead:            func() { _ = e.client.Close() },
	}

	e.consumer.processMessage(context.Background(), msg, 1, exhaustion())

	entries := e.dlqFor(id)
	if len(entries) != 1 {
		t.Fatalf("DLQ rows after failed XACK = %d, want 1", len(entries))
	}
	entry := entries[0]
	if entry.App != "fn" || entry.Handler != "h" || entry.HandlerAttempts != 1 {
		t.Fatalf("persisted entry = %+v", entry)
	}
	// The source ACK did not resolve: the delivery stays unsettled and the
	// message stays pending with recoverable (persistent, non-terminal) state.
	if entry.SettledAt != "" {
		t.Fatalf("entry must stay unsettled while the source ACK has not resolved: %+v", entry)
	}
	if !pendingForClient(t, inspect, e.stream, e.group, id) {
		t.Fatalf("message %s must stay pending after the failed XACK", id)
	}
	if ok, err := inspect.HExists(context.Background(), key, terminalField).Result(); err != nil || ok {
		t.Fatalf("state must not be terminal-retained after a failed XACK (hexists=%v err=%v)", ok, err)
	}

	// Redelivery: a fresh consumer/client reads the still-pending PEL entry and
	// reprocesses. The runner reports the already-terminal invocation without a
	// new execution, so routeToDLQ finds the row already committed (inserted
	//=false) and ACKs; the ACK settles the delivery.
	redeliverCli := auxRedisClient(t, inspect)
	rc := NewConsumer(ConsumerConfig{
		Client:   redeliverCli,
		Stream:   e.stream,
		Group:    e.group,
		Consumer: e.consumer.consumer,
		Log:      testutil.DiscardLogger(),
		DLQStore: e.dlqStore,
	})
	pendingMsg := readPendingForConsumer(t, redeliverCli, e.stream, e.group, e.consumer.consumer, id)
	rc.processMessage(context.Background(), pendingMsg, 2, func(context.Context, string, map[string]any) error {
		return &HandlerExhaustedError{Invocations: []ExhaustedInvocation{
			{App: "fn", Handler: "h", Attempts: 1},
		}}
	})

	// No duplicate row; the message is now acked and its state retained, and the
	// delivery is settled exactly once (same instant on every row of the
	// delivery).
	if got := len(e.dlqFor(id)); got != 1 {
		t.Fatalf("DLQ rows after redelivery = %d, want 1 (no duplicate)", got)
	}
	if pendingForClient(t, inspect, e.stream, e.group, id) {
		t.Fatalf("message %s should be acked on the idempotent redelivery", id)
	}
	waitRetainedForClient(t, inspect, key)
	settled, ok, err := e.dlqStore.GetDLQEntry(context.Background(), entry.ID)
	if err != nil || !ok {
		t.Fatalf("get entry after redelivery: %v (ok %v)", err, ok)
	}
	if settled.SettledAt == "" {
		t.Fatalf("delivery must be settled after the confirmed ACK on the redelivery")
	}
	if _, err := time.Parse(time.RFC3339, settled.SettledAt); err != nil {
		t.Fatalf("SettledAt %q is not RFC3339: %v", settled.SettledAt, err)
	}
}

// TestIntegrationDLQSuccessPathSettlesDelivery pins the ACK-success leg with a
// healthy transport: a genuinely exhausted delivery routes its row to
// PostgreSQL, the ACK succeeds, and the delivery is settled in the same
// operation — no reconciliation needed.
func TestIntegrationDLQSuccessPathSettlesDelivery(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	msg := e.readOneIntoPEL(t)

	e.consumer.processMessage(context.Background(), msg, 1, exhaustion())

	entries := e.dlqFor(id)
	if len(entries) != 1 {
		t.Fatalf("DLQ rows = %d, want 1", len(entries))
	}
	if _, ok := e.pending()[id]; ok {
		t.Fatalf("message %s should be acked after the DLQ route", id)
	}
	if entry := entries[0]; entry.SettledAt == "" {
		t.Fatalf("a confirmed ACK must settle the delivery: %+v", entry)
	}
	// Settlement reconciliation sees no work: the only delivery is already
	// settled.
	refs, err := e.dlqStore.ListUnsettledDLQDeliveries(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("list unsettled: %v", err)
	}
	if len(refs) != 0 {
		t.Fatalf("unsettled deliveries after ACK = %+v, want none", refs)
	}
}

// TestIntegrationDLQPlaceholderPersistedBeforeACK mirrors the failed-ACK
// contract for the malformed-message path: the placeholder row (NoHandler
// identity) is committed before the ACK; the forced ACK failure leaves it
// unsettled and pending; the redelivery ACKs without writing a second row.
func TestIntegrationDLQPlaceholderPersistedBeforeACK(t *testing.T) {
	inspect := testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	// A malformed message (no `event` field) routes straight to the DLQ without
	// reaching a handler.
	id, err := e.client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: e.stream,
		Values: map[string]any{"not_event": "x"},
	}).Result()
	if err != nil {
		t.Fatalf("xadd malformed: %v", err)
	}
	key := invocationStateKey(e.stream, e.group, id)
	t.Cleanup(func() {
		ctx := context.Background()
		_ = inspect.Del(ctx, key, e.stream).Err()
	})
	msg := e.readOneIntoPEL(t)

	// The malformed path never reaches an invocation, so force the ACK failure
	// by closing the client up front: the PG row is committed, then the first
	// Redis call after it (the ACK script) fails.
	_ = e.client.Close()

	e.consumer.processMessage(context.Background(), msg, 1, func(context.Context, string, map[string]any) error {
		return nil
	})

	entries := e.dlqFor(id)
	if len(entries) != 1 {
		t.Fatalf("placeholder rows after failed XACK = %d, want 1", len(entries))
	}
	if entries[0].App != dlq.NoHandler || entries[0].Handler != dlq.NoHandler {
		t.Fatalf("placeholder row = %+v", entries[0])
	}
	if entries[0].SettledAt != "" {
		t.Fatalf("placeholder delivery must stay unsettled after the failed ACK")
	}
	if !pendingForClient(t, inspect, e.stream, e.group, id) {
		t.Fatalf("malformed message %s must stay pending after the failed XACK", id)
	}

	// Redelivery on a fresh consumer: the placeholder row already exists, so no
	// second row is written and the message is ACKed (and the delivery settled).
	// A malformed message is stateless (it never owned invocation state), so the
	// ACK creates no hash: only the message leaving the PEL is observable.
	redeliverCli := auxRedisClient(t, inspect)
	rc := NewConsumer(ConsumerConfig{
		Client:   redeliverCli,
		Stream:   e.stream,
		Group:    e.group,
		Consumer: e.consumer.consumer,
		Log:      testutil.DiscardLogger(),
		DLQStore: e.dlqStore,
	})
	pendingMsg := readPendingForConsumer(t, redeliverCli, e.stream, e.group, e.consumer.consumer, id)
	rc.processMessage(context.Background(), pendingMsg, 2, func(context.Context, string, map[string]any) error {
		return nil
	})

	if got := len(e.dlqFor(id)); got != 1 {
		t.Fatalf("placeholder rows after redelivery = %d, want 1 (no duplicate)", got)
	}
	if pendingForClient(t, inspect, e.stream, e.group, id) {
		t.Fatalf("message %s should be acked on the idempotent redelivery", id)
	}
	if exists, err := inspect.Exists(context.Background(), key).Result(); err != nil || exists != 0 {
		t.Fatalf("a stateless malformed message must not create invocation state (exists=%d err=%v)", exists, err)
	}
}

// TestIntegrationDLQPostgresUnavailableLeavesPendingAndNoReexecution pins the
// PostgreSQL-unavailable branch end to end: with the store unreachable, the
// exhausted handler's row cannot be committed, so the source stays pending (no
// ACK, no counter-met DLQ), the handler is not re-executed on the redelivery
// (the exhausted marker in Redis fences it: TryStart refuses), and once
// PostgreSQL is available again the SAME delivery resumes: the row is written
// once, the message is ACKed, and the delivery is settled.
func TestIntegrationDLQPostgresUnavailableLeavesPendingAndNoReexecution(t *testing.T) {
	testutil.RequireRedis(t)
	// Two handles on one isolated schema: the first is closed to take
	// PostgreSQL "down"; the second stands in for the recovered server.
	dsn := testutil.NewStateDSN(t)
	outage := testutil.OpenState(t, dsn)
	e := newEnv(t, ConsumerConfig{DLQStore: outage})
	id := e.xadd(t, `{"a":1}`)
	msg := e.readOneIntoPEL(t)

	// Take PostgreSQL down: the closed pool is a deterministic transport
	// failure for every DLQ operation.
	e.consumer.dlqStore = outage
	if err := outage.Close(); err != nil {
		t.Fatalf("close state handle: %v", err)
	}
	e.consumer.processMessage(context.Background(), msg, 1, exhaustion())

	if _, ok := e.pending()[id]; !ok {
		t.Fatalf("message %s must stay pending while PostgreSQL is unavailable", id)
	}

	// The redelivery must not re-execute the exhausted handler: the invocation's
	// exhausted marker in Redis is terminal, so the runner's claim is refused
	// and the exhaustion is re-reported without a new execution.
	p := NewInvocationState(context.Background(), e.consumer.invStateStore, e.stream, e.group, id, e.consumer.log)
	if started, claim, _, err := p.TryStart("fn/h", time.Hour); err != nil || started {
		t.Fatalf("TryStart redelivery = (started=%v claim=%+v err=%v), want a refused exhausted claim",
			started, claim, err)
	}

	// PostgreSQL returns: a healthy handle (same schema) stands in, the pending
	// entry is redelivered, and the delivery completes: the row is committed
	// once, ACKed, and settled.
	recovered := testutil.OpenState(t, dsn)
	e.consumer.dlqStore = recovered
	e.dlqStore = recovered
	redelivered := e.readPendingForEnv(t, id)
	e.consumer.processMessage(context.Background(), redelivered, 2, exhaustion())

	entries := e.dlqFor(id)
	if got := len(entries); got != 1 {
		t.Fatalf("DLQ rows after recovery = %d, want 1 (no duplicate, nothing lost)", got)
	}
	if entries[0].HandlerAttempts != 1 {
		t.Fatalf("recorded HandlerAttempts = %d, want the exhausted attempt 1", entries[0].HandlerAttempts)
	}
	if _, ok := e.pending()[id]; ok {
		t.Fatalf("message %s should be acked after the recovered write", id)
	}
	if entries[0].SettledAt == "" {
		t.Fatalf("delivery should be settled after the recovered ACK")
	}
}

// readPendingForEnv is readPendingForConsumer bound to this env's stream and
// group, so a redelivery can be driven through processMessage on the same
// consumer after the original delivery's ACK failed or never happened.
func (e *testEnv) readPendingForEnv(t *testing.T, wantID string) redis.XMessage {
	t.Helper()
	return readPendingForConsumer(t, e.client, e.stream, e.group, e.consumer.consumer, wantID)
}

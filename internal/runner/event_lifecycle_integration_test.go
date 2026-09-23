//go:build integration

// These tests drive the full event path against real Redis end to end: a real
// stream.Consumer is wired with runner.Handle, and the real runner executes
// (fake) handlers and drives the invocation-state lifecycle while the stream
// ACKs, leaves pending, and routes exhausted messages to the DLQ.
//
// They live in the runner package (rather than the stream package) because the
// stream package cannot import the runner (an import cycle), and because the
// semantics under test are the runner's message-level aggregate: a single
// message matching TWO invocations where one succeeds and the other exhausts
// after its configured retries. That aggregate decides whether the message is
// ACKed (all complete) or DLQ'd (all terminal, one exhausted), and it is what
// the stream layer acts on.
package runner

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/config"
	"relay/internal/stream"
	"relay/internal/testutil"
)

// eventEnv is a self-contained event-path integration harness: a real consumer
// (with the real runner's Handle) on a unixnano-unique stream/group.
type eventEnv struct {
	t      *testing.T
	client *redis.Client
	stream string
	group  string
	dlq    string
	ctx    context.Context
	cancel func()
	c      *stream.Consumer
	done   chan struct{}
	errCh  chan error
}

// newEventEnv builds the harness but does not start consuming; the caller wires
// the handler via start.
func newEventEnv(t *testing.T) *eventEnv {
	t.Helper()
	addr := testutil.EnvOr("REDIS_TEST_ADDR", "localhost:6379")
	redisOpts, err := config.RedisOptions(addr)
	if err != nil {
		t.Fatalf("redis options: %v", err)
	}
	cli := redis.NewClient(redisOpts)
	cctx, cancel := context.WithCancel(context.Background())
	prefix := fmt.Sprintf("evitest-%d", time.Now().UnixNano())
	streamName := prefix + "-stream"
	groupName := prefix + "-group"
	c := stream.NewConsumer(stream.ConsumerConfig{
		Client:          cli,
		Stream:          streamName,
		Group:           groupName,
		Consumer:        prefix + "-consumer",
		Log:             testutil.DiscardLogger(),
		MinPendingIdle:  300 * time.Millisecond,
		ReclaimInterval: 200 * time.Millisecond,
		Block:           300 * time.Millisecond,
	})
	if err := c.EnsureGroup(cctx); err != nil {
		cancel()
		_ = cli.Close()
		t.Fatalf("ensure group: %v", err)
	}
	e := &eventEnv{
		t:      t,
		client: cli,
		stream: streamName,
		group:  groupName,
		dlq:    stream.DLQStreamFor(streamName),
		ctx:    cctx,
		cancel: cancel,
		c:      c,
	}
	t.Cleanup(e.cleanup)
	return e
}

func (e *eventEnv) cleanup() {
	// Cancel FIRST, then wait for the consumer goroutine to actually exit.
	e.cancel()
	if e.done != nil {
		select {
		case <-e.done:
		case <-time.After(5 * time.Second):
		}
	}
	cctx := context.Background()
	_ = e.client.Del(cctx, e.stream, e.dlq).Err()
	// Best-effort removal of this env's invocation-state keys. The stream/group
	// names contain no ':' or '%', so a percent-encoding pass would be identity
	// and a direct prefix scan is safe and bounded to this env.
	var cursor uint64
	for {
		keys, next, err := e.client.Scan(cctx, cursor, "relay:invocation:"+e.stream+":*", 100).Result()
		if err != nil {
			break
		}
		if len(keys) > 0 {
			_ = e.client.Del(cctx, keys...).Err()
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	_ = e.client.Close()
}

func (e *eventEnv) start(handler stream.Handler) {
	e.t.Helper()
	e.done = make(chan struct{})
	e.errCh = make(chan error, 1)
	go func() {
		defer close(e.done)
		e.errCh <- e.c.Consume(e.ctx, handler)
	}()
}

// xadd writes an event to this env's stream and returns its ID.
func (e *eventEnv) xadd(event string) string {
	e.t.Helper()
	id, err := e.client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: e.stream,
		Values: map[string]any{"event": event},
	}).Result()
	if err != nil {
		e.t.Fatalf("xadd: %v", err)
	}
	return id
}

// pending reports the retry count and presence of a message in the group PEL.
func (e *eventEnv) pending(msgID string) (retry int64, ok bool) {
	entries, err := e.client.XPendingExt(context.Background(), &redis.XPendingExtArgs{
		Stream: e.stream,
		Group:  e.group,
		Start:  "-",
		End:    "+",
		Count:  100,
	}).Result()
	if err != nil {
		return 0, false
	}
	for _, pe := range entries {
		if pe.ID == msgID {
			return pe.RetryCount, true
		}
	}
	return 0, false
}

// dlqEntry returns the DLQ entry whose original_id is msgID, and whether it
// exists. When a message produces several entries (one per exhausted
// invocation) it returns the first; use dlqEntries to inspect them all.
func (e *eventEnv) dlqEntry(msgID string) (redis.XMessage, bool) {
	entries := e.dlqEntries(msgID)
	if len(entries) == 0 {
		return redis.XMessage{}, false
	}
	return entries[0], true
}

// dlqEntries returns every DLQ entry whose original_id is msgID, in stream
// order. Per-invocation DLQ means one message can produce several entries.
func (e *eventEnv) dlqEntries(msgID string) []redis.XMessage {
	msgs, err := e.client.XRange(context.Background(), e.dlq, "-", "+").Result()
	if err != nil {
		return nil
	}
	var out []redis.XMessage
	for _, m := range msgs {
		if id, _ := m.Values["original_id"].(string); id == msgID {
			out = append(out, m)
		}
	}
	return out
}

// invocationKey returns the invocation-state hash key for a message.
func (e *eventEnv) invocationKey(msgID string) string {
	return "relay:invocation:" + e.stream + ":" + e.group + ":" + msgID
}

func (e *eventEnv) stateField(msgID, invocation string) (string, error) {
	return e.client.HGet(context.Background(), e.invocationKey(msgID), invocation).Result()
}

func (e *eventEnv) hasStateKey(msgID string) bool {
	n, err := e.client.Exists(context.Background(), e.invocationKey(msgID)).Result()
	return err == nil && n == 1
}

// eventually polls pred until it holds, failing with what on timeout.
func (e *eventEnv) eventually(what string, pred func() bool) {
	e.t.Helper()
	testutil.WaitFor(e.t, 10*time.Second, what, pred)
}

// pastNextAttempt encodes an already-elapsed next_attempt_at marker carrying the
// given persisted attempt count, so a protected invocation becomes eligible on
// the next delivery without waiting out the real (1m+) retry backoff.
func pastNextAttempt(attempt int) string {
	return "next_attempt_at:" + strconv.FormatInt(time.Now().Add(-time.Hour).UnixNano(), 10) + "#" + strconv.Itoa(attempt)
}

// TestIntegrationEventSuccessAndExhaustionRoutesToDLQ is the primary end-to-end
// aggregate test for the requested scenario: one message matches handler A
// (succeeds) and handler B (fails its configured retries and exhausts). It pins
// the whole contract:
//
//   - A succeeds on delivery 1 and is marked complete; it is NOT re-run on any
//     later delivery (its execution count stays 1).
//   - B runs attempt 1 (retryable failure, backoff recorded) and, after the
//     backoff is expired, attempt 2 (retries:1 → exhausted). Its execution count
//     is 2 — the real handler execution count, not the Redis delivery count.
//   - While B is unresolved the message stays pending and is NOT ACKed.
//   - Once A is complete and B is exhausted (every matched invocation terminal),
//     the whole message is routed to the DLQ and the original is ACKed; the
//     DLQ's handler_attempts is B's exhaustion attempt (2) and deliveries is the
//     diagnostic Redis/PEL count (>= 2, including protected reclaims).
//   - The invocation-state key is cleared after the DLQ + ACK.
func TestIntegrationEventSuccessAndExhaustionRoutesToDLQ(t *testing.T) {
	_ = redisAvailable(t)
	aExec := &countingExecutor{}           // handler A: always succeeds
	bExec := &countingExecutor{fail: true} // handler B: always fails
	r := NewWithMetrics([]*PreparedFunction{
		fnWithRetries(t, "alpha", 0, aExec),
		fnWithRetries(t, "beta", 1, bExec),
	}, testutil.DiscardLogger(), nil)
	e := newEventEnv(t)
	id := e.xadd(`{"a":1}`)
	e.start(r.Handle)

	// Delivery 1: A succeeds; B fails attempt 1 (retryable) and records a
	// next_attempt_at marker. The message must stay pending (B unresolved).
	e.eventually("A marked complete and B backoff recorded", func() bool {
		av, aerr := e.stateField(id, "alpha/index.run")
		bv, berr := e.stateField(id, "beta/index.run")
		return aerr == nil && av == "ok" &&
			berr == nil && len(bv) > len("next_attempt_at:") && bv[:len("next_attempt_at:")] == "next_attempt_at:"
	})
	if _, ok := e.pending(id); !ok {
		t.Fatalf("message must stay pending while B is unresolved")
	}
	if _, ok := e.dlqEntry(id); ok {
		t.Fatalf("message must not be DLQ'd while B is unresolved")
	}
	if got := aExec.count(); got != 1 {
		t.Fatalf("A executions after delivery 1 = %d, want 1", got)
	}
	if got := bExec.count(); got != 1 {
		t.Fatalf("B executions after delivery 1 = %d, want 1", got)
	}

	// Expire B's retry backoff deterministically (the real backoff is 1m+),
	// carrying its persisted attempt count (1) forward so the next execution is
	// attempt 2, which exhausts (retries:1 → maxAttempts 2).
	if err := e.client.HSet(context.Background(), e.invocationKey(id), "beta/index.run", pastNextAttempt(1)).Err(); err != nil {
		t.Fatalf("expire retry marker: %v", err)
	}

	// Delivery 2 (reclaim): A is skipped (already complete), B executes attempt 2
	// and exhausts. Every matched invocation is terminal → the message routes to
	// the DLQ and is ACKed.
	e.eventually("message routed to DLQ", func() bool {
		_, ok := e.dlqEntry(id)
		return ok
	})
	e.eventually("message acked (gone from PEL)", func() bool {
		_, ok := e.pending(id)
		return !ok
	})
	e.eventually("invocation-state key cleared after DLQ", func() bool {
		return !e.hasStateKey(id)
	})

	// The successful handler was never re-run; each handler executed exactly its
	// real attempt count.
	if got := aExec.count(); got != 1 {
		t.Fatalf("A executions = %d, want 1 (successful handler must not be re-run)", got)
	}
	if got := bExec.count(); got != 2 {
		t.Fatalf("B executions = %d, want 2 (attempt 1 fails, attempt 2 exhausts)", got)
	}

	m, ok := e.dlqEntry(id)
	if !ok {
		t.Fatalf("expected a DLQ entry for %s", id)
	}
	if m.Values["original_stream"] != e.stream {
		t.Errorf("original_stream = %v, want %v", m.Values["original_stream"], e.stream)
	}
	// The DLQ entry names the exact exhausted invocation (B), not the message or
	// a sibling that succeeded.
	if m.Values["function"] != "beta" || m.Values["handler"] != "index.run" {
		t.Errorf("function/handler = %v/%v, want beta/index.run", m.Values["function"], m.Values["handler"])
	}
	// handler_attempts is the execution attempt that exhausted (2), attributed
	// from the invocation retry state.
	if m.Values["handler_attempts"] != "2" {
		t.Errorf("handler_attempts = %v, want 2 (B's exhausting execution attempt)", m.Values["handler_attempts"])
	}
	// Exactly one entry: A succeeded and must never be dead-lettered.
	if entries := e.dlqEntries(id); len(entries) != 1 {
		t.Errorf("DLQ entries = %d, want 1 (only the exhausted B)", len(entries))
	}
	// deliveries is the diagnostic Redis/PEL count and is >= handler_attempts
	// (protected reclaims count as deliveries without advancing the handler
	// attempt).
	deliveries, err := strconv.Atoi(m.Values["deliveries"].(string))
	if err != nil {
		t.Fatalf("deliveries = %v, not an int: %v", m.Values["deliveries"], err)
	}
	if deliveries < 2 {
		t.Errorf("deliveries = %d, want >= 2", deliveries)
	}
	if m.Values["reason"] == "" {
		t.Errorf("reason missing")
	}
}

// TestIntegrationMultipleExhaustionsProducePerInvocationDLQEntries pins the
// per-invocation DLQ contract end to end: one message matching a successful
// handler A and TWO always-failing handlers B and C (retries:0, so each exhausts
// on attempt 1) must produce exactly TWO DLQ entries — one per exhausted
// invocation — each carrying its own exact function/handler and
// handler_attempts, while the successful A is never dead-lettered.
func TestIntegrationMultipleExhaustionsProducePerInvocationDLQEntries(t *testing.T) {
	_ = redisAvailable(t)
	aExec := &countingExecutor{}           // A: succeeds, must never be DLQ'd
	bExec := &countingExecutor{fail: true} // B: exhausts attempt 1
	cExec := &countingExecutor{fail: true} // C: exhausts attempt 1
	r := NewWithMetrics([]*PreparedFunction{
		fnWithRetries(t, "alpha", 0, aExec),
		fnWithRetries(t, "beta", 0, bExec),
		fnWithRetries(t, "gamma", 0, cExec),
	}, testutil.DiscardLogger(), nil)
	e := newEventEnv(t)
	id := e.xadd(`{"a":1}`)
	e.start(r.Handle)

	// The message is terminal on delivery 1: A completes, B and C each exhaust.
	// The whole lifecycle (including the state clear) can finish in one delivery,
	// so wait on the durable outcomes (the two DLQ entries and the ACK) rather
	// than transient intermediate state.
	e.eventually("two DLQ entries written", func() bool {
		return len(e.dlqEntries(id)) == 2
	})
	e.eventually("message acked (gone from PEL)", func() bool {
		_, ok := e.pending(id)
		return !ok
	})
	e.eventually("invocation-state key cleared after DLQ", func() bool {
		return !e.hasStateKey(id)
	})

	// Each handler executed exactly once and was never re-run after exhaustion.
	if aExec.count() != 1 || bExec.count() != 1 || cExec.count() != 1 {
		t.Fatalf("executions alpha=%d beta=%d gamma=%d, want 1/1/1", aExec.count(), bExec.count(), cExec.count())
	}

	byInv := map[string]redis.XMessage{}
	for _, m := range e.dlqEntries(id) {
		fn, _ := m.Values["function"].(string)
		h, _ := m.Values["handler"].(string)
		byInv[fn+"/"+h] = m
	}
	if _, ok := byInv["alpha/index.run"]; ok {
		t.Fatalf("successful alpha must never be dead-lettered: %v", byInv)
	}
	for _, inv := range []string{"beta/index.run", "gamma/index.run"} {
		m, ok := byInv[inv]
		if !ok {
			t.Fatalf("missing DLQ entry for exhausted %s (got %v)", inv, byInv)
		}
		if m.Values["handler_attempts"] != "1" {
			t.Errorf("%s handler_attempts = %v, want 1 (retries:0 exhausts on attempt 1)", inv, m.Values["handler_attempts"])
		}
		if m.Values["original_id"] != id {
			t.Errorf("%s original_id = %v, want %s", inv, m.Values["original_id"], id)
		}
		if m.Values["reason"] == "" {
			t.Errorf("%s reason missing", inv)
		}
	}
}

// TestIntegrationDLQWriteFailureRecoveryReRoutesAfterExhaustion is the regression
// test for the redelivery-after-exhaustion path. Delivery 1 exhausts B (A
// completes) while the DLQ stream name is a wrong-type key, so the DLQ XADD
// fails: the message must stay pending with its invocation state retained (A
// complete, B exhausted). Once the DLQ write can succeed, a reclaim redelivery
// must re-report exhaustion and route the message to the DLQ — NOT return nil
// and ACK a message with no DLQ entry, which would silently lose it.
func TestIntegrationDLQWriteFailureRecoveryReRoutesAfterExhaustion(t *testing.T) {
	_ = redisAvailable(t)
	aExec := &countingExecutor{}           // A: succeeds
	bExec := &countingExecutor{fail: true} // B: exhausts on its first failure (retries:0)
	r := NewWithMetrics([]*PreparedFunction{
		fnWithRetries(t, "alpha", 0, aExec),
		fnWithRetries(t, "beta", 0, bExec),
	}, testutil.DiscardLogger(), nil)
	e := newEventEnv(t)

	// Make the DLQ XADD fail by turning the DLQ stream name into a wrong-type
	// (string) key BEFORE the message is processed.
	if err := e.client.Set(context.Background(), e.dlq, "not-a-stream", 0).Err(); err != nil {
		t.Fatalf("set wrong-type DLQ key: %v", err)
	}

	id := e.xadd(`{"a":1}`)
	e.start(r.Handle)

	// Delivery 1: A completes, B exhausts; the DLQ write fails, so the message
	// stays pending and the invocation state is retained.
	e.eventually("A complete and B exhausted in retained state", func() bool {
		av, aerr := e.stateField(id, "alpha/index.run")
		bv, berr := e.stateField(id, "beta/index.run")
		return aerr == nil && av == "ok" && berr == nil && len(bv) > len("exhausted:") && bv[:len("exhausted:")] == "exhausted:"
	})
	if _, ok := e.pending(id); !ok {
		t.Fatalf("message must stay pending when the DLQ write fails")
	}
	if _, ok := e.dlqEntry(id); ok {
		t.Fatalf("no DLQ entry can exist while the DLQ write is failing")
	}

	// Now let the DLQ write succeed: remove the wrong-type key. The reclaim loop
	// redelivers the still-pending message; the redelivery must re-report
	// exhaustion and route it to the DLQ (the regression).
	if err := e.client.Del(context.Background(), e.dlq).Err(); err != nil {
		t.Fatalf("del wrong-type DLQ key: %v", err)
	}
	e.eventually("message re-routed to DLQ on reclaim", func() bool {
		_, ok := e.dlqEntry(id)
		return ok
	})
	e.eventually("message acked after successful DLQ write", func() bool {
		_, ok := e.pending(id)
		return !ok
	})
	e.eventually("invocation-state key cleared after DLQ", func() bool {
		return !e.hasStateKey(id)
	})

	// Neither handler re-ran: A completed once, B exhausted once.
	if got := aExec.count(); got != 1 {
		t.Fatalf("A executions = %d, want 1 (must not re-run a completed invocation)", got)
	}
	if got := bExec.count(); got != 1 {
		t.Fatalf("B executions = %d, want 1 (exhausted, must not re-run)", got)
	}
	m, ok := e.dlqEntry(id)
	if !ok {
		t.Fatalf("expected a DLQ entry for %s after recovery", id)
	}
	if m.Values["handler_attempts"] != "1" {
		t.Errorf("handler_attempts = %v, want 1 (persisted exhausted attempt)", m.Values["handler_attempts"])
	}
}

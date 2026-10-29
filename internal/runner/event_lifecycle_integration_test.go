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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/app"
	"relay/internal/dlq"
	"relay/internal/observability/metrics"
	"relay/internal/state"
	"relay/internal/stream"
	"relay/internal/testutil"
)

// eventEnv is a self-contained event-path integration harness: a real consumer
// (with the real runner's Handle) on a unixnano-unique stream/group, with the
// production PostgreSQL DLQ store wired on a fresh per-test schema.
type eventEnv struct {
	t        *testing.T
	client   *redis.Client
	stream   string
	group    string
	dlqStore dlq.Store
	pg       *state.State
	ctx      context.Context
	cancel   func()
	c        *stream.Consumer
	done     chan struct{}
	errCh    chan error
}

// newEventEnv builds the harness but does not start consuming; the caller wires
// the handler via start.
func newEventEnv(t *testing.T) *eventEnv {
	t.Helper()
	return newEventEnvWith(t, 0, nil)
}

// newEventEnvWith is newEventEnv with an explicit local buffer capacity
// (ConsumerConfig.MaxBufferedEvents; 0 applies the stream default of 16) and an
// optional metrics registry so a test can observe the live buffered_events
// gauge. It is a test-only setup seam; the production consumer is unchanged. The
// Redis client is the fail-fast one owned by testutil.RequireRedis; its cleanup
// closes the client after this harness's own key cleanup has run.
func newEventEnvWith(t *testing.T, maxBufferedEvents int, m *metrics.Registry) *eventEnv {
	t.Helper()
	return newEventEnvWithDLQ(t, maxBufferedEvents, m, nil)
}

// newEventEnvWithDLQ lets a caller decorate the DLQ store the consumer is wired
// with (e.g. a fail-injecting wrapper). decorate may be nil.
func newEventEnvWithDLQ(t *testing.T, maxBufferedEvents int, m *metrics.Registry, decorate func(dlq.Store) dlq.Store) *eventEnv {
	t.Helper()
	cli := testutil.RequireRedis(t)
	cctx, cancel := context.WithCancel(context.Background())
	prefix := fmt.Sprintf("evitest-%d", time.Now().UnixNano())
	streamName := prefix + "-stream"
	groupName := prefix + "-group"
	// The durable DLQ store: fresh per-test PostgreSQL schema, exactly the
	// production wiring (newStreamConsumer wires the state handle).
	pg := testutil.NewState(t)
	dlqStore := dlq.Store(pg)
	if decorate != nil {
		dlqStore = decorate(pg)
	}
	c := stream.NewConsumer(stream.ConsumerConfig{
		Client:            cli,
		Stream:            streamName,
		Group:             groupName,
		Consumer:          prefix + "-consumer",
		Log:               testutil.DiscardLogger(),
		MinPendingIdle:    300 * time.Millisecond,
		ReclaimInterval:   200 * time.Millisecond,
		Block:             300 * time.Millisecond,
		MaxBufferedEvents: maxBufferedEvents,
		Metrics:           m,
		DLQStore:          dlqStore,
	})
	if err := c.EnsureGroup(cctx); err != nil {
		cancel()
		t.Fatalf("ensure group: %v", err)
	}
	e := &eventEnv{
		t:        t,
		client:   cli,
		stream:   streamName,
		group:    groupName,
		dlqStore: dlqStore,
		pg:       pg,
		ctx:      cctx,
		cancel:   cancel,
		c:        c,
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
	_ = e.client.Del(cctx, e.stream).Err()
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
	// The client is owned by testutil.RequireRedis, whose cleanup closes it after
	// this cleanup (t.Cleanup is LIFO).
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

// dlqEntry returns the durable DLQ entry whose source message ID is msgID, and
// whether it exists. When a message produces several entries (one per exhausted
// invocation) it returns the first in write order; use dlqEntries to inspect
// them all.
func (e *eventEnv) dlqEntry(msgID string) (dlq.Entry, bool) {
	entries := e.dlqEntries(msgID)
	if len(entries) == 0 {
		return dlq.Entry{}, false
	}
	return entries[0], true
}

// dlqEntries returns every DLQ entry whose source message ID is msgID, in write
// order. Per-invocation DLQ means one message can produce several entries. A
// store read error fails loudly: the DLQ assertions are meaningless without
// their rows.
func (e *eventEnv) dlqEntries(msgID string) []dlq.Entry {
	entries, err := e.dlqStore.ListDLQEntries(context.Background())
	if err != nil {
		e.t.Fatalf("list DLQ entries: %v", err)
	}
	var out []dlq.Entry
	for _, entry := range entries {
		if entry.SourceMessageID == msgID {
			out = append(out, entry)
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

// isRetained reports whether the message's invocation-state hash has been
// switched to terminal retention: still present, carrying the reserved terminal
// marker, and holding a positive retention TTL. This is the post-ACK contract
// that replaced the old eager delete.
func (e *eventEnv) isRetained(msgID string) bool {
	key := e.invocationKey(msgID)
	v, err := e.client.HGet(context.Background(), key, "__terminal").Result()
	if err != nil || v != "1" {
		return false
	}
	d, err := e.client.PTTL(context.Background(), key).Result()
	return err == nil && d > 0
}

// eventually polls pred until it holds, failing with what on timeout.
func (e *eventEnv) eventually(what string, pred func() bool) {
	e.t.Helper()
	testutil.WaitFor(e.t, 10*time.Second, what, pred)
}

// pastNextAttempt encodes an already-elapsed next_attempt_at marker carrying the
// given persisted attempt count and a valid claim token, so a protected
// invocation becomes eligible on the next delivery without waiting out the real
// (1m+) retry backoff.
func pastNextAttempt(attempt int) string {
	return "next_attempt_at:" + strconv.FormatInt(time.Now().Add(-time.Hour).UnixMilli(), 10) +
		":" + strconv.Itoa(attempt) + ":" + strings.Repeat("a", 32)
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
	aExec := &countingExecutor{}           // handler A: always succeeds
	bExec := &countingExecutor{fail: true} // handler B: always fails
	r := NewWithMetrics([]*PreparedApp{
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
	if entries := e.dlqEntries(id); len(entries) != 0 {
		t.Fatalf("message must not be DLQ'd while B is unresolved, got %d rows", len(entries))
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
	e.eventually("invocation-state key terminal-retained after DLQ", func() bool {
		return e.isRetained(id)
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
	if m.Source != e.stream {
		t.Errorf("Source = %q, want %q", m.Source, e.stream)
	}
	// The DLQ entry names the exact exhausted invocation (B), not the message or
	// a sibling that succeeded.
	if m.App != "beta" || m.Handler != "index.run" {
		t.Errorf("function/handler = %q/%q, want beta/index.run", m.App, m.Handler)
	}
	// handler_attempts is the execution attempt that exhausted (2), attributed
	// from the invocation retry state.
	if m.HandlerAttempts != 2 {
		t.Errorf("HandlerAttempts = %d, want 2 (B's exhausting execution attempt)", m.HandlerAttempts)
	}
	// Exactly one entry: A succeeded and must never be dead-lettered.
	if entries := e.dlqEntries(id); len(entries) != 1 {
		t.Errorf("DLQ entries = %d, want 1 (only the exhausted B)", len(entries))
	}
	// deliveries is the diagnostic Redis/PEL count and is >= handler_attempts
	// (protected reclaims count as deliveries without advancing the handler
	// attempt).
	if m.Deliveries < 2 {
		t.Errorf("Deliveries = %d, want >= 2", m.Deliveries)
	}
	if m.Reason == "" {
		t.Errorf("reason missing")
	}
	// Every matched invocation is terminal, so the confirmed ACK settles the
	// delivery's rows. The settlement write follows the ACK on the wire, so it
	// is observed by polling (an observed ACK does not yet prove the
	// PostgreSQL update committed).
	e.eventually("the DLQ entry settled after the confirmed ACK", func() bool {
		entry, ok := e.dlqEntry(id)
		return ok && entry.SettledAt != ""
	})
}

// TestIntegrationMultipleExhaustionsProducePerInvocationDLQEntries pins the
// per-invocation DLQ contract end to end: one message matching a successful
// handler A and TWO always-failing handlers B and C (retries:0, so each exhausts
// on attempt 1) must produce exactly TWO DLQ entries — one per exhausted
// invocation — each carrying its own exact app/handler and
// handler_attempts, while the successful A is never dead-lettered.
func TestIntegrationMultipleExhaustionsProducePerInvocationDLQEntries(t *testing.T) {
	aExec := &countingExecutor{}           // A: succeeds, must never be DLQ'd
	bExec := &countingExecutor{fail: true} // B: exhausts attempt 1
	cExec := &countingExecutor{fail: true} // C: exhausts attempt 1
	r := NewWithMetrics([]*PreparedApp{
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
	e.eventually("invocation-state key terminal-retained after DLQ", func() bool {
		return e.isRetained(id)
	})

	// Each handler executed exactly once and was never re-run after exhaustion.
	if aExec.count() != 1 || bExec.count() != 1 || cExec.count() != 1 {
		t.Fatalf("executions alpha=%d beta=%d gamma=%d, want 1/1/1", aExec.count(), bExec.count(), cExec.count())
	}

	byInv := map[string]dlq.Entry{}
	for _, m := range e.dlqEntries(id) {
		byInv[m.App+"/"+m.Handler] = m
	}
	if _, ok := byInv["alpha/index.run"]; ok {
		t.Fatalf("successful alpha must never be dead-lettered: %v", byInv)
	}
	for _, inv := range []string{"beta/index.run", "gamma/index.run"} {
		m, ok := byInv[inv]
		if !ok {
			t.Fatalf("missing DLQ entry for exhausted %s (got %v)", inv, byInv)
		}
		if m.HandlerAttempts != 1 {
			t.Errorf("%s HandlerAttempts = %d, want 1 (retries:0 exhausts on attempt 1)", inv, m.HandlerAttempts)
		}
		if m.SourceMessageID != id {
			t.Errorf("%s SourceMessageID = %q, want %s", inv, m.SourceMessageID, id)
		}
		if m.Reason == "" {
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
	aExec := &countingExecutor{}           // A: succeeds
	bExec := &countingExecutor{fail: true} // B: exhausts on its first failure (retries:0)
	r := NewWithMetrics([]*PreparedApp{
		fnWithRetries(t, "alpha", 0, aExec),
		fnWithRetries(t, "beta", 0, bExec),
	}, testutil.DiscardLogger(), nil)
	// Make the PostgreSQL DLQ insert fail by wiring in a failing store wrapper
	// BEFORE the message is processed (the production transport, so the row
	// write and the ACK ordering are exercised for real).
	failing := &failDLQStore{err: errors.New("injected postgres outage")}
	e := newEventEnvWithDLQ(t, 0, nil, func(store dlq.Store) dlq.Store {
		failing.Store = store
		return failing
	})

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

	// Now let the DLQ write succeed: unblock the wrapper. The reclaim loop
	// redelivers the still-pending message; the redelivery must re-report
	// exhaustion and route it to the DLQ (the regression).
	failing.ok.Store(true)
	e.eventually("message re-routed to DLQ on reclaim", func() bool {
		_, ok := e.dlqEntry(id)
		return ok
	})
	e.eventually("message acked after successful DLQ write", func() bool {
		_, ok := e.pending(id)
		return !ok
	})
	e.eventually("invocation-state key terminal-retained after DLQ", func() bool {
		return e.isRetained(id)
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
	if m.HandlerAttempts != 1 {
		t.Errorf("HandlerAttempts = %d, want 1 (persisted exhausted attempt)", m.HandlerAttempts)
	}
	// The settlement write follows the ACK on the wire, so it is observed by
	// polling rather than immediately after the ACK becomes observable.
	e.eventually("the recovered ACK settled the delivery", func() bool {
		entry, ok := e.dlqEntry(id)
		return ok && entry.SettledAt != ""
	})
}

// TestIntegrationMatchedButUnavailableStaysPendingThenCompletes is the end-to-end
// regression for the matched-but-unavailable fix. A message matching ONLY a
// configured-but-unavailable app must:
//
//   - be delivered into the PEL and STAY pending across reclaim cycles (never
//     ACKed and never DLQ'd) while the app is unavailable — unavailability
//     alone must not dead-letter, and it must not be treated as unmatched (which
//     would ACK it);
//   - once the app is rebuilt and becomes available (the registry entry is
//     swapped for a runnable one under the SAME name, preserving invocation-state
//     identity), a reclaim redelivery must run the handler, ACK the message, and
//     clear the invocation-state key.
func TestIntegrationMatchedButUnavailableStaysPendingThenCompletes(t *testing.T) {
	recovered := &countingExecutor{}
	// Start with the app registered but unavailable (image not built).
	r := NewWithMetrics([]*PreparedApp{unavailableMatchFn(t, "broken")}, testutil.DiscardLogger(), nil)
	e := newEventEnv(t)
	id := e.xadd(`{"a":1}`)
	e.start(r.Handle)

	// The message is delivered into the PEL and stays pending across the reclaim
	// grace window, never ACKed and never DLQ'd (matched but temporarily
	// unavailable).
	e.eventually("unavailable event delivered into PEL", func() bool {
		_, ok := e.pending(id)
		return ok
	})
	deadline := time.Now().Add(1200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, ok := e.pending(id); !ok {
			t.Fatalf("matched-but-unavailable event must stay pending, not be acked")
		}
		if _, ok := e.dlqEntry(id); ok {
			t.Fatalf("matched-but-unavailable event must not be DLQ'd for unavailability alone")
		}
		time.Sleep(25 * time.Millisecond)
	}
	if recovered.count() != 0 {
		t.Fatalf("unavailable function must not execute: calls = %d, want 0", recovered.count())
	}

	// The app is rebuilt and becomes available under the SAME name, so the
	// invocation identity is preserved. A reclaim redelivery runs the handler,
	// ACKs the message, and clears the invocation-state key.
	r.Registry().Replace("broken", alwaysMatchFn(t, "broken", recovered))
	e.eventually("recovered function executed", func() bool {
		return recovered.count() == 1
	})
	e.eventually("message acked (gone from PEL)", func() bool {
		_, ok := e.pending(id)
		return !ok
	})
	e.eventually("invocation-state key terminal-retained after ack", func() bool {
		return e.isRetained(id)
	})
	if _, ok := e.dlqEntry(id); ok {
		t.Fatalf("recovered event must never be DLQ'd")
	}
}

// TestIntegrationMixedFanOutAvailableCompletesUnavailableStaysPending pins the
// mixed fan-out contract end to end: one message matching an AVAILABLE app
// (which succeeds and is marked complete) and an UNAVAILABLE app (which
// cannot run) stays pending across reclaims. The available invocation runs
// exactly once — it is NOT re-run by the reclaims — while the unavailable
// invocation remains unresolved, so the message is never ACKed and never DLQ'd.
func TestIntegrationMixedFanOutAvailableCompletesUnavailableStaysPending(t *testing.T) {
	availableExec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedApp{
		alwaysMatchFn(t, "available", availableExec),
		unavailableMatchFn(t, "broken"),
	}, testutil.DiscardLogger(), nil)
	e := newEventEnv(t)
	id := e.xadd(`{"a":1}`)
	e.start(r.Handle)

	// The available invocation completes; the unavailable sibling keeps the
	// message pending.
	e.eventually("available invocation marked complete", func() bool {
		v, err := e.stateField(id, "available/index.run")
		return err == nil && v == "ok"
	})
	if got := availableExec.count(); got != 1 {
		t.Fatalf("available executions = %d, want 1", got)
	}

	// Across the reclaim grace window the message stays pending (unavailable
	// sibling unresolved) and the completed invocation is never re-run.
	deadline := time.Now().Add(1200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, ok := e.pending(id); !ok {
			t.Fatalf("mixed message must stay pending while the unavailable sibling is unresolved")
		}
		if _, ok := e.dlqEntry(id); ok {
			t.Fatalf("mixed message must not be DLQ'd for an unavailable sibling")
		}
		time.Sleep(25 * time.Millisecond)
	}
	if got := availableExec.count(); got != 1 {
		t.Fatalf("completed available invocation must not re-run: calls = %d, want 1", got)
	}
}

// TestIntegrationPendingDesiredRuleStaysPendingThenRuns is the end-to-end
// regression for the new-app / new-rule event-loss fix. A brand-new app is
// represented in the registry ONLY by its PENDING desired template (no active,
// runnable generation) — exactly the state during its first image build. A
// message matching a pending rule must:
//
//   - be delivered into the PEL and STAY pending across reclaim cycles (never
//     ACKed as unmatched, never DLQ'd) while the generation is pending;
//   - not execute any handler while pending;
//   - once the generation becomes ACTIVE (the pending entry is replaced by a
//     runnable app with the SAME name under the SAME invocation identity), a
//     reclaim redelivery must run the handler, ACK the message, and clear the
//     invocation-state key.
func TestIntegrationPendingDesiredRuleStaysPendingThenRuns(t *testing.T) {
	recovered := &countingExecutor{}
	r := NewWithMetrics(nil, testutil.DiscardLogger(), nil)
	// Represent the app as pending-only: the desired rules are matchable but not
	// runnable.
	r.Registry().SetPending("pending-app", app.App{
		Name:     "pending-app",
		Template: mustTemplateForIntegration(t, pendingIntegrationTemplate),
	})
	e := newEventEnv(t)
	id := e.xadd(`{"event_name":"created"}`)
	e.start(r.Handle)

	// The message is delivered into the PEL and stays pending across the reclaim
	// grace window, never ACKed (matched-but-unavailable, not unmatched) and
	// never DLQ'd; no handler runs.
	e.eventually("pending event delivered into PEL", func() bool {
		_, ok := e.pending(id)
		return ok
	})
	deadline := time.Now().Add(1200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, ok := e.pending(id); !ok {
			t.Fatalf("pending-desired event must stay pending, not be acked as unmatched")
		}
		if _, ok := e.dlqEntry(id); ok {
			t.Fatalf("pending-desired event must not be DLQ'd")
		}
		time.Sleep(25 * time.Millisecond)
	}
	if recovered.count() != 0 {
		t.Fatalf("pending generation must not execute: calls = %d, want 0", recovered.count())
	}

	// Preparation succeeds: publish the runnable generation under the SAME name
	// (Replace clears the pending entry atomically). A reclaim redelivery runs the
	// handler, ACKs the message, and clears the invocation-state key.
	r.Registry().Replace("pending-app", alwaysMatchFn(t, "pending-app", recovered))
	if r.Registry().HasPending("pending-app") {
		t.Fatal("Replace must clear the pending entry")
	}
	e.eventually("activated handler executed", func() bool {
		return recovered.count() == 1
	})
	e.eventually("message acked (gone from PEL)", func() bool {
		_, ok := e.pending(id)
		return !ok
	})
	e.eventually("invocation-state key terminal-retained after ack", func() bool {
		return e.isRetained(id)
	})
	if _, ok := e.dlqEntry(id); ok {
		t.Fatalf("activated event must never be DLQ'd")
	}
}

// pendingIntegrationTemplate is a single-rule template matching the integration
// event used above.
const pendingIntegrationTemplate = `runtime: node24
events:
  - handler: handler.created
    pattern:
      event_name: [created]
`

// mustTemplateForIntegration parses a template for the pending integration test.
func mustTemplateForIntegration(t *testing.T, yaml string) *app.Template {
	t.Helper()
	tmpl, err := app.ParseTemplate([]byte(yaml))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	return tmpl
}

// runningValue encodes a persisted running marker carrying the given absolute
// deadline and attempt, with a valid opaque lowercase-hex claim token. It lets a
// test seed the exact state a process crash after a confirmed TryStart leaves
// behind (the handler never entered).
func runningValue(deadline time.Time, attempt int) string {
	return "running:" + strconv.FormatInt(deadline.UnixMilli(), 10) +
		":" + strconv.Itoa(attempt) + ":" + strings.Repeat("a", 32)
}

// TestIntegrationCrashAfterClaimSpendsAttemptWithoutHandlerExecution pins the
// F-002 claim-before-execute window end to end against real Redis. A running
// marker for attempt 1 with an already-elapsed deadline is exactly the value a
// confirmed TryStart leaves if the process dies before the handler runs (the
// TryStart transition itself is pinned by the stream package's
// TestIntegrationAtomicCrashReclaimAfterDeadline). Once the reclaim observes the
// elapsed deadline it claims attempt 2, runs the handler exactly once, and — with
// retries:0 — exhausts. The whole message is terminal, so it routes to the DLQ and
// is ACKed, and the DLQ's handler_attempts is 2 even though the handler executed
// once: the unexecuted claim was spent. Note the DLQ is driven by the reclaim's
// real failing EXECUTION, not by the crash: a crash alone claims an attempt but
// never exhausts (TryStart has no retry budget).
func TestIntegrationCrashAfterClaimSpendsAttemptWithoutHandlerExecution(t *testing.T) {
	exec := &countingExecutor{fail: true}
	r := NewWithMetrics([]*PreparedApp{fnWithRetries(t, "beta", 0, exec)}, testutil.DiscardLogger(), nil)
	e := newEventEnv(t)
	id := e.xadd(`{"a":1}`)

	// Seed the post-claim crash state BEFORE the consumer starts: attempt 1 is
	// persisted as running with an already-elapsed deadline, and no transition ever
	// followed.
	if err := e.client.HSet(context.Background(), e.invocationKey(id),
		"beta/index.run", runningValue(time.Now().Add(-time.Hour), 1)).Err(); err != nil {
		t.Fatalf("seed crash state: %v", err)
	}

	e.start(r.Handle)

	// The reclaim claims attempt 2, which fails and exhausts: the message routes to
	// the DLQ and is ACKed.
	e.eventually("message routed to DLQ after the post-crash attempt", func() bool {
		_, ok := e.dlqEntry(id)
		return ok
	})
	e.eventually("message acked (gone from PEL)", func() bool {
		_, ok := e.pending(id)
		return !ok
	})
	e.eventually("invocation-state key terminal-retained after DLQ", func() bool {
		return e.isRetained(id)
	})

	// The handler ran exactly once (attempt 2); the crashed claim of attempt 1 was
	// spent without executing.
	if got := exec.count(); got != 1 {
		t.Fatalf("handler executions = %d, want 1 (attempt 1 was claimed but never ran)", got)
	}
	m, ok := e.dlqEntry(id)
	if !ok {
		t.Fatalf("expected a DLQ entry for %s", id)
	}
	if m.HandlerAttempts != 2 {
		t.Errorf("HandlerAttempts = %d, want 2 (the claim before the crash consumed attempt 1)", m.HandlerAttempts)
	}
	if m.App != "beta" || m.Handler != "index.run" {
		t.Errorf("DLQ app/handler = %q/%q, want beta/index.run", m.App, m.Handler)
	}
}

// TestIntegrationProtectedClaimStaysPendingWithoutAckOrDLQ pins the other half of
// the window against real Redis: while the persisted running marker's deadline has
// not elapsed (the crashed attempt could still be executing elsewhere, or is being
// waited out), every reclaim leaves the message PENDING — never ACKed, never
// DLQ'd, and the handler is never called. Nothing is charged until the claim's
// deadline actually elapses.
func TestIntegrationProtectedClaimStaysPendingWithoutAckOrDLQ(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedApp{fnWithRetries(t, "beta", 0, exec)}, testutil.DiscardLogger(), nil)
	e := newEventEnv(t)
	id := e.xadd(`{"a":1}`)

	// A future deadline (the crashed attempt's protected window): reclaims must not
	// run the handler.
	if err := e.client.HSet(context.Background(), e.invocationKey(id),
		"beta/index.run", runningValue(time.Now().Add(time.Hour), 1)).Err(); err != nil {
		t.Fatalf("seed protected state: %v", err)
	}

	// Count deliveries so the test observes the protected path REPEATEDLY (the
	// first delivery plus at least one reclaim) without any wall-clock sleep: each
	// reclaim re-delivers the still-pending message and must again skip it. An ACK
	// or DLQ on the first delivery would stop re-delivery, so waiting for at least
	// two deliveries is itself the deterministic proof that the message stayed
	// pending across a reclaim.
	var deliveries atomic.Int64
	e.start(func(ctx context.Context, msgID string, event map[string]any) error {
		deliveries.Add(1)
		return r.Handle(ctx, msgID, event)
	})

	e.eventually("protected invocation delivered, then reclaimed at least once", func() bool {
		return deliveries.Load() >= 2
	})

	// Invariants hold at every protected delivery: still pending, never DLQ'd, the
	// handler never ran, and the running marker is untouched.
	if _, ok := e.pending(id); !ok {
		t.Fatalf("message left the PEL while its invocation was protected")
	}
	if _, ok := e.dlqEntry(id); ok {
		t.Fatalf("a protected invocation must never be DLQ'd")
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls = %d, want 0 (protected claim must not execute)", exec.count())
	}
	if v, err := e.stateField(id, "beta/index.run"); err != nil || !strings.HasPrefix(v, "running:") {
		t.Fatalf("marker = %q, err=%v, want an unchanged running marker", v, err)
	}
}

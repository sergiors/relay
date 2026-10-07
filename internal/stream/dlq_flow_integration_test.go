//go:build integration

// This file drives the F-008 atomic-DLQ contract through the REAL
// routeToDLQ/processMessage flow, rather than seeding the invocation-state hash
// and the DLQ stream directly (which the state/store-level tests in
// dlq_atomic_integration_test.go and redis_streams_integration_test.go already
// do). It proves the two properties a seeded test cannot:
//
//   - The source XACK that follows a genuinely-persisted DLQ entry is forced to
//     fail deterministically, and a redelivery completes the message without a
//     second entry. This exercises the real append+marker-then-XACK ordering,
//     not a hand-built partial state.
//   - A multi-invocation route where the first persist really succeeds and the
//     second is deterministically refused leaves the message pending with
//     exactly the first entry/marker; a redelivery writes only the missing
//     second entry and ACKs.
//
// Excluded from the default suite by the integration build tag; REQUIRES Redis
// at REDIS_TEST_ADDR (default localhost:6379), failing rather than skipping when
// it is absent.
package stream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/testutil"
)

// failAckAfterPersistStore decorates the real Redis-backed invocation store.
// After the FIRST successful persistDLQ (the atomic append+marker) it runs
// afterPersist, which the test uses to close the consumer's Redis client: the
// XACK routeToDLQ performs immediately after is then deterministically refused
// (redis.ErrClosed) while the DLQ entry and its persistence marker are already
// durably written. Every other method is delegated to the wrapped store
// unchanged, so this is a narrow failure seam, not a reimplementation.
type failAckAfterPersistStore struct {
	invocationStateStore
	afterPersist func()
	once         sync.Once
}

func (s *failAckAfterPersistStore) persistDLQ(
	ctx context.Context,
	dlqStream, stream, group, msgID, invocation string,
	claim InvocationClaim,
	entry map[string]any,
) (bool, error) {
	wrote, err := s.invocationStateStore.persistDLQ(ctx, dlqStream, stream, group, msgID, invocation, claim, entry)
	if err == nil && wrote {
		s.once.Do(s.afterPersist)
	}
	return wrote, err
}

// failNthPersistDLQStore decorates the real Redis-backed invocation store and
// injects one error on exactly the nth persistDLQ call, delegating every other
// call to Redis. It makes a multi-invocation partial DLQ write deterministic:
// the first entry is really persisted, the second is refused before any write.
type failNthPersistDLQStore struct {
	invocationStateStore
	mu     sync.Mutex
	calls  int
	failOn int // 1-based call index to fail; <=0 disables the injection
}

func (s *failNthPersistDLQStore) persistDLQ(
	ctx context.Context,
	dlqStream, stream, group, msgID, invocation string,
	claim InvocationClaim,
	entry map[string]any,
) (bool, error) {
	s.mu.Lock()
	s.calls++
	n := s.calls
	failOn := s.failOn
	s.mu.Unlock()
	if n == failOn {
		return false, errors.New("injected persistDLQ failure")
	}
	return s.invocationStateStore.persistDLQ(ctx, dlqStream, stream, group, msgID, invocation, claim, entry)
}

// disableInjection stops the nth-call failure so a redelivery through the same
// consumer/store behaves normally.
func (s *failNthPersistDLQStore) disableInjection() {
	s.mu.Lock()
	s.failOn = 0
	s.mu.Unlock()
}

// auxRedisClient opens an independent Redis client for a flow test that closes
// the consumer's own client (so it can still inspect state and redeliver). It
// copies the reference client's options so credentials/DB survive, and is
// closed via t.Cleanup.
func auxRedisClient(t *testing.T, ref *redis.Client) *redis.Client {
	t.Helper()
	opts := *ref.Options()
	cli := redis.NewClient(&opts)
	t.Cleanup(func() { _ = cli.Close() })
	return cli
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

// dlqForClient returns every DLQ entry for original_id, using cli.
func dlqForClient(t *testing.T, cli *redis.Client, dlqStream, id string) []redis.XMessage {
	t.Helper()
	msgs, err := cli.XRange(context.Background(), dlqStream, "-", "+").Result()
	if err != nil {
		t.Fatalf("xrange dlq: %v", err)
	}
	var out []redis.XMessage
	for _, m := range msgs {
		if oid, ok := m.Values["original_id"].(string); ok && oid == id {
			out = append(out, m)
		}
	}
	return out
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

// TestIntegrationRouteToDLQXACKFailureRedeliveryIsIdempotent drives the real
// routeToDLQ flow for a genuinely-exhausted invocation: the DLQ append and its
// per-invocation marker are persisted atomically, then the source XACK is
// deterministically forced to fail (the consumer's client is closed from inside
// persistDLQ). The message must stay pending with recoverable state and exactly
// one entry; a redelivery on a fresh consumer must write NO second entry and
// ACK the message.
func TestIntegrationRouteToDLQXACKFailureRedeliveryIsIdempotent(t *testing.T) {
	inspect := testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	key := invocationStateKey(e.stream, e.group, id)
	// The test closes e.client to force the XACK failure, so newEnv's cleanup
	// (which uses e.client) can no longer delete state; clean up through the
	// live inspection client instead. Registered after newEnv, so it runs first
	// (LIFO) while inspect is still open.
	t.Cleanup(func() {
		ctx := context.Background()
		_ = inspect.Del(ctx, key, e.stream, e.consumer.dlqStream).Err()
	})

	msg := e.readOneIntoPEL(t)

	// Force the XACK after the DLQ write to fail: close the consumer's client
	// from inside persistDLQ, once the entry + marker are durably written.
	e.consumer.invStateStore = &failAckAfterPersistStore{
		invocationStateStore: e.consumer.invStateStore,
		afterPersist:         func() { _ = e.client.Close() },
	}

	exhausted := func(ctx context.Context, msgID string, ev map[string]any) error {
		p, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return fmt.Errorf("no invocation state in ctx")
		}
		started, claim, _, err := p.TryStart("fn/h", time.Hour)
		if err != nil || !started {
			return fmt.Errorf("TryStart = (%v,%v), want a started claim", started, err)
		}
		if !p.MarkExhausted("fn/h", claim) {
			return fmt.Errorf("MarkExhausted refused")
		}
		return &HandlerExhaustedError{Invocations: []ExhaustedInvocation{
			{App: "fn", Handler: "h", Attempts: claim.Attempt, Err: ErrInvocationExhausted},
		}}
	}
	e.consumer.processMessage(context.Background(), msg, 1, exhausted)

	// The atomic write succeeded before the XACK failed: exactly one entry, the
	// marker upgraded to ":dlq", and the message still pending with recoverable
	// (persistent, non-terminal) state.
	if got := len(dlqForClient(t, inspect, e.consumer.dlqStream, id)); got != 1 {
		t.Fatalf("DLQ entries after failed XACK = %d, want 1", got)
	}
	if v := marker(t, inspect, e.stream, e.group, id, "fn/h"); !isExhaustedDLQValue(v) {
		t.Fatalf("marker after failed XACK = %q, want exhausted...:dlq", v)
	}
	if !pendingForClient(t, inspect, e.stream, e.group, id) {
		t.Fatalf("message %s must stay pending after the failed XACK", id)
	}
	if ok, err := inspect.HExists(context.Background(), key, terminalField).Result(); err != nil || ok {
		t.Fatalf("state must not be terminal-retained after a failed XACK (hexists=%v err=%v)", ok, err)
	}

	// Redelivery: a fresh consumer/client (the original client is closed) reads
	// the still-pending PEL entry and reprocesses. The runner would find the
	// invocation already terminal and report it without a new execution, which
	// routeToDLQ turns into "skip the persisted entry, ACK".
	redeliverCli := auxRedisClient(t, inspect)
	rc := NewConsumer(ConsumerConfig{
		Client:   redeliverCli,
		Stream:   e.stream,
		Group:    e.group,
		Consumer: e.consumer.consumer,
		Log:      testutil.DiscardLogger(),
	})
	pendingMsg := readPendingForConsumer(t, redeliverCli, e.stream, e.group, e.consumer.consumer, id)
	rc.processMessage(context.Background(), pendingMsg, 2, func(context.Context, string, map[string]any) error {
		return &HandlerExhaustedError{Invocations: []ExhaustedInvocation{
			{App: "fn", Handler: "h", Attempts: 1},
		}}
	})

	// No duplicate entry; the message is now acked and its state retained.
	if got := len(dlqForClient(t, inspect, e.consumer.dlqStream, id)); got != 1 {
		t.Fatalf("DLQ entries after redelivery = %d, want 1 (no duplicate)", got)
	}
	if pendingForClient(t, inspect, e.stream, e.group, id) {
		t.Fatalf("message %s should be acked on the idempotent redelivery", id)
	}
	waitRetainedForClient(t, inspect, key)
}

// TestIntegrationRouteToDLQPlaceholderXACKFailureRedeliveryIsIdempotent is the
// malformed-message counterpart of the test above: a message that never reaches
// a handler is dead-lettered through the real flow (its persistence recorded by
// the message-scoped reserved field), the XACK is forced to fail, and a
// redelivery must NOT append a second placeholder entry. It strengthens the
// seeded TestIntegrationPlaceholderDLQXACKFailureRetryIsIdempotent by actually
// performing the first write and the failed ACK.
func TestIntegrationRouteToDLQPlaceholderXACKFailureRedeliveryIsIdempotent(t *testing.T) {
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
		_ = inspect.Del(ctx, key, e.stream, e.consumer.dlqStream).Err()
	})
	msg := e.readOneIntoPEL(t)
	if msg.ID != id {
		t.Fatalf("read message %s, want %s", msg.ID, id)
	}

	e.consumer.invStateStore = &failAckAfterPersistStore{
		invocationStateStore: e.consumer.invStateStore,
		afterPersist:         func() { _ = e.client.Close() },
	}

	handlerRan := false
	e.consumer.processMessage(context.Background(), msg, 1, func(context.Context, string, map[string]any) error {
		handlerRan = true
		return nil
	})
	if handlerRan {
		t.Fatal("handler must not run for a malformed message")
	}

	// The placeholder entry and its message-scoped marker were written before the
	// failed XACK; the message stays pending.
	if got := len(dlqForClient(t, inspect, e.consumer.dlqStream, id)); got != 1 {
		t.Fatalf("placeholder entries after failed XACK = %d, want 1", got)
	}
	if v, err := inspect.HGet(context.Background(), key, dlqPlaceholderField).Result(); err != nil || v != "1" {
		t.Fatalf("placeholder marker = %q (err %v), want 1", v, err)
	}
	if !pendingForClient(t, inspect, e.stream, e.group, id) {
		t.Fatalf("malformed message %s must stay pending after the failed XACK", id)
	}

	// Redelivery on a fresh consumer: the placeholder is already persisted, so no
	// second entry is written and the message is ACKed.
	redeliverCli := auxRedisClient(t, inspect)
	rc := NewConsumer(ConsumerConfig{
		Client:   redeliverCli,
		Stream:   e.stream,
		Group:    e.group,
		Consumer: e.consumer.consumer,
		Log:      testutil.DiscardLogger(),
	})
	pendingMsg := readPendingForConsumer(t, redeliverCli, e.stream, e.group, e.consumer.consumer, id)
	rc.processMessage(context.Background(), pendingMsg, 2, func(context.Context, string, map[string]any) error {
		return nil
	})

	if got := len(dlqForClient(t, inspect, e.consumer.dlqStream, id)); got != 1 {
		t.Fatalf("placeholder entries after redelivery = %d, want 1 (no duplicate)", got)
	}
	if pendingForClient(t, inspect, e.stream, e.group, id) {
		t.Fatalf("message %s should be acked on the idempotent redelivery", id)
	}
	waitRetainedForClient(t, inspect, key)
}

// TestIntegrationMultiInvocationDLQPartialWriteResumesActualFlow drives the real
// multi-invocation routeToDLQ flow: a handler exhausts two invocations, the
// first DLQ persist really succeeds, and the second is deterministically refused
// by a narrow store wrapper. The message must stay pending with exactly the
// first entry and marker persisted (the second still unmarked); a redelivery
// must write ONLY the missing second entry and ACK. It strengthens the seeded
// TestIntegrationPerInvocationDLQPartialWriteResumes by producing the partial
// state through the real writes instead of seeding it.
func TestIntegrationMultiInvocationDLQPartialWriteResumesActualFlow(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	msg := e.readOneIntoPEL(t)
	key := invocationStateKey(e.stream, e.group, id)

	wrapper := &failNthPersistDLQStore{invocationStateStore: e.consumer.invStateStore, failOn: 2}
	e.consumer.invStateStore = wrapper

	e.consumer.processMessage(context.Background(), msg, 1, func(ctx context.Context, msgID string, ev map[string]any) error {
		p, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return fmt.Errorf("no invocation state in ctx")
		}
		// Two independent invocations: each claims attempt 1 and exhausts.
		for _, inv := range []string{"fnA/h", "fnB/h"} {
			started, claim, _, err := p.TryStart(inv, time.Hour)
			if err != nil || !started {
				return fmt.Errorf("TryStart %s = (%v,%v)", inv, started, err)
			}
			if !p.MarkExhausted(inv, claim) {
				return fmt.Errorf("MarkExhausted %s refused", inv)
			}
		}
		return exhaustedTwo("fnA", "h", 1, "fnB", "h", 1)
	})

	// The first entry (fnA) was really persisted and its marker upgraded; the
	// second (fnB) was refused before any write, so the message stays pending
	// with only fnA done and fnB still unmarked.
	if _, ok := e.pending()[id]; !ok {
		t.Fatalf("message %s must stay pending after the partial DLQ write", id)
	}
	entries := e.dlqFor(id)
	if len(entries) != 1 {
		t.Fatalf("DLQ entries after partial write = %d, want 1 (only fnA)", len(entries))
	}
	if entries[0].Values["app"] != "fnA" {
		t.Fatalf("persisted entry app = %v, want fnA", entries[0].Values["app"])
	}
	fnA := marker(t, e.client, e.stream, e.group, id, "fnA/h")
	if _, dlq, ok := parseExhaustedValue(fnA); !ok || !dlq {
		t.Fatalf("fnA marker = %q, want exhausted...:dlq", fnA)
	}
	fnB := marker(t, e.client, e.stream, e.group, id, "fnB/h")
	if _, dlq, ok := parseExhaustedValue(fnB); !ok || dlq {
		t.Fatalf("fnB marker = %q, want the unpersisted exhausted marker", fnB)
	}

	// Redelivery: the injection is disabled (the partial state is real now) and
	// the runner reports the already-terminal invocations without a new
	// execution, so routeToDLQ writes only fnB's missing entry and ACKs.
	wrapper.disableInjection()
	e.consumer.processMessage(context.Background(), msg, 2, func(context.Context, string, map[string]any) error {
		return exhaustedTwo("fnA", "h", 1, "fnB", "h", 1)
	})

	if got := len(e.dlqFor(id)); got != 2 {
		t.Fatalf("DLQ entries after resume = %d, want 2 (no duplicate of fnA)", got)
	}
	if _, ok := e.pending()[id]; ok {
		t.Fatalf("message %s should be acked after the resumed write", id)
	}
	e.waitRetained(t, key)
}

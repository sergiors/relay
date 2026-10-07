//go:build integration

// This file exercises the ATOMIC DLQ persistence (F-008) against a real Redis
// server: one Lua script appends the DLQ entry AND records its persistence
// marker, so a crash or error between the two can never leave a duplicate or an
// unmarked entry. It covers the append+marker pair, idempotent retries, the
// placeholder's message-scoped marker, validation-before-write (wrong key types
// and terminal-retained hashes must leave no partial effect), and a concurrent
// race that must produce exactly one entry. Excluded from the default suite by
// the integration build tag; REQUIRES Redis at REDIS_TEST_ADDR (default
// localhost:6379), failing rather than skipping when it is absent.
package stream

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/testutil"
)

// TestIntegrationPersistDLQAtomicAppendAndMarker pins the core F-008 contract at
// the store seam: persistDLQ appends exactly one entry and upgrades the
// exhausted marker in the same atomic step; a redelivery is a no-op (no
// duplicate); and a foreign claim is refused before any write, leaving both the
// DLQ stream and the marker untouched.
func TestIntegrationPersistDLQAtomicAppendAndMarker(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := atomicStateStore(t, cli)
	ctx := context.Background()
	dlq := DLQStreamFor(stream)
	t.Cleanup(func() { _ = cli.Del(context.Background(), dlq).Err() })

	now := time.Now()
	started, claim, _, err := store.tryStart(ctx, stream, group, msgID, "fn/h", now, now.Add(time.Hour))
	if err != nil || !started {
		t.Fatalf("tryStart = (%v,%+v,%v)", started, claim, err)
	}
	if ok, err := store.markExhausted(ctx, stream, group, msgID, "fn/h", claim); err != nil || !ok {
		t.Fatalf("markExhausted = (%v,%v)", ok, err)
	}

	entry := dlqPayload(stream, msgID, group, "consumer-1", `{"a":1}`, "boom", "fn", "h", 3, claim.Attempt, "")
	wrote, err := store.persistDLQ(ctx, dlq, stream, group, msgID, "fn/h", claim, entry)
	if err != nil || !wrote {
		t.Fatalf("persistDLQ = (%v,%v), want (true,nil)", wrote, err)
	}
	if v := marker(t, cli, stream, group, msgID, "fn/h"); v != exhaustedValue(claim, true) {
		t.Fatalf("marker = %q, want exhausted...:dlq", v)
	}
	msgs, err := cli.XRange(ctx, dlq, "-", "+").Result()
	if err != nil || len(msgs) != 1 {
		t.Fatalf("XRange = %d entries (err %v), want 1", len(msgs), err)
	}
	parsed, err := ParseDLQEntry(msgs[0].ID, msgs[0].Values)
	if err != nil {
		t.Fatalf("ParseDLQEntry: %v", err)
	}
	if parsed.OriginalID != msgID || parsed.App != "fn" || parsed.Handler != "h" || parsed.HandlerAttempts != claim.Attempt {
		t.Fatalf("parsed entry = %+v", parsed)
	}

	// A redelivery that re-persists the same invocation is a no-op.
	if wrote, err := store.persistDLQ(ctx, dlq, stream, group, msgID, "fn/h", claim, entry); err != nil || wrote {
		t.Fatalf("re persistDLQ = (%v,%v), want (false,nil)", wrote, err)
	}
	if n, err := cli.XLen(ctx, dlq).Result(); err != nil || n != 1 {
		t.Fatalf("XLen = %d (err %v), want 1 (no duplicate)", n, err)
	}

	// A foreign claim is refused before any write.
	if wrote, err := store.persistDLQ(ctx, dlq, stream, group, msgID, "fn/h", InvocationClaim{Attempt: claim.Attempt, Token: "deadbeef"}, entry); err == nil || wrote {
		t.Fatalf("foreign persistDLQ = (%v,%v), want (false,error)", wrote, err)
	}
	if n, err := cli.XLen(ctx, dlq).Result(); err != nil || n != 1 {
		t.Fatalf("XLen after foreign = %d (err %v), want 1", n, err)
	}
	if v := marker(t, cli, stream, group, msgID, "fn/h"); v != exhaustedValue(claim, true) {
		t.Fatalf("marker after foreign = %q, want unchanged :dlq", v)
	}
}

// TestIntegrationPersistDLQValidatesBeforeWrite pins the no-partial-effect
// contract: because a Lua error cannot roll back a prior write, every rejection
// (wrong-type state hash, wrong-type DLQ stream, terminal-retained hash, absent
// marker) must be detected without leaving a marker or an entry behind.
func TestIntegrationPersistDLQValidatesBeforeWrite(t *testing.T) {
	cli := testutil.RequireRedis(t)
	ctx := context.Background()

	t.Run("wrong-type state key", func(t *testing.T) {
		store, stream, group, msgID := atomicStateStore(t, cli)
		dlq := DLQStreamFor(stream)
		t.Cleanup(func() { _ = cli.Del(context.Background(), dlq).Err() })
		// Corrupt the state key into a string: the script must refuse BEFORE XADD.
		if err := cli.Set(ctx, invocationStateKey(stream, group, msgID), "not-a-hash", 0).Err(); err != nil {
			t.Fatalf("set state key: %v", err)
		}
		if wrote, err := store.persistDLQ(ctx, dlq, stream, group, msgID, "", InvocationClaim{}, map[string]any{"original_id": msgID}); err == nil || wrote {
			t.Fatalf("persistDLQ on wrong-type state = (%v,%v), want (false,error)", wrote, err)
		}
		if n, err := cli.XLen(ctx, dlq).Result(); err != nil || n != 0 {
			t.Fatalf("XLen = %d (err %v), want 0 (nothing written)", n, err)
		}
	})

	t.Run("wrong-type DLQ stream", func(t *testing.T) {
		store, stream, group, msgID := atomicStateStore(t, cli)
		dlq := DLQStreamFor(stream)
		t.Cleanup(func() { _ = cli.Del(context.Background(), dlq).Err() })
		now := time.Now()
		started, claim, _, err := store.tryStart(ctx, stream, group, msgID, "fn/h", now, now.Add(time.Hour))
		if err != nil || !started {
			t.Fatalf("tryStart = (%v,%+v,%v)", started, claim, err)
		}
		if ok, err := store.markExhausted(ctx, stream, group, msgID, "fn/h", claim); err != nil || !ok {
			t.Fatalf("markExhausted = (%v,%v)", ok, err)
		}
		// Corrupt the DLQ stream into a string: XADD fails inside the script
		// AFTER validation but BEFORE the marker HSET, so the marker must remain
		// un-upgraded (no partial effect).
		if err := cli.Set(ctx, dlq, "not-a-stream", 0).Err(); err != nil {
			t.Fatalf("set dlq key: %v", err)
		}
		if wrote, err := store.persistDLQ(ctx, dlq, stream, group, msgID, "fn/h", claim, map[string]any{"original_id": msgID}); err == nil || wrote {
			t.Fatalf("persistDLQ on wrong-type DLQ = (%v,%v), want (false,error)", wrote, err)
		}
		if v := marker(t, cli, stream, group, msgID, "fn/h"); v != exhaustedValue(claim, false) {
			t.Fatalf("marker = %q, want the un-upgraded exhausted marker", v)
		}
	})

	t.Run("terminal-retained state", func(t *testing.T) {
		store, stream, group, msgID := atomicStateStore(t, cli)
		dlq := DLQStreamFor(stream)
		t.Cleanup(func() { _ = cli.Del(context.Background(), dlq).Err() })
		seedMarker(t, cli, stream, group, msgID, terminalField, "1")
		if wrote, err := store.persistDLQ(ctx, dlq, stream, group, msgID, "", InvocationClaim{}, map[string]any{"original_id": msgID}); err == nil || wrote {
			t.Fatalf("persistDLQ on retained = (%v,%v), want (false,error)", wrote, err)
		}
		if n, err := cli.XLen(ctx, dlq).Result(); err != nil || n != 0 {
			t.Fatalf("XLen = %d (err %v), want 0", n, err)
		}
	})

	t.Run("absent marker", func(t *testing.T) {
		store, stream, group, msgID := atomicStateStore(t, cli)
		dlq := DLQStreamFor(stream)
		t.Cleanup(func() { _ = cli.Del(context.Background(), dlq).Err() })
		if wrote, err := store.persistDLQ(ctx, dlq, stream, group, msgID, "fn/h", InvocationClaim{Attempt: 1, Token: "aa"}, map[string]any{"original_id": msgID}); err == nil || wrote {
			t.Fatalf("persistDLQ with absent marker = (%v,%v), want (false,error)", wrote, err)
		}
		if n, err := cli.XLen(ctx, dlq).Result(); err != nil || n != 0 {
			t.Fatalf("XLen = %d (err %v), want 0", n, err)
		}
	})
}

// TestIntegrationPersistDLQPlaceholderIdempotent pins the malformed-message
// placeholder contract: persistence is recorded by a message-scoped reserved
// field (never a fabricated app/handler), a retry appends nothing, and the
// marker is independent per message.
func TestIntegrationPersistDLQPlaceholderIdempotent(t *testing.T) {
	cli := testutil.RequireRedis(t)
	ctx := context.Background()
	store, stream, group, msgID := atomicStateStore(t, cli)
	dlq := DLQStreamFor(stream)
	t.Cleanup(func() { _ = cli.Del(context.Background(), dlq).Err() })

	entry := dlqPayload(stream, msgID, group, "c", "-", "malformed", dlqNoHandler, dlqNoHandler, 1, 0, "")
	wrote, err := store.persistDLQ(ctx, dlq, stream, group, msgID, "", InvocationClaim{}, entry)
	if err != nil || !wrote {
		t.Fatalf("placeholder persistDLQ = (%v,%v), want (true,nil)", wrote, err)
	}
	if v, err := cli.HGet(ctx, invocationStateKey(stream, group, msgID), dlqPlaceholderField).Result(); err != nil || v != "1" {
		t.Fatalf("placeholder field = %q (err %v), want 1", v, err)
	}
	if n, err := cli.XLen(ctx, dlq).Result(); err != nil || n != 1 {
		t.Fatalf("XLen = %d (err %v), want 1", n, err)
	}

	// A retry (XACK failure) is a no-op: no duplicate placeholder entry.
	if wrote, err := store.persistDLQ(ctx, dlq, stream, group, msgID, "", InvocationClaim{}, entry); err != nil || wrote {
		t.Fatalf("re placeholder persistDLQ = (%v,%v), want (false,nil)", wrote, err)
	}
	if n, err := cli.XLen(ctx, dlq).Result(); err != nil || n != 1 {
		t.Fatalf("XLen after retry = %d (err %v), want 1", n, err)
	}

	// The marker is message-scoped: another message's placeholder is independent.
	store2, stream2, group2, msgID2 := atomicStateStore(t, cli)
	dlq2 := DLQStreamFor(stream2)
	t.Cleanup(func() { _ = cli.Del(context.Background(), dlq2).Err() })
	entry2 := dlqPayload(stream2, msgID2, group2, "c", "-", "malformed", dlqNoHandler, dlqNoHandler, 1, 0, "")
	if wrote, err := store2.persistDLQ(ctx, dlq2, stream2, group2, msgID2, "", InvocationClaim{}, entry2); err != nil || !wrote {
		t.Fatalf("second message placeholder persistDLQ = (%v,%v), want (true,nil)", wrote, err)
	}
}

// TestIntegrationPersistDLQConcurrentExactlyOneWinner races many callers (each
// with its own client) persisting the SAME exhausted invocation. The script is
// atomic, so exactly one appends the entry and every other observes the marker
// as already persisted — never a duplicate.
func TestIntegrationPersistDLQConcurrentExactlyOneWinner(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := atomicStateStore(t, cli)
	ctx := context.Background()
	dlq := DLQStreamFor(stream)
	t.Cleanup(func() { _ = cli.Del(context.Background(), dlq).Err() })

	now := time.Now()
	started, claim, _, err := store.tryStart(ctx, stream, group, msgID, "fn/h", now, now.Add(time.Hour))
	if err != nil || !started {
		t.Fatalf("tryStart = (%v,%+v,%v)", started, claim, err)
	}
	if ok, err := store.markExhausted(ctx, stream, group, msgID, "fn/h", claim); err != nil || !ok {
		t.Fatalf("markExhausted = (%v,%v)", ok, err)
	}

	const n = 16
	clients := make([]*redis.Client, n)
	for i := range clients {
		c := redis.NewClient(&redis.Options{Addr: cli.Options().Addr})
		clients[i] = c
		t.Cleanup(func() { _ = c.Close() })
	}
	stores := make([]invocationStateStore, n)
	for i := range stores {
		stores[i] = &invocationStore{client: clients[i]}
	}
	entry := dlqPayload(stream, msgID, group, "c", `{"a":1}`, "boom", "fn", "h", 1, claim.Attempt, "")

	start := make(chan struct{})
	wrote := make([]bool, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			wrote[i], errs[i] = stores[i].persistDLQ(ctx, dlq, stream, group, msgID, "fn/h", claim, entry)
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
	if n, err := cli.XLen(ctx, dlq).Result(); err != nil || n != 1 {
		t.Fatalf("XLen = %d (err %v), want 1", n, err)
	}
	if v := marker(t, cli, stream, group, msgID, "fn/h"); v != exhaustedValue(claim, true) {
		t.Fatalf("marker = %q, want exhausted...:dlq", v)
	}
}

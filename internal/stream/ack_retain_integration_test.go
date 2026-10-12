//go:build integration

// This file exercises the ATOMIC terminal ACK boundary against a real
// Redis server: a single Lua script both XACKs the message and switches its
// invocation-state hash to terminal retention, so a crash or error can no longer
// leave a hash whose message already left the PEL without a terminal marker/TTL
// (the post-ACK orphan window). It covers the success+TTL path, the disabled
// retention path, the absent-hash (stateless) ACK, idempotent/monotonic repeats,
// and a deterministically failed XACK — proving a failed ACK never leaves an
// expiring TTL on still-pending state. Excluded from the default suite by the
// integration build tag; REQUIRES Redis at REDIS_TEST_ADDR (default
// localhost:6379), failing rather than skipping when it is absent.
package stream

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/testutil"
)

// ackRetainEnv creates a unique stream and consumer group with one message read
// into the group PEL (so XACK has a real entry to remove) and returns a real
// Redis-backed invocation store plus the stream/group/message identity. Every
// created key is deleted on cleanup.
func ackRetainEnv(t *testing.T, cli *redis.Client) (invocationStateStore, string, string, string) {
	t.Helper()
	ctx := context.Background()
	prefix := fmt.Sprintf("ackretain-%d", time.Now().UnixNano())
	stream, group := prefix+"-stream", prefix+"-group"
	if err := cli.XGroupCreateMkStream(ctx, stream, group, "0").Err(); err != nil {
		t.Fatalf("xgroup create: %v", err)
	}
	id, err := cli.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{"event": `{"a":1}`},
	}).Result()
	if err != nil {
		t.Fatalf("xadd: %v", err)
	}
	if _, err := cli.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: "c",
		Streams:  []string{stream, ">"},
		Count:    1,
	}).Result(); err != nil {
		t.Fatalf("xreadgroup: %v", err)
	}
	t.Cleanup(func() {
		cctx := context.Background()
		_ = cli.Del(cctx, stream, DLQStreamFor(stream), invocationStateKey(stream, group, id)).Err()
	})
	return &invocationStore{client: cli}, stream, group, id
}

// streamPending reports whether id is still in the group's PEL.
func streamPending(t *testing.T, cli *redis.Client, stream, group, id string) bool {
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

// seedCompletedHash seeds a recoverable invocation-state hash (a completed
// invocation, persistent/no TTL) as a real delivery would leave it.
func seedCompletedHash(t *testing.T, cli *redis.Client, key string) {
	t.Helper()
	ctx := context.Background()
	if err := cli.HSet(ctx, key, "fn/h", "ok").Err(); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	if d, err := cli.PTTL(ctx, key).Result(); err != nil || d != -1 {
		t.Fatalf("seeded PTTL = %s (err %v), want -1 (recoverable state is persistent)", d, err)
	}
}

// TestAtomicTerminalAckPreventsOrphanedInvocation verifies that a
// single ackAndRetainTerminal call must BOTH remove the message from the PEL AND
// switch its invocation-state hash to terminal retention (marker + TTL). Because
// the ACK and the retention happen in one atomic script, there is no observable
// state in which the message has left the PEL while its hash is still an
// unretained, persistent orphan.
func TestAtomicTerminalAckPreventsOrphanedInvocation(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := ackRetainEnv(t, cli)
	ctx := context.Background()
	key := invocationStateKey(stream, group, msgID)
	seedCompletedHash(t, cli, key)

	res, err := store.ackAndRetainTerminal(ctx, stream, group, msgID, DefaultInvocationRetention)
	if err != nil {
		t.Fatalf("ackAndRetainTerminal: %v", err)
	}
	if !res.Removed || !res.Retained {
		t.Fatalf("result = %+v, want removed+retained (one atomic operation)", res)
	}

	// The message left the PEL...
	if streamPending(t, cli, stream, group, msgID) {
		t.Fatalf("message %s still pending after ackAndRetainTerminal", msgID)
	}
	// ...and its hash is terminal-retained in the very same step: the terminal
	// marker is present and a positive retention TTL is applied.
	if v, err := cli.HGet(ctx, key, terminalField).Result(); err != nil || v != "1" {
		t.Fatalf("terminal field = %q (err %v), want \"1\"", v, err)
	}
	ttl, err := cli.PTTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("pttl: %v", err)
	}
	if ttl <= 0 || ttl > DefaultInvocationRetention {
		t.Fatalf("retention PTTL = %s, want (0, %s] (the terminal window)", ttl, DefaultInvocationRetention)
	}

	// The retained hash still guards a stale in-memory delivery: no re-open.
	now := time.Now()
	if s, _, _, err := store.tryStart(ctx, stream, group, msgID, "fn/h", now, now.Add(time.Hour)); err != nil || s {
		t.Fatalf("tryStart on a terminal-retained hash = (%v,%v), want (false,nil)", s, err)
	}
}

// TestAtomicTerminalAckIsMonotonic pins that a repeated ackAndRetainTerminal is
// idempotent and NEVER re-extends an existing retention TTL: the first call
// applies the given window, and a second call with a much longer window must
// leave the original (shorter) deadline in place. The short first window makes
// the check deterministic without any sleep (a re-extension to the default 48h
// would be immediately visible).
func TestAtomicTerminalAckIsMonotonic(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := ackRetainEnv(t, cli)
	ctx := context.Background()
	key := invocationStateKey(stream, group, msgID)
	seedCompletedHash(t, cli, key)

	const firstWindow = 5 * time.Second
	if _, err := store.ackAndRetainTerminal(ctx, stream, group, msgID, firstWindow); err != nil {
		t.Fatalf("first ackAndRetainTerminal: %v", err)
	}
	ttl1, err := cli.PTTL(ctx, key).Result()
	if err != nil || ttl1 <= 0 || ttl1 > firstWindow {
		t.Fatalf("first PTTL = %s (err %v), want (0, %s]", ttl1, err, firstWindow)
	}

	// A second call with the much longer default window must be a no-op on the
	// TTL (idempotent/monotonic), and the message is already acked (removed=false).
	res, err := store.ackAndRetainTerminal(ctx, stream, group, msgID, DefaultInvocationRetention)
	if err != nil {
		t.Fatalf("second ackAndRetainTerminal: %v", err)
	}
	if res.Removed {
		t.Fatalf("second result.Removed = true, want false (already acked)")
	}
	if !res.Retained {
		t.Fatalf("second result.Retained = false, want true (hash is retained)")
	}
	ttl2, err := cli.PTTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("second pttl: %v", err)
	}
	if ttl2 <= 0 || ttl2 > firstWindow+time.Second {
		t.Fatalf("second PTTL = %s, want <= the original %s (no re-extension), far below the default %s",
			ttl2, firstWindow, DefaultInvocationRetention)
	}
}

// TestAtomicTerminalAckRetentionDisabled pins the explicit-disable contract
// (REDIS_INVOCATION_RETENTION empty/0/negative): the terminal marker is still
// written atomically (so stale-transition guards are unchanged) but NO TTL is
// applied — the hash is PERSISTed and stays terminal. It coexists with the ACK
// in the same atomic script.
func TestAtomicTerminalAckRetentionDisabled(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := ackRetainEnv(t, cli)
	ctx := context.Background()
	key := invocationStateKey(stream, group, msgID)
	seedCompletedHash(t, cli, key)

	res, err := store.ackAndRetainTerminal(ctx, stream, group, msgID, 0)
	if err != nil {
		t.Fatalf("ackAndRetainTerminal(disabled): %v", err)
	}
	if !res.Removed || !res.Retained {
		t.Fatalf("result = %+v, want removed+retained", res)
	}
	if streamPending(t, cli, stream, group, msgID) {
		t.Fatalf("message %s still pending after the ack", msgID)
	}
	if v, err := cli.HGet(ctx, key, terminalField).Result(); err != nil || v != "1" {
		t.Fatalf("terminal field = %q (err %v), want \"1\"", v, err)
	}
	if d, err := cli.PTTL(ctx, key).Result(); err != nil || d != -1 {
		t.Fatalf("PTTL after disabled retention = %s (err %v), want -1 (persistent)", d, err)
	}
	// A repeated disabled call must not introduce a TTL either (monotonic).
	if _, err := store.ackAndRetainTerminal(ctx, stream, group, msgID, 0); err != nil {
		t.Fatalf("second ackAndRetainTerminal(disabled): %v", err)
	}
	if d, err := cli.PTTL(ctx, key).Result(); err != nil || d != -1 {
		t.Fatalf("PTTL after repeated disabled call = %s (err %v), want -1", d, err)
	}
}

// TestAtomicTerminalAckAbsentHashStillAcks pins the stateless-message case: when
// there is no invocation-state hash there is nothing to retain, but the ACK must
// still happen and NO empty key may be created (the marker must never fabricate
// state that did not exist).
func TestAtomicTerminalAckAbsentHashStillAcks(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := ackRetainEnv(t, cli)
	ctx := context.Background()
	key := invocationStateKey(stream, group, msgID)

	res, err := store.ackAndRetainTerminal(ctx, stream, group, msgID, DefaultInvocationRetention)
	if err != nil {
		t.Fatalf("ackAndRetainTerminal(absent hash): %v", err)
	}
	if !res.Removed {
		t.Fatalf("result.Removed = false, want true (the message must still be acked)")
	}
	if res.Retained {
		t.Fatalf("result.Retained = true, want false (there was no hash to retain)")
	}
	if streamPending(t, cli, stream, group, msgID) {
		t.Fatalf("message %s still pending after the ack", msgID)
	}
	if n, err := cli.Exists(ctx, key).Result(); err != nil || n != 0 {
		t.Fatalf("an absent hash must not be created by the ACK (exists=%d err=%v)", n, err)
	}
}

// TestAtomicTerminalAckXACKFailureLeavesPendingPersistentState pins the PEL
// safety of the atomic script: a failed XACK must never leave an expiring TTL on
// still-pending state, and must never recreate an unmarked orphan.
//
//   - "transport failure": the whole EVAL never runs (the client is closed), so
//     nothing is staged; the message stays in the PEL and the recoverable hash
//     stays persistent (PTTL -1, no terminal marker).
//   - "in-script failure": the state hash is seeded with a LEGACY expiring TTL
//     and the ACK is forced to fail inside the script by pointing the ACK at a
//     wrong-type key. The script stages the terminal marker and PERSISTs BEFORE
//     the ACK, so even though the ACK raises, the hash is left terminal-marked
//     and PERSISTENT (no expiring TTL) — never an orphan that leaks or expires.
func TestAtomicTerminalAckXACKFailureLeavesPendingPersistentState(t *testing.T) {
	t.Run("transport failure leaves PEL and persistent state", func(t *testing.T) {
		inspect := testutil.RequireRedis(t)
		storeCli := auxRedisClient(t, inspect)
		ctx := context.Background()

		// Build the stream/group/PEL with the inspection client, then bind the
		// store to a separate client that we close to force the transport failure.
		prefix := fmt.Sprintf("ackfail-%d", time.Now().UnixNano())
		stream, group := prefix+"-stream", prefix+"-group"
		if err := inspect.XGroupCreateMkStream(ctx, stream, group, "0").Err(); err != nil {
			t.Fatalf("xgroup create: %v", err)
		}
		id, err := inspect.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]any{"event": `{"a":1}`}}).Result()
		if err != nil {
			t.Fatalf("xadd: %v", err)
		}
		if _, err := inspect.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: group, Consumer: "c", Streams: []string{stream, ">"}, Count: 1,
		}).Result(); err != nil {
			t.Fatalf("xreadgroup: %v", err)
		}
		key := invocationStateKey(stream, group, id)
		t.Cleanup(func() {
			cctx := context.Background()
			_ = inspect.Del(cctx, stream, DLQStreamFor(stream), key).Err()
		})
		seedCompletedHash(t, inspect, key)

		store := &invocationStore{client: storeCli}
		if err := storeCli.Close(); err != nil {
			t.Fatalf("close store client: %v", err)
		}
		if _, err := store.ackAndRetainTerminal(ctx, stream, group, id, DefaultInvocationRetention); err == nil {
			t.Fatal("ackAndRetainTerminal with a closed client = nil error, want a transport error")
		}

		// The message stays pending and the state stays recoverable: persistent,
		// completed marker preserved, no terminal marker (nothing ran).
		if !streamPending(t, inspect, stream, group, id) {
			t.Fatalf("message %s must stay pending after a failed ACK", id)
		}
		if v, err := inspect.HGet(ctx, key, "fn/h").Result(); err != nil || v != "ok" {
			t.Fatalf("marker = %q (err %v), want ok preserved for the redelivery", v, err)
		}
		if ok, err := inspect.HExists(ctx, key, terminalField).Result(); err != nil || ok {
			t.Fatalf("no terminal marker may be written when the EVAL never ran (ok=%v err=%v)", ok, err)
		}
		if d, err := inspect.PTTL(ctx, key).Result(); err != nil || d != -1 {
			t.Fatalf("still-pending state must be persistent; pttl=%s (err %v), want -1", d, err)
		}
	})

	t.Run("in-script XACK failure leaves persistent non-expiring state", func(t *testing.T) {
		cli := testutil.RequireRedis(t)
		ctx := context.Background()
		prefix := fmt.Sprintf("ackwrongtype-%d", time.Now().UnixNano())
		// A stream name holding a NON-stream key: the script's XACK raises.
		stream, group, msgID := prefix+"-notastream", "g", "1-0"
		key := invocationStateKey(stream, group, msgID)
		t.Cleanup(func() {
			cctx := context.Background()
			_ = cli.Del(cctx, stream, key).Err()
		})
		if err := cli.Set(ctx, stream, "not-a-stream", 0).Err(); err != nil {
			t.Fatalf("set wrong-type stream key: %v", err)
		}
		// Seed a recoverable hash that carries a LEGACY TTL, exactly the state a
		// pre-persistence Relay could leave: the script must PERSIST it before the
		// failed ACK, so the failed ACK can never leave it expiring.
		if err := cli.HSet(ctx, key, "fn/h", "ok").Err(); err != nil {
			t.Fatalf("seed state: %v", err)
		}
		if err := cli.PExpire(ctx, key, time.Hour).Err(); err != nil {
			t.Fatalf("seed legacy ttl: %v", err)
		}
		if d, err := cli.PTTL(ctx, key).Result(); err != nil || d <= 0 {
			t.Fatalf("seeded PTTL = %s (err %v), want a positive legacy TTL", d, err)
		}

		store := &invocationStore{client: cli}
		if _, err := store.ackAndRetainTerminal(ctx, stream, group, msgID, DefaultInvocationRetention); err == nil {
			t.Fatal("ackAndRetainTerminal against a wrong-type stream = nil error, want the in-script XACK error")
		}

		// The marker was staged (persistent) BEFORE the failed ACK, so the state
		// is terminal-marked and has NO expiring TTL — the still-pending message's
		// state can never expire.
		if v, err := cli.HGet(ctx, key, terminalField).Result(); err != nil || v != "1" {
			t.Fatalf("terminal field = %q (err %v), want \"1\" (marker staged before the ACK)", v, err)
		}
		if d, err := cli.PTTL(ctx, key).Result(); err != nil || d != -1 {
			t.Fatalf("state after a failed in-script ACK must be persistent; pttl=%s (err %v), want -1 (no expiring TTL)", d, err)
		}
	})
}

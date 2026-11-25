//go:build integration

// This file exercises the ATOMIC terminal ACK boundary against a real
// Redis server: a single Lua script both XACKs the message and switches its
// invocation-state hash to terminal retention, so a crash or error can no longer
// leave a hash whose message already left the PEL without a terminal marker/TTL
// (the post-ACK orphan window). It covers the success+TTL path, the disabled
// retention path, the absent-hash (stateless) ACK, idempotent/monotonic repeats,
// and the two refusal/failure legs — a transport failure where the whole EVAL
// never ran, and an in-script disposition refusal (wrong-type stream) that
// writes NOTHING — proving a failed or refused ACK never resolves the delivery
// and never leaves an expiring TTL on still-pending state. Excluded from the
// default suite by the integration build tag; REQUIRES Redis at
// REDIS_TEST_ADDR (default localhost:6379), failing rather than skipping when
// it is absent.
package stream

import (
	"context"
	"fmt"
	"strings"
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
		Values: map[string]any{"value": `{"a":1}`},
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
		_ = cli.Del(cctx, stream, groupGenerationKey(stream, group), invocationStateKey(stream, group, id)).Err()
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

	res, err := store.ackAndRetainTerminal(ctx, stream, group, msgID, DefaultInvocationRetention, false)
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
	if _, err := store.ackAndRetainTerminal(ctx, stream, group, msgID, firstWindow, false); err != nil {
		t.Fatalf("first ackAndRetainTerminal: %v", err)
	}
	ttl1, err := cli.PTTL(ctx, key).Result()
	if err != nil || ttl1 <= 0 || ttl1 > firstWindow {
		t.Fatalf("first PTTL = %s (err %v), want (0, %s]", ttl1, err, firstWindow)
	}

	// A second call with the much longer default window must be a no-op on the
	// TTL (idempotent/monotonic), and the message is already acked (removed=false).
	res, err := store.ackAndRetainTerminal(ctx, stream, group, msgID, DefaultInvocationRetention, false)
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

func TestAtomicTerminalAckRetainsExistingPersistentTerminalHash(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := ackRetainEnv(t, cli)
	ctx := context.Background()
	key := invocationStateKey(stream, group, msgID)

	if err := cli.HSet(ctx, key, terminalField, "1", "fn/h", "ok").Err(); err != nil {
		t.Fatalf("seed terminal state: %v", err)
	}
	if err := cli.XAck(ctx, stream, group, msgID).Err(); err != nil {
		t.Fatalf("xack: %v", err)
	}
	if streamPending(t, cli, stream, group, msgID) {
		t.Fatalf("message %s is still pending after xack", msgID)
	}
	if ttl, err := cli.PTTL(ctx, key).Result(); err != nil || ttl != -1 {
		t.Fatalf("seeded PTTL = %s (err %v), want persistent", ttl, err)
	}

	res, err := store.ackAndRetainTerminal(ctx, stream, group, msgID, DefaultInvocationRetention, false)
	if err != nil {
		t.Fatalf("ackAndRetainTerminal: %v", err)
	}
	if !res.Settled || res.Removed || !res.Retained {
		t.Fatalf("result = %+v, want settled, not removed, and retained", res)
	}
	if receipt, err := cli.HGet(ctx, key, ackReceiptField).Result(); err != nil || receipt != "1" {
		t.Fatalf("ACK receipt = (%q, %v), want \"1\"", receipt, err)
	}
	if ttl, err := cli.PTTL(ctx, key).Result(); err != nil || ttl <= 0 || ttl > DefaultInvocationRetention {
		t.Fatalf("retention PTTL = %s (err %v), want (0, %s]", ttl, err, DefaultInvocationRetention)
	}
}

func TestAtomicTerminalAckDisabledRetentionPersistsExistingTTL(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := ackRetainEnv(t, cli)
	ctx := context.Background()
	key := invocationStateKey(stream, group, msgID)

	if err := cli.HSet(ctx, key, terminalField, "1", "fn/h", "ok").Err(); err != nil {
		t.Fatalf("seed terminal state: %v", err)
	}
	if err := cli.PExpire(ctx, key, time.Hour).Err(); err != nil {
		t.Fatalf("seed terminal retention: %v", err)
	}
	if err := cli.XAck(ctx, stream, group, msgID).Err(); err != nil {
		t.Fatalf("xack: %v", err)
	}

	res, err := store.ackAndRetainTerminal(ctx, stream, group, msgID, 0, false)
	if err != nil {
		t.Fatalf("ackAndRetainTerminal(disabled): %v", err)
	}
	if !res.Settled || res.Removed || !res.Retained {
		t.Fatalf("result = %+v, want settled, not removed, and retained", res)
	}
	if ttl, err := cli.PTTL(ctx, key).Result(); err != nil || ttl != -1 {
		t.Fatalf("PTTL with disabled retention = %s (err %v), want persistent", ttl, err)
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

	res, err := store.ackAndRetainTerminal(ctx, stream, group, msgID, 0, false)
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
	if _, err := store.ackAndRetainTerminal(ctx, stream, group, msgID, 0, false); err != nil {
		t.Fatalf("second ackAndRetainTerminal(disabled): %v", err)
	}
	if d, err := cli.PTTL(ctx, key).Result(); err != nil || d != -1 {
		t.Fatalf("PTTL after repeated disabled call = %s (err %v), want -1", d, err)
	}
}

// TestAtomicTerminalAckAbsentHashStillAcks pins the ordinary stateless case:
// an ACK that does not need DLQ settlement proof leaves no invocation hash.
func TestAtomicTerminalAckAbsentHashStillAcks(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := ackRetainEnv(t, cli)
	ctx := context.Background()
	key := invocationStateKey(stream, group, msgID)

	res, err := store.ackAndRetainTerminal(ctx, stream, group, msgID, DefaultInvocationRetention, false)
	if err != nil {
		t.Fatalf("ackAndRetainTerminal(absent hash): %v", err)
	}
	if !res.Removed {
		t.Fatalf("result.Removed = false, want true (the message must still be acked)")
	}
	if res.Retained {
		t.Fatalf("result.Retained = true, want false (ordinary stateless ACK creates no hash)")
	}
	if streamPending(t, cli, stream, group, msgID) {
		t.Fatalf("message %s still pending after the ack", msgID)
	}
	if exists, err := cli.Exists(ctx, key).Result(); err != nil || exists != 0 {
		t.Fatalf("stateless ACK created invocation state: exists=%d err=%v", exists, err)
	}
}

func TestAtomicTrackedStatelessAckRetainsReceipt(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := ackRetainEnv(t, cli)
	ctx := context.Background()
	key := invocationStateKey(stream, group, msgID)

	res, err := store.ackAndRetainTerminal(ctx, stream, group, msgID, DefaultInvocationRetention, true)
	if err != nil {
		t.Fatalf("tracked ackAndRetainTerminal: %v", err)
	}
	if !res.Removed || !res.Retained {
		t.Fatalf("result = %+v, want removed+retained for DLQ tracking", res)
	}
	if receipt, err := cli.HGet(ctx, key, ackReceiptField).Result(); err != nil || receipt != "1" {
		t.Fatalf("ACK receipt = (%q, %v), want \"1\"", receipt, err)
	}
	if ttl, err := cli.PTTL(ctx, key).Result(); err != nil || ttl <= 0 {
		t.Fatalf("receipt retention PTTL = %s (err %v), want positive", ttl, err)
	}
}

func TestAtomicTerminalReplayRearmsRetentionAfterConfirmedAck(t *testing.T) {
	inspect := testutil.RequireRedis(t)
	ctx := context.Background()
	storeCli := auxRedisClient(t, inspect)
	_, stream, group, id := ackRetainEnv(t, inspect)
	key := invocationStateKey(stream, group, id)
	if err := inspect.HSet(ctx, key, terminalField, "1", "fn/h", "ok").Err(); err != nil {
		t.Fatalf("seed terminal state: %v", err)
	}
	if err := inspect.PExpire(ctx, key, time.Hour).Err(); err != nil {
		t.Fatalf("seed old retention: %v", err)
	}
	before, err := inspect.PTTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("initial pttl: %v", err)
	}

	// The terminal hash is being replayed from the live PEL. Recovery must
	// remove its old expiry before any handler/ACK operation.
	realStore := &invocationStore{client: inspect, consumer: "c"}
	if err := realStore.makeRecoverable(ctx, stream, group, id); err != nil {
		t.Fatalf("makeRecoverable: %v", err)
	}
	if ttl, err := inspect.PTTL(ctx, key).Result(); err != nil || ttl != -1 {
		t.Fatalf("PTTL after replay recovery = %s (err %v), want persistent", ttl, err)
	}

	// A transport failure at the ACK boundary leaves the message pending and
	// the terminal state persistent, never expiring during a replay.
	if err := storeCli.Close(); err != nil {
		t.Fatalf("close ACK client: %v", err)
	}
	failedStore := &invocationStore{client: storeCli, consumer: "c"}
	if _, err := failedStore.ackAndRetainTerminal(ctx, stream, group, id, 2*time.Hour, false); err == nil {
		t.Fatal("ackAndRetainTerminal with a closed client = nil error, want transport failure")
	}
	if !streamPending(t, inspect, stream, group, id) {
		t.Fatal("message left the PEL after the failed ACK")
	}
	if ttl, err := inspect.PTTL(ctx, key).Result(); err != nil || ttl != -1 {
		t.Fatalf("PTTL after failed replay ACK = %s (err %v), want persistent", ttl, err)
	}

	// Retrying the pending replay's ACK starts a fresh retention window even
	// when the hash was already terminal and carried an old TTL.
	res, err := realStore.ackAndRetainTerminal(ctx, stream, group, id, 2*time.Hour, false)
	if err != nil {
		t.Fatalf("replay ACK: %v", err)
	}
	if !res.Settled || !res.Removed {
		t.Fatalf("replay ACK result = %+v, want settled+removed", res)
	}
	after, err := inspect.PTTL(ctx, key).Result()
	if err != nil || after < time.Hour {
		t.Fatalf("PTTL after replay ACK = %s (err %v), want fresh two-hour retention (old %s)", after, err, before)
	}

	// A stale in-memory delivery with no exact PEL entry must not revive the
	// retained hash by making it persistent.
	time.Sleep(40 * time.Millisecond)
	if err := realStore.makeRecoverable(ctx, stream, group, id); err != nil {
		t.Fatalf("makeRecoverable after ACK: %v", err)
	}
	staleTTL, err := inspect.PTTL(ctx, key).Result()
	if err != nil || staleTTL <= 0 || staleTTL >= after {
		t.Fatalf("PTTL after stale makeRecoverable = %s (err %v), want retained and decayed from %s", staleTTL, err, after)
	}

	// A non-pending idempotent ACK must not extend that fresh deadline either.
	res, err = realStore.ackAndRetainTerminal(ctx, stream, group, id, 3*time.Hour, false)
	if err != nil {
		t.Fatalf("repeated no-op ACK: %v", err)
	}
	if res.Removed {
		t.Fatalf("repeated no-op ACK removed a PEL entry: %+v", res)
	}
	repeated, err := inspect.PTTL(ctx, key).Result()
	if err != nil || repeated >= after {
		t.Fatalf("PTTL after no-op ACK = %s (err %v), want less than prior %s", repeated, err, after)
	}
}

// TestAtomicTerminalAckXACKFailureLeavesPendingPersistentState pins the PEL
// safety of the atomic script: a refused or failed ACK must never leave an
// expiring TTL on still-pending state, and must never recreate an unmarked
// orphan.
//
//   - "transport failure": the whole EVAL never runs (the client is closed), so
//     nothing is staged; the message stays in the PEL and the recoverable hash
//     stays persistent (PTTL -1, no terminal marker).
//   - "in-script refusal": the stream key holds a WRONGTYPE value, so the
//     script's disposition validation refuses BEFORE any write. The refusal is
//     a complete no-op — no terminal marker, the recoverable value and its
//     seeded legacy TTL are untouched — and it is a generic error, never
//     NOGROUP (WRONGTYPE must not send the Go recovery path recreating a group
//     that was not lost).
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
		id, err := inspect.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]any{"value": `{"a":1}`}}).Result()
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
			_ = inspect.Del(cctx, stream, key).Err()
		})
		seedCompletedHash(t, inspect, key)

		store := &invocationStore{client: storeCli}
		if err := storeCli.Close(); err != nil {
			t.Fatalf("close store client: %v", err)
		}
		if _, err := store.ackAndRetainTerminal(ctx, stream, group, id, DefaultInvocationRetention, false); err == nil {
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

	// A stream name holding a NON-stream key: the script's disposition
	// validation refuses the ACK before any write.
	t.Run("in-script stream-type refusal writes nothing", func(t *testing.T) {
		cli := testutil.RequireRedis(t)
		ctx := context.Background()
		prefix := fmt.Sprintf("ackwrongtype-%d", time.Now().UnixNano())
		stream, group, msgID := prefix+"-notastream", "g", "1-0"
		key := invocationStateKey(stream, group, msgID)
		t.Cleanup(func() {
			cctx := context.Background()
			_ = cli.Del(cctx, stream, key).Err()
		})
		if err := cli.Set(ctx, stream, "not-a-stream", 0).Err(); err != nil {
			t.Fatalf("set wrong-type stream key: %v", err)
		}
		// Seed a recoverable hash that carries a LEGACY TTL, exactly the
		// state a pre-persistence Relay could leave: a refusal must not
		// touch it at all — no marker staged, no TTL migration, no value
		// change — so the state is byte-identical after the failed ACK.
		if err := cli.HSet(ctx, key, "fn/h", "ok").Err(); err != nil {
			t.Fatalf("seed state: %v", err)
		}
		if err := cli.PExpire(ctx, key, time.Hour).Err(); err != nil {
			t.Fatalf("seed legacy ttl: %v", err)
		}
		seededTTL, err := cli.PTTL(ctx, key).Result()
		if err != nil || seededTTL <= 0 {
			t.Fatalf("seeded PTTL = %s (err %v), want a positive legacy TTL", seededTTL, err)
		}

		store := &invocationStore{client: cli}
		res, err := store.ackAndRetainTerminal(ctx, stream, group, msgID, DefaultInvocationRetention, false)
		if err == nil {
			t.Fatal("ackAndRetainTerminal against a wrong-type stream = nil error, want the in-script refusal")
		}
		if strings.Contains(err.Error(), "NOGROUP") {
			t.Fatalf("a WRONGTYPE stream must not masquerade as a lost group (it would trigger a bogus recovery): %v", err)
		}
		if res.Settled || res.Removed || res.Retained {
			t.Fatalf("result = %+v, want the zero value (a refusal resolves nothing)", res)
		}

		// The refusal wrote NOTHING: no terminal marker, the recoverable
		// value is intact, and even the seeded legacy TTL is untouched (still
		// expiring, never persisted away and never re-armed to the retention
		// window; it may only have decayed by the sub-millisecond test runtime).
		if exists, err := cli.HExists(ctx, key, terminalField).Result(); err != nil || exists {
			t.Fatalf("terminal marker = (%v, %v), want absent (refusal must write nothing)", exists, err)
		}
		if v, err := cli.HGet(ctx, key, "fn/h").Result(); err != nil || v != "ok" {
			t.Fatalf("marker = %q (err %v), want \"ok\" preserved", v, err)
		}
		if d, err := cli.PTTL(ctx, key).Result(); err != nil || d <= 0 || d > seededTTL {
			t.Fatalf("PTTL = %s (err %v), want the seeded %s decayed but untouched", d, err, seededTTL)
		}
	})
}

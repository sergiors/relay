//go:build integration

// Focused F-03 regression coverage for the atomic ackAndRetainTerminal
// disposition against a real Redis server. The single Lua script must decide
// the disposition BEFORE it writes anything:
//
//   - NOGROUP (missing stream or missing group) is an error with ZERO writes:
//     no terminal marker, no TTL, no group creation;
//   - XACK=0 is not success: an UNREAD entry (recreated group has not
//     delivered it yet) is left entirely alone, and a PEL entry owned by
//     ANOTHER consumer is never ACKed — a newly delivered pending entry must
//     survive a stale completion;
//   - stream IDs are compared component-wise (digit length then
//     lexicographic), never as whole strings or lossy numbers;
//   - an already-acknowledged or trimmed-away body settles legitimately (it
//     can never be redelivered), and only then does terminal retention apply.
//
// Excluded from the default suite by the integration build tag; REQUIRES Redis
// at REDIS_TEST_ADDR (default localhost:6379), failing rather than skipping.
package stream

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/testutil"
)

// ackDispositionEnv is a uniquely named stream/group pair plus a real
// invocation store bound to the consumer name "me". The group is created per
// test, so a missing-group scenario needs no extra fixture. The store's
// consumer name matters: it is the PEL owner the ACK script checks.
type ackDispositionEnv struct {
	cli    *redis.Client
	stream string
	group  string
	store  *invocationStore
}

func newAckDispositionEnv(t *testing.T) *ackDispositionEnv {
	t.Helper()
	cli := testutil.RequireRedis(t)
	prefix := fmt.Sprintf("ackdisp-%d", time.Now().UnixNano())
	streamName, group := prefix+"-stream", prefix+"-group"
	t.Cleanup(func() {
		ctx := context.Background()
		cleanupEnvRedisKeys(ctx, cli, streamName, group)
	})
	return &ackDispositionEnv{
		cli:    cli,
		stream: streamName,
		group:  group,
		store:  &invocationStore{client: cli, consumer: "me"},
	}
}

// seedRecoverableState writes a realistic recoverable marker (a running
// invocation with a future deadline, persistent, no TTL) and returns the state
// key and the exact seeded value for later byte-equality assertions.
func seedRecoverableState(t *testing.T, e *ackDispositionEnv, stream, group, msgID string) (key, value string) {
	t.Helper()
	deadline := strconv.FormatInt(time.Now().Add(time.Hour).UnixMilli(), 10)
	value = "running:" + deadline + ":1:abcdef"
	key = invocationStateKey(stream, group, msgID)
	if err := e.cli.HSet(context.Background(), key, "fn/h", value).Err(); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	if d, err := e.cli.PTTL(context.Background(), key).Result(); err != nil || d != -1 {
		t.Fatalf("seeded PTTL = %s (err %v), want -1 (recoverable state is persistent)", d, err)
	}
	return key, value
}

// requireStateUntouched asserts the state hash still carries exactly the
// seeded recoverable marker: no terminal marker, no TTL, no mutation.
func requireStateUntouched(t *testing.T, e *ackDispositionEnv, key, want string) {
	t.Helper()
	ctx := context.Background()
	if exists, err := e.cli.HExists(ctx, key, terminalField).Result(); err != nil || exists {
		t.Fatalf("terminal marker = (%v, %v), want absent (no premature retention)", exists, err)
	}
	if d, err := e.cli.PTTL(ctx, key).Result(); err != nil || d != -1 {
		t.Fatalf("PTTL = %s (err %v), want -1 (no retention TTL on an unresolved delivery)", d, err)
	}
	if got, err := e.cli.HGet(ctx, key, "fn/h").Result(); err != nil || got != want {
		t.Fatalf("marker = %q (err %v), want the seeded %q unchanged", got, err, want)
	}
}

// requireTerminalRetained asserts the state hash was switched to terminal
// retention: marker present with a positive TTL.
func requireTerminalRetained(t *testing.T, e *ackDispositionEnv, key string) {
	t.Helper()
	ctx := context.Background()
	if v, err := e.cli.HGet(ctx, key, terminalField).Result(); err != nil || v != "1" {
		t.Fatalf("terminal marker = %q (err %v), want \"1\"", v, err)
	}
	if d, err := e.cli.PTTL(ctx, key).Result(); err != nil || d <= 0 {
		t.Fatalf("PTTL = %s (err %v), want a positive retention TTL", d, err)
	}
}

// TestIntegrationAckNoGroupPreservesInvocationState pins the NOGROUP leg of
// the ACK script: both a missing stream and a missing group are returned as a
// NOGROUP error with ZERO writes — the recoverable marker stays untouched, no
// retention is applied, and the ACK itself never creates the group (only the
// shared recovery path may).
func TestIntegrationAckNoGroupPreservesInvocationState(t *testing.T) {
	e := newAckDispositionEnv(t)
	ctx := context.Background()

	t.Run("missing stream", func(t *testing.T) {
		missingStream := e.stream + "-absent"
		key, value := seedRecoverableState(t, e, missingStream, e.group, "1-0")

		_, err := e.store.ackAndRetainTerminal(ctx, missingStream, e.group, "1-0", DefaultInvocationRetention, false)
		if err == nil || !strings.Contains(err.Error(), "NOGROUP") {
			t.Fatalf("ack on a missing stream = %v, want a NOGROUP error", err)
		}
		requireStateUntouched(t, e, key, value)
		if n, err := e.cli.Exists(ctx, missingStream).Result(); err != nil || n != 0 {
			t.Fatalf("stream key exists = (%d, %v), want 0 (the ACK must not create it)", n, err)
		}
	})

	t.Run("existing stream, missing group", func(t *testing.T) {
		if err := e.cli.XAdd(ctx, &redis.XAddArgs{
			Stream: e.stream, Values: map[string]any{"value": `{"a":1}`},
		}).Err(); err != nil {
			t.Fatalf("xadd: %v", err)
		}
		key, value := seedRecoverableState(t, e, e.stream, e.group, "1-0")

		res, err := e.store.ackAndRetainTerminal(ctx, e.stream, e.group, "1-0", DefaultInvocationRetention, false)
		if err == nil || !strings.Contains(err.Error(), "NOGROUP") {
			t.Fatalf("ack on a missing group = %v, want a NOGROUP error", err)
		}
		if res.Settled || res.Removed || res.Retained {
			t.Fatalf("result = %+v, want the zero value (nothing may resolve)", res)
		}
		requireStateUntouched(t, e, key, value)
		if groups, err := e.cli.XInfoGroups(ctx, e.stream).Result(); err != nil || len(groups) != 0 {
			t.Fatalf("groups = (%+v, %v), want none (the ACK must never create the group)", groups, err)
		}
	})
}

// TestIntegrationAckUnreadAndForeignLeaveSourceAndStateUntouched pins the
// XACK=0 dispositions end to end: an unread entry in a recreated group and a
// PEL entry owned by another consumer both refuse the ACK with NO error and NO
// write — the stale completion can neither retire the delivery, nor apply
// terminal retention, nor remove a newly delivered pending entry. Only the
// owning consumer's ACK resolves the delivery.
func TestIntegrationAckUnreadAndForeignLeaveSourceAndStateUntouched(t *testing.T) {
	e := newAckDispositionEnv(t)
	ctx := context.Background()
	if err := EnsureGroup(ctx, e.cli, e.stream, e.group); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	id, err := e.cli.XAdd(ctx, &redis.XAddArgs{
		Stream: e.stream, Values: map[string]any{"value": `{"a":1}`},
	}).Result()
	if err != nil {
		t.Fatalf("xadd: %v", err)
	}
	key, value := seedRecoverableState(t, e, e.stream, e.group, id)

	// Unread: the group exists but has not delivered the entry yet (the shape
	// after a recreation at ID 0). XACK would return 0; the script must refuse
	// before writing anything.
	res, err := e.store.ackAndRetainTerminal(ctx, e.stream, e.group, id, DefaultInvocationRetention, false)
	if err != nil {
		t.Fatalf("ack(unread) error = %v, want nil (unread is not a failure)", err)
	}
	if res.Settled || res.Removed || res.Retained {
		t.Fatalf("ack(unread) result = %+v, want all false (the entry will be delivered)", res)
	}
	requireStateUntouched(t, e, key, value)

	// The entry is then delivered to ANOTHER consumer: a stale completion from
	// "me" must not remove that newly delivered pending entry.
	if _, err := e.cli.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: e.group, Consumer: "other-node", Streams: []string{e.stream, ">"}, Count: 1,
	}).Result(); err != nil {
		t.Fatalf("xreadgroup: %v", err)
	}
	res, err = e.store.ackAndRetainTerminal(ctx, e.stream, e.group, id, DefaultInvocationRetention, false)
	if err != nil {
		t.Fatalf("ack(foreign) error = %v, want nil", err)
	}
	if res.Settled || res.Removed || res.Retained {
		t.Fatalf("ack(foreign) result = %+v, want all false (never ACK another consumer's entry)", res)
	}
	requireStateUntouched(t, e, key, value)
	pendingEntries, err := e.cli.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: e.stream, Group: e.group, Start: id, End: id, Count: 1,
	}).Result()
	if err != nil || len(pendingEntries) != 1 || pendingEntries[0].Consumer != "other-node" {
		t.Fatalf("pending entry = (%+v, %v), want exactly one still owned by other-node", pendingEntries, err)
	}

	// Only the owning consumer's ACK resolves the delivery: removed from the
	// PEL and terminal-retained in the same atomic step.
	owner := &invocationStore{client: e.cli, consumer: "other-node"}
	res, err = owner.ackAndRetainTerminal(ctx, e.stream, e.group, id, DefaultInvocationRetention, false)
	if err != nil {
		t.Fatalf("ack(owner) error = %v", err)
	}
	if !res.Settled || !res.Removed || !res.Retained {
		t.Fatalf("ack(owner) result = %+v, want settled+removed+retained", res)
	}
	remaining, err := e.cli.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: e.stream, Group: e.group, Start: id, End: id, Count: 1,
	}).Result()
	if err != nil || len(remaining) != 0 {
		t.Fatalf("pending entry = (%+v, %v), want none after the owning consumer's ACK", remaining, err)
	}
	requireTerminalRetained(t, e, key)
}

// TestIntegrationAckComparesStreamIDsComponentWise pins the exact ID
// comparison inside the ACK script. Whole-string lexicographic comparison
// would misclassify both cases below (1000-0 < 999-0 as text; 1-9 > 1-10 as
// text), and a lossy number parse would collapse the components: only a
// component-wise numeric compare classifies them correctly.
func TestIntegrationAckComparesStreamIDsComponentWise(t *testing.T) {
	ctx := context.Background()

	t.Run("millisecond component dominates", func(t *testing.T) {
		e := newAckDispositionEnv(t)
		if err := EnsureGroup(ctx, e.cli, e.stream, e.group); err != nil {
			t.Fatalf("ensure group: %v", err)
		}
		// 999-0 is delivered and acked while 1000-0 advances the group's
		// last-delivered ID past it.
		for _, id := range []string{"999-0", "1000-0"} {
			if err := e.cli.XAdd(ctx, &redis.XAddArgs{
				Stream: e.stream, ID: id, Values: map[string]any{"value": `{"a":1}`},
			}).Err(); err != nil {
				t.Fatalf("xadd %s: %v", id, err)
			}
		}
		if _, err := e.cli.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: e.group, Consumer: "me", Streams: []string{e.stream, ">"}, Count: 2,
		}).Result(); err != nil {
			t.Fatalf("xreadgroup: %v", err)
		}
		if err := e.cli.XAck(ctx, e.stream, e.group, "999-0").Err(); err != nil {
			t.Fatalf("xack: %v", err)
		}
		key, _ := seedRecoverableState(t, e, e.stream, e.group, "999-0")

		// Numeric: last-delivered 1000-0 >= 999-0 → already acknowledged →
		// settled. Whole-string compare ("1000-0" < "999-0") would say the
		// group has not reached it (unread).
		res, err := e.store.ackAndRetainTerminal(ctx, e.stream, e.group, "999-0", DefaultInvocationRetention, false)
		if err != nil {
			t.Fatalf("ack: %v", err)
		}
		if !res.Settled || res.Removed {
			t.Fatalf("result = %+v, want settled without removal (already acknowledged)", res)
		}
		requireTerminalRetained(t, e, key)
	})

	t.Run("sequence component dominates", func(t *testing.T) {
		e := newAckDispositionEnv(t)
		if err := EnsureGroup(ctx, e.cli, e.stream, e.group); err != nil {
			t.Fatalf("ensure group: %v", err)
		}
		// Only 1-9 is delivered (last-delivered = 1-9); 1-10 is still unread.
		for _, id := range []string{"1-9", "1-10"} {
			if err := e.cli.XAdd(ctx, &redis.XAddArgs{
				Stream: e.stream, ID: id, Values: map[string]any{"value": `{"a":1}`},
			}).Err(); err != nil {
				t.Fatalf("xadd %s: %v", id, err)
			}
		}
		if _, err := e.cli.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: e.group, Consumer: "me", Streams: []string{e.stream, ">"}, Count: 1,
		}).Result(); err != nil {
			t.Fatalf("xreadgroup: %v", err)
		}
		if err := e.cli.XAck(ctx, e.stream, e.group, "1-9").Err(); err != nil {
			t.Fatalf("xack: %v", err)
		}
		key, value := seedRecoverableState(t, e, e.stream, e.group, "1-10")

		// Numeric: sequence 9 < 10, so the group has NOT reached 1-10 and its
		// body exists → unread. Whole-string compare ("1-9" > "1-10") would
		// wrongly settle it.
		res, err := e.store.ackAndRetainTerminal(ctx, e.stream, e.group, "1-10", DefaultInvocationRetention, false)
		if err != nil {
			t.Fatalf("ack: %v", err)
		}
		if res.Settled || res.Removed || res.Retained {
			t.Fatalf("result = %+v, want all false (the group has not reached 1-10)", res)
		}
		requireStateUntouched(t, e, key, value)
	})
}

// TestIntegrationAckSettlesAckedAndTrimmedDeliveries pins the legitimate
// settled dispositions for a delivery that is not in the PEL: an extant entry
// the group already delivered past, and a bodyless entry with an ACK receipt.
// A bodyless entry without that receipt remains unknown.
func TestIntegrationAckSettlesAckedAndTrimmedDeliveries(t *testing.T) {
	ctx := context.Background()

	t.Run("already acknowledged", func(t *testing.T) {
		e := newAckDispositionEnv(t)
		if err := EnsureGroup(ctx, e.cli, e.stream, e.group); err != nil {
			t.Fatalf("ensure group: %v", err)
		}
		id, err := e.cli.XAdd(ctx, &redis.XAddArgs{
			Stream: e.stream, Values: map[string]any{"value": `{"a":1}`},
		}).Result()
		if err != nil {
			t.Fatalf("xadd: %v", err)
		}
		if _, err := e.cli.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: e.group, Consumer: "me", Streams: []string{e.stream, ">"}, Count: 1,
		}).Result(); err != nil {
			t.Fatalf("xreadgroup: %v", err)
		}
		if err := e.cli.XAck(ctx, e.stream, e.group, id).Err(); err != nil {
			t.Fatalf("xack: %v", err)
		}
		key, _ := seedRecoverableState(t, e, e.stream, e.group, id)

		res, err := e.store.ackAndRetainTerminal(ctx, e.stream, e.group, id, DefaultInvocationRetention, false)
		if err != nil {
			t.Fatalf("ack: %v", err)
		}
		if !res.Settled || res.Removed || !res.Retained {
			t.Fatalf("result = %+v, want settled+retained without removal (already acknowledged)", res)
		}
		requireTerminalRetained(t, e, key)
	})

	t.Run("trimmed after acknowledgement settles", func(t *testing.T) {
		e := newAckDispositionEnv(t)
		if err := EnsureGroup(ctx, e.cli, e.stream, e.group); err != nil {
			t.Fatalf("ensure group: %v", err)
		}
		id, err := e.cli.XAdd(ctx, &redis.XAddArgs{
			Stream: e.stream, Values: map[string]any{"value": `{"a":1}`},
		}).Result()
		if err != nil {
			t.Fatalf("xadd: %v", err)
		}
		if _, err := e.cli.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: e.group, Consumer: "me", Streams: []string{e.stream, ">"}, Count: 1,
		}).Result(); err != nil {
			t.Fatalf("xreadgroup: %v", err)
		}
		first, err := e.store.ackAndRetainTerminal(ctx, e.stream, e.group, id, DefaultInvocationRetention, true)
		if err != nil || !first.Settled || !first.Removed {
			t.Fatalf("confirmed ACK = (%+v, %v), want settled+removed", first, err)
		}
		// ACKED retention later trims the acknowledged body. The production ACK
		// script records the receipt before the source body disappears.
		if err := e.cli.XDel(ctx, e.stream, id).Err(); err != nil {
			t.Fatalf("xdel: %v", err)
		}
		key := invocationStateKey(e.stream, e.group, id)

		res, err := e.store.ackAndRetainTerminal(ctx, e.stream, e.group, id, DefaultInvocationRetention, false)
		if err != nil {
			t.Fatalf("ack: %v", err)
		}
		if !res.Settled || res.Removed || !res.Retained {
			t.Fatalf("result = %+v, want settled+retained without removal (the body was trimmed after the ack)", res)
		}
		requireTerminalRetained(t, e, key)
	})

	t.Run("vanished before delivery stays unresolved", func(t *testing.T) {
		e := newAckDispositionEnv(t)
		if err := EnsureGroup(ctx, e.cli, e.stream, e.group); err != nil {
			t.Fatalf("ensure group: %v", err)
		}
		id, err := e.cli.XAdd(ctx, &redis.XAddArgs{
			Stream: e.stream, Values: map[string]any{"value": `{"a":1}`},
		}).Result()
		if err != nil {
			t.Fatalf("xadd: %v", err)
		}
		// The body vanishes BEFORE the group ever delivered it: no read can
		// replay it, but that is absence of proof, not proof of settlement.
		if err := e.cli.XDel(ctx, e.stream, id).Err(); err != nil {
			t.Fatalf("xdel: %v", err)
		}
		key, value := seedRecoverableState(t, e, e.stream, e.group, id)

		res, err := e.store.ackAndRetainTerminal(ctx, e.stream, e.group, id, DefaultInvocationRetention, false)
		if err != nil {
			t.Fatalf("ack: %v", err)
		}
		if res.Settled || res.Removed || res.Retained {
			t.Fatalf("result = %+v, want all false (missing data is never settlement)", res)
		}
		if res.Disposition != dispositionUnknown {
			t.Fatalf("disposition = %s, want unknown", res.Disposition)
		}
		requireStateUntouched(t, e, key, value)
	})
}

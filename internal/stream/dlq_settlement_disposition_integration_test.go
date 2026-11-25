//go:build integration

// Focused F-03 regression coverage for the DLQ settlement boundary between
// the atomic ACK disposition and the durable PostgreSQL rows, against a REAL
// Redis and a REAL PostgreSQL:
//
//   - unread / foreign / missing-group ACK dispositions commit the required
//     DLQ rows but leave the delivery UNSETTLED (no premature settlement) and
//     leave no terminal retention, while the source stays recoverable;
//   - already-acknowledged bodies and trimmed-away deliveries with an ACK
//     receipt settle legitimately with terminal retention applied;
//   - bodyless deliveries without an ACK receipt remain unknown/unsettled.
//
// Excluded from the default suite by the integration build tag; REQUIRES Redis
// at REDIS_TEST_ADDR (default localhost:6379) AND PostgreSQL at
// TEST_DATABASE_URL, failing rather than skipping.
package stream

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/testutil"
)

// requireUnsettledDLQ asserts exactly one DLQ row for the message, still
// unsettled, with no terminal retention on the invocation state.
func requireUnsettledDLQ(t *testing.T, e *testEnv, id, key string) {
	t.Helper()
	rows := e.dlqFor(id)
	if len(rows) != 1 {
		t.Fatalf("DLQ rows = %d, want exactly 1 (committed before the ACK)", len(rows))
	}
	if rows[0].SettledAt != "" {
		t.Fatalf("delivery must stay unsettled while the source is unresolved: %+v", rows[0])
	}
	if exists, err := e.client.HExists(context.Background(), key, terminalField).Result(); err != nil || exists {
		t.Fatalf("terminal marker = (%v, %v), want absent (no premature retention)", exists, err)
	}
}

// requireSettledDLQ asserts exactly one DLQ row for the message, settled, with
// terminal retention applied to the invocation state.
func requireSettledDLQ(t *testing.T, e *testEnv, id, key string) {
	t.Helper()
	rows := e.dlqFor(id)
	if len(rows) != 1 {
		t.Fatalf("DLQ rows = %d, want exactly 1", len(rows))
	}
	if rows[0].SettledAt == "" {
		t.Fatalf("a legitimate settled disposition must settle the delivery: %+v", rows[0])
	}
	if _, err := time.Parse(time.RFC3339, rows[0].SettledAt); err != nil {
		t.Fatalf("SettledAt %q is not RFC3339: %v", rows[0].SettledAt, err)
	}
	e.waitRetained(t, key)
}

// TestIntegrationDLQSettlementFollowsAckDisposition pins which ACK
// dispositions may settle a dead-lettered delivery. Every subtest commits its
// row through the real routeToDLQ path (exhausted handler, real PostgreSQL)
// and then observes the settlement outcome of the atomic ACK under the given
// broker state.
func TestIntegrationDLQSettlementFollowsAckDisposition(t *testing.T) {
	ctx := context.Background()

	t.Run("placeholder DLQ creates ACK receipt without invocation state", func(t *testing.T) {
		testutil.RequireRedis(t)
		e := newEnv(t, ConsumerConfig{})
		id := e.xadd(t, `not-json`)
		key := invocationStateKey(e.stream, e.group, id)
		msg := e.readOneIntoPEL(t)

		e.consumer.processMessage(ctx, msg, 1, exhaustion())
		requireSettledDLQ(t, e, id, key)
		if receipt, err := e.client.HGet(ctx, key, ackReceiptField).Result(); err != nil || receipt != "1" {
			t.Fatalf("placeholder ACK receipt = (%q, %v), want \"1\"", receipt, err)
		}
	})

	t.Run("unread after external recreation stays unsettled", func(t *testing.T) {
		testutil.RequireRedis(t)
		e := newEnv(t, ConsumerConfig{})
		id := e.xadd(t, `{"a":1}`)
		key := invocationStateKey(e.stream, e.group, id)
		msg := e.readOneIntoPEL(t)

		// An external actor destroys and recreates the group at ID 0 while the
		// original delivery is in flight: the entry becomes unread (the local
		// generation is unchanged, so the ACK script runs and classifies).
		if err := e.client.XGroupDestroy(ctx, e.stream, e.group).Err(); err != nil {
			t.Fatalf("xgroup destroy: %v", err)
		}
		if err := EnsureGroup(ctx, e.client, e.stream, e.group); err != nil {
			t.Fatalf("recreate group: %v", err)
		}

		e.consumer.processMessage(ctx, msg, 1, exhaustion())
		requireUnsettledDLQ(t, e, id, key)

		// The source is genuinely still owed: the recreated group replays it.
		replayed := readUnreadIntoPEL(t, e.client, e.stream, e.group, e.consumer.consumer)
		if _, ok := replayed[id]; !ok {
			t.Fatalf("entry %s must remain replayable after an unread disposition; got %v", id, replayed)
		}
	})

	t.Run("foreign owner stays unsettled and pending", func(t *testing.T) {
		testutil.RequireRedis(t)
		e := newEnv(t, ConsumerConfig{})
		id := e.xadd(t, `{"a":1}`)
		key := invocationStateKey(e.stream, e.group, id)
		msg := e.readOneIntoPEL(t)

		// Recreated at ID 0, then delivered to ANOTHER node: the pending entry
		// belongs to that node's live delivery.
		if err := e.client.XGroupDestroy(ctx, e.stream, e.group).Err(); err != nil {
			t.Fatalf("xgroup destroy: %v", err)
		}
		if err := EnsureGroup(ctx, e.client, e.stream, e.group); err != nil {
			t.Fatalf("recreate group: %v", err)
		}
		readUnreadIntoPEL(t, e.client, e.stream, e.group, "other-node")

		e.consumer.processMessage(ctx, msg, 1, exhaustion())
		requireUnsettledDLQ(t, e, id, key)

		// The other node's pending entry must survive the stale completion
		// untouched: still present, still owned by other-node.
		entries, err := e.client.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: e.stream, Group: e.group, Start: id, End: id, Count: 1,
		}).Result()
		if err != nil || len(entries) != 1 || entries[0].Consumer != "other-node" {
			t.Fatalf("pending entry = (%+v, %v), want exactly one owned by other-node", entries, err)
		}
	})

	t.Run("missing group stays unsettled and is recovered", func(t *testing.T) {
		testutil.RequireRedis(t)
		e := newEnv(t, ConsumerConfig{})
		id := e.xadd(t, `{"a":1}`)
		key := invocationStateKey(e.stream, e.group, id)
		msg := e.readOneIntoPEL(t)

		// Destroyed without recreation: the ACK script answers NOGROUP after
		// the rows are committed, so nothing settles and the shared recovery
		// path repairs the group for the replay.
		if err := e.client.XGroupDestroy(ctx, e.stream, e.group).Err(); err != nil {
			t.Fatalf("xgroup destroy: %v", err)
		}

		e.consumer.processMessage(ctx, msg, 1, exhaustion())
		requireUnsettledDLQ(t, e, id, key)
		if !hasGroup(t, e.client, e.stream, e.group) {
			t.Fatal("the NOGROUP ACK failure must trigger the shared group recovery")
		}
		if gen := e.consumer.groupGeneration(); gen < 1 {
			t.Fatalf("group generation = %d, want >= 1 (the recovery must be recorded)", gen)
		}
	})

	t.Run("body trimmed after external bare XACK stays unknown", func(t *testing.T) {
		testutil.RequireRedis(t)
		e := newEnv(t, ConsumerConfig{})
		id := e.xadd(t, `{"a":1}`)
		key := invocationStateKey(e.stream, e.group, id)
		msg := e.readOneIntoPEL(t)

		// A bare external XACK has no Relay receipt. Once the body is trimmed,
		// last-delivered alone cannot distinguish it from a purged dangling PEL.
		if err := e.client.XAck(ctx, e.stream, e.group, id).Err(); err != nil {
			t.Fatalf("xack: %v", err)
		}
		if err := e.client.XDel(ctx, e.stream, id).Err(); err != nil {
			t.Fatalf("xdel: %v", err)
		}

		e.consumer.processMessage(ctx, msg, 1, exhaustion())
		requireUnsettledDLQ(t, e, id, key)
		if state, err := NewDeliveryInspector(e.client).Inspect(ctx, e.stream, e.group, id); err != nil || state != DeliveryUnknown {
			t.Fatalf("Inspect(external bare XACK and trimmed body) = (%v, %v), want unknown", state, err)
		}
	})

	t.Run("vanished body before delivery stays unsettled", func(t *testing.T) {
		testutil.RequireRedis(t)
		e := newEnv(t, ConsumerConfig{})
		id := e.xadd(t, `{"a":1}`)
		key := invocationStateKey(e.stream, e.group, id)
		msg := e.readOneIntoPEL(t)

		// Recreated at ID 0 with the body gone before the fresh group could
		// deliver it. Advance last-delivered past the missing ID; without an ACK
		// receipt it remains unknown and must not settle the committed rows.
		if err := e.client.XGroupDestroy(ctx, e.stream, e.group).Err(); err != nil {
			t.Fatalf("xgroup destroy: %v", err)
		}
		if err := EnsureGroup(ctx, e.client, e.stream, e.group); err != nil {
			t.Fatalf("recreate group: %v", err)
		}
		if err := e.client.XDel(ctx, e.stream, id).Err(); err != nil {
			t.Fatalf("xdel: %v", err)
		}
		advancedID := e.xadd(t, `{"later":true}`)
		if _, err := e.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: e.group, Consumer: e.consumer.consumer,
			Streams: []string{e.stream, ">"}, Count: 1,
		}).Result(); err != nil {
			t.Fatalf("advance recreated group: %v", err)
		}
		if state, err := NewDeliveryInspector(e.client).Inspect(ctx, e.stream, e.group, id); err != nil || state != DeliveryUnknown {
			t.Fatalf("Inspect(missing pre-delivery body) = (%v, %v), want unknown", state, err)
		}
		if !streamPending(t, e.client, e.stream, e.group, advancedID) {
			t.Fatalf("later entry %s was not delivered to advance the group", advancedID)
		}

		e.consumer.processMessage(ctx, msg, 1, exhaustion())
		requireUnsettledDLQ(t, e, id, key)
	})

	t.Run("XAUTOCLAIM purge without ACK leaves DLQ unsettled", func(t *testing.T) {
		testutil.RequireRedis(t)
		e := newEnv(t, ConsumerConfig{})
		id := e.xadd(t, `{"a":1}`)
		key := invocationStateKey(e.stream, e.group, id)
		msg := e.readOneIntoPEL(t)
		if err := e.client.XDel(ctx, e.stream, id).Err(); err != nil {
			t.Fatalf("xdel: %v", err)
		}
		_, _, deleted, err := e.client.XAutoClaimWithDeleted(ctx, &redis.XAutoClaimArgs{
			Stream: e.stream, Group: e.group, Consumer: "reclaimer",
			MinIdle: 0, Start: "0-0", Count: 10,
		}).Result()
		if err != nil {
			t.Fatalf("xautoclaim: %v", err)
		}
		if len(deleted) != 1 || deleted[0] != id {
			t.Fatalf("XAUTOCLAIM deleted IDs = %v, want [%s]", deleted, id)
		}

		e.consumer.processMessage(ctx, msg, 1, exhaustion())
		requireUnsettledDLQ(t, e, id, key)
		if state, err := NewDeliveryInspector(e.client).Inspect(ctx, e.stream, e.group, id); err != nil || state != DeliveryUnknown {
			t.Fatalf("Inspect(purged dangling PEL) = (%v, %v), want unknown", state, err)
		}
	})

	t.Run("already acknowledged settles legitimately", func(t *testing.T) {
		testutil.RequireRedis(t)
		e := newEnv(t, ConsumerConfig{})
		id := e.xadd(t, `{"a":1}`)
		key := invocationStateKey(e.stream, e.group, id)
		msg := e.readOneIntoPEL(t)

		// The source delivery was already resolved (acked by a previous pass
		// whose settlement write was lost): the group's last-delivered ID
		// proves it passed the message, so the rows settle.
		if err := e.client.XAck(ctx, e.stream, e.group, id).Err(); err != nil {
			t.Fatalf("xack: %v", err)
		}

		e.consumer.processMessage(ctx, msg, 1, exhaustion())
		requireSettledDLQ(t, e, id, key)
	})
}

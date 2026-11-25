//go:build integration

// Focused F-03 regression coverage for the settlement-reconciliation broker
// question — DeliveryInspector.Inspect — against a real Redis server. The
// four-way disposition is what lets DLQ settlement mark a row ONLY when the
// broker proves the delivery resolved:
//
//   - pending: still in the PEL, including dangling references (retried, never
//     settled);
//   - unread: the body exists but the group has not reached it yet (replayed
//     after a recreation, never settled);
//   - settled: the group passed an extant body, or an ACK receipt proves a
//     bodyless delivery was acknowledged;
//   - unknown: the body is absent with no ACK receipt — missing data is NOT
//     settlement, even when the group has passed its ID.
//
// It also pins component-wise stream-ID comparison (whole-string comparison
// misclassifies both fixtures below), the NOGROUP error for a missing
// group/stream (ambiguity never settles), and the communication-failure path.
//
// Excluded from the default suite by the integration build tag; REQUIRES Redis
// at REDIS_TEST_ADDR (default localhost:6379), failing rather than skipping.
package stream

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/testutil"
)

// newInspectorStream creates a uniquely named stream with its Relay group and
// registers cleanup of the key.
func newInspectorStream(t *testing.T, cli *redis.Client) (streamName, group string) {
	t.Helper()
	prefix := fmt.Sprintf("inspect-%d", time.Now().UnixNano())
	streamName, group = prefix+"-stream", prefix+"-group"
	if err := EnsureGroup(context.Background(), cli, streamName, group); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	t.Cleanup(func() { cleanupEnvRedisKeys(context.Background(), cli, streamName, group) })
	return streamName, group
}

// requireState asserts the exact Inspect disposition for one identity.
func requireState(t *testing.T, cli *redis.Client, stream, group, msgID string, want DeliveryState) {
	t.Helper()
	got, err := NewDeliveryInspector(cli).Inspect(context.Background(), stream, group, msgID)
	if err != nil {
		t.Fatalf("Inspect(%s/%s/%s) error = %v, want state %v", stream, group, msgID, err, want)
	}
	if got != want {
		t.Fatalf("Inspect(%s/%s/%s) = %v, want %v", stream, group, msgID, got, want)
	}
}

// TestIntegrationDeliveryInspectorClassifiesDispositions pins every
// classification of the atomic inspect script against real broker state.
func TestIntegrationDeliveryInspectorClassifiesDispositions(t *testing.T) {
	cli := testutil.RequireRedis(t)
	ctx := context.Background()

	t.Run("pending including a dangling body", func(t *testing.T) {
		streamName, group := newInspectorStream(t, cli)
		id, err := cli.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName, Values: map[string]any{"value": `{"a":1}`},
		}).Result()
		if err != nil {
			t.Fatalf("xadd: %v", err)
		}
		if _, err := cli.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: group, Consumer: "c", Streams: []string{streamName, ">"}, Count: 1,
		}).Result(); err != nil {
			t.Fatalf("xreadgroup: %v", err)
		}
		requireState(t, cli, streamName, group, id, DeliveryPending)

		// Deleting the body does not un-pend the reference: it stays
		// recoverable (and never settled).
		if err := cli.XDel(ctx, streamName, id).Err(); err != nil {
			t.Fatalf("xdel: %v", err)
		}
		requireState(t, cli, streamName, group, id, DeliveryPending)
	})

	t.Run("unread before the group reaches it", func(t *testing.T) {
		streamName, group := newInspectorStream(t, cli)
		id, err := cli.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName, Values: map[string]any{"value": `{"a":1}`},
		}).Result()
		if err != nil {
			t.Fatalf("xadd: %v", err)
		}
		requireState(t, cli, streamName, group, id, DeliveryUnread)
	})

	t.Run("settled after acknowledgement and after its trim", func(t *testing.T) {
		streamName, group := newInspectorStream(t, cli)
		id, err := cli.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName, Values: map[string]any{"value": `{"a":1}`},
		}).Result()
		if err != nil {
			t.Fatalf("xadd: %v", err)
		}
		if _, err := cli.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: group, Consumer: "c", Streams: []string{streamName, ">"}, Count: 1,
		}).Result(); err != nil {
			t.Fatalf("xreadgroup: %v", err)
		}
		store := &invocationStore{client: cli, consumer: "c"}
		result, err := store.ackAndRetainTerminal(ctx, streamName, group, id, DefaultInvocationRetention, true)
		if err != nil || !result.Settled || !result.Removed {
			t.Fatalf("confirmed ACK = (%+v, %v), want settled+removed", result, err)
		}
		requireState(t, cli, streamName, group, id, DeliverySettled)

		// ACKED retention later trims the acknowledged body. The receipt proves
		// this was settled rather than purged from the PEL without an XACK.
		if err := cli.XDel(ctx, streamName, id).Err(); err != nil {
			t.Fatalf("xdel: %v", err)
		}
		requireState(t, cli, streamName, group, id, DeliverySettled)
	})

	t.Run("unknown when the body vanished before delivery", func(t *testing.T) {
		streamName, group := newInspectorStream(t, cli)
		delivered, err := cli.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName, Values: map[string]any{"value": `{"a":1}`},
		}).Result()
		if err != nil {
			t.Fatalf("xadd: %v", err)
		}
		vanished, err := cli.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName, Values: map[string]any{"value": `{"b":2}`},
		}).Result()
		if err != nil {
			t.Fatalf("xadd: %v", err)
		}
		advanced, err := cli.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName, Values: map[string]any{"value": `{"c":3}`},
		}).Result()
		if err != nil {
			t.Fatalf("xadd: %v", err)
		}
		// Delete the middle body before delivery, then read the later body to
		// move last-delivered past the missing ID without acknowledging it.
		if err := cli.XDel(ctx, streamName, vanished).Err(); err != nil {
			t.Fatalf("xdel: %v", err)
		}
		if _, err := cli.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: group, Consumer: "c", Streams: []string{streamName, ">"}, Count: 2,
		}).Result(); err != nil {
			t.Fatalf("xreadgroup: %v", err)
		}
		requireState(t, cli, streamName, group, delivered, DeliveryPending)
		requireState(t, cli, streamName, group, advanced, DeliveryPending)
		// Missing data with no proof of ACK is unknown even though the group
		// has passed its ID.
		requireState(t, cli, streamName, group, vanished, DeliveryUnknown)
	})

	t.Run("XAUTOCLAIM purges a dangling PEL without ACK proof", func(t *testing.T) {
		streamName, group := newInspectorStream(t, cli)
		id, err := cli.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName, Values: map[string]any{"value": `{"a":1}`},
		}).Result()
		if err != nil {
			t.Fatalf("xadd: %v", err)
		}
		if _, err := cli.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: group, Consumer: "c", Streams: []string{streamName, ">"}, Count: 1,
		}).Result(); err != nil {
			t.Fatalf("xreadgroup: %v", err)
		}
		if err := cli.XDel(ctx, streamName, id).Err(); err != nil {
			t.Fatalf("xdel: %v", err)
		}
		_, _, deleted, err := cli.XAutoClaimWithDeleted(ctx, &redis.XAutoClaimArgs{
			Stream: streamName, Group: group, Consumer: "reclaimer",
			MinIdle: 0, Start: "0-0", Count: 10,
		}).Result()
		if err != nil {
			t.Fatalf("xautoclaim: %v", err)
		}
		if len(deleted) != 1 || deleted[0] != id {
			t.Fatalf("XAUTOCLAIM deleted IDs = %v, want [%s]", deleted, id)
		}
		requireState(t, cli, streamName, group, id, DeliveryUnknown)
	})

	t.Run("expired ACK receipt returns bodyless delivery to unknown", func(t *testing.T) {
		streamName, group := newInspectorStream(t, cli)
		id, err := cli.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName, Values: map[string]any{"value": `{"a":1}`},
		}).Result()
		if err != nil {
			t.Fatalf("xadd: %v", err)
		}
		if _, err := cli.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: group, Consumer: "c", Streams: []string{streamName, ">"}, Count: 1,
		}).Result(); err != nil {
			t.Fatalf("xreadgroup: %v", err)
		}
		store := &invocationStore{client: cli, consumer: "c"}
		result, err := store.ackAndRetainTerminal(ctx, streamName, group, id, 2*time.Second, true)
		if err != nil || !result.Settled {
			t.Fatalf("confirmed ACK = (%+v, %v), want settled", result, err)
		}
		if err := cli.XDel(ctx, streamName, id).Err(); err != nil {
			t.Fatalf("xdel: %v", err)
		}
		testutil.WaitFor(t, 4*time.Second, "ACK receipt retention expiry", func() bool {
			exists, err := cli.Exists(ctx, invocationStateKey(streamName, group, id)).Result()
			return err == nil && exists == 0
		})
		requireState(t, cli, streamName, group, id, DeliveryUnknown)
	})

	t.Run("stream IDs compare component-wise", func(t *testing.T) {
		t.Run("millisecond component dominates", func(t *testing.T) {
			// Whole-string compare would read "1000-0" < "999-0" (unread);
			// numeric compare proves the group passed 999-0 (settled).
			streamName, group := newInspectorStream(t, cli)
			for _, id := range []string{"999-0", "1000-0"} {
				if err := cli.XAdd(ctx, &redis.XAddArgs{
					Stream: streamName, ID: id, Values: map[string]any{"value": `{"a":1}`},
				}).Err(); err != nil {
					t.Fatalf("xadd %s: %v", id, err)
				}
			}
			if _, err := cli.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group: group, Consumer: "c", Streams: []string{streamName, ">"}, Count: 2,
			}).Result(); err != nil {
				t.Fatalf("xreadgroup: %v", err)
			}
			if err := cli.XAck(ctx, streamName, group, "999-0").Err(); err != nil {
				t.Fatalf("xack: %v", err)
			}
			requireState(t, cli, streamName, group, "999-0", DeliverySettled)
		})

		t.Run("sequence component dominates", func(t *testing.T) {
			// Whole-string compare would read "1-9" > "1-10" (settled);
			// numeric compare proves the group has NOT reached 1-10 (unread).
			streamName, group := newInspectorStream(t, cli)
			for _, id := range []string{"1-9", "1-10"} {
				if err := cli.XAdd(ctx, &redis.XAddArgs{
					Stream: streamName, ID: id, Values: map[string]any{"value": `{"a":1}`},
				}).Err(); err != nil {
					t.Fatalf("xadd %s: %v", id, err)
				}
			}
			if _, err := cli.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group: group, Consumer: "c", Streams: []string{streamName, ">"}, Count: 1,
			}).Result(); err != nil {
				t.Fatalf("xreadgroup: %v", err)
			}
			if err := cli.XAck(ctx, streamName, group, "1-9").Err(); err != nil {
				t.Fatalf("xack: %v", err)
			}
			requireState(t, cli, streamName, group, "1-10", DeliveryUnread)
		})
	})

	t.Run("missing group and missing stream are errors, never settled", func(t *testing.T) {
		streamName, _ := newInspectorStream(t, cli)
		inspector := NewDeliveryInspector(cli)
		if state, err := inspector.Inspect(ctx, streamName, "no-such-group", "1-0"); err == nil {
			t.Fatalf("Inspect(missing group) = (%v, nil), want an error", state)
		}
		if state, err := inspector.Inspect(ctx, streamName+"-absent", "g", "1-0"); err == nil {
			t.Fatalf("Inspect(missing stream) = (%v, nil), want an error", state)
		}
	})

	t.Run("communication failure and incomplete identity are errors", func(t *testing.T) {
		streamName, group := newInspectorStream(t, cli)

		dead := auxRedisClient(t, cli)
		if err := dead.Close(); err != nil {
			t.Fatalf("close client: %v", err)
		}
		if state, err := NewDeliveryInspector(dead).Inspect(ctx, streamName, group, "1-0"); err == nil {
			t.Fatalf("Inspect(closed client) = (%v, nil), want an error", state)
		}

		inspector := NewDeliveryInspector(cli)
		for _, parts := range [][3]string{
			{"", group, "1-0"},
			{streamName, "", "1-0"},
			{streamName, group, ""},
		} {
			if state, err := inspector.Inspect(ctx, parts[0], parts[1], parts[2]); err == nil {
				t.Fatalf("Inspect(incomplete %v) = (%v, nil), want an error", parts, state)
			}
		}
	})
}

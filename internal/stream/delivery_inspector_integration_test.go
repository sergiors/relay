//go:build integration

package stream

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/testutil"
)

// TestIntegrationDeliveryInspectorPendingSemantics pins the read-only broker
// question settlement reconciliation asks, against real Redis:
//
//   - a message delivered into the group's PEL is pending, including under
//     another consumer and after its body was deleted (a dangling reference is
//     still recoverable);
//   - a message acked OUT of the PEL is not pending, whether or not its body
//     still exists;
//   - a group/stream that does not exist (NOGROUP) is an error, never a
//     false "settled" — ambiguity must not settle a row.
func TestIntegrationDeliveryInspectorPendingSemantics(t *testing.T) {
	cli := testutil.RequireRedis(t)
	ctx := context.Background()
	streamName := fmt.Sprintf("itest-inspector-%d", time.Now().UnixNano())
	group := "inspector-group"
	if err := EnsureGroup(ctx, cli, streamName, group); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	t.Cleanup(func() { _ = cli.Del(ctx, streamName).Err() })

	inspector := NewDeliveryInspector(cli)
	add := func() string {
		res, err := cli.XAdd(ctx, &redis.XAddArgs{Stream: streamName, Values: map[string]any{"event": `{"a":1}`}}).Result()
		if err != nil {
			t.Fatalf("xadd: %v", err)
		}
		return res
	}
	read := func(consumer string) string {
		msgs, err := cli.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: group, Consumer: consumer, Streams: []string{streamName, ">"}, Count: 1,
		}).Result()
		if err != nil || len(msgs) != 1 || len(msgs[0].Messages) != 1 {
			t.Fatalf("xreadgroup: %+v err=%v", msgs, err)
		}
		return msgs[0].Messages[0].ID
	}

	// Delivered under consumer A: pending.
	pendingID := add()
	read("consumer-a")
	if got, err := inspector.Pending(ctx, streamName, group, pendingID); err != nil || !got {
		t.Fatalf("Pending(delivered) = (%v,%v), want (true,nil)", got, err)
	}

	// Claimed by another consumer: still pending (still recoverable).
	otherID := add()
	read("consumer-b")
	if got, err := inspector.Pending(ctx, streamName, group, otherID); err != nil || !got {
		t.Fatalf("Pending(delivered to another consumer) = (%v,%v), want (true,nil)", got, err)
	}

	// Acked: not pending, whether the body exists or not.
	if err := cli.XAck(ctx, streamName, group, pendingID).Err(); err != nil {
		t.Fatalf("xack: %v", err)
	}
	if got, err := inspector.Pending(ctx, streamName, group, pendingID); err != nil || got {
		t.Fatalf("Pending(acked) = (%v,%v), want (false,nil)", got, err)
	}
	ackedBodyDeleted := add()
	read("consumer-c") // into the PEL
	if err := cli.XAck(ctx, streamName, group, ackedBodyDeleted).Err(); err != nil {
		t.Fatalf("xack: %v", err)
	}
	if err := cli.XDel(ctx, streamName, ackedBodyDeleted).Err(); err != nil {
		t.Fatalf("xdel: %v", err)
	}
	if got, err := inspector.Pending(ctx, streamName, group, ackedBodyDeleted); err != nil || got {
		t.Fatalf("Pending(acked, body deleted) = (%v,%v), want (false,nil)", got, err)
	}

	// A dangling PEL reference (body deleted, still unacked) is still pending:
	// the delivery may be recovered from the PEL even though the payload is gone.
	danglingID := add()
	read("consumer-d")
	if err := cli.XDel(ctx, streamName, danglingID).Err(); err != nil {
		t.Fatalf("xdel: %v", err)
	}
	if got, err := inspector.Pending(ctx, streamName, group, danglingID); err != nil || !got {
		t.Fatalf("Pending(dangling PEL reference) = (%v,%v), want (true,nil)", got, err)
	}

	// An absent group is an ERROR (never a false "settled").
	_, err := inspector.Pending(ctx, streamName, "no-such-group", "1-0")
	if err == nil {
		t.Fatal("Pending on a missing group must return an error (NOGROUP), not a false answer")
	}
	_, err = inspector.Pending(ctx, streamName+"-missing", group, "1-0")
	if err == nil {
		t.Fatal("Pending on a missing stream must return an error, not a false answer")
	}
	// Unknown message IDs inside an existing group are simply not pending (the
	// XPENDING range is empty), which is the correct "no longer recoverable".
	if got, err := inspector.Pending(ctx, streamName, group, "1-999999"); err != nil || got {
		t.Fatalf("Pending(unknown ID) = (%v,%v), want (false,nil)", got, err)
	}
}

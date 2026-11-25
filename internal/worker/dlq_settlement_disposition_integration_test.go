//go:build integration

// Focused F-03 regression coverage for the worker's settlement
// reconciliation — dlqSettlementTick — against REAL Redis and REAL
// PostgreSQL, using the production DeliveryInspector (the atomic classify
// script): a committed dead-letter row is marked settled ONLY on the
// DeliverySettled disposition. Pending, unread, and unknown are preserved;
// a delivery whose acknowledged body retention later trimmed still settles
// (the legitimate missing-body case); and the pass is idempotent.
//
// Excluded from the default suite by the integration build tag; REQUIRES
// Redis at REDIS_TEST_ADDR and PostgreSQL at TEST_DATABASE_URL (defaults are
// local), failing rather than skipping.
package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/dlq"
	"relay/internal/stream"
	"relay/internal/testutil"
)

func ackWithRelayConsumer(t *testing.T, cli *redis.Client, streamName, group, messageID string) {
	t.Helper()
	// The fixture already has a DLQ row; the terminal hash models its
	// disposition and makes a confirmed re-ACK retain the settlement receipt.
	key := "relay:invocation:" + streamName + ":" + group + ":" + messageID
	if err := cli.HSet(context.Background(), key, "__terminal", "1").Err(); err != nil {
		t.Fatalf("seed terminal DLQ state: %v", err)
	}
	consumer := stream.NewConsumer(stream.ConsumerConfig{
		Client:          cli,
		Stream:          streamName,
		Group:           group,
		Consumer:        "settler-ack-itest",
		Block:           10 * time.Millisecond,
		ReclaimInterval: time.Hour,
		MinPendingIdle:  time.Hour,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- consumer.Consume(ctx, func(context.Context, string, map[string]any) error {
			return nil
		})
	}()

	testutil.WaitFor(t, 5*time.Second, "Relay ACK receipt", func() bool {
		value, err := cli.HGet(ctx, key, "__ack_confirmed").Result()
		return err == nil && value == "1"
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("consumer stopped with error: %v", err)
	}
}

func cleanupDLQSettlementRedis(ctx context.Context, cli *redis.Client, streamName, group string) {
	streamPart := encodeDLQSettlementKeyComponent(streamName)
	groupPart := encodeDLQSettlementKeyComponent(group)
	_ = cli.Del(ctx, streamName, "relay:stream-group-generation:"+streamPart+":"+groupPart).Err()

	match := "relay:invocation:" + streamPart + ":" + groupPart + ":*"
	var cursor uint64
	for {
		keys, next, err := cli.Scan(ctx, cursor, match, 0).Result()
		if err != nil {
			break
		}
		if len(keys) > 0 {
			_ = cli.Del(ctx, keys...).Err()
		}
		if next == 0 {
			break
		}
		cursor = next
	}
}

func encodeDLQSettlementKeyComponent(value string) string {
	value = strings.ReplaceAll(value, "%", "%25")
	return strings.ReplaceAll(value, ":", "%3A")
}

// TestIntegrationDLQSettlementClassifiesDispositions seeds four unsettled
// deliveries in one real consumer group and proves the reconciliation marks
// exactly the provably-settled one.
func TestIntegrationDLQSettlementClassifiesDispositions(t *testing.T) {
	testutil.RequireStateDatabase(t)
	cli := testutil.RequireRedis(t)
	st := testutil.NewState(t)
	ctx := context.Background()

	streamName := "itest-settledisp-" + time.Now().Format("20060102150405.000000000")
	group := "settledisp-group"
	if err := stream.EnsureGroup(ctx, cli, streamName, group); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	t.Cleanup(func() { cleanupDLQSettlementRedis(ctx, cli, streamName, group) })

	xadd := func() string {
		t.Helper()
		id, err := cli.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName, Values: map[string]any{"value": `{"case":"disp"}`},
		}).Result()
		if err != nil {
			t.Fatalf("xadd: %v", err)
		}
		return id
	}
	read := func(count int) {
		t.Helper()
		if _, err := cli.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: group, Consumer: "settler-itest",
			Streams: []string{streamName, ">"}, Count: int64(count),
		}).Result(); err != nil {
			t.Fatalf("xreadgroup(%d): %v", count, err)
		}
	}
	seedRow := func(msgID, handler string) dlq.Entry {
		t.Helper()
		deliveryID := dlq.NewDeliveryID(streamName, group, msgID)
		entry := dlq.Entry{
			ID:              dlq.NewIDForDelivery(deliveryID, "fn/settle."+handler),
			Source:          streamName,
			SourceGroup:     group,
			SourceMessageID: msgID,
			DeliveryID:      deliveryID,
			Consumer:        "settler-itest",
			Event:           `{"case":"disp"}`,
			Reason:          "invocation exhausted: function \"fn\" handler \"settle\" exhausted after 1 handler attempts",
			App:             "fn",
			Handler:         "settle." + handler,
			Deliveries:      1,
			HandlerAttempts: 1,
			Timestamp:       time.Now().UTC().Format(time.RFC3339),
		}
		if inserted, err := st.SaveDLQEntry(ctx, entry); err != nil || !inserted {
			t.Fatalf("seed DLQ entry %s = (%v, %v)", entry.ID, inserted, err)
		}
		return entry
	}

	// The production consumer ACKs the message and writes its receipt; after
	// trimming, reconciliation can still prove this delivery settled.
	trimmedID := xadd()
	ackWithRelayConsumer(t, cli, streamName, group, trimmedID)
	if err := cli.XDel(ctx, streamName, trimmedID).Err(); err != nil {
		t.Fatalf("xdel trimmed: %v", err)
	}
	// Delivered and still pending → DeliveryPending (preserved).
	pendingID := xadd()
	read(1)
	// Added, then its body vanished BEFORE the group reached it →
	// DeliveryUnknown (preserved: missing data is not settlement).
	unknownID := xadd()
	if err := cli.XDel(ctx, streamName, unknownID).Err(); err != nil {
		t.Fatalf("xdel unknown: %v", err)
	}
	// Added but never delivered, body intact → DeliveryUnread (preserved).
	unreadID := xadd()

	trimmed := seedRow(trimmedID, "trimmed")
	pending := seedRow(pendingID, "pending")
	unknown := seedRow(unknownID, "unknown")
	unread := seedRow(unreadID, "unread")

	inspector := stream.NewDeliveryInspector(cli)
	tick := func() {
		t.Helper()
		dlqSettlementTick(ctx, st, inspector, "", time.Now, testutil.DiscardLogger())
	}
	settledAt := func(entry dlq.Entry) string {
		t.Helper()
		got, ok, err := st.GetDLQEntry(ctx, entry.ID)
		if err != nil || !ok {
			t.Fatalf("get %s = (%v, %v)", entry.ID, ok, err)
		}
		return got.SettledAt
	}

	tick()

	if at := settledAt(trimmed); at == "" {
		t.Fatal("the acknowledged-then-trimmed delivery must settle (its body can never replay)")
	} else if _, err := time.Parse(time.RFC3339, at); err != nil {
		t.Fatalf("trimmed SettledAt %q is not RFC3339: %v", at, err)
	}
	for _, preserved := range []dlq.Entry{pending, unknown, unread} {
		if at := settledAt(preserved); at != "" {
			t.Fatalf("%s must stay unsettled (SettledAt = %q)", preserved.Handler, at)
		}
	}

	// The pass is idempotent: a second tick re-settles nothing and preserves
	// the rest exactly as before.
	tick()
	if settledAt(trimmed) == "" {
		t.Fatal("the settled delivery must stay settled")
	}
	for _, preserved := range []dlq.Entry{pending, unknown, unread} {
		if at := settledAt(preserved); at != "" {
			t.Fatalf("%s must stay unsettled on a later pass (SettledAt = %q)", preserved.Handler, at)
		}
	}
}

// TestIntegrationDLQSettlementPreservedOnInspectionFailure pins the
// communication-failure direction with the production inspector: when Redis
// cannot answer, the tick preserves every unsettled row (ambiguity is never
// settlement).
func TestIntegrationDLQSettlementPreservedOnInspectionFailure(t *testing.T) {
	testutil.RequireStateDatabase(t)
	cli := testutil.RequireRedis(t)
	st := testutil.NewState(t)
	ctx := context.Background()

	streamName := "itest-settlecomm-" + time.Now().Format("20060102150405.000000000")
	group := "settlecomm-group"
	if err := stream.EnsureGroup(ctx, cli, streamName, group); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	t.Cleanup(func() { cleanupDLQSettlementRedis(ctx, cli, streamName, group) })

	id, err := cli.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName, Values: map[string]any{"value": `{"case":"comm"}`},
	}).Result()
	if err != nil {
		t.Fatalf("xadd: %v", err)
	}
	deliveryID := dlq.NewDeliveryID(streamName, group, id)
	entry := dlq.Entry{
		ID:              dlq.NewIDForDelivery(deliveryID, "fn/settle.comm"),
		Source:          streamName,
		SourceGroup:     group,
		SourceMessageID: id,
		DeliveryID:      deliveryID,
		Consumer:        "settler-itest",
		Event:           `{"case":"comm"}`,
		Reason:          "invocation exhausted",
		App:             "fn",
		Handler:         "settle.comm",
		Deliveries:      1,
		HandlerAttempts: 1,
		Timestamp:       time.Now().UTC().Format(time.RFC3339),
	}
	if inserted, err := st.SaveDLQEntry(ctx, entry); err != nil || !inserted {
		t.Fatalf("seed DLQ entry = (%v, %v)", inserted, err)
	}

	// The inspector's client cannot reach Redis: every inspection errors, so
	// the tick must mark nothing. Build an independent client from the same
	// options and close it to force the transport failure while the live
	// inspection client stays open for cleanup.
	opts := *cli.Options()
	dead := redis.NewClient(&opts)
	t.Cleanup(func() { _ = dead.Close() })
	if err := dead.Close(); err != nil {
		t.Fatalf("close client: %v", err)
	}
	dlqSettlementTick(ctx, st, stream.NewDeliveryInspector(dead), "", time.Now, testutil.DiscardLogger())

	got, ok, err := st.GetDLQEntry(ctx, entry.ID)
	if err != nil || !ok {
		t.Fatalf("get entry = (%v, %v)", ok, err)
	}
	if got.SettledAt != "" {
		t.Fatalf("a failed broker inspection must preserve the row: %+v", got)
	}
	refs, err := st.ListUnsettledDLQDeliveries(ctx, "", 10)
	if err != nil || len(refs) != 1 {
		t.Fatalf("unsettled deliveries = (%+v, %v), want the seeded row preserved", refs, err)
	}
}

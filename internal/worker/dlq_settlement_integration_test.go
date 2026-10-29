//go:build integration

// This file proves the worker's bounded ACK/settlement reconciler against real
// PostgreSQL and real Redis: a delivery whose dead letters were committed but
// whose source ACK reply was lost (the crash-before-settlement window, or any
// lost settlement write) is settled by reconciliation once the broker reports
// the delivery is no longer pending; a delivery still pending — or any broker
// error, including a missing group — is preserved unsettled.
package worker

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/dlq"
	"relay/internal/state"
	"relay/internal/stream"
	"relay/internal/testutil"
)

// settlementEnv is one real Redis stream/group carrying three DLQ deliveries
// whose settlement was lost:
//
//   - confirmed: delivered, committed, and XACKed (no longer pending);
//   - pending:   delivered, committed, and still pending in the PEL;
//   - ambiguous: committed with a source group that no longer exists, so the
//     broker question cannot be answered (NOGROUP).
type settlementEnv struct {
	cli *redis.Client
	st  *state.State

	streamName string
	group      string

	confirmed dlq.Entry
	pending   dlq.Entry
	ambiguous dlq.Entry
}

func newSettlementEnv(t *testing.T) *settlementEnv {
	t.Helper()
	testutil.RequireStateDatabase(t)
	cli := testutil.RequireRedis(t)
	st := testutil.NewState(t)
	ctx := context.Background()

	streamName := "itest-settle-" + time.Now().Format("20060102150405.000000000")
	group := "settle-group"
	if err := stream.EnsureGroup(ctx, cli, streamName, group); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	e := &settlementEnv{
		cli: cli, st: st,
		streamName: streamName,
		group:      group,
	}
	e.confirmed = e.seed(t, confirmedCase, true)
	e.pending = e.seed(t, pendingCase, false)
	e.ambiguous = e.seed(t, ambiguousCase, false)
	return e
}

const (
	confirmedCase = "confirmed"
	pendingCase   = "pending"
	ambiguousCase = "nogroup"
)

// seedsOneCrashedDelivery writes the durable state one delivery leaves behind
// when a worker crashed AFTER committing its DLQ row but BEFORE recording the
// settlement of its source ACK: one unsettled row referring to a real stream
// group message. ack=true performs the source XACK (a confirmed delivery
// whose settlement write was lost); ack=false leaves the message recoverable.
func (e *settlementEnv) seed(t *testing.T, cased string, ack bool) dlq.Entry {
	t.Helper()
	ctx := context.Background()

	source, group, msgID := e.streamName, e.group, ""
	if cased == ambiguousCase {
		// The delivery names a stream/group that never exists: the inspector's
		// XPENDING lookup fails with NOGROUP.
		source, group, msgID = e.streamName+"-missing", "no-such-group", "1-0"
	} else {
		res, err := e.cli.XAdd(ctx, &redis.XAddArgs{
			Stream: source, Values: map[string]any{"event": `{"case":"` + cased + `"}`},
		}).Result()
		if err != nil {
			t.Fatalf("xadd %s: %v", cased, err)
		}
		msgID = res
		// Deliver the message into the group's PEL: a DLQ row always refers to a
		// message that WAS delivered, so the fixture must be in the PEL before
		// the (optional) XACK.
		if _, err := e.cli.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: group, Consumer: "settler-itest", Streams: []string{source, ">"}, Count: 1,
		}).Result(); err != nil {
			t.Fatalf("xreadgroup %s: %v", cased, err)
		}
	}
	deliveryID := dlq.NewDeliveryID(source, group, msgID)
	entry := dlq.Entry{
		ID:              dlq.NewIDForDelivery(deliveryID, "fn/settle."+cased),
		Source:          source,
		SourceGroup:     group,
		SourceMessageID: msgID,
		DeliveryID:      deliveryID,
		Consumer:        "settler-itest",
		Event:           `{"case":"` + cased + `"}`,
		Reason:          "invocation exhausted: function \"fn\" handler \"settle\" exhausted after 1 handler attempts",
		App:             "fn",
		Handler:         "settle." + cased,
		Deliveries:      1,
		HandlerAttempts: 1,
		Timestamp:       time.Now().UTC().Format(time.RFC3339),
	}
	if inserted, err := e.st.SaveDLQEntry(ctx, entry); err != nil || !inserted {
		t.Fatalf("seed DLQ entry %s = (%v,%v)", entry.ID, inserted, err)
	}
	if ack {
		if err := e.cli.XAck(ctx, source, group, msgID).Err(); err != nil {
			t.Fatalf("xack (the confirmed-but-unsettled ACK): %v", err)
		}
	}
	return entry
}

func TestIntegrationDLQSettlementReconcilerRepairsCrashBeforeSettle(t *testing.T) {
	e := newSettlementEnv(t)
	ctx := context.Background()

	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	dlqSettlementTick(ctx, e.st, stream.NewDeliveryInspector(e.cli), "", func() time.Time { return now }, testutil.DiscardLogger())

	// The confirmed delivery is repaired: its rows are settled with the
	// recorded instant and its delivery no longer reconciles.
	settled, ok, err := e.st.GetDLQEntry(ctx, e.confirmed.ID)
	if err != nil || !ok {
		t.Fatalf("get confirmed = (%v,%v)", ok, err)
	}
	if settled.SettledAt == "" {
		t.Fatalf("the crashed-settlement delivery must be repaired: %+v", settled)
	}
	if parsed, err := time.Parse(time.RFC3339, settled.SettledAt); err != nil || !parsed.Equal(now.UTC()) {
		t.Fatalf("SettledAt = %q, want %q", settled.SettledAt, now.UTC().Format(time.RFC3339))
	}

	// The still-pending delivery is preserved: at-least-once delivery means the
	// message may still be recovered from the PEL.
	preserved, _, _ := e.st.GetDLQEntry(ctx, e.pending.ID)
	if preserved.SettledAt != "" {
		t.Fatalf("a still-pending delivery must stay unsettled: %+v", preserved)
	}
	// The ambiguous (missing group) delivery is preserved too: an unsettled
	// broker answer is never proof of settlement.
	ambiguous, _, _ := e.st.GetDLQEntry(ctx, e.ambiguous.ID)
	if ambiguous.SettledAt != "" {
		t.Fatalf("an unresolved group must stay unsettled: %+v", ambiguous)
	}
	refs, err := e.st.ListUnsettledDLQDeliveries(ctx, "", 10)
	if err != nil {
		t.Fatalf("list unsettled: %v", err)
	}
	if len(refs) != 2 {
		t.Fatalf("unsettled deliveries = %+v, want exactly the pending and ambiguous ones", refs)
	}

	// The reconciliation is idempotent: a later pass re-settles nothing and
	// preserves the rest.
	dlqSettlementTick(ctx, e.st, stream.NewDeliveryInspector(e.cli), "", time.Now, testutil.DiscardLogger())
	if settled, _, _ := e.st.GetDLQEntry(ctx, e.confirmed.ID); settled.SettledAt == "" {
		t.Fatal("the confirmed delivery must stay settled")
	}
	if preserved, _, _ := e.st.GetDLQEntry(ctx, e.pending.ID); preserved.SettledAt != "" {
		t.Fatal("the pending delivery must stay unsettled on a later pass")
	}
	if ambiguous, _, _ := e.st.GetDLQEntry(ctx, e.ambiguous.ID); ambiguous.SettledAt != "" {
		t.Fatal("the ambiguous delivery must stay unsettled on a later pass")
	}
}

//go:build integration

// This file exercises the `relay dlq` commands against a real PostgreSQL via
// the production OpenDLQStore path (openPostgresDLQStore -> state.State): an
// exhausted delivery's durable rows are listed, inspected, replay-prepared, and
// removed over the wire.
//
// Excluded from the default suite by the integration build tag. Running it
// (`go test -tags=integration ./...`) REQUIRES PostgreSQL at TEST_DATABASE_URL
// (default localhost:5432); a missing dependency fails rather than skips.
package cli

import (
	"context"
	"strings"
	"testing"
	"time"

	"relay/internal/dlq"
	"relay/internal/testutil"
)

// TestIntegrationDLQCommandsAgainstPostgres drives the real `relay dlq`
// command path (the production openPostgresDLQStore) against a real
// PostgreSQL schema, verifying ls/inspect/rm over the wire, and that an
// UNSETTLED entry (its delivery not yet acked) is refused by `rm`. The whole
// command family is broker-neutral: it opens only the PostgreSQL state handle
// (no Redis client, no worker contact), so removal and listing cannot mutate
// invocation state or cause execution by construction — this test configures
// no broker at all.
func TestIntegrationDLQCommandsAgainstPostgres(t *testing.T) {
	testutil.RequireStateDatabase(t)
	dsn := testutil.NewStateDSN(t)
	ctx := context.Background()

	// Seed entries through the SAME store the commands open, so the identity
	// grammar is the production one.
	st := testutil.OpenState(t, dsn)
	deliveryID := dlq.NewDeliveryID("events-test", "relay-test", "1769199999991-0")
	settled := dlq.Entry{
		ID:              dlq.NewIDForDelivery(deliveryID, "alpha/events.a.handler"),
		Source:          "events-test",
		SourceGroup:     "relay-test",
		SourceMessageID: "1769199999991-0",
		DeliveryID:      deliveryID,
		Consumer:        "worker-1",
		Event:           `{"event_name":"INSERT","id":7}`,
		Reason:          "invocation exhausted",
		App:             "alpha",
		Handler:         "events.a.handler",
		Deliveries:      9,
		HandlerAttempts: 5,
		Timestamp:       time.Now().UTC().Format(time.RFC3339),
	}
	if inserted, err := st.SaveDLQEntry(ctx, settled); err != nil || !inserted {
		t.Fatalf("seed settled entry = (%v,%v)", inserted, err)
	}
	if err := st.MarkDLQDeliverySettled(ctx, deliveryID, time.Now()); err != nil {
		t.Fatalf("settle seeded delivery: %v", err)
	}
	unsettled := settled
	unsettled.ID = dlq.NewIDForDelivery(deliveryID, "alpha/events.b.handler")
	unsettled.HandlerAttempts = 2
	if inserted, err := st.SaveDLQEntry(ctx, unsettled); err != nil || !inserted {
		t.Fatalf("seed unsettled entry = (%v,%v)", inserted, err)
	}

	// The commands resolve the state DSN from the environment, exactly as the
	// production opener does.
	t.Setenv("DATABASE_URL", dsn)
	deps := testDeps(t)
	// The production opener: it resolves DATABASE_URL and returns the state
	// handle the other administrative commands share.
	deps.OpenDLQ = openPostgresDLQStore

	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "ls")
	if err != nil {
		t.Fatalf("dlq ls: %v", err)
	}
	for _, want := range []string{settled.ID, "events-test/1769199999991-0", "alpha", "events.a.handler", "5"} {
		if !strings.Contains(out, want) {
			t.Fatalf("ls missing %q:\n%s", want, out)
		}
	}

	out, _, err = runCLIWithDeps(t, deps, "", "dlq", "inspect", settled.ID)
	if err != nil {
		t.Fatalf("dlq inspect: %v", err)
	}
	flat := normWS(out)
	for _, want := range []string{"App: alpha", "Handler attempts: 5", "Handler: events.a.handler", "Deliveries: 9"} {
		if !strings.Contains(flat, want) {
			t.Fatalf("inspect missing %q:\n%s", want, out)
		}
	}
	// The event JSON is pretty-printed across lines.
	if !strings.Contains(out, `"event_name": "INSERT"`) {
		t.Fatalf("inspect event not pretty-printed:\n%s", out)
	}

	// An unknown ID is a clear error naming the entry.
	if _, _, err := runCLIWithDeps(t, deps, "", "dlq", "inspect", "no-such-id"); err == nil {
		t.Fatal("inspect on an unknown ID must fail")
	} else if !strings.Contains(err.Error(), "no-such-id") {
		t.Fatalf("unknown-ID error = %v, want it to name the entry", err)
	}

	// `rm` refuses the unsettled entry: its source delivery may still be
	// recoverable, so the row is needed by delivery recovery.
	if _, _, err := runCLIWithDeps(t, deps, "", "dlq", "rm", unsettled.ID); err == nil {
		t.Fatal("rm on an unsettled entry must fail")
	}
	if _, ok, _ := st.GetDLQEntry(ctx, unsettled.ID); !ok {
		t.Fatal("the unsettled entry must remain after the refused rm")
	}

	// `rm` removes exactly the settled entry and leaves the sibling row intact.
	if _, _, err := runCLIWithDeps(t, deps, "", "dlq", "rm", settled.ID); err != nil {
		t.Fatalf("dlq rm: %v", err)
	}
	if _, ok, _ := st.GetDLQEntry(ctx, settled.ID); ok {
		t.Fatal("rm must delete exactly the settled entry")
	}
	if _, ok, _ := st.GetDLQEntry(ctx, unsettled.ID); !ok {
		t.Fatal("rm must leave the unsettled sibling entry in place")
	}
}

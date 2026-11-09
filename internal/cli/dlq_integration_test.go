//go:build integration

// This file exercises the `relay dlq` commands against a real PostgreSQL via
// the production OpenDLQStore path (openPostgresDLQStore -> state.State).
//
// Excluded from the default suite by the integration build tag. These focused
// tests require PostgreSQL at TEST_DATABASE_URL (default localhost:5432); a
// missing dependency fails rather than skips.
package cli

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"relay/internal/dlq"
	"relay/internal/testutil"
)

// TestIntegrationDLQCommandsAgainstPostgres drives the real command path and
// verifies that listing displays full IDs while inspect and rm require
// the complete 64-character identity. Malformed and abbreviated IDs take the
// exact-lookup unknown-entry path, without replay execution or deletion.
func TestIntegrationDLQCommandsAgainstPostgres(t *testing.T) {
	dsn := testutil.NewStateDSN(t)
	ctx := context.Background()
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
	t.Setenv("DATABASE_URL", dsn)
	deps := testDeps(t)
	deps.OpenDLQ = openPostgresDLQStore

	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "ls")
	if err != nil {
		t.Fatalf("dlq ls: %v", err)
	}
	for _, want := range []string{settled.ID, unsettled.ID, "alpha", "events.a.handler", "5"} {
		if !strings.Contains(out, want) {
			t.Fatalf("ls missing %q:\n%s", want, out)
		}
	}
	for _, unwanted := range []string{"events-test/1769199999991-0"} {
		if strings.Contains(out, unwanted) {
			t.Fatalf("ls must not print %q:\n%s", unwanted, out)
		}
	}

	out, _, err = runCLIWithDeps(t, deps, "", "dlq", "inspect", settled.ID)
	if err != nil {
		t.Fatalf("dlq inspect: %v", err)
	}
	flat := normWS(out)
	for _, want := range []string{"ID: " + settled.ID, "App: alpha", "Handler attempts: 5", "Handler: events.a.handler", "Deliveries: 9"} {
		if !strings.Contains(flat, want) {
			t.Fatalf("inspect missing %q:\n%s", want, out)
		}
	}

	// Short and malformed IDs are exact misses, not format errors. Replay must
	// fail at claim admission without contacting the worker.
	for _, cmd := range []string{"inspect", "replay", "rm"} {
		for _, id := range []string{settled.ID[:10], "not-a-hex-id", "1-0"} {
			if _, _, err := runCLIWithDeps(t, deps, "", "dlq", cmd, id); err == nil ||
				!strings.Contains(err.Error(), fmt.Sprintf("unknown DLQ entry %q", id)) {
				t.Fatalf("dlq %s %q err = %v, want unknown-entry error", cmd, id, err)
			}
		}
	}

	if _, _, err := runCLIWithDeps(t, deps, "", "dlq", "rm", unsettled.ID); err == nil ||
		!strings.Contains(err.Error(), "is not settled") {
		t.Fatalf("rm of unsettled entry err = %v, want not-settled refusal", err)
	}
	if _, _, err := runCLIWithDeps(t, deps, "", "dlq", "rm", settled.ID); err != nil {
		t.Fatalf("dlq rm by full ID: %v", err)
	}
	if _, ok, _ := st.GetDLQEntry(ctx, settled.ID); ok {
		t.Fatal("rm must delete the exact settled entry")
	}
	if _, ok, _ := st.GetDLQEntry(ctx, unsettled.ID); !ok {
		t.Fatal("rm must leave the unsettled sibling in place")
	}
}

// TestIntegrationDLQReplayByFullID preserves the real PostgreSQL claim,
// fencing, and delete path while requiring the full entry identity.
func TestIntegrationDLQReplayByFullID(t *testing.T) {
	dsn := testutil.NewStateDSN(t)
	ctx := context.Background()
	st := testutil.OpenState(t, dsn)
	deliveryID := dlq.NewDeliveryID("events-test", "relay-test", "1769199999995-0")
	entry := dlq.Entry{
		ID:              dlq.NewIDForDelivery(deliveryID, "user-events/events.created.handler"),
		Source:          "events-test",
		SourceGroup:     "relay-test",
		SourceMessageID: "1769199999995-0",
		DeliveryID:      deliveryID,
		Consumer:        "worker-1",
		Event:           `{"event_name":"INSERT"}`,
		ReplayPayload:   []byte(`{"event_name":"INSERT"}`),
		Reason:          "invocation exhausted",
		App:             "user-events",
		Handler:         "events.created.handler",
		HandlerAttempts: 2,
		Timestamp:       time.Now().UTC().Format(time.RFC3339),
	}
	if inserted, err := st.SaveDLQEntry(ctx, entry); err != nil || !inserted {
		t.Fatalf("seed entry = (%v, %v)", inserted, err)
	}
	if err := st.MarkDLQDeliverySettled(ctx, deliveryID, time.Now()); err != nil {
		t.Fatalf("settle delivery: %v", err)
	}

	exec := &dlqReplayExecutor{}
	deps := integrationReplayDeps(t, dsn, exec)
	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", entry.ID)
	if err != nil {
		t.Fatalf("dlq replay by full ID: %v", err)
	}
	if strings.TrimSpace(out) != "Replayed handler successfully" {
		t.Fatalf("stdout = %q", out)
	}
	if handlers, payloads := exec.got(); len(handlers) != 1 || handlers[0] != entry.Handler {
		t.Fatalf("executed handlers = %v, want exactly %q", handlers, entry.Handler)
	} else if string(payloads[0]) != string(entry.ReplayPayload) {
		t.Fatalf("executed payload = %q, want %q", payloads[0], entry.ReplayPayload)
	}
	if _, ok, err := st.GetDLQEntry(ctx, entry.ID); err != nil || ok {
		t.Fatalf("successful replay must delete the exact row: (ok=%v, err=%v)", ok, err)
	}
}

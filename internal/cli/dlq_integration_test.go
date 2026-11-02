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
	"fmt"
	"strings"
	"testing"
	"time"

	"relay/internal/dlq"
	"relay/internal/testutil"
)

// TestIntegrationDLQCommandsAgainstPostgres drives the real `relay dlq`
// command path (the production openPostgresDLQStore) against a real
// PostgreSQL schema, verifying ls (short IDs only) and inspect/rm over the
// wire — both by the full ID and by the unique ten-character prefix `dlq ls`
// shows — and that an UNSETTLED entry (its delivery not yet acked) is refused
// by `rm` even when addressed by its prefix, while malformed identifiers are
// rejected before any lookup. The whole command family is broker-neutral: it
// opens only the PostgreSQL state handle (no Redis client, no worker
// contact), so removal and listing cannot mutate invocation state or cause
// execution by construction — this test configures no broker at all.
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
	// The prefix seams require each seeded entry's ls-visible prefix to be
	// unique among the schema's rows; the production identity digests make
	// that so, and this guard keeps any fixture drift loud.
	if settled.ID[:10] == unsettled.ID[:10] {
		t.Fatalf("fixture drift: the seeded entries share the visible prefix %q", settled.ID[:10])
	}

	// The commands resolve the state DSN from the environment, exactly as the
	// production opener does.
	t.Setenv("DATABASE_URL", dsn)
	deps := testDeps(t)
	// The production opener: it resolves DATABASE_URL and returns the state
	// handle the other administrative commands share.
	deps.OpenDLQ = openPostgresDLQStore

	// ls shows exactly the ten-character prefix of each entry: never the full
	// stable ID and no ORIGINAL source column.
	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "ls")
	if err != nil {
		t.Fatalf("dlq ls: %v", err)
	}
	for _, want := range []string{settled.ID[:10], unsettled.ID[:10], "alpha", "events.a.handler", "5"} {
		if !strings.Contains(out, want) {
			t.Fatalf("ls missing %q:\n%s", want, out)
		}
	}
	for _, unwanted := range []string{settled.ID, unsettled.ID, "events-test/1769199999991-0"} {
		if strings.Contains(out, unwanted) {
			t.Fatalf("ls must not print %q (full IDs and originals stay out):\n%s", unwanted, out)
		}
	}

	// inspect resolves the unique prefix to the FULL stored ID and the
	// original delivery metadata.
	for _, input := range []string{settled.ID, settled.ID[:10]} {
		out, _, err = runCLIWithDeps(t, deps, "", "dlq", "inspect", input)
		if err != nil {
			t.Fatalf("dlq inspect via %q: %v", input, err)
		}
		flat := normWS(out)
		for _, want := range []string{"ID: " + settled.ID, "App: alpha", "Handler attempts: 5", "Handler: events.a.handler", "Deliveries: 9"} {
			if !strings.Contains(flat, want) {
				t.Fatalf("inspect via %q missing %q:\n%s", input, want, out)
			}
		}
		// The event JSON is pretty-printed across lines.
		if !strings.Contains(out, `"event_name": "INSERT"`) {
			t.Fatalf("inspect event not pretty-printed:\n%s", out)
		}
	}

	// An unknown full ID is a clear error naming the entry.
	unknownID := strings.Repeat("0", 64)
	if _, _, err := runCLIWithDeps(t, deps, "", "dlq", "inspect", unknownID); err == nil {
		t.Fatal("inspect on an unknown ID must fail")
	} else if !strings.Contains(err.Error(), "unknown DLQ entry") || !strings.Contains(err.Error(), unknownID) {
		t.Fatalf("unknown-ID error = %v, want it to name the entry exactly", err)
	}

	// A malformed identifier — wildcards are characters, never a query — is
	// rejected by the grammar before any lookup.
	for _, invalid := range []string{"deadbeef%1", "DEADBEEF01", "1-0"} {
		if _, _, err := runCLIWithDeps(t, deps, "", "dlq", "rm", invalid); err == nil ||
			!strings.Contains(err.Error(), "invalid DLQ entry ID") {
			t.Fatalf("rm of %q err = %v, want an explicit invalid-ID error", invalid, err)
		}
	}

	// `rm` refuses the unsettled entry even when addressed by its prefix: its
	// source delivery may still be recoverable, so the row is needed by
	// delivery recovery.
	if _, _, err := runCLIWithDeps(t, deps, "", "dlq", "rm", unsettled.ID[:10]); err == nil ||
		!strings.Contains(err.Error(), "is not settled") {
		t.Fatalf("rm of the unsettled prefix err = %v, want the not-settled refusal", err)
	}
	if _, ok, _ := st.GetDLQEntry(ctx, unsettled.ID); !ok {
		t.Fatal("the unsettled entry must remain after the refused rm")
	}

	// `rm` via the ls-visible prefix removes exactly the settled entry and
	// leaves the sibling row intact.
	if _, _, err := runCLIWithDeps(t, deps, "", "dlq", "rm", settled.ID[:10]); err != nil {
		t.Fatalf("dlq rm via prefix: %v", err)
	}
	if _, ok, _ := st.GetDLQEntry(ctx, settled.ID); ok {
		t.Fatal("rm must delete exactly the settled entry")
	}
	if _, ok, _ := st.GetDLQEntry(ctx, unsettled.ID); !ok {
		t.Fatal("rm must leave the unsettled sibling entry in place")
	}
}

// integrationDLQHexID builds a deterministic full-length lowercase hex entry
// ID from hex fragments (zero-padded to 64), letting the integration tests
// craft identities with a controlled leading prefix — the characters `dlq ls`
// shows and the prefix inputs match — against the real primary-key range.
func integrationDLQHexID(parts ...string) string {
	id := strings.Join(parts, "")
	if len(id) >= 64 {
		return id
	}
	return id + strings.Repeat("0", 64-len(id))
}

// TestIntegrationDLQReplayAndAmbiguityViaPrefix proves, through the REAL
// PostgreSQL store seam and the real worker socket, that `dlq replay` accepts
// the unique ls-visible prefix (replaying the exact recorded handler once and
// deleting the entry), while an AMBIGUOUS prefix is refused by both replay and
// rm before any claim or delete can be taken — neither entry ever executes or
// disappears, and the resolved row stays claimable afterwards. No broker is
// configured, per the command family's contract.
func TestIntegrationDLQReplayAndAmbiguityViaPrefix(t *testing.T) {
	testutil.RequireStateDatabase(t)
	dsn := testutil.NewStateDSN(t)
	ctx := context.Background()

	st := testutil.OpenState(t, dsn)
	seed := func(entry dlq.Entry) {
		t.Helper()
		if inserted, err := st.SaveDLQEntry(ctx, entry); err != nil || !inserted {
			t.Fatalf("seed entry %s = (%v,%v)", entry.ID, inserted, err)
		}
		if err := st.MarkDLQDeliverySettled(ctx, entry.DeliveryID, time.Now()); err != nil {
			t.Fatalf("settle delivery %s: %v", entry.DeliveryID, err)
		}
	}
	now := time.Now().UTC().Format(time.RFC3339)
	replayable := dlq.Entry{
		ID:              dlq.NewIDForDelivery(dlq.NewDeliveryID("events-test", "relay-test", "1769199999995-0"), "user-events/events.created.handler"),
		Source:          "events-test",
		SourceGroup:     "relay-test",
		SourceMessageID: "1769199999995-0",
		DeliveryID:      dlq.NewDeliveryID("events-test", "relay-test", "1769199999995-0"),
		Consumer:        "worker-1",
		Event:           `{"event_name":"INSERT"}`,
		ReplayPayload:   []byte(`{"event_name":"INSERT"}`),
		Reason:          "invocation exhausted",
		App:             "user-events",
		Handler:         "events.created.handler",
		HandlerAttempts: 2,
		Timestamp:       now,
	}
	seed(replayable)

	// Two settled entries deliberately share a visible prefix, which no
	// legitimate operator input can resolve.
	const sharedPrefix = "7f00cafe01"
	sharedOne := integrationDLQHexID(sharedPrefix, "1")
	sharedTwo := integrationDLQHexID(sharedPrefix, "2")
	for i, id := range []string{sharedOne, sharedTwo} {
		seed(dlq.Entry{
			ID:              id,
			Source:          "events-test",
			SourceGroup:     "relay-test",
			SourceMessageID: fmt.Sprintf("1769199999996-%d", i),
			DeliveryID:      dlq.NewDeliveryID("events-test", "relay-test", fmt.Sprintf("1769199999996-%d", i)),
			Consumer:        "worker-1",
			Event:           `{"a":1}`,
			Reason:          "invocation exhausted",
			App:             "alpha",
			Handler:         fmt.Sprintf("events.%c.handler", 'a'+i),
			HandlerAttempts: 2,
			Timestamp:       now,
		})
	}

	exec := &dlqReplayExecutor{}
	deps := integrationReplayDeps(t, dsn, exec)

	// Guard the fixture: the deliberate shared pair must not also collide
	// with the replayable entry's visible prefix.
	if replayable.ID[:10] == sharedPrefix {
		t.Fatalf("fixture drift: the replayable entry shares the deliberate shared prefix %q", sharedPrefix)
	}

	// A unique replay via the ls-visible prefix: exact handler, once, and the
	// row is gone.
	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", replayable.ID[:10])
	if err != nil {
		t.Fatalf("dlq replay via prefix: %v", err)
	}
	if strings.TrimSpace(out) != "Replayed handler successfully" {
		t.Fatalf("stdout = %q", out)
	}
	if handlers, payloads := exec.got(); len(handlers) != 1 || handlers[0] != "events.created.handler" {
		t.Fatalf("executed handlers = %v, want exactly the recorded handler", handlers)
	} else if string(payloads[0]) != `{"event_name":"INSERT"}` {
		t.Fatalf("executed payload = %q, want the stored replay payload", payloads[0])
	}
	if _, ok, err := st.GetDLQEntry(ctx, replayable.ID); err != nil || ok {
		t.Fatalf("the successful prefix replay must delete its row: (ok=%v, err=%v)", ok, err)
	}

	// The shared prefix is refused by replay AND rm, naming both candidates;
	// nothing executes and nothing is deleted — neither row can be taken.
	for _, cmd := range []string{"replay", "rm"} {
		_, _, err = runCLIWithDeps(t, deps, "", "dlq", cmd, sharedPrefix)
		if err == nil || !strings.Contains(err.Error(), "ambiguous DLQ entry ID prefix") ||
			!strings.Contains(err.Error(), sharedOne) || !strings.Contains(err.Error(), sharedTwo) {
			t.Fatalf("dlq %s via shared prefix err = %v, want the ambiguity refusal", cmd, err)
		}
		for _, id := range []string{sharedOne, sharedTwo} {
			if _, ok, err := st.GetDLQEntry(ctx, id); err != nil || !ok {
				t.Fatalf("the ambiguous %s attempt must leave %s in place: (ok=%v, err=%v)", cmd, id, ok, err)
			}
		}
	}

	// Prove the ambiguous replay attempts never claimed: the row admits a
	// fresh claim immediately afterwards (released again right away so the
	// schema cleanup does not race a lingering lease).
	claim, err := st.ClaimDLQReplay(ctx, sharedOne, dlq.ReplayLease)
	if err != nil {
		t.Fatalf("the ambiguous prefix must never claim: %v", err)
	}
	if err := st.ReleaseDLQReplay(ctx, sharedOne, claim.Token); err != nil {
		t.Fatalf("release the probe claim: %v", err)
	}
}

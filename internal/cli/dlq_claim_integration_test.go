//go:build integration

// This file pins the CLI replay-claim contract against the REAL production
// store seam (openPostgresDLQStore -> state.State) and the real worker socket:
// two concurrent `relay dlq replay` invocations of one settled entry race the
// PostgreSQL claim committed before the worker dial, so exactly one admits and
// executes and the other is refused without ever reaching the handler.
package cli

import (
	"context"
	"strings"
	"testing"
	"time"

	"relay/internal/dlq"
	"relay/internal/runner"
	"relay/internal/testutil"
)

// integrationReplayDeps wires the full production replay path: the real
// PostgreSQL DLQ opener (DATABASE_URL), the real socket server, and the real
// runner over a caller-supplied executor. dlqReplayRunnerOn provides the
// socket/runner wiring; the tests replace the fake opener with the production
// one.
func integrationReplayDeps(t *testing.T, dsn string, exec runner.Executor) Dependencies {
	t.Helper()
	deps := dlqReplayRunnerOn(t, testDeps(t), exec)
	t.Setenv("DATABASE_URL", dsn)
	deps.OpenDLQ = openPostgresDLQStore
	return deps
}

// TestIntegrationDLQReplayConcurrentClaimsRejectSecondClaimant proves
// concurrent CLI admission through the production store: while the first CLI's
// replay executes on the real socket (a handler blocked mid-flight), a second
// CLI of the same entry is refused by the PostgreSQL lease (committed before
// any dial) with the explicit in-progress error, and the handler executes
// exactly once. The first replay then completes and deletes the settled row.
func TestIntegrationDLQReplayConcurrentClaimsRejectSecondClaimant(t *testing.T) {
	testutil.RequireStateDatabase(t)
	dsn := testutil.NewStateDSN(t)
	ctx := context.Background()

	// Seed the settled, replayable entry exactly as the production store does.
	st := testutil.OpenState(t, dsn)
	deliveryID := dlq.NewDeliveryID("events-test", "relay-test", "1769199999992-0")
	entry := dlq.Entry{
		ID:              dlq.NewIDForDelivery(deliveryID, "user-events/events.created.handler"),
		Source:          "events-test",
		SourceGroup:     "relay-test",
		SourceMessageID: "1769199999992-0",
		DeliveryID:      deliveryID,
		Consumer:        "worker-1",
		Event:           `{"event_name":"INSERT","id":7}`,
		Reason:          "invocation exhausted",
		App:             "user-events",
		Handler:         "events.created.handler",
		Deliveries:      9,
		HandlerAttempts: 5,
		Timestamp:       time.Now().UTC().Format(time.RFC3339),
	}
	if inserted, err := st.SaveDLQEntry(ctx, entry); err != nil || !inserted {
		t.Fatalf("seed settled entry = (%v,%v)", inserted, err)
	}
	if err := st.MarkDLQDeliverySettled(ctx, deliveryID, time.Now()); err != nil {
		t.Fatalf("settle seeded delivery: %v", err)
	}

	exec := newBlockingReplayExecutor()
	deps := integrationReplayDeps(t, dsn, exec)
	t.Cleanup(exec.Release)

	// Given: the first CLI admitted its claim and its handler is executing.
	firstDone := make(chan error, 1)
	firstOut := make(chan string, 1)
	go func() {
		out, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", entry.ID)
		firstOut <- out
		firstDone <- err
	}()
	exec.waitEntered(t)

	// When: a second CLI replays the same entry concurrently.
	_, _, secondErr := runCLIWithDeps(t, deps, "", "dlq", "replay", entry.ID)

	// Then: the PostgreSQL claim refuses it before the worker is dialed.
	if secondErr == nil || !strings.Contains(secondErr.Error(), "replay is already in progress") {
		t.Fatalf("second replay err = %v, want the replay-in-progress refusal", secondErr)
	}
	if got := exec.count(); got != 1 {
		t.Fatalf("executed handlers = %d, want exactly 1 (the refused claimant never executed)", got)
	}

	// Finally: the first replay completes and its fenced delete removes the row.
	exec.Release()
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first replay err = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the first replay never finished after release")
	}
	if out := <-firstOut; strings.TrimSpace(out) != "Replayed handler successfully" {
		t.Fatalf("first replay stdout = %q", out)
	}
	if _, ok, err := st.GetDLQEntry(ctx, entry.ID); err != nil || ok {
		t.Fatalf("the successful replay must delete its row: (ok=%v, err=%v)", ok, err)
	}
}

// TestIntegrationDLQReplayClaimLeaseExpiresPermitsRetake proves the CLI-level
// recovery path through the production store: a claim whose lease has expired
// (a crashed claimant simulated by a short test lease written directly to the
// store) no longer blocks the next `relay dlq replay`, which is admitted with a
// newer fencing token and completes.
func TestIntegrationDLQReplayClaimLeaseExpiresPermitsRetake(t *testing.T) {
	testutil.RequireStateDatabase(t)
	dsn := testutil.NewStateDSN(t)
	ctx := context.Background()

	st := testutil.OpenState(t, dsn)
	deliveryID := dlq.NewDeliveryID("events-test", "relay-test", "1769199999993-0")
	entry := dlq.Entry{
		ID:              dlq.NewIDForDelivery(deliveryID, "user-events/events.created.handler"),
		Source:          "events-test",
		SourceGroup:     "relay-test",
		SourceMessageID: "1769199999993-0",
		DeliveryID:      deliveryID,
		Consumer:        "worker-1",
		Event:           `{"event_name":"INSERT"}`,
		Reason:          "invocation exhausted",
		App:             "user-events",
		Handler:         "events.created.handler",
		HandlerAttempts: 2,
		Timestamp:       time.Now().UTC().Format(time.RFC3339),
	}
	if inserted, err := st.SaveDLQEntry(ctx, entry); err != nil || !inserted {
		t.Fatalf("seed settled entry = (%v,%v)", inserted, err)
	}
	if err := st.MarkDLQDeliverySettled(ctx, deliveryID, time.Now()); err != nil {
		t.Fatalf("settle seeded delivery: %v", err)
	}

	// Given: a crashed claimant's claim whose (short) lease already lapsed.
	claim, err := st.ClaimDLQReplay(ctx, entry.ID, 40*time.Millisecond)
	if err != nil {
		t.Fatalf("seed claim: %v", err)
	}
	rest := time.Until(claim.LeaseUntil)
	if rest > 0 {
		time.Sleep(rest + 20*time.Millisecond)
	}

	// Then: the CLI replay is admitted again and completes end to end.
	exec := &dlqReplayExecutor{}
	deps := integrationReplayDeps(t, dsn, exec)

	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "replay", entry.ID)
	if err != nil {
		t.Fatalf("replay after lease expiry: %v", err)
	}
	if strings.TrimSpace(out) != "Replayed handler successfully" {
		t.Fatalf("stdout = %q", out)
	}
	if handlers, _ := exec.got(); len(handlers) != 1 {
		t.Fatalf("executed handlers = %v, want exactly one", handlers)
	}
	if _, ok, err := st.GetDLQEntry(ctx, entry.ID); err != nil || ok {
		t.Fatalf("the successful replay must delete its row: (ok=%v, err=%v)", ok, err)
	}
}

// TestIntegrationDLQReplayRmRefusesEntryWithActiveClaim proves through the
// production command path AND the real store that `relay dlq rm` refuses an
// entry with an active replay claim and becomes removable after the claim is
// released (rm must never delete a row out from under a running replay).
func TestIntegrationDLQReplayRmRefusesEntryWithActiveClaim(t *testing.T) {
	testutil.RequireStateDatabase(t)
	dsn := testutil.NewStateDSN(t)
	ctx := context.Background()

	st := testutil.OpenState(t, dsn)
	deliveryID := dlq.NewDeliveryID("events-test", "relay-test", "1769199999994-0")
	entry := dlq.Entry{
		ID:              dlq.NewIDForDelivery(deliveryID, "user-events/events.created.handler"),
		Source:          "events-test",
		SourceGroup:     "relay-test",
		SourceMessageID: "1769199999994-0",
		DeliveryID:      deliveryID,
		Consumer:        "worker-1",
		Event:           `{"event_name":"INSERT"}`,
		Reason:          "invocation exhausted",
		App:             "user-events",
		Handler:         "events.created.handler",
		HandlerAttempts: 2,
		Timestamp:       time.Now().UTC().Format(time.RFC3339),
	}
	if inserted, err := st.SaveDLQEntry(ctx, entry); err != nil || !inserted {
		t.Fatalf("seed settled entry = (%v,%v)", inserted, err)
	}
	if err := st.MarkDLQDeliverySettled(ctx, deliveryID, time.Now()); err != nil {
		t.Fatalf("settle seeded delivery: %v", err)
	}

	// The production opener resolves DATABASE_URL, exactly as the production
	// commands do (rm needs no worker socket).
	t.Setenv("DATABASE_URL", dsn)
	deps := testDeps(t)
	deps.OpenDLQ = openPostgresDLQStore

	// Given: a concurrent replay holds the entry's claim.
	claim, err := st.ClaimDLQReplay(ctx, entry.ID, dlq.ReplayLease)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	if _, _, err := runCLIWithDeps(t, deps, "", "dlq", "rm", entry.ID); err == nil ||
		!strings.Contains(err.Error(), "has an active replay claim") {
		t.Fatalf("rm err = %v, want the active-claim refusal", err)
	}
	if _, ok, _ := st.GetDLQEntry(ctx, entry.ID); !ok {
		t.Fatal("the actively claimed entry must survive the refused rm")
	}

	// When the claim is released, the entry is removable again.
	if err := st.ReleaseDLQReplay(ctx, entry.ID, claim.Token); err != nil {
		t.Fatalf("release: %v", err)
	}
	out, _, err := runCLIWithDeps(t, deps, "", "dlq", "rm", entry.ID)
	if err != nil || strings.TrimSpace(out) != "DLQ entry removed" {
		t.Fatalf("rm after release = (%q,%v), want removal", out, err)
	}
	if _, ok, _ := st.GetDLQEntry(ctx, entry.ID); ok {
		t.Fatal("the released entry must be removable")
	}
}

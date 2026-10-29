//go:build integration

// This file pins the DLQ retention loop against real PostgreSQL through the
// production dlqPruner seam: only SETTLED rows at or before the cutoff are
// removed, in bounded batches, and an unsettled row survives every prune no
// matter how old it is (its delivery may still be recoverable in the source
// broker). The state-level coverage of PruneDLQEntries lives in the state
// package; this file pins the loop wiring and the settled/unsettled boundary
// through the worker's own seam.
package worker

import (
	"context"
	"testing"
	"time"

	"relay/internal/dlq"
	"relay/internal/observability/metrics"
	"relay/internal/state"
	"relay/internal/testutil"
)

// seedDLQRow writes one dead-letter row directly through the PostgreSQL store,
// settling it first when the fixture wants it prune-eligible.
func seedDLQRow(t *testing.T, st *state.State, entry dlq.Entry, settledAt time.Time) {
	t.Helper()
	if _, err := st.SaveDLQEntry(context.Background(), entry); err != nil {
		t.Fatalf("seed %s: %v", entry.ID, err)
	}
	if !settledAt.IsZero() {
		if err := st.MarkDLQDeliverySettled(context.Background(), entry.DeliveryID, settledAt); err != nil {
			t.Fatalf("settle %s: %v", entry.ID, err)
		}
	}
}

// pruneFixture builds three rows: a settled row older than the window
// (removable), a settled row inside the window (removable later), and an
// unsettled row older than BOTH (never removable).
func pruneFixture(t *testing.T, st *state.State) (oldSettled, recentSettled, unsettled dlq.Entry) {
	t.Helper()
	old := time.Now().Add(-3 * time.Hour)
	recent := time.Now().Add(-time.Minute)
	older := time.Now().Add(-24 * time.Hour)

	oldSettled = dlq.Entry{
		ID:     dlq.NewIDForDelivery(dlq.NewDeliveryID("events-test", "relay-test", "1-0"), "fn/h"),
		Source: "events-test", SourceGroup: "relay-test", SourceMessageID: "1-0",
		DeliveryID: dlq.NewDeliveryID("events-test", "relay-test", "1-0"),
		Event:      `{"a":1}`, Reason: "boom", App: "fn", Handler: "h",
		Timestamp: old.UTC().Format(time.RFC3339),
	}
	recentSettled = dlq.Entry{
		ID:     dlq.NewIDForDelivery(dlq.NewDeliveryID("events-test", "relay-test", "2-0"), "fn/h"),
		Source: "events-test", SourceGroup: "relay-test", SourceMessageID: "2-0",
		DeliveryID: dlq.NewDeliveryID("events-test", "relay-test", "2-0"),
		Event:      `{"a":2}`, Reason: "boom", App: "fn", Handler: "h",
		Timestamp: recent.UTC().Format(time.RFC3339),
	}
	unsettled = dlq.Entry{
		ID:     dlq.NewIDForDelivery(dlq.NewDeliveryID("events-test", "relay-test", "3-0"), "fn/h"),
		Source: "events-test", SourceGroup: "relay-test", SourceMessageID: "3-0",
		DeliveryID: dlq.NewDeliveryID("events-test", "relay-test", "3-0"),
		Event:      `{"a":3}`, Reason: "boom", App: "fn", Handler: "h",
		// The unsettled delivery is genuinely recoverable: still pending in the
		// group. SettledAt must stay empty for retention to skip it.
		Timestamp: older.UTC().Format(time.RFC3339),
	}
	seedDLQRow(t, st, oldSettled, old)
	seedDLQRow(t, st, recentSettled, recent)
	seedDLQRow(t, st, unsettled, time.Time{})
	return oldSettled, recentSettled, unsettled
}

func TestIntegrationDLQRetentionTickPrunesSettledPreservesUnsettled(t *testing.T) {
	testutil.RequireStateDatabase(t)
	st := testutil.NewState(t)
	oldSettled, recentSettled, unsettled := pruneFixture(t, st)
	ctx := context.Background()

	// One hour window: only the old settled row is at or before the cutoff.
	// `now` is injected so the cutoff is deterministic.
	now := time.Now()
	dlqRetentionTick(ctx, st, time.Hour, func() time.Time { return now }, testutil.DiscardLogger())

	if _, ok, _ := st.GetDLQEntry(ctx, oldSettled.ID); ok {
		t.Fatal("the settled row past its window must be pruned")
	}
	if _, ok, _ := st.GetDLQEntry(ctx, recentSettled.ID); !ok {
		t.Fatal("the settled row inside the window must be retained")
	}
	if _, ok, _ := st.GetDLQEntry(ctx, unsettled.ID); !ok {
		t.Fatal("an unsettled row is never removed by retention")
	}

	// A later tick with the retention long past still preserves the unsettled
	// row — and prunes the fresh settled one as it ages out.
	later := now.Add(2 * time.Hour)
	dlqRetentionTick(ctx, st, time.Hour, func() time.Time { return later }, testutil.DiscardLogger())
	if _, ok, _ := st.GetDLQEntry(ctx, recentSettled.ID); ok {
		t.Fatal("the settled row must prune once it crosses the window")
	}
	if _, ok, _ := st.GetDLQEntry(ctx, unsettled.ID); !ok {
		t.Fatal("an unsettled row far past any window is still never removed")
	}
}

// TestIntegrationDLQRetentionTickSkipsRowWithActiveReplayClaim proves the
// worker's retention loop cannot remove a row under an in-flight replay: an
// actively claimed row is skipped even when its lease window and retention
// window are both long past, and it becomes prunable as soon as its claim is
// released (the same protection the state store enforces directly).
func TestIntegrationDLQRetentionTickSkipsRowWithActiveReplayClaim(t *testing.T) {
	testutil.RequireStateDatabase(t)
	st := testutil.NewState(t)
	oldSettled, recentSettled, unsettled := pruneFixture(t, st)
	ctx := context.Background()

	// Given: a replay claim is actively held on the settled old row (exactly
	// what `relay dlq replay` leaves behind while its handler runs).
	claim, err := st.ClaimDLQReplay(ctx, oldSettled.ID, dlq.ReplayLease)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claim.Token <= 0 {
		t.Fatalf("claim token = %d, want the positive fencing token", claim.Token)
	}

	// When retention long past every row's window runs...
	later := time.Now().Add(2 * time.Hour)
	dlqRetentionTick(ctx, st, time.Hour, func() time.Time { return later }, testutil.DiscardLogger())

	// ...the unclaimed settled row is pruned, and ONLY the active replay claim
	// is what preserves the claimed row (both would otherwise be prunable).
	if _, ok, _ := st.GetDLQEntry(ctx, recentSettled.ID); ok {
		t.Fatal("the unclaimed settled row past its window must be pruned")
	}
	if _, ok, _ := st.GetDLQEntry(ctx, oldSettled.ID); !ok {
		t.Fatal("the actively claimed row must survive retention")
	}
	if _, ok, _ := st.GetDLQEntry(ctx, unsettled.ID); !ok {
		t.Fatal("an unsettled row is never removed by retention")
	}

	// Once the claim is released (definitive-failure recovery), the row returns
	// to retention's disposal: the same tick prunes it now.
	if err := st.ReleaseDLQReplay(ctx, oldSettled.ID, claim.Token); err != nil {
		t.Fatalf("release: %v", err)
	}
	dlqRetentionTick(ctx, st, time.Hour, func() time.Time { return later }, testutil.DiscardLogger())
	if _, ok, _ := st.GetDLQEntry(ctx, oldSettled.ID); ok {
		t.Fatal("the released row must prune once its claim is gone")
	}
}

// TestIntegrationDLQDepthGaugeReportsApproximateRowCount pins the periodic
// depth gauge against real PostgreSQL: Refresh records the planner's approximate
// row estimate (pg_class.reltuples), never an exact COUNT, and an unreachable
// table skips the tick instead of failing or zeroing.
func TestIntegrationDLQDepthGaugeReportsApproximateRowCount(t *testing.T) {
	testutil.RequireStateDatabase(t)
	st := testutil.NewState(t)
	m := metrics.New()
	g := &dlqDepthGauge{st: st, metrics: m, log: testutil.DiscardLogger()}

	// Empty table: the estimate is 0.
	g.Refresh(context.Background())
	if got := m.Gauge(metrics.MetricDLQDepth); got != float64(0) {
		t.Fatalf("gauge = %v on an empty table, want 0", got)
	}

	_, _, _ = pruneFixture(t, st)
	estimate, err := st.ApproxDLQDepth(context.Background())
	if err != nil {
		t.Fatalf("approx depth: %v", err)
	}
	g.Refresh(context.Background())
	if got := m.Gauge(metrics.MetricDLQDepth); got != float64(estimate) {
		t.Fatalf("gauge = %v, want the planner estimate %v", got, estimate)
	}

	// A read failure (unreachable table) must skip the tick and keep the last
	// observation rather than zeroing.
	if err := st.Close(); err != nil {
		t.Fatalf("close state handle: %v", err)
	}
	prior := m.Gauge(metrics.MetricDLQDepth)
	g.Refresh(context.Background())
	if got := m.Gauge(metrics.MetricDLQDepth); got != prior {
		t.Fatalf("gauge after a failed read = %v, want the previous %v (never zeroed)", got, prior)
	}
}

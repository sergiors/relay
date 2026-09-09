package state

import (
	"context"
	"testing"
	"time"
)

// TestRecordStatsSnapshotSingleTransaction pins the RecordStatsSnapshot contract:
// a single short transaction that (1) prunes orphaned function_stats rows (rows
// whose function no longer has a functions row), (2) upserts the global stats
// row with absolute values, and (3) per-function upserts guarded by the function
// still existing. It also proves idempotency: a repeated flush with identical
// values never double-counts.
func TestRecordStatsSnapshotSingleTransaction(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)

	// Seed functions rows for "alpha" and "beta". Record "gone" and its
	// function_stats, then delete ONLY its functions row directly via the
	// unexported db handle, leaving a genuine orphaned function_stats row behind.
	c.RecordDiscovered(fnFor(t, "alpha", tmpl))
	c.RecordDiscovered(fnFor(t, "beta", tmpl))
	c.RecordDiscovered(fnFor(t, "gone", tmpl))
	c.RecordFunctionStats(FunctionStats{Function: "gone", EventsProcessedTotal: 5})
	if _, err := c.db.ExecContext(context.Background(), `DELETE FROM functions WHERE name = 'gone'`); err != nil {
		t.Fatalf("delete gone functions row: %v", err)
	}
	// A prior global row to prove the snapshot replaces (not accumulates) it.
	c.RecordStats(Stats{EventsProcessedTotal: 100})

	s := Stats{
		EventsProcessedTotal:    200,
		HandlerSuccessTotal:     190,
		HandlerFailureTotal:     10,
		RetryTotal:              3,
		DLQTotal:                1,
		PendingEntries:          7,
		OldestPendingAgeSeconds: 42,
	}
	fns := []FunctionStats{
		{Function: "alpha", EventsProcessedTotal: 30, HandlerSuccessTotal: 25, HandlerFailureTotal: 5, RetryTotal: 1, DLQTotal: 0},
		{Function: "beta", EventsProcessedTotal: 40, HandlerSuccessTotal: 35, HandlerFailureTotal: 5, RetryTotal: 2, DLQTotal: 1},
		// "gone"'s stale row is present in the snapshot but its functions row was
		// deleted, so it must be pruned and NOT re-created.
		{Function: "gone", EventsProcessedTotal: 5, HandlerSuccessTotal: 5},
	}

	if err := c.RecordStatsSnapshot(context.Background(), s, fns); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Global row holds the absolute values.
	gs, ok := c.Stats()
	if !ok {
		t.Fatal("expected global stats row after snapshot")
	}
	s.UpdatedAt = gs.UpdatedAt
	if gs != s {
		t.Fatalf("global stats = %+v, want %+v", gs, s)
	}

	// alpha/beta upserted with the snapshot values.
	a, ok := c.FunctionStats("alpha")
	if !ok || a.EventsProcessedTotal != 30 || a.HandlerSuccessTotal != 25 {
		t.Fatalf("alpha = %+v, ok=%v; want events 30 success 25", a, ok)
	}
	b, ok := c.FunctionStats("beta")
	if !ok || b.EventsProcessedTotal != 40 || b.RetryTotal != 2 {
		t.Fatalf("beta = %+v, ok=%v; want events 40 retries 2", b, ok)
	}

	// "gone" orphan pruned and not re-created despite being present in fns.
	if _, ok := c.FunctionStats("gone"); ok {
		t.Fatal("orphan 'gone' function_stats must be pruned")
	}
	if _, ok := c.GetFunction("gone"); ok {
		t.Fatal("gone functions row must not be re-created")
	}

	// A repeated flush with identical values: totals unchanged, updated_at
	// refreshed (never goes backwards).
	prevUpdated := gs.UpdatedAt
	if err := c.RecordStatsSnapshot(context.Background(), s, fns); err != nil {
		t.Fatalf("second snapshot: %v", err)
	}
	gs, ok = c.Stats()
	if !ok {
		t.Fatal("expected global stats row after second snapshot")
	}
	if gs.EventsProcessedTotal != 200 {
		t.Fatalf("events after second snapshot = %d, want 200 (no double counting)", gs.EventsProcessedTotal)
	}
	prevT, _ := time.Parse(time.RFC3339, prevUpdated)
	curT, _ := time.Parse(time.RFC3339, gs.UpdatedAt)
	if curT.Before(prevT) {
		t.Fatalf("updated_at went backwards: %q -> %q", prevUpdated, gs.UpdatedAt)
	}
}

// TestRecordStatsSnapshotConditionalUpsertGuardsRemovedFunction verifies the
// per-function upsert guard works for a function that has NO functions row at
// all (never discovered): the flush must not create its function_stats row.
func TestRecordStatsSnapshotConditionalUpsertGuardsRemovedFunction(t *testing.T) {
	c := openTestState(t)
	c.RecordDiscovered(fnFor(t, "alpha", mustTemplate(t, twoHandlerTmpl)))

	fns := []FunctionStats{
		{Function: "alpha", EventsProcessedTotal: 10},
		{Function: "ghost", EventsProcessedTotal: 99}, // never discovered, no functions row
	}
	if err := c.RecordStatsSnapshot(context.Background(), Stats{EventsProcessedTotal: 10}, fns); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	a, ok := c.FunctionStats("alpha")
	if !ok || a.EventsProcessedTotal != 10 {
		t.Fatalf("alpha = %+v, ok=%v; want events 10", a, ok)
	}
	if _, ok := c.FunctionStats("ghost"); ok {
		t.Fatal("ghost must not get a function_stats row (no functions row)")
	}
	if _, ok := c.GetFunction("ghost"); ok {
		t.Fatal("ghost must not get a functions row")
	}
}

// TestRecordStatsSnapshotFailureRetriesNextFlush simulates a failed flush and
// proves recovery: the failed flush must leave persisted values untouched, and a
// later successful flush must persist the accumulated (absolute) values.
func TestRecordStatsSnapshotFailureRetriesNextFlush(t *testing.T) {
	c := openTestState(t)
	c.RecordDiscovered(fnFor(t, "alpha", mustTemplate(t, twoHandlerTmpl)))

	prev := Stats{EventsProcessedTotal: 50, HandlerSuccessTotal: 40, HandlerFailureTotal: 10, RetryTotal: 2, DLQTotal: 1}
	c.RecordStats(prev)

	// Force a failure WITHOUT deleting the DB: a cancelled ctx makes BeginTx (and,
	// if it somehow succeeded, every bound Exec) fail, so nothing commits.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fns := []FunctionStats{{Function: "alpha", EventsProcessedTotal: 5}}
	if err := c.RecordStatsSnapshot(ctx, Stats{EventsProcessedTotal: 500, HandlerSuccessTotal: 300}, fns); err == nil {
		t.Fatal("expected error on a cancelled-context snapshot")
	}

	// Failed flush must not erase or reset persisted values.
	gs, ok := c.Stats()
	if !ok {
		t.Fatal("expected global stats row after failed flush")
	}
	if gs.EventsProcessedTotal != 50 {
		t.Fatalf("events after failed flush = %d, want 50 preserved", gs.EventsProcessedTotal)
	}
	if gs.HandlerSuccessTotal != 40 {
		t.Fatalf("success after failed flush = %d, want 40 preserved", gs.HandlerSuccessTotal)
	}
	// Nothing from the failed flush was committed, so alpha has no function_stats.
	if _, ok := c.FunctionStats("alpha"); ok {
		t.Fatal("failed flush must not write function_stats")
	}

	// A later successful flush with a live ctx persists the accumulated values.
	want := Stats{EventsProcessedTotal: 500, HandlerSuccessTotal: 300, HandlerFailureTotal: 150, RetryTotal: 4, DLQTotal: 1}
	if err := c.RecordStatsSnapshot(context.Background(), want, fns); err != nil {
		t.Fatalf("recovery snapshot: %v", err)
	}
	gs, ok = c.Stats()
	if !ok {
		t.Fatal("expected global stats row after recovery snapshot")
	}
	want.UpdatedAt = gs.UpdatedAt
	if gs != want {
		t.Fatalf("global stats after recovery = %+v, want %+v", gs, want)
	}
	a, ok := c.FunctionStats("alpha")
	if !ok || a.EventsProcessedTotal != 5 {
		t.Fatalf("alpha after recovery = %+v, ok=%v; want events 5", a, ok)
	}
}

// TestRecordStatsSnapshotAbsoluteNotDelta pins the absolute-snapshot semantics:
// each flush writes the caller's CURRENT cumulative values, never a delta, so a
// repeated flush with no new activity leaves totals unchanged.
func TestRecordStatsSnapshotAbsoluteNotDelta(t *testing.T) {
	c := openTestState(t)
	ctx := context.Background()

	if err := c.RecordStatsSnapshot(ctx, Stats{EventsProcessedTotal: 100}, nil); err != nil {
		t.Fatalf("first snapshot: %v", err)
	}
	gs, ok := c.Stats()
	if !ok || gs.EventsProcessedTotal != 100 {
		t.Fatalf("events = %d, ok=%v; want 100", gs.EventsProcessedTotal, ok)
	}

	// No new activity, identical value: still 100, not 200.
	if err := c.RecordStatsSnapshot(ctx, Stats{EventsProcessedTotal: 100}, nil); err != nil {
		t.Fatalf("second snapshot: %v", err)
	}
	gs, _ = c.Stats()
	if gs.EventsProcessedTotal != 100 {
		t.Fatalf("events after repeat = %d, want 100 (absolute, not delta)", gs.EventsProcessedTotal)
	}

	// New activity raises the absolute total to 150.
	if err := c.RecordStatsSnapshot(ctx, Stats{EventsProcessedTotal: 150}, nil); err != nil {
		t.Fatalf("third snapshot: %v", err)
	}
	gs, _ = c.Stats()
	if gs.EventsProcessedTotal != 150 {
		t.Fatalf("events after bump = %d, want 150", gs.EventsProcessedTotal)
	}
}

// TestRecordStatsContextHonorsContext verifies RecordStatsContext writes nothing
// on a cancelled context and writes on a background context.
func TestRecordStatsContextHonorsContext(t *testing.T) {
	c := openTestState(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.RecordStatsContext(ctx, Stats{EventsProcessedTotal: 100})
	if _, ok := c.Stats(); ok {
		t.Fatal("cancelled-context RecordStatsContext must not write a stats row")
	}

	c.RecordStatsContext(context.Background(), Stats{EventsProcessedTotal: 100})
	gs, ok := c.Stats()
	if !ok {
		t.Fatal("expected stats row after background-context write")
	}
	if gs.EventsProcessedTotal != 100 {
		t.Fatalf("events = %d, want 100", gs.EventsProcessedTotal)
	}
}

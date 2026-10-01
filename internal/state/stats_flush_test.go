package state

import (
	"context"
	"testing"
	"time"
)

// TestRecordStatsSnapshotSingleTransaction pins the RecordStatsSnapshot contract:
// a single short transaction that (1) prunes orphaned app_stats rows (rows
// whose app no longer has a apps row), (2) upserts the global stats
// row with absolute values, and (3) per-app upserts guarded by the app
// still existing. It also proves idempotency: a repeated flush with identical
// values never double-counts.
func TestRecordStatsSnapshotSingleTransaction(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)

	// Seed apps rows for "alpha" and "beta". Record "gone" and its
	// app_stats, then delete ONLY its apps row directly via the
	// unexported db handle, leaving a genuine orphaned app_stats row behind.
	c.RecordDiscovered(fnFor(t, "alpha", tmpl))
	c.RecordDiscovered(fnFor(t, "beta", tmpl))
	c.RecordDiscovered(fnFor(t, "gone", tmpl))
	c.RecordAppStats(AppStats{App: "gone", EventsMatchedTotal: 5})
	if _, err := c.db.ExecContext(context.Background(), `DELETE FROM apps WHERE name = 'gone'`); err != nil {
		t.Fatalf("delete gone apps row: %v", err)
	}
	// A prior global row to prove the snapshot replaces (not accumulates) it.
	c.RecordStats(Stats{EventsMatchedTotal: 100})

	s := Stats{
		EventsMatchedTotal:      200,
		HandlerSuccessTotal:     190,
		HandlerFailureTotal:     10,
		RetryTotal:              3,
		DLQTotal:                1,
		PendingEntries:          7,
		OldestPendingAgeSeconds: 42,
	}
	fns := []AppStats{
		{App: "alpha", EventsMatchedTotal: 30, HandlerSuccessTotal: 25, HandlerFailureTotal: 5, RetryTotal: 1, DLQTotal: 0},
		{App: "beta", EventsMatchedTotal: 40, HandlerSuccessTotal: 35, HandlerFailureTotal: 5, RetryTotal: 2, DLQTotal: 1},
		// "gone"'s stale row is present in the snapshot but its apps row was
		// deleted, so it must be pruned and NOT re-created.
		{App: "gone", EventsMatchedTotal: 5, HandlerSuccessTotal: 5},
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
	a, ok := c.AppStats("alpha")
	if !ok || a.EventsMatchedTotal != 30 || a.HandlerSuccessTotal != 25 {
		t.Fatalf("alpha = %+v, ok=%v; want events 30 success 25", a, ok)
	}
	b, ok := c.AppStats("beta")
	if !ok || b.EventsMatchedTotal != 40 || b.RetryTotal != 2 {
		t.Fatalf("beta = %+v, ok=%v; want events 40 retries 2", b, ok)
	}

	// "gone" orphan pruned and not re-created despite being present in fns.
	if _, ok := c.AppStats("gone"); ok {
		t.Fatal("orphan 'gone' app_stats must be pruned")
	}
	if _, ok := c.GetApp("gone"); ok {
		t.Fatal("gone apps row must not be re-created")
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
	if gs.EventsMatchedTotal != 200 {
		t.Fatalf("events after second snapshot = %d, want 200 (no double counting)", gs.EventsMatchedTotal)
	}
	prevT, _ := time.Parse(time.RFC3339, prevUpdated)
	curT, _ := time.Parse(time.RFC3339, gs.UpdatedAt)
	if curT.Before(prevT) {
		t.Fatalf("updated_at went backwards: %q -> %q", prevUpdated, gs.UpdatedAt)
	}
}

// TestRecordStatsSnapshotConditionalUpsertGuardsRemovedApp verifies the
// per-app upsert guard works for an app that has NO apps row at
// all (never discovered): the flush must not create its app_stats row.
func TestRecordStatsSnapshotConditionalUpsertGuardsRemovedApp(t *testing.T) {
	c := openTestState(t)
	c.RecordDiscovered(fnFor(t, "alpha", mustTemplate(t, twoHandlerTmpl)))

	fns := []AppStats{
		{App: "alpha", EventsMatchedTotal: 10},
		{App: "ghost", EventsMatchedTotal: 99}, // never discovered, no apps row
	}
	if err := c.RecordStatsSnapshot(context.Background(), Stats{EventsMatchedTotal: 10}, fns); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	a, ok := c.AppStats("alpha")
	if !ok || a.EventsMatchedTotal != 10 {
		t.Fatalf("alpha = %+v, ok=%v; want events 10", a, ok)
	}
	if _, ok := c.AppStats("ghost"); ok {
		t.Fatal("ghost must not get a app_stats row (no apps row)")
	}
	if _, ok := c.GetApp("ghost"); ok {
		t.Fatal("ghost must not get a apps row")
	}
}

// TestRecordStatsSnapshotFailureRetriesNextFlush simulates a failed flush and
// proves recovery: the failed flush must leave persisted values untouched, and a
// later successful flush must persist the accumulated (absolute) values.
func TestRecordStatsSnapshotFailureRetriesNextFlush(t *testing.T) {
	c := openTestState(t)
	c.RecordDiscovered(fnFor(t, "alpha", mustTemplate(t, twoHandlerTmpl)))

	prev := Stats{EventsMatchedTotal: 50, HandlerSuccessTotal: 40, HandlerFailureTotal: 10, RetryTotal: 2, DLQTotal: 1}
	c.RecordStats(prev)

	// Force a failure WITHOUT deleting the DB: a cancelled ctx makes BeginTx (and,
	// if it somehow succeeded, every bound Exec) fail, so nothing commits.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fns := []AppStats{{App: "alpha", EventsMatchedTotal: 5}}
	if err := c.RecordStatsSnapshot(ctx, Stats{EventsMatchedTotal: 500, HandlerSuccessTotal: 300}, fns); err == nil {
		t.Fatal("expected error on a cancelled-context snapshot")
	}

	// Failed flush must not erase or reset persisted values.
	gs, ok := c.Stats()
	if !ok {
		t.Fatal("expected global stats row after failed flush")
	}
	if gs.EventsMatchedTotal != 50 {
		t.Fatalf("events after failed flush = %d, want 50 preserved", gs.EventsMatchedTotal)
	}
	if gs.HandlerSuccessTotal != 40 {
		t.Fatalf("success after failed flush = %d, want 40 preserved", gs.HandlerSuccessTotal)
	}
	// Nothing from the failed flush was committed, so alpha has no app_stats.
	if _, ok := c.AppStats("alpha"); ok {
		t.Fatal("failed flush must not write app_stats")
	}

	// A later successful flush with a live ctx persists the accumulated values.
	want := Stats{EventsMatchedTotal: 500, HandlerSuccessTotal: 300, HandlerFailureTotal: 150, RetryTotal: 4, DLQTotal: 1}
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
	a, ok := c.AppStats("alpha")
	if !ok || a.EventsMatchedTotal != 5 {
		t.Fatalf("alpha after recovery = %+v, ok=%v; want events 5", a, ok)
	}
}

// TestRecordStatsSnapshotAbsoluteNotDelta pins the absolute-snapshot semantics:
// each flush writes the caller's CURRENT cumulative values, never a delta, so a
// repeated flush with no new activity leaves totals unchanged.
func TestRecordStatsSnapshotAbsoluteNotDelta(t *testing.T) {
	c := openTestState(t)
	ctx := context.Background()

	if err := c.RecordStatsSnapshot(ctx, Stats{EventsMatchedTotal: 100}, nil); err != nil {
		t.Fatalf("first snapshot: %v", err)
	}
	gs, ok := c.Stats()
	if !ok || gs.EventsMatchedTotal != 100 {
		t.Fatalf("events = %d, ok=%v; want 100", gs.EventsMatchedTotal, ok)
	}

	// No new activity, identical value: still 100, not 200.
	if err := c.RecordStatsSnapshot(ctx, Stats{EventsMatchedTotal: 100}, nil); err != nil {
		t.Fatalf("second snapshot: %v", err)
	}
	gs, _ = c.Stats()
	if gs.EventsMatchedTotal != 100 {
		t.Fatalf("events after repeat = %d, want 100 (absolute, not delta)", gs.EventsMatchedTotal)
	}

	// New activity raises the absolute total to 150.
	if err := c.RecordStatsSnapshot(ctx, Stats{EventsMatchedTotal: 150}, nil); err != nil {
		t.Fatalf("third snapshot: %v", err)
	}
	gs, _ = c.Stats()
	if gs.EventsMatchedTotal != 150 {
		t.Fatalf("events after bump = %d, want 150", gs.EventsMatchedTotal)
	}
}

// TestRecordStatsContextHonorsContext verifies RecordStatsContext writes nothing
// on a cancelled context and writes on a background context.
func TestRecordStatsContextHonorsContext(t *testing.T) {
	c := openTestState(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.RecordStatsContext(ctx, Stats{EventsMatchedTotal: 100})
	if _, ok := c.Stats(); ok {
		t.Fatal("cancelled-context RecordStatsContext must not write a stats row")
	}

	c.RecordStatsContext(context.Background(), Stats{EventsMatchedTotal: 100})
	gs, ok := c.Stats()
	if !ok {
		t.Fatal("expected stats row after background-context write")
	}
	if gs.EventsMatchedTotal != 100 {
		t.Fatalf("events = %d, want 100", gs.EventsMatchedTotal)
	}
}

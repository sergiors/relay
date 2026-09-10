package worker

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/metrics"
	"relay/internal/state"
)

// openTempState opens a state DB at a fresh temp-dir path and registers a
// cleanup that closes it. It mirrors openTestState in internal/state but lives
// here because the worker tests are in package worker.
func openTempState(t *testing.T) *state.State {
	t.Helper()
	st, err := state.Open(filepath.Join(t.TempDir(), "db.sqlite3"))
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// TestRestorePersistedStatsSeedsRegistry verifies that restorePersistedStats
// bridges the persisted cumulative counters and per-function rows into a fresh
// process-lifetime registry, so the first snapshot never resets them.
func TestRestorePersistedStatsSeedsRegistry(t *testing.T) {
	m := metrics.New()
	st := openTempState(t)

	st.RecordStats(state.Stats{
		EventsProcessedTotal: 100,
		HandlerSuccessTotal:  70,
		HandlerFailureTotal:  30,
		RetryTotal:           5,
		DLQTotal:             2,
	})
	st.RecordFunctionStats(state.FunctionStats{Function: "alpha", EventsProcessedTotal: 10, HandlerSuccessTotal: 8, HandlerFailureTotal: 2, RetryTotal: 1, DLQTotal: 0})
	st.RecordFunctionStats(state.FunctionStats{Function: "beta", EventsProcessedTotal: 20, HandlerSuccessTotal: 15, HandlerFailureTotal: 5, RetryTotal: 3, DLQTotal: 1})

	restorePersistedStats(m, st)

	if got := m.Counter("events_processed_total"); got != 100 {
		t.Fatalf("events_processed_total = %d, want 100", got)
	}
	if got := m.Counter("handler_success_total"); got != 70 {
		t.Fatalf("handler_success_total = %d, want 70", got)
	}
	if got := m.Counter("handler_failure_total"); got != 30 {
		t.Fatalf("handler_failure_total = %d, want 30", got)
	}
	if got := m.Counter("retries_total"); got != 5 {
		t.Fatalf("retries_total = %d, want 5", got)
	}
	if got := m.Counter("dlq_entries_total"); got != 2 {
		t.Fatalf("dlq_entries_total = %d, want 2", got)
	}

	fs := m.FunctionStatsSnapshot()
	if len(fs) != 2 {
		t.Fatalf("FunctionStatsSnapshot len = %d, want 2: %+v", len(fs), fs)
	}
	if fs[0].Function != "alpha" || fs[0].Events != 10 || fs[0].HandlerSuccessTotal != 8 {
		t.Fatalf("alpha = %+v", fs[0])
	}
	if fs[1].Function != "beta" || fs[1].Events != 20 || fs[1].RetriesTotal != 3 {
		t.Fatalf("beta = %+v", fs[1])
	}
}

// TestRestorePersistedStatsNilSafe guards the nil-safety contract: a nil
// registry or nil state handle must never panic.
func TestRestorePersistedStatsNilSafe(t *testing.T) {
	st := openTempState(t)
	restorePersistedStats(nil, st)            // nil registry
	restorePersistedStats(metrics.New(), nil) // nil state
	restorePersistedStats(nil, nil)           // both nil
}

// TestFirstSnapshotAfterRestorePreservesCounters is the end-to-end regression
// for the restart bug: a fresh registry seeded from persisted values, then an
// immediate snapshot, must not zero the persisted counters. This test FAILS on
// the pre-fix code if restorePersistedStats were removed, because the fresh
// registry (all zeros) would be snapshotted immediately, wiping the persisted
// totals.
func TestFirstSnapshotAfterRestorePreservesCounters(t *testing.T) {
	st := openTempState(t)
	// alpha needs a functions row so its function_stats survives the flush's
	// orphan pruning (a function_stats row with no functions row is pruned).
	st.RecordDiscovered(stateFunction("alpha", t.TempDir()))
	st.RecordStats(state.Stats{
		EventsProcessedTotal: 100,
		RetryTotal:           5,
		PendingEntries:       3,
	})
	st.RecordFunctionStats(state.FunctionStats{Function: "alpha", EventsProcessedTotal: 10})

	// Simulate a restart: a fresh process-lifetime registry seeded from the
	// persisted values.
	m := metrics.New()
	restorePersistedStats(m, st)

	// Simulate ONE new event after restart.
	m.Inc("events_processed_total")
	m.IncLabels("function_events_total", []metrics.Label{{Name: "function", Value: "alpha"}})

	// The worker's statsLoop snapshots immediately on start.
	recordSnapshots(context.Background(), st, m)

	gs, ok := st.Stats()
	if !ok {
		t.Fatal("expected stats row after snapshot")
	}
	if gs.EventsProcessedTotal != 101 {
		t.Fatalf("events = %d, want 101 (100 persisted + 1 new)", gs.EventsProcessedTotal)
	}
	if gs.RetryTotal != 5 {
		t.Fatalf("retries = %d, want 5 (persisted, unchanged)", gs.RetryTotal)
	}
	if gs.HandlerSuccessTotal != 0 || gs.HandlerFailureTotal != 0 || gs.DLQTotal != 0 {
		t.Fatalf("other counters changed: %+v", gs)
	}

	fa, ok := st.FunctionStats("alpha")
	if !ok {
		t.Fatal("expected alpha function stats after snapshot")
	}
	if fa.EventsProcessedTotal != 11 {
		t.Fatalf("alpha events = %d, want 11 (10 persisted + 1 new)", fa.EventsProcessedTotal)
	}

	// A second snapshot with no further activity must be idempotent: counters
	// stay put while the gauge is refreshed.
	m.SetGauge("pending_entries", 9)
	recordSnapshots(context.Background(), st, m)

	gs, ok = st.Stats()
	if !ok {
		t.Fatal("expected stats row after second snapshot")
	}
	if gs.EventsProcessedTotal != 101 {
		t.Fatalf("events after second snapshot = %d, want 101", gs.EventsProcessedTotal)
	}
	if gs.RetryTotal != 5 {
		t.Fatalf("retries after second snapshot = %d, want 5", gs.RetryTotal)
	}
	if gs.PendingEntries != 9 {
		t.Fatalf("pending = %d, want 9 (gauge refreshed)", gs.PendingEntries)
	}

	fa, ok = st.FunctionStats("alpha")
	if !ok {
		t.Fatal("expected alpha function stats after second snapshot")
	}
	if fa.EventsProcessedTotal != 11 {
		t.Fatalf("alpha events after second snapshot = %d, want 11", fa.EventsProcessedTotal)
	}
}

// TestStatsLoopFirstSnapshotPreservesPersistedCounters is the true worker-path
// regression: statsLoop's immediate first snapshot must not zero the persisted
// counters when the registry was seeded from them at startup.
func TestStatsLoopFirstSnapshotPreservesPersistedCounters(t *testing.T) {
	st := openTempState(t)
	st.RecordStats(state.Stats{
		EventsProcessedTotal: 100,
		HandlerSuccessTotal:  70,
		HandlerFailureTotal:  30,
		RetryTotal:           5,
		DLQTotal:             2,
	})

	// Fresh registry seeded from persisted values, as the worker does at startup.
	m := metrics.New()
	restorePersistedStats(m, st)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		// time.Hour interval so no tick fires during the test; only the
		// immediate first snapshot runs.
		statsLoop(ctx, m, st, time.Hour)
	}()

	// Give the goroutine time to run the immediate first snapshot.
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("statsLoop did not stop on cancel")
	}

	gs, ok := st.Stats()
	if !ok {
		t.Fatal("expected stats row after statsLoop")
	}
	if gs.EventsProcessedTotal != 100 {
		t.Fatalf("events = %d, want 100 (persisted, not zeroed)", gs.EventsProcessedTotal)
	}
	if gs.HandlerSuccessTotal != 70 {
		t.Fatalf("success = %d, want 70", gs.HandlerSuccessTotal)
	}
	if gs.HandlerFailureTotal != 30 {
		t.Fatalf("failure = %d, want 30", gs.HandlerFailureTotal)
	}
	if gs.RetryTotal != 5 {
		t.Fatalf("retries = %d, want 5", gs.RetryTotal)
	}
	if gs.DLQTotal != 2 {
		t.Fatalf("dlq = %d, want 2", gs.DLQTotal)
	}
}

// TestStartupSweepPrunesBeforeSeeding proves the startup ordering contract
// end-to-end at the worker level: PruneRemoved must run BEFORE
// restorePersistedStats so a function removed while the worker was down is
// pruned before its stale function_stats row could be re-seeded into metrics.
func TestStartupSweepPrunesBeforeSeeding(t *testing.T) {
	st := openTempState(t)

	// A real functions root with ONE function dir ("kept"); "stale" has no dir.
	root := t.TempDir()
	keptDir := filepath.Join(root, "kept")
	if err := os.MkdirAll(keptDir, 0o755); err != nil {
		t.Fatalf("mkdir kept: %v", err)
	}
	if err := os.WriteFile(filepath.Join(keptDir, "template.yaml"), []byte("runtime: node24\nevents:\n  - handler: index.hi\n    pattern:\n      event_name: [INSERT]\n"), 0o644); err != nil {
		t.Fatalf("write kept template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(keptDir, "index.js"), []byte("export function hi(e){}\n"), 0o644); err != nil {
		t.Fatalf("write kept index: %v", err)
	}

	// Seed the DB as if a prior process had run: "stale" succeeded (no dir on
	// disk now) and both functions have per-function counters.
	st.RecordReconcileSuccess("stale", "img", "fp", time.Now(), stateFunction("stale", keptDir))
	st.RecordFunctionStats(state.FunctionStats{Function: "stale", EventsProcessedTotal: 5})
	st.RecordFunctionStats(state.FunctionStats{Function: "kept", EventsProcessedTotal: 7})
	// A cumulative global row, so restorePersistedStats seeds the registry's
	// global counter and the first snapshot writes it back unchanged.
	st.RecordStats(state.Stats{EventsProcessedTotal: 7})

	// Simulate the worker startup order: rebuild, prune, then seed the registry.
	if err := st.RebuildFromFS(root); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	st.PruneRemoved(root)
	// The real startup path records each loaded function (RecordDiscovered),
	// giving "kept" a functions row so its function_stats survives the flush's
	// orphan pruning.
	st.RecordDiscovered(stateFunction("kept", keptDir))
	m := metrics.New()
	restorePersistedStats(m, st)

	// AllFunctionStats now returns only "kept"; "stale" is absent.
	all := st.AllFunctionStats()
	if len(all) != 1 {
		t.Fatalf("AllFunctionStats len = %d, want 1 (only kept): %+v", len(all), all)
	}
	if all[0].Function != "kept" || all[0].EventsProcessedTotal != 7 {
		t.Fatalf("AllFunctionStats = %+v, want only kept events 7", all[0])
	}
	if _, ok := st.FunctionStats("stale"); ok {
		t.Fatal("stale function_stats must be pruned before seeding")
	}

	// The seeded registry contains ONLY kept's counters: the pruned "stale" row
	// was NOT re-seeded into metrics.
	fs := m.FunctionStatsSnapshot()
	if len(fs) != 1 {
		t.Fatalf("FunctionStatsSnapshot len = %d, want 1: %+v", len(fs), fs)
	}
	if fs[0].Function != "kept" || fs[0].Events != 7 {
		t.Fatalf("seeded registry = %+v, want only kept events 7", fs[0])
	}

	// Run recordSnapshots (exactly what statsLoop does immediately) and confirm
	// the persisted function_stats still has exactly one row and the global
	// counters were written from the seeded registry.
	recordSnapshots(context.Background(), st, m)
	all = st.AllFunctionStats()
	if len(all) != 1 {
		t.Fatalf("AllFunctionStats after snapshot len = %d, want 1: %+v", len(all), all)
	}
	if all[0].Function != "kept" || all[0].EventsProcessedTotal != 7 {
		t.Fatalf("function_stats after snapshot = %+v, want only kept events 7", all[0])
	}
	gs, ok := st.Stats()
	if !ok {
		t.Fatal("expected global stats row after snapshot")
	}
	if gs.EventsProcessedTotal != 7 {
		t.Fatalf("global events = %d, want 7 (from seeded kept registry)", gs.EventsProcessedTotal)
	}
}

// stateFunction builds a minimal function.Function for seeding state rows in
// worker tests without importing the reconciler's helpers.
func stateFunction(name, dir string) function.Function {
	tmpl, err := function.ParseTemplate([]byte("runtime: node24\nevents:\n  - handler: index.hi\n    pattern:\n      event_name: [INSERT]\n"))
	if err != nil {
		panic(err)
	}
	return function.Function{Name: name, Dir: dir, Template: tmpl}
}

package worker

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"relay/internal/function"
	"relay/internal/metrics"
	"relay/internal/state"
)

// seedCorruptRow writes a syntactically invalid JSON payload directly into a
// state DB column, bypassing the state package's marshalling. It is used to pin
// the startup restore path's tolerance of a corrupt persisted payload. The DB
// is first created through state.Open so the schema exists; the raw connection
// then overwrites the target row and closes.
func seedCorruptRow(t *testing.T, path, table, keyCol, key, data string) {
	t.Helper()
	st, err := state.Open(path)
	if err != nil {
		t.Fatalf("prime state: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("prime close: %v", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("raw open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx,
		`INSERT INTO `+table+` (`+keyCol+`, data, updated_at) VALUES (?, ?, ?)`,
		key, data, "2020-01-01T00:00:00Z"); err != nil {
		t.Fatalf("seed corrupt %s row: %v", table, err)
	}
}

// TestRestoreToleratesCorruptGlobalJSON verifies the startup restore path does
// not panic or seed garbage when the persisted global payload is invalid: the
// fresh registry simply stays at zero (the state reader logs the decode error
// and reports the row unreadable).
func TestRestoreToleratesCorruptGlobalJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.sqlite3")
	seedCorruptRow(t, path, "stats", "id", "1", `{not-json`)

	st, err := state.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	m := metrics.New()
	restorePersistedStats(m, st) // must not panic

	if got := m.Counter(metrics.MetricEventsMatched); got != 0 {
		t.Fatalf("events seeded from corrupt payload = %d, want 0", got)
	}
}

// TestRestoreToleratesCorruptFunctionJSON verifies the per-function counterpart:
// a corrupt function_stats payload is skipped by the restore sweep without
// panicking, and it seeds no series.
func TestRestoreToleratesCorruptFunctionJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.sqlite3")
	// Seed a functional row through the public API first, then corrupt it.
	seed, err := state.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	seed.RecordDiscovered(stateFunction("broken", t.TempDir()))
	if err := seed.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	seedCorruptRow(t, path, "function_stats", "function_name", "broken", `{not-json`)

	st, err := state.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	m := metrics.New()
	restorePersistedStats(m, st) // must not panic

	// The corrupt row surfaces no series.
	if fs := m.FunctionStatsSnapshot(); len(fs) != 0 {
		t.Fatalf("corrupt function payload must seed no series: %+v", fs)
	}
}

// TestWorkerFlushReopenRestoreRoundTrip is the worker-path end-to-end for the
// JSON storage format: flush a registry snapshot into a DB, close it, reopen and
// restore into a fresh registry, then flush again — the totals must be
// preserved (never reset) and the pool counters/timestamps must round-trip.
func TestWorkerFlushReopenRestoreRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.sqlite3")

	c1, err := state.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	c1.RecordDiscovered(stateFunction("alpha", t.TempDir()))

	exec := time.Now().Add(-time.Minute).UTC()
	m1 := metrics.New()
	m1.Add(metrics.MetricEventsMatched, 100)
	m1.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "alpha"}})
	m1.AddLabels(metrics.MetricRuntimeContainerAcquires, []metrics.Label{{Name: "function", Value: "alpha"}, {Name: "outcome", Value: metrics.RuntimeOutcomeWarm}}, 7)
	m1.SetFunctionTimestamp("alpha", metrics.FunctionTimestampExecution, exec.Unix())
	recordSnapshots(context.Background(), c1, m1, nil)
	if err := c1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen (restart) and restore into a fresh registry.
	c2, err := state.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer c2.Close()

	m2 := metrics.New()
	restorePersistedStats(m2, c2)
	if got := m2.Counter(metrics.MetricEventsMatched); got != 100 {
		t.Fatalf("restored global events = %d, want 100", got)
	}
	a := byFunction(m2.FunctionStatsSnapshot(), "alpha")
	if a.EventsMatchedTotal != 1 || a.WarmAcquiresTotal != 7 {
		t.Fatalf("restored alpha = %+v, want events 1 warm 7", a)
	}
	if a.LastExecution != exec.Unix() {
		t.Fatalf("restored execution = %d, want %d", a.LastExecution, exec.Unix())
	}

	// The first post-restart flush must write the same totals back.
	recordSnapshots(context.Background(), c2, m2, nil)
	gs, ok := c2.Stats()
	if !ok || gs.EventsMatchedTotal != 100 {
		t.Fatalf("global after restart flush = %+v, ok=%v; want events 100", gs, ok)
	}
	fa, ok := c2.FunctionStats("alpha")
	if !ok || fa.EventsMatchedTotal != 1 || fa.WarmAcquiresTotal != 7 {
		t.Fatalf("alpha after restart flush = %+v, ok=%v", fa, ok)
	}
	if fa.LastExecutionAt != exec.Format(time.RFC3339) {
		t.Fatalf("alpha execution after restart flush = %q, want %q", fa.LastExecutionAt, exec.Format(time.RFC3339))
	}
}

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

// TestSnapshotStatsMapping pins the registry-to-state row mapping: counter names
// feed the columns directly (retries_total → RetryTotal) and the float gauges
// are truncated to int64.
func TestSnapshotStatsMapping(t *testing.T) {
	m := metrics.New()
	m.Add(metrics.MetricEventsReceived, 150)
	m.Add(metrics.MetricEventsMatched, 10)
	m.Add(metrics.MetricEventsUnmatched, 5)
	m.Add(metrics.MetricHandlerSuccess, 7)
	m.Add(metrics.MetricHandlerFailure, 3)
	m.Add(metrics.MetricRetries, 2)
	m.Add(metrics.MetricDLQEntries, 1)
	m.SetGauge(metrics.MetricPendingEntries, 4.9)
	m.SetGauge(metrics.MetricPendingOldestAge, 12.7)

	got := snapshotStats(m, nil)
	want := state.Stats{
		EventsReceivedTotal:     150,
		EventsMatchedTotal:      10,
		EventsUnmatchedTotal:    5,
		HandlerSuccessTotal:     7,
		HandlerFailureTotal:     3,
		RetryTotal:              2, // registry retries_total → stats RetryTotal
		DLQTotal:                1,
		PendingEntries:          4, // float gauge truncated to int64
		OldestPendingAgeSeconds: 12,
	}
	if got != want {
		t.Fatalf("snapshotStats = %+v, want %+v", got, want)
	}
}

// TestSnapshotStatsNilRegistry pins the nil-safety contract of the global mapper.
func TestSnapshotStatsNilRegistry(t *testing.T) {
	got := snapshotStats(nil, nil)
	if got != (state.Stats{}) {
		t.Fatalf("snapshotStats(nil, nil) = %+v, want zero Stats", got)
	}
}

// TestFuncSnapshotStatsMapping pins the per-function registry-to-state row
// mapping, indexed by function name.
func TestFuncSnapshotStatsMapping(t *testing.T) {
	m := metrics.New()
	m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "a"}})
	m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "a"}})
	m.IncLabels(metrics.MetricFunctionHandlerSuccess, []metrics.Label{{Name: "function", Value: "a"}})
	m.IncLabels(metrics.MetricFunctionHandlerFailure, []metrics.Label{{Name: "function", Value: "b"}})
	m.IncLabels(metrics.MetricFunctionRetries, []metrics.Label{{Name: "function", Value: "b"}})
	m.IncLabels(metrics.MetricFunctionDLQ, []metrics.Label{{Name: "function", Value: "b"}})

	got := snapshotFunctionStats(m, nil)
	byName := map[string]state.FunctionStats{}
	for _, fs := range got {
		byName[fs.Function] = fs
	}
	if len(byName) != 2 {
		t.Fatalf("len = %d, want 2: %+v", len(byName), got)
	}
	if a := byName["a"]; a.EventsMatchedTotal != 2 || a.HandlerSuccessTotal != 1 {
		t.Fatalf("function a = %+v", a)
	}
	if b := byName["b"]; b.HandlerFailureTotal != 1 || b.RetryTotal != 1 || b.DLQTotal != 1 {
		t.Fatalf("function b = %+v", b)
	}
}

// TestFuncSnapshotStatsNilRegistry pins the nil-safety contract of the
// per-function mapper.
func TestFuncSnapshotStatsNilRegistry(t *testing.T) {
	if got := snapshotFunctionStats(nil, nil); got != nil {
		t.Fatalf("snapshotFunctionStats(nil, nil) = %+v, want nil", got)
	}
}

// TestSnapshotStatsIgnoresLabeledCounters guards against the unlabeled getters
// accidentally reading labeled-only metrics (which would double-count).
func TestSnapshotStatsIgnoresLabeledCounters(t *testing.T) {
	m := metrics.New()
	m.IncLabels(metrics.MetricHandlerInvocations, []metrics.Label{{Name: "outcome", Value: "success"}, {Name: "function", Value: "a"}, {Name: "handler", Value: "x"}})
	got := snapshotStats(m, nil)
	if got.HandlerSuccessTotal != 0 {
		t.Fatalf("HandlerSuccessTotal = %d, want 0 (labeled only)", got.HandlerSuccessTotal)
	}
	if !strings.Contains(m.Snapshot(), "handler_invocations_total{function=a,handler=x,outcome=success} count=1") {
		t.Fatalf("labeled counter missing from snapshot:\n%s", m.Snapshot())
	}
}

// TestRestorePersistedStatsSeedsRegistry verifies that restorePersistedStats
// bridges the persisted cumulative counters and per-function rows into a fresh
// process-lifetime registry, so the first snapshot never resets them.
func TestRestorePersistedStatsSeedsRegistry(t *testing.T) {
	m := metrics.New()
	st := openTempState(t)

	st.RecordStats(state.Stats{
		EventsReceivedTotal:  105,
		EventsMatchedTotal:   100,
		EventsUnmatchedTotal: 5,
		HandlerSuccessTotal:  70,
		HandlerFailureTotal:  30,
		RetryTotal:           5,
		DLQTotal:             2,
	})
	st.RecordFunctionStats(state.FunctionStats{Function: "alpha", EventsMatchedTotal: 10, HandlerSuccessTotal: 8, HandlerFailureTotal: 2, RetryTotal: 1, DLQTotal: 0})
	st.RecordFunctionStats(state.FunctionStats{Function: "beta", EventsMatchedTotal: 20, HandlerSuccessTotal: 15, HandlerFailureTotal: 5, RetryTotal: 3, DLQTotal: 1})

	restorePersistedStats(m, st)

	if got := m.Counter(metrics.MetricEventsReceived); got != 105 {
		t.Fatalf("events_received_total = %d, want 105", got)
	}
	if got := m.Counter(metrics.MetricEventsMatched); got != 100 {
		t.Fatalf("events_matched_total = %d, want 100", got)
	}
	if got := m.Counter(metrics.MetricEventsUnmatched); got != 5 {
		t.Fatalf("events_unmatched_total = %d, want 5", got)
	}
	if got := m.Counter(metrics.MetricHandlerSuccess); got != 70 {
		t.Fatalf("handler_success_total = %d, want 70", got)
	}
	if got := m.Counter(metrics.MetricHandlerFailure); got != 30 {
		t.Fatalf("handler_failure_total = %d, want 30", got)
	}
	if got := m.Counter(metrics.MetricRetries); got != 5 {
		t.Fatalf("retries_total = %d, want 5", got)
	}
	if got := m.Counter(metrics.MetricDLQEntries); got != 2 {
		t.Fatalf("dlq_entries_total = %d, want 2", got)
	}

	fs := m.FunctionStatsSnapshot()
	if len(fs) != 2 {
		t.Fatalf("FunctionStatsSnapshot len = %d, want 2: %+v", len(fs), fs)
	}
	alpha := byFunction(fs, "alpha")
	if alpha.EventsMatchedTotal != 10 || alpha.HandlerSuccessTotal != 8 {
		t.Fatalf("alpha = %+v", alpha)
	}
	beta := byFunction(fs, "beta")
	if beta.EventsMatchedTotal != 20 || beta.RetriesTotal != 3 {
		t.Fatalf("beta = %+v", beta)
	}
}

// TestRestorePersistedStatsSeedsPoolCounters verifies the restart contract for
// the cumulative warm-container pool counters: a fresh registry seeded from the
// persisted values keeps them monotonic, and the flush writes back the same
// totals (never zeroing them).
func TestRestorePersistedStatsSeedsPoolCounters(t *testing.T) {
	m := metrics.New()
	st := openTempState(t)
	st.RecordDiscovered(stateFunction("alpha", t.TempDir()))
	st.RecordFunctionStats(state.FunctionStats{
		Function:           "alpha",
		EventsMatchedTotal: 1,
		WarmAcquiresTotal:  7,
		ColdStartsTotal:    3,
		DiscardedTotal:     2,
	})

	restorePersistedStats(m, st)
	fs := m.FunctionStatsSnapshot()
	a := byFunction(fs, "alpha")
	if a.WarmAcquiresTotal != 7 || a.ColdStartsTotal != 3 || a.DiscardedTotal != 2 {
		t.Fatalf("seeded pool counters = %+v, want warm 7 cold 3 discarded 2", a)
	}

	// A live acquire/discard after the restore accumulates on top.
	m.IncLabels(metrics.MetricRuntimeContainerAcquires, []metrics.Label{{Name: "function", Value: "alpha"}, {Name: "outcome", Value: metrics.RuntimeOutcomeWarm}})
	m.IncLabels(metrics.MetricRuntimeContainerDiscards, []metrics.Label{{Name: "function", Value: "alpha"}, {Name: "reason", Value: "idle_timeout"}})

	recordSnapshots(context.Background(), st, m, nil)
	got, ok := st.FunctionStats("alpha")
	if !ok {
		t.Fatal("expected alpha function stats after flush")
	}
	if got.WarmAcquiresTotal != 8 || got.ColdStartsTotal != 3 || got.DiscardedTotal != 3 {
		t.Fatalf("pool counters after flush = %+v, want warm 8 cold 3 discarded 3", got)
	}
}

// TestFuncSnapshotStatsPoolCountersMapping verifies the flush mapper carries the
// pool counters from the registry snapshot into the state rows.
func TestFuncSnapshotStatsPoolCountersMapping(t *testing.T) {
	m := metrics.New()
	m.AddLabels(metrics.MetricRuntimeContainerAcquires, []metrics.Label{{Name: "function", Value: "a"}, {Name: "outcome", Value: metrics.RuntimeOutcomeWarm}}, 4)
	m.AddLabels(metrics.MetricRuntimeContainerAcquires, []metrics.Label{{Name: "function", Value: "a"}, {Name: "outcome", Value: metrics.RuntimeOutcomeCold}}, 1)
	m.AddLabels(metrics.MetricRuntimeContainerDiscards, []metrics.Label{{Name: "function", Value: "a"}, {Name: "reason", Value: "timeout"}}, 2)

	got := snapshotFunctionStats(m, nil)
	byName := map[string]state.FunctionStats{}
	for _, fs := range got {
		byName[fs.Function] = fs
	}
	a, ok := byName["a"]
	if !ok {
		t.Fatalf("a missing from flush mapping: %+v", got)
	}
	if a.WarmAcquiresTotal != 4 || a.ColdStartsTotal != 1 || a.DiscardedTotal != 2 {
		t.Fatalf("a pool counters = %+v, want warm 4 cold 1 discarded 2", a)
	}
}

// TestFlushPersistsCountersNotLiveGauges pins the no-live-gauge contract through
// the worker flush path: live pool gauges set on the registry are never carried
// into the persisted per-function snapshot, while the cumulative acquire/cold/
// discard counters are. The typed state.FunctionStats has no gauge fields, so
// the assertion is that the counters round-trip and the gauges leave no trace in
// the decoded row.
func TestFlushPersistsCountersNotLiveGauges(t *testing.T) {
	st := openTempState(t)
	st.RecordDiscovered(stateFunction("alpha", t.TempDir()))

	m := metrics.New()
	m.AddLabels(metrics.MetricRuntimeContainerAcquires, []metrics.Label{{Name: "function", Value: "alpha"}, {Name: "outcome", Value: metrics.RuntimeOutcomeWarm}}, 7)
	m.AddLabels(metrics.MetricRuntimeContainerAcquires, []metrics.Label{{Name: "function", Value: "alpha"}, {Name: "outcome", Value: metrics.RuntimeOutcomeCold}}, 3)
	m.AddLabels(metrics.MetricRuntimeContainerDiscards, []metrics.Label{{Name: "function", Value: "alpha"}, {Name: "reason", Value: "idle_timeout"}}, 2)
	// Live gauges that must NOT be persisted.
	m.SetGaugeLabels(metrics.MetricRuntimeContainers, []metrics.Label{{Name: "function", Value: "alpha"}, {Name: "state", Value: metrics.RuntimeStateIdle}}, 4)
	m.SetGaugeLabels(metrics.MetricRuntimePoolCapacity, []metrics.Label{{Name: "function", Value: "alpha"}}, 5)

	recordSnapshots(context.Background(), st, m, nil)

	got, ok := st.FunctionStats("alpha")
	if !ok {
		t.Fatal("expected alpha after flush")
	}
	if got.WarmAcquiresTotal != 7 || got.ColdStartsTotal != 3 || got.DiscardedTotal != 2 {
		t.Fatalf("persisted counters = %+v, want warm 7 cold 3 discarded 2", got)
	}
	// The decoded row is exactly the typed value: there is no field for a live
	// gauge, so the gauges cannot have leaked into the persisted payload.
	if got != (state.FunctionStats{Function: "alpha", WarmAcquiresTotal: 7, ColdStartsTotal: 3, DiscardedTotal: 2, UpdatedAt: got.UpdatedAt}) {
		t.Fatalf("persisted row carries unexpected fields: %+v", got)
	}

	// A reopen/restore still sees only the counters.
	restored := metrics.New()
	restorePersistedStats(restored, st)
	a := byFunction(restored.FunctionStatsSnapshot(), "alpha")
	if a.WarmAcquiresTotal != 7 || a.ColdStartsTotal != 3 || a.DiscardedTotal != 2 {
		t.Fatalf("restored counters = %+v, want warm 7 cold 3 discarded 2", a)
	}
}

// byFunction finds the FunctionStat for name in a snapshot, or a zero value.
func byFunction(fs []metrics.FunctionStat, name string) metrics.FunctionStat {
	for _, f := range fs {
		if f.Function == name {
			return f
		}
	}
	return metrics.FunctionStat{}
}

// TestRestorePersistedStatsNilSafe guards the nil-safety contract: a nil
// registry or nil state handle must never panic.
func TestRestorePersistedStatsNilSafe(t *testing.T) {
	st := openTempState(t)
	restorePersistedStats(nil, st)            // nil registry
	restorePersistedStats(metrics.New(), nil) // nil state
	restorePersistedStats(nil, nil)           // both nil
}

// TestStatsLoopFirstSnapshotPreservesPersistedCounters is the true worker-path
// regression: statsLoop's immediate first snapshot must not zero the persisted
// counters when the registry was seeded from them at startup.
func TestStatsLoopFirstSnapshotPreservesPersistedCounters(t *testing.T) {
	st := openTempState(t)
	st.RecordStats(state.Stats{
		EventsMatchedTotal:  100,
		HandlerSuccessTotal: 70,
		HandlerFailureTotal: 30,
		RetryTotal:          5,
		DLQTotal:            2,
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
		statsLoop(ctx, newStatsFlusher(st, m), time.Hour)
	}()

	// Wait (bounded) for the goroutine to run the immediate first snapshot: the
	// stats row appears once it has, instead of a fixed sleep.
	waitForStatsRow(t, "statsLoop first snapshot to persist", func() bool {
		_, ok := st.Stats()
		return ok
	})
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
	if gs.EventsMatchedTotal != 100 {
		t.Fatalf("events = %d, want 100 (persisted, not zeroed)", gs.EventsMatchedTotal)
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
	st.RecordFunctionStats(state.FunctionStats{Function: "stale", EventsMatchedTotal: 5})
	st.RecordFunctionStats(state.FunctionStats{Function: "kept", EventsMatchedTotal: 7})
	// A cumulative global row, so restorePersistedStats seeds the registry's
	// global counter and the first snapshot writes it back unchanged.
	st.RecordStats(state.Stats{EventsMatchedTotal: 7})

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
	if all[0].Function != "kept" || all[0].EventsMatchedTotal != 7 {
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
	if fs[0].Function != "kept" || fs[0].EventsMatchedTotal != 7 {
		t.Fatalf("seeded registry = %+v, want only kept events 7", fs[0])
	}

	// Run recordSnapshots (exactly what statsLoop does immediately) and confirm
	// the persisted function_stats still has exactly one row and the global
	// counters were written from the seeded registry.
	recordSnapshots(context.Background(), st, m, nil)
	all = st.AllFunctionStats()
	if len(all) != 1 {
		t.Fatalf("AllFunctionStats after snapshot len = %d, want 1: %+v", len(all), all)
	}
	if all[0].Function != "kept" || all[0].EventsMatchedTotal != 7 {
		t.Fatalf("function_stats after snapshot = %+v, want only kept events 7", all[0])
	}
	gs, ok := st.Stats()
	if !ok {
		t.Fatal("expected global stats row after snapshot")
	}
	if gs.EventsMatchedTotal != 7 {
		t.Fatalf("global events = %d, want 7 (from seeded kept registry)", gs.EventsMatchedTotal)
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

// TestRestoreSeedsAndFlushesTimestamps is the end-to-end timestamp regression:
// a DB sealed with RecordFunctionStats including timestamps (simulating a prior
// process), restorePersistedStats seeding, a flush from a FRESH registry entry
// WITHOUT timestamps, and the persisted timestamps must still survive the
// flush's case-guarded upsert.
func TestRestoreSeedsAndFlushesTimestamps(t *testing.T) {
	st := openTempState(t)
	st.RecordDiscovered(stateFunction("alpha", t.TempDir()))

	exec := time.Now().Add(-time.Minute).UTC()
	dlq := exec.Add(-time.Minute)
	st.RecordFunctionStats(state.FunctionStats{
		Function:           "alpha",
		EventsMatchedTotal: 10,
		LastExecutionAt:    exec.Format(time.RFC3339),
		LastSuccessAt:      exec.Format(time.RFC3339),
		LastDLQAt:          dlq.Format(time.RFC3339),
	})

	// Restore: the registry's unix-seconds timestamps must round-trip.
	m := metrics.New()
	restorePersistedStats(m, st)
	fs := m.FunctionStatsSnapshot()
	if len(fs) != 1 {
		t.Fatalf("snapshot len = %d, want 1: %+v", len(fs), fs)
	}
	wantExec := exec.Unix()
	if fs[0].LastExecution != wantExec || fs[0].LastSuccess != wantExec || fs[0].LastDLQ != dlq.Unix() || fs[0].LastFailure != 0 {
		t.Fatalf("seeded timestamps = %+v", fs[0])
	}

	// Simulate ONE post-restart activity in the restored registry: the live
	// (seeded) timestamps are republished untouched.
	m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "alpha"}})
	m.IncLabels(metrics.MetricFunctionHandlerSuccess, []metrics.Label{{Name: "function", Value: "alpha"}})
	recordSnapshots(context.Background(), st, m, nil)
	got, ok := st.FunctionStats("alpha")
	if !ok {
		t.Fatal("expected alpha stats after flush")
	}
	if got.LastExecutionAt != exec.Format(time.RFC3339) || got.LastSuccessAt != exec.Format(time.RFC3339) {
		t.Fatalf("flush must republish the seeded timestamps: %+v", got)
	}

	// Now the case guard in isolation: flush from a FRESH registry WITHOUT any
	// timestamps for alpha (only a counters series) — the function_stats row
	// must keep the persisted timestamps (an empty incoming value never
	// clobbers them).
	fresh := metrics.New()
	fresh.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "alpha"}})
	recordSnapshots(context.Background(), st, fresh, nil)
	got, ok = st.FunctionStats("alpha")
	if !ok {
		t.Fatal("expected alpha stats after fresh-registry flush")
	}
	wantDLQ := dlq.Format(time.RFC3339)
	if got.LastDLQAt != wantDLQ {
		t.Fatalf("flush with NO DLQ observation must preserve the persisted LastDLQAt %q: %+v", wantDLQ, got)
	}
	if got.LastExecutionAt != exec.Format(time.RFC3339) {
		t.Fatalf("flush with NO execution observation must preserve the persisted LastExecutionAt: %+v", got)
	}
}

// TestRestoreSkipsInvalidTimestamps pins the restore parser's tolerance: empty
// and unparseable persisted strings restore as 0 ("never observed") and the
// worker never errors the startup path over a corrupt timestamp column.
func TestRestoreSkipsInvalidAndEmptyTimestamps(t *testing.T) {
	st := openTempState(t)
	st.RecordDiscovered(stateFunction("alpha", t.TempDir()))
	st.RecordFunctionStats(state.FunctionStats{
		Function:           "alpha",
		EventsMatchedTotal: 1,
		LastExecutionAt:    "not-a-timestamp",
		LastSuccessAt:      "",
	})

	m := metrics.New()
	restorePersistedStats(m, st)
	fs := m.FunctionStatsSnapshot()
	if len(fs) != 1 || fs[0].LastExecution != 0 || fs[0].LastSuccess != 0 {
		t.Fatalf("invalid/empty timestamps must restore as 0: %+v", fs)
	}
}

// TestFuncSnapshotStatsTimestampsMapping verifies the flush mapper: unix
// seconds map to RFC3339 strings and zeros map to "" (never observed).
func TestFuncSnapshotStatsTimestampsMapping(t *testing.T) {
	m := metrics.New()
	ts := int64(1700000000)
	m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "a"}})
	m.SetFunctionTimestamp("a", metrics.FunctionTimestampExecution, ts)
	m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "b"}})
	m.SetFunctionTimestamp("b", metrics.FunctionTimestampDLQ, ts+60)

	got := snapshotFunctionStats(m, nil)
	byName := map[string]state.FunctionStats{}
	for _, fs := range got {
		byName[fs.Function] = fs
	}
	a, ok := byName["a"]
	if !ok {
		t.Fatalf("a missing: %+v", got)
	}
	if a.LastExecutionAt != time.Unix(ts, 0).UTC().Format(time.RFC3339) {
		t.Fatalf("a = %+v; want execution %s", a, time.Unix(ts, 0).UTC().Format(time.RFC3339))
	}
	if a.LastSuccessAt != "" || a.LastFailureAt != "" || a.LastDLQAt != "" {
		t.Fatalf("unobserved timestamps must map to empty strings: %+v", a)
	}
	b := byName["b"]
	if b.LastDLQAt != time.Unix(ts+60, 0).UTC().Format(time.RFC3339) || b.LastExecutionAt != "" {
		t.Fatalf("b = %+v", b)
	}
}

// TestRfc3339ToUnixRoundTrip pins the two conversion helpers as strict
// inverses for valid values and "never" sentinels at the boundaries.
func TestRfc3339ToUnixRoundTrip(t *testing.T) {
	ts := int64(1700000000)
	if got := unixSecToRFC3339(ts); got != "2023-11-14T22:13:20Z" {
		t.Fatalf("unixSecToRFC3339(%d) = %q", ts, got)
	}
	if got := rfc3339ToUnix(unixSecToRFC3339(ts)); got != ts {
		t.Fatalf("round trip = %d, want %d", got, ts)
	}
	// "never" conventions: zero in → empty out; empty/invalid in → 0 out.
	if unixSecToRFC3339(0) != "" || unixSecToRFC3339(-5) != "" {
		t.Fatal("non-positive timestamps must map to empty strings")
	}
	if rfc3339ToUnix("") != 0 || rfc3339ToUnix("garbage") != 0 {
		t.Fatal("empty/invalid values must map to 0")
	}
}

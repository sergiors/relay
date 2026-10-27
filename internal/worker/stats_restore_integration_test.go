//go:build integration

package worker

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"relay/internal/observability/metrics"
	"relay/internal/state"
	"relay/internal/testutil"
)

// TestRestoreToleratesCorruptGlobalJSON verifies the startup restore path does
// not panic or seed garbage when the persisted global payload is invalid: the
// fresh registry simply stays at zero (the state reader logs the decode error
// and reports the row unreadable).
func TestRestoreToleratesCorruptGlobalJSON(t *testing.T) {
	dsn := testutil.NewStateDSN(t)
	st := testutil.OpenState(t, dsn)
	seedCorruptGlobalRow(t, dsn, st.Owner())

	m := metrics.New()
	restorePersistedStats(m, st) // must not panic

	if got := m.Counter(metrics.MetricEventsMatched); got != 0 {
		t.Fatalf("events seeded from corrupt payload = %d, want 0", got)
	}
}

// TestRestoreToleratesCorruptAppJSON verifies the per-app counterpart:
// a corrupt app_stats payload is skipped by the restore sweep without
// panicking, and it seeds no series.
func TestRestoreToleratesCorruptAppJSON(t *testing.T) {
	dsn := testutil.NewStateDSN(t)
	st := testutil.OpenState(t, dsn)
	if _, err := st.RecordDiscovered(context.Background(), stateApp("broken", t.TempDir())); err != nil {
		t.Errorf("state write error: %v", err)
	}
	seedCorruptAppRow(t, dsn, st.Owner(), "broken")

	m := metrics.New()
	restorePersistedStats(m, st) // must not panic

	// The corrupt row surfaces no series.
	if fs := m.AppStatsSnapshot(); len(fs) != 0 {
		t.Fatalf("corrupt function payload must seed no series: %+v", fs)
	}
}

// TestWorkerFlushReopenRestoreRoundTrip is the worker-path end-to-end for the
// JSON storage format: flush a registry snapshot into a DB, close it, reopen and
// restore into a fresh registry, then flush again — the totals must be
// preserved (never reset) and the pool counters/timestamps must round-trip.
func TestWorkerFlushReopenRestoreRoundTrip(t *testing.T) {
	dsn := testutil.NewStateDSN(t)

	c1 := testutil.OpenState(t, dsn)
	if _, err := c1.RecordDiscovered(context.Background(), stateApp("alpha", t.TempDir())); err != nil {
		t.Errorf("state write error: %v", err)
	}

	exec := time.Now().Add(-time.Minute).UTC()
	m1 := metrics.New()
	m1.Add(metrics.MetricEventsMatched, 100)
	m1.IncLabels(metrics.MetricAppEventsMatched, []metrics.Label{{Name: "app", Value: "alpha"}})
	m1.AddLabels(metrics.MetricRuntimeContainerAcquires, []metrics.Label{{Name: "app", Value: "alpha"}, {Name: "outcome", Value: metrics.RuntimeOutcomeWarm}}, 7)
	m1.SetAppTimestamp("alpha", metrics.AppTimestampExecution, exec.Unix())
	newStatsFlusher(c1, m1).flush(context.Background())
	if err := c1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen (restart) and restore into a fresh registry.
	c2 := testutil.OpenState(t, dsn)

	m2 := metrics.New()
	restorePersistedStats(m2, c2)
	if got := m2.Counter(metrics.MetricEventsMatched); got != 100 {
		t.Fatalf("restored global events = %d, want 100", got)
	}
	a := byApp(m2.AppStatsSnapshot(), "alpha")
	if a.EventsMatchedTotal != 1 || a.WarmAcquiresTotal != 7 {
		t.Fatalf("restored alpha = %+v, want events 1 warm 7", a)
	}
	if a.LastExecution != exec.Unix() {
		t.Fatalf("restored execution = %d, want %d", a.LastExecution, exec.Unix())
	}

	// The first post-restart flush must write the same totals back.
	newStatsFlusher(c2, m2).flush(context.Background())
	gs, ok := c2.Stats()
	if !ok || gs.EventsMatchedTotal != 100 {
		t.Fatalf("global after restart flush = %+v, ok=%v; want events 100", gs, ok)
	}
	fa, ok := c2.AppStats("alpha")
	if !ok || fa.EventsMatchedTotal != 1 || fa.WarmAcquiresTotal != 7 {
		t.Fatalf("alpha after restart flush = %+v, ok=%v", fa, ok)
	}
	if fa.LastExecutionAt != exec.Format(time.RFC3339) {
		t.Fatalf("alpha execution after restart flush = %q, want %q", fa.LastExecutionAt, exec.Format(time.RFC3339))
	}
}

// TestRestorePersistedStatsSeedsRegistry verifies that restorePersistedStats
// bridges the persisted cumulative counters and per-app rows into a fresh
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
	st.RecordAppStats(state.AppStats{App: "alpha", EventsMatchedTotal: 10, HandlerSuccessTotal: 8, HandlerFailureTotal: 2, RetryTotal: 1, DLQTotal: 0})
	st.RecordAppStats(state.AppStats{App: "beta", EventsMatchedTotal: 20, HandlerSuccessTotal: 15, HandlerFailureTotal: 5, RetryTotal: 3, DLQTotal: 1})

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

	fs := m.AppStatsSnapshot()
	if len(fs) != 2 {
		t.Fatalf("AppStatsSnapshot len = %d, want 2: %+v", len(fs), fs)
	}
	alpha := byApp(fs, "alpha")
	if alpha.EventsMatchedTotal != 10 || alpha.HandlerSuccessTotal != 8 {
		t.Fatalf("alpha = %+v", alpha)
	}
	beta := byApp(fs, "beta")
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
	if _, err := st.RecordDiscovered(context.Background(), stateApp("alpha", t.TempDir())); err != nil {
		t.Errorf("state write error: %v", err)
	}
	st.RecordAppStats(state.AppStats{
		App:                "alpha",
		EventsMatchedTotal: 1,
		WarmAcquiresTotal:  7,
		ColdStartsTotal:    3,
		DiscardedTotal:     2,
	})

	restorePersistedStats(m, st)
	fs := m.AppStatsSnapshot()
	a := byApp(fs, "alpha")
	if a.WarmAcquiresTotal != 7 || a.ColdStartsTotal != 3 || a.DiscardedTotal != 2 {
		t.Fatalf("seeded pool counters = %+v, want warm 7 cold 3 discarded 2", a)
	}

	// A live acquire/discard after the restore accumulates on top.
	m.IncLabels(metrics.MetricRuntimeContainerAcquires, []metrics.Label{{Name: "app", Value: "alpha"}, {Name: "outcome", Value: metrics.RuntimeOutcomeWarm}})
	m.IncLabels(metrics.MetricRuntimeContainerDiscards, []metrics.Label{{Name: "app", Value: "alpha"}, {Name: "reason", Value: "idle_timeout"}})

	newStatsFlusher(st, m).flush(context.Background())
	got, ok := st.AppStats("alpha")
	if !ok {
		t.Fatal("expected alpha function stats after flush")
	}
	if got.WarmAcquiresTotal != 8 || got.ColdStartsTotal != 3 || got.DiscardedTotal != 3 {
		t.Fatalf("pool counters after flush = %+v, want warm 8 cold 3 discarded 3", got)
	}
}

// TestFlushPersistsCountersNotLiveGauges pins the no-live-gauge contract through
// the worker flush path: live pool gauges set on the registry are never carried
// into the persisted per-app snapshot, while the cumulative acquire/cold/
// discard counters are. The typed state.AppStats has no gauge fields, so
// the assertion is that the counters round-trip and the gauges leave no trace in
// the decoded row.
func TestFlushPersistsCountersNotLiveGauges(t *testing.T) {
	st := openTempState(t)
	if _, err := st.RecordDiscovered(context.Background(), stateApp("alpha", t.TempDir())); err != nil {
		t.Errorf("state write error: %v", err)
	}

	m := metrics.New()
	m.AddLabels(metrics.MetricRuntimeContainerAcquires, []metrics.Label{{Name: "app", Value: "alpha"}, {Name: "outcome", Value: metrics.RuntimeOutcomeWarm}}, 7)
	m.AddLabels(metrics.MetricRuntimeContainerAcquires, []metrics.Label{{Name: "app", Value: "alpha"}, {Name: "outcome", Value: metrics.RuntimeOutcomeCold}}, 3)
	m.AddLabels(metrics.MetricRuntimeContainerDiscards, []metrics.Label{{Name: "app", Value: "alpha"}, {Name: "reason", Value: "idle_timeout"}}, 2)
	// Live gauges that must NOT be persisted.
	m.SetGaugeLabels(metrics.MetricRuntimeContainers, []metrics.Label{{Name: "app", Value: "alpha"}, {Name: "state", Value: metrics.RuntimeStateIdle}}, 4)
	m.SetGaugeLabels(metrics.MetricRuntimePoolCapacity, []metrics.Label{{Name: "app", Value: "alpha"}}, 5)

	newStatsFlusher(st, m).flush(context.Background())

	got, ok := st.AppStats("alpha")
	if !ok {
		t.Fatal("expected alpha after flush")
	}
	if got.WarmAcquiresTotal != 7 || got.ColdStartsTotal != 3 || got.DiscardedTotal != 2 {
		t.Fatalf("persisted counters = %+v, want warm 7 cold 3 discarded 2", got)
	}
	// The decoded row is exactly the typed value: there is no field for a live
	// gauge, so the gauges cannot have leaked into the persisted payload.
	if got != (state.AppStats{App: "alpha", WarmAcquiresTotal: 7, ColdStartsTotal: 3, DiscardedTotal: 2, UpdatedAt: got.UpdatedAt}) {
		t.Fatalf("persisted row carries unexpected fields: %+v", got)
	}

	// A reopen/restore still sees only the counters.
	restored := metrics.New()
	restorePersistedStats(restored, st)
	a := byApp(restored.AppStatsSnapshot(), "alpha")
	if a.WarmAcquiresTotal != 7 || a.ColdStartsTotal != 3 || a.DiscardedTotal != 2 {
		t.Fatalf("restored counters = %+v, want warm 7 cold 3 discarded 2", a)
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
// restorePersistedStats so an app removed while the worker was down is
// pruned before its stale app_stats row could be re-seeded into metrics.
func TestStartupSweepPrunesBeforeSeeding(t *testing.T) {
	st := openTempState(t)

	// A real apps root with ONE app dir ("kept"); "stale" has no dir.
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
	// disk now) and both apps have per-app counters.
	recordSuccess(t, st, stateApp("stale", keptDir), "img", "fp", time.Now())
	st.RecordAppStats(state.AppStats{App: "stale", EventsMatchedTotal: 5})
	st.RecordAppStats(state.AppStats{App: "kept", EventsMatchedTotal: 7})
	// A cumulative global row, so restorePersistedStats seeds the registry's
	// global counter and the first snapshot writes it back unchanged.
	st.RecordStats(state.Stats{EventsMatchedTotal: 7})

	// Simulate the worker startup order: rebuild, prune, then seed the registry.
	if err := st.RebuildFromFS(context.Background(), root); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if err := st.PruneRemoved(context.Background(), root); err != nil {
		t.Errorf("state write error: %v", err)
	}
	// The real startup path records each loaded app (RecordDiscovered),
	// giving "kept" a apps row so its app_stats survives the flush's
	// orphan pruning.
	if _, err := st.RecordDiscovered(context.Background(), stateApp("kept", keptDir)); err != nil {
		t.Errorf("state write error: %v", err)
	}
	m := metrics.New()
	restorePersistedStats(m, st)

	// AllAppStats now returns only "kept"; "stale" is absent.
	all := st.AllAppStats()
	if len(all) != 1 {
		t.Fatalf("AllAppStats len = %d, want 1 (only kept): %+v", len(all), all)
	}
	if all[0].App != "kept" || all[0].EventsMatchedTotal != 7 {
		t.Fatalf("AllAppStats = %+v, want only kept events 7", all[0])
	}
	if _, ok := st.AppStats("stale"); ok {
		t.Fatal("stale app_stats must be pruned before seeding")
	}

	// The seeded registry contains ONLY kept's counters: the pruned "stale" row
	// was NOT re-seeded into metrics.
	fs := m.AppStatsSnapshot()
	if len(fs) != 1 {
		t.Fatalf("AppStatsSnapshot len = %d, want 1: %+v", len(fs), fs)
	}
	if fs[0].App != "kept" || fs[0].EventsMatchedTotal != 7 {
		t.Fatalf("seeded registry = %+v, want only kept events 7", fs[0])
	}

	// Run a flush (exactly what statsLoop does immediately) and confirm
	// the persisted app_stats still has exactly one row and the global
	// counters were written from the seeded registry.
	newStatsFlusher(st, m).flush(context.Background())
	all = st.AllAppStats()
	if len(all) != 1 {
		t.Fatalf("AllAppStats after snapshot len = %d, want 1: %+v", len(all), all)
	}
	if all[0].App != "kept" || all[0].EventsMatchedTotal != 7 {
		t.Fatalf("app_stats after snapshot = %+v, want only kept events 7", all[0])
	}
	gs, ok := st.Stats()
	if !ok {
		t.Fatal("expected global stats row after snapshot")
	}
	if gs.EventsMatchedTotal != 7 {
		t.Fatalf("global events = %d, want 7 (from seeded kept registry)", gs.EventsMatchedTotal)
	}
}

// TestRestoreSeedsAndFlushesTimestamps is the end-to-end timestamp regression:
// a DB sealed with RecordAppStats including timestamps (simulating a prior
// process), restorePersistedStats seeding, a flush from a FRESH registry entry
// WITHOUT timestamps, and the persisted timestamps must still survive the
// flush's case-guarded upsert.
func TestRestoreSeedsAndFlushesTimestamps(t *testing.T) {
	st := openTempState(t)
	if _, err := st.RecordDiscovered(context.Background(), stateApp("alpha", t.TempDir())); err != nil {
		t.Errorf("state write error: %v", err)
	}

	exec := time.Now().Add(-time.Minute).UTC()
	dlq := exec.Add(-time.Minute)
	st.RecordAppStats(state.AppStats{
		App:                "alpha",
		EventsMatchedTotal: 10,
		LastExecutionAt:    exec.Format(time.RFC3339),
		LastSuccessAt:      exec.Format(time.RFC3339),
		LastDLQAt:          dlq.Format(time.RFC3339),
	})

	// Restore: the registry's unix-seconds timestamps must round-trip.
	m := metrics.New()
	restorePersistedStats(m, st)
	fs := m.AppStatsSnapshot()
	if len(fs) != 1 {
		t.Fatalf("snapshot len = %d, want 1: %+v", len(fs), fs)
	}
	wantExec := exec.Unix()
	if fs[0].LastExecution != wantExec || fs[0].LastSuccess != wantExec || fs[0].LastDLQ != dlq.Unix() || fs[0].LastFailure != 0 {
		t.Fatalf("seeded timestamps = %+v", fs[0])
	}

	// Simulate ONE post-restart activity in the restored registry: the live
	// (seeded) timestamps are republished untouched.
	m.IncLabels(metrics.MetricAppEventsMatched, []metrics.Label{{Name: "app", Value: "alpha"}})
	m.IncLabels(metrics.MetricFunctionHandlerSuccess, []metrics.Label{{Name: "app", Value: "alpha"}})
	newStatsFlusher(st, m).flush(context.Background())
	got, ok := st.AppStats("alpha")
	if !ok {
		t.Fatal("expected alpha stats after flush")
	}
	if got.LastExecutionAt != exec.Format(time.RFC3339) || got.LastSuccessAt != exec.Format(time.RFC3339) {
		t.Fatalf("flush must republish the seeded timestamps: %+v", got)
	}

	// Now the case guard in isolation: flush from a FRESH registry WITHOUT any
	// timestamps for alpha (only a counters series) — the app_stats row
	// must keep the persisted timestamps (an empty incoming value never
	// clobbers them).
	fresh := metrics.New()
	fresh.IncLabels(metrics.MetricAppEventsMatched, []metrics.Label{{Name: "app", Value: "alpha"}})
	newStatsFlusher(st, fresh).flush(context.Background())
	got, ok = st.AppStats("alpha")
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
	if _, err := st.RecordDiscovered(context.Background(), stateApp("alpha", t.TempDir())); err != nil {
		t.Errorf("state write error: %v", err)
	}
	st.RecordAppStats(state.AppStats{
		App:                "alpha",
		EventsMatchedTotal: 1,
		LastExecutionAt:    "not-a-timestamp",
		LastSuccessAt:      "",
	})

	m := metrics.New()
	restorePersistedStats(m, st)
	fs := m.AppStatsSnapshot()
	if len(fs) != 1 || fs[0].LastExecution != 0 || fs[0].LastSuccess != 0 {
		t.Fatalf("invalid/empty timestamps must restore as 0: %+v", fs)
	}
}

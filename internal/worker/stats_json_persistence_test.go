package worker

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"

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

	if got := m.Counter(metrics.MetricEventsProcessed); got != 0 {
		t.Fatalf("events seeded from corrupt payload = %d, want 0", got)
	}
}

// TestRestoreToleratesCorruptFunctionJSON verifies the per-function counterpart:
// a corrupt function_stats payload is skipped by the restore sweep without
// panicking, and functions after it are still seeded.
func TestRestoreToleratesCorruptFunctionJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.sqlite3")
	// Seed a good row through the public API first, then corrupt a second row.
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

	recordSnapshots(context.Background(), st, m)

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
	fs := restored.FunctionStatsSnapshot()
	a := byFunction(fs, "alpha")
	if a.WarmAcquiresTotal != 7 || a.ColdStartsTotal != 3 || a.DiscardedTotal != 2 {
		t.Fatalf("restored counters = %+v, want warm 7 cold 3 discarded 2", a)
	}
}

// TestWorkerFlushReopenRestoreRoundTrip is the worker-path end-to-end for the
// JSON storage format: flush a registry snapshot into a DB, close it, reopen and
// restore into a fresh registry, then flush again — the totals must be
// preserved (never reset) and the pool counters/timestamps must round-trip
// through the JSON payload.
func TestWorkerFlushReopenRestoreRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.sqlite3")

	c1, err := state.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	c1.RecordDiscovered(stateFunction("alpha", t.TempDir()))

	exec := time.Now().Add(-time.Minute).UTC()
	m1 := metrics.New()
	m1.Add(metrics.MetricEventsProcessed, 100)
	m1.IncLabels(metrics.MetricFunctionEvents, []metrics.Label{{Name: "function", Value: "alpha"}})
	m1.AddLabels(metrics.MetricRuntimeContainerAcquires, []metrics.Label{{Name: "function", Value: "alpha"}, {Name: "outcome", Value: metrics.RuntimeOutcomeWarm}}, 7)
	m1.SetFunctionTimestamp("alpha", metrics.FunctionTimestampExecution, exec.Unix())
	recordSnapshots(context.Background(), c1, m1)
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
	if got := m2.Counter(metrics.MetricEventsProcessed); got != 100 {
		t.Fatalf("restored global events = %d, want 100", got)
	}
	a := byFunction(m2.FunctionStatsSnapshot(), "alpha")
	if a.Events != 1 || a.WarmAcquiresTotal != 7 {
		t.Fatalf("restored alpha = %+v, want events 1 warm 7", a)
	}
	if a.LastExecution != exec.Unix() {
		t.Fatalf("restored execution = %d, want %d", a.LastExecution, exec.Unix())
	}

	// The first post-restart flush must write the same totals back.
	recordSnapshots(context.Background(), c2, m2)
	gs, ok := c2.Stats()
	if !ok || gs.EventsProcessedTotal != 100 {
		t.Fatalf("global after restart flush = %+v, ok=%v; want events 100", gs, ok)
	}
	fa, ok := c2.FunctionStats("alpha")
	if !ok || fa.EventsProcessedTotal != 1 || fa.WarmAcquiresTotal != 7 {
		t.Fatalf("alpha after restart flush = %+v, ok=%v", fa, ok)
	}
	if fa.LastExecutionAt != exec.Format(time.RFC3339) {
		t.Fatalf("alpha execution after restart flush = %q, want %q", fa.LastExecutionAt, exec.Format(time.RFC3339))
	}
}

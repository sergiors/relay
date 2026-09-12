package worker

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/metrics"
	"relay/internal/state"
)

// waitForStatsRow polls cond until it holds or a generous deadline passes. It
// is the bounded-poll replacement for fixed sleeps that awaited the async SQLite
// statsLoop flush, so the first flush and the >1-tick tracking are observed
// deterministically instead of timing out on slow CI.
func waitForStatsRow(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestRecordSnapshotsSweepsStaleFunctionSeries pins the flush-path invariant:
// recordSnapshots sweeps the registry against the live function set (from the
// state DB), deleting series for a function whose functions row is gone even
// though its series still linger in the registry, while live functions' series
// and global counters survive in BOTH SQLite and the registry.
func TestRecordSnapshotsSweepsStaleFunctionSeries(t *testing.T) {
	st := openTempState(t)
	st.RecordDiscovered(stateFunction("alpha", t.TempDir()))
	st.RecordDiscovered(stateFunction("ghost", t.TempDir()))
	st.RecordFunctionStats(state.FunctionStats{Function: "alpha", EventsProcessedTotal: 10})
	st.RecordFunctionStats(state.FunctionStats{Function: "ghost", EventsProcessedTotal: 5})

	m := metrics.New()
	m.IncLabels("function_events_total", []metrics.Label{{Name: "function", Value: "alpha"}})
	m.IncLabels("function_events_total", []metrics.Label{{Name: "function", Value: "alpha"}})
	m.IncLabels("function_events_total", []metrics.Label{{Name: "function", Value: "ghost"}})
	m.Add("events_processed_total", 42)
	m.Add("handler_success_total", 30)

	// Remove ghost's functions row (the reconciler's RecordRemoved) but KEEP its
	// series in the registry, simulating an in-flight invocation that recreated
	// them after removal. globals are untouched by removal.
	st.RecordRemoved("ghost")

	recordSnapshots(context.Background(), st, m)

	// Ghost's SQLite function_stats is absent (pre-existing orphan-prune/upsert
	// behavior).
	if _, ok := st.FunctionStats("ghost"); ok {
		t.Fatal("ghost function_stats must be absent after the sweep+flush")
	}
	// Ghost's registry series are swept away.
	if fs := m.FunctionStatsSnapshot(); metricsStat(fs, "ghost") {
		t.Fatalf("ghost must be absent from FunctionStatsSnapshot:\n%+v", fs)
	}
	if strings.Contains(m.Snapshot(), "function=ghost,") {
		t.Fatalf("ghost series must be swept from the registry:\n%s", m.Snapshot())
	}

	// Alpha's series survive in both SQLite and the registry. The flush persists
	// the registry's current absolute value (alpha was incremented twice), so
	// its row is present with the live value — proving alpha was NOT swept.
	a, ok := st.FunctionStats("alpha")
	if !ok || a.EventsProcessedTotal != 2 {
		t.Fatalf("alpha = %+v, ok=%v; want events 2 (registry value preserved)", a, ok)
	}
	if fs := m.FunctionStatsSnapshot(); !metricsStat(fs, "alpha") {
		t.Fatalf("alpha must remain in FunctionStatsSnapshot:\n%+v", fs)
	}

	// Global counters unchanged (registry values persisted to SQLite).
	gs, _ := st.Stats()
	if gs.EventsProcessedTotal != 42 {
		t.Fatalf("global events = %d, want 42", gs.EventsProcessedTotal)
	}
	if gs.HandlerSuccessTotal != 30 {
		t.Fatalf("global success = %d, want 30", gs.HandlerSuccessTotal)
	}
	if m.Counter("events_processed_total") != 42 {
		t.Fatalf("registry events = %d, want 42", m.Counter("events_processed_total"))
	}
}

// TestReaddFunctionFreshSeries pins SQLite consistency on re-add: after a
// function's series are removed, re-incrementing starts at 1 and the flush
// persists that fresh small value (the row was recreated/upserted, not held at a
// stale value).
func TestReaddFunctionFreshSeries(t *testing.T) {
	st := openTempState(t)
	st.RecordDiscovered(stateFunction("alpha", t.TempDir()))
	m := metrics.New()

	// Seed and flush an initial value, then remove the series as the reconciler
	// would on removal.
	m.IncLabels("function_events_total", []metrics.Label{{Name: "function", Value: "alpha"}})
	m.IncLabels("function_events_total", []metrics.Label{{Name: "function", Value: "alpha"}})
	recordSnapshots(context.Background(), st, m)
	m.RemoveFunction("alpha")

	// Re-add: a fresh series at count 1.
	m.IncLabels("function_events_total", []metrics.Label{{Name: "function", Value: "alpha"}})
	if !strings.Contains(m.Snapshot(), "function_events_total{function=alpha} count=1") {
		t.Fatalf("re-added alpha must count 1:\n%s", m.Snapshot())
	}
	recordSnapshots(context.Background(), st, m)
	a, ok := st.FunctionStats("alpha")
	if !ok || a.EventsProcessedTotal != 1 {
		t.Fatalf("alpha persisted = %+v, ok=%v; want events 1", a, ok)
	}
}

// TestSweepSkippedOnStateReadError pins the fail-open sweep: when the live set
// cannot be read (FunctionNames errors, here by closing the DB so every read on
// the single pooled connection fails), recordSnapshots must NOT sweep against an
// empty live map — that would delete live functions' series. The sweep is
// skipped entirely, so alpha and ghost series both survive in the registry (and
// the flush itself is a no-op on the closed handle, logging internally without
// panicking). Once the state DB is reopened on the same path (Open's schema init
// is idempotent), a subsequent flush sweeps ghost and keeps alpha — the
// ghost/live behavior resumes.
func TestSweepSkippedOnStateReadError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.sqlite3")
	st, err := state.Open(path)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	st.RecordDiscovered(stateFunction("alpha", t.TempDir()))
	st.RecordDiscovered(stateFunction("ghost", t.TempDir()))

	m := metrics.New()
	m.IncLabels("function_events_total", []metrics.Label{{Name: "function", Value: "alpha"}})
	m.IncLabels("function_events_total", []metrics.Label{{Name: "function", Value: "alpha"}})
	m.IncLabels("function_events_total", []metrics.Label{{Name: "function", Value: "ghost"}})
	m.Add("events_processed_total", 42)

	// Close the DB so the pooled (single) connection is gone: FunctionNames and
	// RecordStatsSnapshot both error, but the sweep must fail open.
	if err := st.Close(); err != nil {
		t.Fatalf("close state: %v", err)
	}

	// Must not panic.
	recordSnapshots(context.Background(), st, m)

	// Fail-open: BOTH alpha and ghost series survive the failed flush.
	if fs := m.FunctionStatsSnapshot(); !metricsStat(fs, "alpha") || !metricsStat(fs, "ghost") {
		t.Fatalf("sweep must be skipped on state read error (fail-open), want alpha+ghost present:\n%+v", fs)
	}
	if !strings.Contains(m.Snapshot(), "{function=ghost}") {
		t.Fatalf("ghost series must NOT be swept when the live set read fails:\n%s", m.Snapshot())
	}
	// Globals unchanged in the registry.
	if m.Counter("events_processed_total") != 42 {
		t.Fatalf("registry events = %d, want 42", m.Counter("events_processed_total"))
	}

	// Reopen on the same path (Open's schema init is idempotent) and flush; the
	// ghost/live behavior resumes: ghost (now stale) is swept, alpha survives.
	st2, err := state.Open(path)
	if err != nil {
		t.Fatalf("reopen state: %v", err)
	}
	defer st2.Close()
	recordSnapshots(context.Background(), st2, m)

	if fs := m.FunctionStatsSnapshot(); !metricsStat(fs, "alpha") {
		t.Fatalf("alpha must survive the resumed flush:\n%+v", fs)
	}
	if strings.Contains(m.Snapshot(), "function=ghost,") {
		t.Fatalf("ghost series must be swept once the live set read succeeds:\n%s", m.Snapshot())
	}
}

// metricsStat reports whether metrics.FunctionStats fs has a stat for name.
func metricsStat(fs []metrics.FunctionStat, name string) bool {
	for _, f := range fs {
		if f.Function == name {
			return true
		}
	}
	return false
}

// TestSnapshotStatsAndFuncSnapshotAreInMemoryOnly pins the hot-path contract:
// updates go to the Prometheus registry only and the registry reflects them
// immediately, while NO write reaches SQLite until recordSnapshots runs once.
func TestSnapshotStatsAndFuncSnapshotAreInMemoryOnly(t *testing.T) {
	m := metrics.New()
	// Give alpha/beta functions rows so their function_stats survive the flush's
	// orphan pruning (a function_stats row with no functions row is pruned).
	st := openTempState(t)
	st.RecordDiscovered(stateFunction("alpha", t.TempDir()))
	st.RecordDiscovered(stateFunction("beta", t.TempDir()))

	const loops = 500
	for i := 0; i < loops; i++ {
		m.Inc("events_processed_total")
		m.Inc("handler_success_total")
		m.Inc("handler_failure_total")
		m.Inc("retries_total")
		m.Inc("dlq_entries_total")
		m.IncLabels("function_events_total", []metrics.Label{{Name: "function", Value: "alpha"}})
		m.IncLabels("function_handler_success_total", []metrics.Label{{Name: "function", Value: "alpha"}})
		m.IncLabels("function_handler_failure_total", []metrics.Label{{Name: "function", Value: "beta"}})
		m.SetGauge("pending_entries", float64(i))
		m.SetGauge("pending_oldest_age_seconds", float64(i/2))
	}

	// No per-event writes: state DB has no stats row before any flush.
	if _, ok := st.Stats(); ok {
		t.Fatal("expected no stats row before any flush (hot path is in memory only)")
	}

	// A further 1000 increments must STILL not write to SQLite.
	for i := 0; i < 1000; i++ {
		m.Inc("events_processed_total")
	}
	if _, ok := st.Stats(); ok {
		t.Fatal("per-event activity must not write to SQLite")
	}

	// Registry reflects the exact totals immediately.
	const wantEvents = loops + 1000
	if got := m.Counter("events_processed_total"); got != wantEvents {
		t.Fatalf("events = %d, want %d", got, wantEvents)
	}
	if got := m.Counter("handler_success_total"); got != loops {
		t.Fatalf("success = %d, want %d", got, loops)
	}

	// A single recordSnapshots writes the absolute snapshot.
	recordSnapshots(context.Background(), st, m)
	gs, ok := st.Stats()
	if !ok {
		t.Fatal("expected stats row after recordSnapshots")
	}
	if gs.EventsProcessedTotal != wantEvents {
		t.Fatalf("persisted events = %d, want %d", gs.EventsProcessedTotal, wantEvents)
	}
	if gs.HandlerSuccessTotal != loops {
		t.Fatalf("persisted success = %d, want %d", gs.HandlerSuccessTotal, loops)
	}

	// Per-function rows persisted for alpha/beta.
	a, ok := st.FunctionStats("alpha")
	if !ok || a.EventsProcessedTotal != loops || a.HandlerSuccessTotal != loops {
		t.Fatalf("alpha = %+v, ok=%v; want events %d success %d", a, ok, loops, loops)
	}
	b, ok := st.FunctionStats("beta")
	if !ok || b.HandlerFailureTotal != loops {
		t.Fatalf("beta = %+v, ok=%v; want failure %d", b, ok, loops)
	}
}

// TestStatsLoopFlushesEveryInterval proves the fixed-cadence flush mirrors the
// in-memory absolute registry into SQLite repeatedly (not as deltas). The
// interval param exists solely for testability — production uses the
// statsFlushInterval constant (5s, see TestStatsFlushIntervalConstant).
func TestStatsLoopFlushesEveryInterval(t *testing.T) {
	st := openTempState(t)
	st.RecordDiscovered(stateFunction("alpha", t.TempDir()))
	m := metrics.New()
	m.Add("events_processed_total", 10)
	m.Add("handler_success_total", 7)
	m.IncLabels("function_events_total", []metrics.Label{{Name: "function", Value: "alpha"}})
	m.IncLabels("function_events_total", []metrics.Label{{Name: "function", Value: "alpha"}})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		statsLoop(ctx, m, st, 20*time.Millisecond)
	}()

	// Wait (bounded) for the first flush to persist a stats row while the loop
	// is still running, instead of a fixed sleep.
	waitForStatsRow(t, "statsLoop to persist first snapshot", func() bool {
		_, ok := st.Stats()
		return ok
	})

	snap := snapshotStats(m)
	gs, ok := st.Stats()
	if !ok {
		t.Fatal("expected stats row after statsLoop")
	}
	snap.UpdatedAt = gs.UpdatedAt
	if gs != snap {
		t.Fatalf("persisted stats = %+v, want snapshot %+v", gs, snap)
	}

	// Increment the registry once more and wait >= 2 ticks while the loop keeps
	// running; the persisted absolute total must track the new value (repeated
	// flushes track absolutes, not deltas).
	m.Inc("events_processed_total")
	waitForStatsRow(t, "statsLoop to flush the incremented value", func() bool {
		gs, ok := st.Stats()
		return ok && gs.EventsProcessedTotal == 11
	})
	gs, ok = st.Stats()
	if !ok {
		t.Fatal("expected stats row after second window")
	}
	if gs.EventsProcessedTotal != 11 {
		t.Fatalf("events = %d, want 11 after one more increment (absolute, not delta)", gs.EventsProcessedTotal)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("statsLoop did not stop on cancel")
	}
}

// TestFinalStatsFlushPersistsOnShutdown proves finalStatsFlush writes the final
// registry snapshot on graceful shutdown, and is nil-safe on both arguments.
func TestFinalStatsFlushPersistsOnShutdown(t *testing.T) {
	m := metrics.New()
	st := openTempState(t)
	st.RecordDiscovered(stateFunction("alpha", t.TempDir()))
	m.Add("events_processed_total", 42)
	m.Add("handler_success_total", 30)
	m.Add("handler_failure_total", 12)
	m.Add("retries_total", 4)
	m.Add("dlq_entries_total", 1)
	m.IncLabels("function_events_total", []metrics.Label{{Name: "function", Value: "alpha"}})
	m.IncLabels("function_handler_success_total", []metrics.Label{{Name: "function", Value: "alpha"}})

	finalStatsFlush(m, st)

	gs, ok := st.Stats()
	if !ok {
		t.Fatal("expected stats row after finalStatsFlush")
	}
	if gs.EventsProcessedTotal != 42 || gs.HandlerSuccessTotal != 30 || gs.HandlerFailureTotal != 12 || gs.RetryTotal != 4 || gs.DLQTotal != 1 {
		t.Fatalf("final global stats = %+v, want events 42 success 30 failure 12 retries 4 dlq 1", gs)
	}

	// Per-function stats match the registry snapshot.
	want := funcSnapshotStats(m)
	// Only alpha has activity.
	if len(want) != 1 || want[0].Function != "alpha" {
		t.Fatalf("funcSnapshotStats = %+v, want only alpha", want)
	}
	fa, ok := st.FunctionStats("alpha")
	if !ok {
		t.Fatal("expected alpha function stats after finalStatsFlush")
	}
	if fa.EventsProcessedTotal != want[0].EventsProcessedTotal || fa.HandlerSuccessTotal != want[0].HandlerSuccessTotal {
		t.Fatalf("alpha = %+v, want %+v", fa, want[0])
	}

	// Nil-safety: none of these may panic.
	finalStatsFlush(nil, st)
	finalStatsFlush(m, nil)
	finalStatsFlush(nil, nil)
}

// TestRecordSnapshotsIdempotent verifies recordSnapshots is idempotent: identical
// registry values produce identical DB totals (and refresh updated_at), and a
// later bump persists the new absolute value without delta accumulation.
func TestRecordSnapshotsIdempotent(t *testing.T) {
	st := openTempState(t)
	st.RecordDiscovered(stateFunction("alpha", t.TempDir()))
	m := metrics.New()
	m.Add("events_processed_total", 100)
	m.Add("handler_success_total", 90)
	m.IncLabels("function_events_total", []metrics.Label{{Name: "function", Value: "alpha"}})

	ctx := context.Background()
	recordSnapshots(ctx, st, m)
	first, ok := st.Stats()
	if !ok {
		t.Fatal("expected stats row after first snapshot")
	}

	recordSnapshots(ctx, st, m)
	second, ok := st.Stats()
	if !ok {
		t.Fatal("expected stats row after second snapshot")
	}
	if second.EventsProcessedTotal != 100 || second.HandlerSuccessTotal != 90 {
		t.Fatalf("second stats = %+v, want events 100 success 90 (no delta)", second)
	}
	if second.UpdatedAt < first.UpdatedAt {
		t.Fatalf("updated_at not refreshed: first=%q second=%q", first.UpdatedAt, second.UpdatedAt)
	}

	// Bump the registry; a single flush persists the new absolute value.
	m.Inc("events_processed_total")
	m.Inc("events_processed_total")
	recordSnapshots(ctx, st, m)
	gs, ok := st.Stats()
	if !ok {
		t.Fatal("expected stats row after bump flush")
	}
	if gs.EventsProcessedTotal != 102 {
		t.Fatalf("events = %d, want 102 (absolute, not 100+2 deltas accumulated)", gs.EventsProcessedTotal)
	}

	fa, ok := st.FunctionStats("alpha")
	if !ok || fa.EventsProcessedTotal != 1 {
		t.Fatalf("alpha = %+v, ok=%v; want events 1", fa, ok)
	}
}

// TestConcurrentIncrementsAndFlushRaceSafe exercises concurrent registry updates
// interleaved with concurrent flushes: the persisted global and per-function
// counters must equal the exact totals with no data race (-race).
func TestConcurrentIncrementsAndFlushRaceSafe(t *testing.T) {
	st := openTempState(t)
	st.RecordDiscovered(stateFunction("fn-a", t.TempDir()))
	st.RecordDiscovered(stateFunction("fn-b", t.TempDir()))
	m := metrics.New()

	ctx, cancel := context.WithCancel(context.Background())
	stopFlushes := make(chan struct{})

	var wg sync.WaitGroup
	// 8 writer goroutines, 200 iterations each. Each iteration adds one to every
	// global counter and attributions for fn-a and fn-b.
	const writers = 8
	const iters = 200
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				m.Inc("events_processed_total")
				m.Inc("handler_success_total")
				m.Inc("handler_failure_total")
				m.Inc("retries_total")
				m.Inc("dlq_entries_total")
				m.IncLabels("function_events_total", []metrics.Label{{Name: "function", Value: "fn-a"}})
				m.IncLabels("function_handler_success_total", []metrics.Label{{Name: "function", Value: "fn-a"}})
				m.IncLabels("function_events_total", []metrics.Label{{Name: "function", Value: "fn-b"}})
				m.IncLabels("function_handler_failure_total", []metrics.Label{{Name: "function", Value: "fn-b"}})
				m.SetGauge("pending_entries", float64(j))
			}
		}()
	}

	// 2 flush goroutines call recordSnapshots in a loop until the writers finish.
	var flushWG sync.WaitGroup
	flushWG.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer flushWG.Done()
			for {
				select {
				case <-stopFlushes:
					return
				default:
					recordSnapshots(ctx, st, m)
				}
			}
		}()
	}

	wg.Wait()
	close(stopFlushes)
	flushWG.Wait()
	cancel()

	// A final flush pins the exact totals.
	recordSnapshots(context.Background(), st, m)

	const total = writers * iters // 1600
	gs, ok := st.Stats()
	if !ok {
		t.Fatal("expected stats row after concurrent flushes")
	}
	if gs.EventsProcessedTotal != total {
		t.Fatalf("events = %d, want %d", gs.EventsProcessedTotal, total)
	}
	if gs.HandlerSuccessTotal != total || gs.HandlerFailureTotal != total {
		t.Fatalf("success/failure = %d/%d, want %d", gs.HandlerSuccessTotal, gs.HandlerFailureTotal, total)
	}
	if gs.RetryTotal != total || gs.DLQTotal != total {
		t.Fatalf("retries/dlq = %d/%d, want %d", gs.RetryTotal, gs.DLQTotal, total)
	}

	a, ok := st.FunctionStats("fn-a")
	if !ok || a.EventsProcessedTotal != total || a.HandlerSuccessTotal != total {
		t.Fatalf("fn-a = %+v, ok=%v; want events %d success %d", a, ok, total, total)
	}
	b, ok := st.FunctionStats("fn-b")
	if !ok || b.EventsProcessedTotal != total || b.HandlerFailureTotal != total {
		t.Fatalf("fn-b = %+v, ok=%v; want events %d failure %d", b, ok, total, total)
	}
}

// TestStatsFlushIntervalConstant guards the durability contract: the fixed SQLite
// snapshot cadence may not drift without this test failing, so nobody silently
// changes the flush interval.
func TestStatsFlushIntervalConstant(t *testing.T) {
	if statsFlushInterval != 5*time.Second {
		t.Fatalf("statsFlushInterval = %v, want 5s (durability contract)", statsFlushInterval)
	}
}

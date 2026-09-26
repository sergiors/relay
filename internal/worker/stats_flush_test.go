package worker

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/observability/metrics"
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
	st.RecordFunctionStats(state.FunctionStats{Function: "alpha", EventsMatchedTotal: 10})
	st.RecordFunctionStats(state.FunctionStats{Function: "ghost", EventsMatchedTotal: 5})

	m := metrics.New()
	m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "alpha"}})
	m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "alpha"}})
	m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "ghost"}})
	m.Add(metrics.MetricEventsMatched, 42)
	m.Add(metrics.MetricHandlerSuccess, 30)

	// Remove ghost's functions row (the reconciler's RecordRemoved) but KEEP its
	// series in the registry, simulating an in-flight invocation that recreated
	// them after removal. globals are untouched by removal.
	st.RecordRemoved("ghost")

	recordSnapshots(context.Background(), st, m, nil)

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
	if !ok || a.EventsMatchedTotal != 2 {
		t.Fatalf("alpha = %+v, ok=%v; want events 2 (registry value preserved)", a, ok)
	}
	if fs := m.FunctionStatsSnapshot(); !metricsStat(fs, "alpha") {
		t.Fatalf("alpha must remain in FunctionStatsSnapshot:\n%+v", fs)
	}

	// Global counters unchanged (registry values persisted to SQLite).
	gs, _ := st.Stats()
	if gs.EventsMatchedTotal != 42 {
		t.Fatalf("global events = %d, want 42", gs.EventsMatchedTotal)
	}
	if gs.HandlerSuccessTotal != 30 {
		t.Fatalf("global success = %d, want 30", gs.HandlerSuccessTotal)
	}
	if m.Counter(metrics.MetricEventsMatched) != 42 {
		t.Fatalf("registry events = %d, want 42", m.Counter(metrics.MetricEventsMatched))
	}
}

// TestReaddedFunctionPersistsFreshSeries verifies SQLite consistency on re-add:
// after a function's series are removed, re-incrementing starts at 1 and the
// flush persists that fresh small value (the row was recreated/upserted, not
// held at a stale value).
func TestReaddedFunctionPersistsFreshSeries(t *testing.T) {
	st := openTempState(t)
	st.RecordDiscovered(stateFunction("alpha", t.TempDir()))
	m := metrics.New()

	// Seed and flush an initial value, then remove the series as the reconciler
	// would on removal.
	m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "alpha"}})
	m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "alpha"}})
	recordSnapshots(context.Background(), st, m, nil)
	m.RemoveFunction("alpha")

	// Re-add: a fresh series at count 1.
	m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "alpha"}})
	if !strings.Contains(m.Snapshot(), "function_events_matched_total{function=alpha} count=1") {
		t.Fatalf("re-added alpha must count 1:\n%s", m.Snapshot())
	}
	recordSnapshots(context.Background(), st, m, nil)
	a, ok := st.FunctionStats("alpha")
	if !ok || a.EventsMatchedTotal != 1 {
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
	m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "alpha"}})
	m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "alpha"}})
	m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "ghost"}})
	m.Add(metrics.MetricEventsMatched, 42)

	// Close the DB so the pooled (single) connection is gone: FunctionNames and
	// RecordStatsSnapshot both error, but the sweep must fail open.
	if err := st.Close(); err != nil {
		t.Fatalf("close state: %v", err)
	}

	// Must not panic.
	recordSnapshots(context.Background(), st, m, nil)

	// Fail-open: BOTH alpha and ghost series survive the failed flush.
	if fs := m.FunctionStatsSnapshot(); !metricsStat(fs, "alpha") || !metricsStat(fs, "ghost") {
		t.Fatalf("sweep must be skipped on state read error (fail-open), want alpha+ghost present:\n%+v", fs)
	}
	if !strings.Contains(m.Snapshot(), "{function=ghost}") {
		t.Fatalf("ghost series must NOT be swept when the live set read fails:\n%s", m.Snapshot())
	}
	// Globals unchanged in the registry.
	if m.Counter(metrics.MetricEventsMatched) != 42 {
		t.Fatalf("registry events = %d, want 42", m.Counter(metrics.MetricEventsMatched))
	}

	// Reopen on the same path (Open's schema init is idempotent) and flush; the
	// ghost/live behavior resumes: ghost (now stale) is swept, alpha survives.
	st2, err := state.Open(path)
	if err != nil {
		t.Fatalf("reopen state: %v", err)
	}
	defer st2.Close()
	recordSnapshots(context.Background(), st2, m, nil)

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
		m.Inc(metrics.MetricEventsMatched)
		m.Inc(metrics.MetricHandlerSuccess)
		m.Inc(metrics.MetricHandlerFailure)
		m.Inc(metrics.MetricRetries)
		m.Inc(metrics.MetricDLQEntries)
		m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "alpha"}})
		m.IncLabels(metrics.MetricFunctionHandlerSuccess, []metrics.Label{{Name: "function", Value: "alpha"}})
		m.IncLabels(metrics.MetricFunctionHandlerFailure, []metrics.Label{{Name: "function", Value: "beta"}})
		m.SetGauge(metrics.MetricPendingEntries, float64(i))
		m.SetGauge(metrics.MetricPendingOldestAge, float64(i/2))
	}

	// No per-event writes: state DB has no stats row before any flush.
	if _, ok := st.Stats(); ok {
		t.Fatal("expected no stats row before any flush (hot path is in memory only)")
	}

	// A further 1000 increments must STILL not write to SQLite.
	for i := 0; i < 1000; i++ {
		m.Inc(metrics.MetricEventsMatched)
	}
	if _, ok := st.Stats(); ok {
		t.Fatal("per-event activity must not write to SQLite")
	}

	// Registry reflects the exact totals immediately.
	const wantEvents = loops + 1000
	if got := m.Counter(metrics.MetricEventsMatched); got != wantEvents {
		t.Fatalf("events = %d, want %d", got, wantEvents)
	}
	if got := m.Counter(metrics.MetricHandlerSuccess); got != loops {
		t.Fatalf("success = %d, want %d", got, loops)
	}

	// A single recordSnapshots writes the absolute snapshot.
	recordSnapshots(context.Background(), st, m, nil)
	gs, ok := st.Stats()
	if !ok {
		t.Fatal("expected stats row after recordSnapshots")
	}
	if gs.EventsMatchedTotal != wantEvents {
		t.Fatalf("persisted events = %d, want %d", gs.EventsMatchedTotal, wantEvents)
	}
	if gs.HandlerSuccessTotal != loops {
		t.Fatalf("persisted success = %d, want %d", gs.HandlerSuccessTotal, loops)
	}

	// Per-function rows persisted for alpha/beta.
	a, ok := st.FunctionStats("alpha")
	if !ok || a.EventsMatchedTotal != loops || a.HandlerSuccessTotal != loops {
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
	m.Add(metrics.MetricEventsMatched, 10)
	m.Add(metrics.MetricHandlerSuccess, 7)
	m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "alpha"}})
	m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "alpha"}})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		statsLoop(ctx, newStatsFlusher(st, m), 20*time.Millisecond)
	}()

	// Wait (bounded) for the first flush to persist a stats row while the loop
	// is still running, instead of a fixed sleep.
	waitForStatsRow(t, "statsLoop to persist first snapshot", func() bool {
		_, ok := st.Stats()
		return ok
	})

	snap := snapshotStats(m, nil)
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
	m.Inc(metrics.MetricEventsMatched)
	waitForStatsRow(t, "statsLoop to flush the incremented value", func() bool {
		gs, ok := st.Stats()
		return ok && gs.EventsMatchedTotal == 11
	})
	gs, ok = st.Stats()
	if !ok {
		t.Fatal("expected stats row after second window")
	}
	if gs.EventsMatchedTotal != 11 {
		t.Fatalf("events = %d, want 11 after one more increment (absolute, not delta)", gs.EventsMatchedTotal)
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
	m.Add(metrics.MetricEventsMatched, 42)
	m.Add(metrics.MetricHandlerSuccess, 30)
	m.Add(metrics.MetricHandlerFailure, 12)
	m.Add(metrics.MetricRetries, 4)
	m.Add(metrics.MetricDLQEntries, 1)
	m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "alpha"}})
	m.IncLabels(metrics.MetricFunctionHandlerSuccess, []metrics.Label{{Name: "function", Value: "alpha"}})

	finalStatsFlush(context.Background(), newStatsFlusher(st, m))

	gs, ok := st.Stats()
	if !ok {
		t.Fatal("expected stats row after finalStatsFlush")
	}
	if gs.EventsMatchedTotal != 42 || gs.HandlerSuccessTotal != 30 || gs.HandlerFailureTotal != 12 || gs.RetryTotal != 4 || gs.DLQTotal != 1 {
		t.Fatalf("final global stats = %+v, want events 42 success 30 failure 12 retries 4 dlq 1", gs)
	}

	// Per-function stats match the registry snapshot.
	want := snapshotFunctionStats(m, nil)
	// Only alpha has activity.
	if len(want) != 1 || want[0].Function != "alpha" {
		t.Fatalf("snapshotFunctionStats = %+v, want only alpha", want)
	}
	fa, ok := st.FunctionStats("alpha")
	if !ok {
		t.Fatal("expected alpha function stats after finalStatsFlush")
	}
	if fa.EventsMatchedTotal != want[0].EventsMatchedTotal || fa.HandlerSuccessTotal != want[0].HandlerSuccessTotal {
		t.Fatalf("alpha = %+v, want %+v", fa, want[0])
	}

	// Nil-safety: none of these may panic.
	finalStatsFlush(context.Background(), newStatsFlusher(st, nil))
	finalStatsFlush(context.Background(), newStatsFlusher(nil, m))
	finalStatsFlush(context.Background(), nil)
}

// TestRecordSnapshotsIdempotent verifies recordSnapshots is idempotent: identical
// registry values produce identical DB totals (and refresh updated_at), and a
// later bump persists the new absolute value without delta accumulation.
func TestRecordSnapshotsIdempotent(t *testing.T) {
	st := openTempState(t)
	st.RecordDiscovered(stateFunction("alpha", t.TempDir()))
	m := metrics.New()
	m.Add(metrics.MetricEventsMatched, 100)
	m.Add(metrics.MetricHandlerSuccess, 90)
	m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "alpha"}})

	ctx := context.Background()
	recordSnapshots(ctx, st, m, nil)
	first, ok := st.Stats()
	if !ok {
		t.Fatal("expected stats row after first snapshot")
	}

	recordSnapshots(ctx, st, m, nil)
	second, ok := st.Stats()
	if !ok {
		t.Fatal("expected stats row after second snapshot")
	}
	if second.EventsMatchedTotal != 100 || second.HandlerSuccessTotal != 90 {
		t.Fatalf("second stats = %+v, want events 100 success 90 (no delta)", second)
	}
	if second.UpdatedAt < first.UpdatedAt {
		t.Fatalf("updated_at not refreshed: first=%q second=%q", first.UpdatedAt, second.UpdatedAt)
	}

	// Bump the registry; a single flush persists the new absolute value.
	m.Inc(metrics.MetricEventsMatched)
	m.Inc(metrics.MetricEventsMatched)
	recordSnapshots(ctx, st, m, nil)
	gs, ok := st.Stats()
	if !ok {
		t.Fatal("expected stats row after bump flush")
	}
	if gs.EventsMatchedTotal != 102 {
		t.Fatalf("events = %d, want 102 (absolute, not 100+2 deltas accumulated)", gs.EventsMatchedTotal)
	}

	fa, ok := st.FunctionStats("alpha")
	if !ok || fa.EventsMatchedTotal != 1 {
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
				m.Inc(metrics.MetricEventsMatched)
				m.Inc(metrics.MetricHandlerSuccess)
				m.Inc(metrics.MetricHandlerFailure)
				m.Inc(metrics.MetricRetries)
				m.Inc(metrics.MetricDLQEntries)
				m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "fn-a"}})
				m.IncLabels(metrics.MetricFunctionHandlerSuccess, []metrics.Label{{Name: "function", Value: "fn-a"}})
				m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "fn-b"}})
				m.IncLabels(metrics.MetricFunctionHandlerFailure, []metrics.Label{{Name: "function", Value: "fn-b"}})
				m.SetGauge(metrics.MetricPendingEntries, float64(j))
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
					recordSnapshots(ctx, st, m, nil)
				}
			}
		}()
	}

	wg.Wait()
	close(stopFlushes)
	flushWG.Wait()
	cancel()

	// A final flush pins the exact totals.
	recordSnapshots(context.Background(), st, m, nil)

	const total = writers * iters // 1600
	gs, ok := st.Stats()
	if !ok {
		t.Fatal("expected stats row after concurrent flushes")
	}
	if gs.EventsMatchedTotal != total {
		t.Fatalf("events = %d, want %d", gs.EventsMatchedTotal, total)
	}
	if gs.HandlerSuccessTotal != total || gs.HandlerFailureTotal != total {
		t.Fatalf("success/failure = %d/%d, want %d", gs.HandlerSuccessTotal, gs.HandlerFailureTotal, total)
	}
	if gs.RetryTotal != total || gs.DLQTotal != total {
		t.Fatalf("retries/dlq = %d/%d, want %d", gs.RetryTotal, gs.DLQTotal, total)
	}

	a, ok := st.FunctionStats("fn-a")
	if !ok || a.EventsMatchedTotal != total || a.HandlerSuccessTotal != total {
		t.Fatalf("fn-a = %+v, ok=%v; want events %d success %d", a, ok, total, total)
	}
	b, ok := st.FunctionStats("fn-b")
	if !ok || b.EventsMatchedTotal != total || b.HandlerFailureTotal != total {
		t.Fatalf("fn-b = %+v, ok=%v; want events %d failure %d", b, ok, total, total)
	}
}

// TestStatsFlusherResetPersistsZero verifies the flusher's ResetStats resets
// both the in-memory source and the persisted rows, and that a subsequent flush
// writes the post-reset totals (never resurrecting the pre-reset values), while
// the Prometheus counters stay monotonic.
func TestStatsFlusherResetPersistsZero(t *testing.T) {
	st := openTempState(t)
	st.RecordDiscovered(stateFunction("alpha", t.TempDir()))
	m := metrics.New()
	m.Add(metrics.MetricEventsMatched, 100)
	m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "alpha"}})
	f := newStatsFlusher(st, m)

	f.flush(context.Background())
	if gs, _ := st.Stats(); gs.EventsMatchedTotal != 100 {
		t.Fatalf("pre-reset persisted events = %d, want 100", gs.EventsMatchedTotal)
	}

	f.ResetStats()

	// Persisted rows are zero immediately.
	if gs, _ := st.Stats(); gs.EventsMatchedTotal != 0 {
		t.Fatalf("persisted events after reset = %d, want 0", gs.EventsMatchedTotal)
	}
	if a, _ := st.FunctionStats("alpha"); a.EventsMatchedTotal != 0 {
		t.Fatalf("persisted function events after reset = %d, want 0", a.EventsMatchedTotal)
	}
	// Prometheus stays monotonic.
	if got := m.Counter(metrics.MetricEventsMatched); got != 100 {
		t.Fatalf("Prometheus events after reset = %d, want 100 (monotonic)", got)
	}

	// A flush right after the reset must write zero, not the pre-reset value.
	f.flush(context.Background())
	if gs, _ := st.Stats(); gs.EventsMatchedTotal != 0 {
		t.Fatalf("flush after reset resurrected events = %d, want 0", gs.EventsMatchedTotal)
	}

	// Post-reset activity accumulates from zero.
	m.Add(metrics.MetricEventsMatched, 5)
	m.AddLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "alpha"}}, 2)
	f.flush(context.Background())
	if gs, _ := st.Stats(); gs.EventsMatchedTotal != 5 {
		t.Fatalf("persisted events after post-reset activity = %d, want 5", gs.EventsMatchedTotal)
	}
	if a, _ := st.FunctionStats("alpha"); a.EventsMatchedTotal != 2 {
		t.Fatalf("persisted function events = %d, want 2", a.EventsMatchedTotal)
	}
	if got := m.Counter(metrics.MetricEventsMatched); got != 105 {
		t.Fatalf("Prometheus events = %d, want 105", got)
	}
}

// TestStatsFlusherResetRaceNoResurrection is the concurrency invariant: a flush
// running concurrently with ResetStats can never persist a pre-reset total after
// the reset completes, because flush and ResetStats share the flusher mutex.
func TestStatsFlusherResetRaceNoResurrection(t *testing.T) {
	st := openTempState(t)
	st.RecordDiscovered(stateFunction("alpha", t.TempDir()))
	m := metrics.New()
	m.Add(metrics.MetricEventsMatched, 1000)
	m.AddLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "alpha"}}, 10)
	f := newStatsFlusher(st, m)

	// A flush loop racing the reset.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				f.flush(context.Background())
			}
		}
	}()

	f.ResetStats()

	// From the moment reset returns, no flush may resurrect a pre-reset value.
	// Poll while the flush loop is still running; every observed value must be
	// the post-reset zero.
	for i := 0; i < 200; i++ {
		gs, ok := st.Stats()
		if !ok {
			continue
		}
		if gs.EventsMatchedTotal != 0 {
			close(stop)
			wg.Wait()
			t.Fatalf("a concurrent flush resurrected pre-reset events: %d", gs.EventsMatchedTotal)
		}
		time.Sleep(time.Millisecond)
	}

	close(stop)
	wg.Wait()

	// Final flush: still zero (no post-reset activity yet).
	f.flush(context.Background())
	if gs, _ := st.Stats(); gs.EventsMatchedTotal != 0 {
		t.Fatalf("final persisted events = %d, want 0", gs.EventsMatchedTotal)
	}
	// Prometheus never went backwards.
	if got := m.Counter(metrics.MetricEventsMatched); got != 1000 {
		t.Fatalf("Prometheus events = %d, want 1000 (monotonic)", got)
	}
}

// TestStatsFlusherResetWithoutStateErrors verifies a worker whose state handle
// is unavailable reports an error (so the socket answers stats_unavailable and
// the CLI falls back to the state DB), while still resetting the in-memory
// baseline.
func TestStatsFlusherResetWithoutStateErrors(t *testing.T) {
	m := metrics.New()
	m.Add(metrics.MetricEventsMatched, 42)
	f := newStatsFlusher(nil, m)

	if err := f.ResetStats(); err == nil {
		t.Fatal("ResetStats without a state handle: err = nil, want error")
	}
	// The worker-owned baseline was still captured: the raw counter is
	// unchanged (Prometheus stays monotonic), but the baselined snapshot is
	// zero.
	if got := m.Counter(metrics.MetricEventsMatched); got != 42 {
		t.Fatalf("Prometheus events after reset = %d, want 42 (monotonic)", got)
	}
	if got := f.baseline.counter(metrics.MetricEventsMatched, m.Counter(metrics.MetricEventsMatched)); got != 0 {
		t.Fatalf("baselined events after reset = %d, want 0", got)
	}
}

// TestRelayBaselineSuppressesPreResetTimestamps pins the timestamp half of the
// worker-owned reset baseline: timestamps are last-observed, not cumulative, so
// a value not advanced since the reset must be reported as "never observed",
// while a post-reset execution (a newer timestamp) resumes reporting. Unlike a
// cumulative counter, subtraction cannot express this.
func TestRelayBaselineSuppressesPreResetTimestamps(t *testing.T) {
	m := metrics.New()
	m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "alpha"}})
	m.SetFunctionTimestamp("alpha", metrics.FunctionTimestampExecution, 1700000000)
	base := captureRelayBaseline(m)

	// The pre-reset timestamp is suppressed to zero.
	got := snapshotFunctionStats(m, &base)
	if len(got) != 1 || got[0].LastExecutionAt != "" {
		t.Fatalf("pre-reset timestamp not suppressed: %+v", got)
	}

	// A post-reset execution advances the timestamp past the baseline and is
	// reported again (as RFC3339), together with the post-reset counter value.
	m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "alpha"}})
	m.SetFunctionTimestamp("alpha", metrics.FunctionTimestampExecution, 1700000100)
	got = snapshotFunctionStats(m, &base)
	if len(got) != 1 || got[0].EventsMatchedTotal != 1 {
		t.Fatalf("post-reset counters = %+v, want events 1", got)
	}
	if got[0].LastExecutionAt != unixSecToRFC3339(1700000100) {
		t.Fatalf("post-reset timestamp = %q, want %q", got[0].LastExecutionAt, unixSecToRFC3339(1700000100))
	}
}

// TestStatsFlusherDropFunctionBaseline verifies the removal hook: after a reset,
// dropping a function's baseline entry lets a re-added function report its fresh
// series value instead of a negative (fresh minus stale pre-removal total).
func TestStatsFlusherDropFunctionBaseline(t *testing.T) {
	m := metrics.New()
	m.AddLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "alpha"}}, 10)
	f := newStatsFlusher(nil, m)
	_ = f.ResetStats() // capture baseline; nil state is fine for this test

	// A fresh series (simulating the registry's removal + re-add) at 1 must be
	// offset by the stale baseline (10) until the hook drops it.
	m.RemoveFunction("alpha")
	m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: "alpha"}})
	if got := snapshotFunctionStats(m, &f.baseline); got[0].EventsMatchedTotal != -9 {
		t.Fatalf("without the removal hook, events = %d, want -9 (stale baseline)", got[0].EventsMatchedTotal)
	}

	f.dropFunctionBaseline("alpha")
	if got := snapshotFunctionStats(m, &f.baseline); got[0].EventsMatchedTotal != 1 {
		t.Fatalf("after the removal hook, events = %d, want 1", got[0].EventsMatchedTotal)
	}
}

// TestStatsFlusherNilSafe verifies a nil flusher never panics.
func TestStatsFlusherNilSafe(t *testing.T) {
	var f *statsFlusher
	f.flush(context.Background())
	_ = f.ResetStats()
}

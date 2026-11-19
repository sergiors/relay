package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"relay/internal/observability/metrics"
	"relay/internal/state"
)

// TestStatsFlusherResetWithoutStateErrors verifies a worker whose state handle
// is unavailable reports an error (so the socket answers stats_reset_failed and
// the CLI surfaces it instead of masking it) AND leaves the effective baseline
// untouched: no reset happened, so the persisted snapshot must keep reporting
// the original counters rather than a reset point that was never committed.
func TestStatsFlusherResetWithoutStateErrors(t *testing.T) {
	m := metrics.New()
	m.Add(metrics.MetricEventsMatched, 42)
	f := newStatsFlusher(nil, m, state.StatsGenerations{
		Global: state.StatsGenerationUnknown,
		Node:   state.StatsGenerationUnknown,
	})

	if err := f.ResetStats(); err == nil {
		t.Fatal("ResetStats without a state handle: err = nil, want error")
	}
	// Prometheus stays monotonic and the baseline was NOT captured: the
	// baselined snapshot still reports the full raw counter.
	if got := m.Counter(metrics.MetricEventsMatched); got != 42 {
		t.Fatalf("Prometheus events after failed reset = %d, want 42 (monotonic)", got)
	}
	if got := f.baseline.counter(metrics.MetricEventsMatched, m.Counter(metrics.MetricEventsMatched)); got != 42 {
		t.Fatalf("baselined events after failed reset = %d, want 42 (baseline untouched)", got)
	}
	// The failed reset must not adopt any epoch either.
	if f.generation != state.StatsGenerationUnknown || f.nodeGeneration != state.StatsGenerationUnknown {
		t.Fatalf("epochs after failed reset = (%d, %d), want both unknown",
			f.generation, f.nodeGeneration)
	}
}

// TestRelayBaselineSuppressesPreResetTimestamps pins the timestamp half of the
// worker-owned reset baseline: timestamps are last-observed, not cumulative, so
// a value not advanced since the reset must be reported as "never observed",
// while a post-reset execution (a newer timestamp) resumes reporting. Unlike a
// cumulative counter, subtraction cannot express this.
func TestRelayBaselineSuppressesPreResetTimestamps(t *testing.T) {
	m := metrics.New()
	m.IncLabels(metrics.MetricAppEventsMatched, []metrics.Label{{Name: "app", Value: "alpha"}})
	m.SetAppTimestamp("alpha", metrics.AppTimestampExecution, 1700000000)
	base := captureRelayBaseline(m)

	// The pre-reset timestamp is suppressed to zero.
	got := snapshotAppStats(m, &base)
	if len(got) != 1 || got[0].LastExecutionAt != "" {
		t.Fatalf("pre-reset timestamp not suppressed: %+v", got)
	}

	// A post-reset execution advances the timestamp past the baseline and is
	// reported again (as RFC3339), together with the post-reset counter value.
	m.IncLabels(metrics.MetricAppEventsMatched, []metrics.Label{{Name: "app", Value: "alpha"}})
	m.SetAppTimestamp("alpha", metrics.AppTimestampExecution, 1700000100)
	got = snapshotAppStats(m, &base)
	if len(got) != 1 || got[0].EventsMatchedTotal != 1 {
		t.Fatalf("post-reset counters = %+v, want events 1", got)
	}
	if got[0].LastExecutionAt != unixSecToRFC3339(1700000100) {
		t.Fatalf("post-reset timestamp = %q, want %q", got[0].LastExecutionAt, unixSecToRFC3339(1700000100))
	}
}

// TestStatsFlusherDropAppBaseline verifies the removal hook: after a reset,
// dropping an app's baseline entry lets a re-added app report its fresh
// series value instead of a negative (fresh minus stale pre-removal total).
func TestStatsFlusherDropAppBaseline(t *testing.T) {
	m := metrics.New()
	m.AddLabels(metrics.MetricAppEventsMatched, []metrics.Label{{Name: "app", Value: "alpha"}}, 10)
	f := newStatsFlusher(nil, m, state.StatsGenerations{
		Global: state.StatsGenerationUnknown,
		Node:   state.StatsGenerationUnknown,
	})
	// Install the reset baseline directly: this unit test has no state DB, and
	// the baseline only ever lands after a SUCCESSFUL state reset (see
	// ResetStats), which the nil handle can never provide.
	f.baseline = captureRelayBaseline(m)

	// A fresh series (simulating the registry's removal + re-add) at 1 must be
	// offset by the stale baseline (10) until the hook drops it.
	m.RemoveApp("alpha")
	m.IncLabels(metrics.MetricAppEventsMatched, []metrics.Label{{Name: "app", Value: "alpha"}})
	if got := snapshotAppStats(m, &f.baseline); got[0].EventsMatchedTotal != -9 {
		t.Fatalf("without the removal hook, events = %d, want -9 (stale baseline)", got[0].EventsMatchedTotal)
	}

	f.dropAppBaseline("alpha")
	if got := snapshotAppStats(m, &f.baseline); got[0].EventsMatchedTotal != 1 {
		t.Fatalf("after the removal hook, events = %d, want 1", got[0].EventsMatchedTotal)
	}
}

// TestStatsFlusherNilSafe verifies a nil flusher never panics.
func TestStatsFlusherNilSafe(t *testing.T) {
	var f *statsFlusher
	f.flush(context.Background())
	_ = f.ResetStats()
}

// fakeStatsStore is the statsStore seam: it records every flush/reset
// interaction and returns configured outcomes so the flusher's fenced write
// contract runs without PostgreSQL. Without a recordHook, a snapshot write is
// accepted and the supplied generations are echoed back.
type fakeStatsStore struct {
	appNames   []string
	appNamesOK bool

	// recordHook, when set, decides each snapshot write's outcome in call
	// order; nil means accept and echo the supplied generations.
	recordHook func(generations state.StatsGenerations) (state.StatsGenerations, error)

	recordCalls    int
	recordGens     []state.StatsGenerations
	recordStats    state.Stats
	recordAppStats []state.AppStats

	pruneCalls     int
	pruneRetention time.Duration

	resetNodeCalls int
	resetNodeID    string
	resetNodeGen   int64
	resetNodeErr   error

	resetAllCalls int
	resetAllGen   int64
	resetAllErr   error

	nodeID string
}

func (f *fakeStatsStore) AppNames(context.Context) ([]string, bool) {
	return f.appNames, f.appNamesOK
}

func (f *fakeStatsStore) RecordStatsSnapshotWithGenerations(
	_ context.Context,
	generations state.StatsGenerations,
	stats state.Stats,
	fns []state.AppStats,
) (state.StatsGenerations, error) {
	f.recordCalls++
	f.recordGens = append(f.recordGens, generations)
	f.recordStats = stats
	f.recordAppStats = fns
	if f.recordHook != nil {
		return f.recordHook(generations)
	}
	return generations, nil
}

func (f *fakeStatsStore) PruneStaleStats(
	_ context.Context, retention time.Duration,
) (int, error) {
	f.pruneCalls++
	f.pruneRetention = retention
	return 0, nil
}

func (f *fakeStatsStore) ResetNodeStats(
	_ context.Context, nodeID string,
) (int64, error) {
	f.resetNodeCalls++
	f.resetNodeID = nodeID
	return f.resetNodeGen, f.resetNodeErr
}

func (f *fakeStatsStore) ResetAllStats(context.Context) (int64, error) {
	f.resetAllCalls++
	return f.resetAllGen, f.resetAllErr
}

func (f *fakeStatsStore) NodeID() string { return f.nodeID }

// TestStatsFlusherFlushWritesSuppliedGenerationsAndAbsoluteSnapshots pins the
// flush seam over a fake statsStore: the epochs handed to newStatsFlusher are
// the ones the first flush writes under, the snapshot carries the registry's
// absolute values, and the live app set sweeps removed apps before the write.
func TestStatsFlusherFlushWritesSuppliedGenerationsAndAbsoluteSnapshots(t *testing.T) {
	// given a registry with global counters, a backlog gauge, and two apps of
	// which only alpha is still live
	m := metrics.New()
	m.Add(metrics.MetricEventsReceived, 150)
	m.Add(metrics.MetricEventsMatched, 10)
	m.Add(metrics.MetricEventsUnmatched, 5)
	m.Add(metrics.MetricHandlerSuccess, 7)
	m.Add(metrics.MetricHandlerFailure, 3)
	m.Add(metrics.MetricRetries, 2)
	m.Add(metrics.MetricDLQEntries, 1)
	m.SetGauge(metrics.MetricPendingEntries, 4.9)
	m.AddLabels(metrics.MetricAppEventsMatched,
		[]metrics.Label{{Name: "app", Value: "alpha"}}, 5)
	m.AddLabels(metrics.MetricAppEventsMatched,
		[]metrics.Label{{Name: "app", Value: "beta"}}, 9)

	store := &fakeStatsStore{appNames: []string{"alpha"}, appNamesOK: true}
	f := newStatsFlusher(store, m, state.StatsGenerations{Global: 7, Node: 3})
	f.retention = time.Minute

	// when one flush runs
	f.flush(context.Background())

	// then the write happened under the supplied generations
	if store.recordCalls != 1 {
		t.Fatalf("snapshot writes = %d, want 1", store.recordCalls)
	}
	wantGens := state.StatsGenerations{Global: 7, Node: 3}
	if store.recordGens[0] != wantGens {
		t.Fatalf("write generations = %+v, want the supplied %+v",
			store.recordGens[0], wantGens)
	}
	if f.generation != 7 || f.nodeGeneration != 3 {
		t.Fatalf("flusher epochs = (%d, %d), want the echoed (7, 3)",
			f.generation, f.nodeGeneration)
	}

	// and the snapshot carries the absolute registry values
	wantStats := state.Stats{
		EventsReceivedTotal:  150,
		EventsMatchedTotal:   10,
		EventsUnmatchedTotal: 5,
		HandlerSuccessTotal:  7,
		HandlerFailureTotal:  3,
		RetryTotal:           2,
		DLQTotal:             1,
		PendingEntries:       4,
	}
	if store.recordStats != wantStats {
		t.Fatalf("snapshot = %+v, want %+v", store.recordStats, wantStats)
	}
	if len(store.recordAppStats) != 1 || store.recordAppStats[0].App != "alpha" {
		t.Fatalf("app snapshot = %+v, want only the live app alpha",
			store.recordAppStats)
	}
	if store.recordAppStats[0].EventsMatchedTotal != 5 {
		t.Fatalf("alpha events = %d, want 5",
			store.recordAppStats[0].EventsMatchedTotal)
	}

	// and the sweep against the live set removed beta before the write, while
	// compaction ran once under the configured retention
	if got := snapshotAppStats(m, nil); len(got) != 1 || got[0].App != "alpha" {
		t.Fatalf("registry apps after sweep = %+v, want only alpha", got)
	}
	if store.pruneCalls != 1 || store.pruneRetention != time.Minute {
		t.Fatalf("prune = %d/%v, want one attempt at 1m",
			store.pruneCalls, store.pruneRetention)
	}
}

// TestStatsFlusherFlushSkipsSweepWhenLiveSetUnavailable pins the fail-open
// seam: an unreadable live app set must not be treated as an empty set (which
// would delete every app-scoped series), and the global snapshot still writes.
func TestStatsFlusherFlushSkipsSweepWhenLiveSetUnavailable(t *testing.T) {
	// given a registry whose only live app cannot be confirmed
	m := metrics.New()
	m.AddLabels(metrics.MetricAppEventsMatched,
		[]metrics.Label{{Name: "app", Value: "alpha"}}, 5)
	store := &fakeStatsStore{}

	// when a flush runs against an unavailable live set
	f := newStatsFlusher(store, m, state.StatsGenerations{
		Global: state.StatsGenerationUnknown,
		Node:   state.StatsGenerationUnknown,
	})
	f.flush(context.Background())

	// then the snapshot still writes and no app series was swept away
	if store.recordCalls != 1 {
		t.Fatalf("snapshot writes = %d, want 1 despite the unavailable set",
			store.recordCalls)
	}
	if got := snapshotAppStats(m, nil); len(got) != 1 || got[0].App != "alpha" {
		t.Fatalf("registry apps after flush = %+v, want alpha retained "+
			"(an unknown live set must not sweep)", got)
	}
}

// TestStatsFlusherResetSuccessAdoptsEpochAndInstallsBaseline pins the reset
// seam: a successful node reset adopts the store's node epoch and installs the
// subtraction baseline under the same flusher, while a global reset adopts the
// shared epoch without disturbing the node epoch.
func TestStatsFlusherResetSuccessAdoptsEpochAndInstallsBaseline(t *testing.T) {
	// given a store that reports new epochs for both reset scopes
	store := &fakeStatsStore{
		nodeID:       "node-a",
		resetNodeGen: 9,
		resetAllGen:  12,
	}
	m := metrics.New()
	m.Add(metrics.MetricEventsMatched, 40)
	f := newStatsFlusher(store, m, state.StatsGenerations{Global: 5, Node: 1})

	// when the node reset succeeds
	if err := f.ResetStats(); err != nil {
		t.Fatalf("ResetStats: %v", err)
	}

	// then the store saw this node's identity, the node epoch advanced, and
	// the baseline now offsets the pre-reset total
	if store.resetNodeCalls != 1 || store.resetNodeID != "node-a" {
		t.Fatalf("node reset = %d calls for %q, want one for node-a",
			store.resetNodeCalls, store.resetNodeID)
	}
	if f.nodeGeneration != 9 {
		t.Fatalf("node epoch after reset = %d, want the adopted 9",
			f.nodeGeneration)
	}
	if f.generation != 5 {
		t.Fatalf("shared epoch after node reset = %d, want untouched 5",
			f.generation)
	}
	raw := m.Counter(metrics.MetricEventsMatched)
	if got := f.baseline.counter(metrics.MetricEventsMatched, raw); got != 0 {
		t.Fatalf("baselined events after reset = %d, want 0 (baseline offsets 40)", got)
	}

	// when the global reset succeeds
	if err := f.ResetAllStats(); err != nil {
		t.Fatalf("ResetAllStats: %v", err)
	}

	// then the shared epoch advances, the node epoch stays put, and the
	// control-socket identity seam reports the store's node
	if store.resetAllCalls != 1 {
		t.Fatalf("global reset calls = %d, want 1", store.resetAllCalls)
	}
	if f.generation != 12 {
		t.Fatalf("shared epoch after global reset = %d, want the adopted 12",
			f.generation)
	}
	if f.nodeGeneration != 9 {
		t.Fatalf("node epoch after global reset = %d, want untouched 9",
			f.nodeGeneration)
	}
	if got := f.NodeID(); got != "node-a" {
		t.Fatalf("NodeID = %q, want node-a", got)
	}
}

// TestStatsFlusherResetFailureKeepsEpochAndBaseline pins the failure half: a
// refused reset reports the error and leaves both epochs and the effective
// baseline exactly as they were, so no reset point is fabricated.
func TestStatsFlusherResetFailureKeepsEpochAndBaseline(t *testing.T) {
	// given a store that refuses both reset scopes
	store := &fakeStatsStore{
		nodeID:       "node-a",
		resetNodeErr: errors.New("node reset refused"),
		resetAllErr:  errors.New("global reset refused"),
	}
	m := metrics.New()
	m.Add(metrics.MetricEventsMatched, 42)
	f := newStatsFlusher(store, m, state.StatsGenerations{Global: 5, Node: 1})

	// when the node reset fails
	if err := f.ResetStats(); err == nil {
		t.Fatal("ResetStats against a refused reset: err = nil, want error")
	}

	// then the epochs and baseline are untouched
	if f.nodeGeneration != 1 || f.generation != 5 {
		t.Fatalf("epochs after failed reset = (%d, %d), want (1, 5)",
			f.generation, f.nodeGeneration)
	}
	raw := m.Counter(metrics.MetricEventsMatched)
	if got := f.baseline.counter(metrics.MetricEventsMatched, raw); got != 42 {
		t.Fatalf("baselined events after failed reset = %d, want 42 "+
			"(no reset point fabricated)", got)
	}

	// when the global reset fails
	if err := f.ResetAllStats(); err == nil {
		t.Fatal("ResetAllStats against a refused reset: err = nil, want error")
	}

	// then the shared epoch is still untouched
	if f.generation != 5 {
		t.Fatalf("shared epoch after failed global reset = %d, want 5",
			f.generation)
	}
}

// TestStatsFlusherRebasesOnceWhenResetAdvanced pins the fenced-retry contract
// over the fake store: a reset that advanced the epochs between construction
// and the first flush fences the write once, the flusher rebases to the
// registry's current values under the new epochs, and the retried snapshot
// never resurrects the pre-reset totals.
func TestStatsFlusherRebasesOnceWhenResetAdvanced(t *testing.T) {
	// given a store whose first write is fenced by an observed reset
	store := &fakeStatsStore{}
	fenced := true
	store.recordHook = func(
		generations state.StatsGenerations,
	) (state.StatsGenerations, error) {
		if fenced {
			fenced = false
			return state.StatsGenerations{Global: 6, Node: 2},
				state.ErrStatsResetAdvanced
		}
		return generations, nil
	}
	m := metrics.New()
	m.Add(metrics.MetricEventsMatched, 40)
	f := newStatsFlusher(store, m, state.StatsGenerations{Global: 5, Node: 1})

	// when one flush runs
	f.flush(context.Background())

	// then the first attempt used the supplied epochs and the single retry
	// used the advanced ones
	if store.recordCalls != 2 {
		t.Fatalf("snapshot writes = %d, want 2 (fence then retry)", store.recordCalls)
	}
	if store.recordGens[0] != (state.StatsGenerations{Global: 5, Node: 1}) {
		t.Fatalf("first write generations = %+v, want (5, 1)",
			store.recordGens[0])
	}
	if store.recordGens[1] != (state.StatsGenerations{Global: 6, Node: 2}) {
		t.Fatalf("retry generations = %+v, want the advanced (6, 2)",
			store.recordGens[1])
	}
	if f.generation != 6 || f.nodeGeneration != 2 {
		t.Fatalf("flusher epochs = (%d, %d), want the adopted (6, 2)",
			f.generation, f.nodeGeneration)
	}

	// and the rebased snapshot reports zeroed totals instead of the
	// pre-reset 40
	if store.recordStats.EventsMatchedTotal != 0 {
		t.Fatalf("rebased events = %d, want 0 (pre-reset totals not resurrected)",
			store.recordStats.EventsMatchedTotal)
	}
}

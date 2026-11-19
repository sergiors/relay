//go:build integration

package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"relay/internal/observability/metrics"
	"relay/internal/state"
	"relay/internal/testutil"
)

// This file pins the worker-side reset behavior against a real state
// database: a reset whose state transaction FAILS must leave the subtraction
// baseline and epochs untouched (a reset that never committed must never zero
// the persisted counters), while a successful local reset scopes to the
// worker's own node and a successful global reset covers every node — both
// adopting their new epoch under the same mutex the flush holds.

// TestStatsFlusherFailedResetKeepsBaseline is the regression for the reset
// baseline: a state reset that fails its TRANSACTION (a corrupt stored payload
// makes the node reset's decode fail and roll the whole reset back) must leave
// the effective baseline untouched, so a subsequent successful flush preserves
// the ORIGINAL counters instead of persisting a reset that never happened.
// Restoring a valid payload afterwards must not lose counts either.
func TestStatsFlusherFailedResetKeepsBaseline(t *testing.T) {
	ctx := context.Background()
	st, dsn := openTempWorkerNodeState(t)
	m := metrics.New()
	m.Add(metrics.MetricEventsMatched, 42)
	f := newStatsFlusher(st, m, state.StatsGenerations{
		Global: state.StatsGenerationUnknown,
		Node:   state.StatsGenerationUnknown,
	})
	f.flush(ctx)
	if s, ok := st.Stats(); !ok || s.EventsMatchedTotal != 42 {
		t.Fatalf("pre-reset persisted events = %+v ok=%v, want 42", s, ok)
	}
	before, err := st.StatsGenerations()
	if err != nil {
		t.Fatalf("StatsGenerations: %v", err)
	}

	// Plant a wrong-shape payload on this node's row: the reset decodes it
	// inside its transaction, fails, and rolls back — a real transaction
	// failure, not a pre-flight rejection.
	raw := newRawStateConn(t, dsn)
	if _, err := raw.ExecContext(ctx,
		`UPDATE stats SET data = $2 WHERE node_id = $1`,
		st.NodeID(), `"not-an-object"`); err != nil {
		t.Fatalf("plant corrupt payload: %v", err)
	}

	if err := f.ResetStats(); err == nil {
		t.Fatal("ResetStats over a corrupt payload: err = nil, want the transaction failure")
	}

	// The failed reset rolled back: neither epoch moved, and the baseline was
	// NOT captured (the next flush must still write the original counters).
	after, err := st.StatsGenerations()
	if err != nil {
		t.Fatalf("StatsGenerations after failed reset: %v", err)
	}
	if after != before {
		t.Fatalf("epochs after failed reset = %+v, want %+v (rollback)", after, before)
	}

	// A valid payload is restored for the row (the operator's fix), and the
	// next successful flush preserves the original counters.
	st.RecordStats(state.Stats{EventsMatchedTotal: 42})
	f.flush(ctx)
	if s, ok := st.Stats(); !ok || s.EventsMatchedTotal != 42 {
		t.Fatalf("persisted events after the failed reset = %+v ok=%v, want 42 (baseline untouched)",
			s, ok)
	}
	if got := m.Counter(metrics.MetricEventsMatched); got != 42 {
		t.Fatalf("Prometheus events = %d, want 42 (monotonic, never reset)", got)
	}
}

// TestStatsFlusherResetStatsResetsOnlyOwnNode verifies the socket's local
// reset from the flusher's side: the worker's own persisted counters zero
// immediately (its backlog gauges and observation preserved) and the flusher
// adopts the new node epoch, while another node's rows, gauges, and epochs —
// and the shared stats_meta epoch — are untouched.
func TestStatsFlusherResetStatsResetsOnlyOwnNode(t *testing.T) {
	ctx := context.Background()
	dsn := testutil.NewStateDSN(t)
	a := testutil.OpenState(t, dsn, state.Options{
		NodeID: testutil.UniqueName(t, "reset-local"), Instance: "inst-a",
	})
	b := testutil.OpenState(t, dsn, state.Options{
		NodeID: testutil.UniqueName(t, "reset-remote"), Instance: "inst-b",
	})
	admin := testutil.OpenState(t, dsn)

	const observed = int64(1767225600000)
	b.RecordStats(state.Stats{
		EventsMatchedTotal:      20,
		PendingEntries:          7,
		OldestPendingAgeSeconds: 9,
		PendingObservedAtUnixMs: observed,
	})

	m := metrics.New()
	m.Add(metrics.MetricEventsMatched, 100)
	m.SetPendingBacklogObserved(3, 4, time.Now())
	f := newStatsFlusher(a, m, state.StatsGenerations{
		Global: state.StatsGenerationUnknown,
		Node:   state.StatsGenerationUnknown,
	})
	f.flush(ctx)
	if own, ok, err := a.ReadNodeStats(ctx, a.NodeID()); err != nil || !ok ||
		own.EventsMatchedTotal != 100 {
		t.Fatalf("a before reset = %+v ok=%v err=%v, want events 100", own, ok, err)
	}

	metaBefore, err := admin.StatsGeneration()
	if err != nil {
		t.Fatalf("StatsGeneration: %v", err)
	}
	if err := f.ResetStats(); err != nil {
		t.Fatalf("ResetStats: %v", err)
	}

	// Own node: counters zeroed, backlog gauges and their observation intact.
	own, ok, err := a.ReadNodeStats(ctx, a.NodeID())
	if err != nil || !ok {
		t.Fatalf("a node read: ok=%v err=%v", ok, err)
	}
	if own.EventsMatchedTotal != 0 {
		t.Fatalf("a events after local reset = %d, want 0", own.EventsMatchedTotal)
	}
	if own.PendingEntries != 3 || own.OldestPendingAgeSeconds != 4 ||
		own.PendingObservedAtUnixMs == 0 {
		t.Fatalf("a gauges after local reset = %+v, want the observed (3, 4) pair preserved", own)
	}

	// The other node is untouched, counters and gauges alike.
	remote, ok, err := b.ReadNodeStats(ctx, b.NodeID())
	if err != nil || !ok {
		t.Fatalf("b node read: ok=%v err=%v", ok, err)
	}
	if remote.EventsMatchedTotal != 20 || remote.PendingEntries != 7 ||
		remote.OldestPendingAgeSeconds != 9 || remote.PendingObservedAtUnixMs != observed {
		t.Fatalf("b after a's local reset = %+v, want its original snapshot untouched", remote)
	}

	// The shared epoch did not move; the flusher adopted the new node epoch.
	if metaAfter, err := admin.StatsGeneration(); err != nil || metaAfter != metaBefore {
		t.Fatalf("shared epoch after local reset = %d err=%v, want %d (untouched)",
			metaAfter, err, metaBefore)
	}
	gens, err := a.StatsGenerations()
	if err != nil {
		t.Fatalf("a StatsGenerations: %v", err)
	}
	if f.nodeGeneration != gens.Node {
		t.Fatalf("flusher node epoch = %d, want %d (adopted by the reset)", f.nodeGeneration, gens.Node)
	}
	if f.generation != metaBefore {
		t.Fatalf("flusher global epoch = %d, want %d (a local reset must not move it)",
			f.generation, metaBefore)
	}

	// The next flush persists the reset point, then post-reset growth from zero.
	f.flush(ctx)
	own, _, _ = a.ReadNodeStats(ctx, a.NodeID())
	if own.EventsMatchedTotal != 0 {
		t.Fatalf("flush after local reset resurrected events = %d, want 0", own.EventsMatchedTotal)
	}
	m.Add(metrics.MetricEventsMatched, 5)
	f.flush(ctx)
	own, _, _ = a.ReadNodeStats(ctx, a.NodeID())
	if own.EventsMatchedTotal != 5 {
		t.Fatalf("post-reset events = %d, want 5", own.EventsMatchedTotal)
	}
	remote, _, _ = b.ReadNodeStats(ctx, b.NodeID())
	if remote.EventsMatchedTotal != 20 {
		t.Fatalf("b events = %d, want 20 (still untouched)", remote.EventsMatchedTotal)
	}
}

// TestStatsFlusherResetAllStatsResetsEveryNode verifies the socket's global
// reset from the flusher's side: every node's persisted counters zero in one
// transaction (backlog gauges preserved), the flusher adopts the advanced
// shared epoch while its node epoch stays put, and the worker continues from
// the reset point.
func TestStatsFlusherResetAllStatsResetsEveryNode(t *testing.T) {
	ctx := context.Background()
	dsn := testutil.NewStateDSN(t)
	a := testutil.OpenState(t, dsn, state.Options{
		NodeID: testutil.UniqueName(t, "global-local"), Instance: "inst-a",
	})
	b := testutil.OpenState(t, dsn, state.Options{
		NodeID: testutil.UniqueName(t, "global-remote"), Instance: "inst-b",
	})
	admin := testutil.OpenState(t, dsn)

	const observed = int64(1767225600000)
	b.RecordStats(state.Stats{
		EventsMatchedTotal:      20,
		PendingEntries:          7,
		OldestPendingAgeSeconds: 9,
		PendingObservedAtUnixMs: observed,
	})

	m := metrics.New()
	m.Add(metrics.MetricEventsMatched, 100)
	f := newStatsFlusher(a, m, state.StatsGenerations{
		Global: state.StatsGenerationUnknown,
		Node:   state.StatsGenerationUnknown,
	})
	f.flush(ctx)

	metaBefore, err := admin.StatsGeneration()
	if err != nil {
		t.Fatalf("StatsGeneration: %v", err)
	}
	nodeBefore, err := a.StatsGenerations()
	if err != nil {
		t.Fatalf("a StatsGenerations: %v", err)
	}
	if err := f.ResetAllStats(); err != nil {
		t.Fatalf("ResetAllStats: %v", err)
	}

	// Both nodes zeroed; the remote row's gauges survive the global rewrite.
	own, _, _ := a.ReadNodeStats(ctx, a.NodeID())
	if own.EventsMatchedTotal != 0 {
		t.Fatalf("a events after global reset = %d, want 0", own.EventsMatchedTotal)
	}
	remote, ok, err := b.ReadNodeStats(ctx, b.NodeID())
	if err != nil || !ok {
		t.Fatalf("b node read: ok=%v err=%v", ok, err)
	}
	if remote.EventsMatchedTotal != 0 {
		t.Fatalf("b events after global reset = %d, want 0", remote.EventsMatchedTotal)
	}
	if remote.PendingEntries != 7 || remote.OldestPendingAgeSeconds != 9 ||
		remote.PendingObservedAtUnixMs != observed {
		t.Fatalf("b gauges after global reset = %+v, want the observed (7, 9) pair preserved", remote)
	}

	// The shared epoch advanced and the flusher adopted it; the node epoch is
	// deliberately not moved by a global reset.
	metaAfter, err := admin.StatsGeneration()
	if err != nil {
		t.Fatalf("StatsGeneration: %v", err)
	}
	if metaAfter != metaBefore+1 {
		t.Fatalf("shared epoch = %d, want %d (exactly one global reset)", metaAfter, metaBefore+1)
	}
	if f.generation != metaAfter {
		t.Fatalf("flusher global epoch = %d, want %d (adopted by the reset)", f.generation, metaAfter)
	}
	nodeAfter, err := a.StatsGenerations()
	if err != nil {
		t.Fatalf("a StatsGenerations: %v", err)
	}
	if nodeAfter.Node != nodeBefore.Node || f.nodeGeneration != nodeBefore.Node {
		t.Fatalf("a node epochs = stored %d / flusher %d, want both %d (global reset leaves them)",
			nodeAfter.Node, f.nodeGeneration, nodeBefore.Node)
	}

	// The worker continues from the reset point under the adopted epochs.
	f.flush(ctx)
	own, _, _ = a.ReadNodeStats(ctx, a.NodeID())
	if own.EventsMatchedTotal != 0 {
		t.Fatalf("flush after global reset resurrected events = %d, want 0", own.EventsMatchedTotal)
	}
	m.Add(metrics.MetricEventsMatched, 5)
	f.flush(ctx)
	own, _, _ = a.ReadNodeStats(ctx, a.NodeID())
	if own.EventsMatchedTotal != 5 {
		t.Fatalf("post-reset events = %d, want 5", own.EventsMatchedTotal)
	}
}

// TestStatsFlusherResetAllRaceNoResurrection is the global-reset counterpart
// of the local reset race: a flush loop running concurrently with
// ResetAllStats can never persist a pre-reset total after the reset returns —
// both share the flusher mutex, and the reset adopts the advanced shared epoch
// under that same lock before any later flush can capture.
func TestStatsFlusherResetAllRaceNoResurrection(t *testing.T) {
	st, _ := openTempWorkerNodeState(t)
	m := metrics.New()
	m.Add(metrics.MetricEventsMatched, 1000)
	gens, err := st.StatsGenerations()
	if err != nil {
		t.Fatalf("StatsGenerations: %v", err)
	}
	f := newStatsFlusher(st, m, gens)

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

	if err := f.ResetAllStats(); err != nil {
		close(stop)
		wg.Wait()
		t.Fatalf("ResetAllStats: %v", err)
	}

	// From the moment the global reset returns, no flush may resurrect a
	// pre-reset value.
	for i := 0; i < 200; i++ {
		gs, ok := st.Stats()
		if ok && gs.EventsMatchedTotal != 0 {
			close(stop)
			wg.Wait()
			t.Fatalf("a concurrent flush resurrected pre-reset events: %d", gs.EventsMatchedTotal)
		}
		time.Sleep(time.Millisecond)
	}

	close(stop)
	wg.Wait()

	f.flush(context.Background())
	if gs, _ := st.Stats(); gs.EventsMatchedTotal != 0 {
		t.Fatalf("final persisted events = %d, want 0", gs.EventsMatchedTotal)
	}
	if got := m.Counter(metrics.MetricEventsMatched); got != 1000 {
		t.Fatalf("Prometheus events = %d, want 1000 (monotonic)", got)
	}
}

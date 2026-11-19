//go:build integration

package state

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

// This file pins the two-epoch reset model at the state boundary: a node-scoped
// reset bumps ONLY that node's generation and fences only that node's stale
// snapshots, while `reset --all` bumps the shared stats_meta epoch and fences
// every node without touching their local epochs. Instance ownership, offline
// (absent) rows, app-row scoping, and retention pruning are covered alongside,
// because they all share the same lock order (stats_meta first, then stats).

// readOwnAppStats reads ONE node's app_stats payload for app directly, so a
// per-node scoping assertion never depends on the cross-node aggregate view.
func readOwnAppStats(t *testing.T, c *State, app, nodeID string) AppStats {
	t.Helper()
	var data sql.NullString
	if err := c.db.QueryRowContext(context.Background(),
		`SELECT `+jsonPayloadExpr+` FROM app_stats WHERE app_name = $1 AND node_id = $2`,
		app, nodeID).Scan(&data); err != nil {
		t.Fatalf("read app stats for %q (node %q): %v", app, nodeID, err)
	}
	fs, err := unmarshalAppStats(data)
	if err != nil {
		t.Fatalf("decode app stats for %q (node %q): %v", app, nodeID, err)
	}
	return fs
}

// TestResetNodeStatsFencesOnlyThatNode verifies the local reset's scope: it
// advances only the target node's epoch (told to its next flush), leaves the
// shared epoch and every other node's epochs untouched so other nodes keep
// flushing uninterrupted, and the fenced node recovers by rebasing — its
// pre-reset totals never come back.
func TestResetNodeStatsFencesOnlyThatNode(t *testing.T) {
	ctx := context.Background()
	dsn := newTestDSN(t)
	a := openWithIdentity(t, dsn, uniqueNodeID(t, "reset-a"), "inst-a")
	b := openWithIdentity(t, dsn, uniqueNodeID(t, "reset-b"), "inst-b")
	admin := testOpenOrFail(t, dsn)

	metaBefore, err := admin.StatsGeneration()
	if err != nil {
		t.Fatalf("StatsGeneration: %v", err)
	}
	aGens, err := a.StatsGenerations()
	if err != nil {
		t.Fatalf("a StatsGenerations: %v", err)
	}
	bGens, err := b.StatsGenerations()
	if err != nil {
		t.Fatalf("b StatsGenerations: %v", err)
	}
	if _, err := a.RecordStatsSnapshotWithGenerations(ctx, aGens,
		Stats{EventsMatchedTotal: 10}, nil); err != nil {
		t.Fatalf("a initial snapshot: %v", err)
	}
	if _, err := b.RecordStatsSnapshotWithGenerations(ctx, bGens,
		Stats{EventsMatchedTotal: 20}, nil); err != nil {
		t.Fatalf("b initial snapshot: %v", err)
	}

	nodeGen, err := admin.ResetNodeStats(ctx, a.NodeID())
	if err != nil {
		t.Fatalf("ResetNodeStats: %v", err)
	}
	if nodeGen != aGens.Node+1 {
		t.Fatalf("node epoch = %d, want %d (exactly one local reset)", nodeGen, aGens.Node+1)
	}
	if metaAfter, err := admin.StatsGeneration(); err != nil || metaAfter != metaBefore {
		t.Fatalf("shared epoch after a local reset = %d err=%v, want %d (untouched)",
			metaAfter, err, metaBefore)
	}

	// a's next flush under its known epochs is fenced and TOLD the new epoch.
	current, err := a.RecordStatsSnapshotWithGenerations(ctx, aGens,
		Stats{EventsMatchedTotal: 999}, nil)
	if !errors.Is(err, ErrStatsResetAdvanced) {
		t.Fatalf("a pre-reset snapshot err = %v, want ErrStatsResetAdvanced", err)
	}
	if current.Node != nodeGen || current.Global != metaBefore {
		t.Fatalf("fence returned generations %+v, want node %d with global %d",
			current, nodeGen, metaBefore)
	}

	// b is not fenced: its own epochs did not move, so it keeps flushing.
	if _, err := b.RecordStatsSnapshotWithGenerations(ctx, bGens,
		Stats{EventsMatchedTotal: 7}, nil); err != nil {
		t.Fatalf("b snapshot after a's local reset: %v", err)
	}

	// Rebasing to the returned epochs lets a write again; the pre-reset 10 is
	// never resurrected.
	rebased := StatsGenerations{Global: current.Global, Node: current.Node}
	if _, err := a.RecordStatsSnapshotWithGenerations(ctx, rebased,
		Stats{EventsMatchedTotal: 5}, nil); err != nil {
		t.Fatalf("a rebased snapshot: %v", err)
	}
	s, ok, err := admin.ReadStats(ctx)
	if err != nil || !ok {
		t.Fatalf("aggregate read: ok=%v err=%v", ok, err)
	}
	if s.EventsMatchedTotal != 12 {
		t.Fatalf("aggregate events = %d, want 12 (a rebased to 5, b kept 7)", s.EventsMatchedTotal)
	}
}

// TestResetNodeStatsScopesRowsAndPreservesGauges verifies what a local reset
// rewrites: only the target node's global and per-app rows (counters zeroed,
// execution timestamps cleared), while its backlog gauges, observation
// instant, and instance fence all survive. The other node's rows and epoch are
// untouched, and the shared epoch does not move.
func TestResetNodeStatsScopesRowsAndPreservesGauges(t *testing.T) {
	ctx := context.Background()
	dsn := newTestDSN(t)
	a := openWithIdentity(t, dsn, uniqueNodeID(t, "scope-a"), "inst-a")
	b := openWithIdentity(t, dsn, uniqueNodeID(t, "scope-b"), "inst-b")
	admin := testOpenOrFail(t, dsn)
	// The flush's per-app upsert is guarded by the app's existence in apps.
	seedDiscoveredAlpha(t, a)

	const observed = int64(1767225600000)
	execA := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).Format(time.RFC3339)
	execB := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339)

	aGens, err := a.StatsGenerations()
	if err != nil {
		t.Fatalf("a StatsGenerations: %v", err)
	}
	bGens, err := b.StatsGenerations()
	if err != nil {
		t.Fatalf("b StatsGenerations: %v", err)
	}
	if _, err := a.RecordStatsSnapshotWithGenerations(ctx, aGens,
		Stats{
			EventsMatchedTotal:      10,
			PendingEntries:          17,
			OldestPendingAgeSeconds: 134,
			PendingObservedAtUnixMs: observed,
		},
		[]AppStats{{App: "alpha", EventsMatchedTotal: 9, LastExecutionAt: execA}}); err != nil {
		t.Fatalf("a initial snapshot: %v", err)
	}
	if _, err := b.RecordStatsSnapshotWithGenerations(ctx, bGens,
		Stats{EventsMatchedTotal: 20},
		[]AppStats{{App: "alpha", EventsMatchedTotal: 4, LastExecutionAt: execB}}); err != nil {
		t.Fatalf("b initial snapshot: %v", err)
	}

	metaBefore, err := admin.StatsGeneration()
	if err != nil {
		t.Fatalf("StatsGeneration: %v", err)
	}
	nodeGen, err := admin.ResetNodeStats(ctx, a.NodeID())
	if err != nil {
		t.Fatalf("ResetNodeStats: %v", err)
	}

	// a's row: counters zeroed, backlog gauges and their observation preserved.
	own, ok, err := a.ReadNodeStats(ctx, a.NodeID())
	if err != nil || !ok {
		t.Fatalf("a node read: ok=%v err=%v", ok, err)
	}
	if own.EventsMatchedTotal != 0 {
		t.Fatalf("a events after local reset = %d, want 0", own.EventsMatchedTotal)
	}
	if own.PendingEntries != 17 || own.OldestPendingAgeSeconds != 134 ||
		own.PendingObservedAtUnixMs != observed {
		t.Fatalf("a gauges/observation after local reset = %+v, want (17, 134, %d)", own, observed)
	}

	// Instance ownership holds across a local reset: the row keeps its fencing
	// value, so a stale incarnation stays fenced.
	var instance string
	if err := admin.db.QueryRowContext(ctx,
		`SELECT instance FROM stats WHERE node_id = $1`, a.NodeID()).Scan(&instance); err != nil {
		t.Fatalf("read a's instance: %v", err)
	}
	if instance != "inst-a" {
		t.Fatalf("instance after local reset = %q, want inst-a (ownership preserved)", instance)
	}
	gens, err := a.StatsGenerations()
	if err != nil {
		t.Fatalf("a StatsGenerations after reset: %v", err)
	}
	if gens.Node != nodeGen || gens.Global != metaBefore {
		t.Fatalf("generations after local reset = %+v, want node %d with global %d",
			gens, nodeGen, metaBefore)
	}

	// b's row is untouched: counters, gauges, and its own epoch.
	remote, ok, err := b.ReadNodeStats(ctx, b.NodeID())
	if err != nil || !ok {
		t.Fatalf("b node read: ok=%v err=%v", ok, err)
	}
	if remote.EventsMatchedTotal != 20 {
		t.Fatalf("b events after a's local reset = %d, want 20 (untouched)", remote.EventsMatchedTotal)
	}
	bGensAfter, err := b.StatsGenerations()
	if err != nil {
		t.Fatalf("b StatsGenerations after reset: %v", err)
	}
	if bGensAfter.Node != bGens.Node {
		t.Fatalf("b node epoch = %d, want %d (untouched by a's reset)", bGensAfter.Node, bGens.Node)
	}

	// Per-app rows are scoped the same way: a's counters and execution history
	// cleared in place, b's preserved verbatim.
	appA := readOwnAppStats(t, admin, "alpha", a.NodeID())
	if appA.EventsMatchedTotal != 0 || appA.LastExecutionAt != "" {
		t.Fatalf("a's alpha after local reset = %+v, want zeroed counters and cleared timestamps", appA)
	}
	appB := readOwnAppStats(t, admin, "alpha", b.NodeID())
	if appB.EventsMatchedTotal != 4 || appB.LastExecutionAt != execB {
		t.Fatalf("b's alpha after a's local reset = %+v, want events 4 with execution %s", appB, execB)
	}
}

// TestResetNodeStatsCreatesAbsentRowAndFencesFirstFlush covers the offline
// (absent-row) case: a local reset against a node that has never flushed still
// records the reset as a fenced epoch on a zero row, and a first flush from
// that node — which cannot have observed the epoch — is rejected with the new
// epoch instead of silently overwriting a reset it never saw, in both the
// stale-known and unknown epoch forms.
func TestResetNodeStatsCreatesAbsentRowAndFencesFirstFlush(t *testing.T) {
	ctx := context.Background()
	dsn := newTestDSN(t)
	admin := testOpenOrFail(t, dsn)
	offline := uniqueNodeID(t, "reset-offline")

	gen, err := admin.ResetNodeStats(ctx, offline)
	if err != nil {
		t.Fatalf("ResetNodeStats on an absent row: %v", err)
	}
	if gen != 1 {
		t.Fatalf("absent-row node epoch = %d, want 1", gen)
	}

	var (
		storedGen int64
		data      sql.NullString
	)
	if err := admin.db.QueryRowContext(ctx,
		`SELECT generation, `+jsonPayloadExpr+` FROM stats WHERE node_id = $1`,
		offline).Scan(&storedGen, &data); err != nil {
		t.Fatalf("read reset row: %v", err)
	}
	if storedGen != gen {
		t.Fatalf("stored epoch = %d, want %d", storedGen, gen)
	}
	if s, err := unmarshalStats(data); err != nil || s != (Stats{}) {
		t.Fatalf("reset row payload = %+v err=%v, want a zero payload", s, err)
	}

	meta, err := admin.StatsGeneration()
	if err != nil {
		t.Fatalf("StatsGeneration: %v", err)
	}
	node := openWithIdentity(t, dsn, offline, "inst-x")

	// A flush carrying a stale known epoch is fenced and told the new one.
	current, err := node.RecordStatsSnapshotWithGenerations(ctx,
		StatsGenerations{Global: meta, Node: 0}, Stats{EventsMatchedTotal: 1}, nil)
	if !errors.Is(err, ErrStatsResetAdvanced) {
		t.Fatalf("stale-epoch flush err = %v, want ErrStatsResetAdvanced", err)
	}
	if current.Node != gen {
		t.Fatalf("stale-epoch fence returned node epoch %d, want %d", current.Node, gen)
	}

	// The unknown-epoch form (a process that observed nothing yet) is fenced
	// too: adopting a non-zero epoch it never observed could resurrect
	// pre-reset history.
	current, err = node.RecordStatsSnapshotWithGenerations(ctx,
		StatsGenerations{Global: meta, Node: StatsGenerationUnknown},
		Stats{EventsMatchedTotal: 1}, nil)
	if !errors.Is(err, ErrStatsResetAdvanced) {
		t.Fatalf("unknown-epoch flush err = %v, want ErrStatsResetAdvanced", err)
	}
	if current.Node != gen {
		t.Fatalf("unknown-epoch fence returned node epoch %d, want %d", current.Node, gen)
	}

	// With the adopted epoch the node writes under the reset's zero row.
	rebased := StatsGenerations{Global: meta, Node: current.Node}
	if _, err := node.RecordStatsSnapshotWithGenerations(ctx, rebased,
		Stats{EventsMatchedTotal: 3}, nil); err != nil {
		t.Fatalf("rebased first flush: %v", err)
	}
	if s, ok := node.OwnerStats(); !ok || s.EventsMatchedTotal != 3 {
		t.Fatalf("stats after rebased first flush = %+v ok=%v, want events 3", s, ok)
	}
}

// TestResetAllStatsFencesEveryNodeAndKeepsNodeEpochs pins the global reset's
// side of the model: the shared stats_meta epoch advances, every node's flush
// is fenced and told the new shared epoch, and the per-node generation columns
// are deliberately NOT advanced — they belong to `stats reset` alone.
func TestResetAllStatsFencesEveryNodeAndKeepsNodeEpochs(t *testing.T) {
	ctx := context.Background()
	dsn := newTestDSN(t)
	a := openWithIdentity(t, dsn, uniqueNodeID(t, "global-a"), "inst-a")
	b := openWithIdentity(t, dsn, uniqueNodeID(t, "global-b"), "inst-b")
	admin := testOpenOrFail(t, dsn)

	aGens, err := a.StatsGenerations()
	if err != nil {
		t.Fatalf("a StatsGenerations: %v", err)
	}
	bGens, err := b.StatsGenerations()
	if err != nil {
		t.Fatalf("b StatsGenerations: %v", err)
	}
	if _, err := a.RecordStatsSnapshotWithGenerations(ctx, aGens,
		Stats{EventsMatchedTotal: 10}, nil); err != nil {
		t.Fatalf("a initial snapshot: %v", err)
	}
	if _, err := b.RecordStatsSnapshotWithGenerations(ctx, bGens,
		Stats{EventsMatchedTotal: 20}, nil); err != nil {
		t.Fatalf("b initial snapshot: %v", err)
	}

	newGen, err := admin.ResetStats()
	if err != nil {
		t.Fatalf("ResetStats: %v", err)
	}

	for _, w := range []struct {
		name string
		st   *State
		gens StatsGenerations
	}{{"a", a, aGens}, {"b", b, bGens}} {
		current, err := w.st.RecordStatsSnapshotWithGenerations(ctx, w.gens,
			Stats{EventsMatchedTotal: 999}, nil)
		if !errors.Is(err, ErrStatsResetAdvanced) {
			t.Fatalf("%s pre-reset snapshot err = %v, want ErrStatsResetAdvanced", w.name, err)
		}
		if current.Global != newGen {
			t.Fatalf("%s fence returned global epoch %d, want %d", w.name, current.Global, newGen)
		}
		if current.Node != w.gens.Node {
			t.Fatalf("%s fence returned node epoch %d, want %d (the global reset must not advance local epochs)",
				w.name, current.Node, w.gens.Node)
		}
	}

	// Rebased writes land; the aggregate restarts from the reset point.
	for _, w := range []struct {
		st     *State
		events int64
	}{{a, 5}, {b, 7}} {
		if _, err := w.st.RecordStatsSnapshotWithGenerations(ctx,
			StatsGenerations{Global: newGen, Node: 0},
			Stats{EventsMatchedTotal: w.events}, nil); err != nil {
			t.Fatalf("rebased snapshot: %v", err)
		}
	}
	s, ok, err := admin.ReadStats(ctx)
	if err != nil || !ok {
		t.Fatalf("aggregate read: ok=%v err=%v", ok, err)
	}
	if s.EventsMatchedTotal != 12 {
		t.Fatalf("aggregate events = %d, want 12 (post-reset values only)", s.EventsMatchedTotal)
	}
}

// TestPruneKeepsStatsMetaAndDropsNodeEpochWithRow pins the retention
// contract for the epochs: pruning a stale owner's row removes its local
// generation WITH the row (no epoch leaks into a future successor for the same
// node identity), while the shared stats_meta row and its advanced epoch
// survive pruning untouched.
func TestPruneKeepsStatsMetaAndDropsNodeEpochWithRow(t *testing.T) {
	ctx := context.Background()
	dsn := newTestDSN(t)
	base := time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC)
	live := openWithIdentity(t, dsn, uniqueNodeID(t, "prune-live"), "inst-live")
	live.nowFn = func() time.Time { return base }
	deadNode := uniqueNodeID(t, "prune-dead")
	dead := openWithIdentity(t, dsn, deadNode, "inst-dead")
	dead.nowFn = func() time.Time { return base.Add(-48 * time.Hour) }
	admin := testOpenOrFail(t, dsn)
	// The admin's resets rewrite updated_at, so they must run on the dead
	// node's clock too — otherwise a real-time reset would make the stale row
	// look fresh to the live node's cutoff.
	admin.nowFn = func() time.Time { return base.Add(-48 * time.Hour) }

	// The dead node flushes, then accumulates two LOCAL epochs; afterwards the
	// shared epoch advances once, so both epoch kinds exist when its row prunes.
	if _, err := dead.RecordStatsSnapshotWithGenerations(ctx,
		StatsGenerations{Global: 0, Node: 0}, Stats{EventsMatchedTotal: 1}, nil); err != nil {
		t.Fatalf("dead snapshot: %v", err)
	}
	for i := 1; i <= 2; i++ {
		if _, err := admin.ResetNodeStats(ctx, deadNode); err != nil {
			t.Fatalf("local reset %d: %v", i, err)
		}
	}
	globalGen, err := admin.ResetStats()
	if err != nil {
		t.Fatalf("global reset: %v", err)
	}

	removed, err := live.PruneStaleStats(ctx, 24*time.Hour)
	if err != nil {
		t.Fatalf("PruneStaleStats: %v", err)
	}
	if removed != 1 {
		t.Fatalf("pruned = %d, want 1 (the stale node's stats row)", removed)
	}

	// stats_meta is coordination state, not stats: pruning never deletes it or
	// rewinds the shared epoch.
	if g, err := admin.StatsGeneration(); err != nil || g != globalGen {
		t.Fatalf("shared epoch after prune = %d err=%v, want %d", g, err, globalGen)
	}
	var metaRows int
	if err := admin.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM stats_meta`).Scan(&metaRows); err != nil {
		t.Fatalf("count stats_meta: %v", err)
	}
	if metaRows != 1 {
		t.Fatalf("stats_meta rows = %d, want 1 (never pruned)", metaRows)
	}

	// A successor for the same node identity starts at epoch 0 — the pruned
	// row's epochs leave WITH the row — so its first flush is adopted, not
	// fenced by an epoch it never observed.
	successor := openUnlockedState(t, dsn, deadNode, "inst-new")
	gens, err := successor.StatsGenerations()
	if err != nil {
		t.Fatalf("successor StatsGenerations: %v", err)
	}
	if gens.Node != 0 {
		t.Fatalf("successor node epoch = %d, want 0 (no epoch leak across prune)", gens.Node)
	}
	if gens.Global != globalGen {
		t.Fatalf("successor global epoch = %d, want %d (stats_meta survived)", gens.Global, globalGen)
	}
	if _, err := successor.RecordStatsSnapshotWithGenerations(ctx,
		StatsGenerations{Global: globalGen, Node: gens.Node},
		Stats{EventsMatchedTotal: 3}, nil); err != nil {
		t.Fatalf("successor first flush must not be fenced by the pruned row's epochs: %v", err)
	}
}

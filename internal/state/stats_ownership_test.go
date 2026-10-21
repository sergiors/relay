package state

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// openWithIdentity opens a state handle on dsn scoped to a worker identity
// (Owner + Instance) and registers its close. It is the identity-aware companion
// of testOpenOrFail for the ownership/fencing tests.
func openWithIdentity(t *testing.T, dsn, worker, instance string) *State {
	t.Helper()
	st, err := Open(context.Background(), Options{DSN: dsn, Owner: worker, Instance: instance})
	if err != nil {
		t.Fatalf("open with identity (%s/%s): %v", worker, instance, err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// seedDiscoveredAlpha records an apps row for "alpha" so app_stats writes pass
// the EXISTS guard.
func seedDiscoveredAlpha(t *testing.T, st *State) {
	t.Helper()
	if _, err := st.RecordDiscovered(context.Background(), fnFor(t, "alpha", mustTemplate(t, resetSeedTmpl))); err != nil {
		t.Errorf("state write error: %v", err)
	}
}

// TestOpenRejectsUnpairedOwnerInstance pins the identity invariant: a handle
// either owns stats rows (Owner AND Instance) or does not (neither). A
// half-configured identity is rejected before touching the database.
func TestOpenRejectsUnpairedOwnerInstance(t *testing.T) {
	dsn := newTestDSN(t)
	if _, err := Open(context.Background(), Options{DSN: dsn, Owner: "worker-a"}); err == nil {
		t.Fatal("Open with Owner but no Instance succeeded, want error")
	}
	if _, err := Open(context.Background(), Options{DSN: dsn, Instance: "inst-a"}); err == nil {
		t.Fatal("Open with Instance but no Owner succeeded, want error")
	}
	st, err := Open(context.Background(), Options{DSN: dsn})
	if err != nil {
		t.Fatalf("Open with neither Owner nor Instance: %v", err)
	}
	_ = st.Close()
}

// TestClaimFencesStaleIncarnation verifies the instance fencing: a second
// process incarnation of the SAME worker takes over the row, and the first
// (now stale) incarnation's flush is rejected with ErrStatsOwnershipLost instead
// of overwriting the newer snapshot. The two incarnations are simulated with
// unlocked handles: production Open now takes an exclusive per-owner lock, so two
// LIVE same-owner handles cannot coexist (see TestIntegrationWorkerLockExclusive);
// this test isolates the DB-level fence that remains as defense in depth for a
// handle whose lock connection was lost.
func TestClaimFencesStaleIncarnation(t *testing.T) {
	ctx := context.Background()
	dsn := newTestDSN(t)
	stale := openUnlockedState(t, dsn, "worker-a", "inst-1")
	if _, err := stale.RecordStatsSnapshot(ctx, -1, Stats{EventsMatchedTotal: 10}, nil); err != nil {
		t.Fatalf("stale snapshot: %v", err)
	}

	// A restart/new process with the same stable worker id claims the row with a
	// fresh instance and writes its own absolute snapshot.
	fresh := openUnlockedState(t, dsn, "worker-a", "inst-2")
	if _, err := fresh.RecordStatsSnapshot(ctx, -1, Stats{EventsMatchedTotal: 20}, nil); err != nil {
		t.Fatalf("fresh snapshot: %v", err)
	}

	if _, err := stale.RecordStatsSnapshot(ctx, -1, Stats{EventsMatchedTotal: 999}, nil); !errors.Is(err, ErrStatsOwnershipLost) {
		t.Fatalf("stale snapshot err = %v, want ErrStatsOwnershipLost", err)
	}

	s, ok := fresh.Stats()
	if !ok || s.EventsMatchedTotal != 20 {
		t.Fatalf("stats = %+v ok=%v, want 20 (stale incarnation fenced)", s, ok)
	}
}

// TestOwnershipLostSkipsAppStats verifies the flush is all-or-nothing under
// lost ownership: when the global row write is rejected, no per-app row is
// written either. As in TestClaimFencesStaleIncarnation, the two incarnations
// bypass the startup lock to exercise the DB-level fence.
func TestOwnershipLostSkipsAppStats(t *testing.T) {
	ctx := context.Background()
	dsn := newTestDSN(t)
	stale := openUnlockedState(t, dsn, "worker-a", "inst-1")
	fresh := openUnlockedState(t, dsn, "worker-a", "inst-2")
	seedDiscoveredAlpha(t, fresh)

	// Claim the row with the fresh incarnation, then attempt a global+app write
	// from the stale one.
	if _, err := fresh.RecordStatsSnapshot(ctx, -1, Stats{EventsMatchedTotal: 1}, nil); err != nil {
		t.Fatalf("fresh snapshot: %v", err)
	}
	_, err := stale.RecordStatsSnapshot(ctx, -1, Stats{EventsMatchedTotal: 1},
		[]AppStats{{App: "alpha", EventsMatchedTotal: 1}})
	if !errors.Is(err, ErrStatsOwnershipLost) {
		t.Fatalf("stale snapshot err = %v, want ErrStatsOwnershipLost", err)
	}
	if _, ok := fresh.AppStats("alpha"); ok {
		t.Fatal("stale incarnation must not have written an app_stats row")
	}
}

// TestRestartSameWorkerKeepsTotalsOnce verifies restart continuity: a restart
// of the same worker restores its own row and writes the resumed cumulative
// totals back as an ABSOLUTE snapshot, so the aggregate stays exactly once (not
// doubled) even though a new instance claims the row. The restart closes the
// prior handle first — as production does — so its exclusive worker lock is
// released and the successor can open.
func TestRestartSameWorkerKeepsTotalsOnce(t *testing.T) {
	ctx := context.Background()
	dsn := newTestDSN(t)
	first := openWithIdentity(t, dsn, "worker-a", "inst-1")
	if _, err := first.RecordStatsSnapshot(ctx, -1, Stats{EventsMatchedTotal: 100}, nil); err != nil {
		t.Fatalf("first snapshot: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first handle: %v", err)
	}

	restarted := openWithIdentity(t, dsn, "worker-a", "inst-2")
	restored, ok := restarted.OwnerStats()
	if !ok || restored.EventsMatchedTotal != 100 {
		t.Fatalf("restored = %+v ok=%v, want events 100", restored, ok)
	}
	if _, err := restarted.RecordStatsSnapshot(ctx, -1, restored, nil); err != nil {
		t.Fatalf("restart snapshot: %v", err)
	}

	s, ok := restarted.Stats()
	if !ok || s.EventsMatchedTotal != 100 {
		t.Fatalf("aggregate = %+v ok=%v, want 100 (resumed once, not doubled)", s, ok)
	}
}

// TestResetStatsFencesEveryWorker verifies the global reset generation fences
// delayed snapshots from EVERY worker, not only the socket-owning one: after a
// reset performed by a third handle, both workers holding a pre-reset epoch are
// rejected with ErrStatsResetAdvanced (and told the new epoch), and only adopt it
// to write again.
func TestResetStatsFencesEveryWorker(t *testing.T) {
	ctx := context.Background()
	dsn := newTestDSN(t)
	a := openWithIdentity(t, dsn, "worker-a", "inst-a")
	b := openWithIdentity(t, dsn, "worker-b", "inst-b")
	admin := testOpenOrFail(t, dsn)

	genA, err := a.RecordStatsSnapshot(ctx, -1, Stats{EventsMatchedTotal: 10}, nil)
	if err != nil {
		t.Fatalf("a initial snapshot: %v", err)
	}
	genB, err := b.RecordStatsSnapshot(ctx, -1, Stats{EventsMatchedTotal: 20}, nil)
	if err != nil {
		t.Fatalf("b initial snapshot: %v", err)
	}

	newGen, err := admin.ResetStats()
	if err != nil {
		t.Fatalf("reset: %v", err)
	}
	if newGen <= genA || newGen <= genB {
		t.Fatalf("reset generation %d must advance past %d/%d", newGen, genA, genB)
	}

	for _, w := range []struct {
		name string
		st   *State
		gen  int64
	}{{"a", a, genA}, {"b", b, genB}} {
		got, err := w.st.RecordStatsSnapshot(ctx, w.gen, Stats{EventsMatchedTotal: 999}, nil)
		if !errors.Is(err, ErrStatsResetAdvanced) {
			t.Fatalf("worker %s pre-reset snapshot err = %v, want ErrStatsResetAdvanced", w.name, err)
		}
		if got != newGen {
			t.Fatalf("worker %s returned generation %d, want %d", w.name, got, newGen)
		}
	}

	// With the new epoch both workers write again; totals are the post-reset
	// absolute values (the pre-reset 10/20 were zeroed and never resurrected).
	if _, err := a.RecordStatsSnapshot(ctx, newGen, Stats{EventsMatchedTotal: 5}, nil); err != nil {
		t.Fatalf("a post-reset snapshot: %v", err)
	}
	if _, err := b.RecordStatsSnapshot(ctx, newGen, Stats{EventsMatchedTotal: 7}, nil); err != nil {
		t.Fatalf("b post-reset snapshot: %v", err)
	}
	s, ok := admin.Stats()
	if !ok || s.EventsMatchedTotal != 12 {
		t.Fatalf("aggregate = %+v ok=%v, want 12", s, ok)
	}
}

// TestStatsGenerationBumpsOnReset pins the epoch reads: every reset bumps the
// stored generation and returns the new value, and StatsGeneration mirrors it.
func TestStatsGenerationBumpsOnReset(t *testing.T) {
	admin := openTestState(t)
	gen, err := admin.StatsGeneration()
	if err != nil {
		t.Fatalf("StatsGeneration: %v", err)
	}
	if gen != 0 {
		t.Fatalf("initial generation = %d, want 0", gen)
	}
	for want := int64(1); want <= 2; want++ {
		got, err := admin.ResetStats()
		if err != nil {
			t.Fatalf("reset %d: %v", want, err)
		}
		if got != want {
			t.Fatalf("reset returned generation %d, want %d", got, want)
		}
		if cur, err := admin.StatsGeneration(); err != nil || cur != want {
			t.Fatalf("StatsGeneration = %d err=%v, want %d", cur, err, want)
		}
	}
}

// TestPruneStaleStatsRemovesOtherOwners verifies compaction deletes a stale
// owner's global AND per-app rows while keeping the live owner's row and the
// shared apps table untouched.
func TestPruneStaleStatsRemovesOtherOwners(t *testing.T) {
	ctx := context.Background()
	dsn := newTestDSN(t)
	live := openWithIdentity(t, dsn, "worker-live", "inst-live")
	base := time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC)
	live.nowFn = func() time.Time { return base }

	stale := openWithIdentity(t, dsn, "worker-stale", "inst-stale")
	stale.nowFn = func() time.Time { return base.Add(-48 * time.Hour) }
	seedDiscoveredAlpha(t, stale)
	if _, err := stale.RecordStatsSnapshot(ctx, -1, Stats{EventsMatchedTotal: 1},
		[]AppStats{{App: "alpha", EventsMatchedTotal: 1}}); err != nil {
		t.Fatalf("stale snapshot: %v", err)
	}
	if _, err := live.RecordStatsSnapshot(ctx, -1, Stats{EventsMatchedTotal: 2}, nil); err != nil {
		t.Fatalf("live snapshot: %v", err)
	}

	removed, err := live.PruneStaleStats(ctx, 24*time.Hour)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if removed != 2 {
		t.Fatalf("pruned = %d, want 2 (stale global + stale app_stats)", removed)
	}
	if _, ok := live.OwnerStats(); !ok {
		t.Fatal("live owner row must survive compaction")
	}
	if _, ok := live.AppStats("alpha"); ok {
		t.Fatal("stale owner app_stats row must be pruned")
	}
	if s, ok := live.Stats(); !ok || s.EventsMatchedTotal != 2 {
		t.Fatalf("aggregate = %+v ok=%v, want live 2 only", s, ok)
	}
}

// TestPruneStaleStatsRespectsRetention verifies the cutoff: a stale owner newer
// than the retention window survives, and the same row is removed once the
// window is shortened past its updated_at.
func TestPruneStaleStatsRespectsRetention(t *testing.T) {
	ctx := context.Background()
	dsn := newTestDSN(t)
	live := openWithIdentity(t, dsn, "worker-live", "inst-live")
	base := time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC)
	live.nowFn = func() time.Time { return base }

	old := openWithIdentity(t, dsn, "worker-old", "inst-old")
	old.nowFn = func() time.Time { return base.Add(-time.Hour) }
	if _, err := old.RecordStatsSnapshot(ctx, -1, Stats{EventsMatchedTotal: 1}, nil); err != nil {
		t.Fatalf("old snapshot: %v", err)
	}

	removed, err := live.PruneStaleStats(ctx, 24*time.Hour)
	if err != nil {
		t.Fatalf("prune (24h): %v", err)
	}
	if removed != 0 {
		t.Fatalf("pruned = %d, want 0 (row is within the retention window)", removed)
	}
	if s, ok := live.Stats(); !ok || s.EventsMatchedTotal != 1 {
		t.Fatalf("aggregate = %+v ok=%v, want the within-window row preserved", s, ok)
	}

	removed, err = live.PruneStaleStats(ctx, 30*time.Minute)
	if err != nil {
		t.Fatalf("prune (30m): %v", err)
	}
	if removed != 1 {
		t.Fatalf("pruned = %d, want 1 (row is now past the window)", removed)
	}
}

// TestPruneStaleStatsKeepsOwnRow verifies a worker never prunes its OWN row,
// however stale its updated_at looks: it is the live owner and must keep the
// cumulative totals it will resume writing.
func TestPruneStaleStatsKeepsOwnRow(t *testing.T) {
	ctx := context.Background()
	dfn := openWithIdentity(t, newTestDSN(t), "worker-live", "inst-live")
	base := time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC)
	dfn.nowFn = func() time.Time { return base.Add(-72 * time.Hour) }
	if _, err := dfn.RecordStatsSnapshot(ctx, -1, Stats{EventsMatchedTotal: 42}, nil); err != nil {
		t.Fatalf("own snapshot: %v", err)
	}

	removed, err := dfn.PruneStaleStats(ctx, time.Hour)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if removed != 0 {
		t.Fatalf("pruned = %d, want 0 (own row excluded)", removed)
	}
	if s, ok := dfn.OwnerStats(); !ok || s.EventsMatchedTotal != 42 {
		t.Fatalf("own row = %+v ok=%v, want preserved", s, ok)
	}
}

// TestPruneStaleStatsDisabled verifies retention <= 0 disables compaction.
func TestPruneStaleStatsDisabled(t *testing.T) {
	ctx := context.Background()
	dsn := newTestDSN(t)
	live := openWithIdentity(t, dsn, "worker-live", "inst-live")
	base := time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC)
	live.nowFn = func() time.Time { return base }

	old := openWithIdentity(t, dsn, "worker-old", "inst-old")
	old.nowFn = func() time.Time { return base.Add(-1000 * time.Hour) }
	if _, err := old.RecordStatsSnapshot(ctx, -1, Stats{EventsMatchedTotal: 1}, nil); err != nil {
		t.Fatalf("old snapshot: %v", err)
	}

	for _, retention := range []time.Duration{0, -time.Hour} {
		removed, err := live.PruneStaleStats(ctx, retention)
		if err != nil {
			t.Fatalf("prune(%v): %v", retention, err)
		}
		if removed != 0 {
			t.Fatalf("prune(%v) removed = %d, want 0 (disabled)", retention, removed)
		}
	}
}

// TestConcurrentIncarnationsOnlySentinelErrors verifies that concurrent flushes
// from two incarnations of the same worker are race-safe: every attempt either
// succeeds (only the current owner can) or is rejected with
// ErrStatsOwnershipLost, never a hard error, and the surviving row belongs to
// one of the incarnations. The two incarnations bypass the startup lock to drive
// the DB-level fence concurrently (production Open would refuse the second).
func TestConcurrentIncarnationsOnlySentinelErrors(t *testing.T) {
	ctx := context.Background()
	dsn := newTestDSN(t)
	a := openUnlockedState(t, dsn, "worker-a", "inst-a")
	b := openUnlockedState(t, dsn, "worker-a", "inst-b")

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func(n int64) {
			defer wg.Done()
			_, err := a.RecordStatsSnapshot(ctx, -1, Stats{EventsMatchedTotal: n}, nil)
			if err != nil && !errors.Is(err, ErrStatsOwnershipLost) {
				t.Errorf("a snapshot err = %v, want nil or ErrStatsOwnershipLost", err)
			}
		}(int64(i))
		go func(n int64) {
			defer wg.Done()
			_, err := b.RecordStatsSnapshot(ctx, -1, Stats{EventsMatchedTotal: n}, nil)
			if err != nil && !errors.Is(err, ErrStatsOwnershipLost) {
				t.Errorf("b snapshot err = %v, want nil or ErrStatsOwnershipLost", err)
			}
		}(int64(i))
	}
	wg.Wait()

	if _, ok := a.Stats(); !ok {
		t.Fatal("a stats row must exist after concurrent flushes")
	}
}

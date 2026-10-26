package worker

import (
	"context"
	"testing"
	"time"

	"relay/internal/observability/metrics"
	"relay/internal/state"
	"relay/internal/testutil"
)

// TestStatsFlusherRebasesAfterRemoteReset verifies the cross-process reset
// fencing from the flusher's side: a reset performed by ANOTHER process advances
// the global epoch, so the next flush is rejected once, rebases its baseline to
// the current registry values, and retries under the new epoch — restarting the
// persisted totals from zero instead of resurrecting the pre-reset counts.
func TestStatsFlusherRebasesAfterRemoteReset(t *testing.T) {
	ctx := context.Background()
	dsn := testutil.NewStateDSN(t)
	// Owner identities must be unique per test: PostgreSQL session advisory locks
	// are database-wide, so per-test schemas do not isolate them.
	owner := testutil.UniqueName(t, "stats-rebases")
	st := testutil.OpenState(t, dsn, state.Options{Owner: owner, Instance: "inst-a"})
	// A second, owner-less handle plays the admin process that resets.
	admin := testutil.OpenState(t, dsn)

	m := metrics.New()
	m.Add(metrics.MetricEventsMatched, 100)

	f := newStatsFlusher(st, m)
	f.flush(ctx)
	if s, ok := st.Stats(); !ok || s.EventsMatchedTotal != 100 {
		t.Fatalf("after first flush = %+v ok=%v, want 100", s, ok)
	}

	// The operator resets persisted stats from another process; the registry
	// keeps accumulating monotonically (no in-memory reset here).
	if _, err := admin.ResetStats(); err != nil {
		t.Fatalf("reset: %v", err)
	}
	m.Add(metrics.MetricEventsMatched, 50) // registry now 150

	// The next flush sees the advanced epoch, rebases to the current registry
	// value (150), and retries: the persisted total restarts from zero. This
	// deliberately drops up to one flush interval of counts (documented).
	f.flush(ctx)
	if s, ok := st.Stats(); !ok || s.EventsMatchedTotal != 0 {
		t.Fatalf("after rebased flush = %+v ok=%v, want 0", s, ok)
	}

	// Growth after the rebase persists from the rebase point.
	m.Add(metrics.MetricEventsMatched, 7)
	f.flush(ctx)
	if s, ok := st.Stats(); !ok || s.EventsMatchedTotal != 7 {
		t.Fatalf("after growth = %+v ok=%v, want 7", s, ok)
	}
}

// TestStatsFlusherStopsOnOwnershipLost verifies that once another incarnation of
// the same worker claims the stats row, this flusher latches ownershipLost and
// stops writing entirely, leaving the newer incarnation's snapshot untouched.
//
// Two LIVE same-owner handles can no longer coexist: state.Open holds an
// exclusive per-owner lock, so a competing worker fails to start rather than
// racing this one. The competing incarnation is therefore simulated at the
// database layer — a direct instance+payload re-stamp, exactly what a successor's
// claim writes — which is the residual race the fence still guards against (for
// example, after this handle's lock connection is lost).
func TestStatsFlusherStopsOnOwnershipLost(t *testing.T) {
	ctx := context.Background()
	dsn := testutil.NewStateDSN(t)

	owner := testutil.UniqueName(t, "stats-lost")
	stale := testutil.OpenState(t, dsn, state.Options{Owner: owner, Instance: "inst-1"})
	mStale := metrics.New()
	mStale.Add(metrics.MetricEventsMatched, 5)
	fStale := newStatsFlusher(stale, mStale)
	fStale.flush(ctx)

	// A new process incarnation of the same worker id claims the row.
	raw := newRawStateConn(t, dsn)
	if _, err := raw.ExecContext(ctx,
		`UPDATE stats SET instance = $2, data = $3, updated_at = $4 WHERE worker = $1`,
		owner, "inst-2", `{"events_matched_total":30}`,
		time.Now().UTC().Format(time.RFC3339)); err != nil {
		t.Fatalf("simulate successor claim: %v", err)
	}

	// The stale incarnation's flush is rejected and latches the flag.
	mStale.Add(metrics.MetricEventsMatched, 100)
	fStale.flush(ctx)
	if !fStale.ownershipLost {
		t.Fatal("stale flusher must latch ownershipLost")
	}
	if s, ok := stale.Stats(); !ok || s.EventsMatchedTotal != 30 {
		t.Fatalf("stats = %+v ok=%v, want the newer incarnation's 30", s, ok)
	}

	// Further stale flushes stay no-ops even as the stale registry keeps moving.
	mStale.Add(metrics.MetricEventsMatched, 1000)
	fStale.flush(ctx)
	if s, _ := stale.Stats(); s.EventsMatchedTotal != 30 {
		t.Fatalf("stale flush must not write after ownership loss: %+v", s)
	}
}

// TestStatsFlusherResetAdoptsEpoch verifies that the in-process socket reset
// adopts the epoch it just bumped, so the very next flush persists the reset
// point instead of treating its own reset as a concurrent one and rebasing a
// second time.
func TestStatsFlusherResetAdoptsEpoch(t *testing.T) {
	ctx := context.Background()
	owner := testutil.UniqueName(t, "stats-reset")
	st := testutil.NewState(t, state.Options{Owner: owner, Instance: "inst-a"})
	m := metrics.New()
	m.Add(metrics.MetricEventsMatched, 100)

	f := newStatsFlusher(st, m)
	f.flush(ctx)
	if _, ok := st.Stats(); !ok {
		t.Fatal("expected a stats row after the first flush")
	}

	if err := f.ResetStats(); err != nil {
		t.Fatalf("reset: %v", err)
	}
	gen, err := st.StatsGeneration()
	if err != nil {
		t.Fatalf("StatsGeneration: %v", err)
	}
	if f.generation != gen {
		t.Fatalf("flusher generation = %d, want the reset epoch %d", f.generation, gen)
	}

	// The next flush persists from the reset baseline (zero here).
	f.flush(ctx)
	if s, ok := st.Stats(); !ok || s.EventsMatchedTotal != 0 {
		t.Fatalf("after reset flush = %+v ok=%v, want 0", s, ok)
	}
}

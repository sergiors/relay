//go:build integration

package cron

import (
	"context"
	"testing"
	"time"

	"relay/internal/observability/metrics"
	"relay/internal/state"
	"relay/internal/testutil"
)

// TestDurableRetryThroughRealStateDB is the end-to-end persistence path with the
// production PostgreSQL outbox: an exhausted immediate publish persists the row,
// the DB is closed (simulating a process exit), reopened, and the durable worker
// resolves the occurrence it finds. It proves the whole chain wires together
// without a fake store.
func TestDurableRetryThroughRealStateDB(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	dsn := testutil.NewStateDSN(t)

	st1 := testutil.OpenState(t, dsn)
	st1.SetLogger(testutil.DiscardLogger())
	o := pendingOccurrence("a", "jobs.a")

	// Immediate publish exhausts its bounded budget and persists.
	fp := newFakePublisher(16)
	fp.err = errBoom
	s1 := testSchedulerWithMetrics(fp, testLogger(), metrics.New())
	s1.now = func() time.Time { return frozen }
	s1.wait = noWait
	s1.SetOutbox(st1)
	s1.publishOccurrence(context.Background(), o, false)
	if err := st1.Close(); err != nil {
		t.Fatalf("close state: %v", err)
	}

	// A "restart": reopen and run the durable worker with a healthy publisher.
	st2 := testutil.OpenState(t, dsn)
	st2.SetLogger(testutil.DiscardLogger())
	fp2 := newFakePublisher(4)
	fp2.published = true
	s2 := testSchedulerWithMetrics(fp2, testLogger(), metrics.New())
	s2.now = func() time.Time { return frozen }
	s2.SetOutbox(st2)

	ctx := context.Background()
	due, ok, err := st2.NextPendingDue(ctx, frozen)
	if err != nil || !ok {
		t.Fatalf("NextPendingDue after reopen = (ok %v, err %v), want (true,nil)", ok, err)
	}
	// Run the durable retry against the real store; it claims and resolves.
	if n, err := s2.RunPendingOnce(ctx); err != nil || n != 1 {
		t.Fatalf("RunPendingOnce = (%d,%v), want (1,nil) (due %v)", n, err, due)
	}
	calls := fp2.got()
	if len(calls) != 1 || calls[0].ID() != o.ID() {
		t.Fatalf("republished = %v, want the original occurrence %s", calls, o.ID())
	}
	// The row is gone and the outbox is empty.
	if _, ok, err := st2.NextPendingDue(ctx, frozen); err != nil || ok {
		t.Fatalf("NextPendingDue after resolution = (ok %v, err %v), want (false,nil)", ok, err)
	}
}

// TestExpiredRecordIsNotRetryableAfterRestart pins the restart contract with the
// real PostgreSQL outbox: a row inside retention remains retryable after a process
// restart, while a row whose deadline has passed is cleaned after restart and
// never published. The scheduler's injectable clock stands in for wall time, so
// the "expired" case uses a clock past the row's real 7-day deadline.
func TestExpiredRecordIsNotRetryableAfterRestart(t *testing.T) {
	o := pendingOccurrence("a", "jobs.a")
	save := func(t *testing.T, path string) {
		t.Helper()
		st := testutil.OpenState(t, path)
		st.SetLogger(testutil.DiscardLogger())
		if _, err := st.SavePendingOccurrence(context.Background(), state.PendingOccurrence{
			ID: o.ID(), App: o.App, Schedule: o.Schedule, Handler: o.Handler,
			ScheduledAt: o.ScheduledAt, NextAttempt: time.Now().Add(-time.Minute),
		}); err != nil {
			t.Fatalf("save: %v", err)
		}
		if err := st.Close(); err != nil {
			t.Fatalf("close state: %v", err)
		}
	}

	t.Run("inside retention remains retryable", func(t *testing.T) {
		dsn := testutil.NewStateDSN(t)
		save(t, dsn)
		st := testutil.OpenState(t, dsn)
		st.SetLogger(testutil.DiscardLogger())
		fp := newFakePublisher(4)
		fp.published = true
		s := testSchedulerWithMetrics(fp, testLogger(), metrics.New())
		s.now = time.Now // present: the row is well inside its 7-day window
		s.SetOutbox(st)
		if n, err := s.RunPendingOnce(context.Background()); err != nil || n != 1 {
			t.Fatalf("RunPendingOnce = (%d,%v), want (1,nil)", n, err)
		}
		if got := fp.callCount(); got != 1 {
			t.Fatalf("publish calls = %d, want 1 (inside retention is retryable)", got)
		}
	})

	t.Run("after expiry is cleaned without publishing", func(t *testing.T) {
		dsn := testutil.NewStateDSN(t)
		save(t, dsn)
		st := testutil.OpenState(t, dsn)
		st.SetLogger(testutil.DiscardLogger())
		fp := newFakePublisher(4)
		fp.published = true
		m := metrics.New()
		s := testSchedulerWithMetrics(fp, testLogger(), m)
		// Past the row's real first-insert + 7-day deadline.
		s.now = func() time.Time { return time.Now().Add(8 * 24 * time.Hour) }
		s.SetOutbox(st)
		if n, err := s.RunPendingOnce(context.Background()); err != nil || n != 1 {
			t.Fatalf("RunPendingOnce = (%d,%v), want (1,nil) (one expired record cleaned)", n, err)
		}
		if got := fp.callCount(); got != 0 {
			t.Fatalf("publish calls = %d, want 0 (expired record never published)", got)
		}
		if got := m.Counter(metrics.MetricSchedulePendingExpired); got != 1 {
			t.Fatalf("expired = %d, want 1", got)
		}
		if _, ok, err := st.NextPendingDue(context.Background(), time.Now().Add(8*24*time.Hour)); err != nil || ok {
			t.Fatalf("NextPendingDue after cleanup = (ok %v, err %v), want (false,nil)", ok, err)
		}
	})
}

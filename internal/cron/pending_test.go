package cron

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/observability/metrics"
	"relay/internal/schedule"
	"relay/internal/state"
	"relay/internal/testutil"
)

// fakePending is one record in the fake outbox.
type fakePending struct {
	p          state.PendingOccurrence
	leaseUntil time.Time
}

// fakeOutbox is a deterministic in-memory Outbox with the same claim/lease
// semantics as the SQLite store. Error injection seams let each failure window
// be exercised without a real database. ignoreLease models a lease race: every
// caller sees every due row, so callers must tolerate overlapping claims (the
// Redis publish-if-new is what makes that safe). It is mutex-protected, so it is
// safe to drive from concurrent RunPendingOnce calls.
//
// nowFn stamps each saved row's retention deadline (state.PendingRetention from
// the fake clock); nil falls back to time.Now. Tests that exercise expiry set it
// to a fixed instant so the deadline is deterministic.
type fakeOutbox struct {
	mu    sync.Mutex
	rows  map[string]*fakePending
	order []string

	saved   []string
	deleted []string
	expired []string

	rescheduled map[string]time.Time

	saveErr    error
	claimErr   error
	reschedErr error
	deleteErr  error
	expireErr  error
	dueErr     error

	nowFn       func() time.Time
	ignoreLease bool
}

func newFakeOutbox() *fakeOutbox {
	return &fakeOutbox{rows: map[string]*fakePending{}, rescheduled: map[string]time.Time{}}
}

// clock returns the fake's retention clock, defaulting to time.Now.
func (f *fakeOutbox) clock() time.Time {
	if f.nowFn != nil {
		return f.nowFn()
	}
	return time.Now()
}

func (f *fakeOutbox) SavePendingOccurrence(_ context.Context, p state.PendingOccurrence) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.saveErr != nil {
		return false, f.saveErr
	}
	if _, ok := f.rows[p.ID]; ok {
		return false, nil
	}
	// Mirror the store: stamp the retention deadline at first insert and never
	// refresh it on a re-save.
	if p.ExpiresAt.IsZero() {
		p.ExpiresAt = f.clock().Add(state.PendingRetention)
	}
	f.rows[p.ID] = &fakePending{p: p}
	f.order = append(f.order, p.ID)
	f.saved = append(f.saved, p.ID)
	return true, nil
}

func (f *fakeOutbox) ClaimPendingOccurrences(ctx context.Context, now, leaseUntil time.Time, limit int) ([]state.PendingOccurrence, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	var out []state.PendingOccurrence
	for _, id := range f.order {
		r, ok := f.rows[id]
		if !ok {
			continue
		}
		// Mirror the store: expired rows are never claimable.
		if !r.p.ExpiresAt.After(now) {
			continue
		}
		if !f.ignoreLease && (r.p.NextAttempt.After(now) || r.leaseUntil.After(now)) {
			continue
		}
		r.leaseUntil = leaseUntil
		out = append(out, r.p)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (f *fakeOutbox) ExpirePendingOccurrences(_ context.Context, now time.Time, limit int) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.expireErr != nil {
		return 0, f.expireErr
	}
	n := 0
	kept := f.order[:0]
	for _, id := range f.order {
		r, ok := f.rows[id]
		if !ok {
			continue
		}
		if n < limit && !r.p.ExpiresAt.After(now) {
			delete(f.rows, id)
			f.expired = append(f.expired, id)
			n++
			continue
		}
		kept = append(kept, id)
	}
	f.order = kept
	return n, nil
}

func (f *fakeOutbox) ReschedulePendingOccurrence(_ context.Context, id string, nextAttempt time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.reschedErr != nil {
		return f.reschedErr
	}
	r, ok := f.rows[id]
	if !ok {
		return nil
	}
	r.p.Attempts++
	r.p.NextAttempt = nextAttempt
	r.leaseUntil = time.Time{}
	f.rescheduled[id] = nextAttempt
	return nil
}

func (f *fakeOutbox) DeletePendingOccurrence(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deleteErr != nil {
		return f.deleteErr
	}
	if _, ok := f.rows[id]; ok {
		delete(f.rows, id)
		f.deleted = append(f.deleted, id)
	}
	return nil
}

func (f *fakeOutbox) NextPendingDue(ctx context.Context, now time.Time) (time.Time, bool, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dueErr != nil {
		return time.Time{}, false, f.dueErr
	}
	var (
		best time.Time
		ok   bool
	)
	for _, r := range f.rows {
		if !r.p.ExpiresAt.After(now) {
			continue
		}
		// Mirror the store: the wake instant is the earlier of the next claim
		// time and the retention deadline, so a row whose backoff outlives its
		// expiry still wakes the loop for cleanup.
		d := r.p.NextAttempt
		if r.leaseUntil.After(d) {
			d = r.leaseUntil
		}
		if r.p.ExpiresAt.Before(d) {
			d = r.p.ExpiresAt
		}
		if !ok || d.Before(best) {
			best, ok = d, true
		}
	}
	return best, ok, nil
}

// has reports whether a record with id is still present.
func (f *fakeOutbox) has(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.rows[id]
	return ok
}

func (f *fakeOutbox) gotSaved() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.saved...)
}

func (f *fakeOutbox) gotDeleted() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deleted...)
}

func (f *fakeOutbox) gotExpired() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.expired...)
}

// pendingOccurrence builds the occurrence a persisted record reconstructs.
func pendingOccurrence(id, handler string) schedule.Occurrence {
	return schedule.Occurrence{
		App:         "app-" + id,
		Schedule:    "sched-" + id,
		Handler:     handler,
		ScheduledAt: time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC),
	}
}

// seedPending inserts a record for o into ob at the given due instant. The fake
// outbox stamps a 7-day retention deadline from its clock; seedPending pins that
// clock to the due instant so seeded records are deterministic and unexpired
// relative to the frozen test clock.
func seedPending(t *testing.T, ob *fakeOutbox, o schedule.Occurrence, due time.Time, attempts int) {
	t.Helper()
	if ob.nowFn == nil {
		ob.nowFn = func() time.Time { return due }
	}
	inserted, err := ob.SavePendingOccurrence(context.Background(), state.PendingOccurrence{
		ID: o.ID(), App: o.App, Schedule: o.Schedule, Handler: o.Handler,
		ScheduledAt: o.ScheduledAt, NextAttempt: due,
	})
	if err != nil || !inserted {
		t.Fatalf("seed pending = (%v,%v), want (true,nil)", inserted, err)
	}
	if attempts > 0 {
		ob.mu.Lock()
		ob.rows[o.ID()].p.Attempts = attempts
		ob.mu.Unlock()
	}
}

// TestPublishHealthyPathsDoNotTouchOutbox pins that the durable outbox is
// untouched on the healthy paths: a first-attempt success and a clean duplicate
// write no row and delete nothing.
func TestPublishHealthyPathsDoNotTouchOutbox(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name    string
		pub     bool
		wantPub bool
	}{
		{"published", true, true},
		{"duplicate", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := metrics.New()
			fp := newFakePublisher(4)
			fp.published = tc.pub
			ob := newFakeOutbox()
			s := testSchedulerWithMetrics(fp, testLogger(), m)
			s.now = func() time.Time { return frozen }
			s.wait = noWait
			s.SetOutbox(ob)

			o := pendingOccurrence("a", "jobs.a")
			pub, resolved := s.publishOccurrence(context.Background(), o, false)
			if !resolved || pub != tc.wantPub {
				t.Fatalf("publish = (%v,%v), want (%v,true)", pub, resolved, tc.wantPub)
			}
			if got := ob.gotSaved(); len(got) != 0 {
				t.Fatalf("healthy path persisted %v, want none", got)
			}
			if got := ob.gotDeleted(); len(got) != 0 {
				t.Fatalf("healthy path deleted %v, want none", got)
			}
			if got := m.Counter(metrics.MetricSchedulePendingPersisted); got != 0 {
				t.Fatalf("pending_persisted = %d, want 0 on the healthy path", got)
			}
		})
	}
}

// TestPublishExhaustionPersistsOccurrence pins the core durability contract: a
// tick whose bounded in-memory budget is exhausted persists the COMPLETE
// immutable occurrence exactly once, with the SAME identity, due immediately, and
// does not delete it.
func TestPublishExhaustionPersistsOccurrence(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	m := metrics.New()
	fp := newFakePublisher(16)
	fp.err = errBoom
	ob := newFakeOutbox()
	s := testSchedulerWithMetrics(fp, testLogger(), m)
	s.now = func() time.Time { return frozen }
	s.wait = noWait
	s.SetOutbox(ob)

	o := pendingOccurrence("a", "jobs.a")
	pub, resolved := s.publishOccurrence(context.Background(), o, false)
	if pub || resolved {
		t.Fatalf("exhausted publish = (%v,%v), want (false,false)", pub, resolved)
	}
	if got := fp.callCount(); got != 1+len(publishRetryDelays) {
		t.Fatalf("attempts = %d, want %d", got, 1+len(publishRetryDelays))
	}
	if got := ob.gotSaved(); len(got) != 1 || got[0] != o.ID() {
		t.Fatalf("persisted = %v, want exactly [%s]", got, o.ID())
	}
	if got := ob.gotDeleted(); len(got) != 0 {
		t.Fatalf("exhaustion deleted %v, want none", got)
	}
	if !ob.has(o.ID()) {
		t.Fatal("exhausted occurrence is not durably queued")
	}
	if got := m.Counter(metrics.MetricSchedulePendingPersisted); got != 1 {
		t.Fatalf("pending_persisted = %d, want 1", got)
	}
	ob.mu.Lock()
	rec := ob.rows[o.ID()].p
	ob.mu.Unlock()
	if rec.App != o.App || rec.Schedule != o.Schedule || rec.Handler != o.Handler ||
		!rec.ScheduledAt.Equal(o.ScheduledAt) {
		t.Fatalf("persisted intent = %+v, want %+v", rec, o)
	}
	if rec.NextAttempt.After(frozen) {
		t.Fatalf("persisted next_attempt = %v, want <= now (%v)", rec.NextAttempt, frozen)
	}
	if len(s.pendingWake) != 1 {
		t.Fatalf("wake tokens = %d, want the worker woken on persist", len(s.pendingWake))
	}
}

// TestPublishInMemoryRecoveryResolvesPending pins that the durable row written on
// the first failure is deleted once a later in-memory retry resolves with a nil
// error, so a row never outlives a resolved publication.
func TestPublishInMemoryRecoveryResolvesPending(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	m := metrics.New()
	fp := newFakePublisher(8)
	fp.script = []pubResult{
		{published: false, err: errBoom},
		{published: false, err: errBoom},
		{published: true},
	}
	ob := newFakeOutbox()
	s := testSchedulerWithMetrics(fp, testLogger(), m)
	s.now = func() time.Time { return frozen }
	s.wait = noWait
	s.SetOutbox(ob)

	o := pendingOccurrence("a", "jobs.a")
	pub, resolved := s.publishOccurrence(context.Background(), o, false)
	if !resolved || !pub {
		t.Fatalf("publish = (%v,%v), want (true,true)", pub, resolved)
	}
	if got := ob.gotSaved(); len(got) != 1 || got[0] != o.ID() {
		t.Fatalf("persisted = %v, want exactly [%s] (one row on the first failure)", got, o.ID())
	}
	if got := ob.gotDeleted(); len(got) != 1 || got[0] != o.ID() {
		t.Fatalf("deleted = %v, want exactly [%s] after resolution", got, o.ID())
	}
	if ob.has(o.ID()) {
		t.Fatal("resolved occurrence left a durable row behind")
	}
}

// TestPublishCancellationPersistsAndLeavesRecoverable pins that a tick cut short
// by lifecycle cancellation still records its occurrence, and that the write uses
// a cancellation-independent context so a cancelled caller cannot suppress it.
func TestPublishCancellationPersistsAndLeavesRecoverable(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	m := metrics.New()
	fp := newFakePublisher(8)
	fp.err = errBoom
	// A cancelled wait stops the bounded loop after the first failure.
	s := testSchedulerWithMetrics(fp, testLogger(), m)
	s.now = func() time.Time { return frozen }
	s.wait = func(context.Context, time.Duration) bool { return false }
	ob := newFakeOutbox()
	s.SetOutbox(ob)

	o := pendingOccurrence("a", "jobs.a")
	pub, resolved := s.publishOccurrence(context.Background(), o, false)
	if pub || resolved {
		t.Fatalf("cancelled publish = (%v,%v), want (false,false)", pub, resolved)
	}
	if !ob.has(o.ID()) {
		t.Fatal("cancelled publication was not persisted")
	}
	if got := ob.gotDeleted(); len(got) != 0 {
		t.Fatalf("cancelled publication deleted %v, want none", got)
	}
}

// TestRunPendingOncePublishesAndDeletes pins the happy durable retry: a claimed
// record is republished with its ORIGINAL identity and resolved by deleting the
// row on a nil-error publication.
func TestRunPendingOncePublishesAndDeletes(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	m := metrics.New()
	fp := newFakePublisher(4)
	fp.published = true
	ob := newFakeOutbox()
	o := pendingOccurrence("a", "jobs.a")
	seedPending(t, ob, o, frozen.Add(-time.Minute), 0)

	s := testSchedulerWithMetrics(fp, testLogger(), m)
	s.now = func() time.Time { return frozen }
	s.SetOutbox(ob)

	n, err := s.RunPendingOnce(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("RunPendingOnce = (%d,%v), want (1,nil)", n, err)
	}
	calls := fp.got()
	if len(calls) != 1 {
		t.Fatalf("publish calls = %d, want 1", len(calls))
	}
	if calls[0].ID() != o.ID() || calls[0].Handler != o.Handler {
		t.Fatalf("republished %+v, want the original occurrence identity %s", calls[0], o.ID())
	}
	if got := ob.gotDeleted(); len(got) != 1 || got[0] != o.ID() {
		t.Fatalf("deleted = %v, want [%s]", got, o.ID())
	}
	if got := m.Counter(metrics.MetricSchedulePendingRetries); got != 1 {
		t.Fatalf("pending_retries = %d, want 1", got)
	}
}

// TestRunPendingOnceDuplicateResolves pins that a durable retry which finds the
// occurrence already published (a clean duplicate over an ambiguous earlier
// success) is resolved exactly like a fresh publication: the row is deleted.
func TestRunPendingOnceDuplicateResolves(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	fp := newFakePublisher(4)
	fp.published = false // clean duplicate
	ob := newFakeOutbox()
	o := pendingOccurrence("a", "jobs.a")
	seedPending(t, ob, o, frozen.Add(-time.Minute), 0)

	s := testSchedulerWithMetrics(fp, testLogger(), metrics.New())
	s.now = func() time.Time { return frozen }
	s.SetOutbox(ob)

	if n, err := s.RunPendingOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("RunPendingOnce = (%d,%v), want (1,nil)", n, err)
	}
	if got := ob.gotDeleted(); len(got) != 1 || got[0] != o.ID() {
		t.Fatalf("duplicate retry did not resolve: deleted = %v", got)
	}
}

// TestRunPendingOnceFailureReschedules pins the failed-retry path: the record is
// retained, its attempt count increments once, and its next due instant moves out
// by the durable backoff.
func TestRunPendingOnceFailureReschedules(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	fp := newFakePublisher(4)
	fp.err = errBoom
	ob := newFakeOutbox()
	o := pendingOccurrence("a", "jobs.a")
	seedPending(t, ob, o, frozen.Add(-time.Minute), 0)

	s := testSchedulerWithMetrics(fp, testLogger(), metrics.New())
	s.now = func() time.Time { return frozen }
	s.SetOutbox(ob)

	if n, err := s.RunPendingOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("RunPendingOnce = (%d,%v), want (1,nil)", n, err)
	}
	if !ob.has(o.ID()) {
		t.Fatal("failed retry deleted the record")
	}
	ob.mu.Lock()
	rec := ob.rows[o.ID()].p
	next := ob.rescheduled[o.ID()]
	ob.mu.Unlock()
	if rec.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", rec.Attempts)
	}
	want := frozen.Add(pendingBackoff(0))
	if !next.Equal(want) {
		t.Fatalf("next attempt = %v, want %v", next, want)
	}
}

// TestRunPendingOnceDeleteFailureRetainsRow pins the cleanup-failure window: when
// deletion of a resolved record fails, the row is left in place so the retry is
// idempotent (the next durable retry is a clean duplicate) rather than lost.
func TestRunPendingOnceDeleteFailureRetainsRow(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	fp := newFakePublisher(4)
	fp.published = false
	ob := newFakeOutbox()
	ob.deleteErr = errBoom
	o := pendingOccurrence("a", "jobs.a")
	seedPending(t, ob, o, frozen.Add(-time.Minute), 0)

	s := testSchedulerWithMetrics(fp, testLogger(), metrics.New())
	s.now = func() time.Time { return frozen }
	s.SetOutbox(ob)

	if n, err := s.RunPendingOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("RunPendingOnce = (%d,%v), want (1,nil)", n, err)
	}
	if !ob.has(o.ID()) {
		t.Fatal("a failed delete must retain the row for idempotent retry")
	}
}

// TestRunPendingOnceCancellationLeavesRow pins that a lifecycle cancelled before
// a retry runs leaves the claimed record recoverable: the claim query reports the
// cancellation and nothing is deleted.
func TestRunPendingOnceCancellationLeavesRow(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	fp := newFakePublisher(4)
	ob := newFakeOutbox()
	o := pendingOccurrence("a", "jobs.a")
	seedPending(t, ob, o, frozen.Add(-time.Minute), 0)

	s := testSchedulerWithMetrics(fp, testLogger(), metrics.New())
	s.now = func() time.Time { return frozen }
	s.SetOutbox(ob)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.RunPendingOnce(ctx); err == nil {
		t.Fatal("a cancelled scan should surface the context error")
	}
	if !ob.has(o.ID()) {
		t.Fatal("cancelled scan dropped the record")
	}
	if fp.callCount() != 0 || len(ob.gotDeleted()) != 0 {
		t.Fatal("cancelled scan published or deleted")
	}
}

// TestRunPendingOnceClaimExpiryRescan pins the claim-expiry recovery: a record
// leased by a previous (crashed/wedged) retrier becomes claimable again once the
// lease expires.
func TestRunPendingOnceClaimExpiryRescan(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	fp := newFakePublisher(4)
	fp.published = true
	ob := newFakeOutbox()
	o := pendingOccurrence("a", "jobs.a")
	seedPending(t, ob, o, frozen.Add(-time.Minute), 0)
	ob.mu.Lock()
	ob.rows[o.ID()].leaseUntil = frozen.Add(-time.Second)
	ob.mu.Unlock()

	s := testSchedulerWithMetrics(fp, testLogger(), metrics.New())
	s.now = func() time.Time { return frozen }
	s.SetOutbox(ob)

	if n, err := s.RunPendingOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("RunPendingOnce = (%d,%v), want (1,nil) after claim expiry", n, err)
	}
	if got := ob.gotDeleted(); len(got) != 1 {
		t.Fatalf("expired-lease record not reclaimed/resolved: deleted = %v", got)
	}
}

// TestOverlappingImmediateAndRetryPublishesSafe pins that the same occurrence
// published concurrently by two retriers is safe: the atomic publish-if-new makes
// exactly one a fresh publication and the other a clean duplicate, and both
// resolve the row. The fake outbox ignores leases so both callers really do
// observe the same record, and the barrier publisher holds both calls until both
// have entered, so the two publications genuinely overlap (the lease race the
// DB's per-row claim prevents) rather than running back to back.
func TestOverlappingImmediateAndRetryPublishesSafe(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	pub := newBarrierDedupPublisher(2)
	ob := newFakeOutbox()
	ob.ignoreLease = true
	o := pendingOccurrence("a", "jobs.a")
	seedPending(t, ob, o, frozen.Add(-time.Minute), 0)

	makeSched := func() *Scheduler {
		s := testScheduler(pub, testLogger())
		s.now = func() time.Time { return frozen }
		s.SetOutbox(ob)
		return s
	}
	s1, s2 := makeSched(), makeSched()

	var wg sync.WaitGroup
	wg.Add(2)
	for _, s := range []*Scheduler{s1, s2} {
		go func(s *Scheduler) {
			defer wg.Done()
			_, _ = s.RunPendingOnce(context.Background())
		}(s)
	}
	wg.Wait()

	if pub.wins() != 1 || pub.dups() != 1 {
		t.Fatalf("overlapping publishes = wins %d dups %d, want exactly 1 win and 1 duplicate",
			pub.wins(), pub.dups())
	}
	if ob.has(o.ID()) {
		t.Fatal("resolved record left behind after the overlap")
	}
}

// barrierDedupPublisher emulates the cluster-wide publish-if-new contract in
// memory (first ID wins, later publishes are clean duplicates) AND holds each
// call until n calls have entered, so a test can force genuine overlap. The
// entry gate is a counting barrier: each caller registers, and the nth caller
// releases everyone.
type barrierDedupPublisher struct {
	mu      sync.Mutex
	seen    map[string]bool
	win     int
	dup     int
	n       int
	entered int
	release chan struct{}
}

func newBarrierDedupPublisher(n int) *barrierDedupPublisher {
	return &barrierDedupPublisher{seen: map[string]bool{}, n: n, release: make(chan struct{})}
}

func (b *barrierDedupPublisher) PublishOccurrence(_ context.Context, o schedule.Occurrence) (bool, error) {
	b.mu.Lock()
	b.entered++
	if b.entered == b.n {
		close(b.release)
	}
	b.mu.Unlock()
	<-b.release // both callers proceed together

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.seen[o.ID()] {
		b.dup++
		return false, nil
	}
	b.seen[o.ID()] = true
	b.win++
	return true, nil
}

func (b *barrierDedupPublisher) wins() int { b.mu.Lock(); defer b.mu.Unlock(); return b.win }
func (b *barrierDedupPublisher) dups() int { b.mu.Lock(); defer b.mu.Unlock(); return b.dup }

// TestStartPendingRetryScansAndStops pins the worker lifecycle: it scans
// immediately on start (so records persisted before a restart are recovered) and
// Stop joins it.
func TestStartPendingRetryScansAndStops(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	ob := newFakeOutbox()
	o := pendingOccurrence("a", "jobs.a")
	seedPending(t, ob, o, frozen.Add(-time.Minute), 0)

	fp := newSignalPublisher()
	s := testScheduler(fp, testLogger())
	s.now = func() time.Time { return frozen }
	s.SetOutbox(ob)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.StartPendingRetry(ctx)
	select {
	case <-fp.published:
	case <-time.After(3 * time.Second):
		t.Fatal("durable retry worker did not scan the persisted record on start")
	}
	deadline := time.Now().Add(3 * time.Second)
	for ob.has(o.ID()) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if ob.has(o.ID()) {
		t.Fatal("worker did not resolve the persisted record")
	}
	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestStopCancelsAndJoinsPendingRetry pins the shutdown ordering requirement: an
// in-flight durable retry that is blocked in the publisher must be cancelled and
// joined by Stop, so no retry can touch Redis or the state DB after Stop returns.
func TestStopCancelsAndJoinsPendingRetry(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	ob := newFakeOutbox()
	o := pendingOccurrence("a", "jobs.a")
	seedPending(t, ob, o, frozen.Add(-time.Minute), 0)

	fp := newBlockingPendingPublisher()
	s := testScheduler(fp, testLogger())
	s.now = func() time.Time { return frozen }
	s.SetOutbox(ob)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.StartPendingRetry(ctx)
	select {
	case <-fp.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("durable retry never entered the publisher")
	}

	joined := make(chan error, 1)
	go func() { joined <- s.Stop(context.Background()) }()
	// Stop must cancel the in-flight retry; the publisher observes it and
	// returns, letting Stop complete with the row left recoverable.
	select {
	case err := <-joined:
		if err != nil {
			t.Fatalf("Stop = %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not join the in-flight durable retry")
	}
	select {
	case <-fp.cancelled:
	case <-time.After(time.Second):
		t.Fatal("in-flight durable retry did not observe lifecycle cancellation")
	}
	// The attempt was cut short, so the record stays for a later process.
	if !ob.has(o.ID()) {
		t.Fatal("cancelled retry dropped the record instead of leaving it recoverable")
	}
}

// TestPendingRetryLoopDoesNotBusyPoll pins the wait policy: with an empty outbox
// the loop waits indefinitely (d < 0) for a wake/cancellation, and with a
// non-empty outbox it waits exactly until the next due instant rather than
// spinning.
func TestPendingRetryLoopDoesNotBusyPoll(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)

	t.Run("empty outbox waits indefinitely", func(t *testing.T) {
		ob := newFakeOutbox()
		s := testScheduler(newFakePublisher(1), testLogger())
		s.now = func() time.Time { return frozen }
		s.SetOutbox(ob)
		var got []time.Duration
		s.pendingWait = func(context.Context, time.Duration, <-chan struct{}) bool {
			got = append(got, -1)
			return false
		}
		s.pendingRetryLoop(context.Background())
		if len(got) != 1 || got[0] != -1 {
			t.Fatalf("empty-outbox wait = %v, want a single indefinite wait (-1)", got)
		}
	})

	t.Run("future record waits until due", func(t *testing.T) {
		ob := newFakeOutbox()
		o := pendingOccurrence("a", "jobs.a")
		seedPending(t, ob, o, frozen.Add(3*time.Minute), 0)
		s := testScheduler(newFakePublisher(1), testLogger())
		s.now = func() time.Time { return frozen }
		s.SetOutbox(ob)
		var saw time.Duration
		s.pendingWait = func(_ context.Context, d time.Duration, _ <-chan struct{}) bool {
			saw = d
			return false
		}
		s.pendingRetryLoop(context.Background())
		if saw != 3*time.Minute {
			t.Fatalf("wait = %v, want exactly until the next due (3m)", saw)
		}
	})
}

// TestPendingRetryLoopWakesOnPersist pins that persisting a record wakes a
// worker parked on an empty outbox, without it polling.
func TestPendingRetryLoopWakesOnPersist(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	ob := newFakeOutbox()
	s := testScheduler(newFakePublisher(1), testLogger())
	s.now = func() time.Time { return frozen }
	s.SetOutbox(ob)

	waited := make(chan struct{}, 1)
	s.pendingWait = func(ctx context.Context, d time.Duration, wake <-chan struct{}) bool {
		if d >= 0 {
			t.Errorf("empty-outbox wait = %v, want indefinite", d)
		}
		select {
		case <-wake:
			waited <- struct{}{}
			return false
		case <-ctx.Done():
			return false
		}
	}
	done := make(chan struct{})
	go func() { defer close(done); s.pendingRetryLoop(context.Background()) }()

	o := pendingOccurrence("a", "jobs.a")
	if _, err := ob.SavePendingOccurrence(context.Background(), state.PendingOccurrence{
		ID: o.ID(), App: o.App, Schedule: o.Schedule, Handler: o.Handler,
		ScheduledAt: o.ScheduledAt, NextAttempt: frozen,
	}); err != nil {
		t.Fatalf("persist: %v", err)
	}
	s.wakePending()
	select {
	case <-waited:
	case <-time.After(3 * time.Second):
		t.Fatal("persisting a record did not wake the retry worker")
	}
	<-done
}

// TestPendingRetryScanErrorDoesNotSpin pins that a persistent outbox scan error
// pauses before retrying rather than becoming a busy loop.
func TestPendingRetryScanErrorDoesNotSpin(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	ob := newFakeOutbox()
	ob.claimErr = errBoom
	s := testScheduler(newFakePublisher(1), testLogger())
	s.now = func() time.Time { return frozen }
	s.SetOutbox(ob)

	calls := 0
	s.pendingWait = func(context.Context, time.Duration, <-chan struct{}) bool {
		calls++
		return false // stop the loop after the first error pause
	}
	s.pendingRetryLoop(context.Background())
	if calls != 1 {
		t.Fatalf("error pause count = %d, want exactly 1 (no busy loop)", calls)
	}
}

// TestDurableRetryLogsDistinguishOutcomes pins the required log distinction:
// persisting a pending occurrence, a durable retry attempt, a newly published
// pending occurrence, a dedup-resolved pending occurrence, and a retry failure
// are each distinguishable in the logs.
func TestDurableRetryLogsDistinguishOutcomes(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	o := pendingOccurrence("a", "jobs.a")

	// Immediate exhaustion persists the occurrence.
	var bufExhaust testutil.SyncBuffer
	fp := newFakePublisher(16)
	fp.err = errBoom
	s := testSchedulerWithMetrics(fp, slog.New(slog.NewTextHandler(&bufExhaust, &slog.HandlerOptions{Level: slog.LevelDebug})), metrics.New())
	s.now = func() time.Time { return frozen }
	s.wait = noWait
	s.SetOutbox(newFakeOutbox())
	s.publishOccurrence(context.Background(), o, false)
	if !strings.Contains(bufExhaust.String(), "publish retries exhausted") {
		t.Fatalf("missing exhaustion log:\n%s", bufExhaust.String())
	}

	assertLog := func(name string, pub bool, pubErr error, want string) {
		t.Helper()
		ob := newFakeOutbox()
		seedPending(t, ob, o, frozen.Add(-time.Minute), 0)
		p := newFakePublisher(4)
		p.published, p.err = pub, pubErr
		var buf testutil.SyncBuffer
		sc := testSchedulerWithMetrics(p, slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})), metrics.New())
		sc.now = func() time.Time { return frozen }
		sc.SetOutbox(ob)
		if _, err := sc.RunPendingOnce(context.Background()); err != nil {
			t.Fatalf("%s: RunPendingOnce: %v", name, err)
		}
		if !strings.Contains(buf.String(), want) {
			t.Fatalf("%s: logs missing %q:\n%s", name, want, buf.String())
		}
	}
	assertLog("published", true, nil, "pending occurrence published")
	assertLog("duplicate", false, nil, "resolved as duplicate")
	assertLog("failure", false, errBoom, "pending retry failed; rescheduled")
}

// TestPendingRetryIndependentOfLiveScheduleChanges pins that the durable outbox
// is independent of live schedule reconciliation: removing or replacing an app's
// schedules does not delete its pending occurrence, and the retry worker still
// republishes it. This is the reconciler-facing invariant — publication recovery
// is driven by the occurrence that was attempted, not by the current template —
// and it is safe because the stream consumer treats an occurrence whose schedule
// was removed (and never admitted) as obsolete and acknowledges it.
func TestPendingRetryIndependentOfLiveScheduleChanges(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	fp := newFakePublisher(16)
	fp.err = errBoom
	ob := newFakeOutbox()
	s := testSchedulerWithMetrics(fp, testLogger(), metrics.New())
	s.now = func() time.Time { return frozen }
	s.wait = noWait
	s.SetOutbox(ob)
	s.ReplaceApp("app-a", schedTemplate("jobs.a", "0 3 * * *", "", ""))

	o := pendingOccurrence("a", "jobs.a")
	pub, resolved := s.publishOccurrence(context.Background(), o, false)
	if pub || resolved {
		t.Fatalf("publish = (%v,%v), want (false,false)", pub, resolved)
	}
	if !ob.has(o.ID()) {
		t.Fatal("exhausted occurrence not persisted")
	}

	// Live reconcile removes the whole app (and its jobs). The pending record
	// must survive.
	s.RemoveApp("app-a")
	if !ob.has(o.ID()) {
		t.Fatal("RemoveApp dropped a pending occurrence")
	}

	// A healthy retry now succeeds and resolves the record even though the
	// schedule no longer exists locally.
	fp2 := newFakePublisher(4)
	fp2.published = true
	s2 := testSchedulerWithMetrics(fp2, testLogger(), metrics.New())
	s2.now = func() time.Time { return frozen }
	s2.SetOutbox(ob)
	if n, err := s2.RunPendingOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("RunPendingOnce = (%d,%v), want (1,nil)", n, err)
	}
	if ob.has(o.ID()) {
		t.Fatal("durable retry did not resolve the removed app's occurrence")
	}
}

// TestRunPendingOnceCorruptOrMismatchedIdentityNeverPublishes pins the identity
// guard: a claimed record the caller cannot trust as the occurrence its key names
// is NEVER published (which would target the wrong occurrence) and NEVER deleted.
// Two shapes are covered: a decode failure (DecodeErr) and a decodable record
// whose reconstructed ID differs from the row key. Both are logged and
// rescheduled under the bounded backoff, so the durable row is retained.
func TestRunPendingOnceCorruptOrMismatchedIdentityNeverPublishes(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	o := pendingOccurrence("a", "jobs.a")

	cases := []struct {
		name    string
		record  state.PendingOccurrence
		wantLog string
	}{
		{
			name: "decode failure",
			record: state.PendingOccurrence{
				ID:          o.ID(),
				DecodeErr:   errors.New("stored payload is not valid JSON"),
				NextAttempt: frozen.Add(-time.Minute),
				ExpiresAt:   frozen.Add(state.PendingRetention),
			},
			wantLog: "corrupt; retaining for repair",
		},
		{
			name: "mismatched row id",
			// The row key is o.ID(), but the decoded intent reconstructs a
			// DIFFERENT occurrence (a different scheduled instant), so publishing
			// it would publish under another ID and deleting would lose the real
			// row. (Handler is not part of occurrence identity, so it is not a
			// valid mismatch.)
			record: state.PendingOccurrence{
				ID:          o.ID(),
				App:         o.App,
				Schedule:    o.Schedule,
				Handler:     o.Handler,
				ScheduledAt: o.ScheduledAt.Add(time.Hour),
				NextAttempt: frozen.Add(-time.Minute),
				ExpiresAt:   frozen.Add(state.PendingRetention),
			},
			wantLog: "corrupt; retaining for repair",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fp := newFakePublisher(4)
			fp.published = true
			ob := newFakeOutbox()
			// Seed the record verbatim (bypassing the intent encoder) so the
			// mismatched/undecodable shape reaches the claim.
			ob.mu.Lock()
			ob.rows[tc.record.ID] = &fakePending{p: tc.record}
			ob.order = append(ob.order, tc.record.ID)
			ob.mu.Unlock()

			var buf testutil.SyncBuffer
			logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
			s := testSchedulerWithMetrics(fp, logger, metrics.New())
			s.now = func() time.Time { return frozen }
			s.SetOutbox(ob)

			n, err := s.RunPendingOnce(context.Background())
			if err != nil || n != 1 {
				t.Fatalf("RunPendingOnce = (%d,%v), want (1,nil)", n, err)
			}
			if got := fp.callCount(); got != 0 {
				t.Fatalf("publish calls = %d, want 0 (must never publish an untrusted record)", got)
			}
			if got := ob.gotDeleted(); len(got) != 0 {
				t.Fatalf("deleted = %v, want none (must retain the row)", got)
			}
			if !ob.has(tc.record.ID) {
				t.Fatal("record was dropped instead of retained")
			}
			ob.mu.Lock()
			next, rescheduled := ob.rescheduled[tc.record.ID]
			attempts := ob.rows[tc.record.ID].p.Attempts
			ob.mu.Unlock()
			if !rescheduled {
				t.Fatal("corrupt record was not rescheduled (would remain leased without retry/repair)")
			}
			if want := frozen.Add(pendingBackoff(0)); !next.Equal(want) {
				t.Fatalf("rescheduled next = %v, want %v (bounded backoff)", next, want)
			}
			if attempts != 1 {
				t.Fatalf("attempts = %d, want 1", attempts)
			}
			if !strings.Contains(buf.String(), tc.wantLog) {
				t.Fatalf("missing operator-retention log %q:\n%s", tc.wantLog, buf.String())
			}
		})
	}
}

// TestPendingIdentityGuardAcceptsExactOccurrence is covered by
// TestRunPendingOncePublishesAndDeletes: a correctly persisted record
// (key == reconstructed ID) is published and resolved normally.

// TestStopJoinsPendingRetryBeforeGocronStart pins the early-startup-failure
// guarantee: the scheduler barrier is registered BEFORE the durable retry worker
// starts, so a startup failure that cancels the lifecycle (before Scheduler.Start
// is ever called) still strictly joins the retry worker. The worker is blocked
// inside the publisher; Stop must cancel it and not return until it has left, so
// a later state/Redis teardown cannot overlap it.
func TestStopJoinsPendingRetryBeforeGocronStart(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	ob := newFakeOutbox()
	o := pendingOccurrence("a", "jobs.a")
	seedPending(t, ob, o, frozen.Add(-time.Minute), 0)

	fp := newBlockingPendingPublisher()
	s := testScheduler(fp, testLogger())
	s.now = func() time.Time { return frozen }
	s.SetOutbox(ob)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Start the retry worker, but NEVER call s.Start(): gocron is not running,
	// exactly the state reached when startup fails before Scheduler.Start.
	s.StartPendingRetry(ctx)
	select {
	case <-fp.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("durable retry never entered the publisher")
	}

	joined := make(chan error, 1)
	go func() { joined <- s.Stop(context.Background()) }()
	select {
	case err := <-joined:
		if err != nil {
			t.Fatalf("Stop = %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not join the in-flight durable retry on the pre-Start path")
	}
	select {
	case <-fp.cancelled:
	case <-time.After(time.Second):
		t.Fatal("in-flight durable retry did not observe cancellation during Stop")
	}
	// The attempt was cut short, so the row survives for a later process.
	if !ob.has(o.ID()) {
		t.Fatal("cancelled retry dropped the record instead of leaving it recoverable")
	}
}

// TestStartPendingRetryNoopWithoutOutboxOrAfterStop pins the guard rails: with no
// outbox there is nothing to run, and after Stop the worker is not resurrected.
// It deliberately builds a NO-OUTBOX scheduler (raw New) so the "no outbox" leg
// is exercised; production never leaves the scheduler without an outbox (it is
// either installed or the scheduler is marked unavailable).
func TestStartPendingRetryNoopWithoutOutboxOrAfterStop(t *testing.T) {
	s := New(newFakePublisher(1), testLogger())
	// No outbox: start is a no-op and Stop returns immediately.
	s.StartPendingRetry(context.Background())
	s.mu.Lock()
	started := s.pendingDone
	s.mu.Unlock()
	if started != nil {
		t.Fatal("StartPendingRetry started a worker with no outbox")
	}
	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("Stop = %v, want nil", err)
	}
	// After Stop, a late StartPendingRetry must not start a worker.
	ob := newFakeOutbox()
	s.SetOutbox(ob)
	s.StartPendingRetry(context.Background())
	s.mu.Lock()
	done := s.pendingDone
	s.mu.Unlock()
	if done != nil {
		t.Fatal("StartPendingRetry started a worker after Stop")
	}
}

// TestPendingBackoffIsCapped pins that the durable backoff is capped at its final
// value so the retry continues indefinitely rather than growing without bound.
func TestPendingBackoffIsCapped(t *testing.T) {
	last := pendingRetryDelays[len(pendingRetryDelays)-1]
	for _, attempts := range []int{0, 1, len(pendingRetryDelays) - 1, len(pendingRetryDelays), 1000} {
		if got := pendingBackoff(attempts); got <= 0 || got > last {
			t.Fatalf("pendingBackoff(%d) = %v, want in (0,%v]", attempts, got, last)
		}
	}
	if got := pendingBackoff(999); got != last {
		t.Fatalf("pendingBackoff(999) = %v, want the cap %v", got, last)
	}
}

// TestDurableRetryThroughRealStateDB is the end-to-end persistence path with the
// production SQLite outbox: an exhausted immediate publish persists the row,
// the DB is closed (simulating a process exit), reopened, and the durable worker
// resolves the occurrence it finds. It proves the whole chain wires together
// without a fake store.
func TestDurableRetryThroughRealStateDB(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "db.sqlite3")

	st1, err := state.Open(path)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
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
	st2, err := state.Open(path)
	if err != nil {
		t.Fatalf("reopen state: %v", err)
	}
	defer st2.Close()
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

// TestRunPendingOnceJustBeforeExpiryPublishes pins that a record that is still
// inside its retention window when the retry runs is published and resolved
// normally; expiry only affects records past their deadline.
func TestRunPendingOnceJustBeforeExpiryPublishes(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	// The record was persisted at day zero; "now" is one instant before its
	// deadline, so it must still be claimed and published.
	expires := frozen.Add(state.PendingRetention)
	ob := newFakeOutbox()
	ob.nowFn = func() time.Time { return frozen }
	o := pendingOccurrence("a", "jobs.a")
	inserted, err := ob.SavePendingOccurrence(context.Background(), state.PendingOccurrence{
		ID: o.ID(), App: o.App, Schedule: o.Schedule, Handler: o.Handler,
		ScheduledAt: o.ScheduledAt, NextAttempt: frozen.Add(-time.Minute),
	})
	if err != nil || !inserted {
		t.Fatalf("seed = (%v,%v), want (true,nil)", inserted, err)
	}
	// Move the fake row's deadline to exactly `expires` and the scheduler clock
	// to one millisecond before it.
	ob.mu.Lock()
	ob.rows[o.ID()].p.ExpiresAt = expires
	ob.mu.Unlock()

	fp := newFakePublisher(4)
	fp.published = true
	m := metrics.New()
	s := testSchedulerWithMetrics(fp, testLogger(), m)
	s.now = func() time.Time { return expires.Add(-time.Millisecond) }
	s.SetOutbox(ob)

	if n, err := s.RunPendingOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("RunPendingOnce = (%d,%v), want (1,nil)", n, err)
	}
	if got := fp.callCount(); got != 1 {
		t.Fatalf("publish calls = %d, want 1 (just before expiry publishes)", got)
	}
	if ob.has(o.ID()) {
		t.Fatal("resolved record left behind")
	}
	if got := m.Counter(metrics.MetricSchedulePendingExpired); got != 0 {
		t.Fatalf("expired = %d, want 0 (record had not expired)", got)
	}
}

// TestRunPendingOnceExpiredDoesNotPublishAndCleansUp pins the expiry contract:
// a record whose deadline passed is NEVER published, is removed from the outbox,
// increments the expiration counter (and NOT the retry/success/failure/duplicate
// counters), and logs a structured expiration. It covers both the store's claim
// exclusion and the claim-to-publish guard: here the record is expired before
// the claim, so it is removed by the bounded cleanup.
func TestRunPendingOnceExpiredDoesNotPublishAndCleansUp(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	o := pendingOccurrence("a", "jobs.a")

	ob := newFakeOutbox()
	ob.nowFn = func() time.Time { return frozen }
	inserted, err := ob.SavePendingOccurrence(context.Background(), state.PendingOccurrence{
		ID: o.ID(), App: o.App, Schedule: o.Schedule, Handler: o.Handler,
		ScheduledAt: o.ScheduledAt, NextAttempt: frozen.Add(-time.Minute),
	})
	if err != nil || !inserted {
		t.Fatalf("seed = (%v,%v), want (true,nil)", inserted, err)
	}
	// The row's deadline is in the past at the scheduler's clock.
	ob.mu.Lock()
	ob.rows[o.ID()].p.ExpiresAt = frozen.Add(-time.Second)
	ob.mu.Unlock()

	fp := newFakePublisher(4)
	fp.published = true
	m := metrics.New()
	var buf testutil.SyncBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	s := testSchedulerWithMetrics(fp, logger, m)
	s.now = func() time.Time { return frozen }
	s.SetOutbox(ob)

	n, err := s.RunPendingOnce(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("RunPendingOnce = (%d,%v), want (1,nil) (one expired record handled)", n, err)
	}
	if got := fp.callCount(); got != 0 {
		t.Fatalf("publish calls = %d, want 0 (an expired record is never published)", got)
	}
	if ob.has(o.ID()) {
		t.Fatal("expired record was not removed")
	}
	if got := m.Counter(metrics.MetricSchedulePendingExpired); got != 1 {
		t.Fatalf("schedule_pending_expired_total = %d, want 1", got)
	}
	// Expiration is NOT a publish outcome.
	if got := m.Counter(metrics.MetricSchedulePendingRetries); got != 0 {
		t.Fatalf("pending_retries = %d, want 0", got)
	}
	if got := m.Counter(metrics.MetricScheduleOccurrencesPublished); got != 0 {
		t.Fatalf("occurrences_published = %d, want 0", got)
	}
	if got := m.Counter(metrics.MetricSchedulePublishFailures); got != 0 {
		t.Fatalf("publish_failures = %d, want 0", got)
	}
	if got := m.Counter(metrics.MetricScheduleOccurrencesDuplicate); got != 0 {
		t.Fatalf("occurrences_duplicate = %d, want 0", got)
	}
	if !strings.Contains(buf.String(), "expired pending occurrences removed") {
		t.Fatalf("missing structured expiration log:\n%s", buf.String())
	}
}

// TestRunPendingOnceClaimedRecordExpiresBeforePublish pins the claim-to-publish
// time boundary: a record that is not yet expired when claimed (simulated by the
// outbox returning it) but whose deadline has passed by the time retryPending
// runs is removed by retryPending WITHOUT a publisher call and counted as
// expiration, never rescheduled.
func TestRunPendingOnceClaimedRecordExpiresBeforePublish(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	o := pendingOccurrence("a", "jobs.a")

	ob := newFakeOutbox()
	ob.nowFn = func() time.Time { return frozen }
	inserted, err := ob.SavePendingOccurrence(context.Background(), state.PendingOccurrence{
		ID: o.ID(), App: o.App, Schedule: o.Schedule, Handler: o.Handler,
		ScheduledAt: o.ScheduledAt, NextAttempt: frozen.Add(-time.Minute),
	})
	if err != nil || !inserted {
		t.Fatalf("seed = (%v,%v), want (true,nil)", inserted, err)
	}
	// Let the claim succeed: expire the row exactly AT now (expires_at <= now is
	// expired, but the fake claim excludes it), so the only way it reaches
	// retryPending is via the direct call below.
	ob.mu.Lock()
	ob.rows[o.ID()].p.ExpiresAt = frozen
	ob.mu.Unlock()

	fp := newFakePublisher(4)
	fp.published = true
	m := metrics.New()
	var buf testutil.SyncBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	s := testSchedulerWithMetrics(fp, logger, m)
	s.now = func() time.Time { return frozen }
	s.SetOutbox(ob)

	// Drive retryPending directly with the expired record: the claim would
	// exclude it, but a record expiring between claim and publish must still be
	// rejected by the guard.
	ob.mu.Lock()
	rec := ob.rows[o.ID()].p
	ob.mu.Unlock()
	s.retryPending(context.Background(), rec, logger)
	if got := fp.callCount(); got != 0 {
		t.Fatalf("publish calls = %d, want 0 (expired between claim and publish)", got)
	}
	if ob.has(o.ID()) {
		t.Fatal("record expired before publish was not removed")
	}
	if got := m.Counter(metrics.MetricSchedulePendingExpired); got != 1 {
		t.Fatalf("schedule_pending_expired_total = %d, want 1", got)
	}
	if got := m.Counter(metrics.MetricSchedulePendingRetries); got != 0 {
		t.Fatalf("pending_retries = %d, want 0 (expiry is not a retry)", got)
	}
	if !strings.Contains(buf.String(), "expired") {
		t.Fatalf("missing expiration log:\n%s", buf.String())
	}
}

// TestRunPendingOnceExpiredCleanupIsBounded pins that a backlog of expired
// records is drained in bounded batches: one RunPendingOnce removes at most
// pendingCleanupBatch, and returns a positive count so the worker scans again.
func TestRunPendingOnceExpiredCleanupIsBounded(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	m := metrics.New()
	fp := newFakePublisher(1)
	ob := newFakeOutbox()
	ob.nowFn = func() time.Time { return frozen }
	total := pendingCleanupBatch + 3
	for i := 0; i < total; i++ {
		o := pendingOccurrence("e"+string(rune('a'+i/26))+string(rune('0'+i%26)), "jobs.e")
		if _, err := ob.SavePendingOccurrence(context.Background(), state.PendingOccurrence{
			ID: o.ID(), App: o.App, Schedule: o.Schedule, Handler: o.Handler,
			ScheduledAt: o.ScheduledAt, NextAttempt: frozen.Add(-time.Minute),
		}); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
		ob.mu.Lock()
		ob.rows[o.ID()].p.ExpiresAt = frozen.Add(-time.Second)
		ob.mu.Unlock()
	}

	s := testSchedulerWithMetrics(fp, testLogger(), m)
	s.now = func() time.Time { return frozen }
	s.SetOutbox(ob)

	n, err := s.RunPendingOnce(context.Background())
	if err != nil {
		t.Fatalf("RunPendingOnce: %v", err)
	}
	if n != pendingCleanupBatch {
		t.Fatalf("first cleanup = %d, want the bounded batch %d", n, pendingCleanupBatch)
	}
	if got := m.Counter(metrics.MetricSchedulePendingExpired); got != int64(pendingCleanupBatch) {
		t.Fatalf("expired = %d, want %d", got, pendingCleanupBatch)
	}
	// A second pass drains the remainder.
	n, err = s.RunPendingOnce(context.Background())
	if err != nil || n != 3 {
		t.Fatalf("second cleanup = (%d,%v), want (3,nil)", n, err)
	}
	if got := m.Counter(metrics.MetricSchedulePendingExpired); got != int64(total) {
		t.Fatalf("expired total = %d, want %d", got, total)
	}
	if _, ok, err := s.NextPendingDue(context.Background()); err != nil || ok {
		t.Fatalf("NextPendingDue after cleanup = (ok %v, err %v), want (false,nil)", ok, err)
	}
}

// TestExpiredRecordIsNotRetryableAfterRestart pins the restart contract with the
// real SQLite outbox: a row inside retention remains retryable after a process
// restart, while a row whose deadline has passed is cleaned after restart and
// never published. The scheduler's injectable clock stands in for wall time, so
// the "expired" case uses a clock past the row's real 7-day deadline.
func TestExpiredRecordIsNotRetryableAfterRestart(t *testing.T) {
	o := pendingOccurrence("a", "jobs.a")
	save := func(t *testing.T, path string) {
		t.Helper()
		st, err := state.Open(path)
		if err != nil {
			t.Fatalf("open state: %v", err)
		}
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
		path := filepath.Join(t.TempDir(), "db.sqlite3")
		save(t, path)
		st, err := state.Open(path)
		if err != nil {
			t.Fatalf("reopen state: %v", err)
		}
		defer st.Close()
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
		path := filepath.Join(t.TempDir(), "db.sqlite3")
		save(t, path)
		st, err := state.Open(path)
		if err != nil {
			t.Fatalf("reopen state: %v", err)
		}
		defer st.Close()
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

// TestPendingRetryDay6AmbiguousPublishResolvesAsDuplicate pins the retry/dedup
// invariant at its most important point: a record persisted at day 0 that is
// still retrying at day 6 (inside its 7-day retention) where the original publish
// actually succeeded but its response was lost. The publisher models that: the
// first call commits the entry in its dedup state but reports a Redis error (the
// response was lost), and the retry sees the still-live key and returns a clean
// duplicate. The row is deleted and only one logical stream entry/ID ever exists.
func TestPendingRetryDay6AmbiguousPublishResolvesAsDuplicate(t *testing.T) {
	day0 := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	day6 := day0.Add(6 * 24 * time.Hour)
	o := pendingOccurrence("a", "jobs.a")

	// One shared publisher: it commits the occurrence but loses the first reply.
	pub := &ambiguousPublisher{entries: map[string]int{}}
	// The original, ambiguous publish at day 0.
	if _, err := pub.PublishOccurrence(context.Background(), o); err == nil {
		t.Fatal("the ambiguous attempt must surface an error")
	}
	if pub.streamEntries() != 1 {
		t.Fatalf("stream entries = %d, want 1 (committed but reply lost)", pub.streamEntries())
	}

	// The outbox stamps the real 7-day deadline from day 0.
	ob := newFakeOutbox()
	ob.nowFn = func() time.Time { return day0 }
	inserted, err := ob.SavePendingOccurrence(context.Background(), state.PendingOccurrence{
		ID: o.ID(), App: o.App, Schedule: o.Schedule, Handler: o.Handler,
		ScheduledAt: o.ScheduledAt, NextAttempt: day0.Add(-time.Minute),
	})
	if err != nil || !inserted {
		t.Fatalf("seed = (%v,%v), want (true,nil)", inserted, err)
	}

	// At day 6 the durable retry runs, still inside retention, through the SAME
	// publisher, and resolves the record as a clean duplicate against the
	// still-live dedup key.
	m := metrics.New()
	s := testSchedulerWithMetrics(pub, testLogger(), m)
	s.now = func() time.Time { return day6 }
	s.SetOutbox(ob)

	if n, err := s.RunPendingOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("RunPendingOnce = (%d,%v), want (1,nil)", n, err)
	}
	if ob.has(o.ID()) {
		t.Fatal("ambiguous occurrence was not resolved/deleted")
	}
	if got := m.Counter(metrics.MetricSchedulePendingExpired); got != 0 {
		t.Fatalf("expired = %d, want 0 (day 6 is inside retention)", got)
	}
	// The dedup key is still live at day 6 (14-day TTL from day 0), so the retry
	// saw the original and did not publish a second entry.
	if pub.streamEntries() != 1 {
		t.Fatalf("stream entries = %d, want 1 (no second entry for the same occurrence)", pub.streamEntries())
	}
	if got := m.Counter(metrics.MetricSchedulePendingRetries); got != 1 {
		t.Fatalf("pending_retries = %d, want 1 (the record was retried, not expired)", got)
	}
}

// ambiguousPublisher models the atomic publish-if-new contract as a Redis-like
// dedup: at most one entry per occurrence ID, ever. Its first attempt records the
// entry but reports an error, modeling an XADD whose reply was lost; every later
// attempt for the same ID is a clean duplicate.
type ambiguousPublisher struct {
	mu      sync.Mutex
	entries map[string]int
}

func (p *ambiguousPublisher) PublishOccurrence(_ context.Context, o schedule.Occurrence) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.entries[o.ID()] > 0 {
		return false, nil // clean duplicate against the live dedup key
	}
	p.entries[o.ID()] = 1
	return false, errors.New("redis: response lost after commit")
}

func (p *ambiguousPublisher) streamEntries() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, c := range p.entries {
		n += c
	}
	return n
}

// signalPublisher signals every publish attempt through a channel and otherwise
// succeeds, so tests can await a durable retry deterministically.
type signalPublisher struct {
	published chan struct{}
}

func newSignalPublisher() *signalPublisher {
	return &signalPublisher{published: make(chan struct{}, 16)}
}

func (p *signalPublisher) PublishOccurrence(context.Context, schedule.Occurrence) (bool, error) {
	select {
	case p.published <- struct{}{}:
	default:
	}
	return true, nil
}

// blockingPendingPublisher blocks inside a publish until its context is
// cancelled, signalling entry and cancellation, so the Stop join can be observed
// without sleeps.
type blockingPendingPublisher struct {
	entered   chan struct{}
	cancelled chan struct{}
	once      sync.Once
	cancelOne sync.Once
}

func newBlockingPendingPublisher() *blockingPendingPublisher {
	return &blockingPendingPublisher{entered: make(chan struct{}), cancelled: make(chan struct{})}
}

func (b *blockingPendingPublisher) PublishOccurrence(ctx context.Context, _ schedule.Occurrence) (bool, error) {
	b.once.Do(func() { close(b.entered) })
	<-ctx.Done()
	b.cancelOne.Do(func() { close(b.cancelled) })
	return false, ctx.Err()
}

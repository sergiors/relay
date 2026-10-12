package cron

import (
	"context"
	"sync"
	"testing"
	"time"

	"relay/internal/observability/metrics"
	"relay/internal/schedule"
)

// awaitWaiterBlocked waits until retain signals that a goroutine registered as a
// waiter for a schedule (and is therefore blocked on its occupied slot).
func awaitWaiterBlocked(t *testing.T, blocked <-chan struct{}) {
	t.Helper()
	select {
	case <-blocked:
	case <-time.After(3 * time.Second):
		t.Fatal("waiter never blocked on the occupied schedule slot")
	}
}

// TestIntentPersistFailureRetainsWithoutPublishing pins the intent-first rule:
// when the durable outbox rejects the write, the tick is NOT published (Redis is
// never contacted) and the EXACT occurrence is retained in memory, with the
// durable retry worker woken, until the store answers again.
func TestIntentPersistFailureRetainsWithoutPublishing(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	m := metrics.New()
	fp := newFakePublisher(8)
	fp.err = errBoom
	ob := newFakeOutbox()
	ob.saveErr = errBoom

	s := testSchedulerWithMetrics(fp, testLogger(), m)
	s.now = func() time.Time { return frozen }
	s.wait = noWait
	s.SetOutbox(ob)

	o := pendingOccurrence("a", "jobs.a")
	if pub, resolved := s.publishOccurrence(context.Background(), o, false); pub || resolved {
		t.Fatalf("persist-failed publish = (%v,%v), want (false,false)", pub, resolved)
	}
	if got := fp.callCount(); got != 0 {
		t.Fatalf("publish attempts = %d, want 0 (Redis must not be contacted)", got)
	}
	if ob.has(o.ID()) {
		t.Fatal("nothing must be durably recorded when the save failed")
	}
	held := s.unsaved.snapshot()
	if len(held) != 1 || held[0].ID() != o.ID() ||
		held[0].App != o.App || held[0].Schedule != o.Schedule ||
		held[0].Handler != o.Handler || !held[0].ScheduledAt.Equal(o.ScheduledAt) {
		t.Fatalf("unsaved = %+v, want the exact occurrence %+v", held, o)
	}
	if len(s.pendingWake) != 1 {
		t.Fatal("a persist failure did not wake the durable retry worker")
	}
	if !s.Degraded() {
		t.Fatal("a persist failure did not degrade the scheduler")
	}
}

// TestIntentPersistenceRecoversInPlace pins that a held occurrence is flushed to
// the outbox by a retry cycle once the store answers again, IN THE SAME PROCESS,
// with no restart and no unrelated schedule tick, and is then published and
// resolved under its original identity.
func TestIntentPersistenceRecoversInPlace(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	fp := newFakePublisher(8)
	fp.published = true
	ob := newFakeOutbox()
	ob.saveErr = errBoom

	s := testSchedulerWithMetrics(fp, testLogger(), metrics.New())
	s.now = func() time.Time { return frozen }
	s.wait = noWait
	s.SetOutbox(ob)

	o := pendingOccurrence("a", "jobs.a")
	if _, resolved := s.publishOccurrence(context.Background(), o, false); resolved {
		t.Fatal("expected the persist-failed publish to be unresolved")
	}
	if ob.has(o.ID()) {
		t.Fatal("row unexpectedly durable while the store was failing")
	}

	// The store recovers; the durable retry worker's next cycle flushes the exact
	// held occurrence, then claims and publishes it. No restart, no schedule tick.
	ob.setSaveErr(nil)
	n, err := s.RunPendingOnce(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("RunPendingOnce = (%d,%v), want (1,nil)", n, err)
	}
	if s.unsaved.len() != 0 {
		t.Fatal("held occurrence was not released after a successful flush")
	}
	if ob.has(o.ID()) {
		t.Fatal("resolved occurrence left a durable row behind")
	}
	calls := fp.got()
	if len(calls) != 1 || calls[0].ID() != o.ID() {
		t.Fatalf("published = %v, want exactly the held occurrence %s", calls, o.ID())
	}
}

// TestIntentDoubleFailureRetainsThenRecovers pins the F-006 fix end to end: Redis
// and the durable store both failing no longer loses the tick. The exact
// occurrence is held in memory, flushed to the outbox once the store recovers,
// and published once Redis recovers — the SAME identity throughout.
func TestIntentDoubleFailureRetainsThenRecovers(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	fp := newFakePublisher(16)
	fp.err = errBoom
	ob := newFakeOutbox()
	ob.saveErr = errBoom

	s := testSchedulerWithMetrics(fp, testLogger(), metrics.New())
	s.now = func() time.Time { return frozen }
	s.wait = noWait
	s.SetOutbox(ob)

	o := pendingOccurrence("a", "jobs.a")
	if _, resolved := s.publishOccurrence(context.Background(), o, false); resolved {
		t.Fatal("the double failure must be unresolved")
	}
	if fp.callCount() != 0 {
		t.Fatal("Redis must not be contacted when the intent is not durable")
	}

	// Store recovers, Redis still down: the worker flushes the exact occurrence
	// durably, then a retry fails and reschedules it.
	ob.setSaveErr(nil)
	if n, err := s.RunPendingOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("flush+retry = (%d,%v), want (1,nil)", n, err)
	}
	if !ob.has(o.ID()) {
		t.Fatal("occurrence was not durably queued after the store recovered")
	}
	if fp.callCount() == 0 {
		t.Fatal("durable retry did not attempt publication")
	}

	// Redis recovers: the next due retry publishes and resolves the occurrence.
	fp.mu.Lock()
	fp.err = nil
	fp.published = true
	fp.mu.Unlock()
	later := frozen.Add(pendingRetryDelays[0] + time.Second)
	s.now = func() time.Time { return later }
	if n, err := s.RunPendingOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("recovery retry = (%d,%v), want (1,nil)", n, err)
	}
	if ob.has(o.ID()) {
		t.Fatal("resolved occurrence left a durable row behind")
	}
	if got := fp.callsFor(o.ID()); got == 0 {
		t.Fatal("occurrence was never published under its original identity")
	}
}

// TestIntentUnpersistedCrashIsTheDocumentedLossWindow models the fundamental
// limitation: if every durable store rejects the initial persistence and the
// process then exits (crashes) before the store recovers, the held intent is lost
// — a restart has no row to recover and publishes nothing. The loss is bounded to
// exactly that window; it is not silent in-process (the occurrence is logged, the
// scheduler degrades, and it is retained until shutdown).
func TestIntentUnpersistedCrashIsTheDocumentedLossWindow(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	fp := newFakePublisher(4)
	ob := newFakeOutbox()
	ob.saveErr = errBoom

	s := testSchedulerWithMetrics(fp, testLogger(), metrics.New())
	s.now = func() time.Time { return frozen }
	s.wait = noWait
	s.SetOutbox(ob)
	o := pendingOccurrence("a", "jobs.a")
	s.publishOccurrence(context.Background(), o, false)
	if s.unsaved.len() != 1 {
		t.Fatal("occurrence not held before the simulated crash")
	}

	// The process "crashes" — the scheduler and its in-memory hold are discarded.
	// Nothing was ever durable, so a restarted process with a healthy store finds
	// no row and republishes nothing.
	fp2 := newFakePublisher(4)
	fp2.published = true
	ob.setSaveErr(nil)
	s2 := testSchedulerWithMetrics(fp2, testLogger(), metrics.New())
	s2.now = func() time.Time { return frozen }
	s2.SetOutbox(ob)
	if n, err := s2.RunPendingOnce(context.Background()); err != nil || n != 0 {
		t.Fatalf("restart retry = (%d,%v), want (0,nil) (no durable row to recover)", n, err)
	}
	if fp2.callCount() != 0 {
		t.Fatal("a crashed, never-durable occurrence must not be recovered")
	}
	if ob.has(o.ID()) {
		t.Fatal("no row should exist for the lost occurrence")
	}
}

// TestIntentMultipleSchedulesRetainedIndependently pins that several callbacks
// already in flight when degradation begins are ALL kept: distinct schedules
// occupy distinct slots, none overwrites another, and a clean store flush
// persists every exact occurrence. The publish calls are UNGATED to model
// callbacks that already passed the pause gate before the first failure degraded
// the scheduler.
func TestIntentMultipleSchedulesRetainedIndependently(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	fp := newFakePublisher(8)
	fp.err = errBoom
	ob := newFakeOutbox()
	ob.saveErr = errBoom

	s := testSchedulerWithMetrics(fp, testLogger(), metrics.New())
	s.now = func() time.Time { return frozen }
	s.wait = noWait
	s.SetOutbox(ob)

	oa := pendingOccurrence("a", "jobs.a")
	ob2 := pendingOccurrence("b", "jobs.b")
	for _, o := range []schedule.Occurrence{oa, ob2} {
		if _, resolved := s.publish(context.Background(), o, false, false); resolved {
			t.Fatalf("publish %s unexpectedly resolved", o.ID())
		}
	}
	held := s.unsaved.snapshot()
	if len(held) != 2 {
		t.Fatalf("held = %d occurrences, want 2", len(held))
	}
	ids := map[string]bool{}
	for _, h := range held {
		ids[h.ID()] = true
	}
	if !ids[oa.ID()] || !ids[ob2.ID()] {
		t.Fatalf("held = %v, want both %s and %s", held, oa.ID(), ob2.ID())
	}

	// A clean store flush persists both exact occurrences and releases the slots.
	ob.setSaveErr(nil)
	if err := s.flushUnsaved(s.log); err != nil {
		t.Fatalf("flushUnsaved: %v", err)
	}
	if !ob.has(oa.ID()) || !ob.has(ob2.ID()) {
		t.Fatalf("both occurrences must be durable; have a=%v b=%v", ob.has(oa.ID()), ob.has(ob2.ID()))
	}
	if s.unsaved.len() != 0 {
		t.Fatal("flush did not release the held slots")
	}
}

// TestUnsavedOccurrencesSameScheduleKeepsBoth pins the ownership model for one
// schedule: a second occurrence for a schedule already holding one WAITS for the
// slot and never overwrites the first; releasing the first admits the second.
func TestUnsavedOccurrencesSameScheduleKeepsBoth(t *testing.T) {
	first := pendingOccurrence("a", "jobs.a")
	second := first
	second.ScheduledAt = first.ScheduledAt.Add(time.Minute)

	u := newUnsavedOccurrences()
	if !u.retain(context.Background(), first) {
		t.Fatal("first retain failed")
	}
	if got := u.snapshot(); len(got) != 1 || got[0].ID() != first.ID() {
		t.Fatalf("after first retain, held = %v", got)
	}

	entered := make(chan struct{})
	done := make(chan bool, 1)
	go func() {
		close(entered)
		done <- u.retain(context.Background(), second)
	}()
	<-entered
	// The second occurrence cannot have entered the slot: the first still owns it.
	if got := u.snapshot(); len(got) != 1 || got[0].ID() != first.ID() {
		t.Fatalf("second occurrence overwrote the first: held = %v", got)
	}

	u.release(first)
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("second retain must succeed once the slot is released")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("release did not wake the waiting retain")
	}
	got := u.snapshot()
	if len(got) != 1 || got[0].ID() != second.ID() {
		t.Fatalf("after release, held = %v, want the second occurrence", got)
	}
	u.release(second)
	if u.len() != 0 {
		t.Fatal("set not empty after releasing both")
	}
}

// TestUnsavedOccurrencesCancelDoesNotOverwrite pins that a waiting retain gives up
// on lifecycle cancellation WITHOUT overwriting the held occurrence.
func TestUnsavedOccurrencesCancelDoesNotOverwrite(t *testing.T) {
	first := pendingOccurrence("a", "jobs.a")
	second := first
	second.ScheduledAt = first.ScheduledAt.Add(time.Minute)

	u := newUnsavedOccurrences()
	if !u.retain(context.Background(), first) {
		t.Fatal("first retain failed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() { done <- u.retain(ctx, second) }()
	cancel()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("retain must report false when the lifecycle is cancelled")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("waiting retain did not observe cancellation")
	}
	if got := u.snapshot(); len(got) != 1 || got[0].ID() != first.ID() {
		t.Fatalf("cancelled retain altered the held set: %v", got)
	}
}

// TestIntentAmbiguousSuccessResolvesAsDuplicate pins that an ambiguous publish
// (the entry committed but the call reported an error) is resolved as a clean
// duplicate on the next attempt, so only one stream entry ever exists and the
// durable row is deleted.
func TestIntentAmbiguousSuccessResolvesAsDuplicate(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	pub := &ambiguousPublisher{entries: map[string]int{}}
	ob := newFakeOutbox()
	s := testSchedulerWithMetrics(pub, testLogger(), metrics.New())
	s.now = func() time.Time { return frozen }
	s.wait = noWait
	s.SetOutbox(ob)

	o := pendingOccurrence("a", "jobs.a")
	published, resolved := s.publishOccurrence(context.Background(), o, false)
	if published || !resolved {
		t.Fatalf("ambiguous publish = (%v,%v), want (false,true) (resolved as duplicate)", published, resolved)
	}
	if ob.has(o.ID()) {
		t.Fatal("resolved ambiguous occurrence left a durable row behind")
	}
	if got := pub.streamEntries(); got != 1 {
		t.Fatalf("stream entries = %d, want 1 (no second entry for the same occurrence)", got)
	}
}

// TestRecoveryFlushesHeldOccurrenceBeforeCatchUp pins the recovery ordering
// requirement: an occurrence held after a failed initial persist is written to
// the outbox (and published) BEFORE the bounded catch-up synthesizes a newer
// occurrence for the same schedule. The held intent is therefore never
// reconstructed from the current time or lost behind a newer occurrence.
func TestRecoveryFlushesHeldOccurrenceBeforeCatchUp(t *testing.T) {
	now := time.Date(2026, 7, 2, 10, 0, 0, 0, time.UTC)
	fp := newFakePublisher(8)
	ob := newFakeOutbox()
	ob.saveErr = errBoom

	s := testSchedulerWithMetrics(fp, testLogger(), metrics.New())
	s.now = func() time.Time { return now }
	s.wait = noWait
	s.SetOutbox(ob)
	s.ReplaceApp("fn", schedTemplate("jobs.a", "0 3 * * *", "", ""))

	// A due occurrence from an earlier tick is held after its persist failed.
	held := schedule.Occurrence{
		App: "fn", Schedule: "jobs.a", Handler: "jobs.a",
		ScheduledAt: time.Date(2026, 7, 1, 3, 0, 0, 0, time.UTC),
	}
	if _, resolved := s.publish(context.Background(), held, false, false); resolved {
		t.Fatal("expected the held occurrence to be unresolved")
	}
	if s.unsaved.len() != 1 {
		t.Fatal("occurrence was not retained")
	}

	// The store recovers; recovery must flush the exact held occurrence and
	// publish it BEFORE the bounded catch-up's newer (03:00 today) occurrence.
	ob.setSaveErr(nil)
	fp.published = true
	if !s.attemptRecovery(context.Background(), testLogger()) {
		t.Fatal("attemptRecovery reported still degraded with a healthy store")
	}
	calls := fp.got()
	if len(calls) < 2 {
		t.Fatalf("publish calls = %d, want the held occurrence and the catch-up occurrence", len(calls))
	}
	if calls[0].ID() != held.ID() {
		t.Fatalf("first publish = %s, want the HELD occurrence %s (flushed before catch-up)", calls[0].ID(), held.ID())
	}
	wantCatchUp := schedule.Occurrence{
		App: "fn", Schedule: "jobs.a", Handler: "jobs.a",
		ScheduledAt: time.Date(2026, 7, 2, 3, 0, 0, 0, time.UTC),
	}
	if calls[1].ID() != wantCatchUp.ID() {
		t.Fatalf("second publish = %s, want the catch-up occurrence %s", calls[1].ID(), wantCatchUp.ID())
	}
	if s.unsaved.len() != 0 {
		t.Fatal("recovery did not release the held occurrence")
	}
	if ob.has(held.ID()) {
		t.Fatal("flushed occurrence left a durable row behind after resolving")
	}
}

// TestDegradeWakeProbesWithoutBusyPolling pins the recovery-wake contract: a
// transition into degraded wakes a retry worker parked on an empty outbox into an
// immediate recovery probe, and a persistently failing store then backs off with
// the existing pending error delay rather than spinning.
func TestDegradeWakeProbesWithoutBusyPolling(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	ob := newFakeOutbox()
	s := testScheduler(newFakePublisher(1), testLogger())
	s.now = func() time.Time { return frozen }
	s.SetOutbox(ob)

	parked := make(chan struct{})
	var (
		mu     sync.Mutex
		delays []time.Duration
		first  = true
	)
	s.pendingWait = func(ctx context.Context, d time.Duration, wake <-chan struct{}) bool {
		mu.Lock()
		if first {
			first = false
			mu.Unlock()
			if d >= 0 {
				t.Errorf("empty-outbox wait = %v, want indefinite", d)
			}
			close(parked)
			select {
			case <-wake:
				return true
			case <-ctx.Done():
				return false
			}
		}
		mu.Unlock()
		delays = append(delays, d)
		return false // stop the loop after the post-probe pause
	}
	done := make(chan struct{})
	go func() { defer close(done); s.pendingRetryLoop(context.Background()) }()

	select {
	case <-parked:
	case <-time.After(3 * time.Second):
		t.Fatal("retry worker did not park on the empty outbox")
	}
	// The store fails and the scheduler degrades: the parked worker must be woken
	// into an immediate probe.
	ob.setDueErr(errBoom)
	s.markDegraded(errBoom)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("retry worker did not finish after the degrade wake")
	}
	if ob.gotDueCalls() == 0 {
		t.Fatal("a degrade transition did not cause an immediate recovery probe")
	}
	mu.Lock()
	got := append([]time.Duration(nil), delays...)
	mu.Unlock()
	if len(got) != 1 || got[0] != pendingErrorDelay {
		t.Fatalf("post-probe waits = %v, want exactly one %v (no busy polling)", got, pendingErrorDelay)
	}
}

// TestRecoveryFlushesHeldAndAdmittedWaiterInOneCycle pins the drain contract: an
// occurrence A held after a failed persist, and a later same-schedule occurrence
// B blocked on A's slot, are BOTH flushed to the outbox and published by a SINGLE
// retry cycle once the store recovers. This is the race a one-shot snapshot
// missed: releasing A admits B, and the drain loop must observe and flush B
// before concluding, with no unrelated event or restart involved.
func TestRecoveryFlushesHeldAndAdmittedWaiterInOneCycle(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	fp := newFakePublisher(8)
	fp.published = true
	ob := newFakeOutbox()
	ob.saveErr = errBoom

	s := testSchedulerWithMetrics(fp, testLogger(), metrics.New())
	s.now = func() time.Time { return frozen }
	s.wait = noWait
	s.SetOutbox(ob)

	oa := pendingOccurrence("a", "jobs.a")
	ob2 := oa
	ob2.ScheduledAt = oa.ScheduledAt.Add(time.Minute) // later tick, SAME schedule slot

	if _, resolved := s.publishOccurrence(context.Background(), oa, false); resolved {
		t.Fatal("A unexpectedly resolved")
	}
	if s.unsaved.len() != 1 {
		t.Fatalf("held = %d, want A held", s.unsaved.len())
	}

	blocked := make(chan struct{}, 1)
	s.unsaved.onWait = func(unsavedKey) {
		select {
		case blocked <- struct{}{}:
		default:
		}
	}
	doneB := make(chan bool, 1)
	go func() {
		_, resolved := s.publish(context.Background(), ob2, false, false)
		doneB <- resolved
	}()
	awaitWaiterBlocked(t, blocked)
	if s.unsaved.len() != 1 {
		t.Fatal("B must not overwrite A while blocked")
	}

	// The store recovers: one cycle drains A, admits B, and drains B too.
	ob.setSaveErr(nil)
	n, err := s.RunPendingOnce(context.Background())
	if err != nil {
		t.Fatalf("RunPendingOnce: %v", err)
	}
	select {
	case resolved := <-doneB:
		if resolved {
			t.Fatal("B's call must be unresolved (its intent was held, not published by that call)")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the flush did not admit/release B's blocked caller")
	}
	if s.unsaved.len() != 0 {
		t.Fatalf("held after one cycle = %d, want 0", s.unsaved.len())
	}
	if n < 2 {
		t.Fatalf("handled = %d, want both occurrences", n)
	}
	if got := fp.callsFor(oa.ID()); got != 1 {
		t.Fatalf("A published %d times, want 1", got)
	}
	if got := fp.callsFor(ob2.ID()); got != 1 {
		t.Fatalf("B published %d times, want 1", got)
	}
	if ob.has(oa.ID()) || ob.has(ob2.ID()) {
		t.Fatal("resolved rows left behind")
	}
}

// TestSameScheduleWaiterCancelledWhileBlocked pins the cancellation contract at
// the scheduler level: a later same-schedule occurrence whose persist failed
// blocks on the held occurrence's slot; cancelling its lifecycle releases the
// caller with an unresolved result, without publishing (no Redis) and without
// overwriting the held intent.
func TestSameScheduleWaiterCancelledWhileBlocked(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	fp := newFakePublisher(4)
	ob := newFakeOutbox()
	ob.saveErr = errBoom

	s := testSchedulerWithMetrics(fp, testLogger(), metrics.New())
	s.now = func() time.Time { return frozen }
	s.wait = noWait
	s.SetOutbox(ob)

	oa := pendingOccurrence("a", "jobs.a")
	ob2 := oa
	ob2.ScheduledAt = oa.ScheduledAt.Add(time.Minute)
	if _, resolved := s.publishOccurrence(context.Background(), oa, false); resolved {
		t.Fatal("A unexpectedly resolved")
	}

	blocked := make(chan struct{}, 1)
	s.unsaved.onWait = func(unsavedKey) {
		select {
		case blocked <- struct{}{}:
		default:
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan bool, 1)
	go func() {
		_, resolved := s.publish(ctx, ob2, false, false)
		done <- resolved
	}()
	awaitWaiterBlocked(t, blocked)
	cancel()

	select {
	case resolved := <-done:
		if resolved {
			t.Fatal("a cancelled waiter must return unresolved")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation did not release the blocked waiter")
	}
	held := s.unsaved.snapshot()
	if len(held) != 1 || held[0].ID() != oa.ID() {
		t.Fatalf("held = %v, want the first intent %s only", held, oa.ID())
	}
	if got := fp.callCount(); got != 0 {
		t.Fatalf("publish calls = %d, want 0 (Redis never contacted; A only held)", got)
	}
}

// TestRunPendingOnceCancellationDoesNotDegrade pins that a lifecycle cancellation
// observed by the flush or the claim does NOT degrade the scheduler (no spurious
// gauge/counter), while a genuine store error on a live lifecycle still does.
func TestRunPendingOnceCancellationDoesNotDegrade(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)

	t.Run("claim cancellation", func(t *testing.T) {
		m := metrics.New()
		ob := newFakeOutbox()
		o := pendingOccurrence("a", "jobs.a")
		seedPending(t, ob, o, frozen.Add(-time.Minute), 0)
		s := testSchedulerWithMetrics(newFakePublisher(1), testLogger(), m)
		s.now = func() time.Time { return frozen }
		s.SetOutbox(ob)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := s.RunPendingOnce(ctx); err == nil {
			t.Fatal("a cancelled claim must surface the cancellation error")
		}
		if s.Degraded() {
			t.Fatal("a cancelled claim must not degrade the scheduler")
		}
		if got := m.Counter(metrics.MetricSchedulerDegraded); got != 0 {
			t.Fatalf("degraded counter = %d, want 0 on cancellation", got)
		}
		if got := s.StateLabel(); got != metrics.SchedulerStateRunning {
			t.Fatalf("state = %q, want running", got)
		}
	})

	t.Run("flush failure with cancelled lifecycle", func(t *testing.T) {
		m := metrics.New()
		ob := newFakeOutbox()
		ob.saveErr = errBoom
		s := testSchedulerWithMetrics(newFakePublisher(1), testLogger(), m)
		s.now = func() time.Time { return frozen }
		s.SetOutbox(ob)
		// Hold an occurrence directly, modelling a prior failed persist without a
		// degradation, so the flush-failure branch is exercised in isolation.
		if !s.unsaved.retain(context.Background(), pendingOccurrence("a", "jobs.a")) {
			t.Fatal("retain failed")
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := s.RunPendingOnce(ctx); err == nil {
			t.Fatal("want the flush error")
		}
		if s.Degraded() {
			t.Fatal("a flush failure during cancellation must not degrade")
		}
		if got := m.Counter(metrics.MetricSchedulerDegraded); got != 0 {
			t.Fatalf("degraded counter = %d, want 0 on cancellation", got)
		}
		if got := s.StateLabel(); got != metrics.SchedulerStateRunning {
			t.Fatalf("state = %q, want running", got)
		}
	})

	t.Run("flush failure without cancellation degrades", func(t *testing.T) {
		m := metrics.New()
		ob := newFakeOutbox()
		ob.saveErr = errBoom
		s := testSchedulerWithMetrics(newFakePublisher(1), testLogger(), m)
		s.now = func() time.Time { return frozen }
		s.SetOutbox(ob)
		if !s.unsaved.retain(context.Background(), pendingOccurrence("a", "jobs.a")) {
			t.Fatal("retain failed")
		}
		if _, err := s.RunPendingOnce(context.Background()); err == nil {
			t.Fatal("want the flush error")
		}
		if !s.Degraded() {
			t.Fatal("a genuine flush failure must degrade")
		}
		if got := m.Counter(metrics.MetricSchedulerDegraded); got != 1 {
			t.Fatalf("degraded counter = %d, want 1", got)
		}
	})
}

// TestMarkRunningIfDrainedRefusesWhileHeldOrFaulted pins the recovery transition
// guard: the scheduler is NOT re-enabled while an exact intent is still held or a
// store fault is pending, but is once the set is drained and no fault remains.
func TestMarkRunningIfDrainedRefusesWhileHeldOrFaulted(t *testing.T) {
	frozen := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	s := testScheduler(newFakePublisher(1), testLogger())
	s.now = func() time.Time { return frozen }
	s.SetOutbox(newFakeOutbox())
	s.markDegraded(errBoom)
	// Isolate the held-intent check from the fault the degrade just recorded.
	s.fault.Store(false)

	o := pendingOccurrence("a", "jobs.a")
	if !s.unsaved.retain(context.Background(), o) {
		t.Fatal("retain failed")
	}
	if s.markRunningIfDrained(true) {
		t.Fatal("must not enable live publication while an intent is held")
	}
	if got := s.StateLabel(); got != metrics.SchedulerStateDegraded {
		t.Fatalf("state = %q, want degraded", got)
	}
	s.unsaved.release(o)
	if !s.markRunningIfDrained(true) {
		t.Fatal("should enable live publication once the held set is drained")
	}
	if got := s.StateLabel(); got != metrics.SchedulerStateRunning {
		t.Fatalf("state = %q, want running", got)
	}

	// A pending fault alone (a concurrent persistence failure) must also refuse.
	s.markDegraded(errBoom)
	if s.markRunningIfDrained(true) {
		t.Fatal("must not enable live publication while a store fault is pending")
	}
	if got := s.StateLabel(); got != metrics.SchedulerStateDegraded {
		t.Fatalf("state = %q, want degraded", got)
	}
}

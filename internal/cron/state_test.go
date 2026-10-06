package cron

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"relay/internal/observability/metrics"
	"relay/internal/schedule"
)

// fakeCloser records that a scheduler-owned outbox handle was released. It is
// used to prove the scheduler closes ONLY the handle it opened, and only after
// all scheduler work has stopped.
type fakeCloser struct {
	once   sync.Once
	closed chan struct{}
}

func newFakeCloser() *fakeCloser { return &fakeCloser{closed: make(chan struct{})} }

func (c *fakeCloser) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

// TestStartGocronIsSerializedAgainstStop pins the non-restartable-Start handshake:
// the gocron-start seam must run while the scheduler lock is held, so a
// concurrent Stop cannot slip its g.Shutdown between the scheduler clearing
// gocronStarted and the start taking effect. The seam asserts, with TryLock, that
// s.mu is held during the start for BOTH entry points (Start and markRunning).
func TestStartGocronIsSerializedAgainstStop(t *testing.T) {
	t.Run("Start", func(t *testing.T) {
		s := New(newFakePublisher(1), testLogger())
		s.SetOutbox(newFakeOutbox()) // enable publication
		var held bool
		s.startFn = func() {
			if s.mu.TryLock() {
				// We acquired it, so it was NOT held during the start.
				s.mu.Unlock()
				held = false
				return
			}
			held = true // TryLock failed => s.mu is held by the caller
		}
		s.Start()
		if !held {
			t.Fatal("gocron start ran WITHOUT s.mu held; Stop could Shutdown in the gap")
		}
		if err := s.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}
	})

	t.Run("markRunning (deferred start)", func(t *testing.T) {
		s := New(newFakePublisher(1), testLogger())
		s.MarkStorageUnavailable()
		// The worker requests Start before storage is ready: gocron is deferred.
		s.Start()
		var held bool
		s.startFn = func() {
			if s.mu.TryLock() {
				s.mu.Unlock()
				return
			}
			held = true
		}
		// Recovery installs the outbox and starts the deferred gocron scheduler.
		s.SetOutbox(newFakeOutbox())
		if !held {
			t.Fatal("deferred gocron start ran WITHOUT s.mu held; Stop could Shutdown in the gap")
		}
		if err := s.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}
	})
}

// TestStartGocronNotRunAfterStop pins that a start is never attempted from a
// stopped scheduler (deferred markRunning and Start are both no-ops), so gocron is
// never restarted after its non-restartable Shutdown.
func TestStartGocronNotRunAfterStop(t *testing.T) {
	s := New(newFakePublisher(1), testLogger())
	s.MarkStorageUnavailable()
	s.Start() // deferred: storage not ready
	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	var starts int
	s.startFn = func() { starts++ }
	s.Start()
	s.SetOutbox(newFakeOutbox()) // a late outbox install must not restart gocron
	if starts != 0 {
		t.Fatalf("gocron start attempted %d times after Stop, want 0", starts)
	}
}

// TestSchedulerStateGaugeConsistentUnderConcurrentTransitions pins that the
// one-hot scheduler_state gauge is updated inside the same critical section as the
// internal state, so concurrent MarkStorageUnavailable/markDegraded/markRunning
// cannot leave a stale series. It drives many interleaved transitions under -race
// and asserts the gauge's one-hot set always matches the final internal state.
func TestSchedulerStateGaugeConsistentUnderConcurrentTransitions(t *testing.T) {
	m := metrics.New()
	s := NewWithMetrics(newFakePublisher(1), testLogger(), m)
	s.SetOutbox(newFakeOutbox())

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(3)
		go func() { defer wg.Done(); s.MarkStorageUnavailable() }()
		go func() { defer wg.Done(); s.markDegraded(errBoom) }()
		go func() { defer wg.Done(); s.markRunning(true) }()
	}
	wg.Wait()
	// Whichever transition ran last, the gauge's one-hot set must contain exactly
	// one set series and it must match the scheduler's reported label.
	want := s.StateLabel()
	if got := oneHotSchedulerState(m); got != want {
		t.Fatalf("scheduler_state gauge = %q, internal state = %q (stale/incoherent)", got, want)
	}
	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// oneHotSchedulerState reads the scheduler_state gauge and returns the single
// state whose series is 1. It fails the test if the one-hot invariant is broken.
func oneHotSchedulerState(m *metrics.Registry) string {
	set := ""
	for _, st := range metrics.SchedulerStates {
		v := m.GaugeLabels(metrics.MetricSchedulerState, []metrics.Label{{Name: "state", Value: st}})
		switch {
		case v == 1 && set == "":
			set = st
		case v != 0:
			// More than one series set (or an unexpected value): report the
			// violation distinctly.
			return "INVALID:" + st
		}
	}
	if set == "" {
		return "INVALID:none"
	}
	return set
}

// TestNoOutboxNeverPublishes pins the core storage gate: a scheduler with no
// usable outbox (the startup-unavailable state) never evaluates or publishes an
// occurrence. fire returns before computing a due instant, publishOccurrence
// refuses, and CatchUp neither publishes nor consumes its once-only flag.
func TestNoOutboxNeverPublishes(t *testing.T) {
	fp := newFakePublisher(4)
	m := metrics.New()
	s := NewWithMetrics(fp, testLogger(), m)
	s.now = func() time.Time { return time.Date(2026, 7, 2, 10, 0, 0, 0, time.UTC) }

	if s.StateLabel() != metrics.SchedulerStateUnavailable {
		t.Fatalf("initial state = %q, want unavailable", s.StateLabel())
	}

	s.ReplaceApp("fn", schedTemplate("jobs.a", "0 3 * * *", "", ""))

	// fire is a no-op before it even computes an occurrence.
	parsed, err := parseSchedule("0 3 * * *", time.UTC)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	s.fire(context.Background(), "fn", "jobs.a", "jobs.a", parsed)

	o := schedule.Occurrence{App: "fn", Schedule: "jobs.a", Handler: "jobs.a",
		ScheduledAt: time.Date(2026, 7, 2, 3, 0, 0, 0, time.UTC)}
	if pub, resolved := s.publishOccurrence(context.Background(), o, false); pub || resolved {
		t.Fatalf("unavailable publish = (%v,%v), want (false,false)", pub, resolved)
	}
	if n := s.CatchUp(context.Background()); n != 0 {
		t.Fatalf("unavailable catch-up = %d, want 0", n)
	}
	// The once-only flag must NOT have been consumed by a paused catch-up.
	s.mu.Lock()
	done := s.catchUpDone
	s.mu.Unlock()
	if done {
		t.Fatal("paused catch-up consumed catchUpDone; recovery could not re-run it")
	}
	if got := fp.callCount(); got != 0 {
		t.Fatalf("publish attempts = %d, want 0 (no outbox -> no publication)", got)
	}
	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestRuntimeSaveFailureDegradesAndStopsTicks pins the runtime-degraded
// contract: when a Redis publish fails AND the durable persist fails, the
// scheduler transitions to degraded immediately, and the occurrence is treated as
// unresolved. No in-memory retry is attempted as a substitute for persistence,
// and subsequent ticks are refused.
func TestRuntimeSaveFailureDegradesAndStopsTicks(t *testing.T) {
	m := metrics.New()
	fp := newFakePublisher(16)
	fp.err = errBoom
	ob := newFakeOutbox()
	ob.saveErr = errBoom

	s := NewWithMetrics(fp, testLogger(), m)
	s.wait = noWait // would otherwise drive the bounded in-memory retries
	s.SetOutbox(ob) // enables publication
	s.now = func() time.Time { return time.Date(2026, 7, 2, 10, 0, 0, 0, time.UTC) }

	o := schedule.Occurrence{App: "fn", Schedule: "jobs.a", Handler: "jobs.a",
		ScheduledAt: time.Date(2026, 7, 2, 3, 0, 0, 0, time.UTC)}
	if pub, resolved := s.publishOccurrence(context.Background(), o, false); pub || resolved {
		t.Fatalf("persist-failed publish = (%v,%v), want (false,false)", pub, resolved)
	}
	if got := fp.callCount(); got != 1 {
		t.Fatalf("publish attempts = %d, want exactly 1 (no in-memory retry after persist failure)", got)
	}
	if !s.Degraded() || s.StateLabel() != metrics.SchedulerStateDegraded {
		t.Fatalf("state = %q, degraded=%v; want degraded", s.StateLabel(), s.Degraded())
	}
	if got := m.Counter(metrics.MetricSchedulerDegraded); got != 1 {
		t.Fatalf("degraded counter = %d, want 1", got)
	}

	// A subsequent tick is refused before computing an occurrence.
	parsed, err := parseSchedule("0 3 * * *", time.UTC)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	s.fire(context.Background(), "fn", "jobs.a", "jobs.a", parsed)
	if got := fp.callCount(); got != 1 {
		t.Fatalf("publish attempts after degradation = %d, want still 1", got)
	}
}

// TestRecoveryIncompleteWhenStoreStillFails pins that a failure during the
// recovery drain/catch-up (a store that answers a scan but not a later write)
// leaves the scheduler degraded rather than re-enabling live publication.
func TestRecoveryIncompleteWhenStoreStillFails(t *testing.T) {
	frozen := time.Date(2026, 7, 2, 10, 2, 0, 0, time.UTC)
	fp := newFakePublisher(8)
	fp.err = errBoom
	ob := newFakeOutbox()
	ob.saveErr = errBoom // a persist during the recovery catch-up fails
	s := NewWithMetrics(fp, testLogger(), metrics.New())
	s.now = func() time.Time { return frozen }
	s.wait = noWait
	s.SetOutbox(ob)
	s.ReplaceApp("fn", schedTemplate("jobs.a", "0 3 * * *", "", ""))

	// Force a degraded state, then run recovery while saves still fail.
	s.markDegraded(errBoom)
	if s.attemptRecovery(context.Background(), testLogger()) {
		t.Fatal("attemptRecovery reported recovery while a store write still fails")
	}
	if !s.Degraded() {
		t.Fatalf("state = %q, want degraded after an incomplete recovery", s.StateLabel())
	}
}

// TestRecoveryReenablesAndDrainsPending pins the recovery contract: once outbox
// operations work again, the durable retry worker drains existing rows, runs the
// bounded latest-only catch-up, and re-enables live publication. A persisted row
// is republished after recovery.
func TestRecoveryReenablesAndDrainsPending(t *testing.T) {
	frozen := time.Date(2026, 7, 2, 10, 2, 0, 0, time.UTC)
	m := metrics.New()
	fp := newFakePublisher(8)
	ob := newFakeOutbox()
	o := pendingOccurrence("a", "jobs.a")
	seedPending(t, ob, o, frozen.Add(-time.Minute), 0)

	s := NewWithMetrics(fp, testLogger(), m)
	s.now = func() time.Time { return frozen }
	s.wait = noWait
	s.SetOutbox(ob)
	s.ReplaceApp("fn", schedTemplate("jobs.a", "0 3 * * *", "", ""))

	// Degrade the scheduler, then observe the outbox is healthy again.
	ob.claimErr = errBoom
	if _, err := s.RunPendingOnce(context.Background()); err == nil {
		t.Fatal("expected a claim error")
	}
	if !s.Degraded() {
		t.Fatal("a claim error did not degrade the scheduler")
	}
	ob.claimErr = nil

	if !s.attemptRecovery(context.Background(), testLogger()) {
		t.Fatal("attemptRecovery reported still degraded")
	}
	if s.Degraded() || s.StateLabel() != metrics.SchedulerStateRunning {
		t.Fatalf("state = %q, want running after recovery", s.StateLabel())
	}
	if got := m.Counter(metrics.MetricSchedulerRecoveries); got != 1 {
		t.Fatalf("recoveries counter = %d, want 1", got)
	}
	if ob.has(o.ID()) {
		t.Fatal("persisted pending row was not drained by recovery")
	}
	if n := fp.callsFor(o.ID()); n != 1 {
		t.Fatalf("pending row republished %d times, want 1", n)
	}
	// The recovery catch-up also republished the latest missed occurrence.
	if fp.callCount() < 2 {
		t.Fatalf("recovery catch-up did not publish a missed occurrence (calls=%d)", fp.callCount())
	}
}

// TestOutboxFailuresDegrade pins that each outbox error path drives degraded:
// claim, reschedule, and delete.
func TestOutboxFailuresDegrade(t *testing.T) {
	frozen := time.Date(2026, 7, 2, 10, 2, 0, 0, time.UTC)
	o := pendingOccurrence("a", "jobs.a")

	t.Run("claim", func(t *testing.T) {
		ob := newFakeOutbox()
		ob.claimErr = errBoom
		s := NewWithMetrics(newFakePublisher(1), testLogger(), metrics.New())
		s.now = func() time.Time { return frozen }
		s.SetOutbox(ob)
		if _, err := s.RunPendingOnce(context.Background()); err == nil {
			t.Fatal("expected claim error")
		}
		if !s.Degraded() {
			t.Fatal("claim error did not degrade")
		}
	})

	t.Run("reschedule", func(t *testing.T) {
		ob := newFakeOutbox()
		ob.reschedErr = errBoom
		seedPending(t, ob, o, frozen.Add(-time.Minute), 0)
		fp := newFakePublisher(4)
		fp.err = errBoom
		s := NewWithMetrics(fp, testLogger(), metrics.New())
		s.now = func() time.Time { return frozen }
		s.SetOutbox(ob)
		if _, err := s.RunPendingOnce(context.Background()); err != nil {
			t.Fatalf("RunPendingOnce: %v", err)
		}
		if !s.Degraded() {
			t.Fatal("reschedule error did not degrade")
		}
	})

	t.Run("delete", func(t *testing.T) {
		ob := newFakeOutbox()
		ob.deleteErr = errBoom
		seedPending(t, ob, o, frozen.Add(-time.Minute), 0)
		fp := newFakePublisher(4)
		fp.published = false // clean duplicate resolves
		s := NewWithMetrics(fp, testLogger(), metrics.New())
		s.now = func() time.Time { return frozen }
		s.SetOutbox(ob)
		if _, err := s.RunPendingOnce(context.Background()); err != nil {
			t.Fatalf("RunPendingOnce: %v", err)
		}
		if !s.Degraded() {
			t.Fatal("delete error did not degrade")
		}
	})
}

// TestRecoveryCatchUpUsesBoundedLatestOnlyDedup pins that the recovery catch-up
// reuses the existing latest-only 24h semantics and atomic dedup: a repeated
// recovery scan publishes nothing new and never replays older occurrences.
func TestRecoveryCatchUpUsesBoundedLatestOnlyDedup(t *testing.T) {
	now := time.Date(2026, 7, 2, 10, 2, 0, 0, time.UTC)
	pub := newDedupPublisher()
	s := NewWithMetrics(pub, testLogger(), metrics.New())
	s.now = func() time.Time { return now }
	s.wait = noWait
	s.SetOutbox(newFakeOutbox())
	s.ReplaceApp("fn", schedTemplate("jobs.a", "0 3 * * *", "", ""))

	// Two recovery scans: the first publishes the latest missed occurrence
	// (03:00), the second is an atomic-dedup duplicate.
	s.recoveryCatchUp(context.Background())
	s.recoveryCatchUp(context.Background())

	pub.mu.Lock()
	defer pub.mu.Unlock()
	if pub.wins != 1 || pub.dups != 1 {
		t.Fatalf("recovery catch-up outcomes = wins %d dups %d, want exactly 1 win and 1 duplicate", pub.wins, pub.dups)
	}
}

// TestStorageBootstrapRecoversAfterFailure pins the startup-unavailable recovery:
// with no shared outbox, a scheduler-owned bootstrap retries opening the store,
// installs the first successful one, recovers (catch-up + running), and closes
// that owned handle only on Stop.
func TestStorageBootstrapRecoversAfterFailure(t *testing.T) {
	now := time.Date(2026, 7, 2, 10, 2, 0, 0, time.UTC)
	fp := newFakePublisher(4)
	m := metrics.New()
	s := NewWithMetrics(fp, testLogger(), m)
	s.now = func() time.Time { return now }
	s.wait = noWait
	s.MarkStorageUnavailable()
	s.ReplaceApp("fn", schedTemplate("jobs.a", "0 3 * * *", "", ""))

	ob := newFakeOutbox()
	closer := newFakeCloser()
	var calls int
	var mu sync.Mutex
	// The first open fails; the second succeeds. The bootstrap's failure pause is
	// driven by the injected pendingWait seam (no real 30s sleep).
	s.pendingWait = func(context.Context, time.Duration, <-chan struct{}) bool { return true }
	open := func() (Outbox, io.Closer, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			return nil, nil, errors.New("open failed")
		}
		return ob, closer, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Install the deterministic gocron-start seam BEFORE spawning the bootstrap:
	// Start defers gocron (storage is not yet ready), and the bootstrap's
	// markRunning invokes this seam only once recovery has completed. Setting it
	// before the goroutine starts also establishes happens-before, so there is no
	// data race on s.startFn. Waiting on the channel is the recovery signal; the
	// watchdog is a deadlock guard, not a sequencing sleep.
	started := make(chan struct{})
	s.startFn = func() { close(started) }

	s.StartStorageBootstrap(ctx, open)

	// Ask the scheduler to fire (as the worker does) BEFORE recovery: gocron must
	// not start until storage is ready.
	s.Start()

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatalf("bootstrap recovery did not start gocron (state = %q)", s.StateLabel())
	}
	if got := s.StateLabel(); got != metrics.SchedulerStateRunning {
		t.Fatalf("state = %q, want running after bootstrap recovery", got)
	}
	if got := m.Counter(metrics.MetricSchedulerRecoveries); got != 1 {
		t.Fatalf("recoveries counter = %d, want 1", got)
	}
	// gocron is now started and the recovery catch-up published the missed 03:00.
	if fp.callCount() == 0 {
		t.Fatal("bootstrap recovery did not run the catch-up")
	}

	// Stop joins the bootstrap and closes the scheduler-owned handle.
	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	select {
	case <-closer.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not close the scheduler-owned outbox handle")
	}
}

// TestStopJoinsBootstrapAndLeavesSharedHandleOpen pins the isolation rule: when
// the scheduler opened NO handle of its own (the shared global handle was wired),
// Stop never closes it; and a bootstrap in flight is joined before Stop returns.
func TestStopJoinsBootstrapAndLeavesSharedHandleOpen(t *testing.T) {
	s := New(newFakePublisher(1), testLogger())
	shared := newFakeCloser() // stands in for the worker's shared handle
	s.SetOutbox(newFakeOutbox())

	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	select {
	case <-shared.closed:
		t.Fatal("Stop closed a handle it did not own")
	case <-time.After(50 * time.Millisecond):
	}

	// A later bootstrap that opens a handle after Stop releases it itself.
	s2 := New(newFakePublisher(1), testLogger())
	s2.MarkStorageUnavailable()
	own := newFakeCloser()
	entered := make(chan struct{})
	release := make(chan struct{})
	open := func() (Outbox, io.Closer, error) {
		close(entered)
		<-release
		return newFakeOutbox(), own, nil
	}
	s2.StartStorageBootstrap(context.Background(), open)
	<-entered
	joined := make(chan error, 1)
	go func() { joined <- s2.Stop(context.Background()) }()
	// Stop must not return while the bootstrap is opening.
	select {
	case <-joined:
		t.Fatal("Stop returned before the bootstrap finished")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-joined:
		if err != nil {
			t.Fatalf("Stop: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not join the bootstrap")
	}
	select {
	case <-own.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("a handle opened after Stop was not released")
	}
}

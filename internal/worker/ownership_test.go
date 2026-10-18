package worker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestOwnershipLossLatchIsOnceAndNilSafe pins the once-only semantics of the
// ownership latch: the first lose() wins (so the loss side effects run once),
// later calls are no-ops, isLost reflects the state, and a nil latch is safe.
func TestOwnershipLossLatchIsOnceAndNilSafe(t *testing.T) {
	var l ownershipLoss
	if l.isLost() {
		t.Fatal("a fresh latch must not report lost")
	}
	if !l.lose() {
		t.Fatal("the first lose() must win")
	}
	if !l.isLost() {
		t.Fatal("isLost must be true after lose()")
	}
	if l.lose() {
		t.Fatal("a second lose() must be a no-op")
	}

	var nilLatch *ownershipLoss
	if nilLatch.isLost() {
		t.Fatal("a nil latch must report not lost")
	}
	if nilLatch.lose() {
		t.Fatal("a nil latch must not claim the first loss")
	}
}

// TestMonitorOwnershipDetectsLossOnceAndCancels pins the monitor's loss path
// through the verify seam: a check failure on a LIVE lifecycle is a detected
// loss — it runs the loss action exactly once (readiness cleared, one warning,
// lifecycle cancelled) and the monitor exits. No PostgreSQL is needed; the
// fake's closed channel makes the detection deterministic without sleeps.
func TestMonitorOwnershipDetectsLossOnceAndCancels(t *testing.T) {
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		verifyCalls atomic.Int32
		loss        ownershipLoss
		ready       = newReadiness(lifecycle)
	)
	ready.setReady()

	var logs bytes.Buffer
	cfg := ownershipMonitorConfig{
		verify: func(context.Context) error {
			verifyCalls.Add(1)
			return errors.New("simulated lock loss")
		},
		// Long interval: the immediate first check must catch the loss without
		// waiting a tick.
		interval: time.Hour,
		timeout:  time.Second,
	}
	onLost := ownershipLossAction(
		&loss, ready,
		slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		"worker-x", "inst-1", cancel,
	)

	done := startOwnershipMonitor(lifecycle, onLost, cfg)
	waitClosed(t, done, 2*time.Second)
	if !loss.isLost() {
		t.Fatal("monitor must latch the loss")
	}
	if verifyCalls.Load() == 0 {
		t.Fatal("monitor never ran a verification")
	}
	if lifecycle.Err() == nil {
		t.Fatal("monitor must cancel the lifecycle on loss")
	}
	if ready, reason := ready.Ready(context.Background()); ready || reason != "worker ownership lost" {
		t.Fatalf("readiness after loss = (%v, %q), want not-ready with the loss reason", ready, reason)
	}
	if !strings.Contains(logs.String(), "Worker ownership lost") {
		t.Fatalf("monitor must log the loss warning; logs:\n%s", logs.String())
	}
}

// TestMonitorOwnershipReportsLossOnlyOnce pins that a detected loss runs the
// loss action exactly once and the loop then returns: the monitor does not keep
// polling (and therefore cannot repeat side effects).
func TestMonitorOwnershipReportsLossOnlyOnce(t *testing.T) {
	var onLostCalls atomic.Int32
	verify := func(context.Context) error { return errors.New("lost") }

	monitorOwnership(context.Background(), verify, func() { onLostCalls.Add(1) }, time.Millisecond, time.Millisecond)

	if got := onLostCalls.Load(); got != 1 {
		t.Fatalf("loss action ran %d times, want exactly 1", got)
	}
}

// TestMonitorOwnershipCleanShutdownIsNotALoss pins the ordinary-shutdown path:
// when the lifecycle is cancelled, a failing verification (the pinned connection
// closing) is shutdown, NOT a loss, so the loss action must not run and the
// monitor must exit cleanly. There are no sleeps: the lifecycle is cancelled
// before the monitor starts.
func TestMonitorOwnershipCleanShutdownIsNotALoss(t *testing.T) {
	lifecycle, cancel := context.WithCancel(context.Background())
	cancel() // shutdown already underway

	var onLostCalls atomic.Int32
	// A verification that fails because its context is cancelled — exactly what
	// VerifyOwnerLock reports once the pinned connection is closing.
	verify := func(ctx context.Context) error { return ctx.Err() }

	done := make(chan struct{})
	go func() {
		defer close(done)
		monitorOwnership(lifecycle, verify, func() { onLostCalls.Add(1) }, time.Millisecond, time.Second)
	}()
	waitClosed(t, done, 2*time.Second)

	if onLostCalls.Load() != 0 {
		t.Fatal("a cancelled lifecycle must not be reported as an ownership loss")
	}
}

// TestMonitorOwnershipStopsOnLifecycleCancelWithHealthyLock pins the steady
// state: while the lock verifies, the monitor polls and then exits promptly when
// the lifecycle is cancelled, without reporting a loss.
func TestMonitorOwnershipStopsOnLifecycleCancelWithHealthyLock(t *testing.T) {
	lifecycle, cancel := context.WithCancel(context.Background())
	var (
		checks  atomic.Int32
		onLost  atomic.Int32
		entered = make(chan struct{})
	)
	var once sync.Once
	verify := func(context.Context) error {
		checks.Add(1)
		once.Do(func() { close(entered) })
		return nil
	}

	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		monitorOwnership(lifecycle, verify, func() { onLost.Add(1) }, time.Millisecond, time.Second)
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("monitor never ran its first check")
	}
	cancel()
	waitClosed(t, monitorDone, 2*time.Second)
	if onLost.Load() != 0 {
		t.Fatal("a healthy-lock monitor must not report a loss on cancellation")
	}
	if checks.Load() == 0 {
		t.Fatal("monitor must have run at least one check")
	}
}

// TestGuardIdentityCleanupSkipsOnlyAfterLoss pins the identity-scoped cleanup
// guard's latch path: with a healthy lock (and a passing fresh proof) the
// wrapped cleanup runs normally, and once the loss is latched it is skipped
// (returns nil without invoking the cleanup, and without spending another proof)
// so a replacement owner's services/stats are never disturbed.
func TestGuardIdentityCleanupSkipsOnlyAfterLoss(t *testing.T) {
	var (
		ran         atomic.Int32
		verifyCalls atomic.Int32
	)
	run := func(context.Context) error { ran.Add(1); return nil }

	loss := &ownershipLoss{}
	proof := ownershipProof{
		loss:   loss,
		verify: func(context.Context) error { verifyCalls.Add(1); return nil },
	}
	guarded := guardIdentityCleanup(proof, nil, shutdownStepServiceCleanup, run)
	if err := guarded(context.Background()); err != nil {
		t.Fatalf("guarded cleanup with a healthy lock = %v, want nil", err)
	}
	if ran.Load() != 1 {
		t.Fatalf("cleanup ran %d times with a healthy lock, want 1", ran.Load())
	}
	if verifyCalls.Load() != 1 {
		t.Fatalf("fresh proof ran %d times with a healthy lock, want 1", verifyCalls.Load())
	}

	loss.lose()
	if err := guarded(context.Background()); err != nil {
		t.Fatalf("guarded cleanup after loss = %v, want nil", err)
	}
	if ran.Load() != 1 {
		t.Fatalf("cleanup ran %d times after loss, want still 1 (skipped)", ran.Load())
	}
	// The latch short-circuits BEFORE the proof, so no extra round trip is spent.
	if verifyCalls.Load() != 1 {
		t.Fatalf("proof ran %d times after a latched loss, want still 1 (short-circuited)", verifyCalls.Load())
	}
}

// TestGuardIdentityCleanupFreshProofFailureLatchesAndSkips pins the fail-closed
// path the review calls out: the latch is UNSET (the monitor never observed the
// loss) but the fresh bounded proof fails because the lock was lost since the
// monitor's last tick. The guard must run the shared loss action exactly once
// (latch set, readiness cleared, lifecycle cancelled) and SKIP the
// identity-scoped cleanup rather than delete a replacement owner's containers.
func TestGuardIdentityCleanupFreshProofFailureLatchesAndSkips(t *testing.T) {
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := newReadiness(lifecycle)

	var (
		ran     atomic.Int32
		cancels atomic.Int32
	)
	run := func(context.Context) error { ran.Add(1); return nil }

	loss := &ownershipLoss{}
	proof := ownershipProof{
		loss:   loss,
		verify: func(context.Context) error { return errors.New("lock lost since the last poll") },
		onLost: ownershipLossAction(loss, ready, nil, "worker-x", "inst-1", func() {
			cancels.Add(1)
			cancel()
		}),
	}

	guarded := guardIdentityCleanup(proof, nil, shutdownStepServiceCleanup, run)
	if err := guarded(context.Background()); err != nil {
		t.Fatalf("guarded cleanup after a failed proof = %v, want nil (skipped)", err)
	}
	if ran.Load() != 0 {
		t.Fatalf("cleanup ran %d times after a failed proof, want 0 (skipped)", ran.Load())
	}
	if !loss.isLost() {
		t.Fatal("a failed fresh proof must latch the ownership loss")
	}
	if cancels.Load() != 1 {
		t.Fatalf("lifecycle cancelled %d times, want exactly 1", cancels.Load())
	}
	if lifecycle.Err() == nil {
		t.Fatal("a failed fresh proof must cancel the lifecycle")
	}
	if r, reason := ready.Ready(context.Background()); r || reason != "worker ownership lost" {
		t.Fatalf("readiness after a failed proof = (%v, %q), want not-ready with the loss reason", r, reason)
	}
}

// TestGuardIdentityCleanupAmbiguousProofFailsClosed pins that an AMBIGUOUS proof
// (here the verification reports its own bounded-context deadline) is treated
// exactly like a detected loss: the guard latches and skips rather than guessing
// the lock is still held.
func TestGuardIdentityCleanupAmbiguousProofFailsClosed(t *testing.T) {
	var ran atomic.Int32
	run := func(context.Context) error { ran.Add(1); return nil }

	loss := &ownershipLoss{}
	proof := ownershipProof{
		loss:   loss,
		verify: func(context.Context) error { return context.DeadlineExceeded },
		onLost: ownershipLossAction(loss, nil, nil, "worker-x", "inst-1", nil),
	}

	guarded := guardIdentityCleanup(proof, nil, shutdownStepStatsFlush, run)
	if err := guarded(context.Background()); err != nil {
		t.Fatalf("guarded cleanup on an ambiguous proof = %v, want nil (skipped)", err)
	}
	if ran.Load() != 0 {
		t.Fatalf("cleanup ran %d times on an ambiguous proof, want 0 (fail closed)", ran.Load())
	}
	if !loss.isLost() {
		t.Fatal("an ambiguous proof must fail closed and latch the loss")
	}
}

// TestOwnershipProofUsesFreshBoundedContextAndFailsClosed pins prove's contract:
// it runs the seam with a context that is rooted in context.Background (never a
// cancelled lifecycle), it bounds the seam so a stalled verification cannot hang
// shutdown, and the stall FAILS CLOSED by latching the loss.
func TestOwnershipProofUsesFreshBoundedContextAndFailsClosed(t *testing.T) {
	var (
		observedErr error
		mu          sync.Mutex
		lost        ownershipLoss
	)
	proof := ownershipProof{
		loss: &lost,
		verify: func(ctx context.Context) error {
			mu.Lock()
			observedErr = ctx.Err()
			mu.Unlock()
			<-ctx.Done() // a stalled verification: only the proof's bound ends it
			return ctx.Err()
		},
		onLost: ownershipLossAction(&lost, nil, nil, "worker-x", "inst-1", nil),
	}

	if err := proof.prove(50 * time.Millisecond); err == nil {
		t.Fatal("a stalled proof must return an error, not nil")
	}
	mu.Lock()
	got := observedErr
	mu.Unlock()
	if got != nil {
		t.Fatalf("proof seam observed ctx.Err() = %v, want nil (fresh background context)", got)
	}
	if !lost.isLost() {
		t.Fatal("a stalled/ambiguous proof must fail closed and latch the loss")
	}
}

// TestOwnershipLossActionRunsOnce pins that the shared loss action performs its
// side effects exactly once through the latch, no matter how many detectors
// observe the same loss (the monitor, the final barrier verification, or an
// identity-cleanup guard).
func TestOwnershipLossActionRunsOnce(t *testing.T) {
	var (
		loss    ownershipLoss
		cancels atomic.Int32
	)
	act := ownershipLossAction(&loss, nil, nil, "worker-x", "inst-1", func() { cancels.Add(1) })
	act()
	act()
	act()
	if cancels.Load() != 1 {
		t.Fatalf("loss action cancelled %d times, want exactly 1", cancels.Load())
	}
	if !loss.isLost() {
		t.Fatal("loss action must latch the loss")
	}
}

// TestOwnershipBarrierFinalVerificationRunsAfterMonitorJoinDespiteCancelledLifecycle
// pins the fix for the residual ownership-loss race: Run's shutdown defer
// cancels the lifecycle before teardown, the monitor then classifies a failing
// check as ordinary shutdown and exits, and the ownership barrier — after
// strictly joining the monitor — still runs ONE final verification on a fresh
// background context. A loss detected only here is latched and skips
// identity-scoped cleanup.
func TestOwnershipBarrierFinalVerificationRunsAfterMonitorJoinDespiteCancelledLifecycle(t *testing.T) {
	// The monitor already exited (done is closed to model the completed join)
	// without reporting a loss, exactly as it does after the lifecycle is
	// cancelled by Run's shutdown defer.
	monitorDone := make(chan struct{})
	close(monitorDone)

	var (
		lost        ownershipLoss
		verified    atomic.Int32
		observedErr error
		mu          sync.Mutex
	)
	proof := ownershipProof{
		loss: &lost,
		verify: func(ctx context.Context) error {
			verified.Add(1)
			mu.Lock()
			observedErr = ctx.Err()
			mu.Unlock()
			return errors.New("lock lost just before shutdown")
		},
		onLost: ownershipLossAction(&lost, nil, nil, "worker-x", "inst-1", nil),
	}

	// The step's own context is cancelled too: the final verification must
	// ignore it and use a fresh background context.
	stepCtx, stepCancel := context.WithCancel(context.Background())
	stepCancel()

	step := ownershipBarrierStep(monitorDone, proof, nil)
	// The barrier returns its own expired bound (context.Canceled) when the step
	// context is cancelled — the same documented timeout contract every barrier
	// step has; the join itself completed. The point under test is that the final
	// verification still ran regardless.
	if err := step.run(stepCtx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("ownership barrier run = %v, want nil or its expired bound error", err)
	}
	if verified.Load() != 1 {
		t.Fatalf("final verification ran %d times, want exactly 1", verified.Load())
	}
	mu.Lock()
	got := observedErr
	mu.Unlock()
	if got != nil {
		t.Fatalf("final verification observed ctx.Err() = %v, want nil (fresh background, not the cancelled lifecycle)", got)
	}
	if !lost.isLost() {
		t.Fatal("the final shutdown verification must latch a loss the monitor missed")
	}
}

// TestOwnershipBarrierValidFinalVerificationPreservesCleanup pins the healthy
// path: a passing final verification does not latch a loss, so the later
// identity-scoped cleanup guards keep running normally.
func TestOwnershipBarrierValidFinalVerificationPreservesCleanup(t *testing.T) {
	monitorDone := make(chan struct{})
	close(monitorDone)

	var lost ownershipLoss
	proof := ownershipProof{
		loss:   &lost,
		verify: func(context.Context) error { return nil },
		onLost: ownershipLossAction(&lost, nil, nil, "worker-x", "inst-1", nil),
	}

	step := ownershipBarrierStep(monitorDone, proof, nil)
	if err := step.run(context.Background()); err != nil {
		t.Fatalf("ownership barrier run = %v, want nil", err)
	}
	if lost.isLost() {
		t.Fatal("a passing final verification must not latch an ownership loss")
	}
}

// TestReadinessReportsOwnershipLostDistinctly pins that an ownership loss clears
// readiness and is reported as such — both before the ready boundary and after,
// where it also dominates a live dependency probe (so a healthy-looking probe
// cannot mask a lost identity).
func TestReadinessReportsOwnershipLostDistinctly(t *testing.T) {
	starting := newReadiness(context.Background())
	starting.setOwnershipLost()
	if ready, reason := starting.Ready(context.Background()); ready || reason != "worker ownership lost" {
		t.Fatalf("ownership lost before ready = (%v, %q), want not-ready with the loss reason", ready, reason)
	}

	ready := newReadiness(context.Background())
	ready.startReady(&fakeConsumerHealth{healthy: true}, &fakeDockerReadiness{netOK: true}, nil)
	if r, _ := ready.Ready(context.Background()); !r {
		t.Fatal("worker must be ready before the loss")
	}
	ready.setOwnershipLost()
	if r, reason := ready.Ready(context.Background()); r || reason != "worker ownership lost" {
		t.Fatalf("ownership lost after ready = (%v, %q), want not-ready with the loss reason", r, reason)
	}
}

// TestOwnershipRunError pins the Run error override: a lost lock turns any
// result into a distinctive abnormal-termination error (joined with any existing
// error), while a healthy worker returns its result unchanged.
func TestOwnershipRunError(t *testing.T) {
	healthy := &ownershipLoss{}
	if err := ownershipRunError(healthy, nil); err != nil {
		t.Fatalf("healthy nil result = %v, want nil", err)
	}
	if err := ownershipRunError(healthy, errStartupInterrupted); !errors.Is(err, errStartupInterrupted) {
		t.Fatalf("healthy error = %v, want the original", err)
	}

	lost := &ownershipLoss{}
	lost.lose()
	if err := ownershipRunError(lost, nil); !errors.Is(err, errOwnershipLost) {
		t.Fatalf("lost nil result = %v, want errOwnershipLost", err)
	}
	consumeErr := errors.New("consume failed")
	err := ownershipRunError(lost, consumeErr)
	if !errors.Is(err, errOwnershipLost) || !errors.Is(err, consumeErr) {
		t.Fatalf("lost result = %v, want both errOwnershipLost and the consume error", err)
	}
}

// TestGuardIdentitySnapshotCleanupSnapshotsProofsThenRemoves pins the security
// ordering the guard exists for: discovery runs FIRST, the fresh bounded proof
// SECOND, and removal LAST with exactly the captured candidates — never a
// discovery after the proof.
func TestGuardIdentitySnapshotCleanupSnapshotsProofsThenRemoves(t *testing.T) {
	var (
		mu     sync.Mutex
		events []string
	)
	record := func(s string) {
		mu.Lock()
		events = append(events, s)
		mu.Unlock()
	}

	loss := &ownershipLoss{}
	proof := ownershipProof{
		loss: loss,
		verify: func(context.Context) error {
			record("proof")
			return nil
		},
	}
	candidates := []string{"a", "b"}
	guarded := guardIdentitySnapshotCleanup(proof, nil, shutdownStepServiceCleanup,
		func(context.Context) ([]string, error) {
			record("discover")
			return candidates, nil
		},
		func(_ context.Context, got []string) error {
			record("remove")
			if len(got) != 2 || got[0] != "a" || got[1] != "b" {
				t.Errorf("remove got %v, want exactly the captured candidates", got)
			}
			return nil
		},
	)
	if err := guarded(context.Background()); err != nil {
		t.Fatalf("guarded = %v, want nil", err)
	}

	mu.Lock()
	got := append([]string(nil), events...)
	mu.Unlock()
	want := []string{"discover", "proof", "remove"}
	if len(got) != len(want) {
		t.Fatalf("event order = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("event order = %v, want %v", got, want)
		}
	}
}

// TestGuardIdentitySnapshotCleanupLatchedLossSkipsEverything pins the
// short-circuit: a latched loss runs neither discovery, the proof, nor removal
// (no Docker round trip, no removal of a replacement owner's containers).
func TestGuardIdentitySnapshotCleanupLatchedLossSkipsEverything(t *testing.T) {
	var (
		discoverCalls atomic.Int32
		proofCalls    atomic.Int32
		removeCalls   atomic.Int32
	)
	loss := &ownershipLoss{}
	loss.lose()
	proof := ownershipProof{
		loss:   loss,
		verify: func(context.Context) error { proofCalls.Add(1); return nil },
	}
	guarded := guardIdentitySnapshotCleanup(proof, nil, shutdownStepServiceCleanup,
		func(context.Context) ([]string, error) { discoverCalls.Add(1); return []string{"a"}, nil },
		func(context.Context, []string) error { removeCalls.Add(1); return nil },
	)
	if err := guarded(context.Background()); err != nil {
		t.Fatalf("guarded = %v, want nil", err)
	}
	if discoverCalls.Load() != 0 || proofCalls.Load() != 0 || removeCalls.Load() != 0 {
		t.Fatalf("calls after latched loss = discover:%d proof:%d remove:%d, want all 0",
			discoverCalls.Load(), proofCalls.Load(), removeCalls.Load())
	}
}

// TestGuardIdentitySnapshotCleanupFailedProofSkipsRemovalAndLatches pins the
// fail-closed path: the snapshot happens, the fresh proof fails (the lock was
// lost since the last poll), the shared loss action runs once, and the captured
// containers are NOT removed.
func TestGuardIdentitySnapshotCleanupFailedProofSkipsRemovalAndLatches(t *testing.T) {
	var removeCalls atomic.Int32
	loss := &ownershipLoss{}
	var cancels atomic.Int32
	proof := ownershipProof{
		loss:   loss,
		verify: func(context.Context) error { return errors.New("lock lost during cleanup") },
		onLost: ownershipLossAction(loss, nil, nil, "worker-x", "inst-1", func() { cancels.Add(1) }),
	}
	guarded := guardIdentitySnapshotCleanup(proof, nil, shutdownStepServiceCleanup,
		func(context.Context) ([]string, error) { return []string{"a"}, nil },
		func(context.Context, []string) error { removeCalls.Add(1); return nil },
	)
	if err := guarded(context.Background()); err != nil {
		t.Fatalf("guarded = %v, want nil (skipped)", err)
	}
	if removeCalls.Load() != 0 {
		t.Fatalf("removal ran %d times after a failed proof, want 0", removeCalls.Load())
	}
	if !loss.isLost() {
		t.Fatal("a failed fresh proof must latch the ownership loss")
	}
	if cancels.Load() != 1 {
		t.Fatalf("loss action ran %d times, want exactly 1", cancels.Load())
	}
}

// TestGuardIdentitySnapshotCleanupNoCandidatesSkipsProofAndRemoval pins that an
// empty snapshot skips both the proof and the removal.
func TestGuardIdentitySnapshotCleanupNoCandidatesSkipsProofAndRemoval(t *testing.T) {
	var (
		proofCalls  atomic.Int32
		removeCalls atomic.Int32
	)
	proof := ownershipProof{
		loss:   &ownershipLoss{},
		verify: func(context.Context) error { proofCalls.Add(1); return nil },
	}
	guarded := guardIdentitySnapshotCleanup(proof, nil, shutdownStepServiceCleanup,
		func(context.Context) ([]string, error) { return nil, nil },
		func(context.Context, []string) error { removeCalls.Add(1); return nil },
	)
	if err := guarded(context.Background()); err != nil {
		t.Fatalf("guarded = %v, want nil", err)
	}
	if proofCalls.Load() != 0 {
		t.Fatalf("proof ran %d times with no candidates, want 0", proofCalls.Load())
	}
	if removeCalls.Load() != 0 {
		t.Fatalf("removal ran %d times with no candidates, want 0", removeCalls.Load())
	}
}

// TestGuardIdentitySnapshotCleanupPropagatesDiscoveryError pins that a discovery
// failure is returned (so the shutdown registry surfaces it with its step name)
// and removal never runs.
func TestGuardIdentitySnapshotCleanupPropagatesDiscoveryError(t *testing.T) {
	sentinel := errors.New("list containers: daemon down")
	guarded := guardIdentitySnapshotCleanup(ownershipProof{loss: &ownershipLoss{}}, nil, shutdownStepServiceCleanup,
		func(context.Context) ([]string, error) { return nil, sentinel },
		func(context.Context, []string) error {
			t.Fatal("removal must not run after a discovery error")
			return nil
		},
	)
	if err := guarded(context.Background()); !errors.Is(err, sentinel) {
		t.Fatalf("guarded = %v, want the discovery error", err)
	}
}

// waitClosed fails the test unless done closes within d (a deadlock guard, never
// a sequencing wait).
func waitClosed(t *testing.T, done <-chan struct{}, d time.Duration) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatal("channel did not close in time")
	}
}

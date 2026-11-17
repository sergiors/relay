package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"relay/internal/state"
)

// TestOwnershipLossLatchIsOnceAndNilSafe pins the once-only semantics of the
// ownership latch: the first lose() wins (so the loss side effects run once),
// later calls are no-ops, isLost reflects the state, and a nil latch is safe.
func TestOwnershipLossLatchIsOnceAndNilSafe(t *testing.T) {
	first := errors.New("first verification failure")
	second := errors.New("later verification failure")
	var l ownershipLoss
	if l.isLost() {
		t.Fatal("a fresh latch must not report lost")
	}
	if !l.lose(first) {
		t.Fatal("the first lose() must win")
	}
	if !l.isLost() {
		t.Fatal("isLost must be true after lose()")
	}
	if got := l.cause(); got != first {
		t.Fatalf("cause = %v, want the first verification failure %v", got, first)
	}
	if l.lose(second) {
		t.Fatal("a second lose() must be a no-op")
	}
	if got := l.cause(); got != first {
		t.Fatalf("cause after a later lose = %v, want the preserved first cause %v", got, first)
	}

	var nilLatch *ownershipLoss
	if nilLatch.isLost() {
		t.Fatal("a nil latch must report not lost")
	}
	if nilLatch.cause() != nil {
		t.Fatal("a nil latch must have no cause")
	}
	if nilLatch.lose(first) {
		t.Fatal("a nil latch must not claim the first loss")
	}
}

// TestOwnershipLossReasonClassifiesVerificationFailure pins the stable
// classification carried by the once-only loss warning. Every case fails
// closed; the reason distinguishes the CONFIRMED losses state.VerifyOwnerLock
// reports (the lock is proven gone or the pinned backend changed) from an
// unverified bounded-check timeout, a typed PostgreSQL/network connection
// failure, and any other verification failure. The causes below mirror the
// exact wrapping VerifyOwnerLock produces, and driver text never appears in
// the reason field itself.
func TestOwnershipLossReasonClassifiesVerificationFailure(t *testing.T) {
	cases := []struct {
		name  string
		cause error
		want  string
	}{
		{
			name: "absent session advisory lock",
			cause: fmt.Errorf("%w: %w: session advisory lock for node %q is no longer held",
				state.ErrOwnerLockLost, state.ErrOwnerLockNotHeld, "worker-x"),
			want: ownershipReasonLockNotHeld,
		},
		{
			name: "backend pid changed",
			cause: fmt.Errorf("%w: %w: lock-connection backend pid changed from 100 to 200",
				state.ErrOwnerLockLost, state.ErrOwnerBackendPIDChanged),
			want: ownershipReasonPIDChanged,
		},
		{
			name: "typed PostgreSQL connection error",
			cause: fmt.Errorf("%w: %w: verify on lock connection: %w",
				state.ErrOwnerLockLost, state.ErrOwnerLockConnection, syscall.ECONNREFUSED),
			want: ownershipReasonConnection,
		},
		{
			// A timed-out check also carries the connection sentinel (a
			// context deadline satisfies net.Error); the timeout must win so a
			// bounded check is never misreported as an established connection
			// failure or a lock loss.
			name: "bounded check timed out",
			cause: fmt.Errorf("%w: %w: verify on lock connection: %w",
				state.ErrOwnerLockLost, state.ErrOwnerLockConnection,
				context.DeadlineExceeded),
			want: ownershipReasonTimeout,
		},
		{
			name: "network-layer timeout without a context cause",
			cause: fmt.Errorf("%w: %w: verify on lock connection: %w",
				state.ErrOwnerLockLost, state.ErrOwnerLockConnection,
				fakeTimeoutNetError{}),
			want: ownershipReasonTimeout,
		},
		{
			name: "unknown verification error",
			cause: fmt.Errorf("%w: verify on lock connection: %w",
				state.ErrOwnerLockLost, errors.New("driver: unexpected response")),
			want: ownershipReasonVerificationErr,
		},
		{
			name:  "latched without a cause",
			cause: nil,
			want:  ownershipReasonVerificationErr,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ownershipLossReason(tc.cause); got != tc.want {
				t.Fatalf("ownershipLossReason(%v) = %q, want %q", tc.cause, got, tc.want)
			}
			// Only the confirmed-loss reasons may mark the failure confirmed;
			// timeouts, connection failures, and unknown errors stay
			// fail-closed but unverified.
			wantConfirmed := tc.want == ownershipReasonLockNotHeld ||
				tc.want == ownershipReasonPIDChanged
			if got := ownershipLossConfirmed(tc.cause); got != wantConfirmed {
				t.Fatalf("ownershipLossConfirmed(%v) = %v for reason %q, want %v",
					tc.cause, got, tc.want, wantConfirmed)
			}
		})
	}
}

// fakeTimeoutNetError is a net.Error that reports only a positive timeout, the
// shape of a dial or read that expired at the network layer.
type fakeTimeoutNetError struct{}

func (fakeTimeoutNetError) Error() string   { return "i/o timeout" }
func (fakeTimeoutNetError) Timeout() bool   { return true }
func (fakeTimeoutNetError) Temporary() bool { return true }

// TestOwnershipCheckPreservesVerificationCause pins the detection boundary: a
// failing check on a live lifecycle returns the verification error alongside
// lost/stop so the one warning and the process error can report it, while a
// failure caused by lifecycle cancellation remains an ordinary shutdown with no
// loss cause.
func TestOwnershipCheckPreservesVerificationCause(t *testing.T) {
	cause := errors.Join(state.ErrOwnerLockLost, errors.New("advisory lock not held"))
	lost, stop, got := ownershipCheck(
		context.Background(),
		func(context.Context) error { return cause },
		time.Second,
	)
	if !lost || !stop {
		t.Fatalf("live failing check = (lost=%v, stop=%v), want both true", lost, stop)
	}
	if !errors.Is(got, state.ErrOwnerLockLost) || got.Error() != cause.Error() {
		t.Fatalf("check cause = %v, want the verification error %v", got, cause)
	}

	lifecycle, cancel := context.WithCancel(context.Background())
	cancel()
	lost, stop, got = ownershipCheck(
		lifecycle,
		func(ctx context.Context) error { return ctx.Err() },
		time.Second,
	)
	if lost || !stop || got != nil {
		t.Fatalf("cancelled lifecycle check = (lost=%v, stop=%v, cause=%v), want ordinary shutdown",
			lost, stop, got)
	}
}

// TestMonitorOwnershipDetectsLossOnceAndCancels pins the monitor's loss path
// through the verify seam: a check failure on a LIVE lifecycle is a detected
// loss — it runs the loss action exactly once (readiness cleared, one warning
// carrying the detecting source and the confirmed-loss reason, lifecycle
// cancelled) and the monitor exits. No PostgreSQL is needed; the fake's closed
// channel makes the detection deterministic without sleeps.
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
			// The exact wrapping state.VerifyOwnerLock produces for an absent
			// session advisory lock: a CONFIRMED loss.
			return fmt.Errorf("%w: %w: simulated lock loss",
				state.ErrOwnerLockLost, state.ErrOwnerLockNotHeld)
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
	if ready, reason := ready.Ready(context.Background()); ready || reason != "node ownership lost" {
		t.Fatalf("readiness after loss = (%v, %q), want not-ready with the loss reason", ready, reason)
	}
	if !strings.Contains(logs.String(), "Node ownership lost") {
		t.Fatalf("monitor must log the confirmed-loss warning; logs:\n%s", logs.String())
	}
	if got := strings.Count(logs.String(), "Node ownership lost"); got != 1 {
		t.Fatalf("loss warning emitted %d times, want exactly 1; logs:\n%s", got, logs.String())
	}
	for _, want := range []string{
		"reason=" + ownershipReasonLockNotHeld,
		"source=" + ownershipSourceMonitor,
		"simulated lock loss",
		"node_id=worker-x",
		"instance_id=inst-1",
	} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("loss warning missing %q:\n%s", want, logs.String())
		}
	}
	got := loss.cause()
	if got == nil || !strings.Contains(got.Error(), "simulated lock loss") {
		t.Fatalf("latch cause = %v, want the verification error", got)
	}
	if !errors.Is(got, state.ErrOwnerLockNotHeld) {
		t.Fatalf("latch cause = %v, want the confirmed-loss sentinel preserved", got)
	}
	if src := ownershipLossSource(got); src != ownershipSourceMonitor {
		t.Fatalf("latch source = %q, want %q", src, ownershipSourceMonitor)
	}
}

// TestMonitorOwnershipReportsLossOnlyOnce pins that a detected loss runs the
// loss action exactly once and the loop then returns: the monitor does not keep
// polling (and therefore cannot repeat side effects).
func TestMonitorOwnershipReportsLossOnlyOnce(t *testing.T) {
	var onLostCalls atomic.Int32
	verify := func(context.Context) error { return errors.New("lost") }

	monitorOwnership(context.Background(), verify, func(error) { onLostCalls.Add(1) }, time.Millisecond, time.Millisecond)

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
		monitorOwnership(lifecycle, verify, func(error) { onLostCalls.Add(1) }, time.Millisecond, time.Second)
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
		monitorOwnership(lifecycle, verify, func(error) { onLost.Add(1) }, time.Millisecond, time.Second)
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

	loss.lose(errors.New("lock lost"))
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
	if r, reason := ready.Ready(context.Background()); r || reason != "node ownership lost" {
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
	first := errors.New("lock lost")
	act(first)
	act(errors.New("later loss"))
	act(errors.New("another loss"))
	if cancels.Load() != 1 {
		t.Fatalf("loss action cancelled %d times, want exactly 1", cancels.Load())
	}
	if !loss.isLost() {
		t.Fatal("loss action must latch the loss")
	}
	if got := loss.cause(); got != first {
		t.Fatalf("latched cause = %v, want the first observation %v", got, first)
	}
}

// TestOwnershipWarningDistinguishesConfirmedFromUnverified pins the
// operator-facing wording of the once-only warning: only a CONFIRMED loss (the
// lock is proven gone or the pinned backend changed) claims "Node ownership
// lost", while unverified failures (timeout, connection, unknown) state that
// verification failed. Both classifications still perform every fail-closed
// side effect: latch, readiness clear, lifecycle cancellation.
func TestOwnershipWarningDistinguishesConfirmedFromUnverified(t *testing.T) {
	cases := []struct {
		name      string
		cause     error
		wantMsg   string
		wantNotIn string
	}{
		{
			name: "confirmed absent lock claims the loss",
			cause: fmt.Errorf("%w: %w: session advisory lock for node %q is no longer held",
				state.ErrOwnerLockLost, state.ErrOwnerLockNotHeld, "worker-x"),
			wantMsg:   "Node ownership lost; shutting down",
			wantNotIn: "Node ownership verification failed",
		},
		{
			name: "confirmed backend pid change claims the loss",
			cause: fmt.Errorf("%w: %w: lock-connection backend pid changed from 100 to 200",
				state.ErrOwnerLockLost, state.ErrOwnerBackendPIDChanged),
			wantMsg:   "Node ownership lost; shutting down",
			wantNotIn: "Node ownership verification failed",
		},
		{
			name: "connection failure does not claim a confirmed loss",
			cause: fmt.Errorf("%w: %w: verify on lock connection: %w",
				state.ErrOwnerLockLost, state.ErrOwnerLockConnection, syscall.ECONNREFUSED),
			wantMsg:   "Node ownership verification failed; shutting down",
			wantNotIn: "Node ownership lost",
		},
		{
			name: "bounded-check timeout does not claim a confirmed loss",
			cause: fmt.Errorf("%w: %w: verify on lock connection: %w",
				state.ErrOwnerLockLost, state.ErrOwnerLockConnection,
				context.DeadlineExceeded),
			wantMsg:   "Node ownership verification failed; shutting down",
			wantNotIn: "Node ownership lost",
		},
		{
			name: "unknown failure does not claim a confirmed loss",
			cause: fmt.Errorf("%w: verify on lock connection: %w",
				state.ErrOwnerLockLost, errors.New("driver: unexpected response")),
			wantMsg:   "Node ownership verification failed; shutting down",
			wantNotIn: "Node ownership lost",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lifecycle, cancel := context.WithCancel(context.Background())
			defer cancel()
			var (
				loss    ownershipLoss
				cancels atomic.Int32
				logs    bytes.Buffer
			)
			act := ownershipLossAction(&loss, newReadiness(lifecycle),
				slog.New(slog.NewTextHandler(&logs, nil)),
				"worker-x", "inst-1", func() { cancels.Add(1); cancel() })

			act(tc.cause)

			if got := strings.Count(logs.String(), tc.wantMsg); got != 1 {
				t.Fatalf("warning %q emitted %d times, want exactly 1; logs:\n%s",
					tc.wantMsg, got, logs.String())
			}
			if strings.Contains(logs.String(), tc.wantNotIn) {
				t.Fatalf("warning must not contain %q; logs:\n%s", tc.wantNotIn, logs.String())
			}
			if !loss.isLost() {
				t.Fatal("every classification must latch the loss (fail closed)")
			}
			if cancels.Load() != 1 {
				t.Fatalf("lifecycle cancelled %d times, want exactly 1", cancels.Load())
			}
			if lifecycle.Err() == nil {
				t.Fatal("every classification must cancel the lifecycle")
			}
		})
	}
}

// TestOwnershipWarningReportsDetectingSource pins provenance on the once-only
// warning: whichever path observes the failed verification records WHERE it
// happened — an identity-cleanup proof, the final shutdown verification — and a
// loss latched without provenance reports "unknown". The monitor path is
// pinned in TestMonitorOwnershipDetectsLossOnceAndCancels.
func TestOwnershipWarningReportsDetectingSource(t *testing.T) {
	cause := fmt.Errorf("%w: %w: verify on lock connection: %w",
		state.ErrOwnerLockLost, state.ErrOwnerLockConnection, syscall.ECONNREFUSED)

	assertSource := func(t *testing.T, logs, want string) {
		t.Helper()
		if !strings.Contains(logs, "source="+want) {
			t.Fatalf("warning missing source=%s; logs:\n%s", want, logs)
		}
		if !strings.Contains(logs, "reason="+ownershipReasonConnection) {
			t.Fatalf("warning missing reason=%s; logs:\n%s",
				ownershipReasonConnection, logs)
		}
		if got := strings.Count(logs, "Node ownership verification failed"); got != 1 {
			t.Fatalf("warning emitted %d times, want exactly 1; logs:\n%s", got, logs)
		}
	}

	t.Run("identity-scoped cleanup proof", func(t *testing.T) {
		var logs bytes.Buffer
		loss := &ownershipLoss{}
		proof := ownershipProof{
			loss:   loss,
			verify: func(context.Context) error { return cause },
			onLost: ownershipLossAction(loss, nil,
				slog.New(slog.NewTextHandler(&logs, nil)),
				"worker-x", "inst-1", nil),
		}
		guarded := guardIdentityCleanup(proof, nil, shutdownStepServiceCleanup,
			func(context.Context) error {
				t.Fatal("cleanup must not run after a failed proof")
				return nil
			})
		if err := guarded(context.Background()); err != nil {
			t.Fatalf("guarded = %v, want nil (skipped)", err)
		}
		assertSource(t, logs.String(), ownershipSourceIdentity)
	})

	t.Run("final shutdown verification", func(t *testing.T) {
		var logs bytes.Buffer
		loss := &ownershipLoss{}
		proof := ownershipProof{
			loss:   loss,
			verify: func(context.Context) error { return cause },
			onLost: ownershipLossAction(loss, nil,
				slog.New(slog.NewTextHandler(&logs, nil)),
				"worker-x", "inst-1", nil),
		}
		done := make(chan struct{})
		close(done)
		step := ownershipBarrierStep(done, proof, nil)
		if err := step.run(context.Background()); err != nil {
			t.Fatalf("ownership barrier run = %v, want nil", err)
		}
		assertSource(t, logs.String(), ownershipSourceShutdown)
	})

	t.Run("latched without provenance", func(t *testing.T) {
		var logs bytes.Buffer
		loss := &ownershipLoss{}
		ownershipLossAction(loss, nil,
			slog.New(slog.NewTextHandler(&logs, nil)),
			"worker-x", "inst-1", nil)(cause)
		assertSource(t, logs.String(), ownershipSourceUnknown)
	})
}

// TestOwnershipLossReportedOnceAcrossConcurrentMonitorAndProof pins the
// once-only report under real concurrency: the periodic monitor and an
// identity-cleanup proof rendezvous on the same failed verification, yet
// exactly ONE structured warning is logged, readiness clears, the lifecycle
// cancels once, and the latch keeps the cause and source of whichever
// observation won. The proof's returned error stays its own verification
// failure either way.
func TestOwnershipLossReportedOnceAcrossConcurrentMonitorAndProof(t *testing.T) {
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := newReadiness(lifecycle)

	monitorCause := fmt.Errorf("%w: %w: session advisory lock is no longer held",
		state.ErrOwnerLockLost, state.ErrOwnerLockNotHeld)
	proofCause := fmt.Errorf("%w: %w: verify on lock connection: %w",
		state.ErrOwnerLockLost, state.ErrOwnerLockConnection, syscall.ECONNREFUSED)

	// Both verifications block until BOTH the monitor and the proof are inside
	// them, so the two reports collide deterministically instead of racing by
	// luck. Neither fake honors its bounded context: the rendezvous itself is
	// the bound.
	var arrived atomic.Int32
	rendezvous := make(chan struct{})
	awaitPeer := func() {
		if arrived.Add(1) == 2 {
			close(rendezvous)
		}
		<-rendezvous
	}

	var (
		loss    ownershipLoss
		cancels atomic.Int32
		logs    bytes.Buffer
	)
	act := ownershipLossAction(&loss, ready,
		slog.New(slog.NewTextHandler(&logs, nil)),
		"worker-x", "inst-1", func() {
			cancels.Add(1)
			cancel()
		})

	monitorDone := startOwnershipMonitor(lifecycle, act, ownershipMonitorConfig{
		verify:   func(context.Context) error { awaitPeer(); return monitorCause },
		interval: time.Hour,
		timeout:  5 * time.Second,
	})
	proof := ownershipProof{
		loss:   &loss,
		verify: func(context.Context) error { awaitPeer(); return proofCause },
		onLost: act,
		source: ownershipSourceIdentity,
	}
	proofErrCh := make(chan error, 1)
	go func() { proofErrCh <- proof.prove(5 * time.Second) }()

	waitClosed(t, monitorDone, 5*time.Second)
	var proofErr error
	select {
	case proofErr = <-proofErrCh:
	case <-time.After(5 * time.Second):
		t.Fatal("proof did not complete")
	}

	if got := strings.Count(logs.String(), "shutting down"); got != 1 {
		t.Fatalf("loss warning emitted %d times across monitor and proof, want exactly 1; logs:\n%s",
			got, logs.String())
	}
	if cancels.Load() != 1 {
		t.Fatalf("lifecycle cancelled %d times, want exactly 1", cancels.Load())
	}
	if lifecycle.Err() == nil {
		t.Fatal("the concurrent loss must cancel the lifecycle")
	}
	if r, reason := ready.Ready(context.Background()); r || reason != "node ownership lost" {
		t.Fatalf("readiness after loss = (%v, %q), want not-ready with the loss reason", r, reason)
	}
	if !loss.isLost() {
		t.Fatal("the concurrent loss must latch")
	}

	// The latch keeps the winning observation's cause and source together.
	switch src := ownershipLossSource(loss.cause()); src {
	case ownershipSourceMonitor:
		if !errors.Is(loss.cause(), state.ErrOwnerLockNotHeld) {
			t.Fatalf("monitor-won cause = %v, want the monitor's confirmed loss", loss.cause())
		}
	case ownershipSourceIdentity:
		if !errors.Is(loss.cause(), state.ErrOwnerLockConnection) {
			t.Fatalf("proof-won cause = %v, want the proof's connection failure", loss.cause())
		}
	default:
		t.Fatalf("latch source = %q, want %q or %q",
			src, ownershipSourceMonitor, ownershipSourceIdentity)
	}

	// The proof always returns its OWN verification error, preserved for its
	// caller regardless of who won the latch.
	if proofErr == nil || !errors.Is(proofErr, state.ErrOwnerLockConnection) {
		t.Fatalf("proof error = %v, want its connection failure preserved", proofErr)
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
	if ready, reason := starting.Ready(context.Background()); ready || reason != "node ownership lost" {
		t.Fatalf("ownership lost before ready = (%v, %q), want not-ready with the loss reason", ready, reason)
	}

	ready := newReadiness(context.Background())
	ready.startReady(&fakeConsumerHealth{healthy: true}, &fakeDockerReadiness{netOK: true}, nil)
	if r, _ := ready.Ready(context.Background()); !r {
		t.Fatal("worker must be ready before the loss")
	}
	ready.setOwnershipLost()
	if r, reason := ready.Ready(context.Background()); r || reason != "node ownership lost" {
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
	verifyErr := errors.New("verify on lock connection: backend terminated")
	lost.lose(verifyErr)
	if err := ownershipRunError(lost, nil); !errors.Is(err, errOwnershipLost) || !errors.Is(err, verifyErr) {
		t.Fatalf("lost nil result = %v, want errOwnershipLost and the preserved verification cause", err)
	}
	consumeErr := errors.New("consume failed")
	err := ownershipRunError(lost, consumeErr)
	if !errors.Is(err, errOwnershipLost) || !errors.Is(err, consumeErr) || !errors.Is(err, verifyErr) {
		t.Fatalf("lost result = %v, want errOwnershipLost, the preserved cause, and the consume error", err)
	}
	if !strings.Contains(err.Error(), "backend terminated") {
		t.Fatalf("lost result = %q, want the verification cause preserved in the message", err)
	}

	// A latch recorded through the shared loss action carries the detection
	// wrapper; Run's result must preserve both the typed cause and the
	// detecting source for the process boundary.
	cause := fmt.Errorf("%w: %w: verify on lock connection: %w",
		state.ErrOwnerLockLost, state.ErrOwnerLockConnection,
		errors.New("backend terminated"))
	detected := &ownershipLoss{}
	detected.lose(&ownershipDetection{source: ownershipSourceMonitor, err: cause})
	err = ownershipRunError(detected, consumeErr)
	if !errors.Is(err, errOwnershipLost) || !errors.Is(err, cause) ||
		!errors.Is(err, state.ErrOwnerLockLost) || !errors.Is(err, consumeErr) {
		t.Fatalf("detected lost result = %v, want every cause preserved", err)
	}
	var detection *ownershipDetection
	if !errors.As(err, &detection) || detection.source != ownershipSourceMonitor {
		t.Fatalf("detected lost result = %v, want the detecting source preserved", err)
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
	loss.lose(errors.New("lock lost"))
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

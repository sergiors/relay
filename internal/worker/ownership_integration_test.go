//go:build integration

// This file proves the worker ownership monitor against a real PostgreSQL
// server. It is excluded from the default suite by the integration build tag and
// REQUIRES a reachable PostgreSQL at TEST_DATABASE_URL (the CI service
// container). A missing dependency FAILS rather than skips, matching the
// Redis/Docker integration convention.
package worker

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"relay/internal/state"
	"relay/internal/testutil"
)

// TestIntegrationOwnershipMonitorDetectsTerminatedLockBackend proves the
// end-to-end detection path over a real server: the monitor polls a worker state
// handle's pinned WORKER_ID lock, the pinned backend is terminated from a
// separate connection (the residual "lock lost while the process runs" case),
// and the monitor latches the loss, clears readiness, and cancels the lifecycle.
// A replacement incarnation of the same worker can then take the identity.
func TestIntegrationOwnershipMonitorDetectsTerminatedLockBackend(t *testing.T) {
	testutil.RequireStateDatabase(t)
	dsn := testutil.NewStateDSN(t)

	owner := testutil.UniqueName(t, "owner-monitor")
	st := testutil.OpenState(t, dsn, state.Options{Owner: owner, Instance: "inst-1"})
	ownerPID := st.OwnerBackendPID()
	if ownerPID <= 0 {
		t.Fatalf("owner backend pid = %d, want > 0", ownerPID)
	}

	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := newReadiness(lifecycle)
	ready.setReady()
	loss := &ownershipLoss{}

	done := startOwnershipMonitor(lifecycle,
		ownershipLossAction(loss, ready, slog.New(slog.NewTextHandler(io.Discard, nil)), owner, "inst-1", cancel),
		ownershipMonitorConfig{
			verify:   st.VerifyOwnerLock,
			interval: 50 * time.Millisecond,
			timeout:  time.Second,
		})

	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open admin connection: %v", err)
	}
	defer admin.Close()
	if _, err := admin.ExecContext(context.Background(), `SELECT pg_terminate_backend($1)`, ownerPID); err != nil {
		t.Fatalf("terminate owner backend: %v", err)
	}

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("ownership monitor did not detect the terminated lock backend")
	}
	if !loss.isLost() {
		t.Fatal("monitor must latch the loss")
	}
	if lifecycle.Err() == nil {
		t.Fatal("monitor must cancel the lifecycle on loss")
	}
	if ready, reason := ready.Ready(context.Background()); ready || reason != "worker ownership lost" {
		t.Fatalf("readiness after loss = (%v, %q), want not-ready with the loss reason", ready, reason)
	}

	// The session advisory lock died with the backend, so a replacement
	// incarnation of the same WORKER_ID can open and verify.
	replacement := testutil.OpenState(t, dsn, state.Options{Owner: owner, Instance: "inst-2"})
	if err := replacement.VerifyOwnerLock(context.Background()); err != nil {
		t.Fatalf("replacement VerifyOwnerLock = %v, want nil", err)
	}
}

// TestIntegrationFinalOwnershipVerificationDetectsMissedLoss proves the fix for
// the residual shutdown race against a real server: the lifecycle is already
// cancelled (as Run's shutdown defer leaves it), so the monitor would classify a
// failing check as ordinary shutdown and exit WITHOUT reporting a loss, and the
// pinned backend is then terminated. The ownership barrier's final
// background-context verification must STILL detect the lost lock and latch it,
// so the identity-scoped cleanup guards that run later skip their work.
func TestIntegrationFinalOwnershipVerificationDetectsMissedLoss(t *testing.T) {
	testutil.RequireStateDatabase(t)
	dsn := testutil.NewStateDSN(t)

	st := testutil.OpenState(t, dsn, state.Options{Owner: "worker-final", Instance: "inst-1"})
	ownerPID := st.OwnerBackendPID()
	if ownerPID <= 0 {
		t.Fatalf("owner backend pid = %d, want > 0", ownerPID)
	}

	// The lifecycle is already cancelled, exactly as Run's shutdown defer leaves
	// it; the monitor (not started here) would treat a failing check as ordinary
	// shutdown. readiness was true before the loss.
	lifecycle, cancel := context.WithCancel(context.Background())
	cancel()
	ready := newReadiness(lifecycle)
	ready.setReady()
	loss := &ownershipLoss{}
	proof := ownershipProof{
		loss:   loss,
		verify: st.VerifyOwnerLock,
		onLost: ownershipLossAction(loss, ready, slog.New(slog.NewTextHandler(io.Discard, nil)), "worker-final", "inst-1", cancel),
	}

	// Terminate the pinned lock backend from a SEPARATE connection: the lock was
	// lost since the monitor's last (missed) tick.
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open admin connection: %v", err)
	}
	defer admin.Close()
	if _, err := admin.ExecContext(context.Background(), `SELECT pg_terminate_backend($1)`, ownerPID); err != nil {
		t.Fatalf("terminate owner backend: %v", err)
	}

	// The monitor join already completed (done closed); the barrier must still
	// run its final verification despite the cancelled lifecycle.
	monitorDone := make(chan struct{})
	close(monitorDone)
	step := ownershipBarrierStep(monitorDone, proof, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := step.run(context.Background()); err != nil {
		t.Fatalf("ownership barrier run = %v, want nil", err)
	}
	if !loss.isLost() {
		t.Fatal("the final verification must latch a loss the cancelled-lifecycle monitor missed")
	}
	if r, reason := ready.Ready(context.Background()); r || reason != "worker ownership lost" {
		t.Fatalf("readiness after the final verification = (%v, %q), want not-ready with the loss reason", r, reason)
	}
}

// TestIntegrationVerifyOwnerLockConcurrentUnderMonitor is the regression stress
// test for sharing ONE pinned lock connection between the ownership monitor and
// the startup/shutdown ownership proofs: the periodic monitor polls
// st.VerifyOwnerLock while many callers invoke the SAME method concurrently
// against the same State handle. While the backend is healthy every verification
// must succeed and the monitor must never latch a spurious loss. Before the
// State connection guard, the interleaved queries reached pgx at once and it
// failed them with "conn busy", which the monitor would misread as a lost lock
// and use to shut the worker down.
func TestIntegrationVerifyOwnerLockConcurrentUnderMonitor(t *testing.T) {
	testutil.RequireStateDatabase(t)
	dsn := testutil.NewStateDSN(t)

	owner := testutil.UniqueName(t, "owner-stress")
	st := testutil.OpenState(t, dsn, state.Options{Owner: owner, Instance: "inst-1"})

	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := newReadiness(lifecycle)
	ready.setReady()
	loss := &ownershipLoss{}
	done := startOwnershipMonitor(lifecycle,
		ownershipLossAction(loss, ready, slog.New(slog.NewTextHandler(io.Discard, nil)), owner, "inst-1", cancel),
		ownershipMonitorConfig{
			verify:   st.VerifyOwnerLock,
			interval: 10 * time.Millisecond,
			timeout:  time.Second,
		})

	const (
		callers = 8
		rounds  = 50
	)
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < rounds; j++ {
				ctx, ctxCancel := context.WithTimeout(context.Background(), 10*time.Second)
				err := st.VerifyOwnerLock(ctx)
				ctxCancel()
				if err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
					return
				}
			}
		}()
	}
	wg.Wait()
	if len(errs) > 0 {
		t.Fatalf("concurrent VerifyOwnerLock reported %d spurious failures (first: %v) while the backend was healthy", len(errs), errs[0])
	}
	if loss.isLost() {
		t.Fatal("monitor latched a spurious ownership loss while the backend stayed healthy")
	}

	// The monitor must stop on ordinary cancellation without reporting a loss.
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("ownership monitor did not stop after cancellation")
	}
	if loss.isLost() {
		t.Fatal("monitor reported a loss on ordinary cancellation")
	}
}

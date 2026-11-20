//go:build integration

package deployment

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/state"
	"relay/internal/testutil"
)

// TestConvergencePassesNeverScanAppsTree pins the scanner's scope: the
// desired-poll/retry/wake convergence passes only re-read the shared desired
// row and never load /apps when the desired revision equals the applied one —
// the bounded drift backstop is the loop's only filesystem scanner, which the
// control subtest proves by inverting the drift interval.
func TestConvergencePassesNeverScanAppsTree(t *testing.T) {
	t.Run("unchanged desired/applied passes perform no scan", func(t *testing.T) {
		env := newCoordinatorEnv(t)
		var loadCalls atomic.Int64
		cfg := env.cfg
		// Fast convergence cadences, drift cadence an hour away: every pass in
		// this window comes from the poll ticker or an explicit wake.
		cfg.PollInterval = 10 * time.Millisecond
		cfg.RetryInterval = 10 * time.Millisecond
		cfg.DriftInterval = time.Hour
		cfg.LoadApps = func(string) ([]app.App, []app.LoadIssue, error) {
			loadCalls.Add(1)
			return nil, nil, nil
		}
		seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne)
		ctx := context.Background()

		c := New(cfg)
		if _, err := c.ObserveStartup(ctx); err != nil {
			t.Fatalf("ObserveStartup: %v", err)
		}
		// Desired equals applied (generation AND commit), so every pass takes
		// the unchanged-generation fast path.
		publishDesired(t, env.st, commitOne, "main")
		baseline := readDeploymentNode(t, env)
		if c.retryPending() {
			t.Fatal("precondition: no retryable work after a clean startup")
		}

		runCtx, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)
		done := c.Start(runCtx, nil, nil)
		for i := 0; i < 5; i++ {
			c.Wake()
		}
		// Let the 10ms poll ticks and the queued wakes run several passes.
		select {
		case <-time.After(300 * time.Millisecond):
		}
		if n := loadCalls.Load(); n != 0 {
			t.Fatalf("LoadApps calls = %d, want 0: an unchanged desired/applied "+
				"pass must not scan /apps (only the drift backstop may)", n)
		}
		after := readDeploymentNode(t, env)
		if after.Incarnation != baseline.Incarnation || after.Phase != baseline.Phase {
			t.Fatalf("node progress moved %+v -> %+v; an unchanged pass must not "+
				"rematerialize", baseline, after)
		}

		// Prove the loop actually processed wake passes during the window: a
		// failed desired read arms the retry state, which only a live pass can
		// do — and that pass still performs no scan.
		if err := env.st.Close(); err != nil {
			t.Fatalf("close state: %v", err)
		}
		c.Wake()
		testutil.WaitFor(t, 5*time.Second, "wake processed by the live loop", func() bool {
			return c.retryPending()
		})
		if n := loadCalls.Load(); n != 0 {
			t.Fatalf("LoadApps calls after failed desired reads = %d, want 0", n)
		}

		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("coordinator loop did not exit after cancellation")
		}
	})

	t.Run("control: the drift cadence performs the scan", func(t *testing.T) {
		env := newCoordinatorEnv(t)
		var loadCalls atomic.Int64
		cfg := env.cfg
		cfg.PollInterval = time.Hour
		cfg.RetryInterval = time.Hour
		cfg.DriftInterval = 30 * time.Millisecond
		cfg.LoadApps = func(string) ([]app.App, []app.LoadIssue, error) {
			loadCalls.Add(1)
			return nil, nil, nil
		}
		seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne)

		c := New(cfg)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		if _, err := c.ObserveStartup(ctx); err != nil {
			t.Fatalf("ObserveStartup: %v", err)
		}
		done := c.Start(ctx, nil, nil)

		// With poll and retry an hour away, any scan can only come from the
		// drift backstop — proving the counting seam above observes every
		// /apps load the loop performs.
		testutil.WaitFor(t, 5*time.Second, "drift backstop scan", func() bool {
			return loadCalls.Load() > 0
		})

		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("coordinator loop did not exit after cancellation")
		}
	})
}

// TestCheckDriftSkipsAppsVerifiedByReconciler pins the verification-ledger
// contract of the independent drift backstop: the ledger window is exactly one
// drift interval, an authorized app the reconciler verified within that window
// is never re-hashed (its bytes are neither trusted nor re-compared here — the
// reconciler owns that verdict), an app outside the window IS hashed so local
// edits are still detected, and the root name audit (a missing authorized app)
// never trusts the ledger.
func TestCheckDriftSkipsAppsVerifiedByReconciler(t *testing.T) {
	env := newCoordinatorEnv(t)
	fingerprint, templateID := writeCoordinatorApp(t, env.cfg.AppsDir, "fn")
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne,
		seededApp{name: "fn", fingerprint: fingerprint, templateID: templateID,
			phase: state.DeploymentAppPending})
	builtAt := time.Now()
	c := New(env.cfg)
	ctx := context.Background()
	if _, err := c.ObserveStartup(ctx); err != nil {
		t.Fatalf("ObserveStartup: %v", err)
	}

	var lastSince time.Time
	c.SetVerificationLedger(func(since time.Time) map[string]struct{} {
		lastSince = since
		return map[string]struct{}{"fn": {}}
	})

	// A local edit lands, but the reconciler already verified "fn" within the
	// interval: the backstop must not hash the same bytes to re-reach that
	// verdict.
	indexPath := filepath.Join(env.cfg.AppsDir, "fn", "index.js")
	if err := os.WriteFile(indexPath,
		[]byte("export function hi(e, context){ /* drifted */ }\n"), 0o644); err != nil {
		t.Fatalf("edit app: %v", err)
	}
	c.checkDrift(ctx)
	select {
	case <-c.kick:
		t.Fatal("checkDrift re-hashed an app the reconciler verified within the " +
			"drift interval")
	default:
	}
	// The ledger window is the previous-check boundary (sliding): the first
	// check's window reaches back to construction, so everything verified since
	// then is trusted by exactly this one check.
	now := time.Now()
	if lastSince.Before(builtAt) || lastSince.After(now) {
		t.Fatalf("first ledger window since = %v, want the construction boundary "+
			"in [%v, %v]", lastSince, builtAt, now)
	}

	// Outside the verification window the backstop hashes again and detects
	// the edit.
	c.SetVerificationLedger(func(time.Time) map[string]struct{} { return nil })
	c.checkDrift(ctx)
	select {
	case <-c.kick:
	default:
		t.Fatal("checkDrift missed a locally edited app outside the verification " +
			"window")
	}

	// The name audit never trusts the ledger: a missing authorized app is drift
	// even when the reconciler verified it moments ago.
	if err := os.RemoveAll(filepath.Join(env.cfg.AppsDir, "fn")); err != nil {
		t.Fatalf("remove app: %v", err)
	}
	c.SetVerificationLedger(func(time.Time) map[string]struct{} {
		return map[string]struct{}{"fn": {}}
	})
	c.checkDrift(ctx)
	select {
	case <-c.kick:
	default:
		t.Fatal("checkDrift ignored a missing authorized app because the ledger " +
			"marked it verified")
	}
}

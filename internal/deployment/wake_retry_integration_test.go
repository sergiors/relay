//go:build integration

package deployment

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/state"
	"relay/internal/testutil"
)

// Wake/retry cadence contracts against real PostgreSQL. Materialization
// attempts are observable through the node-progress claim
// (AcquireDeploymentNode bumps the incarnation BEFORE any Git work), so these
// tests count attempts without a real remote repository: an unreadable local
// source makes every attempt fail fast right after the claim.

// readDeploymentNode returns the node's current progress row.
func readDeploymentNode(t *testing.T, env *coordinatorEnv) state.DeploymentNodeProgress {
	t.Helper()
	node, found, err := env.st.ReadDeploymentNode(context.Background(), env.cfg.NodeID)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v), want the seeded row", node, found, err)
	}
	return node
}

// waitDeploymentNode waits until the node-progress row satisfies pred. Each
// materialization attempt claims a fresh incarnation, so an incarnation
// increase is exactly one new attempt.
func waitDeploymentNode(
	t *testing.T, env *coordinatorEnv, what string,
	pred func(state.DeploymentNodeProgress) bool,
) state.DeploymentNodeProgress {
	t.Helper()
	var latest state.DeploymentNodeProgress
	testutil.WaitFor(t, 5*time.Second, what, func() bool {
		node, found, err := env.st.ReadDeploymentNode(context.Background(), env.cfg.NodeID)
		if err != nil || !found {
			return false
		}
		latest = node
		return pred(node)
	})
	return latest
}

// TestNormalWakeOnlyChecksUnchangedGeneration pins the split between the
// database-hint path and drift repair: a non-forcing pass (what Wake and every
// delivered notification run) re-reads the desired row but performs NO
// materialization attempt when the shared revision equals the applied one,
// while a forced pass (Kick/ReportDrift) rematerializes the unchanged
// generation and records the failure as retryable work for the short retry
// cadence.
func TestNormalWakeOnlyChecksUnchangedGeneration(t *testing.T) {
	env := newCoordinatorEnv(t)
	if err := os.WriteFile(env.cfg.GitConfigPath, []byte("{not json"), 0o600); err != nil {
		t.Fatalf("write broken config: %v", err)
	}
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne)
	ctx := context.Background()

	c := New(env.cfg)
	snapshot, err := c.ObserveStartup(ctx)
	if err != nil {
		t.Fatalf("ObserveStartup: %v", err)
	}
	if !snapshot.Authoritative || snapshot.Generation != 1 || snapshot.Error != nil {
		t.Fatalf("startup snapshot = %+v, want the applied generation 1 without an attempt",
			snapshot)
	}
	// The shared revision now equals the applied state; nothing is pending.
	publishDesired(t, env.st, commitOne, "main")
	if c.retryPending() {
		t.Fatal("precondition: no retryable work after a clean startup observation")
	}
	before := readDeploymentNode(t, env)

	// When the wake path only checks: unchanged generation AND commit means
	// no claim, no attempt, no log.
	c.syncOnce(ctx, func(string) {}, false)
	after := readDeploymentNode(t, env)
	if after.Incarnation != before.Incarnation || after.Phase != before.Phase {
		t.Fatalf("normal pass moved node progress %+v -> %+v; "+
			"an unchanged generation must not rematerialize", before, after)
	}
	if strings.Contains(env.log.String(), "materialization failed") {
		t.Fatalf("normal pass attempted materialization:\n%s", env.log.String())
	}
	if c.retryPending() {
		t.Fatal("a successful check must not arm the retry cadence")
	}

	// The forced drift path DOES rematerialize the unchanged generation...
	c.syncOnce(ctx, func(string) {}, true)
	forced := readDeploymentNode(t, env)
	if forced.Incarnation <= before.Incarnation {
		t.Fatalf("forced pass incarnation = %d, want a fresh claim above %d",
			forced.Incarnation, before.Incarnation)
	}
	if !strings.Contains(env.log.String(),
		"materialization failed; retaining active generation") {
		t.Fatalf("forced pass did not attempt (and report) materialization:\n%s",
			env.log.String())
	}
	if forced.Phase != state.DeploymentNodeFailed || forced.AppliedGeneration != 1 {
		t.Fatalf("forced node = %+v, want a failed attempt that keeps generation 1 applied",
			forced)
	}
	// ...and arms the short retry cadence as pending work.
	if !c.retryPending() {
		t.Fatal("a failed forced attempt must be retryable work")
	}
}

// TestWakeDrivesConvergenceWithoutPollOrRetry pins that the non-forcing wake
// converges a published generation on its own: with the fallback poll, the
// retry ticker, and the drift ticker all moved an hour out, the startup pass
// converges the published change, an already-failed attempt does not spin
// early on its own, and a further generation converges only when woken.
func TestWakeDrivesConvergenceWithoutPollOrRetry(t *testing.T) {
	env := newCoordinatorEnv(t)
	if err := os.WriteFile(env.cfg.GitConfigPath, []byte("{not json"), 0o600); err != nil {
		t.Fatalf("write broken config: %v", err)
	}
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne)
	ctx := context.Background()

	cfg := env.cfg
	cfg.PollInterval = time.Hour
	cfg.RetryInterval = time.Hour
	cfg.DriftInterval = time.Hour
	c := New(cfg)
	if _, err := c.ObserveStartup(ctx); err != nil {
		t.Fatalf("ObserveStartup: %v", err)
	}
	baseline := readDeploymentNode(t, env)

	// A new commit is published before the loop starts.
	publishDesired(t, env.st, commitTwo, "main")
	runCtx, cancel := context.WithCancel(ctx)
	done := c.Start(runCtx, nil, nil)

	// The immediate startup pass converges it without waiting for any timer...
	waitDeploymentNode(t, env, "startup pass attempt", func(n state.DeploymentNodeProgress) bool {
		return n.Incarnation > baseline.Incarnation
	})

	// ...then goes quiet: with poll, retry, and drift an hour away, the failed
	// attempt (which IS pending retry work) must not fire early on its own.
	quiet := readDeploymentNode(t, env)
	select {
	case <-time.After(400 * time.Millisecond):
	}
	if again := readDeploymentNode(t, env); again.Incarnation != quiet.Incarnation {
		t.Fatalf("attempt fired while only the hour timers were armed (incarnation %d -> %d)",
			quiet.Incarnation, again.Incarnation)
	}

	// A further generation converges ONLY through the wake.
	publishDesired(t, env.st, commitOne, "main") // generation 2
	c.Wake()
	waitDeploymentNode(t, env, "wake-driven generation 2 attempt",
		func(n state.DeploymentNodeProgress) bool {
			return n.Generation == 2
		})

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("coordinator loop did not exit after cancellation")
	}
}

// TestRetryTickerRunsNoPassWithoutPendingWork pins the pending-only gate: the
// short retry ticker fires continuously, but with no failed read and no failed
// materialization it must not run a convergence pass — while a wake still
// drives one, proving the loop is healthy rather than stalled. The fallback
// poll is an hour away throughout, so the wake is the only possible trigger.
func TestRetryTickerRunsNoPassWithoutPendingWork(t *testing.T) {
	env := newCoordinatorEnv(t)
	cfg := env.cfg
	cfg.PollInterval = time.Hour
	cfg.RetryInterval = 40 * time.Millisecond
	cfg.DriftInterval = time.Hour
	c := New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	done := c.Start(ctx, nil, nil)

	// Let the coordinator's immediate startup pass settle BEFORE publishing:
	// with no desired row yet it must claim nothing, and no retry work is
	// armed — so every retry tick that follows stays gated.
	select {
	case <-time.After(400 * time.Millisecond):
	}

	// A revision is published with nothing pending: an ungated retry ticker
	// would claim it within one 40ms tick (and a previously armed failure
	// would keep re-driving it), so a silent node row proves the gate.
	publishDesired(t, env.st, commitOne, "main")
	select {
	case <-time.After(400 * time.Millisecond):
	}
	if node, found, err := env.st.ReadDeploymentNode(ctx, env.cfg.NodeID); err != nil ||
		found || node.NodeID != "" {
		t.Fatalf("retry ticker ran a pass with no pending work (node = %+v, found %v, err %v)",
			node, found, err)
	}

	// A wake still converges: the gate only silences the ticker, not the loop.
	c.Wake()
	waitDeploymentNode(t, env, "wake-driven pass", func(n state.DeploymentNodeProgress) bool {
		return n.Generation == 1
	})

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("coordinator loop did not exit after cancellation")
	}
}

// TestFailedMaterializationRetriesOnShortCadence pins the prompt-retry half:
// with the fallback poll an hour away, a failed Git materialization is
// re-driven on the short ticker — because, and only while, the failure is
// pending.
func TestFailedMaterializationRetriesOnShortCadence(t *testing.T) {
	env := newCoordinatorEnv(t)
	if err := os.WriteFile(env.cfg.GitConfigPath, []byte("{not json"), 0o600); err != nil {
		t.Fatalf("write broken config: %v", err)
	}
	publishDesired(t, env.st, commitOne, "main")
	cfg := env.cfg
	cfg.PollInterval = time.Hour
	cfg.RetryInterval = 40 * time.Millisecond
	cfg.DriftInterval = time.Hour
	c := New(cfg)
	ctx := context.Background()

	snapshot, err := c.ObserveStartup(ctx) // the startup attempt fails -> retry armed
	if err != nil {
		t.Fatalf("ObserveStartup: %v", err)
	}
	if snapshot.Error == nil {
		t.Fatal("precondition: the startup materialization must fail against the broken config")
	}
	baseline := readDeploymentNode(t, env)

	runCtx, cancel := context.WithCancel(ctx)
	done := c.Start(runCtx, nil, nil)
	// The immediate pass retries the pending failure...
	waitDeploymentNode(t, env, "startup retry pass", func(n state.DeploymentNodeProgress) bool {
		return n.Incarnation > baseline.Incarnation
	})
	// ...and the short ticker keeps re-driving it far faster than the
	// hour-away fallback poll ever could.
	afterFirst := readDeploymentNode(t, env)
	waitDeploymentNode(t, env, "short-cadence retry pass",
		func(n state.DeploymentNodeProgress) bool {
			return n.Incarnation > afterFirst.Incarnation
		})

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("coordinator loop did not exit after cancellation")
	}
}

// TestFailedDesiredReadIsPendingRetryWork pins the other retryable state: a
// failed desired-state read arms the short retry cadence instead of silently
// waiting for the fallback poll. (Any later successful read clears it —
// pinned by TestNormalWakeOnlyChecksUnchangedGeneration.)
func TestFailedDesiredReadIsPendingRetryWork(t *testing.T) {
	env := newCoordinatorEnv(t)
	c := New(env.cfg)
	ctx := context.Background()

	// Closing the handle makes every desired-row read fail.
	if err := env.st.Close(); err != nil {
		t.Fatalf("close state: %v", err)
	}
	c.syncOnce(ctx, func(string) {}, false)
	if !strings.Contains(env.log.String(), "read desired revision failed") {
		t.Fatalf("failed read was not reported:\n%s", env.log.String())
	}
	if !c.retryPending() {
		t.Fatal("a failed desired-state read must be pending retry work")
	}
}

// TestWakeQueuedDuringActiveSyncIsNotLost pins the coalescing guarantee end to
// end: while the loop goroutine is busy inside one pass (the drift backstop
// held in its /apps load), wakes queue rather than block or vanish, and the
// queued pass converges a revision published during that activity — with poll
// and retry an hour away, that queued wake is the only possible trigger.
func TestWakeQueuedDuringActiveSyncIsNotLost(t *testing.T) {
	env := newCoordinatorEnv(t)
	if err := os.WriteFile(env.cfg.GitConfigPath, []byte("{not json"), 0o600); err != nil {
		t.Fatalf("write broken config: %v", err)
	}
	// Authoritative applied generation with no app rows: the drift backstop
	// runs its /apps check, reaches the load seam, and later sees a matching
	// (empty) tree, so it never forces a pass of its own.
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne)

	gateEntered := make(chan struct{})
	releaseGate := make(chan struct{})
	var gateOnce sync.Once
	cfg := env.cfg
	cfg.PollInterval = time.Hour
	cfg.RetryInterval = time.Hour
	cfg.DriftInterval = 30 * time.Millisecond
	cfg.LoadApps = func(string) ([]app.App, []app.LoadIssue, error) {
		gateOnce.Do(func() { close(gateEntered) })
		<-releaseGate
		return nil, nil, nil
	}

	ctx := context.Background()
	c := New(cfg)
	if _, err := c.ObserveStartup(ctx); err != nil {
		t.Fatalf("ObserveStartup: %v", err)
	}
	baseline := readDeploymentNode(t, env)

	runCtx, cancel := context.WithCancel(ctx)
	done := c.Start(runCtx, nil, nil)

	// The drift backstop enters its /apps check and holds the loop goroutine.
	select {
	case <-gateEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("drift backstop never reached its /apps load")
	}
	// While that pass is active, a new revision is published and hinted.
	publishDesired(t, env.st, commitTwo, "main")
	c.Wake()
	c.Wake() // coalesces: never blocks, never duplicates the pending signal
	if pending := len(c.kick); pending != 1 {
		t.Fatalf("pending wake signals during an active pass = %d, want 1 (queued, not lost)",
			pending)
	}
	close(releaseGate)

	// Poll and retry are an hour away: only the queued wake can run the pass
	// that claims the published revision.
	waitDeploymentNode(t, env, "wake queued during an active pass",
		func(n state.DeploymentNodeProgress) bool {
			return n.Incarnation > baseline.Incarnation
		})

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("coordinator loop did not exit after cancellation")
	}
}

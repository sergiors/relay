package worker

import (
	"context"
	"testing"
	"time"

	"relay/internal/deployment"
	"relay/internal/state"
)

// TestNextDeploymentListenBackoffDoublesToCap pins the bounded reconnect
// cadence: 1s doubling per failed subscription or dropped session, capped at
// 30s, so a persistent listener outage retries promptly at first without ever
// spinning.
func TestNextDeploymentListenBackoffDoublesToCap(t *testing.T) {
	if deploymentListenRetryInitial != time.Second {
		t.Fatalf("deploymentListenRetryInitial = %s, want 1s", deploymentListenRetryInitial)
	}
	if deploymentListenRetryMax != 30*time.Second {
		t.Fatalf("deploymentListenRetryMax = %s, want 30s", deploymentListenRetryMax)
	}
	want := []time.Duration{
		2 * time.Second, 4 * time.Second, 8 * time.Second,
		16 * time.Second, 30 * time.Second, 30 * time.Second,
	}
	backoff := deploymentListenRetryInitial
	for i, expected := range want {
		backoff = nextDeploymentListenBackoff(backoff)
		if backoff != expected {
			t.Fatalf("backoff step %d = %s, want %s", i+1, backoff, expected)
		}
	}
}

// TestDeploymentListenerBarrierPrecedesDeploymentAndState pins the teardown
// ordering the listener's shutdown safety depends on: the supervisor is joined
// (and its dedicated connection released) BEFORE the coordinator loop it wakes
// and BEFORE the State handle whose pool it borrows closes. The step is a
// strict quiescence barrier like the other shared-dependency joins.
func TestDeploymentListenerBarrierPrecedesDeploymentAndState(t *testing.T) {
	indexOf := func(name string) int {
		for i, step := range shutdownStepOrder {
			if step == name {
				return i
			}
		}
		return -1
	}
	listenIdx := indexOf(shutdownStepDeploymentListen)
	deployIdx := indexOf(shutdownStepDeployment)
	stateIdx := indexOf(shutdownStepState)
	switch {
	case listenIdx < 0:
		t.Fatalf("%q is missing from the shutdown order %v",
			shutdownStepDeploymentListen, shutdownStepOrder)
	case listenIdx >= deployIdx:
		t.Fatalf("deployment-listen (%d) must be joined before deployment (%d)",
			listenIdx, deployIdx)
	case listenIdx >= stateIdx:
		t.Fatalf("deployment-listen (%d) must be joined before state (%d)",
			listenIdx, stateIdx)
	}

	done := make(chan struct{})
	step := deploymentListenerBarrierStep(done)
	if step.name != shutdownStepDeploymentListen || !step.barrier {
		t.Fatalf("step = %+v, want a barrier named %q",
			step, shutdownStepDeploymentListen)
	}
	// Strict join: with its bound already expired, run must still wait for
	// the supervisor to exit before any later step could close State.
	assertBarrierRunBlocksPastCancelledContext(t, step, func() { close(done) })
}

// TestStartDeploymentListenerSkipsWithoutDependencies pins the wiring
// contract: without both a State handle and a coordinator there is nothing to
// supervise, so no goroutine starts and no shutdown step is registered.
func TestStartDeploymentListenerSkipsWithoutDependencies(t *testing.T) {
	ctx := context.Background()

	withoutState := &shutdownRegistry{}
	startDeploymentListener(ctx, discardLogger(), nil,
		deployment.New(deployment.Config{}), withoutState)
	if len(withoutState.steps) != 0 {
		t.Fatalf("registered %d steps without a state handle, want none",
			len(withoutState.steps))
	}

	withoutCoordinator := &shutdownRegistry{}
	startDeploymentListener(ctx, discardLogger(), &state.State{}, nil,
		withoutCoordinator)
	if len(withoutCoordinator.steps) != 0 {
		t.Fatalf("registered %d steps without a coordinator, want none",
			len(withoutCoordinator.steps))
	}
}

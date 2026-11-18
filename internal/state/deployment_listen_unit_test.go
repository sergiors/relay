package state

import (
	"context"
	"strings"
	"testing"
)

// TestListenDeploymentDesiredFailsSafeWithoutAnOpenHandle pins the
// configuration boundary: a nil or unopened state handle reports the missing
// connection instead of panicking or handing out a half-built listener.
func TestListenDeploymentDesiredFailsSafeWithoutAnOpenHandle(t *testing.T) {
	var nilState *State
	if _, err := nilState.ListenDeploymentDesired(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "state is not open") {
		t.Fatalf("nil state ListenDeploymentDesired err = %v, want a state-not-open error", err)
	}

	unopened := &State{}
	if _, err := unopened.ListenDeploymentDesired(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "state is not open") {
		t.Fatalf("unopened state ListenDeploymentDesired err = %v, want a state-not-open error", err)
	}
}

// TestListenerNilReceiverFailsSafe pins the nil-receiver contract of the
// listener helpers: a nil Wait reports an error, and a nil Close is a no-op.
func TestListenerNilReceiverFailsSafe(t *testing.T) {
	var listener *DeploymentDesiredListener
	if _, err := listener.Wait(context.Background()); err == nil {
		t.Fatal("nil listener Wait returned nil error, want a failure")
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("nil listener Close = %v, want nil", err)
	}
}

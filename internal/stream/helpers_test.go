package stream

import (
	"context"
	"testing"
	"time"
)

// invocationStateFromCtx returns the per-message InvocationState carried in
// ctx. It reports failure with t.Errorf (which, unlike t.Fatalf/t.FailNow, is
// safe to call from a handler goroutine) and returns ok=false so the handler
// can return an error instead of aborting the test goroutine. testutil.WaitFor
// still times out if no handler ever claims the message, so a missing state
// does not silently pass.
func invocationStateFromCtx(t *testing.T, ctx context.Context) (InvocationState, bool) {
	t.Helper()
	p, ok := InvocationStateFrom(ctx)
	if !ok {
		t.Errorf("no invocation state in ctx")
	}
	return p, ok
}

// waitSustained polls pred until it has held continuously for the given
// duration. It is the deterministic replacement for a fixed "grace period"
// sleep when we must assert that some state persists for a minimum window
// (e.g. a message stays pending / a protected invocation is not re-run). It
// fails the test if the state does not hold. Callers must invoke it from the
// test goroutine.
func waitSustained(t *testing.T, what string, dur time.Duration, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	holdStart := time.Time{}
	for time.Now().Before(deadline) {
		if pred() {
			if holdStart.IsZero() {
				holdStart = time.Now()
			} else if time.Since(holdStart) >= dur {
				return
			}
		} else {
			holdStart = time.Time{}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s to hold for %v", what, dur)
}

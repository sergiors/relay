package testutil

import (
	"testing"
	"time"
)

// WaitFor polls pred until it returns true or the deadline passes. It is a
// bounded poll: a generous budget avoids spurious CI flakes while the loop
// never spins forever. On timeout it fails the test with what as context.
//
// Callers must invoke it from the test goroutine: t.Fatalf is not
// goroutine-safe.
func WaitFor(t *testing.T, timeout time.Duration, what string, pred func() bool) {
	t.Helper()
	deadline := time.After(timeout)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		if pred() {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		case <-tick.C:
		}
	}
}

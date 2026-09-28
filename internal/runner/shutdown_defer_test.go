package runner

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// waitCleanupAttemptDone blocks until the async image-cleanup attempt signals
// its terminal classification. Its timeout is a deadlock failure bound only: on
// success the signal is delivered by the cleanup goroutine itself, never by a
// delay.
func waitCleanupAttemptDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("async image cleanup attempt never reached its terminal classification")
	}
}

// TestRetireImageShutdownDeferredNotRetried pins that a runner async cleanup
// which runs AFTER the manager began shutting down is a terminal deferral, not a
// retryable skip: the manager refuses the reference check with
// ErrManagerShuttingDown, the runner logs a debug deferral, never a Warn, and
// never reschedules another attempt against the closing manager.
func TestRetireImageShutdownDeferredNotRetried(t *testing.T) {
	exec := &blockingExecutor{removeErrIsShutdown: true}

	logger, buf := debugBufferLogger()
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, logger, nil)

	// Deterministic gate: the async attempt signals when it has reached its
	// terminal classification (the signal is deferred until after the
	// classification log), so the test never sleeps to observe a
	// (non-)reschedule. A rescheduled retry would only be reached through
	// skipAndRetryImageCleanup, which logs its skip synchronously before
	// scheduling, so the absence of that line is an exact proof no retry was
	// scheduled — no timer wait required.
	attemptDone := make(chan struct{})
	var once sync.Once
	r.imageCleanupAttemptDone = func(string) { once.Do(func() { close(attemptDone) }) }

	r.RetireImage("relay-fn-a:old")
	waitCleanupAttemptDone(t, attemptDone)

	if got := exec.removedImages(); len(got) != 0 {
		t.Fatalf("removed despite shutdown: %v, want none", got)
	}
	logged := buf.String()
	if strings.Contains(logged, "remove retired failed") {
		t.Fatalf("a shutdown deferral must not log a removal Warn, got:\n%s", logged)
	}
	if strings.Contains(logged, "image still in use; skipping") {
		t.Fatalf("a shutdown deferral must not schedule a retry, got:\n%s", logged)
	}
	if strings.Contains(logged, "deferring to a later cleanup pass") {
		t.Fatalf("a shutdown deferral must be terminal, not retried, got:\n%s", logged)
	}
	if n := strings.Count(logged, "manager shutting down; deferring to next boot"); n != 1 {
		t.Fatalf("expected exactly one shutdown deferral, got %d:\n%s", n, logged)
	}
}

// TestRetireImageRemovalShutdownDeferredNotRetried pins that a manager which
// begins shutting down BETWEEN the reference check and the removal classifies
// the removal's ErrManagerShuttingDown as a terminal debug deferral, never a
// Warn and never a rescheduled retry.
func TestRetireImageRemovalShutdownDeferredNotRetried(t *testing.T) {
	// The reference check succeeds (empty referenced map); only RemoveImage
	// reports shutdown, so the classification under test is the removal branch.
	exec := &blockingExecutor{removeErrShutdownOnly: true, referenced: map[string]bool{}}

	logger, buf := debugBufferLogger()
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, logger, nil)

	attemptDone := make(chan struct{})
	var once sync.Once
	r.imageCleanupAttemptDone = func(string) { once.Do(func() { close(attemptDone) }) }

	r.RetireImage("relay-fn-a:old")
	waitCleanupAttemptDone(t, attemptDone)

	if got := exec.removedImages(); len(got) != 0 {
		t.Fatalf("removed despite shutdown: %v, want none", got)
	}
	logged := buf.String()
	if strings.Contains(logged, "remove retired failed") {
		t.Fatalf("a shutdown deferral must not log a removal Warn, got:\n%s", logged)
	}
	if strings.Contains(logged, "image still in use; skipping") {
		t.Fatalf("a shutdown deferral must not schedule a retry, got:\n%s", logged)
	}
	if strings.Contains(logged, "deferring to a later cleanup pass") {
		t.Fatalf("a shutdown deferral must be terminal, not retried, got:\n%s", logged)
	}
	if n := strings.Count(logged, "manager shutting down; deferring to next boot"); n != 1 {
		t.Fatalf("expected exactly one shutdown deferral, got %d:\n%s", n, logged)
	}
}

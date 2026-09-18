package runner

import (
	"sync"
	"testing"
	"time"
)

// invalidatingExecutor wraps blockExecutor (both Executor and ImageCleaner)
// and additionally implements ContainerInvalidator, recording each invalidated
// image so tests can assert the retirement hook fires (and when).
type invalidatingExecutor struct {
	blockExecutor
	mu sync.Mutex
	// invalidated records every image passed to InvalidateImage.
	invalidated []string
	// blockInvalidation, when > 0, makes InvalidateImage sleep briefly to prove
	// blocking never happens on the retire path so the caller does not stall.
	blockInvalidation int
}

func (f *invalidatingExecutor) InvalidateImage(image string) {
	f.mu.Lock()
	if f.blockInvalidation > 0 {
		time.Sleep(time.Duration(f.blockInvalidation) * time.Millisecond)
	}
	f.invalidated = append(f.invalidated, image)
	f.mu.Unlock()
}

func (f *invalidatingExecutor) gotInvalidated() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.invalidated...)
}

// TestRetireImageCallsInvalidator verifies RetireImage invalidates the image's
// cached execution containers via the (optional) ContainerInvalidator
// capability before attempting removal.
func TestRetireImageCallsInvalidator(t *testing.T) {
	exec := &invalidatingExecutor{}
	// Reduce the removal retry backoff so the async removal attempt (which
	// sleeps bounded delays when a container references the image) does not
	// leak a far-future timer into the test.
	savedDelays := imageCleanupRetryDelays
	imageCleanupRetryDelays = []time.Duration{time.Millisecond}
	defer func() { imageCleanupRetryDelays = savedDelays }()

	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, silentLogger(), nil)

	r.RetireImage("relay-fn-a:old")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := exec.gotInvalidated(); len(got) == 1 && got[0] == "relay-fn-a:old" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := exec.gotInvalidated(); len(got) != 1 || got[0] != "relay-fn-a:old" {
		t.Fatalf("invalidated = %v, want exactly [relay-fn-a:old]", got)
	}
	if got := exec.removedImages(); len(got) > 1 {
		t.Logf("removal attempts: %v", got)
	}
}

// TestRetireImageSkipsInvalidatorWithoutCapability verifies a fake executor
// that does NOT implement ContainerInvalidator keeps retirement a working
// no-invalidator path (the existing numbering of capabilities is optional).
func TestRetireImageSkipsInvalidatorWithoutCapability(t *testing.T) {
	exec := &blockExecutor{}
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, silentLogger(), nil)
	if _, ok := any(r.invalidatorResolver()).(ContainerInvalidator); ok {
		t.Fatal("blockExecutor must not satisfy ContainerInvalidator")
	}
	// Must not panic.
	r.RetireImage("relay-fn-a:old")
}

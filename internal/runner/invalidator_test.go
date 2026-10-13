package runner

import (
	"sync"
	"testing"
	"time"

	"relay/internal/testutil"
)

// invalidatingExecutor wraps blockingExecutor (both Executor and ImageCleaner)
// and additionally implements ContainerInvalidator, recording each invalidated
// image so tests can assert the retirement hook fires (and when).
type invalidatingExecutor struct {
	blockingExecutor
	mu sync.Mutex
	// invalidated records every image passed to InvalidateImage.
	invalidated []string
}

func (f *invalidatingExecutor) InvalidateImage(image string) {
	f.mu.Lock()
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
	r := newRetirementRunner(t, exec, testutil.DiscardLogger())
	// Reduce the removal retry backoff so the async removal attempt (which
	// retries bounded delays when a container references the image) does not
	// leak a far-future timer into the test.
	r.imageCleanupRetryDelays = []time.Duration{time.Millisecond}

	r.RetireImage(retiredImage)
	waitFor(t, func() bool {
		got := exec.gotInvalidated()
		return len(got) == 1 && got[0] == retiredImage
	})
	if got := exec.gotInvalidated(); len(got) != 1 || got[0] != retiredImage {
		t.Fatalf("invalidated = %v, want exactly [%s]", got, retiredImage)
	}
}

// TestRetireImageWithoutInvalidatorCapabilityStillWorks verifies a fake executor
// that does NOT implement ContainerInvalidator keeps retirement a working
// no-invalidator path (the capability is optional).
func TestRetireImageWithoutInvalidatorCapabilityStillWorks(t *testing.T) {
	exec := &blockingExecutor{}
	r := newRetirementRunner(t, exec, testutil.DiscardLogger())
	if _, ok := any(r.invalidatorResolver()).(ContainerInvalidator); ok {
		t.Fatal("blockingExecutor must not satisfy ContainerInvalidator")
	}
	// Must not panic; the image is still removed through the cleaner path.
	r.RetireImage(retiredImage)
	waitFor(t, func() bool { return len(exec.removedImages()) == 1 })
}

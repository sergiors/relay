package runner

import (
	"context"
	"strings"
	"testing"
	"time"

	"relay/internal/testutil"
)

// ImageInUse is true while Handle is executing against an image and false as
// soon as it returns, so the runner can gate retirement on live executions.
func TestHandleCountsImageRefs(t *testing.T) {
	exec := newBlockingExecutor(make(chan struct{}))
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, testutil.DiscardLogger(), nil)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.Handle(context.Background(), "m", map[string]any{"status": "ok"})
	}()
	exec.waitEntered() // Execute is now holding ref "relay-fn-a:old"

	if !r.ImageInUse("relay-fn-a:old") {
		t.Fatal("expected image in use while Execute is running")
	}
	if r.ImageInUse("relay-fn-a:other") {
		t.Fatal("unrelated image must not be considered in use")
	}

	close(exec.release) // unblock Execute
	<-done

	if r.ImageInUse("relay-fn-a:old") {
		t.Fatal("expected image not in use after Handle returns")
	}
}

// A panicking executor must still release the image reference, otherwise the
// image stays "in use" forever and can never be retired. With the runner's
// per-invocation panic boundary, Handle returns an error (wrapping the panic)
// instead of propagating it, so the test calls Handle directly and asserts both
// the error and the refcount release.
func TestImageRefsReleasedOnExecutorPanic(t *testing.T) {
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", panicExecutor{})}, testutil.DiscardLogger(), nil)

	err := r.Handle(context.Background(), "m", map[string]any{"status": "ok"})
	if err == nil {
		t.Fatal("expected Handle to return an error wrapping the executor panic")
	}
	if !strings.Contains(err.Error(), "executor panic") {
		t.Fatalf("error = %v, want it to mention the executor panic", err)
	}

	if r.ImageInUse("relay-fn-a:old") {
		t.Fatal("expected image not in use after executor panic")
	}
}

// A retired image that is not being executed is removed immediately (via the
// cleaner), exactly once.
func TestRetiredImageRemovedWhenNotInUse(t *testing.T) {
	exec := &blockingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, testutil.DiscardLogger(), nil)

	r.RetireImage("relay-fn-a:old")
	waitFor(t, func() bool { return len(exec.removedImages()) == 1 })

	got := exec.removedImages()
	if len(got) != 1 || got[0] != "relay-fn-a:old" {
		t.Fatalf("removed = %v, want [relay-fn-a:old]", got)
	}
}

// A retired image that is currently being executed is NOT removed; once the
// in-flight execution releases it, it is removed asynchronously.
func TestRetiredImageNotRemovedWhileInUse(t *testing.T) {
	exec := newBlockingExecutor(make(chan struct{}))
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, testutil.DiscardLogger(), nil)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.Handle(context.Background(), "m", map[string]any{"status": "ok"})
	}()
	exec.waitEntered()

	// Retire while the execution holds the image: nothing is removed yet.
	r.RetireImage("relay-fn-a:old")
	if len(exec.removedImages()) != 0 {
		t.Fatalf("removed while in use: %v, want none", exec.removedImages())
	}

	// Unblock the execution; on release the retired image becomes idle and is
	// removed async.
	close(exec.release)
	<-done
	waitFor(t, func() bool { return len(exec.removedImages()) == 1 })
	if got := exec.removedImages(); len(got) != 1 || got[0] != "relay-fn-a:old" {
		t.Fatalf("removed after idle = %v, want [relay-fn-a:old]", got)
	}
}

// Function removal retires every recorded version of the function's images.
func TestFunctionRemovalRetiresAllVersions(t *testing.T) {
	exec := &blockingExecutor{tags: map[string][]string{"a": {"relay-fn-a:aaaa", "relay-fn-a:bbbb"}}}
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:aaaa", exec)}, testutil.DiscardLogger(), nil)

	r.RemoveFunctionImages("a")
	waitFor(t, func() bool { return len(exec.removedImages()) == 2 })

	got := exec.removedImages()
	if len(got) != 2 {
		t.Fatalf("removed = %v, want 2 versions", got)
	}
}

// A failing image removal must not break subsequent invocation handling: the
// removal is logged and skipped, and Handle still runs normally.
func TestImageCleanupFailureDoesNotFailInvocationHandling(t *testing.T) {
	exec := &blockingExecutor{removeErr: true, tags: map[string][]string{}}
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, testutil.DiscardLogger(), nil)

	// Retiring with a failing cleaner must not panic; the removal is logged and
	// skipped.
	r.RetireImage("relay-fn-a:old")

	// Processing continues normally.
	if err := r.Handle(context.Background(), "m", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
}

// A nil cleaner (fake executors that do not implement ImageCleaner) makes
// retirement a safe no-op: no panic, and processing is unaffected.
func TestRetirementNilCleanerSafe(t *testing.T) {
	r := NewWithMetrics([]*PreparedFunction{newFn(t, "a")}, testutil.DiscardLogger(), nil)

	r.RetireImage("relay-fn-a:old") // no cleaner -> no-op, no panic
	r.RemoveFunctionImages("a")     // resolver nil -> no-op, no panic
	if err := r.Handle(context.Background(), "m", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
}

// A retired image that a relay-owned container still references is NOT removed;
// the removal is skipped at debug level (Image in use), and no "remove retired"
// Warn is emitted. This is the runner-level pin for "never remove an image a
// running service container depends on".
func TestRetireImageSkippedWhileRelayContainerReferences(t *testing.T) {
	exec := &blockingExecutor{referenced: map[string]bool{"relay-fn-a:old": true}}

	logger, buf := debugBufferLogger()
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, logger, nil)
	// The reference never clears, so the retry chain must terminate promptly (a
	// short backoff bounds its lifetime and prevents a test-leaked goroutine).
	r.imageCleanupRetryDelays = []time.Duration{5 * time.Millisecond, 5 * time.Millisecond}

	r.RetireImage("relay-fn-a:old")

	// Wait for the first skip to be logged (the guard fired); the image must not
	// have been removed.
	waitFor(t, func() bool { return strings.Contains(buf.String(), "Image cleanup: image still in use; skipping") })
	if got := exec.removedImages(); len(got) != 0 {
		t.Fatalf("removed while a relay container references the image: %v, want none", got)
	}
	logged := buf.String()
	if strings.Contains(logged, "remove retired") {
		t.Fatalf("expected no 'remove retired' Warn while referenced, got:\n%s", logged)
	}
}

// A retired image that was referenced by a container but whose reference clears
// after a delay is eventually removed exactly once by the retry loop.
func TestRetireImageRemovedAfterContainerReferenceClears(t *testing.T) {
	exec := &blockingExecutor{referenced: map[string]bool{"relay-fn-a:old": true}}

	logger, buf := debugBufferLogger()
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, logger, nil)
	// Shrink the retry backoff so the test observes retries quickly.
	r.imageCleanupRetryDelays = []time.Duration{50 * time.Millisecond, 50 * time.Millisecond}

	// The image is referenced initially, so the first attempt skips.
	r.RetireImage("relay-fn-a:old")

	// Wait until the first guard-skip fired, then clear the reference: the retry
	// loop now removes the image exactly once.
	waitFor(t, func() bool { return strings.Contains(buf.String(), "Image cleanup: image still in use; skipping") })
	exec.setReferenced("relay-fn-a:old", false)

	waitFor(t, func() bool { return len(exec.removedImages()) == 1 })
	got := exec.removedImages()
	if len(got) != 1 || got[0] != "relay-fn-a:old" {
		t.Fatalf("removed = %v, want exactly [relay-fn-a:old]", got)
	}
	// No deferral Info should be logged (the removal eventually succeeded).
	if strings.Contains(buf.String(), "deferring to a later cleanup pass") {
		t.Fatalf("unexpected deferral log on a successful retry:\n%s", buf.String())
	}
}

// When a relay-owned container references an image forever, the bounded retry
// loop gives up cleanly: exactly ONE final Info defers the image to a later
// cleanup pass, nothing is removed, and no repeated Warns are emitted.
func TestRetireImageRetryGivesUpCleanly(t *testing.T) {
	exec := &blockingExecutor{referenced: map[string]bool{"relay-fn-a:old": true}}

	logger, buf := debugBufferLogger()
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, logger, nil)
	r.imageCleanupRetryDelays = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}

	r.RetireImage("relay-fn-a:old")

	waitFor(t, func() bool {
		return strings.Count(buf.String(), "deferring to a later cleanup pass") == 1
	})
	if got := exec.removedImages(); len(got) != 0 {
		t.Fatalf("removed referenced image: %v, want none", got)
	}
	logged := buf.String()
	if strings.Count(logged, "deferring to a later cleanup pass") != 1 {
		t.Fatalf("exactly one deferral Info expected, got:\n%s", logged)
	}
	if strings.Contains(logged, "remove retired") {
		t.Fatalf("expected no 'remove retired' Warn during a guard-skip chain, got:\n%s", logged)
	}
}

// A partial service reconcile failure — a RemoveImage that errors while a
// relay-owned container still references the image — must NOT delete the image.
// The runner classifies it as a guard-skip (debug, retried), never a Warn, and a
// later retry once the reference clears succeeds.
func TestRetireImagePartialServiceFailureDoesNotDelete(t *testing.T) {
	// The daemon refuses the first removal while a container simultaneously
	// references the image (the remove/image race). The runner's re-consult
	// classifies the failure as a guarded skip (debug, retried), NOT a genuine
	// Warn, and the image is never deleted.
	exec := &blockingExecutor{referenced: map[string]bool{}, removeErrCountdown: 1}

	logger, buf := debugBufferLogger()
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, logger, nil)
	r.imageCleanupRetryDelays = []time.Duration{50 * time.Millisecond, 50 * time.Millisecond}

	r.RetireImage("relay-fn-a:old")

	// The first RemoveImage fails AND a container reference appears (the race),
	// so the runner must classify it as a guarded skip — no removal, no
	// "remove retired" Warn.
	waitFor(t, func() bool { return strings.Contains(buf.String(), "Image cleanup: image still in use; skipping") })
	if got := exec.removedImages(); len(got) != 0 {
		t.Fatalf("removed while a container still referenced the image: %v, want none", got)
	}
	if strings.Contains(buf.String(), "remove retired") {
		t.Fatalf("expected no 'remove retired' Warn for an in-use removal failure, got:\n%s", buf.String())
	}

	// The container is eventually replaced (reference clears); the retry loop now
	// succeeds exactly once.
	exec.setReferenced("relay-fn-a:old", false)

	waitFor(t, func() bool { return len(exec.removedImages()) == 1 })
	got := exec.removedImages()
	if len(got) != 1 || got[0] != "relay-fn-a:old" {
		t.Fatalf("removed after reference cleared = %v, want exactly [relay-fn-a:old]", got)
	}
}

// A successful function-image removal fires dependency GC exactly once, off the
// event path, after the removal succeeded: the function image that referenced
// its dependency is gone, so the now-possibly-orphaned dependency layer can be
// pruned.
func TestSuccessfulRemovalRunsDependencyGC(t *testing.T) {
	exec := &blockingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, testutil.DiscardLogger(), nil)

	r.RetireImage("relay-fn-a:old")
	waitFor(t, func() bool { return len(exec.removedImages()) == 1 })

	// GC must have fired exactly once, after the successful removal.
	waitFor(t, func() bool { return exec.gcCallCount() == 1 })
	if got := exec.removedImages(); len(got) != 1 || got[0] != "relay-fn-a:old" {
		t.Fatalf("removed = %v, want [relay-fn-a:old]", got)
	}
}

// A removal that is skipped (a relay-owned container references the image
// forever) never fires dependency GC: the function image was NOT removed, so its
// dependency is still referenced and pruning would be pointless. This pins "no
// GC before/during a removal that did not succeed".
func TestSkippedRemovalRunsNoDependencyGC(t *testing.T) {
	exec := &blockingExecutor{referenced: map[string]bool{"relay-fn-a:old": true}}

	logger, buf := debugBufferLogger()
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, logger, nil)
	r.imageCleanupRetryDelays = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}

	r.RetireImage("relay-fn-a:old")

	// Wait for the retry loop to give up (exactly one deferral Info); the image
	// was never removed, so GC must never have run.
	waitFor(t, func() bool {
		return strings.Count(buf.String(), "deferring to a later cleanup pass") == 1
	})
	if got := exec.removedImages(); len(got) != 0 {
		t.Fatalf("removed a referenced image: %v, want none", got)
	}
	if n := exec.gcCallCount(); n != 0 {
		t.Fatalf("dependency GC ran %d times on an unremoved image, want 0 (no GC before removal succeeds)", n)
	}
}

// A dependency-GC failure is logged at Warn but must NOT change the outcome of
// the function-image removal that preceded it: the removal already succeeded, so
// a genuine GC failure only means the dependency layer is pruned on the next
// natural lifecycle point.
func TestDependencyGCFailureDoesNotAffectRemoval(t *testing.T) {
	exec := &blockingExecutor{gcErr: true}
	logger, buf := debugBufferLogger()
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, logger, nil)

	r.RetireImage("relay-fn-a:old")
	waitFor(t, func() bool { return len(exec.removedImages()) == 1 })

	// GC was attempted and failed: the failure is logged as a Warn ("Dependency
	// image cleanup failed"), and the removal outcome is unchanged (the image was
	// removed).
	waitFor(t, func() bool { return exec.gcCallCount() == 1 })
	if !strings.Contains(buf.String(), "Dependency image cleanup failed") {
		t.Fatalf("expected a Warn logging the dependency-GC failure, got:\n%s", buf.String())
	}
	if got := exec.removedImages(); len(got) != 1 || got[0] != "relay-fn-a:old" {
		t.Fatalf("removed = %v, want [relay-fn-a:old] despite the GC failure", got)
	}
}

// A reference-check error is unknown state: the runner must conservatively treat
// the image as referenced, skip removal, and schedule a retry — never remove on
// unknown state. A final deferral Info is emitted once the bounded retries
// exhaust.
func TestRetireImageReferenceCheckErrorRetriesConservatively(t *testing.T) {
	exec := &blockingExecutor{
		referenced:        map[string]bool{},
		referenceCheckErr: errRemoveBoom,
	}
	logger, buf := debugBufferLogger()
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, logger, nil)
	r.imageCleanupRetryDelays = []time.Duration{time.Millisecond, time.Millisecond}

	r.RetireImage("relay-fn-a:old")

	waitFor(t, func() bool {
		return strings.Count(buf.String(), "deferring to a later cleanup pass") == 1
	})
	if got := exec.removedImages(); len(got) != 0 {
		t.Fatalf("removed on an unknown reference state: %v, want none (conservative skip)", got)
	}
	if !strings.Contains(buf.String(), "reference check failed") {
		t.Fatalf("expected the skip reason to name the failed reference check, got:\n%s", buf.String())
	}
}

// When removal fails AND the re-consult of the reference check also errors, the
// runner cannot classify the failure as a guard-skip, so it logs the genuine
// Warn instead of silently retrying.
func TestRetireImageRemovalFailureWithReconsultErrorWarns(t *testing.T) {
	exec := &blockingExecutor{
		removeErr:              true,
		referenced:             map[string]bool{},
		referenceCheckErr:      errRemoveBoom,
		referenceCheckErrAfter: 1,
	}
	logger, buf := debugBufferLogger()
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, logger, nil)

	r.RetireImage("relay-fn-a:old")

	waitFor(t, func() bool {
		return strings.Contains(buf.String(), "Image cleanup: remove retired failed")
	})
	if got := exec.removedImages(); len(got) != 0 {
		t.Fatalf("removed despite a failed removal: %v, want none", got)
	}
}

// Retiring an image that is then re-acquired before the in-flight execution
// releases it aborts the pending removal: acquire clears the retired mark, so no
// release fires the idle hook and the image is never handed to the cleaner.
func TestRetireImageReacquireAbortsPendingRemoval(t *testing.T) {
	exec := newBlockingExecutor(make(chan struct{}))
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, testutil.DiscardLogger(), nil)

	// First invocation holds the image.
	first := make(chan struct{})
	go func() {
		defer close(first)
		_ = r.Handle(context.Background(), "m-1", map[string]any{"status": "ok"})
	}()
	exec.waitEntered()

	// Retire while the image is in use: the removal is pending on release and
	// nothing is removed yet.
	r.RetireImage("relay-fn-a:old")
	if got := exec.removedImages(); len(got) != 0 {
		t.Fatalf("removed while in use: %v, want none", got)
	}

	// A second invocation re-acquires the image before any release, clearing the
	// retired mark.
	second := make(chan struct{})
	go func() {
		defer close(second)
		_ = r.Handle(context.Background(), "m-2", map[string]any{"status": "ok"})
	}()
	waitFor(t, func() bool { return exec.callCount() == 2 })

	// Release both; because the retired mark was cleared by the re-acquire, the
	// idle transition never fires removal.
	close(exec.release)
	<-first
	<-second

	if got := exec.removedImages(); len(got) != 0 {
		t.Fatalf("re-acquired image must not be removed, got %v", got)
	}
}

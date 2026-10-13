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
	r := NewWithMetrics([]*PreparedApp{fpClean(t, "a", "relay-app-a:old", exec)}, testutil.DiscardLogger(), nil)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.Handle(context.Background(), "m", map[string]any{"status": "ok"})
	}()
	exec.waitEntered() // Execute is now holding ref "relay-app-a:old"

	if !r.ImageInUse("relay-app-a:old") {
		t.Fatal("expected image in use while Execute is running")
	}
	if r.ImageInUse("relay-app-a:other") {
		t.Fatal("unrelated image must not be considered in use")
	}

	close(exec.release) // unblock Execute
	<-done

	if r.ImageInUse("relay-app-a:old") {
		t.Fatal("expected image not in use after Handle returns")
	}
}

// A panicking executor must still release the image reference, otherwise the
// image stays "in use" forever and can never be retired. With the runner's
// per-invocation panic boundary, Handle returns an error (wrapping the panic)
// instead of propagating it, so the test calls Handle directly and asserts both
// the error and the refcount release.
func TestImageRefsReleasedOnExecutorPanic(t *testing.T) {
	r := NewWithMetrics([]*PreparedApp{fpClean(t, "a", "relay-app-a:old", panicExecutor{})}, testutil.DiscardLogger(), nil)

	err := r.Handle(context.Background(), "m", map[string]any{"status": "ok"})
	if err == nil {
		t.Fatal("expected Handle to return an error wrapping the executor panic")
	}
	if !strings.Contains(err.Error(), "executor panic") {
		t.Fatalf("error = %v, want it to mention the executor panic", err)
	}

	if r.ImageInUse("relay-app-a:old") {
		t.Fatal("expected image not in use after executor panic")
	}
}

// A retired image that is not being executed is removed immediately (via the
// cleaner), exactly once.
func TestRetiredImageRemovedWhenNotInUse(t *testing.T) {
	exec := &blockingExecutor{}
	r := newRetirementRunner(t, exec, testutil.DiscardLogger())

	r.RetireImage(retiredImage)
	waitFor(t, func() bool { return len(exec.removedImages()) == 1 })

	got := exec.removedImages()
	if len(got) != 1 || got[0] != retiredImage {
		t.Fatalf("removed = %v, want [%s]", got, retiredImage)
	}
}

// A retired image that is currently being executed is NOT removed; once the
// in-flight execution releases it, it is removed asynchronously.
func TestRetiredImageNotRemovedWhileInUse(t *testing.T) {
	exec := &blockingExecutor{}
	r := newRetirementRunner(t, exec, testutil.DiscardLogger())

	// A stale pre-swap snapshot holds the superseded image.
	holdImage(r, retiredImage)

	// Retire while the execution holds the image: nothing is removed yet.
	r.RetireImage(retiredImage)
	if got := exec.removedImages(); len(got) != 0 {
		t.Fatalf("removed while in use: %v, want none", got)
	}

	// Release: the retired image becomes idle and is removed async.
	releaseImage(r, retiredImage)
	waitFor(t, func() bool { return len(exec.removedImages()) == 1 })
	if got := exec.removedImages(); len(got) != 1 || got[0] != retiredImage {
		t.Fatalf("removed after idle = %v, want [%s]", got, retiredImage)
	}
}

// App removal retires every recorded version of the app's images.
func TestAppRemovalRetiresAllVersions(t *testing.T) {
	exec := &blockingExecutor{tags: map[string][]string{"a": {"relay-app-a:aaaa", "relay-app-a:bbbb"}}}
	r := newRetirementRunner(t, exec, testutil.DiscardLogger())

	r.RemoveAppImages("a")
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
	r := newRetirementRunner(t, exec, testutil.DiscardLogger())

	// Retiring with a failing cleaner must not panic; the removal is logged and
	// skipped (and its retirement is retained for a later re-drive).
	r.RetireImage(retiredImage)

	// Processing continues normally.
	if err := r.Handle(context.Background(), "m", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
}

// A nil cleaner (fake executors that do not implement ImageCleaner) makes
// retirement a safe no-op: no panic, and processing is unaffected.
func TestRetirementNilCleanerSafe(t *testing.T) {
	r := NewWithMetrics([]*PreparedApp{newFn(t, "a")}, testutil.DiscardLogger(), nil)

	r.RetireImage(retiredImage) // no cleaner -> no-op, no panic
	r.RemoveAppImages("a")      // resolver nil -> no-op, no panic
	if err := r.Handle(context.Background(), "m", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
}

// A retired image that a relay-owned container still references is NOT removed;
// the removal is skipped at debug level (Image in use), and no "remove retired"
// Warn is emitted. This is the runner-level pin for "never remove an image a
// running service container depends on".
func TestRetireImageSkippedWhileRelayContainerReferences(t *testing.T) {
	exec := &blockingExecutor{referenced: map[string]bool{retiredImage: true}}

	logger, buf := debugBufferLogger()
	r := newRetirementRunner(t, exec, logger)
	// The reference never clears, so the retry chain must terminate promptly (a
	// short backoff bounds its lifetime and prevents a test-leaked goroutine).
	r.imageCleanupRetryDelays = []time.Duration{5 * time.Millisecond, 5 * time.Millisecond}

	r.RetireImage(retiredImage)

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
	exec := &blockingExecutor{referenced: map[string]bool{retiredImage: true}}

	logger, buf := debugBufferLogger()
	r := newRetirementRunner(t, exec, logger)
	// Shrink the retry backoff so the test observes retries quickly.
	r.imageCleanupRetryDelays = []time.Duration{50 * time.Millisecond, 50 * time.Millisecond}

	// The image is referenced initially, so the first attempt skips.
	r.RetireImage(retiredImage)

	// Wait until the first guard-skip fired, then clear the reference: the retry
	// loop now removes the image exactly once.
	waitFor(t, func() bool { return strings.Contains(buf.String(), "Image cleanup: image still in use; skipping") })
	exec.setReferenced(retiredImage, false)

	waitFor(t, func() bool { return len(exec.removedImages()) == 1 })
	got := exec.removedImages()
	if len(got) != 1 || got[0] != retiredImage {
		t.Fatalf("removed = %v, want exactly [%s]", got, retiredImage)
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
	exec := &blockingExecutor{referenced: map[string]bool{retiredImage: true}}

	logger, buf := debugBufferLogger()
	r := newRetirementRunner(t, exec, logger)
	r.imageCleanupRetryDelays = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}

	r.RetireImage(retiredImage)

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
	// The deferral retains ownership: nothing else would remove the layer after
	// the initial retry window.
	if got := r.refs.deferredImages(); len(got) != 1 || got[0] != retiredImage {
		t.Fatalf("deferred images = %v, want [%s]", got, retiredImage)
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
	r := newRetirementRunner(t, exec, logger)
	r.imageCleanupRetryDelays = []time.Duration{50 * time.Millisecond, 50 * time.Millisecond}

	r.RetireImage(retiredImage)

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
	exec.setReferenced(retiredImage, false)

	waitFor(t, func() bool { return len(exec.removedImages()) == 1 })
	got := exec.removedImages()
	if len(got) != 1 || got[0] != retiredImage {
		t.Fatalf("removed after reference cleared = %v, want exactly [%s]", got, retiredImage)
	}
}

// A successful app-image removal fires dependency GC exactly once, off the
// event path, after the removal succeeded: the app image that referenced
// its dependency is gone, so the now-possibly-orphaned dependency layer can be
// pruned.
func TestSuccessfulRemovalRunsDependencyGC(t *testing.T) {
	exec := &blockingExecutor{}
	r := newRetirementRunner(t, exec, testutil.DiscardLogger())

	r.RetireImage(retiredImage)
	waitFor(t, func() bool { return len(exec.removedImages()) == 1 })

	// GC must have fired exactly once, after the successful removal.
	waitFor(t, func() bool { return exec.gcCallCount() == 1 })
	if got := exec.removedImages(); len(got) != 1 || got[0] != retiredImage {
		t.Fatalf("removed = %v, want [%s]", got, retiredImage)
	}
}

// A removal that is skipped (a relay-owned container references the image
// forever) never fires dependency GC: the app image was NOT removed, so its
// dependency is still referenced and pruning would be pointless. This pins "no
// GC before/during a removal that did not succeed".
func TestSkippedRemovalRunsNoDependencyGC(t *testing.T) {
	exec := &blockingExecutor{referenced: map[string]bool{retiredImage: true}}

	logger, buf := debugBufferLogger()
	r := newRetirementRunner(t, exec, logger)
	r.imageCleanupRetryDelays = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}

	r.RetireImage(retiredImage)

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
// the app-image removal that preceded it: the removal already succeeded, so
// a genuine GC failure only means the dependency layer is pruned on the next
// natural lifecycle point.
func TestDependencyGCFailureDoesNotAffectRemoval(t *testing.T) {
	exec := &blockingExecutor{gcErr: true}
	logger, buf := debugBufferLogger()
	r := newRetirementRunner(t, exec, logger)

	r.RetireImage(retiredImage)
	waitFor(t, func() bool { return len(exec.removedImages()) == 1 })

	// GC was attempted and failed: the failure is logged as a Warn ("Dependency
	// image cleanup failed"), and the removal outcome is unchanged (the image was
	// removed).
	waitFor(t, func() bool { return exec.gcCallCount() == 1 })
	if !strings.Contains(buf.String(), "Dependency image cleanup failed") {
		t.Fatalf("expected a Warn logging the dependency-GC failure, got:\n%s", buf.String())
	}
	if got := exec.removedImages(); len(got) != 1 || got[0] != retiredImage {
		t.Fatalf("removed = %v, want [%s] despite the GC failure", got, retiredImage)
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
	r := newRetirementRunner(t, exec, logger)
	r.imageCleanupRetryDelays = []time.Duration{time.Millisecond, time.Millisecond}

	r.RetireImage(retiredImage)

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
	r := newRetirementRunner(t, exec, logger)

	r.RetireImage(retiredImage)

	waitFor(t, func() bool {
		return strings.Contains(buf.String(), "Image cleanup: remove retired failed")
	})
	if got := exec.removedImages(); len(got) != 0 {
		t.Fatalf("removed despite a failed removal: %v, want none", got)
	}
}

// A stale pre-swap snapshot that claims the superseded image must NOT cancel its
// retirement: the registry no longer serves that image, so it is still garbage,
// and the last idle transition (when the stale use releases) re-drives removal.
// This is the runner-level pin for "retirement intent survives an admitted stale
// use".
func TestRetireImageSurvivesStaleReacquire(t *testing.T) {
	exec := &blockingExecutor{}
	r := newRetirementRunner(t, exec, testutil.DiscardLogger())
	w := newAttemptWatcher(r, 1)

	// Two overlapping stale uses hold the superseded image.
	holdImage(r, retiredImage)
	holdImage(r, retiredImage)

	// Retire while they hold it: the removal waits for the idle transition.
	r.RetireImage(retiredImage)
	if got := exec.removedImages(); len(got) != 0 {
		t.Fatalf("removed while in use: %v, want none", got)
	}

	// One use ends: the image is still in use, so no removal is driven at all.
	releaseImage(r, retiredImage)
	if got := w.count(); got != 0 {
		t.Fatalf("removal was driven %d times while a stale use still held the image", got)
	}
	if got := exec.removedImages(); len(got) != 0 {
		t.Fatalf("removed while a stale use still held the image: %v, want none", got)
	}

	// The last use ends: the retirement is still in force, so removal is driven
	// and succeeds exactly once.
	releaseImage(r, retiredImage)
	w.wait(t, 1)
	if got := exec.removedImages(); len(got) != 1 || got[0] != retiredImage {
		t.Fatalf("removed after the last stale use ended = %v, want exactly [%s]", got, retiredImage)
	}
	if st := r.refs.stateOf(retiredImage); st.retired || st.active {
		t.Fatalf("a successful removal must drop every per-image record, state = %+v", st)
	}
	if got := r.refs.deferredImages(); len(got) != 0 {
		t.Fatalf("deferred images after removal = %v, want none", got)
	}
}

// TestRetireImageCancelledForServedImage pins the explicit activation signal: an
// image the registry serves again is CURRENT, so a stale retirement for it is
// cancelled — never removed — and no periodic re-drive keeps retrying it.
func TestRetireImageCancelledForServedImage(t *testing.T) {
	exec := &blockingExecutor{}
	logger, buf := debugBufferLogger()
	// The registry publishes relay-app-a:new, and the test retires exactly the
	// image it serves (the state a re-published/reverted generation produces).
	r := newRetirementRunner(t, exec, logger)

	r.RetireImage(currentImage)
	waitFor(t, func() bool {
		return strings.Contains(buf.String(), "image is served again; cancelling retirement")
	})

	if got := exec.removedImages(); len(got) != 0 {
		t.Fatalf("removed the image the registry serves: %v, want none", got)
	}
	// Every record is dropped: the retirement is void, not deferred.
	if st := r.refs.stateOf(currentImage); st.retired {
		t.Fatal("a served image's retirement must be cleared")
	}
	if got := r.refs.deferredImages(); len(got) != 0 {
		t.Fatalf("served image left deferred: %v, want none", got)
	}
	if st := r.refs.stateOf(currentImage); st.active {
		t.Fatal("the chain admission must be released after cancellation")
	}

	// No churn: a periodic re-drive and a use/release cycle of the current image
	// start no further attempt.
	checks := exec.referenceChecksCount()
	r.RetryDeferredImageRemovals()
	holdImage(r, currentImage)
	releaseImage(r, currentImage)
	r.RetryDeferredImageRemovals()
	if got := exec.referenceChecksCount(); got != checks {
		t.Fatalf("a cancelled retirement ran %d further reference checks, want 0", got-checks)
	}
	if got := exec.removedImages(); len(got) != 0 {
		t.Fatalf("removed a served image on re-drive: %v, want none", got)
	}
}

// TestRetireImageRepublishedAfterDeferralCancelsRetirement pins the
// re-publication case end to end at the runner level: a deferred retirement for
// an image the registry did NOT serve is cancelled once that exact image is
// published again, so the currently served version is never removed and the
// deferred record stops being retried.
func TestRetireImageRepublishedAfterDeferralCancelsRetirement(t *testing.T) {
	exec := &blockingExecutor{referenced: map[string]bool{retiredImage: true}}
	logger, buf := debugBufferLogger()
	r := newRetirementRunner(t, exec, logger)
	r.imageCleanupRetryDelays = nil // one guard-skip defers immediately

	w := newAttemptWatcher(r, 2)
	r.RetireImage(retiredImage)
	w.wait(t, 1)
	if got := r.refs.deferredImages(); len(got) != 1 {
		t.Fatalf("deferred images = %v, want one", got)
	}

	// The generation is reverted/re-prepared: the registry publishes exactly the
	// deferred image again.
	r.reg.Replace("a", fpClean(t, "a", retiredImage, exec))
	if !r.reg.ServesImage(retiredImage) {
		t.Fatal("the registry must serve the re-published image")
	}

	// The periodic re-drive observes the activation and cancels the retirement.
	r.RetryDeferredImageRemovals()
	w.wait(t, 2)
	if got := exec.removedImages(); len(got) != 0 {
		t.Fatalf("removed a re-published image: %v, want none", got)
	}
	if got := r.refs.deferredImages(); len(got) != 0 {
		t.Fatalf("a re-published image must not stay deferred, got %v", got)
	}
	if st := r.refs.stateOf(retiredImage); st.retired {
		t.Fatal("a re-published image must not stay retired")
	}
	if strings.Contains(buf.String(), "remove retired") {
		t.Fatalf("a re-published image must never reach removal, got:\n%s", buf.String())
	}

	// No churn: further re-drives attempt nothing.
	r.RetryDeferredImageRemovals()
	r.RetryDeferredImageRemovals()
	if got := w.count(); got != 2 {
		t.Fatalf("completed attempts = %d, want 2 (the cancel must end the chain)", got)
	}
}

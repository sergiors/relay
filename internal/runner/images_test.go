package runner

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/runtime"
)

// blockExecutor implements both Executor and ImageCleaner: it can block inside
// Execute to hold an image reference while a test drives retirement, records
// every image it removes, reports a fixed set of function image tags, and lets
// tests control whether a container references an image and whether removal
// fails. It is what lets the retirement tests exercise the full
// acquire→retire→release flow without Docker.
type blockExecutor struct {
	mu        sync.Mutex
	removed   []string
	removeErr bool
	// removeErrCountdown, when > 0, makes the next that-many RemoveImage calls
	// fail (decrementing each call). It lets a test model a single transient
	// removal failure before a later retry succeeds.
	removeErrCountdown int
	// referenced reports, per image, whether a relay-owned container currently
	// references it (ImageReferencedByManagedContainer).
	referenced map[string]bool
	// gcCalls counts how many times CleanupUnusedDependencies was invoked, for
	// the dependency-GC-after-removal assertions.
	gcCalls int
	// gcErr, when true, makes CleanupUnusedDependencies return a genuine error
	// so a test can assert the failure is logged but ignores the removal
	// outcome.
	gcErr bool
	// entered is closed (once, under mu) when a blocking Execute begins, so the
	// test can synchronize on it. release is closed to unblock Execute.
	entered     chan struct{}
	release     chan struct{}
	startBlocks bool
	tags        map[string][]string
}

func (f *blockExecutor) Execute(ctx context.Context, prepared *runtime.Prepared, handler string, _ []byte, _ []string) error {
	if f.startBlocks {
		f.mu.Lock()
		select {
		case <-f.entered:
			// already signalled
		default:
			close(f.entered)
		}
		f.mu.Unlock()
		<-f.release
	}
	return nil
}

func (f *blockExecutor) RemoveImage(ctx context.Context, image string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.removeErr {
		return errRemoveBoom
	}
	if f.removeErrCountdown > 0 {
		f.removeErrCountdown--
		// Model the daemon refusing because a relay-owned container references
		// the image mid-removal (the race the guard classifies): the removal
		// fails AND a reference appears, so the runner's re-consult classifies it
		// as a guarded skip rather than a genuine failure.
		if f.referenced == nil {
			f.referenced = map[string]bool{}
		}
		f.referenced[image] = true
		return errRemoveBoom
	}
	f.removed = append(f.removed, image)
	return nil
}

func (f *blockExecutor) ImageReferencedByManagedContainer(_ context.Context, image string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.referenced[image], nil
}

// setReferenced mutates whether a container references image.
func (f *blockExecutor) setReferenced(image string, v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.referenced[image] = v
}

func (f *blockExecutor) FunctionImageTags(ctx context.Context, name string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.tags == nil {
		return nil, nil
	}
	return append([]string(nil), f.tags[name]...), nil
}

func (f *blockExecutor) CleanupUnusedDependencies(_ context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gcCalls++
	if f.gcErr {
		return 0, errRemoveBoom
	}
	return 0, nil
}

// gcCallCount returns how many times CleanupUnusedDependencies has run.
func (f *blockExecutor) gcCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gcCalls
}

func (f *blockExecutor) removedImages() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.removed...)
}

var errRemoveBoom = &removeBoomErr{}

type removeBoomErr struct{}

func (*removeBoomErr) Error() string { return "remove boom" }

// fp is a valid, always-matching prepared function whose executor doubles as an
// ImageCleaner so the runner's resolver finds it.
func fpClean(t *testing.T, name, image string, exec *blockExecutor) *PreparedFunction {
	t.Helper()
	return NewPrepared(
		function.Function{
			Name: name,
			Template: &function.Template{
				Runtime: "node24",
				Rules:   []function.Rule{{Handler: "index.run", Pattern: function.Pattern{}, Timeout: time.Second}},
			},
		},
		&runtime.Prepared{Name: name, Image: image},
		exec,
	)
}

// ImageInUse is true while Handle is executing against an image and false as
// soon as it returns, so the runner can gate retirement on live executions.
func TestHandleCountsImageRefs(t *testing.T) {
	exec := &blockExecutor{startBlocks: true, entered: make(chan struct{}), release: make(chan struct{})}
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, silentLogger(), nil)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.Handle(context.Background(), "m", map[string]any{"status": "ok"})
	}()
	<-exec.entered // Execute is now holding ref "relay-fn-a:old"

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

// panicExecutor panics inside Execute to exercise the release-on-panic path.
type panicExecutor struct{}

func (panicExecutor) Execute(context.Context, *runtime.Prepared, string, []byte, []string) error {
	panic("executor boom")
}

// A panicking executor must still release the image reference, otherwise the
// image stays "in use" forever and can never be retired. With the runner's
// per-invocation panic boundary, Handle now RETURNS an error (wrapping the
// panic) instead of propagating it, so the test calls Handle directly and
// asserts both the error and the refcount release.
func TestImageRefsReleasedOnExecutorPanic(t *testing.T) {
	r := NewWithMetrics([]*PreparedFunction{NewPrepared(
		function.Function{
			Name: "a",
			Template: &function.Template{
				Runtime: "node24",
				Rules:   []function.Rule{{Handler: "index.run", Pattern: function.Pattern{}, Timeout: time.Second}},
			},
		},
		&runtime.Prepared{Name: "a", Image: "relay-fn-a:old"},
		panicExecutor{},
	)}, silentLogger(), nil)

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
	exec := &blockExecutor{startBlocks: false, tags: map[string][]string{}}
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, silentLogger(), nil)

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
	exec := &blockExecutor{startBlocks: true, entered: make(chan struct{}), release: make(chan struct{}), tags: map[string][]string{}}
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, silentLogger(), nil)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.Handle(context.Background(), "m", map[string]any{"status": "ok"})
	}()
	<-exec.entered

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
	exec := &blockExecutor{
		startBlocks: false,
		tags:        map[string][]string{"a": {"relay-fn-a:aaaa", "relay-fn-a:bbbb"}},
	}
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:aaaa", exec)}, silentLogger(), nil)

	r.RemoveFunctionImages("a")
	waitFor(t, func() bool { return len(exec.removedImages()) == 2 })

	got := exec.removedImages()
	if len(got) != 2 {
		t.Fatalf("removed = %v, want 2 versions", got)
	}
}

// A failing image removal must not break subsequent processing or panic.
func TestCleanupFailureDoesNotBreakProcessing(t *testing.T) {
	exec := &blockExecutor{startBlocks: false, removeErr: true, tags: map[string][]string{}}
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, silentLogger(), nil)

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
	r := NewWithMetrics([]*PreparedFunction{newFn(t, "a")}, silentLogger(), nil)

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
	exec := &blockExecutor{
		startBlocks: false,
		tags:        map[string][]string{},
		referenced:  map[string]bool{"relay-fn-a:old": true},
	}
	// The reference never clears, so the retry chain must terminate promptly (a
	// short backoff bounds its lifetime and prevents a test-leaked goroutine).
	orig := imageCleanupRetryDelays
	imageCleanupRetryDelays = []time.Duration{5 * time.Millisecond, 5 * time.Millisecond}
	t.Cleanup(func() { imageCleanupRetryDelays = orig })

	logger, buf := debugBufferLogger()
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, logger, nil)

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
	exec := &blockExecutor{
		startBlocks: false,
		tags:        map[string][]string{},
		referenced:  map[string]bool{"relay-fn-a:old": true},
	}
	// Shrink the retry backoff so the test observes retries quickly.
	orig := imageCleanupRetryDelays
	imageCleanupRetryDelays = []time.Duration{50 * time.Millisecond, 50 * time.Millisecond}
	t.Cleanup(func() { imageCleanupRetryDelays = orig })

	logger, buf := debugBufferLogger()
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, logger, nil)

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
	exec := &blockExecutor{
		startBlocks: false,
		tags:        map[string][]string{},
		referenced:  map[string]bool{"relay-fn-a:old": true},
	}
	orig := imageCleanupRetryDelays
	imageCleanupRetryDelays = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	t.Cleanup(func() { imageCleanupRetryDelays = orig })

	logger, buf := debugBufferLogger()
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, logger, nil)

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
	exec := &blockExecutor{
		startBlocks: false,
		tags:        map[string][]string{},
		referenced:  map[string]bool{},
		// Model a partial service reconcile failure: on the FIRST RemoveImage the
		// daemon refuses the removal while a container simultaneously references
		// the image (the remove/image race). The runner's re-consult classifies the
		// failure as a guarded skip (debug, retried), NOT a genuine Warn, and the
		// image is never deleted.
		removeErrCountdown: 1,
	}
	orig := imageCleanupRetryDelays
	imageCleanupRetryDelays = []time.Duration{50 * time.Millisecond, 50 * time.Millisecond}
	t.Cleanup(func() { imageCleanupRetryDelays = orig })

	logger, buf := debugBufferLogger()
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, logger, nil)

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

// waitFor polls cond until it holds or the timeout elapses, so async removal
// goroutines are observed deterministically.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not reached before timeout")
}

// A successful function-image removal fires dependency GC exactly once, off the
// event path, after the removal succeeded: the function image that referenced
// its dependency is gone, so the now-possibly-orphaned dependency layer can be
// pruned.
func TestSuccessfulRemovalRunsDependencyGC(t *testing.T) {
	exec := &blockExecutor{startBlocks: false, tags: map[string][]string{}}
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, silentLogger(), nil)

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
	exec := &blockExecutor{
		startBlocks: false,
		tags:        map[string][]string{},
		referenced:  map[string]bool{"relay-fn-a:old": true},
	}
	orig := imageCleanupRetryDelays
	imageCleanupRetryDelays = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	t.Cleanup(func() { imageCleanupRetryDelays = orig })

	logger, buf := debugBufferLogger()
	r := NewWithMetrics([]*PreparedFunction{fpClean(t, "a", "relay-fn-a:old", exec)}, logger, nil)

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
	exec := &blockExecutor{startBlocks: false, tags: map[string][]string{}, gcErr: true}
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

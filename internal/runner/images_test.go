package runner

import (
	"context"
	"sync"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/runtime"
)

// blockExecutor implements both Executor and ImageCleaner: it can block inside
// Execute to hold an image reference while a test drives retirement, records
// every image it removes, and reports a fixed set of function image tags. It is
// what lets the retirement tests exercise the full acquire→retire→release flow
// without Docker.
type blockExecutor struct {
	mu        sync.Mutex
	removed   []string
	removeErr bool
	// entered is closed (once, under mu) when a blocking Execute begins, so the
	// test can synchronize on it. release is closed to unblock Execute.
	entered     chan struct{}
	release     chan struct{}
	startBlocks bool
	tags        map[string][]string
}

func (f *blockExecutor) Execute(ctx context.Context, prepared *runtime.Prepared, handler string, _ []byte) error {
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
	f.removed = append(f.removed, image)
	return nil
}

func (f *blockExecutor) FunctionImageTags(ctx context.Context, name string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.tags == nil {
		return nil, nil
	}
	return append([]string(nil), f.tags[name]...), nil
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

func (panicExecutor) Execute(context.Context, *runtime.Prepared, string, []byte) error {
	panic("executor boom")
}

// A panicking executor must still release the image reference, otherwise the
// image stays "in use" forever and can never be retired. Handle propagates the
// panic (existing behavior), so the test runs Handle in a goroutine and recovers
// there, then asserts the refcount was released.
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

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			_ = recover() // Handle propagates the executor panic; swallow it here.
		}()
		_ = r.Handle(context.Background(), "m", map[string]any{"status": "ok"})
	}()
	<-done

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

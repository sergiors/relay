package runtime

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakeContainer implements reusableContainer for cache tests.
type fakeContainer struct {
	mu        sync.Mutex
	discarded []string
	deadFlag  bool
	// release, when non-nil, makes Invoke block until it is closed (to
	// exercise serialization and busy-entry invalidation). err is returned.
	release chan struct{}
	err     error
}

func (f *fakeContainer) Invoke(_ context.Context, _ string, _ []byte, _ map[string]string) error {
	if f.release != nil {
		<-f.release
	}
	return f.err
}

func (f *fakeContainer) discard(reason string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deadFlag {
		return false
	}
	f.deadFlag = true
	f.discarded = append(f.discarded, reason)
	return true
}

func (f *fakeContainer) dead() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deadFlag
}

func (f *fakeContainer) reasons() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.discarded...)
}

// fakeFactory is the injected start seam: it creates containers, counts them,
// and detects concurrent factory calls (the cache's per-entry serialization
// must never let two factory runs overlap: that would corrupt the protocol).
type fakeFactory struct {
	mu          sync.Mutex
	creations   int
	inside      int
	overlapSeen bool
	build       func() *fakeContainer
	all         []*fakeContainer
}

func (ff *fakeFactory) lastContainer() *fakeContainer {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	if n := len(ff.all); n == 0 {
		return nil
	}
	return ff.all[len(ff.all)-1]
}

func (ff *fakeFactory) allContainers() []*fakeContainer {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	return append([]*fakeContainer(nil), ff.all...)
}

func (ff *fakeFactory) create() (reusableContainer, error) {
	ff.mu.Lock()
	ff.creations++
	ff.inside++
	overlap := ff.inside > 1
	if overlap {
		ff.overlapSeen = true
	}
	ff.mu.Unlock()
	defer func() {
		ff.mu.Lock()
		ff.inside--
		ff.mu.Unlock()
	}()
	if overlap {
		return nil, errOverlap
	}
	var c *fakeContainer
	if ff.build != nil {
		c = ff.build()
	} else {
		c = &fakeContainer{}
	}
	ff.mu.Lock()
	ff.all = append(ff.all, c)
	ff.mu.Unlock()
	// A small delay makes a (broken) overlap observable under the overlap
	// detection above; the serialization contract must make it impossible.
	time.Sleep(20 * time.Millisecond)
	return c, nil
}

func (ff *fakeFactory) start() func() (reusableContainer, error) {
	return ff.create
}

func (ff *fakeFactory) count() int {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	return ff.creations
}

var errOverlap = &overlapError{}

type overlapError struct{}

func (*overlapError) Error() string { return "concurrent factory calls" }

func mkCache() (*containerCache, *fakeFactory) {
	return newContainerCache(), &fakeFactory{}
}

func run(t *testing.T, cc *containerCache, ff *fakeFactory, fnName, image, handler string) error {
	t.Helper()
	return cc.execute(context.Background(), fnName, image, ff.start(), handler, []byte(`{}`), nil)
}

func TestContainerCacheCreatesThenReuses(t *testing.T) {
	cc, ff := mkCache()
	if err := run(t, cc, ff, "fn-a", "img-1", "h1"); err != nil {
		t.Fatalf("first execute: %v", err)
	}
	if ff.count() != 1 {
		t.Fatalf("creations = %d, want 1", ff.count())
	}
	if err := run(t, cc, ff, "fn-a", "img-1", "h2"); err != nil {
		t.Fatalf("second execute: %v", err)
	}
	if ff.count() != 1 {
		t.Fatalf("after reuse creations = %d, want 1 (same container reused)", ff.count())
	}
}

func TestContainerCacheDiscardsOnImageChange(t *testing.T) {
	cc, ff := mkCache()
	if err := run(t, cc, ff, "fn-a", "img-1", "h"); err != nil {
		t.Fatalf("execute v1: %v", err)
	}
	c1 := ff.lastContainer()
	if c1 == nil {
		t.Fatal("first container not tracked")
	}

	// Same function, new image: the old container gets the "image_changed"
	// discard and a fresh container is created.
	if err := run(t, cc, ff, "fn-a", "img-2", "h"); err != nil {
		t.Fatalf("execute v2: %v", err)
	}
	if got := c1.reasons(); len(got) != 1 || got[0] != "image_changed" {
		t.Fatalf("old container discards = %v, want [image_changed]", got)
	}
	if ff.count() != 2 {
		t.Fatalf("creations = %d, want 2 after image change", ff.count())
	}
}

func TestContainerCacheFunctionsNeverShare(t *testing.T) {
	cc, ff := mkCache()
	if err := run(t, cc, ff, "fn-a", "img-1", "h"); err != nil {
		t.Fatalf("A: %v", err)
	}
	if err := run(t, cc, ff, "fn-b", "img-1", "h"); err != nil {
		t.Fatalf("B: %v", err)
	}
	if ff.count() != 2 {
		t.Fatalf("creations = %d, want 2 (each function gets its own container)", ff.count())
	}
}

func TestContainerCacheSerializesConcurrentCalls(t *testing.T) {
	cc, ff := mkCache()
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- cc.execute(context.Background(), "fn-a", "img-1", ff.start(), "h", []byte(`{}`), nil)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent execute: %v", err)
		}
	}
	ff.mu.Lock()
	defer ff.mu.Unlock()
	if ff.overlapSeen {
		t.Fatal("factory must never run concurrently (per-entry serialization broken)")
	}
	if ff.creations != 1 {
		t.Fatalf("creations = %d, want 1 (serialized reuse)", ff.creations)
	}
}

func TestContainerCacheInvalidateImage(t *testing.T) {
	cc, ff := mkCache()
	if err := run(t, cc, ff, "fn-a", "img-1", "h"); err != nil {
		t.Fatalf("execute: %v", err)
	}
	c1 := ff.lastContainer()
	if c1 == nil {
		t.Fatal("container not tracked")
	}

	// An unrelated image invalidates nothing.
	cc.invalidateImage("relay-fn-other:xyz")
	if got := c1.reasons(); len(got) != 0 {
		t.Fatalf("unrelated invalidation discarded our container: %v", got)
	}

	// Our image: discard with "image_changed"; the entry is dropped.
	cc.invalidateImage("img-1")
	if got := c1.reasons(); len(got) != 1 || got[0] != "image_changed" {
		t.Fatalf("discard reasons = %v, want [image_changed]", got)
	}

	// The next execute starts a fresh container.
	if err := run(t, cc, ff, "fn-a", "img-1", "h"); err != nil {
		t.Fatalf("execute after invalidate: %v", err)
	}
	if ff.count() != 2 {
		t.Errorf("creations = %d, want 2 after invalidation", ff.count())
	}
}

func TestContainerCacheInvalidationWhileInFlightDefers(t *testing.T) {
	cc, ff := mkCache()
	if err := run(t, cc, ff, "fn-a", "img-1", "h"); err != nil {
		t.Fatalf("execute: %v", err)
	}
	c1 := ff.lastContainer()
	if c1 == nil {
		t.Fatal("container not tracked")
	}

	// Occupy the entry with a blocking Invoke.
	c1.mu.Lock()
	c1.release = make(chan struct{})
	c1.mu.Unlock()
	errCh := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		errCh <- cc.execute(context.Background(), "fn-a", "img-1", ff.start(), "h2", []byte(`{}`), nil)
	}()
	time.Sleep(50 * time.Millisecond) // let it enter Invoke (blocked on release)

	// The invalidation must NOT block on the busy entry (TryLock).
	invDone := make(chan struct{})
	go func() {
		cc.invalidateImage("img-1")
		close(invDone)
	}()
	select {
	case <-invDone:
	case <-time.After(2 * time.Second):
		t.Fatal("InvalidateImage blocked on a busy entry")
	}
	if got := c1.reasons(); len(got) != 0 {
		t.Fatalf("busy-entry invalidation must defer, discarded now: %v", got)
	}

	// Unblock the in-flight Invoke; the deferred discard drains on the
	// post-Invoke path.
	close(c1.release)
	if err := <-errCh; err != nil {
		t.Fatalf("in-flight execute: %v", err)
	}
	<-done
	if got := c1.reasons(); len(got) != 1 || got[0] != "image_changed" {
		t.Fatalf("deferred discard reasons = %v, want [image_changed]", got)
	}
	// A subsequent execute creates a fresh container.
	if err := run(t, cc, ff, "fn-a", "img-2", "h"); err != nil {
		t.Fatalf("execute after deferred discard: %v", err)
	}
	if ff.count() != 2 {
		t.Errorf("creations = %d, want 2", ff.count())
	}
}

func TestContainerCacheReplacesDiscardedContainer(t *testing.T) {
	cc, ff := mkCache()
	if err := run(t, cc, ff, "fn-a", "img-1", "h"); err != nil {
		t.Fatalf("execute: %v", err)
	}
	c1 := ff.lastContainer()
	c1.mu.Lock()
	c1.deadFlag = true
	c1.mu.Unlock()
	if got := c1.reasons(); len(got) != 0 {
		t.Fatalf("manual dead flag must not add a discard reason, got %v", got)
	}
	if err := run(t, cc, ff, "fn-a", "img-1", "h"); err != nil {
		t.Fatalf("execute after discard: %v", err)
	}
	if ff.count() != 2 {
		t.Errorf("creations = %d, want 2 (dead container replaced)", ff.count())
	}
}

func TestContainerCacheCloseDiscardsAll(t *testing.T) {
	cc, ff := mkCache()
	if err := run(t, cc, ff, "fn-a", "img-1", "h"); err != nil {
		t.Fatalf("A: %v", err)
	}
	if err := run(t, cc, ff, "fn-b", "img-1", "h"); err != nil {
		t.Fatalf("B: %v", err)
	}
	// The second invocation created a new container and discarded... no image
	// change here, so both entries hold their own containers.
	cc.close()
	for _, c := range ff.allContainers() {
		got := c.reasons()
		if len(got) != 1 || got[0] != "shutdown" {
			t.Fatalf("container discards = %v, want [shutdown]", got)
		}
	}
	if n := len(ff.allContainers()); n != 2 {
		t.Fatalf("tracked containers = %d, want 2", n)
	}
}

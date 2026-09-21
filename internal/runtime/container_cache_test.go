package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"relay/internal/function"
)

// fakeContainer implements reusableContainer for pool tests.
type fakeContainer struct {
	mu          sync.Mutex
	discarded   []string
	deadFlag    bool
	invocations int
	// reason is the reason recorded by the container's own teardown path, read
	// back via discardReason (mirrors executionContainer's self-recorded
	// timeout/process_exit/protocol_error reasons).
	reason string
	// release, when non-nil, makes Invoke block until it is closed (to exercise
	// capacity waits and busy-entry invalidation). err is returned.
	release chan struct{}
	// entered, when non-nil, receives one signal per Invoke entry so a test can
	// deterministically observe that an invocation reached the container.
	entered chan struct{}
	// panicOnInvoke makes Invoke panic (to exercise panic-safe lease release).
	panicOnInvoke bool
	err           error
	// selfDiscardReason, when non-empty, makes Invoke tear the container down
	// itself with that reason (mirroring executionContainer's timeout/
	// process_exit/protocol_error paths) before returning an error.
	selfDiscardReason string
	// discardFails makes discard report failure WITHOUT marking the container
	// dead (and without recording a reason), modelling a teardown that could not
	// clean up the container. Used to prove eviction never reinserts a container
	// whose cleanup failed.
	discardFails bool
	// discardAttempts counts every discard call (even failed ones), so a test
	// can prove cleanup is not retried/reinserted.
	discardAttempts int
}

func (f *fakeContainer) Invoke(_ context.Context, _ string, _ []byte, _ map[string]string) error {
	f.mu.Lock()
	f.invocations++
	f.mu.Unlock()
	if f.entered != nil {
		f.entered <- struct{}{}
	}
	if f.panicOnInvoke {
		panic("boom")
	}
	if f.release != nil {
		<-f.release
	}
	if f.selfDiscardReason != "" {
		f.discard(f.selfDiscardReason)
	}
	return f.err
}

func (f *fakeContainer) discard(reason string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.discardAttempts++
	if f.discardFails {
		return false
	}
	if f.deadFlag {
		return false
	}
	f.deadFlag = true
	f.reason = reason
	f.discarded = append(f.discarded, reason)
	return true
}

// discardReason returns the reason recorded by the container's own discard, or
// "" when it did not tear itself down.
func (f *fakeContainer) discardReason() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reason
}

func (f *fakeContainer) attempts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.discardAttempts
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

func (f *fakeContainer) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.invocations
}

// fakeFactory is the injected start seam: it creates containers, counts them,
// and tracks whether two factory runs overlapped (legitimate when the pool has
// capacity for more than one; never when max == 1).
type fakeFactory struct {
	mu          sync.Mutex
	creations   int
	inside      int
	overlapSeen bool
	// startErr, when non-nil, is consumed by the next create to simulate a
	// failed lazy start.
	startErr error
	build    func() *fakeContainer
	all      []*fakeContainer
	// created receives every container as it is built so a test can synchronize
	// on lazy creation without polling.
	created chan *fakeContainer
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
	if ff.startErr != nil {
		err := ff.startErr
		ff.startErr = nil
		ff.mu.Unlock()
		return nil, err
	}
	ff.creations++
	ff.inside++
	if ff.inside > 1 {
		ff.overlapSeen = true
	}
	ff.mu.Unlock()
	defer func() {
		ff.mu.Lock()
		ff.inside--
		ff.mu.Unlock()
	}()
	var c *fakeContainer
	if ff.build != nil {
		c = ff.build()
	} else {
		c = &fakeContainer{}
	}
	ff.mu.Lock()
	ff.all = append(ff.all, c)
	created := ff.created
	ff.mu.Unlock()
	if created != nil {
		created <- c
	}
	// A small delay makes a (broken) overlap observable under the overlap
	// detection above; at max == 1 the reservation must make it impossible.
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

func (ff *fakeFactory) overlapped() bool {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	return ff.overlapSeen
}

// newTestCache returns an empty cache plus its fake factory.
func newTestCache() (*containerCache, *fakeFactory) {
	return newContainerCache(), &fakeFactory{}
}

// runInvoke drives one invoke through the cache at max and returns its error.
func runInvoke(t *testing.T, cc *containerCache, ff *fakeFactory, fnName, image string, max int, handler string) error {
	t.Helper()
	return cc.execute(context.Background(), fnName, image, max, ff.start(), handler, []byte(`{}`), nil)
}

// newBlockingContainer returns a fakeContainer whose Invoke blocks until its
// release channel is closed, signalling once per Invoke entry on entered (buffer
// bounds how many concurrent entries a test observes). It is the shared fixture
// for the many "blocked fake container orchestration" tests.
func newBlockingContainer(buffer int) *fakeContainer {
	return &fakeContainer{release: make(chan struct{}), entered: make(chan struct{}, buffer)}
}

func TestContainerPoolCreatesThenReuses(t *testing.T) {
	cc, ff := newTestCache()
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 2, "h1"); err != nil {
		t.Fatalf("first execute: %v", err)
	}
	if ff.count() != 1 {
		t.Fatalf("creations = %d, want 1", ff.count())
	}
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 2, "h2"); err != nil {
		t.Fatalf("second execute: %v", err)
	}
	if ff.count() != 1 {
		t.Fatalf("after reuse creations = %d, want 1 (same container reused)", ff.count())
	}
	if got := ff.lastContainer().calls(); got != 2 {
		t.Fatalf("container invocations = %d, want 2", got)
	}
}

func TestContainerPoolFunctionsNeverShare(t *testing.T) {
	cc, ff := newTestCache()
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 2, "h"); err != nil {
		t.Fatalf("A: %v", err)
	}
	if err := runInvoke(t, cc, ff, "fn-b", "img-1", 2, "h"); err != nil {
		t.Fatalf("B: %v", err)
	}
	if ff.count() != 2 {
		t.Fatalf("creations = %d, want 2 (each function gets its own container)", ff.count())
	}
}

// TestContainerPoolConcurrentDistinctContainers proves that with capacity the
// pool hands two concurrent invocations of the SAME function two distinct
// containers that are in flight simultaneously.
func TestContainerPoolConcurrentDistinctContainers(t *testing.T) {
	cc, ff := newTestCache()
	ff.created = make(chan *fakeContainer, 2)
	ff.build = func() *fakeContainer {
		return newBlockingContainer(2)
	}

	const max = 2
	var wg sync.WaitGroup
	for i := 0; i < max; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = cc.execute(context.Background(), "fn-a", "img-1", max, ff.start(), "h", []byte(`{}`), nil)
		}()
	}
	// Wait for both lazy creations, then for both invocations to enter.
	c1 := <-ff.created
	c2 := <-ff.created
	if c1 == c2 {
		t.Fatal("pool handed the same container to two concurrent invocations")
	}
	<-c1.entered
	<-c2.entered
	if ff.count() != 2 {
		t.Fatalf("creations = %d, want 2", ff.count())
	}
	if !ff.overlapped() {
		t.Fatal("expected lazy starts to overlap at capacity >= 2")
	}
	close(c1.release)
	close(c2.release)
	wg.Wait()
}

// TestContainerPoolBlocksAtCapacity proves that with max == 1 the second
// invocation blocks until the first releases, then reuses the same container;
// the lazy start never overlaps.
func TestContainerPoolBlocksAtCapacity(t *testing.T) {
	cc, ff := newTestCache()
	// max == 1: the factory is used exactly once, and the second invocation
	// must reuse the released container.
	c1 := newBlockingContainer(2)
	ff.build = func() *fakeContainer { return c1 }

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- cc.execute(context.Background(), "fn-a", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
	}()
	<-c1.entered // first is in flight, holding the sole slot

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- cc.execute(context.Background(), "fn-a", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
	}()
	select {
	case err := <-secondDone:
		t.Fatalf("second invocation completed while the pool was at capacity: %v", err)
	case <-time.After(80 * time.Millisecond):
	}
	if ff.count() != 1 {
		t.Fatalf("creations = %d, want 1 (second must wait, not start)", ff.count())
	}

	close(c1.release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first execute: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second execute: %v", err)
	}
	if ff.count() != 1 {
		t.Fatalf("creations = %d, want 1 (second reuses the released container)", ff.count())
	}
	if got := c1.calls(); got != 2 {
		t.Fatalf("container invocations = %d, want 2", got)
	}
}

// TestContainerPoolAcquireCanceled proves a capacity wait is interruptible by
// context cancellation without creating a container or leaking a reservation.
func TestContainerPoolAcquireCanceled(t *testing.T) {
	cc, ff := newTestCache()
	c1 := newBlockingContainer(1)
	ff.build = func() *fakeContainer { return c1 }

	firstDone := make(chan struct{})
	go func() {
		_ = cc.execute(context.Background(), "fn-a", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
		close(firstDone)
	}()
	<-c1.entered

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	err := cc.execute(ctx, "fn-a", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter error = %v, want context.DeadlineExceeded", err)
	}
	if ff.count() != 1 {
		t.Fatalf("creations = %d, want 1 (waiter must not start a container)", ff.count())
	}

	close(c1.release)
	<-firstDone
}

// TestContainerPoolStartFailureRollsBackReservation proves a failed lazy start
// releases its capacity reservation so a later acquire can start instead of
// blocking forever.
func TestContainerPoolStartFailureRollsBackReservation(t *testing.T) {
	cc, ff := newTestCache()
	ff.startErr = errors.New("start boom")

	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err == nil {
		t.Fatal("expected the failed start to surface")
	}
	if ff.count() != 0 {
		t.Fatalf("creations = %d, want 0", ff.count())
	}

	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("execute after failed start: %v", err)
	}
	if ff.count() != 1 {
		t.Fatalf("creations = %d, want 1 (reservation rolled back)", ff.count())
	}
}

// TestContainerPoolPanicInStartRollsBackReservation proves a panicking start
// factory does not leak capacity: the reservation is rolled back, so a later
// acquire can still start (the pool is not permanently at capacity).
func TestContainerPoolPanicInStartRollsBackReservation(t *testing.T) {
	cc, ff := newTestCache()
	panicked := false
	ff.build = func() *fakeContainer { return &fakeContainer{} }
	start := func() (reusableContainer, error) {
		if !panicked {
			panicked = true
			panic("factory boom")
		}
		return ff.create()
	}

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected the factory panic to propagate")
			}
		}()
		_ = cc.execute(context.Background(), "fn-a", "img-1", 1, start, "h", []byte(`{}`), nil)
	}()

	// The reservation must have rolled back: a fresh acquire starts.
	if err := cc.execute(context.Background(), "fn-a", "img-1", 1, start, "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("execute after factory panic: %v", err)
	}
	if ff.count() != 1 {
		t.Fatalf("creations = %d, want 1 (reservation rolled back after panic)", ff.count())
	}
}

func TestContainerPoolDiscardsOnImageChange(t *testing.T) {
	cc, ff := newTestCache()
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("execute v1: %v", err)
	}
	c1 := ff.lastContainer()
	if c1 == nil {
		t.Fatal("first container not tracked")
	}

	// Same function, new image: the idle old container is discarded immediately
	// on the new acquire and a fresh container is created.
	if err := runInvoke(t, cc, ff, "fn-a", "img-2", 1, "h"); err != nil {
		t.Fatalf("execute v2: %v", err)
	}
	if got := c1.reasons(); len(got) != 1 || got[0] != "image_changed" {
		t.Fatalf("old container discards = %v, want [image_changed]", got)
	}
	if ff.count() != 2 {
		t.Fatalf("creations = %d, want 2 after image change", ff.count())
	}
}

// TestContainerPoolImageChangeRetiresBusy proves a BUSY old-image container is
// marked retired (not discarded mid-invocation) and discarded on release, and
// can never be leased again.
func TestContainerPoolImageChangeRetiresBusy(t *testing.T) {
	cc, ff := newTestCache()
	c1 := newBlockingContainer(1)
	built := 0
	ff.build = func() *fakeContainer {
		built++
		if built == 1 {
			return c1
		}
		return &fakeContainer{}
	}

	v1Done := make(chan error, 1)
	go func() {
		v1Done <- cc.execute(context.Background(), "fn-a", "img-1", 2, ff.start(), "h", []byte(`{}`), nil)
	}()
	<-c1.entered

	// New image: c1 is busy, so it is retired, not discarded yet; a second
	// container serves the new image.
	if err := runInvoke(t, cc, ff, "fn-a", "img-2", 2, "h"); err != nil {
		t.Fatalf("execute v2: %v", err)
	}
	if got := c1.reasons(); len(got) != 0 {
		t.Fatalf("busy old container discarded mid-invocation: %v", got)
	}
	if ff.count() != 2 {
		t.Fatalf("creations = %d, want 2", ff.count())
	}

	close(c1.release)
	if err := <-v1Done; err != nil {
		t.Fatalf("v1 execute: %v", err)
	}
	if got := c1.reasons(); len(got) != 1 || got[0] != "image_changed" {
		t.Fatalf("retired container discards on release = %v, want [image_changed]", got)
	}

	// A later img-1 acquire must NOT reuse the discarded old container.
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 2, "h"); err != nil {
		t.Fatalf("execute v1 again: %v", err)
	}
	if ff.count() != 3 {
		t.Fatalf("creations = %d, want 3 (no re-acquire of the old container)", ff.count())
	}
}

// TestContainerPoolImageTransitionRetiresIdleAndBusy proves a forward image
// transition (a request for a new image) discards a superseded idle container
// immediately, retires a superseded busy container until release, and pools the
// new version. This covers direct callers that never call InvalidateImage.
func TestContainerPoolImageTransitionRetiresIdleAndBusy(t *testing.T) {
	cc, ff := newTestCache()
	const max = 2

	// busy1 is created first and blocks, holding one of the two slots.
	busy1 := newBlockingContainer(1)
	idle1 := &fakeContainer{}
	created := 0
	ff.build = func() *fakeContainer {
		created++
		if created == 1 {
			return busy1
		}
		return idle1
	}
	busyDone := make(chan error, 1)
	go func() {
		busyDone <- cc.execute(context.Background(), "fn-a", "img-1", max, ff.start(), "h", []byte(`{}`), nil)
	}()
	<-busy1.entered

	// idle1 is the second img-1 container, released to idle.
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", max, "h"); err != nil {
		t.Fatalf("seed idle img-1: %v", err)
	}

	// Transition to img-2: idle1 is discarded immediately, busy1 kept busy.
	ff.build = func() *fakeContainer { return &fakeContainer{} }
	if err := runInvoke(t, cc, ff, "fn-a", "img-2", max, "h"); err != nil {
		t.Fatalf("execute img-2: %v", err)
	}
	if got := idle1.reasons(); len(got) != 1 || got[0] != "image_changed" {
		t.Fatalf("superseded idle discards = %v, want [image_changed]", got)
	}
	if got := busy1.reasons(); len(got) != 0 {
		t.Fatalf("superseded busy container discarded mid-invocation: %v", got)
	}

	close(busy1.release)
	if err := <-busyDone; err != nil {
		t.Fatalf("busy img-1 execute: %v", err)
	}
	if got := busy1.reasons(); len(got) != 1 || got[0] != "image_changed" {
		t.Fatalf("superseded busy discards on release = %v, want [image_changed]", got)
	}

	// The new version's idle container is reused, not recreated.
	before := ff.count()
	if err := runInvoke(t, cc, ff, "fn-a", "img-2", max, "h"); err != nil {
		t.Fatalf("reuse img-2: %v", err)
	}
	if ff.count() != before {
		t.Fatalf("creations = %d, want %d (img-2 container reused)", ff.count(), before)
	}
}

// TestContainerPoolRetiredImageNeverPooled proves a stale acquire for an
// invalidated image is served on a throwaway container that is discarded on
// release and never reused by a later acquire.
func TestContainerPoolRetiredImageNeverPooled(t *testing.T) {
	cc, ff := newTestCache()
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	c1 := ff.lastContainer()
	cc.invalidateImage("img-1")
	if got := c1.reasons(); len(got) != 1 {
		t.Fatalf("idle invalidated container discards = %v, want one", got)
	}

	// A stale img-1 request is served at-least-once but not pooled.
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("stale acquire: %v", err)
	}
	c2 := ff.lastContainer()
	if c2 == c1 {
		t.Fatal("stale acquire reused the invalidated container")
	}
	if got := c2.reasons(); len(got) != 1 || got[0] != "image_changed" {
		t.Fatalf("throwaway container discards on release = %v, want [image_changed]", got)
	}

	// Another stale request must start a NEW throwaway, not reuse c2.
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("second stale acquire: %v", err)
	}
	c3 := ff.lastContainer()
	if c3 == c2 {
		t.Fatal("second stale acquire reused the discarded throwaway container")
	}
	if ff.count() != 3 {
		t.Fatalf("creations = %d, want 3 (one per stale acquire)", ff.count())
	}
}

// TestContainerPoolInvalidateBeforePoolExists is the deterministic regression
// for the invalidate-vs-first-acquire race: invalidation runs BEFORE any pool
// for the function exists (so the invalidate snapshot cannot include it), and
// the first acquire must still treat the image as retired — a throwaway
// container that is never pooled — rather than a warm one.
func TestContainerPoolInvalidateBeforePoolExists(t *testing.T) {
	cc, ff := newTestCache()
	// No acquire has run, so fn-a has no pool at all.
	cc.invalidateImage("img-1")

	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("first acquire after invalidate: %v", err)
	}
	c1 := ff.lastContainer()
	if got := c1.reasons(); len(got) != 1 || got[0] != "image_changed" {
		t.Fatalf("first-acquire container discards = %v, want [image_changed] (must not be pooled)", got)
	}

	// A second stale acquire must start a NEW throwaway, proving the first was
	// never retained as an idle pooled container.
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("second acquire after invalidate: %v", err)
	}
	c2 := ff.lastContainer()
	if c2 == c1 {
		t.Fatal("second acquire reused the throwaway container; retired image was pooled")
	}
	if ff.count() != 2 {
		t.Fatalf("creations = %d, want 2 (one throwaway per stale acquire)", ff.count())
	}
}

// TestContainerPoolInvalidateDuringFirstLazyStart forces the brand-new-pool
// interleaving: the very first acquire creates the pool and blocks in start;
// an invalidation runs while that start is in flight. The completed container
// must be marked retired/transient and discarded on release, never pooled.
func TestContainerPoolInvalidateDuringFirstLazyStart(t *testing.T) {
	cc, ff := newTestCache()
	started := make(chan struct{})
	unblock := make(chan struct{})
	var first sync.Once
	ff.build = func() *fakeContainer {
		first.Do(func() {
			close(started)
			<-unblock
		})
		return &fakeContainer{}
	}

	leaseCh := make(chan *containerLease, 1)
	errCh := make(chan error, 1)
	go func() {
		l, err := cc.acquire(context.Background(), "fn-a", "img-1", 1, ff.start())
		if err != nil {
			errCh <- err
			return
		}
		leaseCh <- l
	}()
	<-started

	// The pool now exists and the start is in flight; invalidate the image.
	invDone := make(chan struct{})
	go func() {
		cc.invalidateImage("img-1")
		close(invDone)
	}()
	select {
	case <-invDone:
	case <-time.After(2 * time.Second):
		t.Fatal("InvalidateImage blocked on the in-flight start")
	}

	close(unblock)
	var lease *containerLease
	select {
	case lease = <-leaseCh:
	case err := <-errCh:
		t.Fatalf("acquire after in-flight invalidation: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("acquire did not complete after unblocking the start")
	}

	// The container served the invocation but must be discarded on release and
	// never pooled, so a later acquire starts a fresh throwaway.
	c1 := ff.lastContainer()
	lease.release()
	if got := c1.reasons(); len(got) != 1 || got[0] != "image_changed" {
		t.Fatalf("in-flight-start container discards = %v, want [image_changed]", got)
	}

	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("later acquire: %v", err)
	}
	if c2 := ff.lastContainer(); c2 == c1 {
		t.Fatal("later acquire reused the invalidated in-flight container")
	}
}

func TestContainerPoolInvalidateImageIdleImmediate(t *testing.T) {
	cc, ff := newTestCache()
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
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

	// Our image, idle: discard with "image_changed" immediately.
	cc.invalidateImage("img-1")
	if got := c1.reasons(); len(got) != 1 || got[0] != "image_changed" {
		t.Fatalf("discard reasons = %v, want [image_changed]", got)
	}

	// The next execute starts a fresh container.
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("execute after invalidate: %v", err)
	}
	if ff.count() != 2 {
		t.Errorf("creations = %d, want 2 after invalidation", ff.count())
	}
}

func TestContainerPoolInvalidationWhileInFlightDefers(t *testing.T) {
	cc, ff := newTestCache()
	c1 := newBlockingContainer(1)
	ff.build = func() *fakeContainer { return c1 }

	errCh := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		errCh <- cc.execute(context.Background(), "fn-a", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
	}()
	<-c1.entered

	// The invalidation must NOT block and must NOT discard the busy container;
	// it is retired and discarded on release.
	invDone := make(chan struct{})
	go func() {
		cc.invalidateImage("img-1")
		close(invDone)
	}()
	select {
	case <-invDone:
	case <-time.After(2 * time.Second):
		t.Fatal("InvalidateImage blocked on an in-flight invocation")
	}
	if got := c1.reasons(); len(got) != 0 {
		t.Fatalf("busy-entry invalidation must defer, discarded now: %v", got)
	}

	close(c1.release)
	if err := <-errCh; err != nil {
		t.Fatalf("in-flight execute: %v", err)
	}
	<-done
	if got := c1.reasons(); len(got) != 1 || got[0] != "image_changed" {
		t.Fatalf("deferred discard reasons = %v, want [image_changed]", got)
	}
}

// TestContainerPoolPanicDiscardsAndFreesCapacity proves a panicking Invoke
// discards the container (never returning a corrupt one to the pool) and still
// releases capacity.
func TestContainerPoolPanicDiscardsAndFreesCapacity(t *testing.T) {
	cc, ff := newTestCache()
	panicking := &fakeContainer{panicOnInvoke: true}
	ff.build = func() *fakeContainer { return panicking }

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected the invocation panic to propagate")
			}
		}()
		_ = cc.execute(context.Background(), "fn-a", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
	}()
	if got := panicking.reasons(); len(got) != 1 || got[0] != "protocol_error" {
		t.Fatalf("panicking container discards = %v, want [protocol_error]", got)
	}

	// Capacity must have been freed: a second execute succeeds and starts fresh
	// (the panicking container is not reused).
	ff.build = func() *fakeContainer { return &fakeContainer{} }
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("execute after panic: %v", err)
	}
	if ff.count() != 2 {
		t.Fatalf("creations = %d, want 2", ff.count())
	}
}

func TestContainerPoolReplacesDeadContainer(t *testing.T) {
	cc, ff := newTestCache()
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("execute: %v", err)
	}
	c1 := ff.lastContainer()
	c1.mu.Lock()
	c1.deadFlag = true
	c1.mu.Unlock()
	if got := c1.reasons(); len(got) != 0 {
		t.Fatalf("manual dead flag must not add a discard reason, got %v", got)
	}
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("execute after discard: %v", err)
	}
	if ff.count() != 2 {
		t.Errorf("creations = %d, want 2 (dead container replaced)", ff.count())
	}
}

// TestContainerPoolCloseWakesWaitersAndDiscardsAll proves close discards idle
// AND active containers, wakes a capacity waiter with errPoolClosed, and
// prevents further acquires.
func TestContainerPoolCloseWakesWaitersAndDiscardsAll(t *testing.T) {
	cc, ff := newTestCache()

	// fn-a: one idle container.
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("A: %v", err)
	}
	idle := ff.lastContainer()

	// fn-b: one busy container holding the sole slot.
	busy := newBlockingContainer(1)
	ff.build = func() *fakeContainer { return busy }
	busyDone := make(chan error, 1)
	go func() {
		busyDone <- cc.execute(context.Background(), "fn-b", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
	}()
	<-busy.entered

	// fn-b: a waiter blocked at capacity.
	waiterDone := make(chan error, 1)
	go func() {
		waiterDone <- cc.execute(context.Background(), "fn-b", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
	}()

	cc.close()

	if err := <-waiterDone; !errors.Is(err, errPoolClosed) {
		t.Fatalf("waiter after close = %v, want errPoolClosed", err)
	}
	if got := idle.reasons(); len(got) != 1 || got[0] != "shutdown" {
		t.Fatalf("idle container discards = %v, want [shutdown]", got)
	}
	if got := busy.reasons(); len(got) != 1 || got[0] != "shutdown" {
		t.Fatalf("busy container discards = %v, want [shutdown]", got)
	}

	// A subsequent acquire fails immediately.
	if err := runInvoke(t, cc, ff, "fn-c", "img-1", 1, "h"); !errors.Is(err, errPoolClosed) {
		t.Fatalf("execute after close = %v, want errPoolClosed", err)
	}

	// Unblock the busy invocation; the lease release is a harmless no-op.
	close(busy.release)
	if err := <-busyDone; err != nil {
		t.Fatalf("busy execute: %v", err)
	}
	if got := busy.reasons(); len(got) != 1 {
		t.Fatalf("busy discard must be exactly once, got %v", got)
	}
}

// TestContainerPoolTransientBounded proves concurrent stale (retired-image)
// acquires are bounded among themselves by max and wake as transients release.
func TestContainerPoolTransientBounded(t *testing.T) {
	cc, ff := newTestCache()
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 2, "h"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	cc.invalidateImage("img-1")

	ff.build = func() *fakeContainer {
		return newBlockingContainer(1)
	}
	before := ff.count()

	start := func() (reusableContainer, error) { return ff.create() }
	done := make(chan *containerLease, 2)
	errCh := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			l, err := cc.acquire(context.Background(), "fn-a", "img-1", 2, start)
			if err != nil {
				errCh <- err
				return
			}
			done <- l
		}()
	}
	l1 := <-done
	l2 := <-done
	if got := ff.count() - before; got != 2 {
		t.Fatalf("transient creations = %d, want 2", got)
	}

	// A third stale acquire must block at the transient bound.
	third := make(chan *containerLease, 1)
	go func() {
		l, err := cc.acquire(context.Background(), "fn-a", "img-1", 2, start)
		if err != nil {
			errCh <- err
			return
		}
		third <- l
	}()
	select {
	case l := <-third:
		l.release()
		t.Fatal("third transient acquire must block at the bound")
	case err := <-errCh:
		t.Fatalf("third transient acquire: %v", err)
	case <-time.After(80 * time.Millisecond):
	}

	// Releasing one transient frees a slot and wakes the third.
	l1.release()
	select {
	case l := <-third:
		l.release()
	case err := <-errCh:
		t.Fatalf("third transient acquire after release: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("third transient acquire not woken by release")
	}
	l2.release()
}

// TestContainerPoolCloseDiscardsTransient proves Close tears down a leased
// throwaway container serving a stale (retired-image) request.
func TestContainerPoolCloseDiscardsTransient(t *testing.T) {
	cc, ff := newTestCache()
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	cc.invalidateImage("img-1")

	transient := newBlockingContainer(1)
	ff.build = func() *fakeContainer { return transient }
	done := make(chan error, 1)
	go func() {
		done <- cc.execute(context.Background(), "fn-a", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
	}()
	<-transient.entered

	cc.close()
	if got := transient.reasons(); len(got) != 1 || got[0] != "shutdown" {
		t.Fatalf("transient container discards = %v, want [shutdown]", got)
	}
	close(transient.release)
	if err := <-done; err != nil {
		t.Fatalf("transient execute: %v", err)
	}
	// Release after close must not discard a second time.
	if got := transient.reasons(); len(got) != 1 {
		t.Fatalf("transient discard must be exactly once, got %v", got)
	}
}

func TestResolveConcurrencyDefaultsTemplateValue(t *testing.T) {
	cases := []struct {
		name string
		fn   function.Function
		want int
	}{
		{"nil template", function.Function{}, function.DefaultConcurrency},
		{"zero", function.Function{Template: &function.Template{Concurrency: 0}}, function.DefaultConcurrency},
		{"negative", function.Function{Template: &function.Template{Concurrency: -3}}, function.DefaultConcurrency},
		{"explicit", function.Function{Template: &function.Template{Concurrency: 5}}, 5},
	}
	for _, tc := range cases {
		if got := resolveConcurrency(tc.fn); got != tc.want {
			t.Errorf("%s: resolveConcurrency = %d, want %d", tc.name, got, tc.want)
		}
	}
}

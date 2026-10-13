package runner

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/runtime"
	"relay/internal/testutil"
)

// attemptWatcher installs the runner's attempt-completion seam and lets a test
// wait for the Nth completed cleanup attempt, so a chain's terminal
// classification is observed deterministically instead of by polling or
// sleeping. All awaited counts must be PRE-REGISTERED at construction (max), so
// a signal can never be missed by registering after the attempt already ran.
type attemptWatcher struct {
	mu      sync.Mutex
	n       int
	signals map[int]chan struct{}
}

func newAttemptWatcher(r *Runner, max int) *attemptWatcher {
	w := &attemptWatcher{signals: map[int]chan struct{}{}}
	for i := 1; i <= max; i++ {
		w.signals[i] = make(chan struct{})
	}
	r.imageCleanupAttemptDone = w.record
	return w
}

func (w *attemptWatcher) record(string) {
	w.mu.Lock()
	w.n++
	ch := w.signals[w.n]
	w.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

func (w *attemptWatcher) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.n
}

func (w *attemptWatcher) wait(t *testing.T, n int) {
	t.Helper()
	w.mu.Lock()
	ch := w.signals[n]
	w.mu.Unlock()
	if ch == nil {
		t.Fatalf("attempt %d was not pre-registered", n)
	}
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("cleanup attempt %d was never classified (completed: %d)", n, w.count())
	}
}

// gatingCleaner is an Executor + ImageCleaner whose managed-container reference
// check parks until the gate is opened, so a test can interleave a stale
// acquisition EXACTLY between two removal attempts: the first attempt parks
// inside its reference check (after its in-use check passed), the acquisition
// lands, and the next attempt's in-use check then observes it.
type gatingCleaner struct {
	mu          sync.Mutex
	referenced  map[string]bool
	removed     []string
	gcCalls     int
	gate        chan struct{}
	entered     chan struct{}
	enteredOnce sync.Once
}

func newGatingCleaner() *gatingCleaner {
	return &gatingCleaner{
		referenced: map[string]bool{},
		gate:       make(chan struct{}),
		entered:    make(chan struct{}),
	}
}

func (*gatingCleaner) Execute(context.Context, *runtime.Prepared, string, []byte, []string) error {
	return nil
}

// AppImageTags is part of the ImageCleaner contract; this test cleaner serves no
// app tags.
func (*gatingCleaner) AppImageTags(context.Context, string) ([]string, error) {
	return nil, nil
}

func (g *gatingCleaner) ImageReferencedByManagedContainer(ctx context.Context, image string) (bool, error) {
	g.mu.Lock()
	gate := g.gate
	referenced := g.referenced[image]
	g.mu.Unlock()
	g.enteredOnce.Do(func() { close(g.entered) })
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	return referenced, nil
}

func (g *gatingCleaner) RemoveImage(_ context.Context, image string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.removed = append(g.removed, image)
	return nil
}

func (g *gatingCleaner) CleanupUnusedDependencies(context.Context) (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.gcCalls++
	return 0, nil
}

// openGate unblocks the parked reference check and lets later checks pass
// straight through.
func (g *gatingCleaner) openGate() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.gate != nil {
		close(g.gate)
		g.gate = nil
	}
}

func (g *gatingCleaner) setReferenced(image string, v bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.referenced[image] = v
}

func (g *gatingCleaner) removedImages() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.removed...)
}

// TestRetireImageInUseAttemptDefersThenIdleReDrivesRemoval pins the reviewed
// race: a stale pre-swap snapshot claims the superseded image AFTER retirement
// but BEFORE the cleanup attempt's in-use check, so that attempt concludes
// in-use. The retirement must survive the stale use (no acquire may cancel it),
// the attempt must retain the retirement as a DEFERRED candidate while releasing
// its admission, and the stale use's release must re-drive removal — otherwise
// the image is stranded until a restart.
func TestRetireImageInUseAttemptDefersThenIdleReDrivesRemoval(t *testing.T) {
	cleaner := newGatingCleaner()
	// The first attempt's reference check reports the image referenced, turning
	// it into a guard-skip that arms one retry.
	cleaner.setReferenced(retiredImage, true)

	logger, buf := debugBufferLogger()
	r := newRetirementRunner(t, cleaner, logger)
	r.imageCleanupRetryDelays = []time.Duration{5 * time.Millisecond}
	w := newAttemptWatcher(r, 3)

	// Attempt 1: in-use check passed (no use yet), then parks inside the
	// reference check.
	r.RetireImage(retiredImage)
	select {
	case <-cleaner.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the first cleanup attempt never reached its reference check")
	}

	// The stale pre-swap snapshot claims the image NOW — after retirement and
	// before attempt 2's in-use check.
	holdImage(r, retiredImage)

	// Let attempt 1 conclude its guard-skip and arm the retry.
	cleaner.openGate()
	w.wait(t, 1)

	// Attempt 2 sees the stale use and concludes DEFERRED — the retirement is
	// retained (not cancelled) and the admission is released, so the idle
	// handoff can re-drive removal.
	w.wait(t, 2)
	if got := cleaner.removedImages(); len(got) != 0 {
		t.Fatalf("removed while a stale use held the image: %v, want none", got)
	}
	if st := r.refs.stateOf(retiredImage); !st.retired {
		t.Fatal("a stale use must not cancel the retirement")
	} else if st.active {
		t.Fatal("a concluded chain must release its admission so the idle handoff can re-drive")
	} else if !st.deferred {
		t.Fatal("an in-use attempt must retain the retirement as a deferred candidate, or a coalesced idle handoff would strand the image")
	}
	if strings.Contains(buf.String(), "image is served again") {
		t.Fatal("a stale use must not be misread as a re-activation")
	}

	// The stale use ends AND the container reference clears (the replaced service
	// container is gone): the idle transition re-drives removal, which now
	// succeeds exactly once.
	cleaner.setReferenced(retiredImage, false)
	releaseImage(r, retiredImage)
	w.wait(t, 3)
	if got := cleaner.removedImages(); len(got) != 1 || got[0] != retiredImage {
		t.Fatalf("removed after the stale use ended = %v, want exactly [%s]", got, retiredImage)
	}
	if st := r.refs.stateOf(retiredImage); st.retired || st.active {
		t.Fatalf("a successful removal must drop every per-image record, state = %+v", st)
	}
	if got := r.refs.deferredImages(); len(got) != 0 {
		t.Fatalf("deferred images after removal = %v, want none", got)
	}
}

// TestRetireImageDeferredThenReDrivenAfterReferenceClears pins the fixed leak:
// when a retired image is still referenced by a relay-owned container past its
// bounded retry horizon, retirement ownership is RETAINED, and an existing
// lifecycle re-drive (the reconciler audit / service-convergence hook) removes
// it once the reference clears — including after the initial retry window
// elapsed.
func TestRetireImageDeferredThenReDrivenAfterReferenceClears(t *testing.T) {
	exec := &blockingExecutor{referenced: map[string]bool{retiredImage: true}}
	logger, buf := debugBufferLogger()
	r := newRetirementRunner(t, exec, logger)
	// No retry timers: the first guard-skip exhausts the bounded retries
	// immediately, so the deferral is reached in exactly one attempt and the
	// test needs no timers.
	r.imageCleanupRetryDelays = nil
	w := newAttemptWatcher(r, 2)

	r.RetireImage(retiredImage)
	w.wait(t, 1)

	if got := exec.removedImages(); len(got) != 0 {
		t.Fatalf("removed a referenced image: %v, want none", got)
	}
	if !strings.Contains(buf.String(), "deferring to a later cleanup pass") {
		t.Fatalf("expected the deferral Info, got:\n%s", buf.String())
	}
	// Ownership is retained: without it nothing would remove the image after the
	// initial retry window.
	if got := r.refs.deferredImages(); len(got) != 1 || got[0] != retiredImage {
		t.Fatalf("deferred images = %v, want exactly [%s]", got, retiredImage)
	}

	// The service container is replaced: the reference clears. The lifecycle
	// re-drive now removes the image.
	exec.setReferenced(retiredImage, false)
	r.RetryDeferredImageRemovals()
	w.wait(t, 2)

	if got := exec.removedImages(); len(got) != 1 || got[0] != retiredImage {
		t.Fatalf("removed after re-drive = %v, want exactly [%s]", got, retiredImage)
	}
	// A successful removal drops the deferred record: nothing is left to churn.
	if got := r.refs.deferredImages(); len(got) != 0 {
		t.Fatalf("deferred images after removal = %v, want none", got)
	}
}

// TestRetireImageAfterDeferralReDrivesOnSecondRetire pins that the retired
// marker no longer suppresses a later retirement: re-retiring an image whose
// chain deferred re-drives removal instead of being a permanent no-op.
func TestRetireImageAfterDeferralReDrivesOnSecondRetire(t *testing.T) {
	exec := &blockingExecutor{referenced: map[string]bool{retiredImage: true}}
	logger, buf := debugBufferLogger()
	r := newRetirementRunner(t, exec, logger)
	r.imageCleanupRetryDelays = nil
	w := newAttemptWatcher(r, 2)

	r.RetireImage(retiredImage)
	w.wait(t, 1)
	if !strings.Contains(buf.String(), "deferring to a later cleanup pass") {
		t.Fatalf("expected the deferral Info, got:\n%s", buf.String())
	}

	exec.setReferenced(retiredImage, false)
	r.RetireImage(retiredImage) // already-retired marker must not block
	w.wait(t, 2)
	if got := exec.removedImages(); len(got) != 1 || got[0] != retiredImage {
		t.Fatalf("removed on the second retire = %v, want exactly [%s]", got, retiredImage)
	}
}

// TestRetireImageGenuineRemoveFailureReDriven pins the sibling leak: a genuine
// removal error (not a guard-skip) retains deferred retirement ownership, so a
// later lifecycle re-drive retries the removal instead of the image being left
// behind until a boot sweep.
func TestRetireImageGenuineRemoveFailureReDriven(t *testing.T) {
	exec := &blockingExecutor{removeErr: true}
	logger, buf := debugBufferLogger()
	r := newRetirementRunner(t, exec, logger)
	w := newAttemptWatcher(r, 2)

	r.RetireImage(retiredImage)
	w.wait(t, 1)

	if !strings.Contains(buf.String(), "remove retired failed") {
		t.Fatalf("a genuine removal failure must Warn, got:\n%s", buf.String())
	}
	if got := exec.removedImages(); len(got) != 0 {
		t.Fatalf("removed despite a failed removal: %v, want none", got)
	}
	if got := r.refs.deferredImages(); len(got) != 1 || got[0] != retiredImage {
		t.Fatalf("a genuine removal failure must retain ownership, deferred = %v", got)
	}

	exec.setRemoveErr(false)
	r.RetryDeferredImageRemovals()
	w.wait(t, 2)
	if got := exec.removedImages(); len(got) != 1 || got[0] != retiredImage {
		t.Fatalf("removed on the re-drive = %v, want exactly [%s]", got, retiredImage)
	}
}

// TestRetireImageWhileChainActiveCoalesces pins that chain admission is
// coalesced per image: re-retiring an image whose removal chain is already
// active starts no second chain (and therefore runs no extra reference check).
func TestRetireImageWhileChainActiveCoalesces(t *testing.T) {
	exec := &blockingExecutor{referenced: map[string]bool{retiredImage: true}}
	r := newRetirementRunner(t, exec, testutil.DiscardLogger())
	// A long retry delay keeps the chain active (rescheduled) for the whole
	// test, so a coalesced re-retire is observable synchronously.
	r.imageCleanupRetryDelays = []time.Duration{time.Hour}
	w := newAttemptWatcher(r, 1)

	r.RetireImage(retiredImage)
	w.wait(t, 1)

	checks := exec.referenceChecksCount()
	r.RetireImage(retiredImage)
	r.RetireImage(retiredImage)
	if got := exec.referenceChecksCount(); got != checks {
		t.Fatalf("re-retiring an image with an active chain ran %d extra reference checks, want 0", got-checks)
	}
	if got := w.count(); got != 1 {
		t.Fatalf("completed attempts = %d, want 1 (the second retires must coalesce)", got)
	}
}

// TestRetryDeferredImageRemovalsIdempotentAndNilSafe pins that the re-drive hook
// is a no-op with nothing deferred and with no cleaner at all.
func TestRetryDeferredImageRemovalsIdempotentAndNilSafe(t *testing.T) {
	exec := &blockingExecutor{}
	r := newRetirementRunner(t, exec, testutil.DiscardLogger())

	r.RetryDeferredImageRemovals()
	r.RetryDeferredImageRemovals()
	if got := exec.removedImages(); len(got) != 0 {
		t.Fatalf("idle re-drive removed images: %v, want none", got)
	}
	if n := exec.referenceChecksCount(); n != 0 {
		t.Fatalf("idle re-drive ran %d reference checks, want 0", n)
	}

	// A runner whose executor cannot clean (no ImageCleaner capability) is safe.
	plain := NewWithMetrics([]*PreparedApp{newFn(t, "a")}, testutil.DiscardLogger(), nil)
	plain.RetryDeferredImageRemovals()
	plain.RetireImage(retiredImage)
}

// TestImageRefCounterCleanupAdmissionCoalescesAndDefers is the unit pin for the
// chain-admission state: one active chain per image, a deferred image is
// re-admissible (which clears the deferred record), a successful removal drops
// every record, and an abandoned chain keeps the retirement so a later idle
// transition still owns removal.
func TestImageRefCounterCleanupAdmissionCoalescesAndDefers(t *testing.T) {
	c := newImageRefCounter()
	const img = "relay-app-a:old"

	if !c.beginCleanup(img) {
		t.Fatal("the first admission must own the chain")
	}
	if c.beginCleanup(img) {
		t.Fatal("a concurrent admission must coalesce to the active chain")
	}
	c.finishCleanup(img, false) // deferred
	if got := c.deferredImages(); len(got) != 1 || got[0] != img {
		t.Fatalf("deferred images = %v, want [%s]", got, img)
	}

	if !c.beginCleanup(img) {
		t.Fatal("a deferred image must be re-admissible")
	}
	if got := c.deferredImages(); len(got) != 0 {
		t.Fatalf("re-admission must clear the deferred record, got %v", got)
	}
	c.finishCleanup(img, true) // removed
	if got := c.deferredImages(); len(got) != 0 {
		t.Fatalf("a successful removal must leave nothing deferred, got %v", got)
	}
	if st := c.stateOf(img); st.active || st.retired || st.deferred {
		t.Fatalf("a successful removal must drop every per-image record, state = %+v", st)
	}

	// An abandoned chain releases the admission but KEEPS the retirement, so the
	// next idle transition re-owns removal.
	c.recordRetired(img)
	if !c.beginCleanup(img) {
		t.Fatal("an idle retirement must be admissible")
	}
	c.abandonCleanup(img)
	if st := c.stateOf(img); st.active {
		t.Fatal("an abandoned chain must drop its admission")
	} else if !st.retired {
		t.Fatal("an abandoned chain must keep the retirement so the idle transition re-drives")
	}
	c.acquire(img)
	c.release(img)
	if st := c.stateOf(img); !st.retired {
		t.Fatal("a use must never cancel a retirement")
	}
	c.clearRetirement(img)
	if st := c.stateOf(img); st.retired || st.deferred || st.active {
		t.Fatalf("clearRetirement must drop every per-image record, state = %+v", st)
	}
}

// TestImageRefCounterRefcountStepsDownPerRelease pins the refcount contract an
// image retirement depends on: every release advances the count exactly one
// step, so an image with overlapping uses becomes idle (and its idle transition
// fires) once the LAST use ends. A release that fails to persist the decrement
// leaves ImageInUse true forever, which permanently blocks a retirement.
func TestImageRefCounterRefcountStepsDownPerRelease(t *testing.T) {
	c := newImageRefCounter()
	const img = "relay-app-a:old"

	idleFires := 0
	c.setOnIdle(func(string) { idleFires++ })

	c.acquire(img)
	c.acquire(img)
	if !c.inUse(img) {
		t.Fatal("the image must be in use while a use is held")
	}

	c.release(img)
	if !c.inUse(img) {
		t.Fatal("the image is still in use after only one of two uses released")
	}
	if idleFires != 0 {
		t.Fatalf("idle transitions = %d, want 0 while a use is still held", idleFires)
	}

	c.release(img)
	if c.inUse(img) {
		t.Fatal("the image must be idle once every use released")
	}
	if idleFires != 0 {
		t.Fatalf("idle transitions = %d, want 0 (the image is not retired)", idleFires)
	}

	// A retired image fires exactly one idle transition per idle crossing.
	c.recordRetired(img)
	c.acquire(img)
	c.release(img)
	if idleFires != 1 {
		t.Fatalf("idle transitions = %d, want 1 for a retired image going idle", idleFires)
	}
	// The retirement is retained, so the NEXT use/release cycle re-owns removal.
	c.acquire(img)
	c.release(img)
	if idleFires != 2 {
		t.Fatalf("idle transitions = %d, want 2 (retirement must survive a stale use)", idleFires)
	}
}

// TestRetireImageCoalescedIdleHandoffLeavesDeferredCandidate pins the exact
// lost-wakeup ordering the in-use conclusion exists to survive, purely at the
// state-machine level (the same primitives the cleanup chain uses):
//
//  1. an admission is active (an in-flight chain);
//  2. the image's last execution releases, so release fires onIdle — but its
//     drive COALESCES against the active admission and is dropped;
//  3. the chain observes the image in use and concludes DEFERRED.
//
// Only the deferred record makes step 3's conclusion durable: without it the
// coalesced idle handoff is lost and the superseded image is stranded. A later
// lifecycle re-drive must then remove it once the refs are gone. No sleeps: every
// observation is a state read or a pre-registered attempt signal.
func TestRetireImageCoalescedIdleHandoffLeavesDeferredCandidate(t *testing.T) {
	exec := &blockingExecutor{}
	r := newRetirementRunner(t, exec, testutil.DiscardLogger())
	w := newAttemptWatcher(r, 1)

	// A retirement exists and an in-flight chain owns the admission.
	r.refs.recordRetired(retiredImage)
	if !r.refs.beginCleanup(retiredImage) {
		t.Fatal("the chain admission must be free")
	}
	// A stale use holds the image (so the in-flight chain observed it in use).
	holdImage(r, retiredImage)

	// The last execution releases: onIdle fires and COALESCES against the active
	// admission, so no new chain starts.
	releaseImage(r, retiredImage)
	if got := w.count(); got != 0 {
		t.Fatalf("the coalesced idle handoff started %d chains, want 0", got)
	}

	// The in-flight chain concludes DEFERRED (the production outcome for an
	// in-use attempt).
	r.refs.finishCleanup(retiredImage, false)

	// The retirement is retained as a deferred candidate, with the admission
	// released, so a later lifecycle event still owns the removal.
	st := r.refs.stateOf(retiredImage)
	if !st.retired || !st.deferred || st.active {
		t.Fatalf("state after the coalesced handoff = %+v, want retired+deferred and no admission", st)
	}
	if got := r.refs.deferredImages(); len(got) != 1 || got[0] != retiredImage {
		t.Fatalf("deferred images = %v, want [%s]", got, retiredImage)
	}

	// A future re-drive (the periodic audit / service-completion hook) removes it
	// now that no reference is held — exactly once.
	r.RetryDeferredImageRemovals()
	w.wait(t, 1)
	if got := exec.removedImages(); len(got) != 1 || got[0] != retiredImage {
		t.Fatalf("re-drive removed %v, want exactly [%s]", got, retiredImage)
	}
	if st := r.refs.stateOf(retiredImage); st.retired || st.deferred || st.active {
		t.Fatalf("a successful removal must drop every record, state = %+v", st)
	}
}

// TestRetireImageDeferredInUseSkipsRedriveThenIdleRemoves pins both halves of the
// in-use deferral: a deferred candidate whose image is still in use is NOT
// re-driven by the lifecycle hook — no chain is spawned for an image that
// provably cannot be cleaned yet, so repeated re-drives are pure state reads (no
// churn, no hot loop) — and once the executing use releases, the ordinary idle
// handoff re-drives removal with no audit needed.
func TestRetireImageDeferredInUseSkipsRedriveThenIdleRemoves(t *testing.T) {
	exec := &blockingExecutor{}
	r := newRetirementRunner(t, exec, testutil.DiscardLogger())
	w := newAttemptWatcher(r, 1)

	// The state a chain that observed an in-use image leaves behind: a deferred
	// candidate with the admission released.
	r.refs.recordRetired(retiredImage)
	r.refs.beginCleanup(retiredImage)
	r.refs.finishCleanup(retiredImage, false)

	// A stale use still holds the image.
	holdImage(r, retiredImage)

	// Repeated lifecycle re-drives must not spawn a chain for an in-use
	// candidate, and must not lose it.
	r.RetryDeferredImageRemovals()
	r.RetryDeferredImageRemovals()
	if got := w.count(); got != 0 {
		t.Fatalf("an in-use deferred candidate was driven %d times, want 0", got)
	}
	if st := r.refs.stateOf(retiredImage); !st.retired || !st.deferred {
		t.Fatalf("an in-use deferred candidate must be retained, state = %+v", st)
	}
	if got := exec.removedImages(); len(got) != 0 {
		t.Fatalf("removed while in use: %v, want none", got)
	}

	// The use ends: the idle handoff re-drives removal immediately.
	releaseImage(r, retiredImage)
	w.wait(t, 1)
	if got := exec.removedImages(); len(got) != 1 || got[0] != retiredImage {
		t.Fatalf("removed after idle = %v, want exactly [%s]", got, retiredImage)
	}
	if st := r.refs.stateOf(retiredImage); st.retired || st.deferred || st.active {
		t.Fatalf("a successful removal must drop every record, state = %+v", st)
	}
}

// TestRetireImageServedAndInUseIsCancelledNotDeferred pins that the served-image
// check wins over the in-use deferral: a generation the registry currently serves
// has its stale retirement CANCELLED (every record dropped) rather than re-armed
// as a deferred candidate, and neither holding nor releasing a use of that
// current generation can arm anything.
func TestRetireImageServedAndInUseIsCancelledNotDeferred(t *testing.T) {
	exec := &blockingExecutor{}
	logger, buf := debugBufferLogger()
	r := newRetirementRunner(t, exec, logger)
	w := newAttemptWatcher(r, 1)

	// A stale retirement for the generation the registry serves, left as a
	// deferred candidate (so the re-drive has something to act on).
	r.refs.recordRetired(currentImage)
	r.refs.beginCleanup(currentImage)
	r.refs.finishCleanup(currentImage, false)

	r.RetryDeferredImageRemovals()
	w.wait(t, 1)

	if got := exec.removedImages(); len(got) != 0 {
		t.Fatalf("removed the image the registry serves: %v, want none", got)
	}
	if st := r.refs.stateOf(currentImage); st.retired || st.deferred || st.active {
		t.Fatalf("a served image must be cancelled, never re-armed, state = %+v", st)
	}
	if !strings.Contains(buf.String(), "image is served again; cancelling retirement") {
		t.Fatalf("expected the cancellation log, got:\n%s", buf.String())
	}

	// Holding and releasing a use of the current generation (and further
	// re-drives) must arm nothing: no chain, no removal.
	holdImage(r, currentImage)
	r.RetryDeferredImageRemovals()
	releaseImage(r, currentImage)
	r.RetryDeferredImageRemovals()
	if got := w.count(); got != 1 {
		t.Fatalf("completed attempts = %d, want 1 (a served image must not be retried)", got)
	}
	if got := exec.removedImages(); len(got) != 0 {
		t.Fatalf("removed a served image on a later drive: %v, want none", got)
	}
}

// TestRetireImageConcurrentDrivesAndReDrives is a race-detector exercise: many
// concurrent retirements, re-drives, and stale acquisitions for one image must
// never race the chain-admission state nor stack chains, and the image is removed
// exactly once once it goes idle. The removal is observed through the attempt
// seam (never a sleep).
func TestRetireImageConcurrentDrivesAndReDrives(t *testing.T) {
	exec := &blockingExecutor{referenced: map[string]bool{retiredImage: true}}
	r := newRetirementRunner(t, exec, testutil.DiscardLogger())
	r.imageCleanupRetryDelays = []time.Duration{time.Millisecond, time.Millisecond}

	removedSignal := make(chan struct{})
	var once sync.Once
	r.imageCleanupAttemptDone = func(string) {
		if len(exec.removedImages()) > 0 {
			once.Do(func() { close(removedSignal) })
		}
	}

	const drivers = 8
	var wg sync.WaitGroup
	wg.Add(drivers)
	for i := 0; i < drivers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				r.RetireImage(retiredImage)
				holdImage(r, retiredImage)
				releaseImage(r, retiredImage)
				r.RetryDeferredImageRemovals()
			}
		}()
	}
	wg.Wait()

	// The container reference clears: exactly one removal must follow (no
	// duplicate chains, no double remove).
	exec.setReferenced(retiredImage, false)
	r.RetryDeferredImageRemovals()

	select {
	case <-removedSignal:
	case <-time.After(2 * time.Second):
		t.Fatal("the re-driven image was never removed")
	}
	if got := exec.removedImages(); len(got) != 1 || got[0] != retiredImage {
		t.Fatalf("removed images = %v, want exactly [%s]", got, retiredImage)
	}
}

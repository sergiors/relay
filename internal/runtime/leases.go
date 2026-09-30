package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// ErrImageRetiring is returned when a new independent reference to an image is
// requested after the image has been committed to removal. It is a retryable
// transitional state, not a failure: the image is being (or about to be)
// retired, so the caller should defer its use — a build/reconcile retries on a
// later pass, and the runner's removal loop retries removal.
var ErrImageRetiring = errors.New("runtime: image is being retired")

// ErrManagerShuttingDown is returned by image-retirement operations refused
// because the manager has begun shutting down. It is a terminal deferral for
// this manager instance, not a genuine failure: the image is left committed (or
// reusable) for the next boot's natural cleanup pass, and no Docker removal is
// issued against a client that is about to close. Callers treat it like
// ErrImageRetiring — a retryable transition — never a cleanup failure.
var ErrManagerShuttingDown = errors.New("runtime: manager is shutting down")

// ImageLease is one admitted reference to a Relay-owned image. It is the
// ownership token that closes the check-then-remove TOCTOU window: while any
// lease is held the image cannot be committed to removal, and once removal is
// committed the coordinator admits no NEW independent lease, so no use can slip
// between a reference check and ImageRemove.
//
// A lease is obtained from Manager.AcquireImageLease (a new independent
// reference) or by Share-ing an existing lease (authority for work that has
// already started). Release is idempotent and nil-safe.
//
// The nil lease is safe: Release does nothing, Share returns nil, and
// Image/context plumbing treats it as "no authority", so fake executors and
// hand-built Prepared values (tests) never need one.
type ImageLease struct {
	coord *imageCoordinator
	image string
	once  sync.Once
	// released mirrors once's completion under the coordinator lock, so a
	// concurrent Share is refused atomically with the reference decrement and
	// can never revive authority from a released lease.
	released atomic.Bool
}

// Image returns the image reference this lease pins ("" for a nil lease).
func (l *ImageLease) Image() string {
	if l == nil {
		return ""
	}
	return l.image
}

// Entitles reports whether this lease is a LIVE admitted reference to exactly
// image, so a caller may treat it as that image's admission entitlement. It is
// the predicate every "use the lease carried on ctx" path must consult before
// relying on it: the lease must be non-nil, must not have been released by its
// holder, and must pin exactly image.
//
// A nil lease, a lease pinning a DIFFERENT image, and a lease the holder already
// released all report false. A caller that needs an entitlement and is handed a
// lease that does not entitle the image must acquire a FRESH independent lease
// (which a retirement in progress rejects with ErrImageRetiring) instead of
// trusting a token that no longer protects the image — otherwise the use would
// slip past the retirement gate the lease exists to close.
func (l *ImageLease) Entitles(image string) bool {
	if l == nil || l.coord == nil || image == "" || l.released.Load() {
		return false
	}
	return l.image == image
}

// Release drops this reference. It is idempotent and nil-safe. When the last
// reference to a retiring image is dropped, the coordinator signals the removal
// waiter.
func (l *ImageLease) Release() {
	if l == nil || l.coord == nil {
		return
	}
	l.once.Do(func() { l.coord.releaseLease(l) })
}

// Share returns an ADDITIONAL lease for work that has already started under
// this lease, such as a registry snapshot pinning a published image or a
// service request handing authority to its StartService child. Sharing is
// deliberately allowed even while the image is retiring: the work was admitted
// before retirement, so it must be able to complete. The shared lease must be
// released independently.
//
// A released or nil lease shares nothing (returns nil). The check and the
// reference increment are atomic with the coordinator lock, so a share can
// never race a release and extend the image's lifetime past the drain point.
//
// A shared lease belongs to the SAME coordinator as its source. A caller
// deciding whether a carried lease entitles an image it is about to use must
// check both Entitles (the exact image, still live) and that the lease's
// coordinator is its own; Manager.admitLease is that decision point.
func (l *ImageLease) Share() *ImageLease {
	if l == nil || l.coord == nil {
		return nil
	}
	return l.coord.shareLease(l)
}

// imageCoordinator is the single production ownership authority for Relay-owned
// images: every admitted reference to an image is a lease, and removal commits
// the image to retirement, which rejects new leases and waits for the admitted
// ones to drain before ImageRemove runs. A successful removal drops the image's
// state; a failed removal clears the retirement gate so the image stays
// retryable and reusable.
//
// It is ALSO the manager's shutdown gate for image removal: CloseContext calls
// beginShutdown, after which beginRemoval refuses every new retirement (so a
// runner async cleanup that fires after lifecycle cancellation cannot start a
// removal) and cancellation of shutdownCtx wakes every removal blocked on a
// drain so it aborts instead of proceeding to ImageRemove. waitRetirements joins
// every removal already in progress, so the Docker client is never closed under
// a live removal.
type imageCoordinator struct {
	mu sync.Mutex
	// refs counts admitted leases per image.
	refs map[string]int
	// retiring holds, per image, the wait state of an in-progress retirement.
	// Its presence is the "no new independent leases" gate.
	retiring map[string]*retireWait
	// shuttingDown is set once by beginShutdown. It is the removal gate: no new
	// removal may begin after it is set.
	shuttingDown bool
	// shutdownCtx is cancelled by beginShutdown. Every removal operation derives
	// its context from it (see Manager.removalContext), so shutdown cancels an
	// in-flight Docker removal even for a Manager constructed directly by tests
	// with no lifecycle context. It is created lazily so a zero-valued
	// coordinator still works.
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
	// retireOps counts OWNED removal operations from beginRemoval to
	// finishRemoval, and retireIdle is closed whenever it reaches zero, so
	// waitRetirements can join them before the Docker client closes. Both are
	// guarded by mu: beginRemoval registers under the same lock that checks
	// shuttingDown, so waitRetirements can never miss an operation that had
	// already started.
	retireOps  int
	retireIdle chan struct{}
	// hooks is the test-only observability seam. Production never installs it;
	// a race test uses it to synchronize on exact state transitions instead of
	// polling or sleeping. It is read/written under mu; every callback runs
	// outside c.mu and must not block.
	hooks *coordinatorHooks
}

// coordinatorHooks is the test-only observability seam for the image
// coordinator (see imageCoordinator.hooks). Each callback fires at the exact
// transition named, on the acting goroutine, outside c.mu.
type coordinatorHooks struct {
	// retireEntered fires when beginRemoval commits a NEW retirement (the
	// caller owns removal), with the image reference. It lets a test wait for
	// the gate to be established deterministically.
	retireEntered func(image string)
	// waitEntered fires once waitRetirements has snapshotted the join channel
	// and is about to block on it. It lets a test prove a join is parked (rather
	// than already returned) without a delay-based assertion.
	waitEntered func()
}

// retireWait is one image's in-progress retirement. ch is closed once the last
// admitted lease is released (or immediately when none are held).
type retireWait struct {
	ch     chan struct{}
	closed bool
}

func newImageCoordinator() *imageCoordinator {
	c := &imageCoordinator{
		refs:       map[string]int{},
		retiring:   map[string]*retireWait{},
		retireIdle: make(chan struct{}),
	}
	c.shutdownCtx, c.shutdownCancel = context.WithCancel(context.Background())
	close(c.retireIdle)
	return c
}

// lazyInit makes a zero-valued coordinator usable; it supports Managers
// constructed directly by tests. It must be called with c.mu held.
func (c *imageCoordinator) lazyInit() {
	if c.refs == nil {
		c.refs = map[string]int{}
	}
	if c.retiring == nil {
		c.retiring = map[string]*retireWait{}
	}
	if c.shutdownCtx == nil {
		c.shutdownCtx, c.shutdownCancel = context.WithCancel(context.Background())
	}
	if c.retireIdle == nil {
		c.retireIdle = make(chan struct{})
		close(c.retireIdle)
	}
}

// acquire admits one new independent reference to image, or reports
// ErrImageRetiring when the image has been committed to removal. An empty image
// is not leased (nil, nil): a function with no runtime-backed image has nothing
// to pin.
func (c *imageCoordinator) acquire(image string) (*ImageLease, error) {
	if image == "" {
		return nil, nil
	}
	c.mu.Lock()
	c.lazyInit()
	if _, ok := c.retiring[image]; ok {
		c.mu.Unlock()
		return nil, fmt.Errorf("acquire image lease: %w", ErrImageRetiring)
	}
	c.refs[image]++
	c.mu.Unlock()
	return &ImageLease{coord: c, image: image}, nil
}

// shareLease admits one additional reference for already-started work,
// atomically with respect to release. It never consults the retirement gate:
// the work holding the original lease was admitted before retirement and must
// be able to hand off authority. It returns nil when the source lease has
// already been released or has no outstanding reference, so a share can never
// be the count that keeps a retirement from draining.
func (c *imageCoordinator) shareLease(src *ImageLease) *ImageLease {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lazyInit()
	if src.released.Load() || c.refs[src.image] <= 0 {
		return nil
	}
	c.refs[src.image]++
	return &ImageLease{coord: c, image: src.image}
}

// releaseLease drops one admitted reference, marking the lease released under
// the same lock that decrements the count, so a concurrent Share observes the
// release atomically. When the count reaches zero and the image is retiring, it
// closes the retirement waiter so the remover proceeds.
func (c *imageCoordinator) releaseLease(l *ImageLease) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lazyInit()
	l.released.Store(true)
	if n := c.refs[l.image]; n > 1 {
		c.refs[l.image] = n - 1
		return
	}
	delete(c.refs, l.image)
	if w, ok := c.retiring[l.image]; ok && !w.closed {
		w.closed = true
		close(w.ch)
	}
}

// beginRetire commits image to retirement. owner reports whether the caller
// owns the removal: only the first committer may call finishRetire and actually
// remove. A duplicate committer receives (channel, false) and must treat the
// image as still in use (retry later) rather than racing a second removal.
//
// The returned channel is closed once every admitted lease has drained (or
// immediately when none are held). It is only meaningful when owner is true.
//
// beginRetire is the lower-level commit primitive: it performs no shutdown gate
// and registers no operation to join. Production removal paths use beginRemoval
// instead; beginRetire remains for tests that simulate an in-progress retirement
// directly.
func (c *imageCoordinator) beginRetire(image string) (<-chan struct{}, bool) {
	if image == "" {
		return closedSignal(), false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lazyInit()
	return c.beginRetireLocked(image)
}

// beginRetireLocked is beginRetire's implementation; the caller must hold
// c.mu (and have called lazyInit).
func (c *imageCoordinator) beginRetireLocked(image string) (<-chan struct{}, bool) {
	if w, ok := c.retiring[image]; ok {
		// A retirement is already in progress (or committed): report it as
		// still in use so this caller retries rather than racing a removal.
		return w.ch, false
	}
	w := &retireWait{ch: make(chan struct{})}
	c.retiring[image] = w
	if c.refs[image] == 0 {
		w.closed = true
		close(w.ch)
	}
	return w.ch, true
}

// beginRemoval commits image to retirement for an OWNED removal operation and
// registers the operation so shutdown can join it. It is the production entry
// point behind Manager.retireOwned and dependency GC.
//
// It differs from beginRetire in two ways that make removal lifecycle-owned:
// once the manager has begun shutting down it REFUSES (ErrManagerShuttingDown),
// so a runner async cleanup that fires after lifecycle cancellation can never
// start a Docker removal; and when it owns the removal it registers the
// operation in retireOps BEFORE releasing the lock, so waitRetirements — which
// runs after beginShutdown set the same lock's gate — is guaranteed to observe
// (and join) every removal that had already begun.
//
// drained closes once every admitted lease drains (or immediately when none are
// held) and is meaningful only when owner is true.
func (c *imageCoordinator) beginRemoval(image string) (drained <-chan struct{}, owner bool, err error) {
	if image == "" {
		return closedSignal(), false, nil
	}
	c.mu.Lock()
	c.lazyInit()
	if c.shuttingDown {
		c.mu.Unlock()
		return closedSignal(), false, fmt.Errorf("remove image %s: %w", image, ErrManagerShuttingDown)
	}
	drained, owner = c.beginRetireLocked(image)
	if owner {
		c.enterRemovalOpLocked()
	}
	var entered func(string)
	if owner && c.hooks != nil {
		entered = c.hooks.retireEntered
	}
	c.mu.Unlock()
	if entered != nil {
		// Test-only: signal the exact instant this removal owns the gate. The
		// callback runs outside c.mu so it may synchronize without deadlocking.
		entered(image)
	}
	return drained, owner, nil
}

// enterRemovalOpLocked records one in-flight removal-related operation. The
// caller must hold c.mu and must already have rejected the operation when
// shuttingDown is set. retireIdle is replaced with a fresh open channel whenever
// the count rises from zero, so waitRetirements can block on a stable snapshot.
func (c *imageCoordinator) enterRemovalOpLocked() {
	if c.retireOps == 0 {
		c.retireIdle = make(chan struct{})
	}
	c.retireOps++
}

// leaveRemovalOpLocked releases one in-flight removal-related operation and
// closes retireIdle once the count returns to zero. The caller must hold c.mu.
func (c *imageCoordinator) leaveRemovalOpLocked() {
	if c.retireOps > 0 {
		c.retireOps--
	}
	if c.retireOps == 0 {
		select {
		case <-c.retireIdle:
			// Already closed: leave it closed.
		default:
			close(c.retireIdle)
		}
	}
}

// beginRemovalOp registers a removal-RELATED operation (a removal-guard
// container/image listing) with the shutdown gate, WITHOUT committing any image
// to retirement. It reports ErrManagerShuttingDown once shutdown has begun, so a
// runner async cleanup that fires after lifecycle cancellation can never issue a
// listing against a closing client. The caller must pair it with finishRemovalOp.
func (c *imageCoordinator) beginRemovalOp() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lazyInit()
	if c.shuttingDown {
		return ErrManagerShuttingDown
	}
	c.enterRemovalOpLocked()
	return nil
}

// finishRemovalOp releases a registration made by beginRemovalOp. It is the
// counterpart of beginRemovalOp for non-retirement removal operations.
func (c *imageCoordinator) finishRemovalOp() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lazyInit()
	c.leaveRemovalOpLocked()
}

// finishRetire concludes the caller's owned retirement. On success the image's
// state is dropped entirely (it is gone; a later lease/build starts clean). On
// failure the retirement gate is cleared so the image remains usable and a
// later natural cleanup pass can retry the removal.
func (c *imageCoordinator) finishRetire(image string, success bool) {
	if image == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lazyInit()
	delete(c.retiring, image)
	if success {
		delete(c.refs, image)
	}
}

// finishRemoval concludes an operation begun with beginRemoval: it clears the
// retirement state (retryable on failure) and releases the operation's join
// registration exactly once.
func (c *imageCoordinator) finishRemoval(image string, success bool) {
	c.finishRetire(image, success)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lazyInit()
	c.leaveRemovalOpLocked()
}

// beginShutdown closes the removal gate and cancels the coordinator's shutdown
// context, so no new removal can begin and every in-flight removal's derived
// context (see Manager.removalContext) is cancelled. It is idempotent and does
// NOT wait for in-flight operations; waitRetirements performs the join after
// the gate is closed.
func (c *imageCoordinator) beginShutdown() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lazyInit()
	if c.shuttingDown {
		return
	}
	c.shuttingDown = true
	if c.shutdownCancel != nil {
		c.shutdownCancel()
	}
}

// shutdownContext returns the coordinator's shutdown context, cancelled by
// beginShutdown. It is always non-nil and is the manager-shutdown half of every
// removal context.
func (c *imageCoordinator) shutdownContext() context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lazyInit()
	return c.shutdownCtx
}

// isShuttingDown reports whether beginShutdown has closed the removal gate. It
// lets a removal boundary classify a context error that raced shutdown as a
// terminal ErrManagerShuttingDown rather than an opaque cancellation.
func (c *imageCoordinator) isShuttingDown() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lazyInit()
	return c.shuttingDown
}

// shutdownErr converts err into ErrManagerShuttingDown when the manager is
// shutting down and the caller's own ctx did NOT cause the error (so a genuine
// caller deadline is never masked). It returns err unchanged otherwise. It is
// the single classification used by every removal boundary, so a Docker context
// error that races shutdown is reported as the terminal deferral the runner
// expects, not a spurious cleanup failure.
func (c *imageCoordinator) shutdownErr(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return err
	}
	if c.isShuttingDown() {
		return fmt.Errorf("%w: %w", ErrManagerShuttingDown, err)
	}
	return err
}

// waitRetirements joins every in-progress OWNED removal operation, or returns
// when ctx is done, whichever first. Because beginShutdown cancels each
// operation's derived context, the join is prompt: an operation blocked on a
// lease drain or inside a Docker call observes the cancellation, concludes
// (clearing its gate so the image is retryable next boot), and decrements its
// registration. This is the barrier that lets CloseContext close the Docker
// client only after no removal can still issue a request against it.
func (c *imageCoordinator) waitRetirements(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.Lock()
	c.lazyInit()
	idle := c.retireIdle
	var entered func()
	if c.hooks != nil {
		entered = c.hooks.waitEntered
	}
	c.mu.Unlock()
	if entered != nil {
		// Test-only: the join channel is snapshotted and this call is about to
		// block (or return immediately if already idle).
		entered()
	}
	select {
	case <-idle:
	case <-ctx.Done():
	}
}

// setTestHooks installs (or clears, with nil) the test-only observability seam.
// It is unexported and unused in production; race tests use it to synchronize on
// exact coordinator transitions instead of polling state or sleeping.
func (c *imageCoordinator) setTestHooks(h *coordinatorHooks) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lazyInit()
	c.hooks = h
}

// retired reports whether image is currently committed to removal. It is a
// diagnostic/coordination helper (never a substitute for acquire's atomic
// admission).
func (c *imageCoordinator) retired(image string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lazyInit()
	_, ok := c.retiring[image]
	return ok
}

// count returns the number of admitted references currently held for image. It
// is a diagnostic helper only.
func (c *imageCoordinator) count(image string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lazyInit()
	return c.refs[image]
}

// reset drops every reference and retirement wait. It is called only at manager
// shutdown, AFTER beginShutdown rejected new removals and waitRetirements joined
// every in-progress one. It exists to release any lease a caller forgot (and any
// test-simulated retirement state) so a Manager's memory does not outlive it.
//
// It deliberately does NOT close retirement waiters. Closing them would wake a
// remover blocked on a drain into executing ImageRemove against a client that is
// about to close — the exact race this shutdown sequence eliminates. With the
// operations already joined, no waiter can still be observing these channels.
func (c *imageCoordinator) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lazyInit()
	c.refs = map[string]int{}
	c.retiring = map[string]*retireWait{}
}

// closedSignal returns an already-closed channel for the no-op retirement case
// (an empty image has nothing to drain).
func closedSignal() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// imageLeaseCtxKey is the context key carrying an admitted image lease to
// Manager.Execute, so an invocation admitted before retirement still executes
// under that admitted authority rather than acquiring a fresh (rejected) lease.
type imageLeaseCtxKey struct{}

// WithImageLease attaches an admitted image lease to ctx. A nil lease leaves
// ctx unchanged, so callers with no lease (fake executors, no-runtime
// functions) remain transparent.
func WithImageLease(ctx context.Context, lease *ImageLease) context.Context {
	if lease == nil {
		return ctx
	}
	return context.WithValue(ctx, imageLeaseCtxKey{}, lease)
}

// ImageLeaseFrom returns the admitted image lease carried by ctx, or nil.
func ImageLeaseFrom(ctx context.Context) *ImageLease {
	if ctx == nil {
		return nil
	}
	lease, _ := ctx.Value(imageLeaseCtxKey{}).(*ImageLease)
	return lease
}

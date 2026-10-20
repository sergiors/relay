package runner

import (
	"context"
	"errors"
	"sync"
	"time"

	"relay/internal/runtime"
)

// ImageCleaner is the subset of the runtime Manager that image retirement
// needs. It is a small interface so the runner can retire superseded app
// images without depending on the runtime package concretely; test fakes that
// do not implement it simply yield a nil cleaner (no retirement).
// ContainerInvalidator is the optional capability of the runtime executor
// that image retirement needs on top of ImageCleaner: invalidate pooled
// execution containers for a retired image so the image's
// container-reference guard clears promptly. Implementations must never
// block on an in-flight invocation: Manager.InvalidateImage discards idle
// containers immediately and retires busy ones (discarded on release) without
// waiting for them.
type ContainerInvalidator interface {
	// InvalidateImage discards any cached execution container running the
	// given image.
	InvalidateImage(image string)
}

type ImageCleaner interface {
	// RemoveImage removes a single relay-owned image, treating an already-gone
	// image as success.
	RemoveImage(ctx context.Context, image string) error
	// ImageReferencedByManagedContainer reports whether any relay-owned container
	// (event, schedule, or service) still references the image via its
	// relay.image label. It lets the runner confirm no relay-owned container still
	// references an image before removing it, and to decide (conservatively) how
	// to interpret a RemoveImage failure.
	ImageReferencedByManagedContainer(ctx context.Context, image string) (bool, error)
	// AppImageTags lists every local image tag (full references) belonging
	// to an app's repository, so the runner can retire each version with
	// in-flight safety.
	AppImageTags(ctx context.Context, name string) ([]string, error)
	// CleanupUnusedDependencies removes managed dependency images no managed
	// app image references anymore. Lifecycle-driven: call it after a
	// managed app image was successfully removed, so a dependency layer
	// whose last referencing app version just disappeared is pruned. It is
	// best-effort and must not affect the outcome of the removal that preceded
	// it.
	CleanupUnusedDependencies(ctx context.Context) (int, error)
}

// defaultImageCleanupRetryDelays is the production backoff schedule for image
// removal retries: a ~60s horizon across 5 attempts before deferring to a later
// natural cleanup pass.
var defaultImageCleanupRetryDelays = []time.Duration{
	2 * time.Second,
	4 * time.Second,
	8 * time.Second,
	16 * time.Second,
	30 * time.Second,
}

// imageRemovedIdle is the onIdle hook: a retired image just became idle, so
// remove it off the event path through the same coalescing admission every other
// drive uses.
func (r *Runner) imageRemovedIdle(image string) {
	r.driveImageRemoval(image)
}

// resolver returns the runner's resolved image cleaner, or nil when the executor
// does not implement retirement (tests, unavailable-only runners). It is resolved
// once and cached; resolution scanning the registry is cheap and safe.
func (r *Runner) resolver() ImageCleaner {
	r.cleanerOnce.Do(func() {
		for _, pf := range r.reg.snapshot() {
			if cleaner, ok := pf.executor.(ImageCleaner); ok {
				r.cleaner = cleaner
				return
			}
		}
	})
	return r.cleaner
}

// invalidatorResolver returns the runner's resolved container invalidator, or
// nil when the executor does not implement retirement (tests, unavailable-only
// runners). Resolution scans the registry snapshot once and caches.
func (r *Runner) invalidatorResolver() ContainerInvalidator {
	r.invalidatorOnce.Do(func() {
		for _, pf := range r.reg.snapshot() {
			if invalidator, ok := pf.executor.(ContainerInvalidator); ok {
				r.invalidator = invalidator
				return
			}
		}
	})
	return r.invalidator
}

// cleanupTimeout bounds every docker image-removal call made off the event path
// (see removeImageAsync), so a wedged daemon cannot hold a goroutine or block
// shutdown indefinitely. It is deliberately short: retirement is best-effort
// cleanup, never on the critical path.
const cleanupTimeout = 10 * time.Second

// ImageInUse reports whether any execution is currently holding a reference to
// the given image (in-flight Handle). It is the guard the reconciler consults
// before removing a superseded image, and Release uses it to know when a retired
// image becomes removable.
func (r *Runner) ImageInUse(image string) bool {
	return r.refs.inUse(image)
}

// RetireImage retires the given image reference so it can be removed once it is
// no longer in use. When nothing holds it now, it is removed immediately off the
// event path; otherwise it is marked pending and removed once the last in-flight
// execution releases it (see imageRefCounter.release → imageRemovedIdle). A nil
// cleaner (no ImageCleaner executor) makes this a no-op, which is correct for
// test fakes and unavailable-only runners.
//
// Retiring an image whose earlier removal chain gave up (its bounded retries
// were exhausted while a relay-owned container still referenced it, or the
// removal failed) RE-DRIVES removal instead of being suppressed forever: the
// deferred retirement ownership the chain retained is what lets a later
// lifecycle event remove the image once the blocking reference clears. Chain
// admission is coalesced per image, so a retirement arriving while a chain is
// already retrying starts nothing new (see imageRefCounter.beginCleanup).
func (r *Runner) RetireImage(image string) {
	if image == "" {
		return
	}
	if r.resolver() == nil {
		// No ImageCleaner executor (fake/unavailable-only runner): nothing can
		// be removed, so no retirement state is registered either.
		return
	}
	// Register the retirement (idempotent). The marker is what makes a release
	// of the image's last in-flight execution hand removal to the runner; chain
	// admission below is what coalesces concurrent drives, so re-retiring a
	// deferred image re-drives removal rather than short-circuiting.
	r.refs.recordRetired(image)
	// Invalidate pooled execution containers running this image BEFORE any
	// removal attempt: the discard clears the image's relay-owned-container
	// reference promptly, so ErrImageInUse / the reference guard does not wait
	// for a retry pass. InvalidateImage is strictly non-blocking manager-side
	// (idle containers are discarded now, busy ones retired until release), so
	// retirement never stalls on an in-flight invocation.
	if inv := r.invalidatorResolver(); inv != nil {
		inv.InvalidateImage(image)
	}
	if !r.ImageInUse(image) {
		// Idle right now: remove immediately instead of waiting for a release
		// that will only ever fire if a new execution picks this image up.
		r.driveImageRemoval(image)
	}
}

// RetryDeferredImageRemovals re-drives removal for every image whose retirement
// chain gave up, either because a bounded guard-skip retry or a genuine removal
// failure deferred it, or because an attempt concluded while the image was still
// in use. It is the re-drive hook for RETAINED retirement ownership: it is called
// on an existing lifecycle event at which service or reference state may have
// changed — a persistent-service convergence completing, or the reconciler's
// periodic audit — so an image left behind by its initial bounded retry window is
// still removed once the reference clears.
//
// Admission is coalesced per image (a chain already in flight is left alone) and
// idempotent (a no-op when nothing is deferred). Every attempt rechecks the
// served-image signal, the in-flight refcount (ImageInUse), the relay-owned
// container reference guard, and — through the cleaner's RemoveImage — the image
// lease coordinator, so a re-drive can never remove an image that became in use
// again. It never blocks the caller: each admission starts (or resumes) an async
// chain. A nil cleaner is a no-op.
//
// A candidate whose image is currently in use is left deferred WITHOUT admitting
// a chain: the executing use's release re-drives removal through the ordinary
// idle handoff (release → onIdle), and if that handoff is coalesced against an
// in-flight chain the candidate simply survives to the next re-drive. So the
// skip is a pure state read that cannot lose a removal, and it keeps a periodic
// hook from spawning chains for images that provably cannot be cleaned yet.
func (r *Runner) RetryDeferredImageRemovals() {
	if r.resolver() == nil {
		return
	}
	for _, image := range r.refs.deferredImages() {
		if r.ImageInUse(image) {
			continue
		}
		r.driveImageRemoval(image)
	}
}

// driveImageRemoval is the single admission point for every asynchronous image
// removal chain (the first retirement, the release-driven idle handoff, and the
// deferred re-drive). It admits one chain per image: a drive for an image whose
// chain is already retrying is coalesced to a no-op, so concurrent retirements
// and re-drives can never stack chains or goroutines per image.
func (r *Runner) driveImageRemoval(image string) {
	if r.resolver() == nil {
		return
	}
	if !r.refs.beginCleanup(image) {
		return
	}
	r.removeImageAsync(image)
}

// RemoveAppImages retires every local version of an app's images so
// that, once idle, each is removed. It is the app-removal path: the
// reconciler calls it when an app directory vanishes, and all of its version
// images become garbage. A nil cleaner makes this a no-op.
func (r *Runner) RemoveAppImages(name string) {
	cleaner := r.resolver()
	if cleaner == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	tags, err := cleaner.AppImageTags(ctx, name)
	if err != nil {
		// Manager shutdown is a terminal deferral for this worker: the images are
		// left for the next boot's natural cleanup pass. It is not a genuine
		// failure, so it is a debug deferral, not a Warn.
		if errors.Is(err, runtime.ErrManagerShuttingDown) {
			r.log.Debug("Image cleanup: manager shutting down; deferring app image listing", "app", name)
			return
		}
		r.log.Warn("Image cleanup: list app versions failed", "app", name, "error", err)
		return
	}
	for _, tag := range tags {
		r.RetireImage(tag)
	}
}

// imageCleanupOutcome classifies the state of an asynchronous image-removal
// attempt, so the chain knows whether it still owns removal for the image or has
// reached a terminal state (and, if terminal, whether retirement ownership is
// retained for a later re-drive).
type imageCleanupOutcome int

const (
	// imageCleanupRescheduled: another bounded attempt was armed; the chain
	// keeps owning removal for the image.
	imageCleanupRescheduled imageCleanupOutcome = iota
	// imageCleanupRemoved: the image was removed; every per-image record is
	// dropped.
	imageCleanupRemoved
	// imageCleanupDeferred: the chain gave up — a bounded guard-skip retry was
	// exhausted, a genuine removal failed, or the image was still in use. In
	// every case retirement ownership is RETAINED (the image is recorded as
	// deferred) so a later lifecycle event re-drives removal once the blocking
	// reference may have cleared.
	imageCleanupDeferred
	// imageCleanupAbandoned: the manager began shutting down, so this attempt
	// cannot decide anything. The chain admission is dropped and nothing is
	// retained: a shutdown is a terminal deferral for this worker, and the image
	// is retried by the next boot's natural cleanup pass. (An image that is
	// merely still in use is NOT abandoned — see imageCleanupDeferred — because a
	// concurrent idle handoff can be coalesced against this chain.)
	imageCleanupAbandoned
	// imageCleanupCancelled: the registry serves the image again, so the
	// retirement is VOID — the image is a current generation, not garbage. Every
	// per-image record is dropped so no re-drive ever retries it.
	imageCleanupCancelled
)

// removeImageAsync removes a retired image off the event path so a docker round
// trip can never add latency (or failure) to Handle. Removal is gated by two
// independent guards: the in-flight refcount (an execution may have (re)claimed
// the image after it was retired) and the relay-owned container reference check
// (a persistent service container may still reference it). When either guard
// holds, removal is NOT abandoned: the chain retains deferred retirement
// ownership for the image — recording it as deferred when it gives up — so an
// existing lifecycle event (or the execution's own release) re-drives removal
// (see RetryDeferredImageRemovals). A nil cleaner is a no-op.
func (r *Runner) removeImageAsync(image string) {
	cleaner := r.resolver()
	if cleaner == nil {
		// Defensive: admission is owned by driveImageRemoval, which never calls
		// this without a resolved cleaner. Drop the admission rather than
		// stranding it, so the image stays re-drivable.
		r.refs.abandonCleanup(image)
		return
	}
	// Snapshot the retry backoff once so a goroutine that outlives a test
	// (which may set r.imageCleanupRetryDelays) never races a later field write.
	// attempt is the retry counter shared across a single guard-skip chain; the
	// chain owns the image's removal until it reaches a terminal outcome, and
	// admission (imageRefCounter.beginCleanup) ensures at most one chain exists
	// per image, so goroutine count stays bounded.
	delays := append([]time.Duration(nil), r.imageCleanupRetryDelays...)
	attempt := 0
	r.retryImageCleanupAttempt(image, cleaner, delays, &attempt)
}

// retryImageCleanupAttempt performs one removal attempt off the event path and
// applies its outcome to the image's chain admission: a removed image drops
// every record, a deferred image (a guard-skip exhausted, a genuine removal
// failure, or an image still in use) RETAINS deferred retirement ownership for a
// later re-drive, a cancelled image (served again by the registry) drops every
// record, and an abandoned chain (manager shutdown) drops only its admission. A
// rescheduled attempt (see skipAndRetryImageCleanup) keeps the chain admitted and
// is the one outcome that starts no follow-up attempt here (the armed timer
// does).
func (r *Runner) retryImageCleanupAttempt(image string, cleaner ImageCleaner, delays []time.Duration, attempt *int) {
	go func() {
		// The attempt reached a classification (removed, deferred, abandoned,
		// cancelled, or rescheduled) when this goroutine returns. Signal it to
		// the test-only seam so a test can assert the decision without sleeping.
		if done := r.imageCleanupAttemptDone; done != nil {
			defer done(image)
		}
		switch r.imageCleanupOnce(image, cleaner, delays, attempt) {
		case imageCleanupRemoved:
			r.refs.finishCleanup(image, true)
		case imageCleanupDeferred:
			r.refs.finishCleanup(image, false)
		case imageCleanupCancelled:
			r.refs.clearRetirement(image)
		case imageCleanupAbandoned:
			r.refs.abandonCleanup(image)
		}
	}()
}

// imageCleanupOnce performs one guarded removal attempt and reports its outcome.
// It rechecks, on every attempt, whether the registry now serves the image (the
// retirement is void then), the in-flight refcount, and the relay-owned
// container reference guard; the image lease coordinator is rechecked inside the
// cleaner's RemoveImage (its drain), so a re-drive can never remove an image
// that became in-use again or is still pinned by an admitted lease.
func (r *Runner) imageCleanupOnce(image string, cleaner ImageCleaner, delays []time.Duration, attempt *int) imageCleanupOutcome {
	// The registry serves this image again: it is the CURRENT generation, so a
	// stale retirement for it is void. Cancel (dropping every record) BEFORE the
	// in-use check, so a served image is never removed and no periodic re-drive
	// keeps retrying it.
	if r.reg != nil && r.reg.ServesImage(image) {
		r.log.Debug("Image cleanup: image is served again; cancelling retirement", "image", image)
		return imageCleanupCancelled
	}
	// A stale pre-swap snapshot claimed the image (or an execution is running):
	// do not remove it now, but RETAIN the retirement as a DEFERRED candidate
	// rather than dropping the admission. The execution's release normally
	// re-drives removal immediately (release → onIdle → driveImageRemoval), but
	// that idle transition can race THIS chain: the release fires onIdle while
	// this admission is still active, so its drive coalesces and is dropped — and
	// this attempt then concludes. Recording deferred here is what keeps the
	// removal owned for the periodic audit / service-completion re-drive, so that
	// lost wakeup cannot strand a superseded image until a restart.
	//
	// It is deliberately AFTER the served-image check: a generation the registry
	// currently serves is cancelled above, never re-armed as deferred.
	if r.ImageInUse(image) {
		return imageCleanupDeferred
	}
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	referenced, err := cleaner.ImageReferencedByManagedContainer(ctx, image)
	if err != nil {
		// Manager shutdown is a terminal deferral for this worker: the image
		// is left for the next boot's natural cleanup pass, and retrying
		// would only hammer a closing Docker client. It is NOT a genuine
		// failure, so it is a debug deferral, not a Warn, and it never
		// reschedules.
		if errors.Is(err, runtime.ErrManagerShuttingDown) {
			r.log.Debug("Image cleanup: manager shutting down; deferring to next boot", "image", image)
			return imageCleanupAbandoned
		}
		// Never remove on unknown state: treat the reference check failure as
		// conservatively referenced and retry.
		return r.skipAndRetryImageCleanup(image, cleaner, delays, "reference check failed", attempt)
	}
	if referenced {
		return r.skipAndRetryImageCleanup(image, cleaner, delays, "relay-owned container references it", attempt)
	}

	if err := cleaner.RemoveImage(ctx, image); err != nil {
		// Manager shutdown mid-attempt: terminal deferral, never a second
		// removal against a closing client, never rescheduled.
		if errors.Is(err, runtime.ErrManagerShuttingDown) {
			r.log.Debug("Image cleanup: manager shutting down; deferring to next boot", "image", image)
			return imageCleanupAbandoned
		}
		// A removal that fails while a container references the image is a
		// guard-skip (the daemon refused, or the reference appeared mid-call),
		// not a genuine failure. Re-consult the reference to classify the
		// error: referenced -> debug skip + retry; otherwise the genuine Warn.
		if again, aerr := cleaner.ImageReferencedByManagedContainer(ctx, image); aerr == nil && again {
			return r.skipAndRetryImageCleanup(image, cleaner, delays, "relay-owned container references it", attempt)
		} else if errors.Is(aerr, runtime.ErrManagerShuttingDown) {
			r.log.Debug("Image cleanup: manager shutting down; deferring to next boot", "image", image)
			return imageCleanupAbandoned
		}
		// A retirement still draining an admitted lease (a registry snapshot
		// or a service pass) or a drain bound that expired is a retryable
		// transitional state, not a failure: defer and retry rather than
		// warning. The runtime's lease coordinator keeps the image committed
		// to removal, so the next attempt resumes the drain.
		if errors.Is(err, runtime.ErrImageRetiring) || errors.Is(err, context.DeadlineExceeded) {
			return r.skipAndRetryImageCleanup(image, cleaner, delays, "image retirement still draining", attempt)
		}
		// A genuine removal failure: retain deferred retirement ownership so a
		// later lifecycle event re-drives it instead of the image being left
		// behind until a boot sweep.
		r.log.Warn("Image cleanup: remove retired failed", "image", image, "error", err)
		return imageCleanupDeferred
	}

	// The app image was successfully removed. This is the lifecycle
	// moment a dependency layer may become orphaned: the removed app
	// image was the only reference to its dependency, so run dependency GC
	// now to prune any layer no managed app image references anymore.
	// It is best-effort: a failure here (a genuine daemon error) is worth a
	// Warn — it retries on the next natural pass after the next app-image
	// removal or at the next startup — and must NOT affect the outcome of the
	// removal that already succeeded. The app image's own removal (and
	// this GC) both proceed off the event path in this same goroutine; the GC
	// itself is lease-gated, so it cannot remove a layer a concurrent
	// preparation is consuming.
	if _, err := cleaner.CleanupUnusedDependencies(ctx); err != nil {
		// Manager shutdown is a terminal deferral for this worker (the next
		// boot's startup GC prunes the layer), not a failure — and it must not
		// turn the successful removal into a retained retirement.
		if errors.Is(err, runtime.ErrManagerShuttingDown) {
			r.log.Debug("Image cleanup: manager shutting down; deferring dependency GC", "image", image)
			return imageCleanupRemoved
		}
		r.log.Warn("Dependency image cleanup failed", "error", err)
	}
	return imageCleanupRemoved
}

// skipAndRetryImageCleanup logs a debug-level skip and schedules the next
// removal attempt with bounded backoff (delays is the per-chain snapshot),
// returning imageCleanupRescheduled. On the final attempt it logs a single Info
// and returns imageCleanupDeferred: the image's deferred retirement ownership is
// retained so a later lifecycle event (a service convergence completing, the
// reconciler's periodic audit, a boot sweep, the next rebuild's retire, or
// app-removal retirement) re-drives removal. It is context-free and off the
// event path. cleaner is the chain's already-resolved cleaner, reused so a
// rescheduled attempt can never observe a different (or nil) cleaner.
func (r *Runner) skipAndRetryImageCleanup(image string, cleaner ImageCleaner, delays []time.Duration, reason string, attempt *int) imageCleanupOutcome {
	r.log.Debug("Image cleanup: image still in use; skipping", "image", image, "reason", reason)
	*attempt++
	if *attempt > len(delays) {
		r.log.Info("Image cleanup: image still in use after retries; deferring to a later cleanup pass", "image", image)
		return imageCleanupDeferred
	}
	delay := delays[*attempt-1]
	time.AfterFunc(delay, func() {
		r.retryImageCleanupAttempt(image, cleaner, delays, attempt)
	})
	return imageCleanupRescheduled
}

// toImage returns the image a prepared app executes, or "" when it is
// unavailable/nil so refcount and retirement stay nil-safe for fake and
// unavailable paths.
func toImage(pf *PreparedApp) string {
	if pf == nil || pf.prepared == nil {
		return ""
	}
	return pf.prepared.Image
}

// imageRefCounter tracks how many in-flight executions hold each image, which
// images have been retired (superseded) but can only be removed once idle, which
// images have an asynchronous removal chain in flight, and which images'
// retirement was deferred for a later re-drive.
//
// acquire/release bracket a single invocation: the count for prepared.Image is
// bumped on entry and dropped on return, so ImageInUse reports live executions
// even while a swap concurrently replaces the registry entry. RetireImage marks
// an image retired; when its count reaches zero, release triggers its
// asynchronous removal via the runner (which owns the cleaner). A retired image
// that is never in flight (count already zero at retire time) is removed
// immediately by RetireImage instead.
//
// A RETIREMENT IS NEVER REVOKED BY A USE. retire marks a SUPERSEDED image: a
// snapshot taken before the registry swap can still execute it (a stale use),
// and that must not strand the image. So retire persists across acquires and
// releases — every idle transition re-owns removal — and is cleared only by a
// successful removal (finishCleanup) or by the explicit cancellation signal that
// the registry serves the image again (clearRetirement). cleanupActive is the
// per-image chain admission (at most one chain owns an image's removal at a
// time, see beginCleanup) and deferred is the retained ownership of a chain that
// gave up.
type imageRefCounter struct {
	mu            sync.Mutex
	uses          map[string]int64
	retired       map[string]bool
	cleanupActive map[string]bool
	deferred      map[string]bool
	onIdle        func(image string)
}

// newImageRefCounter builds an empty counter whose onIdle hook fires removal.
func newImageRefCounter() *imageRefCounter {
	return &imageRefCounter{
		uses:          map[string]int64{},
		retired:       map[string]bool{},
		cleanupActive: map[string]bool{},
		deferred:      map[string]bool{},
	}
}

func (c *imageRefCounter) setOnIdle(fn func(string)) {
	c.mu.Lock()
	c.onIdle = fn
	c.mu.Unlock()
}

func (c *imageRefCounter) acquire(image string) {
	if image == "" {
		return
	}
	c.mu.Lock()
	c.uses[image]++
	// A USE never revokes a retirement. The registry may have already moved off
	// this image while a snapshot taken before the swap still executes it: that
	// use is stale, so the intent to remove the image must survive it (the next
	// idle transition re-drives removal — see release). A GENUINE re-activation
	// is signalled explicitly by the registry serving the image again, which the
	// cleanup chain observes and cancels on (see Registry.ServesImage →
	// clearRetirement); clearing retirement here would instead let a stale
	// snapshot strand a superseded image forever.
	c.mu.Unlock()
}

// release drops a reference and, when a retired image's count just reached zero,
// fires onIdle so the runner can remove it. Retirement is NOT cleared here: it
// stands until the image is actually removed or the registry serves it again, so
// every idle transition of a retired image re-owns removal. That is what makes a
// stale pre-swap use harmless: if it raced the cleanup attempt (which abandoned
// on seeing it in use), its release re-drives removal instead of the image being
// stranded.
func (c *imageRefCounter) release(image string) {
	if image == "" {
		return
	}
	c.mu.Lock()
	left := c.uses[image] - 1
	if left <= 0 {
		delete(c.uses, image)
		left = 0
	} else {
		// Persist the decrement. An unreleased counter is not merely a
		// diagnostic wart: ImageInUse reports true forever (so a retirement is
		// never handed to the cleaner once two uses overlapped) and the release's
		// idle transition is never observed. Every release must advance the
		// count exactly one step.
		c.uses[image] = left
	}
	// If the image is retired and just became idle, ownership of its removal
	// transfers to this release. The retirement intent is deliberately retained,
	// so a later idle transition re-drives removal if this one's chain defers or
	// abandons (a concurrent stale use claimed the image again).
	becameIdle := left == 0 && c.retired[image]
	onIdle := c.onIdle
	c.mu.Unlock()
	if becameIdle && onIdle != nil {
		onIdle(image)
	}
}

// inUse reports whether an execution currently holds the image.
func (c *imageRefCounter) inUse(image string) bool {
	if image == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.uses[image] > 0
}

// recordRetired marks image as retired (superseded), so a release of its last
// in-flight execution hands removal to the runner (see release → onIdle). It is
// idempotent and is NOT the chain-admission guard: beginCleanup owns that, so a
// repeat retirement of a deferred image still re-drives removal. Only a
// successful removal or the explicit activation signal (clearRetirement) ends
// it — never a use.
func (c *imageRefCounter) recordRetired(image string) {
	if image == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.retired[image] = true
}

// beginCleanup admits one asynchronous removal chain for image. It returns true
// when the caller now owns that chain (no chain was active), including the case
// of re-admitting an image whose earlier chain deferred: the deferred record is
// cleared and the image's removal is retried now. An image whose chain is
// already in flight returns false, so concurrent retirements and re-drives
// coalesce to the single admitted chain.
func (c *imageRefCounter) beginCleanup(image string) bool {
	if image == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cleanupActive[image] {
		return false
	}
	c.cleanupActive[image] = true
	delete(c.deferred, image)
	return true
}

// finishCleanup concludes image's admitted removal chain. removed=true drops
// every per-image record (the image is gone), so no stale retirement state
// survives a successful removal. removed=false retains deferred retirement
// ownership: the image is recorded as deferred so a later lifecycle event
// re-drives removal once the blocking reference may have cleared.
func (c *imageRefCounter) finishCleanup(image string, removed bool) {
	if image == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cleanupActive, image)
	if removed {
		delete(c.deferred, image)
		delete(c.retired, image)
		return
	}
	c.deferred[image] = true
}

// abandonCleanup drops image's chain admission WITHOUT retaining anything: it is
// used only when the manager has begun shutting down and the attempt therefore
// cannot decide anything (a terminal deferral for this worker, retried by the
// next boot's cleanup pass). An image that is merely still in use is NOT
// abandoned — it is recorded as deferred (finishCleanup) — because a concurrent
// idle handoff can be coalesced against this chain and would otherwise be lost.
func (c *imageRefCounter) abandonCleanup(image string) {
	if image == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cleanupActive, image)
}

// clearRetirement drops EVERY per-image record — including the retirement itself
// — for an image the registry serves again. The retirement is void: the image is
// a current generation, so no drive (a release-to-idle handoff or a deferred
// re-drive) may attempt its removal, and no periodic re-drive keeps retrying it.
// It is the explicit counterpart of retire, and the only way a use can end a
// retirement.
func (c *imageRefCounter) clearRetirement(image string) {
	if image == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cleanupActive, image)
	delete(c.deferred, image)
	delete(c.retired, image)
}

// deferredImages returns a snapshot of the images whose retirement chain gave up
// and whose removal is retained for a later re-drive.
func (c *imageRefCounter) deferredImages() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.deferred) == 0 {
		return nil
	}
	out := make([]string, 0, len(c.deferred))
	for image := range c.deferred {
		out = append(out, image)
	}
	return out
}

// retirementState is one image's per-image retirement records, sampled under the
// counter lock. It exists so diagnostics and tests can INSPECT the state without
// racing a cleanup goroutine's transition; production decisions are made only by
// the atomic beginCleanup/finishCleanup/clearRetirement transitions.
type retirementState struct {
	retired  bool
	active   bool
	deferred bool
}

// stateOf samples image's retirement records.
func (c *imageRefCounter) stateOf(image string) retirementState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return retirementState{
		retired:  c.retired[image],
		active:   c.cleanupActive[image],
		deferred: c.deferred[image],
	}
}

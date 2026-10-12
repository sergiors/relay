package runtime

import (
	"context"
	"time"
)

// leaseCoord returns the manager's image-lease coordinator, lazily initializing
// it on first use. Exactly one coordinator exists per Manager and it lives for
// the manager's whole lifetime: it is the single ownership authority every
// admission and every removal is gated by, so a retirement observed by one use
// is observed by all of them. Lazy initialization is what lets a Manager built
// without NewManager (a zero-value/test-constructed one) still own that single
// authority. It is safe for concurrent use.
//
// A nil receiver is a programming error, not a supported state: there is no
// manager to own the coordinator, and minting one per call would hide the
// missing manager behind an authority no shutdown ever joins.
func (m *Manager) leaseCoord() *imageCoordinator {
	if m == nil {
		panic("runtime: nil Manager")
	}
	m.leaseInit.Do(func() {
		if m.leases == nil {
			m.leases = newImageCoordinator()
		}
	})
	return m.leases
}

// beginRemovalOperation starts a lifecycle-owned removal-RELATED Docker
// operation (a container/image listing that guards removal) that does not itself
// commit an image to retirement. It admits the operation through the Manager's
// general Docker operation gate (so its context observes both the caller's ctx
// and Manager shutdown) and ALSO registers it with the coordinator's listing
// gate, so manager shutdown refuses new operations with ErrManagerShuttingDown
// and joins this one before the Docker client closes. The returned finish must be
// called (defer) when the operation concludes.
func (m *Manager) beginRemovalOperation(ctx context.Context) (context.Context, func(), error) {
	opCtx, release, err := m.beginDockerOp(ctx)
	if err != nil {
		return nil, nil, err
	}
	coord := m.leaseCoord()
	if err := coord.beginRemovalOp(); err != nil {
		release()
		return nil, nil, err
	}
	return opCtx, func() {
		coord.finishRemovalOp()
		release()
	}, nil
}

// AcquireImageLease admits a new independent reference to a Relay-owned image.
// It is the ownership token every NEW use of an image must hold across the
// whole check-then-use window (reuse probe, build, container start, service
// start, dependency consumption) so removal can never commit between the check
// and the use.
//
// An empty image OR an image outside Relay's namespaces ("relay-app-" /
// "relay-dep-") is not leased: it returns (nil, nil) because Relay has no
// ownership of it (a no-runtime app has no image to pin, and an external
// service image must never be GC'd by Relay). A retired/removing image returns
// a wrapped ErrImageRetiring: callers treat it as a retryable transitional
// state, never a hard failure.
func (m *Manager) AcquireImageLease(image string) (*ImageLease, error) {
	if !IsRelayImage(image) {
		return nil, nil
	}
	return m.leaseCoord().acquire(image)
}

// WithSnapshotLease attaches an admitted lease to ctx for Manager.Execute, so
// an invocation admitted before an image's retirement executes under that
// admitted authority instead of being rejected by the retirement gate. It is
// exposed so the runner can carry a registry snapshot's publication lease into
// the execution path; a nil lease leaves ctx unchanged (see WithImageLease).
func WithSnapshotLease(ctx context.Context, lease *ImageLease) context.Context {
	return WithImageLease(ctx, lease)
}

// admitLease returns a live admitted reference to image for work that is about
// to use it. It derives that reference from the lease carried on ctx when that
// lease actually ENTITLES image (see ImageLease.Entitles), and acquires a fresh
// independent reference otherwise.
//
// An entitlement is only authoritative for the coordinator that owns it: the
// carried lease must also have been admitted by THIS manager's image coordinator
// (the single authority that gates this manager's removals). A lease minted by a
// different manager does not protect this manager's image, so it is ignored and a
// fresh independent reference is acquired rather than trusting a token another
// coordinator's drain does not observe.
//
// The carried-entitlement path is what lets work ADMITTED BEFORE an image's
// retirement — a registry snapshot pinning a superseded published image, a
// service pass converging to it — hand its authority to a child use: the child
// takes its OWN share of the carried lease, so the use stays admitted even while
// a retirement drains and even if the original holder releases its lease
// concurrently. That share is admitted while the image is retiring by design:
// the work it protects was admitted before retirement. Holding an independent
// share also means the removal cannot proceed until this use concludes, rather
// than trusting a token the caller may drop mid-use.
//
// When the carried lease is absent, already released, pins a DIFFERENT image, or
// belongs to another coordinator, the caller is NOT entitled and this acquires a
// fresh independent reference, which a retirement in progress rejects with a
// retryable ErrImageRetiring so the use defers rather than racing ImageRemove.
// The returned lease (nil for an image outside Relay's namespaces, e.g. a
// no-runtime app or an external service image) must be released by the
// caller.
func (m *Manager) admitLease(ctx context.Context, image string) (*ImageLease, error) {
	coord := m.leaseCoord()
	if carried := ImageLeaseFrom(ctx); carried.Entitles(image) && carried.coord == coord {
		if shared := coord.shareLease(carried); shared != nil {
			return shared, nil
		}
	}
	return m.AcquireImageLease(image)
}

// RetireImageLease is the single authority-aware image-removal entry point for
// callers that have already decided an image is superseded (the runner's async
// retirement, the app-removal path, the startup sweep). It commits the
// image to retirement (rejecting NEW independent leases while work admitted
// before this point drains), invalidates the image's warm execution containers,
// waits on ctx for every admitted lease to drain, then performs the FORCE-FREE
// removal through Manager.RemoveImage's container-reference guard. On success
// the image's coordination state is dropped; on failure the retirement gate is
// cleared so the image stays retryable and reusable.
//
// removed reports whether this call actually performed the removal. A duplicate
// retirement (another goroutine already owns removal) reports (false, nil):
// removal is already in progress. When ctx is done before the drain completes
// the returned error is ctx.Err(), so the caller retries rather than removing
// early.
//
// The operation is lifecycle-owned: once the manager has begun shutting down it
// is refused with ErrManagerShuttingDown (a terminal deferral for this manager,
// retried at the next boot's natural cleanup pass) and a removal already in
// flight is cancelled and joined before the Docker client closes.
func (m *Manager) RetireImageLease(ctx context.Context, image string) (removed bool, err error) {
	return m.retireOwned(ctx, image, true)
}

// retireBound is the default per-image removal bound applied by
// RetireImageContext when the caller supplies an unbounded context. It mirrors
// the runner's cleanupTimeout so a wedged daemon cannot hold a retirement
// goroutine forever.
const retireBound = 10 * time.Second

// RetireImageContext is RetireImageLease with a finite bound derived from ctx:
// when ctx has no deadline, retireBound is applied. Callers that already have a
// bounded context (the runner's cleanupTimeout) pass it through unchanged.
func (m *Manager) RetireImageContext(ctx context.Context, image string) (bool, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, retireBound)
		defer cancel()
	}
	return m.RetireImageLease(ctx, image)
}

// IsImageRetiring reports whether image has been committed to removal. It is
// used by tests and diagnostics; production callers rely on AcquireImageLease's
// atomic admission instead.
func (m *Manager) IsImageRetiring(image string) bool {
	if image == "" {
		return false
	}
	return m.leaseCoord().retired(image)
}

// LeaseCount returns the number of admitted references currently held for
// image. It is a diagnostic/observability accessor (never a substitute for
// AcquireImageLease's atomic admission): it lets tests and live inspection
// confirm that a publication or snapshot pin is still held.
func (m *Manager) LeaseCount(image string) int {
	if image == "" {
		return 0
	}
	return m.leaseCoord().count(image)
}

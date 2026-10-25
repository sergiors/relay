package runtime

import (
	"context"
	"errors"
	"fmt"
)

// ErrDependencyImageCoordination reports that dependency-image coordination is
// unavailable for an operation that requires it. It is fail-closed: a
// preparation that needs to build from a dependency image fails retryably (so
// the reconciler retries), and a dependency GC pass removes nothing. The error
// carries the underlying cause (for example a closed state handle or an
// unreachable database), so callers can log both.
var ErrDependencyImageCoordination = errors.New(
	"runtime: dependency image coordination unavailable")

// DependencyImageLease is one admitted reference to a dependency image across
// Relay processes. Runtime uses it for two purposes:
//
//   - a SHARED use lease, held by a preparation from before the dependency
//     probe/build through the app-image build that consumes the layer via FROM;
//   - an EXCLUSIVE GC lease, held by dependency garbage collection while it
//     revalidates the daemon's image references and removes one layer.
//
// The shared/exclusive pair is what makes a layer unremovable while any process
// is building from it, across managers and across processes — the per-Manager
// image lease coordinator alone cannot see another manager's admitted use.
//
// Verify reports whether the lease still protects its reference (fail closed on
// any ambiguity) and is checked immediately before a Docker call whose safety
// depends on the protection. Release is idempotent and must be called on every
// path. Neither claims instantaneous fencing: a lease lives on a database
// session, and the residual window between Verify and the following Docker call
// cannot be closed from the database side.
type DependencyImageLease interface {
	Verify(ctx context.Context) error
	Release() error
}

// DependencyImageCoordinator is the cross-process dependency-image ownership
// authority a Manager uses. The production implementation is backed by the
// shared PostgreSQL state database (PostgreSQL session advisory locks; see
// internal/state), injected by the worker.
//
// AcquireUse is a bounded, context-aware shared acquisition: it waits for an
// exclusive GC hold to clear and then fails retryably rather than wedging a
// preparation. TryAcquireGC never waits: acquired == false with a nil error means
// the layer is in use (by this manager, another manager, or another process), and
// the GC must skip it. Any error is fail-closed.
//
// A Manager constructed without a coordinator FAILS CLOSED when a preparation
// must build from a dependency image and for dependency GC. Reusing a completed
// app image does not consume the dependency reference. There is deliberately no
// manager-local fallback: a process-local lock cannot protect an image on a
// shared Docker daemon, and silently pretending otherwise is the defect this
// seam exists to prevent.
type DependencyImageCoordinator interface {
	AcquireUse(ctx context.Context, depRef string) (DependencyImageLease, error)
	TryAcquireGC(ctx context.Context, depRef string) (lease DependencyImageLease, acquired bool, err error)
}

// WithDependencyImageCoordinator installs the cross-process dependency-image
// coordination authority. The worker wires the State-backed coordinator; a
// Manager constructed directly (tests, embedders) must inject one to prepare
// apps that declare dependencies or to run dependency GC.
func WithDependencyImageCoordinator(coord DependencyImageCoordinator) ManagerOption {
	return func(o *managerOptions) { o.depCoord = coord }
}

// acquireDependencyImageUse admits the shared cross-process use lease for one
// dependency reference. A Manager without a coordinator fails closed.
func (m *Manager) acquireDependencyImageUse(
	ctx context.Context,
	depRef string,
) (DependencyImageLease, error) {
	if m.depCoord == nil {
		return nil, fmt.Errorf(
			"%w: no dependency image coordinator configured for dependency build",
			ErrDependencyImageCoordination)
	}
	lease, err := m.depCoord.AcquireUse(ctx, depRef)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDependencyImageCoordination, err)
	}
	if lease == nil {
		return nil, fmt.Errorf("%w: coordinator returned no lease for %s",
			ErrDependencyImageCoordination, depRef)
	}
	return lease, nil
}

// tryAcquireDependencyImageGC takes the non-blocking exclusive view for one GC
// candidate. A Manager without a coordinator fails closed (the caller must not
// remove the layer). A contended candidate reports (nil, false, nil); any error
// is fail-closed.
func (m *Manager) tryAcquireDependencyImageGC(
	ctx context.Context,
	depRef string,
) (DependencyImageLease, bool, error) {
	if m.depCoord == nil {
		return nil, false, fmt.Errorf(
			"%w: no dependency image coordinator configured for dependency GC",
			ErrDependencyImageCoordination)
	}
	lease, acquired, err := m.depCoord.TryAcquireGC(ctx, depRef)
	if err != nil {
		return nil, false, fmt.Errorf("%w: %w", ErrDependencyImageCoordination, err)
	}
	if !acquired {
		return nil, false, nil
	}
	if lease == nil {
		return nil, false, fmt.Errorf("%w: coordinator granted no GC lease for %s",
			ErrDependencyImageCoordination, depRef)
	}
	return lease, true, nil
}

// verifyDependencyImageUse re-proves a shared use lease immediately before a
// Docker call that consumes the protected layer, and after the build that used
// it. Any error is fail-closed for the preparation.
func verifyDependencyImageUse(
	ctx context.Context,
	lease DependencyImageLease,
	depRef string,
) error {
	if lease == nil {
		return nil
	}
	if err := lease.Verify(ctx); err != nil {
		return fmt.Errorf("%w: dependency %s: %w", ErrDependencyImageCoordination, depRef, err)
	}
	return nil
}

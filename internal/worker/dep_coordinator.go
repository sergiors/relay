package worker

import (
	"context"

	"relay/internal/runtime"
	"relay/internal/state"
)

// stateDependencyImageCoordinator adapts the State-backed dependency-image lease
// API to the runtime's coordination seam. The adapter is deliberately this small:
// state owns the PostgreSQL session advisory locks, ownership verification, and
// fail-closed semantics, and runtime owns when a preparation or a GC pass needs
// them. Keeping the two contracts structurally identical means no policy lives
// in the adapter.
type stateDependencyImageCoordinator struct {
	st *state.State
}

// newStateDependencyImageCoordinator returns the coordinator for a worker's
// state handle, or nil when no handle was opened. A nil coordinator makes a
// dependency-consuming app-image build and dependency GC fail closed in the
// runtime; it is never replaced by a process-local fallback, because a
// process-local lock cannot protect an image on a shared Docker daemon.
func newStateDependencyImageCoordinator(st *state.State) runtime.DependencyImageCoordinator {
	if st == nil {
		return nil
	}
	return stateDependencyImageCoordinator{st: st}
}

// AcquireUse takes the shared cross-process use lease for depRef.
func (c stateDependencyImageCoordinator) AcquireUse(
	ctx context.Context, depRef string,
) (runtime.DependencyImageLease, error) {
	inner, err := c.st.AcquireDependencyImageUse(ctx, depRef)
	if err != nil {
		return nil, err
	}
	return stateDependencyImageLease{inner: inner}, nil
}

// TryAcquireGC takes the non-blocking exclusive GC view for depRef.
func (c stateDependencyImageCoordinator) TryAcquireGC(
	ctx context.Context, depRef string,
) (runtime.DependencyImageLease, bool, error) {
	inner, acquired, err := c.st.TryAcquireDependencyImageGC(ctx, depRef)
	if err != nil {
		return nil, false, err
	}
	if !acquired {
		return nil, false, nil
	}
	return stateDependencyImageLease{inner: inner}, true, nil
}

// stateDependencyImageLease adapts a state lease to the runtime contract.
type stateDependencyImageLease struct {
	inner state.DependencyImageLease
}

func (l stateDependencyImageLease) Verify(ctx context.Context) error {
	return l.inner.Verify(ctx)
}

func (l stateDependencyImageLease) Release() error {
	return l.inner.Release()
}

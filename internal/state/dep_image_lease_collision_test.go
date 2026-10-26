//go:build integration

package state

import (
	"context"
	"testing"
	"time"
)

// collidingDependencyRefs returns two DISTINCT dependency references that map to
// one advisory-lock key. The mapping is a 32-bit FNV-1a hash, so collisions are
// expected; the pair is hardcoded and re-verified here so a hash change fails
// loudly instead of silently testing nothing.
func collidingDependencyRefs(t *testing.T) (string, string) {
	t.Helper()
	const (
		refA = "relay-dep-collision-68458"
		refB = "relay-dep-collision-290402"
	)
	if refA == refB {
		t.Fatal("collision fixture refs must be distinct")
	}
	if got, want := dependencyImageLockKey(refA), dependencyImageLockKey(refB); got != want {
		t.Fatalf("collision fixture no longer collides: %q -> %d, %q -> %d", refA, got, refB, want)
	}
	return refA, refB
}

// TestDependencyLeaseKeyCollisionsSerializeLocally proves local lease bookkeeping
// and the local use/exclusive conflict check are keyed by the advisory-lock
// identity, not by the reference string. PostgreSQL grants a session a
// conflicting lock mode on a key it already holds, so with reference-keyed
// bookkeeping a GC for one of two colliding references would be granted the
// exclusive lock while this same session still holds a shared use of the other.
// The colliding pair must therefore conservatively serialize locally: shared
// uses of both may coexist, no GC hold is granted until the last one releases,
// and a GC hold then blocks a new shared use on either reference.
func TestDependencyLeaseKeyCollisionsSerializeLocally(t *testing.T) {
	refA, refB := collidingDependencyRefs(t)
	st := testOpenOrFail(t, newTestDSN(t))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Shared uses of the colliding pair coexist: each takes its own counted
	// session lock on the one shared key.
	useA, err := st.AcquireDependencyImageUse(ctx, refA)
	if err != nil {
		t.Fatalf("acquire use A: %v", err)
	}
	useB, err := st.AcquireDependencyImageUse(ctx, refB)
	if err != nil {
		t.Fatalf("acquire use B (a colliding key must still allow a shared use): %v", err)
	}

	// GC for B is refused while A's shared use is held. Both references map to
	// one advisory key, and PostgreSQL would grant this session the exclusive
	// lock despite its own shared hold; the local check must refuse it.
	gc, acquired, err := st.TryAcquireDependencyImageGC(ctx, refB)
	if err != nil {
		t.Fatalf("GC attempt for the colliding B: %v", err)
	}
	if acquired || gc != nil {
		t.Fatal("GC for colliding reference B was granted while A holds a shared use")
	}

	// Releasing ONE of the two shared references is not enough.
	if err := useA.Release(); err != nil {
		t.Fatalf("release use A: %v", err)
	}
	gc, acquired, err = st.TryAcquireDependencyImageGC(ctx, refB)
	if err != nil {
		t.Fatalf("GC attempt after releasing A only: %v", err)
	}
	if acquired || gc != nil {
		t.Fatal("GC granted while the colliding B use is still held")
	}

	// The last release opens the gate. Acquire the GC hold through A's reference
	// to prove the grant is decided at lock identity, not per reference string.
	if err := useB.Release(); err != nil {
		t.Fatalf("release use B: %v", err)
	}
	gc, acquired, err = st.TryAcquireDependencyImageGC(ctx, refA)
	if err != nil {
		t.Fatalf("GC attempt after every use released: %v", err)
	}
	if !acquired || gc == nil {
		t.Fatal("GC must be granted after every colliding shared use released")
	}

	// While the GC hold is active, a shared use of the OTHER colliding
	// reference must be refused. A short caller bound keeps the retryable
	// refusal prompt.
	shortCtx, shortCancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer shortCancel()
	if lease, err := st.AcquireDependencyImageUse(shortCtx, refB); err == nil {
		if lease != nil {
			_ = lease.Release()
		}
		t.Fatal("a shared use of colliding B was granted while GC holds the shared key")
	}
	if err := gc.Release(); err != nil {
		t.Fatalf("release GC: %v", err)
	}

	// Everything is free again for either reference.
	useB2, err := st.AcquireDependencyImageUse(ctx, refB)
	if err != nil {
		t.Fatalf("acquire use B after the GC release: %v", err)
	}
	if err := useB2.Release(); err != nil {
		t.Fatalf("release use B after the GC release: %v", err)
	}
}

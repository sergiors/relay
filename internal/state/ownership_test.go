package state

import (
	"context"
	"errors"
	"testing"
)

// TestOwnerBackendPIDCapture pins the backend-PID capture Open performs right
// after taking the worker lock: a worker handle records the pinned connection's
// PostgreSQL backend PID, while an ownerless admin/CLI handle (which never takes
// the lock) records none.
func TestOwnerBackendPIDCapture(t *testing.T) {
	dsn := newTestDSN(t)

	owner := openWithIdentity(t, dsn, "worker-pid", "inst-1")
	if pid := owner.OwnerBackendPID(); pid <= 0 {
		t.Fatalf("owner OwnerBackendPID = %d, want > 0", pid)
	}
	if err := owner.VerifyOwnerLock(context.Background()); err != nil {
		t.Fatalf("VerifyOwnerLock on a held lock = %v, want nil", err)
	}

	admin := testOpenOrFail(t, dsn)
	if pid := admin.OwnerBackendPID(); pid != 0 {
		t.Fatalf("ownerless OwnerBackendPID = %d, want 0", pid)
	}
	// An ownerless handle never takes the lock, so it is never monitored and its
	// verification is a nil no-op.
	if err := admin.VerifyOwnerLock(context.Background()); err != nil {
		t.Fatalf("ownerless VerifyOwnerLock = %v, want nil", err)
	}
}

// TestVerifyOwnerLockNilReceiverIsNoOp pins the nil-safety of the verification
// API, so a defensively-called handle cannot panic.
func TestVerifyOwnerLockNilReceiverIsNoOp(t *testing.T) {
	var st *State
	if err := st.VerifyOwnerLock(context.Background()); err != nil {
		t.Fatalf("nil VerifyOwnerLock = %v, want nil", err)
	}
	if pid := st.OwnerBackendPID(); pid != 0 {
		t.Fatalf("nil OwnerBackendPID = %d, want 0", pid)
	}
}

// TestVerifyOwnerLockFailsClosedAfterClose pins that verification fails closed
// once the handle's pinned lock connection is gone (a closed handle): the lock
// must be treated as lost, never silently assumed held.
func TestVerifyOwnerLockFailsClosedAfterClose(t *testing.T) {
	dsn := newTestDSN(t)
	owner := openWithIdentity(t, dsn, "worker-closed", "inst-1")
	if err := owner.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := owner.VerifyOwnerLock(context.Background()); !errors.Is(err, ErrOwnerLockLost) {
		t.Fatalf("VerifyOwnerLock after close = %v, want ErrOwnerLockLost", err)
	}
}

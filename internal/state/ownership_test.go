package state

import (
	"context"
	"testing"
)

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

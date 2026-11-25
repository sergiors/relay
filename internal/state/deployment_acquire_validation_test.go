package state

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// AcquireDeploymentNode validates its caller BEFORE any database work: the
// incarnation it allocates is the root of every later deployment fence, so a
// nil or ownerless handle, a handle without a pinned lock backend, an identity
// the handle does not own, and a non-positive generation are all refused
// without running the acquisition statement. The handles below deliberately
// carry no pool: any statement would panic the test, so a passing case proves
// the rejection happened locally.
func TestAcquireDeploymentNodeValidatesBeforeDatabaseWork(t *testing.T) {
	ctx := context.Background()
	progress := DeploymentNodeProgress{NodeID: "node-a", Generation: 1}

	for _, tc := range []struct {
		name       string
		handle     *State
		nodeID     string
		instance   string
		generation int64
		want       string
	}{
		{"nil handle", nil, "node-a", "inst-1", 1,
			"requires a node-owned state handle"},
		{"ownerless handle", &State{}, "node-a", "inst-1", 1,
			"requires a node-owned state handle"},
		{"handle without a lock backend",
			&State{nodeID: "node-a", instance: "inst-1"}, "node-a", "inst-1", 1,
			"requires a node-owned state handle"},
		{"foreign node identity",
			&State{nodeID: "node-a", instance: "inst-1", ownerPID: 42},
			"node-b", "inst-1", 1, "state handle owns node"},
		{"empty node identity",
			&State{nodeID: "node-a", instance: "inst-1", ownerPID: 42},
			"", "inst-1", 1, "state handle owns node"},
		{"foreign instance",
			&State{nodeID: "node-a", instance: "inst-1", ownerPID: 42},
			"node-a", "inst-2", 1, "state handle owns node"},
		{"non-positive generation",
			&State{nodeID: "node-a", instance: "inst-1", ownerPID: 42},
			"node-a", "inst-1", 0, "generation must be positive"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.handle.AcquireDeploymentNode(
				ctx, tc.nodeID, tc.instance, tc.generation, progress)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("AcquireDeploymentNode = %v, want an error containing %q",
					err, tc.want)
			}
		})
	}
}

// A node-owned handle whose pinned lock connection is already released (a
// closed handle) fails CLOSED with the ownership-loss sentinels instead of
// falling back to the pool: a handle that cannot query its pinned backend can
// no longer prove ownership, so it must never allocate an incarnation.
func TestAcquireDeploymentNodeFailsClosedWithoutPinnedConnection(t *testing.T) {
	released := &State{nodeID: "node-a", instance: "inst-1", ownerPID: 42}
	_, err := released.AcquireDeploymentNode(context.Background(),
		"node-a", "inst-1", 1,
		DeploymentNodeProgress{NodeID: "node-a", Generation: 1})
	if !errors.Is(err, ErrOwnerLockLost) || !errors.Is(err, ErrOwnerLockConnection) {
		t.Fatalf("acquire on a released lock connection = %v, want "+
			"ErrOwnerLockLost + ErrOwnerLockConnection", err)
	}
	if errors.Is(err, ErrOwnerLockNotHeld) || errors.Is(err, ErrOwnerBackendPIDChanged) {
		t.Fatalf("acquire on a released lock connection = %v, must not claim a "+
			"confirmed loss no query observed", err)
	}
}

// fakeSQLStateError (ownership_classify_test.go) is the server-reported
// failure shape used here as well: it carries its SQLSTATE through the
// standard interface, like a pgconn.PgError.
//
// The ambiguous-commit classifier behind ErrDeploymentNodeOutcomeUnknown is
// the single decision that separates a KNOWN commit rollback (a plain
// server-reported error, returned generically) from an outcome the client
// cannot prove: connection-class failures, canceled/expired contexts, and the
// server SQLSTATEs 40003 (serialization failure) and 57014 (statement timeout
// or cancel) — a commit-time cancellation can arrive after the durable commit
// point, so it is not proof of rollback. All unknown outcomes are reported
// through the sentinel so startup treats them as fatal instead of retryable.
func TestDeploymentCommitOutcomeUnknownClassification(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"network EOF", io.EOF, true},
		{"connection-class SQLSTATE",
			fakeSQLStateError{sqlState: "08006", msg: "connection failure"}, true},
		{"canceled context", context.Canceled, true},
		{"wrapped canceled context",
			fmt.Errorf("commit: %w", context.Canceled), true},
		{"expired deadline", context.DeadlineExceeded, true},
		{"serialization failure at commit",
			fakeSQLStateError{sqlState: "40003", msg: "serialization failure"}, true},
		{"statement timeout at commit",
			fakeSQLStateError{sqlState: "57014",
				msg: "canceling statement due to statement timeout"}, true},
		{"unique-constraint violation",
			fakeSQLStateError{sqlState: "23505", msg: "duplicate key"}, false},
		{"deferred constraint violation",
			fakeSQLStateError{sqlState: "P0001", msg: "injected commit failure"}, false},
		// An error carrying NO server SQLSTATE cannot prove the server rolled
		// the commit back, so it fails closed to the unknown outcome.
		{"error without a server SQLSTATE",
			errors.New("commit failed"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := deploymentCommitOutcomeUnknown(tc.err); got != tc.want {
				t.Fatalf("deploymentCommitOutcomeUnknown(%v) = %t, want %t",
					tc.err, got, tc.want)
			}
		})
	}
}

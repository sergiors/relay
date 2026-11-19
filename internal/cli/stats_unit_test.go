package cli

import (
	"errors"
	"strings"
	"testing"
)

// TestHumanAge pins Go-duration formatting, including zero rendered as "0s".
func TestHumanAge(t *testing.T) {
	for _, tc := range []struct {
		in   int64
		want string
	}{
		{0, "0s"},
		{134, "2m14s"},
		{3723, "1h2m3s"},
	} {
		if got := humanAge(tc.in); got != tc.want {
			t.Errorf("humanAge(%d) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestStatsUnknownTokenFallsThroughToParentGuard pins that a junk token under
// `stats` is NOT swallowed by the subcommand list: with no matching subcommand,
// urfave runs the parent Action, whose too-many-arguments guard preserves the
// previous `stats: too many arguments` semantics. The guard fires before the
// state lookup, so the default database-free fixture suffices.
func TestStatsUnknownTokenFallsThroughToParentGuard(t *testing.T) {
	_, _, err := runCLIWithDeps(t, testDeps(t), "", "stats", "asdsa")
	if err == nil || !strings.Contains(err.Error(), "stats: too many arguments") {
		t.Fatalf("stats asdsa: err = %v, want parent guard", err)
	}
}

// TestStatsDefaultRequiresValidNodeID pins the default read's identity rule:
// with neither selector flag, `relay stats` resolves NODE_ID through exactly
// the worker's own validation and fails on a missing, blank, or malformed value
// — never falling back to a hostname, a generated name, or an aggregate read.
// The selector resolves before any state access, so the database-free fixture
// suffices.
func TestStatsDefaultRequiresValidNodeID(t *testing.T) {
	for _, tc := range []struct {
		name   string
		nodeID string
	}{
		{"missing", ""},
		{"whitespace only", "   "},
		{"invalid characters", "node/1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("NODE_ID", tc.nodeID)
			_, _, err := runCLIWithDeps(t, testDeps(t), "", "stats")
			if err == nil || !strings.Contains(err.Error(), "invalid NODE_ID") {
				t.Fatalf("stats: err = %v, want an invalid NODE_ID error", err)
			}
		})
	}
}

// TestStatsNodeFlagValidatesSelectedIdentity pins that --node validates the
// NAMED identity with the same rules and names the flag in the error, even
// when NODE_ID itself would be valid — the selected row's key must be sound.
func TestStatsNodeFlagValidatesSelectedIdentity(t *testing.T) {
	t.Setenv("NODE_ID", "valid-default")
	_, _, err := runCLIWithDeps(t, testDeps(t), "", "stats", "--node", "not a node id")
	if err == nil ||
		!strings.Contains(err.Error(), "stats: --node") ||
		!strings.Contains(err.Error(), "invalid NODE_ID") {
		t.Fatalf("stats --node: err = %v, want the flagged NODE_ID error", err)
	}
}

// TestStatsAllSkipsNodeID pins that --all never reads NODE_ID: the selector
// passes and the command proceeds to state access, which the database-free
// fixture rejects with its unexpected-state-access error. Reaching that error
// proves the identity guard did not fire.
func TestStatsAllSkipsNodeID(t *testing.T) {
	t.Setenv("NODE_ID", "")
	_, _, err := runCLIWithDeps(t, testDeps(t), "", "stats", "--all")
	if !errors.Is(err, errUnexpectedStateAccess) {
		t.Fatalf("stats --all: err = %v, want the fixture's state-access error (NODE_ID must not be consulted)", err)
	}
}

// TestStatsNodeAndAllConflict pins the selector conflict: combining --node and
// --all is a usage error exiting 2 rather than silently preferring one selector
// (a wrong choice would either miss or invent other nodes' counters).
func TestStatsNodeAndAllConflict(t *testing.T) {
	_, _, err := runCLIWithDeps(t, testDeps(t), "", "stats", "--node", "worker-b", "--all")
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("stats --node --all: err = %v, want the mutual-exclusion error", err)
	}
	requireExitCode2(t, err, "stats: --node and --all are mutually exclusive")
}

// TestStatsResetRequiresNodeID pins the local reset's identity rule: a bare
// `relay stats reset` names one node's rows, so it fails fast on a missing or
// invalid NODE_ID before touching the socket or the state database.
func TestStatsResetRequiresNodeID(t *testing.T) {
	for _, tc := range []struct {
		name   string
		nodeID string
	}{
		{"missing", ""},
		{"invalid characters", "node/1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("NODE_ID", tc.nodeID)
			_, _, err := runCLIWithDeps(t, testDeps(t), "", "stats", "reset")
			if err == nil ||
				!strings.Contains(err.Error(), "stats reset: invalid NODE_ID") {
				t.Fatalf("stats reset: err = %v, want the flagged NODE_ID error", err)
			}
		})
	}
}

// TestStatsResetAllSkipsNodeID pins that `reset --all` needs no NODE_ID: with
// no worker socket it falls back to the state database and reaches the
// database-free fixture's state-access error, proving the identity guard was
// skipped while the fallback path was taken.
func TestStatsResetAllSkipsNodeID(t *testing.T) {
	t.Setenv("NODE_ID", "")
	_, _, err := runCLIWithDeps(t, testDeps(t), "", "stats", "reset", "--all")
	if !errors.Is(err, errUnexpectedStateAccess) {
		t.Fatalf("stats reset --all: err = %v, want the fixture's state-access error (NODE_ID must not be consulted)", err)
	}
}

//go:build integration

package cli

import (
	"context"
	"strings"
	"testing"

	"relay/internal/state"
)

// This file pins the CLI's read-failure contract against a real state
// database: `stats`, `app ls`, and `app inspect` surface an unreadable state
// view as a returned error — never as a fabricated zero snapshot, empty
// listing, or unknown-app message. The legitimate-empty outcomes (a fresh
// database renders zeros; an absent app reports unknown) stay covered by
// TestPrintStatsEmptyDB and TestAppInspectUnknownMessage; one command-level
// case is repeated below so the whole contract reads at the command boundary.
//
// The failure is injected through the production read path: the opener hands
// the command a real handle whose pool is closed the moment it is returned, so
// every read fails inside database/sql ("database is closed") — a genuine
// query failure on an initialized schema, not the fixture's unexpected-access
// sentinel that fires before any read.

// brokenStateDeps wires stateTestDeps' opener so every command run with these
// deps receives a usable-looking state handle whose pool is already closed.
// The schema still exists (the same per-test schema), so the commands reach
// their strict read and fail it, instead of failing at OpenState.
func brokenStateDeps(t *testing.T) Dependencies {
	t.Helper()
	deps := stateTestDeps(t)
	open := deps.OpenState
	deps.OpenState = func(ctx context.Context) (*state.State, func(), error) {
		st, _, err := open(ctx)
		if err != nil {
			return nil, nil, err
		}
		// Close exactly once on return. A failure here would be the fixture's
		// own defect; report it as a test failure rather than letting it
		// masquerade as the command's open path.
		if closeErr := st.Close(); closeErr != nil {
			t.Errorf("close seeded state handle: %v", closeErr)
		}
		return st, func() {}, nil
	}
	return deps
}

// TestStatsCommandReadFailureSurfaces pins the strict read contract for
// `relay stats`: an unreadable state view fails the command, and nothing is
// rendered on stdout — a read failure must not pass as the legitimate
// all-zero snapshot a missing row produces.
func TestStatsCommandReadFailureSurfaces(t *testing.T) {
	deps := brokenStateDeps(t)

	out, _, err := runCLIWithDeps(t, deps, "", "stats")
	if err == nil {
		t.Fatal("stats on an unreadable state view must fail")
	}
	if !strings.Contains(err.Error(), "stats:") {
		t.Fatalf("stats err = %v, want it prefixed by the command", err)
	}
	if !strings.Contains(err.Error(), "database is closed") {
		t.Fatalf("stats err = %v, want the underlying read failure", err)
	}
	if strings.Contains(out, "Events received:") {
		t.Fatalf("a failed read must not render a (zero) table:\n%s", out)
	}
}

// TestStatsCommandZeroRowRendersZeros pins the other half of the contract at
// the command boundary: a FRESH database has no stats row, which is legitimate
// data — `relay stats` exits 0 with the zero snapshot, not an error.
func TestStatsCommandZeroRowRendersZeros(t *testing.T) {
	deps := stateTestDeps(t)

	out, _, err := runCLIWithDeps(t, deps, "", "stats")
	if err != nil {
		t.Fatalf("stats on an empty database: err = %v, want nil", err)
	}
	flat := normWS(out)
	for _, want := range []string{"Events received: 0", "DLQ entries: 0", "Updated: never"} {
		if !strings.Contains(flat, want) {
			t.Fatalf("stats on an empty database missing %q:\n%s", want, out)
		}
	}
}

// TestAppLsReadFailureSurfaces pins `app ls`'s strict read: an unreadable
// state view must fail, never be presented as an empty listing (the shared
// Action also backs the `apps` alias, so both entry points are pinned).
func TestAppLsReadFailureSurfaces(t *testing.T) {
	deps := brokenStateDeps(t)

	out, _, err := runCLIWithDeps(t, deps, "", "app", "ls")
	if err == nil {
		t.Fatal("app ls on an unreadable state view must fail")
	}
	if !strings.Contains(err.Error(), "list apps") || !strings.Contains(err.Error(), "database is closed") {
		t.Fatalf("app ls err = %v, want the wrapped query failure", err)
	}
	if strings.Contains(out, "NAME") {
		t.Fatalf("a failed read must not render the (empty) table header:\n%s", out)
	}
}

// TestAppInspectReadFailureIsNotUnknown pins the distinction appInspect must
// preserve with the strict ReadApp: an unreadable state row is a read failure
// naming the command, NEVER the unknown-app message — an operator must not be
// told their app is gone because the state view could not be read.
func TestAppInspectReadFailureIsNotUnknown(t *testing.T) {
	deps := brokenStateDeps(t)

	out, _, err := runCLIWithDeps(t, deps, "", "app", "inspect", "user-events-python")
	if err == nil {
		t.Fatal("app inspect on an unreadable state view must fail")
	}
	if strings.Contains(err.Error(), "unknown app") {
		t.Fatalf("a failed read must not report the app unknown: %v", err)
	}
	if !strings.Contains(err.Error(), "app inspect") || !strings.Contains(err.Error(), "database is closed") {
		t.Fatalf("app inspect err = %v, want the wrapped read failure", err)
	}
	if strings.Contains(out, "Name:") {
		t.Fatalf("a failed read must not render a detail record:\n%s", out)
	}
}

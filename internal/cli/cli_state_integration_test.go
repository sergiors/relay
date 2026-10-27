//go:build integration

package cli

import (
	"context"
	"os"
	"testing"

	"relay/internal/processlock"
	"relay/internal/state"
	"relay/internal/testutil"
)

// stateTestDeps is the opt-in real-state fixture: testDeps plus an OpenState
// that opens a PostgreSQL handle on a fresh per-test schema. It is
// integration-only: it requires a reachable PostgreSQL (failing, not skipping,
// when absent) and reuses testutil.NewStateDSN, so every test gets its own
// schema (dropped on cleanup after the handles opened on it close). Only tests
// that genuinely read or write state should use it; the default testDeps stays
// database-free.
func stateTestDeps(t *testing.T) Dependencies {
	t.Helper()
	testutil.RequireStateDatabase(t)
	deps := testDeps(t)
	dsn := testutil.NewStateDSN(t)
	deps.OpenState = func(ctx context.Context) (*state.State, func(), error) {
		st, err := state.Open(ctx, state.Options{DSN: dsn})
		if err != nil {
			return nil, nil, err
		}
		return st, func() { _ = st.Close() }, nil
	}
	return deps
}

// TestNonStartCommandsDoNotAcquireLock pins that administrative commands never
// touch the start lock: with the lock held elsewhere, they still succeed and the
// lock file is not created or modified by them.
func TestNonStartCommandsDoNotAcquireLock(t *testing.T) {
	testutil.RequireStateDatabase(t)
	deps := stateTestDeps(t)
	held, err := processlock.Acquire(deps.LockPath)
	if err != nil {
		t.Fatalf("pre-acquire lock: %v", err)
	}
	defer held.Close()

	// The lock file exists (created by Acquire) and is held; capture its
	// identity so the administrative commands can be proven not to recreate it.
	before, err := os.Stat(deps.LockPath)
	if err != nil {
		t.Fatalf("stat held lock: %v", err)
	}

	// deps.OpenState opens a fresh isolated state database, so `app ls`
	// never touches the real state (the lock is the only path under test here).
	if _, _, err := runCLIWithDeps(t, deps, "", "app", "ls"); err != nil {
		t.Fatalf("function ls err = %v, want nil (held lock must not matter)", err)
	}
	if _, _, err := runCLI(t, "", "--help"); err != nil {
		t.Fatalf("--help err = %v, want nil", err)
	}

	after, err := os.Stat(deps.LockPath)
	if err != nil {
		t.Fatalf("stat lock after admin commands: %v", err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("administrative commands replaced the lock file; they must not touch it")
	}
	if !after.ModTime().Equal(before.ModTime()) {
		t.Fatal("administrative commands modified the lock file; they must not touch it")
	}
}

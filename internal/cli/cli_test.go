package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/processlock"
	"relay/internal/state"
)

// errUnexpectedStateAccess is the default test fixture's state-opener error.
// It makes accidental state access fail loudly instead of silently opening
// PostgreSQL (or returning a fake success): a test that needs state must opt in
// with stateTestDeps.
var errUnexpectedStateAccess = errors.New(
	"cli test fixture: unexpected state access; use stateTestDeps for state-backed tests")

// testDeps returns the default CLI test dependencies. The worker socket,
// process lock, git paths, and secrets dir all live under fresh per-test temp
// locations. The socket lives under /tmp (not t.TempDir's /var/folders on
// macOS) to stay inside the ~104-byte Unix socket sun_path limit. It replaces
// the former package-level path globals (statePath, runtimeSocketPath,
// startLockPath, the git dirs, and secretsPath) with per-test values, so tests
// never touch /var/lib/relay or /run/relay. Start defaults to a no-op runner
// so an accidental `start` never launches the real worker; tests asserting
// dispatch override deps.Start directly.
//
// testDeps is deliberately database-free: OpenState returns
// errUnexpectedStateAccess without opening PostgreSQL, so tests that never
// touch state (help, start, health, DLQ, git, secrets) need no database. Tests
// that genuinely read or write state opt in with stateTestDeps. The optional
// command-local seams — the inspect pool provider (PoolSnapshotProvider) and
// the git sync transport override (GitCloneURL) — stay zero-valued, so each
// test that wants them sets the field on its own dependencies instead of
// mutating any package state.
func testDeps(t *testing.T) Dependencies {
	t.Helper()
	dir := t.TempDir()
	sockDir, err := os.MkdirTemp("/tmp", "relay-cli-sock-")
	if err != nil {
		t.Fatalf("short temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	return Dependencies{
		OpenState: func(context.Context) (*state.State, func(), error) {
			return nil, nil, errUnexpectedStateAccess
		},
		SocketPath: filepath.Join(sockDir, "relay.sock"),
		LockPath:   filepath.Join(dir, "relay.lock"),
		// Git and secrets paths under the same per-test temp root, so git and
		// secret commands never touch /var/lib/relay or the production store.
		GitConfigPath:  filepath.Join(dir, "git", "source.json"),
		GitCheckoutDir: filepath.Join(dir, "git", "checkout"),
		GitSSHDir:      filepath.Join(dir, "ssh"),
		GitAppsDir:     filepath.Join(dir, "apps"),
		SecretsDir:     filepath.Join(dir, "secrets"),
		Start:          func(*slog.Logger) error { return nil },
	}
}

// TestDefaultFixtureRejectsStateAccess pins the default fixture's state
// contract: OpenState must fail loudly with exactly errUnexpectedStateAccess,
// naming stateTestDeps, and return no handle. The fixture neither opens nor
// depends on PostgreSQL, so the test passes with or without a reachable server;
// a regression that makes testDeps need a database fails the state-backed
// integration suite, which fails (never skips) without one.
func TestDefaultFixtureRejectsStateAccess(t *testing.T) {
	st, _, err := testDeps(t).OpenState(context.Background())
	if !errors.Is(err, errUnexpectedStateAccess) {
		t.Fatalf("OpenState err = %v, want the fixture's unexpected-state-access error", err)
	}
	if !strings.Contains(err.Error(), "stateTestDeps") {
		t.Fatalf("OpenState err %q must name the real-state fixture it requires", err)
	}
	if st != nil {
		t.Fatalf("OpenState returned a handle %v alongside its error", st)
	}
}

// runCLI builds the command tree with New and runs it against args (which
// include the program name slot urfave's parser consumes) with test-
// controllable Reader/Writer/ErrWriter buffers, temp filesystem dependencies,
// and a discard logger, and returns the captured output/error streams plus the
// error the command tree returned. Errors flow into the returned error, not
// onto the injected ErrWriter — printing happens in cmd/main.go, which tests do
// not execute.
func runCLI(t *testing.T, stdin string, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	return runCLIWithDeps(t, testDeps(t), stdin, args...)
}

// runCLIWithDeps is runCLI with caller-supplied dependencies, for tests that
// seed a state DB or hold a lock at paths the command must open.
func runCLIWithDeps(t *testing.T, deps Dependencies, stdin string, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	return runCLIWithLoggerAndDeps(t, slog.New(slog.NewTextHandler(io.Discard, nil)), deps, stdin, args...)
}

// runCLIWithLogger runs the command tree with a caller-supplied process logger
// so tests can assert whether any messages reach slog versus the command writer.
// The writer/reader/error streams are the usual test buffers.
func runCLIWithLogger(t *testing.T, logger *slog.Logger, stdin string, args ...string) (
	stdout, stderr string, err error) {
	t.Helper()
	return runCLIWithLoggerAndDeps(t, logger, testDeps(t), stdin, args...)
}

// runCLIWithLoggerAndDeps is the shared implementation behind the runCLI
// variants: it builds the tree with New (the injected logger, an output buffer,
// and the supplied deps) and runs it against args. The Reader is always an
// injected strings.Reader, even for empty stdin, so a command never observes
// the test process's inherited stdin. Terminal behavior is exercised through
// direct helper calls that provide their own reader. Errors flow into the
// returned error, never onto the ErrWriter (printing happens in cmd/main.go).
func runCLIWithLoggerAndDeps(t *testing.T, logger *slog.Logger, deps Dependencies, stdin string, args ...string) (
	stdout, stderr string, err error) {
	t.Helper()
	var out, errOut bytes.Buffer
	cmd := New(logger, &out, deps)
	cmd.Reader = strings.NewReader(stdin)
	cmd.ErrWriter = &errOut
	err = cmd.Run(t.Context(), append([]string{"relay"}, args...))
	return out.String(), errOut.String(), err
}

// Root --help/-h renders the COMMANDS block listing every documented command
// and the global help option, to stdout, exiting 0. This follows the
// user-required pattern: build the tree with New (an injected buffer writer and
// deps), then call cmd.Run directly with a ctx and args including the program
// name — no global stdout swapping or subprocess.
func TestRootHelp(t *testing.T) {
	commands := []string{"start", "app", "dlq", "secret", "git", "stats", "health"}
	for _, flag := range []string{"--help", "-h"} {
		var output bytes.Buffer
		cmd := New(slog.New(slog.NewTextHandler(io.Discard, nil)), &output, testDeps(t))
		err := cmd.Run(context.Background(), []string{"relay", flag})
		if err != nil {
			t.Fatalf("%s: err = %v, want nil", flag, err)
		}
		out := output.String()
		// Scope to the COMMANDS block: the root Usage line also contains words
		// like "app", so an unanchored search would not prove listing.
		idx := strings.Index(out, "COMMANDS:")
		if idx < 0 {
			t.Fatalf("%s: help has no COMMANDS section:\n%s", flag, out)
		}
		block := out[idx:]
		for _, name := range commands {
			if !strings.Contains(block, name) {
				t.Fatalf("%s: COMMANDS block missing %q:\n%s", flag, name, out)
			}
		}
		if !strings.Contains(out, "--help, -h") {
			t.Fatalf("%s: help missing the global --help option:\n%s", flag, out)
		}
	}
}

// Root help lists the start command first among the administrative commands.
func TestRootHelpContainsStart(t *testing.T) {
	out, _, err := runCLI(t, "", "--help")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	// Only consider the COMMANDS block; the root Usage line ("Relay
	// event-driven app runner") also contains the word "app", so an unanchored
	// index of the whole output would point at the header.
	cmds := out
	if i := strings.Index(cmds, "COMMANDS:"); i >= 0 {
		cmds = cmds[i:]
	}
	si := strings.Index(cmds, "start")
	fi := strings.Index(cmds, "app")
	if si == -1 || fi == -1 || si > fi {
		t.Fatalf("start should be listed before app:\n%s", out)
	}
}

// TestHelpAndArgumentValidationRunWithoutReachableDatabase pins that help
// rendering and argument validation never touch state: --help succeeds and
// `start extra` reports the arity usage error, never the fixture's state-access
// error, so a regression that routes these paths through the database fails
// here. TEST_DATABASE_URL is set to an unreachable address only as a defensive
// tripwire: the database-free fixture itself does not consult it, so no
// database is required or touched.
func TestHelpAndArgumentValidationRunWithoutReachableDatabase(t *testing.T) {
	t.Setenv("TEST_DATABASE_URL",
		"postgres://relay:relay@127.0.0.1:1/relay?sslmode=disable")

	if _, _, err := runCLI(t, "", "--help"); err != nil {
		t.Fatalf("--help: err = %v, want nil without a reachable database", err)
	}
	_, _, err := runCLI(t, "", "start", "extra")
	if err == nil || !strings.Contains(err.Error(), "start: too many arguments") {
		t.Fatalf("start extra: err = %v, want the arity usage error without a reachable database",
			err)
	}
}

// TestStartHelp verifies `relay start --help` exits 0 without starting anything.
func TestStartHelp(t *testing.T) {
	deps := testDeps(t)
	called := false
	deps.Start = func(l *slog.Logger) error { called = true; return nil }

	out, _, err := runCLIWithDeps(t, deps, "", "start", "--help")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if !strings.Contains(out, "Start the runtime") {
		t.Fatalf("stdout missing start usage:\n%s", out)
	}
	if called {
		t.Fatal("start --help must not invoke the worker startup path")
	}
}

// TestStartTooManyArgs verifies `relay start extra` is a usage error (exit 2).
func TestStartTooManyArgs(t *testing.T) {
	_, _, err := runCLI(t, "", "start", "extra")
	if err == nil || !strings.Contains(err.Error(), "start: too many arguments") {
		t.Fatalf("returned error missing usage message: %v", err)
	}
}

// TestStartCreatesRuntimeDirBeforeLock verifies the ephemeral runtime directory
// is created before the lock is taken: the injected lock points at a missing
// nested directory, and `relay start` must create it (so both the lock file and
// the worker's later socket bind have their parent).
func TestStartCreatesRuntimeDirBeforeLock(t *testing.T) {
	deps := testDeps(t)
	deps.LockPath = filepath.Join(t.TempDir(), "run", "relay", "relay.lock")

	called := false
	deps.Start = func(l *slog.Logger) error { called = true; return nil }

	if _, _, err := runCLIWithDeps(t, deps, "", "start"); err != nil {
		t.Fatalf("start: err = %v, want nil", err)
	}
	if !called {
		t.Fatal("start did not delegate to the worker startup path")
	}
	if info, err := os.Stat(filepath.Dir(deps.LockPath)); err != nil || !info.IsDir() {
		t.Fatalf("runtime dir %s not created (err=%v)", filepath.Dir(deps.LockPath), err)
	}
}

// TestStartDelegatesToWorker verifies the "start" command dispatches to the
// worker startup path without actually launching the runtime.
func TestStartDelegatesToWorker(t *testing.T) {
	deps := testDeps(t)
	called := false
	deps.Start = func(l *slog.Logger) error { called = true; return nil }

	if _, _, err := runCLIWithDeps(t, deps, "", "start"); err != nil {
		t.Fatalf("start: err = %v, want nil", err)
	}
	if !called {
		t.Fatal("start did not delegate to the worker startup path")
	}
}

// TestStartPassesInjectedLoggerToWorker pins that `relay start` threads the
// process logger into the worker startup path unchanged. Unlike the short-lived
// administrative commands, start MUST preserve normal worker operational logs,
// so the logger passed to New is the exact logger handed to the injected
// runner (never nil, never a discard).
func TestStartPassesInjectedLoggerToWorker(t *testing.T) {
	deps := testDeps(t)
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	var got *slog.Logger
	deps.Start = func(l *slog.Logger) error {
		got = l
		// Emulate a worker operational log; it must land on the injected logger.
		l.Info("worker operational log")
		return nil
	}

	if _, _, err := runCLIWithLoggerAndDeps(t, logger, deps, "", "start"); err != nil {
		t.Fatalf("start: err = %v, want nil", err)
	}
	if got != logger {
		t.Fatal("start did not pass the injected process logger to the worker startup path")
	}
	if !strings.Contains(logs.String(), "worker operational log") {
		t.Fatalf("worker operational log did not reach the injected logger:\n%s", logs.String())
	}
}

// TestInformationalCommandsNeverStartWorker pins that no informational CLI
// command accidentally starts the runtime.
func TestInformationalCommandsNeverStartWorker(t *testing.T) {
	deps := testDeps(t)
	called := false
	deps.Start = func(l *slog.Logger) error { called = true; return nil }

	// --help and an unknown command must not reach the injected runner.
	_, _, _ = runCLIWithDeps(t, deps, "", "--help")
	_, _, _ = runCLIWithDeps(t, deps, "", "bogus")
	if called {
		t.Fatal("informational CLI commands must not start the worker")
	}
}

// TestStartAlreadyRunning verifies that when the process lock is already held,
// `relay start` returns the concise operator-facing error without invoking the
// worker startup path (no stack trace, no runtime side effects).
func TestStartAlreadyRunning(t *testing.T) {
	deps := testDeps(t)
	held, err := processlock.Acquire(deps.LockPath)
	if err != nil {
		t.Fatalf("pre-acquire lock: %v", err)
	}
	defer held.Close()

	called := false
	deps.Start = func(l *slog.Logger) error { called = true; return nil }

	_, _, err = runCLIWithDeps(t, deps, "", "start")
	if err == nil || err.Error() != "relay start is already running" {
		t.Fatalf("err = %v, want %q", err, "relay start is already running")
	}
	if called {
		t.Fatal("already-running start must not invoke the worker startup path")
	}
}

// TestStartRuntimeDirCreationFailure verifies a failure to create the runtime
// directory (its parent is a regular file) surfaces a clear error before the
// worker startup path runs.
func TestStartRuntimeDirCreationFailure(t *testing.T) {
	deps := testDeps(t)
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, nil, 0o644); err != nil {
		t.Fatalf("seed blocker: %v", err)
	}
	deps.LockPath = filepath.Join(blocker, "relay.lock")

	called := false
	deps.Start = func(l *slog.Logger) error { called = true; return nil }

	_, _, err := runCLIWithDeps(t, deps, "", "start")
	if err == nil || !strings.Contains(err.Error(), "cannot create runtime dir") {
		t.Fatalf("err = %v, want 'cannot create runtime dir'", err)
	}
	if called {
		t.Fatal("start must not delegate to the worker when the runtime dir cannot be created")
	}
}

// TestStartReleasesLockAfterRun verifies the deferred release: once a start run
// returns, the lock is free again for a subsequent run.
func TestStartReleasesLockAfterRun(t *testing.T) {
	deps := testDeps(t)
	deps.Start = func(l *slog.Logger) error { return nil }

	for i := 0; i < 2; i++ {
		if _, _, err := runCLIWithDeps(t, deps, "", "start"); err != nil {
			t.Fatalf("start run %d: %v", i+1, err)
		}
	}
}

// TestStartPropagatesWorkerError verifies the worker's Run error is returned
// unchanged to the command layer (cmd/main.go owns printing and exit), and that
// the deferred lock release still runs on the failure path so a subsequent run
// is not blocked by a failed one.
func TestStartPropagatesWorkerError(t *testing.T) {
	deps := testDeps(t)
	workerErr := errors.New("worker failed")
	deps.Start = func(l *slog.Logger) error { return workerErr }

	_, _, err := runCLIWithDeps(t, deps, "", "start")
	if !errors.Is(err, workerErr) {
		t.Fatalf("err = %v, want %v", err, workerErr)
	}

	// The deferred lock release must have run: a second start must not report
	// "already running" from the failed first run.
	if _, _, err := runCLIWithDeps(t, deps, "", "start"); !errors.Is(err, workerErr) {
		t.Fatalf("second start err = %v, want %v (lock must be released)", err, workerErr)
	}
}

// TestStatelessAdminCommandsDoNotAcquireLockWithoutDatabase pins the non-start
// lock contract for commands that never read state (git, secret): with the
// start lock held elsewhere, both succeed and the lock file is neither
// recreated nor modified. TEST_DATABASE_URL is set to an unreachable address
// only as a defensive tripwire: the database-free fixture itself does not
// consult it, and these commands never read state. The state-backed sibling
// TestNonStartCommandsDoNotAcquireLock covers only state-reading commands in
// the integration suite (where it fails without PostgreSQL); the default
// database-free suite keeps a lock regression guard here.
func TestStatelessAdminCommandsDoNotAcquireLockWithoutDatabase(t *testing.T) {
	t.Setenv("TEST_DATABASE_URL",
		"postgres://relay:relay@127.0.0.1:1/relay?sslmode=disable")

	deps := testDeps(t)
	held, err := processlock.Acquire(deps.LockPath)
	if err != nil {
		t.Fatalf("pre-acquire lock: %v", err)
	}
	defer held.Close()

	// The lock file exists (created by Acquire) and is held; capture its
	// identity so the commands can be proven not to recreate or modify it.
	before, err := os.Stat(deps.LockPath)
	if err != nil {
		t.Fatalf("stat held lock: %v", err)
	}

	for _, args := range [][]string{
		{"git", "status"},
		{"secret", "ls"},
	} {
		if _, _, err := runCLIWithDeps(t, deps, "", args...); err != nil {
			t.Fatalf("%v: err = %v, want nil (held lock must not matter)", args, err)
		}
	}

	after, err := os.Stat(deps.LockPath)
	if err != nil {
		t.Fatalf("stat lock after stateless admin commands: %v", err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("stateless admin commands replaced the lock file; they must not touch it")
	}
	if !after.ModTime().Equal(before.ModTime()) {
		t.Fatal("stateless admin commands modified the lock file; they must not touch it")
	}
}

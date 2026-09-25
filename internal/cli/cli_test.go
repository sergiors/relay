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
)

// testDeps returns CLI dependencies whose state DB, worker socket, and process
// lock all live under fresh per-test temp locations. The socket lives under
// /tmp (not t.TempDir's /var/folders on macOS) to stay inside the ~104-byte
// Unix socket sun_path limit. It replaces the former package-level path globals
// (statePath, runtimeSocketPath, startLockPath) with per-test values, so tests
// never touch /var/lib/relay or /run/relay. Start defaults to a no-op runner so
// an accidental `start` never launches the real worker; tests asserting
// dispatch override deps.Start directly. A few unrelated seams (the CLI git
// dir globals and secretsPath) remain package vars; tests that mutate them
// restore the previous value via t.Cleanup.
func testDeps(t *testing.T) Dependencies {
	t.Helper()
	dir := t.TempDir()
	sockDir, err := os.MkdirTemp("/tmp", "relay-cli-sock-")
	if err != nil {
		t.Fatalf("short temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	return Dependencies{
		StatePath:  filepath.Join(dir, "db.sqlite3"),
		SocketPath: filepath.Join(sockDir, "relay.sock"),
		LockPath:   filepath.Join(dir, "relay.lock"),
		Start:      func(*slog.Logger) error { return nil },
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
// and the supplied deps) and runs it against args. Errors flow into the returned
// error, never onto the ErrWriter (printing happens in cmd/main.go).
func runCLIWithLoggerAndDeps(t *testing.T, logger *slog.Logger, deps Dependencies, stdin string, args ...string) (
	stdout, stderr string, err error) {
	t.Helper()
	var out, errOut bytes.Buffer
	cmd := New(logger, &out, deps)
	if stdin != "" {
		cmd.Reader = strings.NewReader(stdin)
	}
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
	commands := []string{"start", "function", "dlq", "secret", "git", "stats", "health"}
	for _, flag := range []string{"--help", "-h"} {
		var output bytes.Buffer
		cmd := New(slog.New(slog.NewTextHandler(io.Discard, nil)), &output, testDeps(t))
		err := cmd.Run(context.Background(), []string{"relay", flag})
		if err != nil {
			t.Fatalf("%s: err = %v, want nil", flag, err)
		}
		out := output.String()
		// Scope to the COMMANDS block: the root Usage line also contains words
		// like "function", so an unanchored search would not prove listing.
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
	// Only consider the COMMANDS block; the root Usage line ("function event
	// relay") also contains the word "function", so an unanchored index of the
	// whole output would point at the header.
	cmds := out
	if i := strings.Index(cmds, "COMMANDS:"); i >= 0 {
		cmds = cmds[i:]
	}
	si := strings.Index(cmds, "start")
	fi := strings.Index(cmds, "function")
	if si == -1 || fi == -1 || si > fi {
		t.Fatalf("start should be listed before function:\n%s", out)
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

// TestNonStartCommandsDoNotAcquireLock pins that administrative commands never
// touch the start lock: with the lock held elsewhere, they still succeed and the
// lock file is not created or modified by them.
func TestNonStartCommandsDoNotAcquireLock(t *testing.T) {
	deps := testDeps(t)
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

	// deps.StatePath is a fresh temp DB, so `function ls` never touches
	// /var/lib/relay (the lock is the only path under test here).
	if _, _, err := runCLIWithDeps(t, deps, "", "function", "ls"); err != nil {
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

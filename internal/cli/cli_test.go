package cli

import (
	"bytes"
	"context"
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
// (statePath, runtimeSocketPath, startLockPath): no test mutates shared state,
// so tests are safe under -race and never touch /var/lib/relay or /run/relay.
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
	var out, errOut bytes.Buffer
	cmd := New(slog.New(slog.NewTextHandler(io.Discard, nil)), &out, deps)
	if stdin != "" {
		cmd.Reader = strings.NewReader(stdin)
	}
	cmd.ErrWriter = &errOut
	err = cmd.Run(t.Context(), append([]string{"relay"}, args...))
	return out.String(), errOut.String(), err
}

// Root --help/-h prints the root help to stdout and exits 0, listing every
// command. This literally follows the user-required pattern: build the tree
// with New (an injected buffer writer and deps), then call cmd.Run directly with
// a ctx and the args including the program name — no global stdout swapping or
// subprocess.
func TestRootHelp(t *testing.T) {
	for _, flag := range []string{"--help", "-h"} {
		var output bytes.Buffer
		cmd := New(slog.New(slog.NewTextHandler(io.Discard, nil)), &output, testDeps(t))
		err := cmd.Run(context.Background(), []string{"relay", flag})
		if err != nil {
			t.Fatalf("%s: err = %v, want nil", flag, err)
		}
		for _, want := range []string{
			"start",
			"function",
			"health",
			"stats",
			"secret",
			"git",
		} {
			if !strings.Contains(output.String(), want) {
				t.Fatalf("%s: stdout missing %q:\n%s", flag, want, output.String())
			}
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
	called := false
	orig := startRun
	startRun = func(l *slog.Logger) error { called = true; return nil }
	defer func() { startRun = orig }()

	out, _, err := runCLI(t, "", "start", "--help")
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
	origRun := startRun
	startRun = func(l *slog.Logger) error { called = true; return nil }
	defer func() { startRun = origRun }()

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
	orig := startRun
	startRun = func(l *slog.Logger) error { called = true; return nil }
	defer func() { startRun = orig }()

	if _, _, err := runCLIWithDeps(t, deps, "", "start"); err != nil {
		t.Fatalf("start: err = %v, want nil", err)
	}
	if !called {
		t.Fatal("start did not delegate to the worker startup path")
	}
}

// TestInformationalCommandsNeverStartWorker pins that no informational CLI
// command accidentally starts the runtime.
func TestInformationalCommandsNeverStartWorker(t *testing.T) {
	called := false
	orig := startRun
	startRun = func(l *slog.Logger) error { called = true; return nil }
	defer func() { startRun = orig }()

	// --help and an unknown command must not reach the start hook.
	_, _, _ = runCLI(t, "", "--help")
	_, _, _ = runCLI(t, "", "bogus")
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
	orig := startRun
	startRun = func(l *slog.Logger) error { called = true; return nil }
	defer func() { startRun = orig }()

	_, _, err = runCLIWithDeps(t, deps, "", "start")
	if err == nil || err.Error() != "relay start is already running" {
		t.Fatalf("err = %v, want %q", err, "relay start is already running")
	}
	if called {
		t.Fatal("already-running start must not invoke the worker startup path")
	}
}

// TestStartReleasesLockAfterRun verifies the deferred release: once a start run
// returns, the lock is free again for a subsequent run.
func TestStartReleasesLockAfterRun(t *testing.T) {
	deps := testDeps(t)
	orig := startRun
	startRun = func(l *slog.Logger) error { return nil }
	defer func() { startRun = orig }()

	for i := 0; i < 2; i++ {
		if _, _, err := runCLIWithDeps(t, deps, "", "start"); err != nil {
			t.Fatalf("start run %d: %v", i+1, err)
		}
	}
}

// TestNonStartCommandsDoNotAcquireLock pins that administrative commands never
// touch the start lock: a lock held elsewhere must not affect them.
func TestNonStartCommandsDoNotAcquireLock(t *testing.T) {
	deps := testDeps(t)
	held, err := processlock.Acquire(deps.LockPath)
	if err != nil {
		t.Fatalf("pre-acquire lock: %v", err)
	}
	defer held.Close()

	// deps.StatePath is a fresh temp DB, so `function ls` never touches
	// /var/lib/relay (the lock is the only path under test here).
	if _, _, err := runCLIWithDeps(t, deps, "", "function", "ls"); err != nil {
		t.Fatalf("function ls err = %v, want nil (held lock must not matter)", err)
	}
	if _, _, err := runCLI(t, "", "--help"); err != nil {
		t.Fatalf("--help err = %v, want nil", err)
	}
}

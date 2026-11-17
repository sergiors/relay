package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	urfavecli "github.com/urfave/cli/v3"

	"relay/internal/cli"
	"relay/internal/processlock"
	"relay/internal/worker"
)

// TestRunCommand_Success pins the success path of the process boundary: a nil
// error maps to exit code 0 and writes nothing to stderr, so a successful
// command exits 0 without emitting output.
func TestRunCommand_Success(t *testing.T) {
	var stderr bytes.Buffer

	if code := runCommand(func() error { return nil }, &stderr, discardLogger()); code != 0 {
		t.Fatalf("runCommand returned %d, want 0", code)
	}
	if got := stderr.String(); got != "" {
		t.Fatalf("runCommand wrote %q to stderr, want no output", got)
	}
}

// TestRunCommand_GenericFailure pins the generic runtime-failure path: a plain
// error maps to exit code 1 and is printed exactly once to stderr.
func TestRunCommand_GenericFailure(t *testing.T) {
	var stderr bytes.Buffer
	err := errors.New("boom")

	if code := runCommand(func() error { return err }, &stderr, discardLogger()); code != 1 {
		t.Fatalf("runCommand returned %d, want 1", code)
	}
	if got, want := stderr.String(), "boom\n"; got != want {
		t.Fatalf("stderr = %q, want %q", got, want)
	}
}

// TestRunCommand_ExitCoder pins that a urfave/cli ExitCoder (the cli.Exit(msg, 2)
// usage errors the command tree returns) propagates its explicit exit code and
// is still printed exactly once — not duplicated, not swallowed.
func TestRunCommand_ExitCoder(t *testing.T) {
	var stderr bytes.Buffer
	err := urfavecli.Exit("usage error", 2)

	if code := runCommand(func() error { return err }, &stderr, discardLogger()); code != 2 {
		t.Fatalf("runCommand returned %d, want 2", code)
	}
	if got, want := stderr.String(), "usage error\n"; got != want {
		t.Fatalf("stderr = %q, want %q", got, want)
	}
}

// TestRunCommand_ExitCoderWrapped pins that the ExitCoder lookup uses errors.As,
// so a wrapped ExitCoder still yields its explicit code rather than the generic
// 1. Commands may wrap usage errors before returning them.
func TestRunCommand_ExitCoderWrapped(t *testing.T) {
	var stderr bytes.Buffer
	err := fmt.Errorf("context: %w", urfavecli.Exit("wrapped usage", 3))

	if code := runCommand(func() error { return err }, &stderr, discardLogger()); code != 3 {
		t.Fatalf("runCommand returned %d, want 3", code)
	}
	if got, want := stderr.String(), "context: wrapped usage\n"; got != want {
		t.Fatalf("stderr = %q, want %q", got, want)
	}
}

// discardLogger is the process logger used by tests that do not assert
// structured output.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestRunCommand_StartFailureUsesStructuredError pins the start-specific
// process-boundary policy: a `relay start` lifecycle failure without a worker
// phase (a CLI lock or setup failure) is reported as ONE structured error event
// — "Worker startup failed" — with no plain stderr line, and the full wrapped
// cause, including the PostgreSQL startup context, is retained. A successful
// exit and administrative errors keep their existing paths.
func TestRunCommand_StartFailureUsesStructuredError(t *testing.T) {
	deps := cli.Dependencies{
		LockPath: filepath.Join(t.TempDir(), "relay.lock"),
		Start: func(*slog.Logger) error {
			return fmt.Errorf("state: open failed: %w", errors.New(
				"open state: connect to PostgreSQL failed: dial tcp db.example.com:5432: connect: connection refused"))
		},
	}
	var stderr, logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))

	code := runCommand(func() error {
		return cli.New(logger, io.Discard, deps).Run(context.Background(), []string{"relay", "start"})
	}, &stderr, logger)

	if code != 1 {
		t.Fatalf("runCommand returned %d, want 1", code)
	}
	if got := stderr.String(); got != "" {
		t.Fatalf("structured start failure must not duplicate onto stderr, got %q", got)
	}
	if got := strings.Count(logs.String(), `msg="Worker startup failed"`); got != 1 {
		t.Fatalf("structured start failure emitted %d times, want exactly 1:\n%s", got, logs.String())
	}
	if got := strings.Count(logs.String(), "level=ERROR"); got != 1 {
		t.Fatalf("structured start failure logged %d error events, want exactly 1 (no duplicate reporting):\n%s",
			got, logs.String())
	}
	for _, want := range []string{
		"state: open failed",
		"connect to PostgreSQL failed",
		"db.example.com:5432",
		"connection refused",
	} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("structured start failure missing %q:\n%s", want, logs.String())
		}
	}
}

// TestRunCommand_StartFailurePhaseSelectsReportMessage pins the startup-vs-
// runtime reporting of `relay start` failures: a failure before the worker is
// ready to consume — whether it carries the startup phase or no phase at all
// (CLI lock/setup errors never reach worker.Run) — is reported as "Worker
// startup failed", while a failure after the ready boundary is reported as
// "Worker stopped". Every case is exactly one structured error event with the
// full cause retained and no stderr duplication.
func TestRunCommand_StartFailurePhaseSelectsReportMessage(t *testing.T) {
	cases := []struct {
		name     string
		startErr error
		wantMsg  string
	}{
		{
			name:     "CLI lock or setup failure carries no phase",
			startErr: errors.New("relay start is already running"),
			wantMsg:  `msg="Worker startup failed"`,
		},
		{
			name: "worker failure before ready is startup",
			startErr: &worker.RunError{
				Phase: worker.RunPhaseStartup,
				Err: errors.New(
					"open state: connect to PostgreSQL failed: dial tcp db:5432: connection refused"),
			},
			wantMsg: `msg="Worker startup failed"`,
		},
		{
			name: "worker failure after ready is runtime",
			startErr: &worker.RunError{
				Phase: worker.RunPhaseRuntime,
				Err:   errors.New("consume: redis: connection reset by peer"),
			},
			wantMsg: `msg="Worker stopped"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			deps := cli.Dependencies{
				LockPath: filepath.Join(t.TempDir(), "relay.lock"),
				Start:    func(*slog.Logger) error { return tc.startErr },
			}
			var stderr, logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))

			code := runCommand(func() error {
				return cli.New(logger, io.Discard, deps).Run(
					context.Background(), []string{"relay", "start"})
			}, &stderr, logger)

			if code != 1 {
				t.Fatalf("runCommand returned %d, want 1", code)
			}
			if got := stderr.String(); got != "" {
				t.Fatalf("start failure must not duplicate onto stderr, got %q", got)
			}
			if got := strings.Count(logs.String(), tc.wantMsg); got != 1 {
				t.Fatalf("report %s emitted %d times, want exactly 1:\n%s",
					tc.wantMsg, got, logs.String())
			}
			if got := strings.Count(logs.String(), "level=ERROR"); got != 1 {
				t.Fatalf("logged %d error events, want exactly 1 (no duplicate reporting):\n%s",
					got, logs.String())
			}
			if !strings.Contains(logs.String(), tc.startErr.Error()) {
				t.Fatalf("report missing the full cause %q:\n%s",
					tc.startErr.Error(), logs.String())
			}
		})
	}
}

// TestRunCommand_StartAlreadyRunningLockReportsStructuredStartup pins the real
// single-instance lock path end to end: while another holder owns the start
// lock, `relay start` fails fast before any worker startup, never reaches the
// worker, and is reported as one structured startup event naming the lock —
// not as a plain stderr line.
func TestRunCommand_StartAlreadyRunningLockReportsStructuredStartup(t *testing.T) {
	lockPath := filepath.Join(t.TempDir(), "relay.lock")
	held, err := processlock.Acquire(lockPath)
	if err != nil {
		t.Fatalf("acquire start lock: %v", err)
	}
	defer held.Close()

	deps := cli.Dependencies{
		LockPath: lockPath,
		Start: func(*slog.Logger) error {
			t.Fatal("start must fail fast on the held lock, never reaching the worker")
			return nil
		},
	}
	var stderr, logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))

	code := runCommand(func() error {
		return cli.New(logger, io.Discard, deps).Run(context.Background(), []string{"relay", "start"})
	}, &stderr, logger)

	if code != 1 {
		t.Fatalf("runCommand returned %d, want 1", code)
	}
	if got := stderr.String(); got != "" {
		t.Fatalf("already-running failure must not go to stderr, got %q", got)
	}
	for _, want := range []string{
		`msg="Worker startup failed"`,
		"relay start is already running",
	} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("structured report missing %q:\n%s", want, logs.String())
		}
	}
	if got := strings.Count(logs.String(), "level=ERROR"); got != 1 {
		t.Fatalf("logged %d error events, want exactly 1:\n%s", got, logs.String())
	}
}

// TestRunCommand_GracefulShutdownReportsNothing pins that a graceful `relay
// start` shutdown (the worker returning nil after a signal) exits 0 with
// nothing on stderr and no error-level log event: only failures are reported.
func TestRunCommand_GracefulShutdownReportsNothing(t *testing.T) {
	deps := cli.Dependencies{
		LockPath: filepath.Join(t.TempDir(), "relay.lock"),
		Start:    func(*slog.Logger) error { return nil },
	}
	var stderr, logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))

	code := runCommand(func() error {
		return cli.New(logger, io.Discard, deps).Run(context.Background(), []string{"relay", "start"})
	}, &stderr, logger)

	if code != 0 {
		t.Fatalf("runCommand returned %d, want 0 for a graceful shutdown", code)
	}
	if got := stderr.String(); got != "" {
		t.Fatalf("graceful shutdown wrote %q to stderr, want no output", got)
	}
	if strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("graceful shutdown must not log an error-level event:\n%s", logs.String())
	}
}

// TestRunCommand_AdminFailureStaysOnStderr pins that administrative commands
// keep their plain stderr report and never reach the structured start path.
func TestRunCommand_AdminFailureStaysOnStderr(t *testing.T) {
	var stderr, logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))

	code := runCommand(func() error { return errors.New("database: DATABASE_URL is required") }, &stderr, logger)

	if code != 1 {
		t.Fatalf("runCommand returned %d, want 1", code)
	}
	if got, want := stderr.String(), "database: DATABASE_URL is required\n"; got != want {
		t.Fatalf("stderr = %q, want %q", got, want)
	}
	if got := logs.String(); got != "" {
		t.Fatalf("admin failure must not be logged structurally, got %q", got)
	}
}

// TestRunCommand_StartUsageErrorKeepsNativeExitAndStderr pins that a native
// usage error from `relay start` still uses its ExitCoder code and plain
// message rather than the structured failure path.
func TestRunCommand_StartUsageErrorKeepsNativeExitAndStderr(t *testing.T) {
	var stderr, logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	deps := cli.Dependencies{
		LockPath: filepath.Join(t.TempDir(), "relay.lock"),
		Start: func(*slog.Logger) error {
			t.Fatal("a usage error must not reach the worker startup path")
			return nil
		},
	}

	code := runCommand(func() error {
		return cli.New(logger, io.Discard, deps).Run(
			context.Background(), []string{"relay", "start", "extra"})
	}, &stderr, logger)

	if code != 2 {
		t.Fatalf("runCommand returned %d, want the native usage exit code 2", code)
	}
	if got, want := stderr.String(), "start: too many arguments\n"; got != want {
		t.Fatalf("stderr = %q, want %q", got, want)
	}
	if got := logs.String(); got != "" {
		t.Fatalf("native usage/help output must not be logged structurally, got %q", got)
	}
}

// TestRunCommand_StartHelpIsNotReportedAsFailure pins that native help remains
// a successful, non-logged command.
func TestRunCommand_StartHelpIsNotReportedAsFailure(t *testing.T) {
	var stderr, logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	deps := cli.Dependencies{
		LockPath: filepath.Join(t.TempDir(), "relay.lock"),
		Start: func(*slog.Logger) error {
			t.Fatal("help must not reach the worker startup path")
			return nil
		},
	}

	code := runCommand(func() error {
		return cli.New(logger, io.Discard, deps).Run(
			context.Background(), []string{"relay", "start", "--help"})
	}, &stderr, logger)

	if code != 0 {
		t.Fatalf("runCommand returned %d, want 0 for native help", code)
	}
	if got := stderr.String(); got != "" {
		t.Fatalf("help must not write to stderr, got %q", got)
	}
	if got := logs.String(); got != "" {
		t.Fatalf("help must not be reported as a failure, got %q", got)
	}
}

// moduleRoot locates the module root by walking up from this test file's own
// directory until it finds go.mod. Resolving from runtime.Caller keeps the
// guardrail scan below independent of the test working directory.
func moduleRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller could not locate cmd test")
	}
	dir := filepath.Dir(thisFile)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("walked to filesystem root from %s without finding go.mod", filepath.Dir(thisFile))
		}
		dir = parent
	}
}

// TestInternalPackagesDoNotCallOsExit pins the process-boundary contract: only
// cmd/main.go owns os.Exit. If internal command, worker, or config code called
// os.Exit it would tear down the process without unwinding the command
// boundary, breaking error propagation and testability. The scan walks EVERY
// production Go file under internal recursively (not just cli/worker), so a
// new os.Exit anywhere in the internal tree — config included — is caught.
// Test files are skipped (subprocess helpers legitimately use os.Exit). The
// scan parses the AST (not raw source) so comments discussing os.Exit, like
// worker.go's, are not false positives.
func TestInternalPackagesDoNotCallOsExit(t *testing.T) {
	root := moduleRoot(t)
	internalDir := filepath.Join(root, "internal")
	fset := token.NewFileSet()
	scanned := 0

	err := filepath.WalkDir(internalDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}
		scanned++
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			ident, ok := sel.X.(*ast.Ident)
			if ok && ident.Name == "os" && sel.Sel.Name == "Exit" {
				t.Errorf("%s references os.Exit; only cmd/main.go owns the process exit", path)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("scan internal packages: %v", err)
	}
	if scanned == 0 {
		t.Fatalf("scanned no production files under %s; the guard is not effective", internalDir)
	}
}

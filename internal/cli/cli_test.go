package cli

import (
	"bytes"
	"context"
	"io"
	"log"
	"strings"
	"testing"
)

// runCLI builds the command tree with New and runs it against args (which
// include the program name slot urfave's parser consumes) with test-
// controllable Reader/Writer/ErrWriter buffers and a discard logger, and
// returns the captured output/error streams plus the error the command tree
// returned. Errors flow into the returned error, not onto the injected
// ErrWriter — printing happens in cmd/main.go, which tests do not execute.
func runCLI(t *testing.T, stdin string, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	var out, errOut bytes.Buffer
	cmd := New(log.New(io.Discard, "", 0), &out)
	if stdin != "" {
		cmd.Reader = strings.NewReader(stdin)
	}
	cmd.ErrWriter = &errOut
	err = cmd.Run(t.Context(), append([]string{"relay"}, args...))
	return out.String(), errOut.String(), err
}

// TestUsageOutputAndExitCode exercises the CLI entrypoint dispatch without any
// dependency: a missing or unknown command returns a usage error, and the
// binary must never attempt to start the runtime. The error is RETURNED (the
// root ExitErrHandler is a silent no-op); cmd/main.go prints it via its logger
// and exits 1.
func TestUsageOutputAndExitCode(t *testing.T) {
	for _, args := range [][]string{nil, {"bogus"}} {
		_, _, err := runCLI(t, "", args...)
		if err == nil || err.Error() == "" {
			t.Fatalf("args %v: missing returned error message: %v", args, err)
		}
	}
}

// Root --help/-h prints the root help to stdout and exits 0, listing every
// command. This literally follows the user-required pattern: build the tree
// with New (an injected buffer writer), then call cmd.Run directly with a
// ctx and the args including the program name — no global stdout swapping or
// subprocess.
func TestRootHelp(t *testing.T) {
	for _, flag := range []string{"--help", "-h"} {
		var output bytes.Buffer
		cmd := New(log.New(io.Discard, "", 0), &output)
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
	startRun = func(l *log.Logger) error { called = true; return nil }
	defer func() { startRun = orig }()

	out, _, err := runCLI(t, "", "start", "--help")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if !strings.Contains(out, "Start Relay") {
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
	if err == nil || !strings.Contains(err.Error(), "start: too many arguments") {
		t.Fatalf("returned error missing usage message: %v", err)
	}
}

// TestStartDelegatesToWorker verifies the "start" command dispatches to the
// worker startup path without actually launching the runtime.
func TestStartDelegatesToWorker(t *testing.T) {
	called := false
	orig := startRun
	startRun = func(l *log.Logger) error { called = true; return nil }
	defer func() { startRun = orig }()

	if _, _, err := runCLI(t, "", "start"); err != nil {
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
	startRun = func(l *log.Logger) error { called = true; return nil }
	defer func() { startRun = orig }()

	// --help and an unknown command must not reach the start hook.
	_, _, _ = runCLI(t, "", "--help")
	_, _, _ = runCLI(t, "", "bogus")
	if called {
		t.Fatal("informational CLI commands must not start the worker")
	}
}

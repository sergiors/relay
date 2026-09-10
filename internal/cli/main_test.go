package cli

import (
	"bytes"
	"log"
	"os"
	"strings"
	"testing"
)

// usage tests exercise the CLI entrypoint dispatch without any dependency: the
// binary must print usage and exit 2 for missing or unknown commands, and must
// never attempt to start the runtime.
func TestUsageOutputAndExitCode(t *testing.T) {
	for _, cmd := range [][]string{nil, {"bogus"}} {
		var code int
		errOut := captureErr(t, func() {
			code = runCLI(cmd)
		})
		if code != 2 {
			t.Fatalf("args %v: exit = %d, want 2", cmd, code)
		}
		if !strings.Contains(errOut, "Usage:") {
			t.Fatalf("args %v: stderr missing usage text: %q", cmd, errOut)
		}
	}
}

// Root --help/-h prints the root help to stdout and exits 0.
func TestRootHelp(t *testing.T) {
	for _, flag := range []string{"--help", "-h"} {
		var code int
		out := capture(t, func() {
			code = runCLI([]string{flag})
		})
		if code != 0 {
			t.Fatalf("%s: exit = %d, want 0", flag, code)
		}
		for _, want := range []string{
			"relay COMMAND",
			"function",
			"health",
			"stats",
			"Run 'relay COMMAND --help' for more information on a command.",
		} {
			if !strings.Contains(out, want) {
				t.Fatalf("%s: stdout missing %q:\n%s", flag, want, out)
			}
		}
	}
}

// Misplaced --help at the root is an unknown command: exit 2 with error+usage
// on stderr.
func TestRootHelpMisplaced(t *testing.T) {
	var code int
	errOut := captureErr(t, func() {
		code = runCLI([]string{"--help", "function"})
	})
	if code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
	if !strings.Contains(errOut, "Error:") || !strings.Contains(errOut, "relay COMMAND") {
		t.Fatalf("stderr missing error+usage: %q", errOut)
	}
}

// capture runs fn capturing stdout to a string.
func capture(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	var buf bytes.Buffer
	done := make(chan struct{})
	go func() {
		_, _ = buf.ReadFrom(r)
		close(done)
	}()
	fn()
	_ = w.Close()
	os.Stdout = old
	<-done
	return buf.String()
}

// captureErr runs fn capturing stderr to a string.
func captureErr(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w
	var buf bytes.Buffer
	done := make(chan struct{})
	go func() {
		_, _ = buf.ReadFrom(r)
		close(done)
	}()
	fn()
	_ = w.Close()
	os.Stderr = old
	<-done
	return buf.String()
}

// TestRootHelpContainsStart verifies the root help lists the start command
// first and still lists the administrative commands.
func TestRootHelpContainsStart(t *testing.T) {
	var code int
	out := capture(t, func() {
		code = runCLI([]string{"--help"})
	})
	if code != 0 {
		t.Fatalf("exit = %d, want 0", code)
	}
	for _, want := range []string{
		"start",
		"Start Relay",
		"function",
		"health",
		"secret",
		"stats",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("stdout missing %q:\n%s", want, out)
		}
	}
	// start must be listed first.
	if !strings.Contains(out, "start") || strings.Index(out, "start") > strings.Index(out, "function") {
		t.Fatalf("start should be listed before function:\n%s", out)
	}
}

// TestStartHelp verifies `relay start --help` prints the start usage (with
// foreground wording) and exits 0 without starting anything.
func TestStartHelp(t *testing.T) {
	called := false
	orig := startRun
	startRun = func(l *log.Logger) int { called = true; return 0 }
	defer func() { startRun = orig }()

	var code int
	out := capture(t, func() {
		code = runCLI([]string{"start", "--help"})
	})
	if code != 0 {
		t.Fatalf("exit = %d, want 0", code)
	}
	for _, want := range []string{"Usage:", "foreground"} {
		if !strings.Contains(out, want) {
			t.Fatalf("stdout missing %q:\n%s", want, out)
		}
	}
	if called {
		t.Fatal("start --help must not invoke the worker startup path")
	}
}

// TestStartTooManyArgs verifies `relay start extra` is a usage error (exit 2).
func TestStartTooManyArgs(t *testing.T) {
	var code int
	errOut := captureErr(t, func() {
		code = runCLI([]string{"start", "extra"})
	})
	if code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
	if !strings.Contains(errOut, "Error:") || !strings.Contains(errOut, "relay start") {
		t.Fatalf("stderr missing error+usage: %q", errOut)
	}
}

// TestStartDelegatesToWorker verifies the "start" command dispatches to the
// worker startup path without actually launching the runtime.
func TestStartDelegatesToWorker(t *testing.T) {
	called := false
	orig := startRun
	startRun = func(l *log.Logger) int { called = true; return 0 }
	defer func() { startRun = orig }()

	if code := runCLI([]string{"start"}); code != 0 {
		t.Fatalf("start: exit = %d, want 0", code)
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
	startRun = func(l *log.Logger) int { called = true; return 0 }
	defer func() { startRun = orig }()

	// --help and an unknown command must not reach the start hook.
	_ = runCLI([]string{"--help"})
	_ = runCLI([]string{"bogus"})
	if called {
		t.Fatal("informational CLI commands must not start the worker")
	}
}

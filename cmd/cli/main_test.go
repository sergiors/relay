package main

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

// usage tests exercise the CLI entrypoint dispatch without any dependency: the
// binary must print usage and exit 2 for missing or unknown commands, and must
// never attempt to start the worker.
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
		if strings.Contains(errOut, "relay-worker") {
			t.Fatalf("args %v: stderr should not reference the worker binary: %q", cmd, errOut)
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
		if strings.Contains(out, "relay-worker") {
			t.Fatalf("%s: stdout should not reference the worker binary:\n%s", flag, out)
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

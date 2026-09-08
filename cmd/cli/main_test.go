package main

import (
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
		if !strings.Contains(errOut, "relay-worker") {
			t.Fatalf("args %v: stderr should reference the worker binary: %q", cmd, errOut)
		}
	}
}

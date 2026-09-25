package runtime

import (
	"testing"
)

// TestExecutionEndpoints pins the pure NetworkingConfig builder used for
// execution containers: each named network exactly once, empty names skipped,
// and nil for no networks (so a worker with NETWORKS unset sends no
// NetworkingConfig at all).
func TestExecutionEndpoints(t *testing.T) {
	if got := executionEndpoints(nil); got != nil {
		t.Fatalf("nil networks = %v, want nil", got)
	}
	if got := executionEndpoints([]string{}); got != nil {
		t.Fatalf("empty networks = %v, want nil", got)
	}
	if got := executionEndpoints([]string{"", "  "}); got != nil {
		// "  " is not empty; only the empty string is skipped. A whitespace
		// name is the template parser's job to reject; here the entry is kept.
		if len(got) != 1 {
			t.Fatalf("whitespace-only name should be kept, got %v", got)
		}
	}
	got := executionEndpoints([]string{"backend", "frontend", "backend"})
	if len(got) != 2 {
		t.Fatalf("endpoints = %v, want backend+frontend deduped", got)
	}
	if _, ok := got["backend"]; !ok {
		t.Fatalf("missing backend: %v", got)
	}
	if _, ok := got["frontend"]; !ok {
		t.Fatalf("missing frontend: %v", got)
	}
}

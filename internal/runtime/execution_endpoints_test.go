package runtime

import (
	"testing"
)

// TestExecutionEndpoints pins the pure NetworkingConfig builder used for
// execution containers: each named network exactly once, empty names skipped,
// and nil for no networks. In production the input is executionNetworks() —
// the internal control network plus the optional NETWORKS set — which is never
// empty once the control network is provisioned, so the Manager path always
// sends at least the control endpoint; the nil branch only guards a caller
// passing no networks at all (a directly constructed, non-production shape).
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
	// The production shape: control network plus the optional app set arrives
	// pre-canonicalized from executionNetworks(); the builder must keep every
	// entry exactly once.
	got := executionEndpoints([]string{unitControlNetwork, "backend", "frontend", unitControlNetwork})
	if len(got) != 3 {
		t.Fatalf("endpoints = %v, want control+backend+frontend deduped", got)
	}
	for _, want := range []string{unitControlNetwork, "backend", "frontend"} {
		if _, ok := got[want]; !ok {
			t.Fatalf("missing %s: %v", want, got)
		}
	}
}

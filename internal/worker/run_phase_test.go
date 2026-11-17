package worker

import (
	"errors"
	"fmt"
	"testing"
)

// TestRunPhaseOfClassifiesRunErrors pins the phase carrier the process boundary
// reads to choose between "Worker startup failed" and "Worker stopped": a
// RunError reports its phase through errors.As even when wrapped, it preserves
// the wrapped failure's identity and message (so the structured report keeps
// the full cause), and an error that did not originate from worker.Run — an
// administrative failure or a CLI setup error — carries no phase.
func TestRunPhaseOfClassifiesRunErrors(t *testing.T) {
	cause := errors.New("open state: connect to PostgreSQL failed: connection refused")

	startup := &RunError{Phase: RunPhaseStartup, Err: cause}
	if phase, ok := RunPhaseOf(startup); !ok || phase != RunPhaseStartup {
		t.Fatalf("RunPhaseOf(startup) = (%q, %v), want (%q, true)",
			phase, ok, RunPhaseStartup)
	}
	// Identity and message preservation: the boundary reports the original
	// failure, not a replacement error.
	if got, want := startup.Error(), cause.Error(); got != want {
		t.Fatalf("RunError text = %q, want the wrapped cause %q", got, want)
	}
	if !errors.Is(startup, cause) {
		t.Fatal("RunError must keep errors.Is compatibility with its cause")
	}

	runtimeCause := errors.New("consume: redis: connection reset by peer")
	runtime := &RunError{Phase: RunPhaseRuntime, Err: runtimeCause}
	wrapped := fmt.Errorf("relay start: %w", runtime)
	if phase, ok := RunPhaseOf(wrapped); !ok || phase != RunPhaseRuntime {
		t.Fatalf("RunPhaseOf(wrapped runtime) = (%q, %v), want (%q, true)",
			phase, ok, RunPhaseRuntime)
	}
	var inner *RunError
	if !errors.As(wrapped, &inner) || inner != runtime {
		t.Fatalf("errors.As(%v) must recover the RunError through a wrap", wrapped)
	}
	if !errors.Is(wrapped, runtimeCause) {
		t.Fatal("a wrapped RunError must keep errors.Is compatibility with its cause")
	}

	// Errors outside worker.Run carry no phase.
	if phase, ok := RunPhaseOf(cause); ok {
		t.Fatalf("RunPhaseOf(plain error) = (%q, %v), want no phase", phase, ok)
	}
	if phase, ok := RunPhaseOf(nil); ok {
		t.Fatalf("RunPhaseOf(nil) = (%q, %v), want no phase", phase, ok)
	}
}

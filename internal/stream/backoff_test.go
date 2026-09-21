package stream

import (
	"testing"
	"time"
)

func TestBackoffProgressionAndCap(t *testing.T) {
	// Deterministic: no jitter (identity) so we assert exact table values.
	b := newBackoff(nil, func(f float64) float64 { return f })
	want := []time.Duration{1, 2, 4, 8, 15, 30, 30, 30}
	for i, w := range want {
		if got := b.next(); got != w*time.Second {
			t.Fatalf("step %d: got %s, want %s", i, got, w*time.Second)
		}
	}
}

func TestBackoffReset(t *testing.T) {
	b := newBackoff(nil, func(f float64) float64 { return f })
	b.next()
	b.next()
	b.reset()
	if got := b.next(); got != time.Second {
		t.Fatalf("after reset, got %s, want 1s", got)
	}
}

// TestBackoffJitterScalesExactFactor pins that next applies the injected jitter
// function to the base delay exactly (here the max 1.2x and min 0.8x factors),
// so the bounds test below can trust the scaling arithmetic.
func TestBackoffJitterScalesExactFactor(t *testing.T) {
	b := newBackoff(nil, func(f float64) float64 { return f * 1.2 })
	if got := b.next(); got != 1200*time.Millisecond {
		t.Fatalf("max jitter: got %s, want 1.2s", got)
	}
	b.reset()
	b = newBackoff(nil, func(f float64) float64 { return f * 0.8 })
	if got := b.next(); got != 800*time.Millisecond {
		t.Fatalf("min jitter: got %s, want 800ms", got)
	}
}

func TestBackoffJitterNeverOutOfBounds(t *testing.T) {
	// The default rand-based jitter must never produce a value outside
	// [0.8, 1.2) of the base across many draws. Use a single-entry table so the
	// base stays constant and the jitter factor is what varies.
	b := newBackoff([]time.Duration{time.Second}, nil)
	for i := 0; i < 1000; i++ {
		got := b.next()
		if got < 800*time.Millisecond || got >= 1200*time.Millisecond {
			t.Fatalf("jitter out of bounds: %s", got)
		}
	}
}

// TestBackoffPeekDoesNotAdvance pins peek: it returns the current table value
// without advancing the step, so the recovery loop's ticker-paced peek never
// consumes the main read loop's backoff progression.
func TestBackoffPeekDoesNotAdvance(t *testing.T) {
	b := newBackoff([]time.Duration{1 * time.Second, 2 * time.Second}, func(f float64) float64 { return f })
	if got := b.peek(); got != time.Second {
		t.Fatalf("initial peek = %s, want 1s", got)
	}
	if got := b.peek(); got != time.Second {
		t.Fatalf("repeated peek = %s, want 1s (must not advance)", got)
	}
	// peek never advances, so next still starts at the first entry.
	if got := b.next(); got != time.Second {
		t.Fatalf("next after peek = %s, want 1s", got)
	}
	if got := b.peek(); got != 2*time.Second {
		t.Fatalf("peek after next = %s, want 2s", got)
	}
}

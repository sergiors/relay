package worker

import (
	"testing"

	"relay/internal/runner"
	"relay/internal/stream"
)

// TestEffectiveMaxConcurrencyNormalizes verifies the "Concurrency limits" log
// normalization: a value < 1 falls back to the runner's default, and a positive
// value passes through unchanged.
func TestEffectiveMaxConcurrencyNormalizes(t *testing.T) {
	cases := []struct {
		name string
		in   int
		want int
	}{
		{"zero falls back to default", 0, runner.DefaultMaxConcurrency},
		{"negative falls back to default", -3, runner.DefaultMaxConcurrency},
		{"positive passes through", 5, 5},
		{"one passes through", 1, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := effectiveMaxConcurrency(tc.in); got != tc.want {
				t.Fatalf("effectiveMaxConcurrency(%d) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

// TestEffectiveMaxBufferedNormalizes verifies the stream buffered-events
// normalization: a value < 1 falls back to the stream default, and a positive
// value passes through unchanged.
func TestEffectiveMaxBufferedNormalizes(t *testing.T) {
	cases := []struct {
		name string
		in   int
		want int
	}{
		{"zero falls back to default", 0, stream.DefaultMaxBufferedEvents},
		{"negative falls back to default", -1, stream.DefaultMaxBufferedEvents},
		{"positive passes through", 32, 32},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := effectiveMaxBuffered(tc.in); got != tc.want {
				t.Fatalf("effectiveMaxBuffered(%d) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

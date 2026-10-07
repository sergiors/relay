package worker

import (
	"testing"

	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/stream"
)

// TestEffectiveMaxConcurrentInvocationsNormalizes verifies the "Concurrency limits" log
// normalization: a value < 1 falls back to the runner's default, and a positive
// value passes through unchanged.
func TestEffectiveMaxConcurrentInvocationsNormalizes(t *testing.T) {
	cases := []struct {
		name string
		in   int
		want int
	}{
		{"zero falls back to default", 0, runner.DefaultMaxConcurrentInvocations},
		{"negative falls back to default", -3, runner.DefaultMaxConcurrentInvocations},
		{"positive passes through", 5, 5},
		{"one passes through", 1, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := effectiveMaxConcurrentInvocations(tc.in); got != tc.want {
				t.Fatalf("effectiveMaxConcurrentInvocations(%d) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

// TestEffectiveMaxConcurrentBuildsNormalizes verifies the build-preparation
// concurrency normalization: a value < 1 falls back to the runtime default, and
// a positive value passes through unchanged.
func TestEffectiveMaxConcurrentBuildsNormalizes(t *testing.T) {
	cases := []struct {
		name string
		in   int
		want int
	}{
		{"zero falls back to default", 0, runtime.DefaultMaxConcurrentBuilds},
		{"negative falls back to default", -2, runtime.DefaultMaxConcurrentBuilds},
		{"positive passes through", 4, 4},
		{"one passes through", 1, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := effectiveMaxConcurrentBuilds(tc.in); got != tc.want {
				t.Fatalf("effectiveMaxConcurrentBuilds(%d) = %d, want %d", tc.in, got, tc.want)
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

// TestEffectiveMaxEventBytesNormalizes verifies the raw-event byte-cap
// normalization: a value < 1 falls back to the stream default (zero must not
// mean unlimited), and a positive value passes through unchanged.
func TestEffectiveMaxEventBytesNormalizes(t *testing.T) {
	cases := []struct {
		name string
		in   int
		want int
	}{
		{"zero falls back to default", 0, stream.DefaultMaxEventBytes},
		{"negative falls back to default", -1, stream.DefaultMaxEventBytes},
		{"positive passes through", 1024, 1024},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := effectiveMaxEventBytes(tc.in); got != tc.want {
				t.Fatalf("effectiveMaxEventBytes(%d) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

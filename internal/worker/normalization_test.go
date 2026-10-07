package worker

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"relay/internal/bytesize"
	"relay/internal/config"
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

// TestMaxEventBytesLogRendering verifies the "Concurrency limits" log renders
// the effective raw-event byte cap as a human-readable binary size: the
// effective value is passed through bytesize.Format, so the default 256 KiB
// logs as "256KiB" and an unset/zero cap renders the enforced default rather
// than the raw config zero.
func TestMaxEventBytesLogRendering(t *testing.T) {
	cases := []struct {
		name string
		in   int
		want string
	}{
		{"default size", 256 << 10, "256KiB"},
		{"hard max", 1 << 20, "1MiB"},
		{"sub-unit byte count", 300000, "300000B"},
		{"zero renders enforced default", 0, "256KiB"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := bytesize.Format(int64(effectiveMaxEventBytes(tc.in))); got != tc.want {
				t.Fatalf("rendered max_event_bytes = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestLogConcurrencyLimitsEmitsStartupSummary captures the ACTUAL emitted
// startup "Concurrency limits" summary through the extracted logConcurrencyLimits
// seam (so no full worker startup — Docker, Redis, app loading — is exercised)
// and pins the wire format an operator reads in the startup log: the raw
// max_event_bytes byte cap is rendered as a human-readable binary size
// ("max_event_bytes=256KiB"), while the unrelated numeric limits stay raw
// numbers. A value of 0 renders the enforced default (256KiB), never the raw
// config zero.
func TestLogConcurrencyLimitsEmitsStartupSummary(t *testing.T) {
	cases := []struct {
		name      string
		cfg       config.Config
		wantBytes string
	}{
		{
			name: "loaded default 256KiB",
			cfg: config.Config{
				MaxConcurrentInvocations: 3,
				MaxConcurrentBuilds:      4,
				MaxWarmContainers:        5,
				MaxBufferedEvents:        32,
				MaxEventBytes:            config.DefaultMaxEventBytes,
			},
			wantBytes: "max_event_bytes=256KiB",
		},
		{
			name: "unset zero renders enforced default",
			cfg: config.Config{
				MaxConcurrentInvocations: 3,
				MaxConcurrentBuilds:      4,
				MaxWarmContainers:        5,
				MaxBufferedEvents:        32,
				MaxEventBytes:            0,
			},
			wantBytes: "max_event_bytes=256KiB",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
			logConcurrencyLimits(logger, tc.cfg)
			out := buf.String()

			if !strings.Contains(out, `msg="Concurrency limits"`) {
				t.Fatalf("emitted startup summary missing the \"Concurrency limits\" message:\n%s", out)
			}
			if !strings.Contains(out, tc.wantBytes) {
				t.Fatalf("emitted startup summary missing %q:\n%s", tc.wantBytes, out)
			}
			// The unrelated numeric limits must stay raw numbers, not be
			// formatted as sizes.
			for _, want := range []string{
				"max_concurrent_invocations=3",
				"max_concurrent_builds=4",
				"max_warm_containers=5",
				"max_buffered_events=32",
			} {
				if !strings.Contains(out, want) {
					t.Errorf("emitted startup summary missing raw %q:\n%s", want, out)
				}
			}
		})
	}
}

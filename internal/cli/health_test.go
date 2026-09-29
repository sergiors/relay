package cli

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"relay/internal/worker"
)

// fakeReadinessChecker is a deterministic worker.ReadinessChecker for CLI
// health tests.
type fakeReadinessChecker struct {
	ready  bool
	reason string
}

func (f fakeReadinessChecker) Ready(context.Context) (bool, string) { return f.ready, f.reason }

// startHealthSocket starts a real worker query socket at path with the given
// readiness checker wired, so `relay health` is exercised end to end against a
// real socket without Redis or Docker.
func startHealthSocket(t *testing.T, path string, checker worker.ReadinessChecker) {
	t.Helper()
	s, err := worker.NewSocketServer(
		path,
		fakePoolSnapshotter{pools: nil},
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("NewSocketServer: %v", err)
	}
	s.SetReadiness(checker)
	t.Cleanup(func() { _ = s.Close() })
}

// `relay health` reports healthy and exits 0 when the worker answers ready.
func TestHealthCommandHealthy(t *testing.T) {
	deps := testDeps(t)
	startHealthSocket(t, deps.SocketPath, fakeReadinessChecker{ready: true})

	out, _, err := runCLIWithDeps(t, deps, "", "health")
	if err != nil {
		t.Fatalf("health: err = %v, want nil", err)
	}
	if out != "healthy\n" {
		t.Fatalf("health stdout = %q, want %q", out, "healthy\n")
	}
}

// `relay health` fails with "worker not running" when no worker socket answers,
// even with no Redis or Docker involved: worker health requires a running
// worker.
func TestHealthCommandNoWorker(t *testing.T) {
	deps := testDeps(t)
	// No socket server is started at deps.SocketPath: the dial fails.

	_, _, err := runCLIWithDeps(t, deps, "", "health")
	if err == nil {
		t.Fatal("health with no worker must error")
	}
	if !strings.Contains(err.Error(), "worker not running") {
		t.Fatalf("error = %q, want it to name that the worker is not running", err)
	}
	if strings.Contains(err.Error(), "redis") || strings.Contains(err.Error(), "docker") {
		t.Fatalf("error = %q, must not mention direct dependency probes", err)
	}
}

// `relay health` fails with the worker's own reason when the worker is not
// ready: a still-starting worker and a degraded worker are distinguished, and
// the reason is surfaced.
func TestHealthCommandNotReady(t *testing.T) {
	tests := []struct {
		name       string
		checker    worker.ReadinessChecker
		wantSubstr []string
	}{
		{
			name:       "worker starting",
			checker:    fakeReadinessChecker{ready: false, reason: "worker starting"},
			wantSubstr: []string{"health:", "worker starting"},
		},
		{
			name:       "worker shutting down",
			checker:    fakeReadinessChecker{ready: false, reason: "worker shutting down"},
			wantSubstr: []string{"health:", "worker shutting down"},
		},
		{
			name:       "degraded dependency",
			checker:    fakeReadinessChecker{ready: false, reason: "redis consumer unhealthy"},
			wantSubstr: []string{"health:", "redis consumer unhealthy"},
		},
		{
			name:       "docker unavailable",
			checker:    fakeReadinessChecker{ready: false, reason: "docker unavailable"},
			wantSubstr: []string{"health:", "docker unavailable"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			deps := testDeps(t)
			startHealthSocket(t, deps.SocketPath, tc.checker)

			_, _, err := runCLIWithDeps(t, deps, "", "health")
			if err == nil {
				t.Fatal("health with a not-ready worker must error")
			}
			for _, want := range tc.wantSubstr {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error = %q, want it to contain %q", err, want)
				}
			}
		})
	}
}

// A socket with no readiness checker wired answers not-ready (a worker that
// cannot prove readiness is not healthy), and health surfaces the reason.
func TestHealthCommandNoChecker(t *testing.T) {
	deps := testDeps(t)
	startHealthSocket(t, deps.SocketPath, nil)

	_, _, err := runCLIWithDeps(t, deps, "", "health")
	if err == nil {
		t.Fatal("health with an unwired checker must error")
	}
	if !strings.Contains(err.Error(), "readiness unavailable") {
		t.Fatalf("error = %q, want it to surface the worker's not-ready reason", err)
	}
}

// TestHealthCommandUsesSocketOnly pins that `relay health` fails without any
// external configuration or dependency clients when no worker is running: the
// old one-shot Redis/Docker probe (which required REDIS_* config) is gone and
// there is no fallback.
func TestHealthCommandUsesSocketOnly(t *testing.T) {
	// Explicitly scrub REDIS_* so a config-dependent implementation would fail
	// differently (a config error) rather than the expected socket error.
	t.Setenv("REDIS_URI", "")
	t.Setenv("REDIS_STREAM", "")
	t.Setenv("REDIS_GROUP", "")

	deps := testDeps(t)
	// No worker socket at deps.SocketPath.

	_, _, err := runCLIWithDeps(t, deps, "", "health")
	if err == nil {
		t.Fatal("health with no worker must error")
	}
	if strings.Contains(err.Error(), "config") || strings.Contains(err.Error(), "REDIS") {
		t.Fatalf("health must not load external config: %q", err)
	}
	if !strings.Contains(err.Error(), "worker not running") {
		t.Fatalf("error = %q, want the socket-based worker not running error", err)
	}
}

// The health command exits 2 on extra args.
func TestHealthArgError(t *testing.T) {
	_, _, err := runCLI(t, "", "health", "extra")
	if err == nil || !strings.Contains(err.Error(), "health: too many arguments") {
		t.Fatalf("returned error missing usage error: %v", err)
	}
}

// `relay ready` is no longer a command: it is an unknown command.
func TestReadyCommandRemoved(t *testing.T) {
	_, _, err := runCLI(t, "", "ready")
	if err == nil {
		t.Fatal("removed ready command must not be accepted")
	}
	if !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("error = %q, want an unknown command error", err)
	}
}

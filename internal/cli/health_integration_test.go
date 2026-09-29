//go:build integration

// This file exercises the `relay health` command's real path — the CLI dialing
// the worker's control socket — end to end.
//
// It is excluded from the default suite by the integration build tag. Unlike
// the other integration tests it REQUIRES no external dependency: no Redis, no
// Docker, and no live deployment. `relay health` owns no dependency clients in
// the CLI process any more, so the worker socket is the only seam and a real
// SocketServer with a deterministic readiness checker is enough to drive it.
package cli

import (
	"io"
	"log/slog"
	"strings"
	"testing"

	"relay/internal/worker"
)

// TestIntegrationHealthSocketBacked pins the real `relay health` path over a
// real worker socket: it is healthy while the worker answers ready, and fails
// with "worker not running" once the socket is gone — with no Redis or Docker
// involved at any point, proving the CLI no longer probes dependencies itself.
func TestIntegrationHealthSocketBacked(t *testing.T) {
	deps := testDeps(t)

	s, err := worker.NewSocketServer(
		deps.SocketPath,
		fakePoolSnapshotter{pools: nil},
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("NewSocketServer: %v", err)
	}
	s.SetReadiness(fakeReadinessChecker{ready: true})

	out, _, err := runCLIWithDeps(t, deps, "", "health")
	if err != nil {
		t.Fatalf("health with a ready worker failed: %v", err)
	}
	if out != "healthy\n" {
		t.Fatalf("health stdout = %q, want %q", out, "healthy\n")
	}

	// Stop the worker: health must now fail with the socket-based reason and
	// must not fall back to any direct dependency probe.
	if err := s.Close(); err != nil {
		t.Fatalf("close socket: %v", err)
	}
	_, _, err = runCLIWithDeps(t, deps, "", "health")
	if err == nil {
		t.Fatal("health with no worker must fail")
	}
	if !strings.Contains(err.Error(), "worker not running") {
		t.Fatalf("error = %q, want the socket-based worker not running error", err)
	}
	if strings.Contains(err.Error(), "redis") || strings.Contains(err.Error(), "docker") {
		t.Fatalf("error = %q must not mention direct dependency probes", err)
	}
}

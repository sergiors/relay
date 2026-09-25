package runtime

import (
	"context"
	"net/http"
	"testing"

	"relay/internal/testutil"
)

// TestNetworkExistsNotFoundSemantics pins that a 404 from the daemon is the
// only "absent" signal: NetworkExists reports (false, nil) for it, and any
// other inspect failure is a genuine error — never mistaken for a missing
// network. The not-found matching goes through cerrdefs.IsNotFound, which
// recognizes both the errdefs sentinel and any NotFound()-implementing type.
func TestNetworkExistsNotFoundSemantics(t *testing.T) {
	t.Run("404 is absent, not an error", func(t *testing.T) {
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/networks/gone", status: http.StatusNotFound, body: `{"message":"network gone not found"}`},
		)
		m := &Manager{cli: cli, log: testutil.DiscardLogger()}
		ok, err := m.NetworkExists(context.Background(), "gone")
		if err != nil {
			t.Fatalf("a missing network must not be an error: %v", err)
		}
		if ok {
			t.Fatal("NetworkExists = true for a 404, want false")
		}
	})

	t.Run("non-404 failure is a genuine error", func(t *testing.T) {
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/networks/boom", status: http.StatusInternalServerError, body: `{"message":"daemon exploded"}`},
		)
		m := &Manager{cli: cli, log: testutil.DiscardLogger()}
		ok, err := m.NetworkExists(context.Background(), "boom")
		if err == nil {
			t.Fatal("a non-not-found failure must surface as an error")
		}
		if ok {
			t.Fatal("NetworkExists = true on an error, want false")
		}
	})

	t.Run("present network is true", func(t *testing.T) {
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/networks/here", body: `{"Name":"here"}`},
		)
		m := &Manager{cli: cli, log: testutil.DiscardLogger()}
		ok, err := m.NetworkExists(context.Background(), "here")
		if err != nil || !ok {
			t.Fatalf("NetworkExists = (%v, %v), want (true, nil)", ok, err)
		}
	})
}

// TestVerifyNetworks pins the startup pre-flight the worker performs for the
// worker-global NETWORKS set: every network must exist; the first missing one is
// reported (ok=false) and Relay NEVER creates a network. A non-not-found inspect
// error is surfaced as a genuine error.
func TestVerifyNetworks(t *testing.T) {
	t.Run("all present", func(t *testing.T) {
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/networks/backend", body: `{"Name":"backend"}`},
			dockerRoute{method: http.MethodGet, path: "/networks/frontend", body: `{"Name":"frontend"}`},
		)
		m := &Manager{cli: cli, log: testutil.DiscardLogger()}
		missing, ok, err := m.VerifyNetworks(context.Background(), []string{"backend", "frontend"})
		if err != nil || !ok || missing != "" {
			t.Fatalf("VerifyNetworks = (%q, %v, %v), want (\"\", true, nil)", missing, ok, err)
		}
	})

	t.Run("missing network reports the first missing name", func(t *testing.T) {
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/networks/backend", status: http.StatusNotFound, body: `{"message":"network backend not found"}`},
		)
		m := &Manager{cli: cli, log: testutil.DiscardLogger()}
		missing, ok, err := m.VerifyNetworks(context.Background(), []string{"backend"})
		if err != nil {
			t.Fatalf("missing network must not be an error: %v", err)
		}
		if ok || missing != "backend" {
			t.Fatalf("VerifyNetworks = (%q, %v, %v), want (\"backend\", false, nil)", missing, ok, err)
		}
	})

	t.Run("empty list is vacuously ok", func(t *testing.T) {
		cli := newScriptedDockerClient(t)
		m := &Manager{cli: cli, log: testutil.DiscardLogger()}
		missing, ok, err := m.VerifyNetworks(context.Background(), nil)
		if err != nil || !ok || missing != "" {
			t.Fatalf("VerifyNetworks(nil) = (%q, %v, %v), want (\"\", true, nil)", missing, ok, err)
		}
	})

	t.Run("empty names are skipped", func(t *testing.T) {
		cli := newScriptedDockerClient(t)
		m := &Manager{cli: cli, log: testutil.DiscardLogger()}
		missing, ok, err := m.VerifyNetworks(context.Background(), []string{"", ""})
		if err != nil || !ok || missing != "" {
			t.Fatalf("VerifyNetworks(empties) = (%q, %v, %v), want (\"\", true, nil)", missing, ok, err)
		}
	})
}

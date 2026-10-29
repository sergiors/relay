package runtime

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"relay/internal/testutil"
)

// TestRestartServiceContainerConfirmsRunning pins the HOT_RELOAD in-place
// restart primitive: it POSTs the daemon's restart endpoint for the SAME
// container id and returns only once a Docker inspect confirms the container is
// running again (no polling/sleep).
func TestRestartServiceContainerConfirmsRunning(t *testing.T) {
	restarted := 0
	inspected := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodPost, path: "/containers/cid-1/restart", body: `{}`,
			onMatch: func() { restarted++ }},
		dockerRoute{method: http.MethodGet, path: "/containers/cid-1/json",
			body:    `{"Id":"cid-1","State":{"Running":true}}`,
			onMatch: func() { inspected++ }},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: "h"}
	if err := m.RestartServiceContainer(context.Background(), "cid-1"); err != nil {
		t.Fatalf("RestartServiceContainer: %v", err)
	}
	if restarted != 1 {
		t.Fatalf("restart calls = %d, want exactly 1", restarted)
	}
	if inspected != 1 {
		t.Fatalf("inspect calls = %d, want exactly 1 (no polling/sleep)", inspected)
	}
}

// TestRestartServiceContainerNonRunningFails pins the running gate: a container
// that restarts but inspects as not running (e.g. the edited source crashes on
// boot) is reported as an error rather than treated as reloaded.
func TestRestartServiceContainerNonRunningFails(t *testing.T) {
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodPost, path: "/containers/cid-1/restart", body: `{}`},
		dockerRoute{method: http.MethodGet, path: "/containers/cid-1/json",
			body: `{"Id":"cid-1","State":{"Running":false,"Status":"exited"}}`},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: "h"}
	err := m.RestartServiceContainer(context.Background(), "cid-1")
	if err == nil {
		t.Fatal("a non-running restarted container must fail RestartServiceContainer")
	}
	if !strings.Contains(err.Error(), "not running") {
		t.Fatalf("error = %v, want it to mention the running gate", err)
	}
}

// TestRestartServiceContainerDaemonErrorSurfaces pins that a daemon restart
// failure is surfaced so the reconciler can preserve the running generation and
// retry rather than recording a phantom reload.
func TestRestartServiceContainerDaemonErrorSurfaces(t *testing.T) {
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodPost, path: "/containers/cid-1/restart",
			status: http.StatusInternalServerError, body: `{"message":"daemon down"}`},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger(), nodeID: "h"}
	if err := m.RestartServiceContainer(context.Background(), "cid-1"); err == nil {
		t.Fatal("a daemon restart failure must surface as an error")
	}
}

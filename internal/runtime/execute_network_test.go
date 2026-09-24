package runtime

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"relay/internal/testutil"
)

// TestExecuteMissingNetworkFailsWithoutCreate proves the runtime verifies a
// Prepared's networks BEFORE creating an execution container: a missing network
// makes the invocation fail with a clear error and NO container create is
// attempted (the scripted daemon has no /containers/create route, so a create
// would fail the test loudly).
func TestExecuteMissingNetworkFailsWithoutCreate(t *testing.T) {
	cli := newScriptedDockerClient(t,
		dockerRoute{
			method: http.MethodGet, path: "/networks/missing-net",
			status: http.StatusNotFound, body: `{"message":"network missing-net not found"}`,
		},
	)
	m := &Manager{
		cli:        cli,
		log:        testutil.DiscardLogger(),
		hostname:   "test-host",
		containers: newContainerCache(),
	}
	prepared := &Prepared{
		Name:     "fn",
		Image:    "img",
		Networks: []string{"missing-net"},
	}
	err := m.Execute(context.Background(), prepared, "index.main", []byte(`{}`), nil)
	if err == nil {
		t.Fatal("expected Execute to fail for a missing network")
	}
	if !strings.Contains(err.Error(), `required Docker network "missing-net" does not exist`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestExecuteNoNetworksSkipsVerification proves a Prepared with no networks
// performs no network Docker round trip before create: with no /networks route
// scripted, a stray inspect would fail the test loudly. The create is scripted
// to fail, so the invocation fails at create — proving the verify step was
// skipped.
func TestExecuteNoNetworksSkipsVerification(t *testing.T) {
	cli := newScriptedDockerClient(t,
		dockerRoute{
			method: http.MethodPost, path: "/containers/create",
			status: http.StatusInternalServerError, body: `{"message":"create boom"}`,
		},
	)
	m := &Manager{
		cli:        cli,
		log:        testutil.DiscardLogger(),
		hostname:   "test-host",
		containers: newContainerCache(),
	}
	prepared := &Prepared{Name: "fn", Image: "img"}
	err := m.Execute(context.Background(), prepared, "index.main", []byte(`{}`), nil)
	if err == nil {
		t.Fatal("expected the failing create to fail the invocation")
	}
	// The failure is at container create, never a network inspect.
	if strings.Contains(err.Error(), "verify networks") || strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("no-networks invocation must not verify networks: %v", err)
	}
	if !strings.Contains(err.Error(), "create container") {
		t.Fatalf("expected a create failure, got: %v", err)
	}
}

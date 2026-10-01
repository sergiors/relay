package runtime

import (
	"context"
	"net/http"
	"testing"

	"relay/internal/app"
	"relay/internal/testutil"
)

// TestStartExecutionContainerNetworkingConfig drives the real
// startExecutionContainer client path against the scripted daemon and inspects
// the ACTUAL serialized /containers/create request (not the pure helper), so
// the worker-global NETWORKS set is proven to reach Docker's
// NetworkingConfig.EndpointsConfig on the wire.
//
// The create route is scripted to FAIL, which makes startExecutionContainer
// return before attach/start; the request body is still captured, and no real
// daemon (or attach hijack) is ever involved. Because the scripted daemon fails
// the test on an unhandled request, any unexpected call (in particular a
// network create) surfaces as a failure.
func TestStartExecutionContainerNetworkingConfig(t *testing.T) {
	t.Run("global networks serialized exactly once", func(t *testing.T) {
		var createBody []byte
		cli := newScriptedDockerClient(t, dockerRoute{
			method: http.MethodPost, path: "/containers/create",
			status: http.StatusInternalServerError,
			body:   `{"message":"scripted create failure"}`,
			onBody: func(b []byte) { createBody = append([]byte(nil), b...) },
		})
		m := &Manager{cli: cli, log: testutil.DiscardLogger()}

		_, err := startExecutionContainer(context.Background(), m.cli, m.log,
			"fn", "img", nil, []string{"backend", "frontend", "backend"}, app.ResourceLimits{}, RunMeta{})
		if err == nil {
			t.Fatal("expected the scripted create failure")
		}
		if len(createBody) == 0 {
			t.Fatal("no /containers/create body captured")
		}
		req := decodeCreateRequest(t, createBody)
		if req.NetworkingConfig == nil {
			t.Fatal("execution container create request has no NetworkingConfig")
		}
		got := req.NetworkingConfig.EndpointsConfig
		if len(got) != 2 {
			t.Fatalf("endpoints = %v, want backend+frontend exactly once", got)
		}
		for _, want := range []string{"backend", "frontend"} {
			if _, ok := got[want]; !ok {
				t.Fatalf("missing %q endpoint: %v", want, got)
			}
		}
	})

	t.Run("no networks sends no NetworkingConfig", func(t *testing.T) {
		var createBody []byte
		cli := newScriptedDockerClient(t, dockerRoute{
			method: http.MethodPost, path: "/containers/create",
			status: http.StatusInternalServerError,
			body:   `{"message":"scripted create failure"}`,
			onBody: func(b []byte) { createBody = append([]byte(nil), b...) },
		})
		m := &Manager{cli: cli, log: testutil.DiscardLogger()}

		_, err := startExecutionContainer(context.Background(), m.cli, m.log,
			"fn", "img", nil, nil, app.ResourceLimits{}, RunMeta{})
		if err == nil {
			t.Fatal("expected the scripted create failure")
		}
		if len(createBody) == 0 {
			t.Fatal("no /containers/create body captured")
		}
		req := decodeCreateRequest(t, createBody)
		if req.NetworkingConfig != nil {
			t.Fatalf("expected no NetworkingConfig, got %v", req.NetworkingConfig)
		}
	})

	// A network that disappeared after startup verification surfaces as a
	// create failure and Relay NEVER issues a network create to fix it up. The
	// scripted create answers the daemon's network-not-found error; a
	// /networks/create route would be counted, so the assertion is explicit as
	// well as structural (an unhandled route fails the test).
	t.Run("deleted-after-start create failure surfaces with no network create", func(t *testing.T) {
		networkCreates := 0
		cli := newScriptedDockerClient(t,
			dockerRoute{
				method: http.MethodPost, path: "/containers/create",
				status: http.StatusNotFound,
				body:   `{"message":"network backend not found"}`,
			},
			dockerRoute{
				method: http.MethodPost, path: "/networks/create", body: `{"Id":"nid"}`,
				onMatch: func() { networkCreates++ },
			},
		)
		m := &Manager{cli: cli, log: testutil.DiscardLogger()}

		_, err := startExecutionContainer(context.Background(), m.cli, m.log,
			"fn", "img", nil, []string{"backend"}, app.ResourceLimits{}, RunMeta{})
		if err == nil {
			t.Fatal("a missing network must surface as a container create error")
		}
		if networkCreates != 0 {
			t.Fatalf("Relay issued %d network create(s); it must never create networks", networkCreates)
		}
	})
}

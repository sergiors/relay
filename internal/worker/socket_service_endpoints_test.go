package worker

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"

	"relay/internal/runtime"
)

type serviceEndpointTestManager struct {
	endpoints []runtime.ServiceEndpoint
	err       error
	gotApp    string
	gotBind   string
}

func (f *serviceEndpointTestManager) PoolSnapshot(string) (runtime.PoolSnapshot, bool) {
	return runtime.PoolSnapshot{}, false
}

func (f *serviceEndpointTestManager) ServiceEndpoints(
	_ context.Context, appName, bindAddr string,
) ([]runtime.ServiceEndpoint, error) {
	f.gotApp = appName
	f.gotBind = bindAddr
	return f.endpoints, f.err
}

func TestQueryServiceEndpointsReturnsLiveDockerFacts(t *testing.T) {
	manager := &serviceEndpointTestManager{
		endpoints: []runtime.ServiceEndpoint{{
			Service: "web", Replica: 0, ContainerID: "c1",
			State: "running", Health: "healthy", Port: 8080,
			HostIP: "127.0.0.1", HostPort: 49152,
		}},
	}
	path := testSocketPath(t)
	server, err := NewSocketServer(
		path, manager, nil, slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("NewSocketServer: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })
	server.SetServicePublication("127.0.0.1", "10.0.0.5")

	result, err := QueryServiceEndpoints(path, "fn")
	if err != nil {
		t.Fatalf("QueryServiceEndpoints: %v", err)
	}
	if manager.gotApp != "fn" || manager.gotBind != "127.0.0.1" {
		t.Fatalf("provider got app=%q bind=%q, want fn/127.0.0.1",
			manager.gotApp, manager.gotBind)
	}
	if result.BindAddr != "127.0.0.1" || result.AdvertiseAddr != "10.0.0.5" {
		t.Fatalf("addresses = %q/%q, want the wired values",
			result.BindAddr, result.AdvertiseAddr)
	}
	if len(result.Endpoints) != 1 {
		t.Fatalf("endpoints = %+v, want one", result.Endpoints)
	}
	got := result.Endpoints[0]
	if got.Service != "web" || got.Replica != 0 || got.State != "running" ||
		got.Health != "healthy" || got.HostIP != "127.0.0.1" || got.HostPort != 49152 {
		t.Fatalf("endpoint = %+v, want the live Docker facts", got)
	}
}

func TestQueryServiceEndpointsUnavailableWithoutProvider(t *testing.T) {
	path := testSocketPath(t)
	server, err := NewSocketServer(
		path, &fakeSnapshotter{}, nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("NewSocketServer: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })

	_, err = QueryServiceEndpoints(path, "fn")
	if !errors.Is(err, ErrServiceEndpointsUnavailable) {
		t.Fatalf("error = %v, want ErrServiceEndpointsUnavailable", err)
	}
}

func TestQueryServiceEndpointsUnavailableOnDockerError(t *testing.T) {
	manager := &serviceEndpointTestManager{err: errors.New("docker down")}
	path := testSocketPath(t)
	server, err := NewSocketServer(
		path, manager, nil, slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("NewSocketServer: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })

	_, err = QueryServiceEndpoints(path, "fn")
	if !errors.Is(err, ErrServiceEndpointsUnavailable) {
		t.Fatalf("error = %v, want ErrServiceEndpointsUnavailable", err)
	}
}

// A malformed query (no app name) is answered as unavailable and never reaches
// the Docker endpoint provider.
func TestQueryServiceEndpointsRejectsEmptyAppName(t *testing.T) {
	manager := &serviceEndpointTestManager{
		endpoints: []runtime.ServiceEndpoint{{Service: "web"}},
	}
	path := testSocketPath(t)
	server, err := NewSocketServer(
		path, manager, nil, slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("NewSocketServer: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })

	_, err = QueryServiceEndpoints(path, "")
	if !errors.Is(err, ErrServiceEndpointsUnavailable) {
		t.Fatalf("error = %v, want ErrServiceEndpointsUnavailable", err)
	}
	if manager.gotApp != "" {
		t.Fatalf("provider invoked with app %q for an empty-name query", manager.gotApp)
	}
}

// A worker that acknowledges the command but returns no endpoint payload (for
// example a version without the capability) must surface as unavailable, never
// as an empty live view the CLI would render as "none".
func TestQueryServiceEndpointsRejectsResponseWithoutPayload(t *testing.T) {
	path := testSocketPath(t)
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		_, _ = bufio.NewReader(conn).ReadBytes('\n')
		_, _ = io.WriteString(conn, "{}\n")
	}()

	result, err := QueryServiceEndpoints(path, "fn")
	if !errors.Is(err, ErrServiceEndpointsUnavailable) {
		t.Fatalf("error = %v, want ErrServiceEndpointsUnavailable", err)
	}
	if !strings.Contains(err.Error(), "empty response") {
		t.Fatalf("error = %v, want it to name the missing payload", err)
	}
	if len(result.Endpoints) != 0 || result.BindAddr != "" {
		t.Fatalf("result = %+v, want the empty zero value", result)
	}
}

//go:build integration

// This file verifies SERVICE_BIND_ADDR host publication against a real Docker
// daemon, covering what unit scripting cannot: Docker itself allocates the
// dynamic host port on exactly the configured bind address, replicas coexist
// with distinct allocated ports, an unpublished service publishes nothing, and
// the production ServiceEndpoints read path reports precisely what
// ContainerInspect proves — including after a stop, when the daemon clears the
// live mapping.
package runtime

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"

	"relay/internal/testutil"
)

const (
	bindPubApp   = "svc-bind-pub"
	bindPubImage = "relay-test-svc-bind:1"
)

func TestIntegrationServiceBindPublication(t *testing.T) {
	cli := testutil.RequireDocker(t)
	// Construct the manager directly: service containers never use the
	// execution control network, so publication needs only the daemon, and the
	// test stays runnable where a host process cannot prove a
	// control-network route (e.g. Docker Desktop).
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	m, err := NewManager(logger, nil, "test-host")
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	isolateAppServiceContainers(t, ctx, cli, bindPubApp)
	t.Cleanup(func() {
		cc, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		// Scoped to THIS test's app and image only; removal failures are
		// logged so cleanup never fails the test.
		if _, rmErr := removeAppServiceContainers(cc, cli, bindPubApp); rmErr != nil {
			t.Logf("cleanup: remove service containers: %v", rmErr)
		}
		cleanupImagePrefixes(cli, bindPubImage)()
	})

	// A minimal long-lived fixture image: the service starts it WITHOUT an
	// entry override (the external-image shape), so the container runs the
	// image's own CMD. The shell installs a SIGTERM handler so this test's
	// stops are prompt instead of paying Docker's 10s SIGKILL grace.
	buildTestImage(ctx, t, bindPubImage, `
FROM alpine:3
CMD ["sh", "-c", "trap 'exit 0' TERM; while :; do sleep 60; done"]
`)

	specFor := func(name string, port int, bind string) ServiceSpec {
		return ServiceSpec{
			App: bindPubApp, Name: name, SourceRef: "fixture.js",
			Port: port, Image: bindPubImage,
			Env:      []string{"PORT=" + strconv.Itoa(port)},
			BindAddr: bind,
		}
	}

	// Two published replicas must coexist with DISTINCT Docker-allocated host
	// ports on the configured bind address.
	ids := map[int]string{}
	for replica := 0; replica < 2; replica++ {
		id, startErr := m.StartService(ctx, specFor("web", 8080, "127.0.0.1"), replica)
		if startErr != nil {
			t.Fatalf("start published replica %d: %v", replica, startErr)
		}
		ids[replica] = id
	}
	// An unpublished service of the same app coexists without any host binding.
	internalID, startErr := m.StartService(ctx, specFor("worker", 9000, ""), 0)
	if startErr != nil {
		t.Fatalf("start unpublished service: %v", startErr)
	}

	port0 := waitForPublishedBinding(t, ctx, cli, ids[0], "127.0.0.1", "8080")
	port1 := waitForPublishedBinding(t, ctx, cli, ids[1], "127.0.0.1", "8080")
	if port0 == port1 {
		t.Fatalf("both replicas published on host port %d, want distinct Docker-allocated ports", port0)
	}
	assertNoHostBinding(t, ctx, cli, internalID, "9000")
	assertPublishedConfig(t, ctx, cli, ids[0], "8080", "127.0.0.1")

	// The production read path reports exactly the inspected bindings and
	// omits the unpublished replica.
	endpoints, epErr := m.ServiceEndpoints(ctx, bindPubApp, "127.0.0.1")
	if epErr != nil {
		t.Fatalf("ServiceEndpoints: %v", epErr)
	}
	if len(endpoints) != 2 {
		t.Fatalf("endpoints = %+v, want exactly the two published replicas", endpoints)
	}
	wantPorts := map[int]int{0: port0, 1: port1}
	for _, ep := range endpoints {
		if ep.Service != "web" {
			t.Fatalf("endpoint = %+v, want only the published service reported", ep)
		}
		if ep.State != container.StateRunning {
			t.Fatalf("endpoint = %+v, want the running state", ep)
		}
		if ep.HostIP != "127.0.0.1" || ep.HostPort != wantPorts[ep.Replica] {
			t.Fatalf("endpoint = %+v, want 127.0.0.1:%d from ContainerInspect",
				ep, wantPorts[ep.Replica])
		}
	}

	// A different expected address never matches a live binding: the replicas
	// stay visible with their state but are never reported as published.
	mismatched, mmErr := m.ServiceEndpoints(ctx, bindPubApp, "10.0.0.9")
	if mmErr != nil {
		t.Fatalf("ServiceEndpoints (mismatched bind): %v", mmErr)
	}
	if len(mismatched) != 2 {
		t.Fatalf("endpoints = %+v, want both replicas still reported", mismatched)
	}
	for _, ep := range mismatched {
		if ep.HostPort != 0 || ep.HostIP != "" {
			t.Fatalf("endpoint = %+v, want unpublished against a non-matching address", ep)
		}
	}

	// After a stop the daemon clears the live mapping: the replica stays
	// visible with its actual state but never a stale host port.
	stopTimeout := 5
	if _, stopErr := cli.ContainerStop(
		ctx, ids[0], client.ContainerStopOptions{Timeout: &stopTimeout},
	); stopErr != nil {
		t.Fatalf("stop replica 0: %v", stopErr)
	}
	if !pollUntil(ctx, 15*time.Second, func() bool {
		eps, err := m.ServiceEndpoints(ctx, bindPubApp, "127.0.0.1")
		if err != nil {
			return false
		}
		for _, ep := range eps {
			if ep.Service == "web" && ep.Replica == 0 &&
				ep.State == container.StateExited &&
				ep.HostPort == 0 && ep.HostIP == "" {
				return true
			}
		}
		return false
	}) {
		t.Fatalf("stopped replica never reported as exited and unpublished: %v",
			endpointsSnapshot(t, ctx, m))
	}
}

// waitForPublishedBinding polls ContainerInspect until the container reports
// exactly one live host binding for containerPort targeting wantIP, and returns
// the Docker-allocated host port.
func waitForPublishedBinding(
	t *testing.T, ctx context.Context,
	cli *client.Client, id, wantIP, containerPort string,
) int {
	t.Helper()
	portKey := network.MustParsePort(containerPort)
	var allocated string
	if !pollUntil(ctx, 30*time.Second, func() bool {
		insp, err := cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
		if err != nil || insp.Container.NetworkSettings == nil {
			return false
		}
		bindings := insp.Container.NetworkSettings.Ports[portKey]
		if len(bindings) != 1 {
			return false
		}
		if bindings[0].HostIP.Unmap().String() != wantIP {
			return false
		}
		allocated = bindings[0].HostPort
		return true
	}) {
		t.Fatalf("container %s never published %s on %s within 30s", id, containerPort, wantIP)
	}
	port, err := strconv.Atoi(allocated)
	if err != nil || port < 1 || port > 65535 {
		t.Fatalf("container %s allocated host port %q, want a valid dynamic port", id, allocated)
	}
	return port
}

// assertNoHostBinding proves an unpublished service carries the port as
// container metadata only: no requested HostConfig binding and no live host
// binding for it.
func assertNoHostBinding(
	t *testing.T, ctx context.Context, cli *client.Client, id, containerPort string,
) {
	t.Helper()
	insp, err := cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatalf("inspect %s: %v", id, err)
	}
	if hc := insp.Container.HostConfig; hc == nil || len(hc.PortBindings) != 0 {
		t.Fatalf("HostConfig.PortBindings = %v, want none for an unpublished service",
			insp.Container.HostConfig.PortBindings)
	}
	if insp.Container.NetworkSettings == nil {
		t.Fatal("inspect: nil NetworkSettings")
	}
	portKey := network.MustParsePort(containerPort)
	if len(insp.Container.NetworkSettings.Ports[portKey]) != 0 {
		t.Fatalf("live bindings for %s = %v, want none",
			containerPort, insp.Container.NetworkSettings.Ports[portKey])
	}
	if _, exposed := insp.Container.Config.ExposedPorts[portKey]; !exposed {
		t.Fatalf("ExposedPorts = %v, want %s carried as metadata",
			insp.Container.Config.ExposedPorts, containerPort)
	}
}

// assertPublishedConfig proves the REQUESTED configuration Docker stored: one
// binding entry with an empty (daemon-allocated) host port, and the
// relay.bind_addr identity label matching the configured address.
func assertPublishedConfig(
	t *testing.T, ctx context.Context, cli *client.Client, id, containerPort, wantBind string,
) {
	t.Helper()
	insp, err := cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatalf("inspect %s: %v", id, err)
	}
	hc := insp.Container.HostConfig
	if hc == nil {
		t.Fatal("inspect: nil HostConfig")
	}
	portKey := network.MustParsePort(containerPort)
	bindings := hc.PortBindings[portKey]
	if len(bindings) != 1 || bindings[0].HostPort != "" {
		t.Fatalf("requested PortBindings = %v, want one entry with a Docker-allocated (empty) host port",
			bindings)
	}
	if got := insp.Container.Config.Labels[labelBindAddr]; got != wantBind {
		t.Fatalf("label %s = %q, want %q", labelBindAddr, got, wantBind)
	}
}

// endpointsSnapshot renders the current endpoint view for a failure message.
func endpointsSnapshot(
	t *testing.T, ctx context.Context, m *Manager,
) []ServiceEndpoint {
	t.Helper()
	eps, err := m.ServiceEndpoints(ctx, bindPubApp, "127.0.0.1")
	if err != nil {
		return nil
	}
	return eps
}

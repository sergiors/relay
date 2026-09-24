//go:build integration

// This file exercises the top-level `networks` feature against a real Docker
// daemon: execution containers join the template networks at create time, the
// networks are verified before create, and a missing network fails the
// invocation without creating a container. It complements the service-network
// integration test in services_integration_test.go.
package runtime

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/client"

	"relay/internal/function"
	"relay/internal/testutil"
)

// newManagerForNetworks builds a Manager with an explicit warm idle timeout and
// the given hostname, so tests can create unique-named networks and containers.
func newManagerForNetworks(t *testing.T) *Manager {
	t.Helper()
	m, err := NewManager(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
		"test-host",
		WithWarmContainerIdleTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// TestIntegrationExecutionContainerJoinsTemplateNetworks pins that an execution
// container is created attached to every network the function's template
// declares, that the relay.networks label matches the canonical set, and that a
// missing network fails the invocation without creating a container.
func TestIntegrationExecutionContainerJoinsTemplateNetworks(t *testing.T) {
	cli := testutil.RequireDocker(t)
	m := newManagerForNetworks(t)
	out := newFunctionOutputSink(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Two dedicated networks, created by the TEST (the infra owner). Relay never
	// creates networks.
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	netA := "relay-test-net-a-" + suffix
	netB := "relay-test-net-b-" + suffix
	for _, n := range []string{netA, netB} {
		if _, err := cli.NetworkCreate(ctx, n, client.NetworkCreateOptions{Driver: "bridge"}); err != nil {
			t.Fatalf("create network %s: %v", n, err)
		}
	}
	t.Cleanup(func() {
		cc, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		for _, n := range []string{netA, netB} {
			_, _ = cli.NetworkRemove(cc, n, client.NetworkRemoveOptions{})
		}
	})

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
networks:
  - `+netA+`
  - `+netB+`
events:
  - handler: index.run
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", `
export async function run(event) {
  console.log("networks-ok");
}
`)
	fn := function.Function{Name: "networks-e2e", Dir: dir, Template: &function.Template{
		Runtime:  "node24",
		Networks: []string{netA, netB},
	}}
	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	execCtx := context.WithValue(context.Background(), runMetaKey{},
		RunMeta{Type: ContainerTypeEvent, Function: "networks-e2e", Handler: "index.run", Hostname: "test-host", Image: prepared.Image})
	if err := m.Execute(execCtx, prepared, "index.run", []byte(`{"event_name":"INSERT"}`), nil); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !strings.Contains(out.String(), "networks-ok") {
		t.Fatalf("handler output missing: %s", out.String())
	}

	// The warm container is idle now; inspect it. It must carry both networks
	// and the canonical relay.networks label.
	id := findContainerByLabel(ctx, m.cli, labelFunction, "networks-e2e")
	if id == "" {
		t.Fatal("no execution container found")
	}
	insp, err := cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if insp.Container.NetworkSettings == nil {
		t.Fatal("inspect: nil NetworkSettings")
	}
	for _, n := range []string{netA, netB} {
		if _, ok := insp.Container.NetworkSettings.Networks[n]; !ok {
			t.Errorf("container not attached to network %q; networks = %v", n, insp.Container.NetworkSettings.Networks)
		}
	}
	// Exactly the two template networks are joined.
	if len(insp.Container.NetworkSettings.Networks) != 2 {
		t.Errorf("container networks = %v, want exactly %s and %s",
			insp.Container.NetworkSettings.Networks, netA, netB)
	}
}

// TestIntegrationExecutionMissingNetworkNoContainer pins that a template
// network that does not exist fails the invocation BEFORE any container is
// created: Relay never creates networks, and a missing one is scoped to the
// function.
func TestIntegrationExecutionMissingNetworkNoContainer(t *testing.T) {
	testutil.RequireDocker(t)
	m := newManagerForNetworks(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	missing := "relay-test-missing-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	writeFile(t, dir, "template.yaml", `
runtime: node24
networks:
  - `+missing+`
events:
  - handler: index.run
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", `export async function run(event) {}`)
	fn := function.Function{Name: "missing-net-e2e", Dir: dir, Template: &function.Template{
		Runtime:  "node24",
		Networks: []string{missing},
	}}
	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	execCtx := context.WithValue(context.Background(), runMetaKey{},
		RunMeta{Type: ContainerTypeEvent, Function: "missing-net-e2e", Handler: "index.run", Hostname: "test-host", Image: prepared.Image})
	err = m.Execute(execCtx, prepared, "index.run", []byte(`{"event_name":"INSERT"}`), nil)
	if err == nil {
		t.Fatal("expected Execute to fail for a missing network")
	}
	if !strings.Contains(err.Error(), missing) {
		t.Fatalf("error %v must name the missing network %q", err, missing)
	}

	// No container was created for the function.
	if id := findContainerByLabel(ctx, m.cli, labelFunction, "missing-net-e2e"); id != "" {
		t.Fatalf("a container was created despite the missing network: %s", id)
	}
}

// TestIntegrationNetworksOnlyEditReusesImage proves against a real daemon that a
// networks-only template edit does NOT rebuild the image: Prepare returns the
// same image reference across the edit, while the function's CONTENT fingerprint
// changes (so the reconciler still reacts). The runtime generation changes, so
// warm containers would be replaced without a rebuild.
func TestIntegrationNetworksOnlyEditReusesImage(t *testing.T) {
	testutil.RequireDocker(t)
	m := newManagerForNetworks(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	writeFile(t, dir, "index.js", "export function run(e){}\n")
	writeTemplate := func(networks string) {
		writeFile(t, dir, "template.yaml", `runtime: node24
networks:
`+networks+`events:
  - handler: index.run
    pattern:
      event_name: [INSERT]
`)
	}
	writeTemplate("  - alpha\n")

	contentBefore, err := function.Fingerprint(dir)
	if err != nil {
		t.Fatalf("content fingerprint: %v", err)
	}
	fn1, err := function.LoadSingle(dir, "networks-edit-e2e")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	p1, err := m.Prepare(ctx, fn1)
	if err != nil {
		t.Fatalf("prepare 1: %v", err)
	}
	gen1 := p1.RuntimeGeneration

	// Edit ONLY the networks list.
	writeTemplate("  - alpha\n  - beta\n")
	contentAfter, err := function.Fingerprint(dir)
	if err != nil {
		t.Fatalf("content fingerprint after: %v", err)
	}
	if contentAfter == contentBefore {
		t.Fatal("a networks edit must change the content fingerprint (reconciler must react)")
	}
	fn2, err := function.LoadSingle(dir, "networks-edit-e2e")
	if err != nil {
		t.Fatalf("load after: %v", err)
	}
	p2, err := m.Prepare(ctx, fn2)
	if err != nil {
		t.Fatalf("prepare 2: %v", err)
	}
	if p2.Image != p1.Image {
		t.Fatalf("a networks-only edit rebuilt the image: %s -> %s", p1.Image, p2.Image)
	}
	if p2.RuntimeGeneration == gen1 {
		t.Fatal("a changed network set must change the runtime generation")
	}
}

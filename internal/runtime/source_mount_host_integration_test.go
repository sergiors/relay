//go:build integration

package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"

	"relay/internal/testutil"
)

// TestIntegrationSourceMountDaemonPathMapsRelayContainerMount proves the
// containerized-Relay resolver against a REAL Docker daemon. It creates a
// temporary container that mirrors the bundled Compose shape — a host directory
// bind-mounted at /apps — and points the Manager's hostname at that container's
// short ID (exactly what os.Hostname() returns inside the Relay container), so
// the self-inspect path is exercised end to end: /apps/<app> maps to the
// inspected mount's Source plus the relative path, while an uncovered path keeps
// the historical app dir. The container is never started, so the test has no
// runtime side effects beyond create/inspect/remove.
func TestIntegrationSourceMountDaemonPathMapsRelayContainerMount(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// A base image is required to create the probe container.
	pull, err := cli.ImagePull(ctx, "alpine:3", client.ImagePullOptions{})
	if err != nil {
		t.Fatalf("pull alpine:3: %v", err)
	}
	if err := pull.Wait(ctx); err != nil {
		t.Fatalf("wait for alpine:3 pull: %v", err)
	}

	hostRoot := t.TempDir()
	if err := os.MkdirAll(filepath.Join(hostRoot, "myapp"), 0o755); err != nil {
		t.Fatalf("mkdir app dir: %v", err)
	}

	created, err := cli.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config: &container.Config{Image: "alpine:3"},
		HostConfig: &container.HostConfig{
			Mounts: []mount.Mount{{
				Type: mount.TypeBind, Source: hostRoot, Target: "/apps", ReadOnly: true,
			}},
		},
	})
	if err != nil {
		t.Fatalf("create probe container: %v", err)
	}
	t.Cleanup(func() { _ = removeContainer(cli, created.ID) })

	insp, err := cli.ContainerInspect(ctx, created.ID, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatalf("inspect probe container: %v", err)
	}
	short := insp.Container.Config.Hostname
	if short == "" || !strings.HasPrefix(created.ID, short) {
		t.Fatalf("probe container hostname %q is not its short ID (%q)", short, created.ID)
	}

	m, err := NewManager(testutil.DiscardLogger(), nil, short)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()

	if got, want := m.daemonSourcePath(ctx, "/apps/myapp"), filepath.Join(hostRoot, "myapp"); got != want {
		t.Fatalf("daemonSourcePath(/apps/myapp) = %q, want %q", got, want)
	}
	if got := m.daemonSourcePath(ctx, "/not-mounted/app"); got != "/not-mounted/app" {
		t.Fatalf("uncovered path = %q, want it unchanged", got)
	}
}

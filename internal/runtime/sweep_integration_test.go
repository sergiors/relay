//go:build integration

// Orphan-container sweep integration test: hostname/label scoping against a
// real daemon with this worker, another worker, and an unrelated container.
package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"

	"relay/internal/testutil"
)

// createOrphanContainer creates and starts a node:24-alpine container carrying
// the given labels and command, returning its ID. t.Cleanup force-removes it so
// the sweep tests never leak.
func createOrphanContainer(t *testing.T, cli *client.Client, ctx context.Context, labels map[string]string) string {
	t.Helper()
	resp, err := cli.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config:     &container.Config{Image: "node:24-alpine", Labels: labels, Cmd: []string{"sh", "-c", "sleep 300"}},
		HostConfig: &container.HostConfig{},
	})
	if err != nil {
		t.Fatalf("create orphan container: %v", err)
	}
	id := resp.ID
	t.Cleanup(func() {
		_ = removeContainer(cli, id)
	})
	if _, err := cli.ContainerStart(ctx, id, client.ContainerStartOptions{}); err != nil {
		t.Fatalf("start orphan container %s: %v", id, err)
	}
	return id
}

// TestIntegrationSweepOrphanContainers verifies the startup sweep removes a
// stalled Relay container owned by the current hostname, leaves another worker's
// container alone, and never touches an unrelated (non-Relay-labeled) container.
func TestIntegrationSweepOrphanContainers(t *testing.T) {
	m, _ := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cli := testutil.RequireDocker(t)

	// Our orphan: relay labels + our hostname, left running (as a crashed prior
	// process would leave a mid-invocation container).
	ours := createOrphanContainer(t, cli, ctx, map[string]string{
		labelType: ContainerTypeEvent, labelFunction: "orphan-fn", labelHostname: "test-host", labelHandler: "index.hi",
	})
	// Another worker's orphan: different hostname, must survive.
	theirs := createOrphanContainer(t, cli, ctx, map[string]string{
		labelType: ContainerTypeEvent, labelFunction: "orphan-fn", labelHostname: "other-host", labelHandler: "index.hi",
	})
	// Unrelated container: no relay labels, must survive.
	unrelated := createOrphanContainer(t, cli, ctx, map[string]string{"app": "whatever"})

	if _, err := m.SweepOrphanContainers(ctx, "test-host"); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	// Our hostname's orphan is gone...
	list, _ := cli.ContainerList(ctx, client.ContainerListOptions{All: true})
	for _, c := range list.Items {
		if c.ID == ours {
			t.Errorf("orphan container %s (ours) should have been swept", ours)
		}
	}
	// ...while the other worker's and the unrelated container survive.
	for _, cid := range []string{theirs, unrelated} {
		found := false
		for _, c := range list.Items {
			if c.ID == cid {
				found = true
			}
		}
		if !found {
			t.Errorf("container %s should NOT have been swept", cid)
		}
	}
}

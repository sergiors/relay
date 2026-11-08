package testutil

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/client"
)

// imageRequirement records one process-local ensure attempt for a reference.
// The result is cached so sibling integration tests in the same package do not
// repeat the pull, while a failed ensure is still reported to every caller.
type imageRequirement struct {
	once sync.Once
	err  error
}

var requiredImages sync.Map // map[string]*imageRequirement

// RequireImage ensures ref is present in the local Docker daemon, pulling it
// only when the daemon does not already have it. The check is process-local and
// keyed by daemon host and reference, so several integration tests sharing one
// fixture image cause at most one pull per test process.
//
// The pull uses the daemon's default platform, the same platform a local
// container created from ref will run as. It fails the test rather than skipping
// when the image cannot be obtained, matching RequireDocker.
func RequireImage(t *testing.T, cli *client.Client, ref string) {
	t.Helper()
	key := cli.DaemonHost() + "\x00" + ref
	v, _ := requiredImages.LoadOrStore(key, &imageRequirement{})
	req := v.(*imageRequirement)
	req.once.Do(func() {
		req.err = ensureImage(cli, ref)
	})
	if req.err != nil {
		t.Fatalf("docker integration test requires image %s: %v", ref, req.err)
	}
}

func ensureImage(cli *client.Client, ref string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if _, err := cli.ImageInspect(ctx, ref); err != nil {
		if !cerrdefs.IsNotFound(err) {
			return fmt.Errorf("inspect: %w", err)
		}
	} else {
		return nil
	}
	pull, err := cli.ImagePull(ctx, ref, client.ImagePullOptions{})
	if err != nil {
		return fmt.Errorf("pull: %w", err)
	}
	if err := pull.Wait(ctx); err != nil {
		return fmt.Errorf("wait for pull: %w", err)
	}
	return nil
}

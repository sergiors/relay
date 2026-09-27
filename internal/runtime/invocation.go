package runtime

import (
	"context"
	"time"

	"github.com/moby/moby/client"
)

// containerOpTimeout bounds one best-effort container kill/remove Docker call.
// It is the per-call cap used by the detached (non-shutdown) teardown paths and
// by every context-aware variant, so a slow daemon can never hold a teardown
// open longer than this.
const containerOpTimeout = 5 * time.Second

// killContainer force-kills a container with a detached, bounded context so it
// works even when the invocation ctx is already cancelled.
func killContainer(cli *client.Client, id string) {
	killContainerContext(context.Background(), cli, id)
}

// killContainerContext is the context-aware kill: the bound is min(ctx, the
// containerOpTimeout cap), so a shutdown teardown observes the shutdown context
// (and stops waiting promptly) while a detached caller still gets a finite cap.
func killContainerContext(ctx context.Context, cli *client.Client, id string) {
	killCtx, cancel := context.WithTimeout(ctx, containerOpTimeout)
	defer cancel()
	_, _ = cli.ContainerKill(killCtx, id, client.ContainerKillOptions{})
}

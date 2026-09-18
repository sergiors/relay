package runtime

import (
	"context"
	"time"

	"github.com/moby/moby/client"
)

// killContainer force-kills a container with a detached, bounded context so it
// works even when the invocation ctx is already cancelled.
func killContainer(cli *client.Client, id string) {
	killCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = cli.ContainerKill(killCtx, id, client.ContainerKillOptions{})
}

package runtime

import (
	"context"
	"errors"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

// ptr returns a pointer to v. It is a tiny helper for the pointer-typed fields
// in the container HostConfig (e.g. PidsLimit) so the hardening values read as
// literals rather than requiring a local variable.
func ptr[T any](v T) *T { return &v }

// hardenedHostConfig returns the shared security/resource baseline HostConfig
// every Relay container is created with: all capabilities dropped, memory/
// CPU/pids-limited, a read-only rootfs, and a bounded /tmp tmpfs as the only
// writable path. These are internal defaults, not configuration. Networking is
// left enabled (outbound access is a legitimate function need). autoRemove is
// true for one-shot invocation containers (the daemon removes them the moment
// they exit) and false for persistent service containers (the reconciler owns
// their removal).
func hardenedHostConfig(autoRemove bool) *container.HostConfig {
	return &container.HostConfig{
		AutoRemove: autoRemove,
		Resources: container.Resources{
			Memory:    128 << 20,     // 128 MiB
			NanoCPUs:  1_000_000_000, // 1 CPU
			PidsLimit: ptr(int64(128)),
		},
		CapDrop:        []string{"ALL"},
		ReadonlyRootfs: true,
		Tmpfs:          map[string]string{"/tmp": "rw,nosuid,noexec,size=64m"},
	}
}

// benignRemovalErr reports whether a container-removal error means the container
// will not outlive the call, which is the only contract a best-effort removal
// has. Two daemon responses are benign:
//
//   - not-found (cerrdefs.ErrNotFound): the daemon already removed the container
//     (AutoRemove finished, or a previous remove succeeded). Nothing left to do.
//   - conflict (cerrdefs.ErrConflict, HTTP 409): the daemon is removing the
//     container right now (AutoRemove in flight). The container still exists in
//     the daemon's map until the removal completes, so a DELETE issued in that
//     window races the daemon's own removal and answers 409 rather than 404; the
//     container will still be gone.
//
// A nil error (no error) is trivially benign. Any other error (permission
// denied, internal, ...) is a genuine failure and is NOT benign. Matching is
// typed (errors.Is against the errdefs sentinels), never on the error string.
func benignRemovalErr(err error) bool {
	return err == nil || errors.Is(err, cerrdefs.ErrNotFound) || errors.Is(err, cerrdefs.ErrConflict)
}

// removeContainer removes a container, treating an already-removed container and
// a removal already in progress as success. With AutoRemove the daemon removes
// the container as soon as it exits, so Relay's best-effort removal routinely
// races the daemon: it either finds the container already gone (not-found) or
// already being removed (conflict/409). Both mean "the container will not outlive
// this call", which is the only contract a best-effort remove has, so neither is
// logged as noise. Only a real (non-benign) removal failure is surfaced so the
// caller can log it.
func removeContainer(cli *client.Client, id string) error {
	rmCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cli.ContainerRemove(rmCtx, id, client.ContainerRemoveOptions{Force: true})
	if benignRemovalErr(err) {
		return nil
	}
	return err
}

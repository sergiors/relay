package runtime

import (
	"context"
	"fmt"

	"github.com/moby/moby/client"
)

// SweepOrphanContainers removes execution containers left behind by a previous
// Relay process on THIS hostname that never exited (crashed mid-invocation, or
// a create-without-exit). It is hostname-scoped and label-scoped so it can never
// touch another worker's containers or any non-Relay container. It returns the
// number of containers removed; per-container failures are logged (with the
// container's label context) and do not abort the sweep.
//
// Ownership predicate: a container is Relay-owned and eligible for removal when
// it carries the relay.function label AND its relay.hostname label equals the
// current hostname. Containers owned by a different hostname — live or crashed
// — are another worker's property and are never touched. Containers without a
// relay.function label are not Relay execution containers and are never touched.
//
// Running-container decision: at sweep time this process has not yet created any
// containers (the sweep runs before the first Prepare/Execute), so any Relay
// container carrying our hostname already existed before we started and must be
// from a previous process on this host. That previous process is the only Relay
// process that could own our hostname (hostnames are unique per worker), so a
// RUNNING container with our hostname is necessarily that stale process's. Both
// exited and running stale containers are therefore removed; running ones are
// force-removed (which stops them).
//
// Limitation: two bare-metal workers sharing BOTH the same hostname AND the same
// Docker daemon would each consider the other's live containers stale and remove
// them. Docker and Kubernetes never produce this (containers/pods get unique
// hostnames), so it is a documented, accepted hazard of the hostname-as-identity
// model rather than a correctness bug in normal deployments.
func (m *Manager) SweepOrphanContainers(ctx context.Context, hostname string) (int, error) {
	list, err := m.cli.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		return 0, fmt.Errorf("orphan sweep: list containers: %w", err)
	}

	skippedOther := 0
	removed := 0
	var firstErr error
	for _, c := range list.Items {
		_, owned := c.Labels[labelFunction]
		if !owned {
			// Not a Relay execution container; never touch it.
			continue
		}
		if c.Labels[labelHostname] != hostname {
			// Another worker's property (live or crashed); leave it alone.
			skippedOther++
			continue
		}
		if err := removeContainer(m.cli, c.ID); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			m.log.Printf("orphan sweep: remove container %s (%s %s): %v",
				c.ID, c.Labels[labelFunction], c.Labels[labelHandler], err)
			continue
		}
		removed++
	}
	if skippedOther > 0 {
		m.log.Printf("orphan sweep: skipped %d container(s) owned by other hostnames", skippedOther)
	}
	if firstErr != nil {
		return removed, firstErr
	}
	return removed, nil
}

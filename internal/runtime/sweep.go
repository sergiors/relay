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
// it is an invocation container (relay.type is strictly event or schedule) AND
// its relay.hostname label equals the current hostname. This is the strict,
// exclusive sweep set: the sweep only ever removes containers stamped with an
// explicit relocation type of event or schedule, never anything else — not
// service containers (reconciler-owned), not containers with an unknown/missing
// type (non-Relay). Containers owned by a different hostname — live or crashed
// — are another worker's property and are never touched.
//
// Backward compatibility with labels from Relay processes older than the strict
// relay.type model is deliberately NOT required: a restart treats such untyped
// containers as non-Relay and leaves them alone.
//
// sweepSkips reports whether a container's label set must be excluded from the
// orphan sweep, before the ownership predicate is even considered. Three
// populations are excluded: service containers (isServiceContainer — persistent
// and reconciler-owned, never swept) and non-Relay containers that carry no
// known relay.type (not isRelayContainer). Only invocation containers
// (isInvocationContainer) pass through to the hostname check.
func sweepSkips(labels map[string]string) bool {
	if isServiceContainer(labels) {
		// Persistent service containers are reconciler-owned and long-lived; the
		// startup orphan sweep must never kill a live service.
		return true
	}
	return !isRelayContainer(labels)
}

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
		if sweepSkips(c.Labels) {
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
			m.log.Warn(fmt.Sprintf("Orphan sweep: remove container %s (%s %s): %v",
				c.ID, c.Labels[labelFunction], c.Labels[labelHandler], err))
			continue
		}
		removed++
	}
	if skippedOther > 0 {
		m.log.Debug(fmt.Sprintf("Orphan sweep: skipped %d container(s) owned by other hostnames", skippedOther))
	}
	if firstErr != nil {
		return removed, firstErr
	}
	return removed, nil
}

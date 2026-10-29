package runtime

import (
	"context"
	"fmt"

	"github.com/moby/moby/client"
)

// OrphanContainer identifies ONE execution container the startup orphan sweep
// has decided is safe to remove: a container whose strict relay.type label is
// event or schedule and whose relay.node_id equals this node's identity. Its
// ID is an immutable Docker identifier, so the removal step targets exactly the
// container that was observed and can never match a different container later
// created under the same node identity (a replacement process always gets a
// NEW id).
//
// It deliberately carries only what the removal step reports/logs (the id and
// the app/handler label context); ownership is decided at discovery time and
// encoded in the fact that the candidate exists at all.
type OrphanContainer struct {
	ID      string
	App     string
	Handler string
}

// sweepSkips reports whether a container's label set must be excluded from the
// orphan sweep, before the ownership predicate is even considered. Two
// populations are excluded: service containers (isServiceContainer — persistent
// and reconciler-owned, never swept) and non-Relay containers that carry no
// known relay.type (not isManagedContainer). Only invocation containers (an
// explicit relay.type of event or schedule) pass through to the node-identity
// check.
func sweepSkips(labels map[string]string) bool {
	if isServiceContainer(labels) {
		// Persistent service containers are reconciler-owned and long-lived; the
		// startup orphan sweep must never kill a live service.
		return true
	}
	return !isManagedContainer(labels)
}

// DiscoverOrphanContainers lists the execution containers left behind by a
// previous Relay process for THIS node identity that never exited (crashed
// mid-invocation, or a create-without-exit). It is the DISCOVERY half of the
// startup orphan sweep, split out so the caller can snapshot the candidates and
// then prove ownership BEFORE removing anything (see the ownership-ordering note
// on Manager.RemoveOrphanContainers). It is node-scoped and label-scoped so
// it can never select another worker's containers or any non-Relay container.
//
// Ownership predicate: a container is Relay-owned and eligible for removal when
// it is an invocation container (relay.type is strictly event or schedule) AND
// its relay.node_id label equals the current node identity. This is the strict,
// exclusive sweep set: the sweep only ever removes containers stamped with an
// explicit relocation type of event or schedule, never anything else — not
// service containers (reconciler-owned), not containers with an unknown/missing
// type (non-Relay). Containers owned by a different node identity — live or
// crashed — are another worker's property and are never selected.
//
// Backward compatibility with labels from Relay processes older than the strict
// relay.type model is deliberately NOT required: a restart treats such untyped
// containers as non-Relay and leaves them alone.
func (m *Manager) DiscoverOrphanContainers(ctx context.Context, nodeID string) ([]OrphanContainer, error) {
	callerCtx := ctx
	opCtx, release, err := m.beginDockerOp(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	ctx = opCtx
	list, err := m.cli.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		return nil, fmt.Errorf("orphan sweep: list containers: %w", m.classifyDockerErr(callerCtx, err))
	}

	var out []OrphanContainer
	skippedOther := 0
	for _, c := range list.Items {
		if sweepSkips(c.Labels) {
			continue
		}
		if c.Labels[labelNodeID] != nodeID {
			// Another worker's property (live or crashed); leave it alone.
			skippedOther++
			continue
		}
		out = append(out, OrphanContainer{
			ID:      c.ID,
			App:     c.Labels[labelApp],
			Handler: c.Labels[labelHandler],
		})
	}
	if skippedOther > 0 {
		m.log.Debug(
			"Orphan sweep: skipped containers owned by other node identities",
			"count", skippedOther,
		)
	}
	return out, nil
}

// RemoveOrphanContainers force-removes exactly the given containers, BY
// IMMUTABLE DOCKER ID. It is the REMOVAL half of the startup orphan sweep and
// never re-lists the daemon: the caller passes the candidates captured by
// DiscoverOrphanContainers, so a container created AFTER that snapshot (for
// example by a replacement process that took over this node identity while the
// ownership proof was running) can never be selected — its id is not in the
// captured set. A captured container that has since disappeared is a benign
// not-found (removeContainerContext treats it as success).
//
// It returns the number removed; per-container failures are logged (with the
// container's label context) and do not abort the sweep, and the first error is
// returned at the end (classified against Manager shutdown). The loop stops
// promptly once ctx is done.
//
// Running-container decision: at sweep time this process has not yet created
// any containers (the sweep runs before the first Prepare/Execute), so any
// Relay container carrying our node identity already existed before we started
// and must be from a previous process on this host. Both exited and running
// stale containers are therefore removed; running ones are force-removed (which
// stops them).
//
// Limitation: two bare-metal workers sharing BOTH the same node identity AND
// the same Docker daemon would each consider the other's live containers stale
// and remove them. Docker and Kubernetes never produce this (containers/pods get
// unique hostnames), so it is a documented, accepted hazard of the node-identity
// model rather than a correctness bug in normal deployments.
func (m *Manager) RemoveOrphanContainers(ctx context.Context, orphans []OrphanContainer) (int, error) {
	if len(orphans) == 0 {
		return 0, nil
	}
	callerCtx := ctx
	opCtx, release, err := m.beginDockerOp(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	ctx = opCtx

	removed := 0
	var firstErr error
	for _, o := range orphans {
		if err := ctx.Err(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			break
		}
		if err := removeContainerContext(ctx, m.cli, o.ID); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			m.log.Warn("Orphan sweep: remove container failed",
				"container", o.ID,
				"app", o.App,
				"handler", o.Handler,
				"error", err,
			)
			continue
		}
		removed++
	}
	if firstErr != nil {
		// A sweep error that races Manager shutdown is reported as the terminal
		// deferral (ErrManagerShuttingDown); a genuine Docker failure, or the
		// caller's own cancellation, is returned unchanged (classify never masks
		// a caller-owned context error).
		return removed, m.classifyDockerErr(callerCtx, firstErr)
	}
	return removed, nil
}

// SweepOrphanContainers removes execution containers left behind by a previous
// Relay process on THIS nodeID that never exited. It is the convenience form
// of the split sweep: DiscoverOrphanContainers followed immediately by
// RemoveOrphanContainers. Callers that will prove ownership between discovery
// and removal must call the two halves directly, so the ownership proof runs
// AFTER the candidate snapshot and BEFORE any removal — and there must be no
// discovery after that proof.
func (m *Manager) SweepOrphanContainers(ctx context.Context, nodeID string) (int, error) {
	orphans, err := m.DiscoverOrphanContainers(ctx, nodeID)
	if err != nil {
		return 0, err
	}
	return m.RemoveOrphanContainers(ctx, orphans)
}

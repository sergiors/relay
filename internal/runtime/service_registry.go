package runtime

import (
	"context"
	"fmt"
	"sort"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

// RegistryServiceEndpoint is one persistent service replica this manager can
// prove is currently serving, with the host endpoint Docker actually assigned.
// It is deliberately a REGISTRY collection view, separate from ServiceEndpoint
// (the raw `relay app inspect` view): only committed, running, ready replicas
// with a verified host binding are returned, and the caller still decides
// whether the container's routing configuration matches the current template.
//
// Generation is the Docker-owned committed-generation identity recorded by the
// service reconciler; ContainerID is the exact committed container. Labels is
// the container's full label set, used by the caller to match the container's
// configured route (never to infer identity here).
type RegistryServiceEndpoint struct {
	App          string
	Service      string
	Replica      int
	InternalPort int
	ContainerID  string
	Generation   string
	HostIP       string
	HostPort     int
	Labels       map[string]string
}

// RegistryServiceEndpoints returns this node's service replicas that are safe to
// advertise in the distributed service registry. A replica is included only
// when ALL of the following hold:
//
//   - it carries this manager's relay.node_id;
//   - a Docker-owned committed/candidate marker names this exact container as
//     the committed replica for its (app, service, replica) slot, with a
//     non-empty generation identity;
//   - it is running and, when Relay applied a healthcheck, Docker reports it
//     healthy (a service without Relay's healthcheck keeps the running-is-ready
//     behavior; an image-declared HEALTHCHECK never changes this decision);
//   - it publishes its port on expectedBindAddr, and inspection proves exactly
//     one host binding for the service's internal TCP port on that exact
//     address.
//
// A missing marker, a stopped/unhealthy container, an absent or unexpected
// binding, and a container that disappeared before inspection are all simply
// omitted. A failure to list containers or markers is returned with no result,
// so the caller keeps the previously published snapshot. A failure to inspect
// ONE container is returned alongside the successfully verified endpoints: the
// caller can publish the partial batch instead of letting one unreadable
// container suppress every healthy endpoint (the omitted container is retried on
// the next sync).
//
// expectedBindAddr is the resolved SERVICE_BIND_ADDR. An empty value disables
// collection (services are internal-only). The registry never creates, mutates,
// or stops a container.
func (m *Manager) RegistryServiceEndpoints(
	ctx context.Context,
	expectedBindAddr string,
) ([]RegistryServiceEndpoint, error) {
	expected, err := parseServiceBindAddr(expectedBindAddr)
	if err != nil {
		return nil, fmt.Errorf("service registry: %w", err)
	}
	if !expected.IsValid() {
		return nil, nil
	}

	containers, err := m.ServiceContainerList(ctx)
	if err != nil {
		return nil, err
	}
	states, err := m.ListServiceConvergenceStates(ctx)
	if err != nil {
		return nil, err
	}
	committed := committedServiceReplicas(states, m.nodeID)

	callerCtx := ctx
	opCtx, release, err := m.beginDockerOp(ctx)
	if err != nil {
		return nil, err
	}
	defer release()

	out := make([]RegistryServiceEndpoint, 0, len(containers))
	var firstErr error
	for _, ctr := range containers {
		if ctr.NodeID != m.nodeID || ctr.BindAddr == "" {
			continue
		}
		marker, ok := committed[serviceRegistrySlot{
			app: ctr.App, service: ctr.Name, replica: ctr.Replica,
		}]
		if !ok || marker.CommittedContainerID != ctr.ID || marker.Generation == "" {
			continue
		}
		if ctr.State != container.StateRunning || ctr.Port < 1 || ctr.Replica < 0 {
			continue
		}
		if ctr.Ready != nil && !*ctr.Ready {
			continue
		}
		if ctr.Healthcheck != "" && ctr.Health != container.Healthy {
			continue
		}
		insp, err := m.cli.ContainerInspect(opCtx, ctr.ID, client.ContainerInspectOptions{})
		if err != nil {
			if cerrdefs.IsNotFound(err) {
				continue
			}
			if firstErr == nil {
				firstErr = fmt.Errorf(
					"service registry: inspect container %s: %w",
					ctr.ID, m.classifyDockerErr(callerCtx, err))
			}
			continue
		}
		// Re-check the authoritative inspect state: the list snapshot may be
		// stale, and a commit decision must not be based on it alone.
		state := insp.Container.State
		if state == nil || state.Status != container.StateRunning {
			continue
		}
		if ctr.Healthcheck != "" &&
			(state.Health == nil || state.Health.Status != container.Healthy) {
			continue
		}
		hostIP, hostPort, ok := servicePublishedBinding(insp.Container, ctr.Port, expected)
		if !ok {
			continue
		}
		out = append(out, RegistryServiceEndpoint{
			App:          ctr.App,
			Service:      ctr.Name,
			Replica:      ctr.Replica,
			InternalPort: ctr.Port,
			ContainerID:  ctr.ID,
			Generation:   marker.Generation,
			HostIP:       hostIP,
			HostPort:     hostPort,
			Labels:       ctr.Labels,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].App != out[j].App {
			return out[i].App < out[j].App
		}
		if out[i].Service != out[j].Service {
			return out[i].Service < out[j].Service
		}
		if out[i].Replica != out[j].Replica {
			return out[i].Replica < out[j].Replica
		}
		return out[i].ContainerID < out[j].ContainerID
	})
	return out, firstErr
}

type serviceRegistrySlot struct {
	app     string
	service string
	replica int
}

// committedServiceReplicas reduces the marker volumes to the newest committed
// record per replica slot for nodeID. Old marker versions can coexist while a
// write is in flight, so the newest UpdatedAt wins, mirroring the service
// reconciler's recovery rule.
func committedServiceReplicas(
	states []ServiceConvergenceState,
	nodeID string,
) map[serviceRegistrySlot]ServiceConvergenceState {
	out := make(map[serviceRegistrySlot]ServiceConvergenceState, len(states))
	for _, state := range states {
		if state.Kind != "" || state.NodeID != nodeID ||
			state.CommittedContainerID == "" || state.Replica < 0 {
			continue
		}
		key := serviceRegistrySlot{
			app: state.App, service: state.Service, replica: state.Replica,
		}
		if existing, ok := out[key]; ok && existing.UpdatedAt.After(state.UpdatedAt) {
			continue
		}
		out[key] = state
	}
	return out
}

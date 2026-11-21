package runtime

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

// ServiceHealthCandidate identifies one persistent service container whose
// Docker readiness a health observer should watch. It carries no desired-state
// authority: an observation only hints the coordinator to re-run reconcile,
// which revalidates everything against Docker.
type ServiceHealthCandidate struct {
	App         string
	ContainerID string
}

// ServiceHealthObservation is the Docker state and health of one service
// container at observation time. State is the container lifecycle state (running,
// exited, ...); Health is "" when Docker reports no health status. StartedAt is
// the instant Docker actually started the container (zero when unreadable), the
// durable anchor for the candidate's startup grace.
type ServiceHealthObservation struct {
	State     container.ContainerState
	Health    container.HealthStatus
	StartedAt time.Time
	// Missing is a DEFINITIVE not-found observation. It is distinct from a
	// transient inspect failure, which is reported through the call error and
	// leaves the container out of the result map.
	Missing bool
}

// ServiceStateKindPreparation is the marker family for the service-level
// PREPARATION retry record. It is a distinct key from a replica's committed /
// candidate marker (the empty kind), so a preparation failure and a failed
// candidate for the same service/replica keep their own attempt and window
// instead of overwriting each other. Both are ordinary Docker-owned marker
// volumes, scoped by the same app/service/node identity and generation-fenced
// independently.
const ServiceStateKindPreparation = "preparation"

// ServiceStateKindPreparationCompleted is the marker family recording that one
// ADMITTED preparation attempt COMPLETED successfully. It is keyed by the same
// app/service/node identity as the retry family but is a separate kind, so the
// retry family can still be pruned after success while the completed outcome
// remains durable. The record carries the admission's generation, attempt, and
// start instant (CandidateStartedAt) as its identity and no retry window; a
// restart therefore distinguishes a leftover admission whose preparation
// succeeded from one whose outcome was never recorded.
const ServiceStateKindPreparationCompleted = "preparation-completed"

// ServiceConvergenceState is the durable, Docker-owned marker that distinguishes
// a committed (active) service replica from an uncommitted healthcheck candidate.
// It is stored as labels on a tiny Relay-owned Docker volume (never mounted) so
// it survives worker restarts without a PostgreSQL table. CommittedContainerID is
// the replica believed active for this exact Generation; an absent/mismatched
// marker means a config-matching container is still a candidate.
//
// Kind selects the independent marker family: "" is the per-replica
// committed/candidate record, ServiceStateKindPreparation is the
// service-level preparation retry record (which has no replica container), and
// ServiceStateKindPreparationCompleted records a successfully completed
// preparation admission. The key includes Kind, so the families never share a
// volume or a cache entry.
type ServiceConvergenceState struct {
	Kind                 string
	App                  string
	Service              string
	NodeID               string
	Replica              int
	CommittedContainerID string
	Generation           string
	// Candidate fields persist the current uncommitted healthcheck candidate and
	// its failure retry eligibility. RetryNotBefore is set when a failure is
	// observed, so a restart reconstructs the same bounded wait instead of
	// recalculating it from create time. CandidateStartedAt is the instant the
	// current attempt began (the container's actual start when known, otherwise
	// the pre-start/admission write); it records the attempt for diagnostics and
	// ordering. It is deliberately NOT the anchor for an unrecorded outcome: when
	// the failure instant was never persisted, a restart measures the full bounded
	// window from recovery instead, conservatively (an admission-time anchor could
	// already be elapsed after a long operation). CandidateContainerID is empty
	// while an attempt is admitted but has no container yet (a preparation failure
	// or an in-flight start).
	CandidateContainerID string
	CandidateGeneration  string
	CandidateAttempt     int
	CandidateStartedAt   time.Time
	RetryNotBefore       time.Time
	UpdatedAt            time.Time
}

// StateMarkerStore is the optional Docker capability used to persist and
// reconstruct committed-generation markers. The production Manager implements
// it; a minimal fake without it keeps the in-memory readiness behavior.
//
// These methods are ordinary Manager-admitted Docker operations: admission
// propagates cancellation and joins shutdown, but deliberately adds no deadline
// of its own (like every other Manager method). A caller that owns a normal
// operation budget — the service reconciler's reconcileTimeout — must derive a
// bounded context for each distinct marker list/write/delete, so a daemon that
// stops answering cannot pin a worker or a startup step.
type StateMarkerStore interface {
	ListServiceConvergenceStates(ctx context.Context) ([]ServiceConvergenceState, error)
	WriteServiceConvergenceState(ctx context.Context, state ServiceConvergenceState) error
	DeleteServiceConvergenceStates(ctx context.Context, app, service, nodeID string) error
}

// ObserveServiceHealth inspects exactly the given service containers and returns
// their current state and Docker health. It is the observer's narrow Docker seam:
// only pending candidates are inspected, never every service container. A
// container that no longer exists is omitted from the result; a genuine inspect
// failure is returned with the partial observation map so the caller can log it
// and retry on the next tick.
func (m *Manager) ObserveServiceHealth(
	ctx context.Context,
	ids []string,
) (map[string]ServiceHealthObservation, error) {
	out := make(map[string]ServiceHealthObservation, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	callerCtx := ctx
	opCtx, release, err := m.beginDockerOp(ctx)
	if err != nil {
		return out, err
	}
	defer release()
	ctx = opCtx
	var firstErr error
	for _, id := range ids {
		if id == "" {
			continue
		}
		insp, err := m.cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
		if err != nil {
			if cerrdefs.IsNotFound(err) {
				out[id] = ServiceHealthObservation{Missing: true}
				continue
			}
			if firstErr == nil {
				firstErr = fmt.Errorf("inspect service container %s: %w", id, m.classifyDockerErr(callerCtx, err))
			}
			continue
		}
		observation := ServiceHealthObservation{}
		if insp.Container.State != nil {
			observation.State = insp.Container.State.Status
			observation.StartedAt = parseDockerStartInstant(insp.Container.State.StartedAt)
			if insp.Container.State.Health != nil {
				observation.Health = insp.Container.State.Health.Status
			}
		}
		out[id] = observation
	}
	return out, firstErr
}

// ListServiceConvergenceStates returns every committed-generation marker volume
// on the daemon. Only volumes carrying Relay's strict service-state label are
// considered; no name-based or global pruning is involved.
func (m *Manager) ListServiceConvergenceStates(
	ctx context.Context,
) ([]ServiceConvergenceState, error) {
	callerCtx := ctx
	opCtx, release, err := m.beginDockerOp(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	ctx = opCtx
	filters := client.Filters{}.Add("label", labelType+"="+labelServiceStateType)
	resp, err := m.cli.VolumeList(ctx, client.VolumeListOptions{Filters: filters})
	if err != nil {
		return nil, fmt.Errorf("service: list convergence state: %w", m.classifyDockerErr(callerCtx, err))
	}
	out := make([]ServiceConvergenceState, 0, len(resp.Items))
	for _, volume := range resp.Items {
		if volume.Labels[labelType] != labelServiceStateType {
			continue
		}
		replica, err := strconv.Atoi(volume.Labels[labelReplica])
		if err != nil {
			continue
		}
		updated, err := time.Parse(time.RFC3339Nano, volume.Labels[labelUpdatedAt])
		if err != nil {
			updated, err = time.Parse(time.RFC3339, volume.CreatedAt)
			if err != nil {
				updated = time.Time{}
			}
		}
		attempt, _ := strconv.Atoi(volume.Labels[labelCandidateAttempt])
		retryAt, _ := time.Parse(time.RFC3339Nano, volume.Labels[labelRetryNotBefore])
		startedAt, _ := time.Parse(time.RFC3339Nano, volume.Labels[labelCandidateStartedAt])
		out = append(out, ServiceConvergenceState{
			Kind:                 volume.Labels[labelStateKind],
			App:                  volume.Labels[labelApp],
			Service:              volume.Labels[labelService],
			NodeID:               volume.Labels[labelNodeID],
			Replica:              replica,
			CommittedContainerID: volume.Labels[labelCommitted],
			Generation:           volume.Labels[labelGeneration],
			CandidateContainerID: volume.Labels[labelCandidateContainer],
			CandidateGeneration:  volume.Labels[labelCandidateGeneration],
			CandidateAttempt:     attempt,
			CandidateStartedAt:   startedAt,
			RetryNotBefore:       retryAt,
			UpdatedAt:            updated,
		})
	}
	return out, nil
}

// WriteServiceConvergenceState records one committed-generation marker by
// creating a new versioned Relay-owned volume with the current labels and then
// removing older marker volumes for the same key. Create-before-delete makes an
// interrupted update safe: the old marker remains until the new one exists, and
// discovery always picks the newest. Only volumes with the strict state label
// are removed.
func (m *Manager) WriteServiceConvergenceState(
	ctx context.Context,
	state ServiceConvergenceState,
) error {
	if state.App == "" || state.Service == "" || state.Replica < 0 {
		return errors.New("service: invalid convergence state key")
	}
	if state.UpdatedAt.IsZero() {
		state.UpdatedAt = time.Now()
	}
	callerCtx := ctx
	opCtx, release, err := m.beginDockerOp(ctx)
	if err != nil {
		return err
	}
	defer release()
	ctx = opCtx
	labels := map[string]string{
		labelType:       labelServiceStateType,
		labelApp:        state.App,
		labelService:    state.Service,
		labelNodeID:     state.NodeID,
		labelReplica:    strconv.Itoa(state.Replica),
		labelCommitted:  state.CommittedContainerID,
		labelGeneration: state.Generation,
		labelUpdatedAt:  state.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
	if state.CandidateContainerID != "" {
		labels[labelCandidateContainer] = state.CandidateContainerID
	}
	if state.CandidateGeneration != "" {
		labels[labelCandidateGeneration] = state.CandidateGeneration
	}
	if state.CandidateAttempt > 0 {
		labels[labelCandidateAttempt] = strconv.Itoa(state.CandidateAttempt)
	}
	if !state.CandidateStartedAt.IsZero() {
		labels[labelCandidateStartedAt] = state.CandidateStartedAt.UTC().Format(time.RFC3339Nano)
	}
	if !state.RetryNotBefore.IsZero() {
		labels[labelRetryNotBefore] = state.RetryNotBefore.UTC().Format(time.RFC3339Nano)
	}
	if state.Kind != "" {
		labels[labelStateKind] = state.Kind
	}
	name := serviceStateVolumeName(state)
	if _, err := m.cli.VolumeCreate(ctx, client.VolumeCreateOptions{
		Name:   name,
		Labels: labels,
	}); err != nil {
		return fmt.Errorf("service: write convergence state: %w", m.classifyDockerErr(callerCtx, err))
	}
	created := name
	// Cleanup is scoped to the SAME kind: a preparation record must never delete
	// a replica's candidate/committed marker just because they share the
	// app/service/replica identity.
	old, listErr := m.listServiceStateVolumes(ctx, state, true, true)
	if listErr != nil {
		// The new marker is authoritative; an old marker is harmless because
		// discovery selects the newest. Log at debug only.
		m.log.Debug("Service: convergence state cleanup list failed",
			"app", state.App, "service", state.Service, "replica", state.Replica, "error", listErr)
		return nil
	}
	for _, volume := range old {
		if volume.Name == created {
			continue
		}
		if err := m.removeServiceStateVolume(ctx, volume.Name); err != nil {
			m.log.Debug("Service: convergence state cleanup failed",
				"app", state.App, "service", state.Service, "replica", state.Replica, "error", err)
		}
	}
	return nil
}

// DeleteServiceConvergenceStates removes marker volumes for one app (and, when
// service is non-empty, one service). It is used when an app or service is
// removed. Only Relay service-state volumes are touched.
func (m *Manager) DeleteServiceConvergenceStates(
	ctx context.Context,
	app, service, nodeID string,
) error {
	return m.deleteServiceConvergenceStates(ctx, app, service, nodeID, false, "")
}

// DeleteServiceConvergenceStatesForKind removes marker volumes for one app (and,
// when service is non-empty, one service) restricted to ONE marker family
// (kind). It is the bounded cleanup a caller uses to drop an obsolete
// preparation-retry record without touching the replica's committed/candidate
// markers. An empty kind means the committed/candidate family; deleting every
// family must use DeleteServiceConvergenceStates. Only Relay service-state
// volumes of that kind are touched.
func (m *Manager) DeleteServiceConvergenceStatesForKind(
	ctx context.Context,
	app, service, nodeID, kind string,
) error {
	return m.deleteServiceConvergenceStates(ctx, app, service, nodeID, true, kind)
}

func (m *Manager) deleteServiceConvergenceStates(
	ctx context.Context,
	app, service, nodeID string,
	matchKind bool,
	kind string,
) error {
	if app == "" {
		return nil
	}
	state := ServiceConvergenceState{Kind: kind, App: app, Service: service, NodeID: nodeID}
	opCtx, release, err := m.beginDockerOp(ctx)
	if err != nil {
		return err
	}
	defer release()
	// The wildcard (matchKind false) is kind-agnostic: removing an app/service
	// removes every marker family for it.
	volumes, listErr := m.listServiceStateVolumes(opCtx, state, false, matchKind)
	if listErr != nil {
		return listErr
	}
	var firstErr error
	for _, volume := range volumes {
		if err := m.removeServiceStateVolume(opCtx, volume.Name); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// listServiceStateVolumes selects Relay service-state volumes matching the key.
// exactReplica scopes the selection to one replica for write cleanup; false is
// the wildcard used by app/service deletion and orphan pruning. matchKind scopes
// the selection to the key's marker family: write cleanup must only ever remove
// older volumes of the SAME family, while deletion is kind-agnostic.
func (m *Manager) listServiceStateVolumes(
	ctx context.Context,
	key ServiceConvergenceState,
	exactReplica bool,
	matchKind bool,
) ([]volumeRef, error) {
	filters := client.Filters{}.Add("label", labelType+"="+labelServiceStateType)
	resp, err := m.cli.VolumeList(ctx, client.VolumeListOptions{Filters: filters})
	if err != nil {
		return nil, fmt.Errorf("service: list convergence state: %w", err)
	}
	out := make([]volumeRef, 0, len(resp.Items))
	for _, volume := range resp.Items {
		if volume.Labels[labelType] != labelServiceStateType ||
			volume.Labels[labelApp] != key.App {
			continue
		}
		if matchKind && volume.Labels[labelStateKind] != key.Kind {
			continue
		}
		if key.Service != "" && volume.Labels[labelService] != key.Service {
			continue
		}
		if key.NodeID != "" && volume.Labels[labelNodeID] != key.NodeID {
			continue
		}
		if exactReplica && volume.Labels[labelReplica] != strconv.Itoa(key.Replica) {
			continue
		}
		out = append(out, volumeRef{Name: volume.Name})
	}
	return out, nil
}

type volumeRef struct{ Name string }

func (m *Manager) removeServiceStateVolume(ctx context.Context, name string) error {
	_, err := m.cli.VolumeRemove(ctx, name, client.VolumeRemoveOptions{Force: true})
	if err != nil && !cerrdefs.IsNotFound(err) {
		return err
	}
	return nil
}

func serviceStateVolumeName(state ServiceConvergenceState) string {
	kind := ""
	if state.Kind != "" {
		kind = "-" + sanitizeContainerNamePart(state.Kind)
	}
	base := fmt.Sprintf(
		"relay-svc-state-%s-%s-%s%s-%d",
		sanitizeContainerNamePart(state.NodeID),
		sanitizeContainerNamePart(state.App),
		sanitizeContainerNamePart(state.Service),
		kind,
		state.Replica,
	)
	const capLength = 200
	if len(base) > capLength {
		base = base[:capLength]
	}
	return base + "-" + newRequestID()
}

// serviceContainerHealthAttempt normalizes a container's durable attempt label.
func serviceContainerHealthAttempt(ctr ServiceContainer) int {
	if ctr.HealthcheckAttempt < 1 {
		return 1
	}
	return ctr.HealthcheckAttempt
}

// ServiceHealthAttempt returns the retry generation to stamp on a new candidate
// based on the highest-attempt failed/config-matching generation, or 1 when there
// is none. It never lowers the attempt across a restart.
func ServiceHealthAttempt(candidates []ServiceContainer) int {
	attempt := 0
	for _, ctr := range candidates {
		if ctr.Healthcheck == "" {
			continue
		}
		if got := serviceContainerHealthAttempt(ctr); got > attempt {
			attempt = got
		}
	}
	return attempt + 1
}

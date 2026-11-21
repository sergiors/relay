// The service reconciler (services.go) reconciles an app's persistent
// service containers to its template. At startup and on every reconcile of the
// owning app, it lists the daemon's service containers, classifies each
// against the desired configuration, and converges the running set to the
// template's declared services and replica counts.
//
// Replacement is ZERO-DOWNTIME-BY-CONSTRUCTION: for every desired replica slot
// the reconciler starts the replacement and requires StartService to confirm it
// RUNNING (and, for a service with a healthcheck, HEALTHY) before stopping the
// superseded generation for that same logical slot. A failed create/start/inspect
// or an unhealthy candidate leaves the old generation running and the slot keeps
// serving; the next reconcile retries. Old generations, excess replicas, and
// removed services are stopped only after the desired slots have converged, so a
// service is never taken to zero replicas by a failed replacement.
//
// The component is deliberately small and the owning layer (worker wiring) is
// what decides when to invoke it. Containers owned by OTHER apps are never
// touched here: each app's reconcile owns only its own containers, and
// SweepOrphans is the single cross-app pass (called once at startup with
// the set of present app names) that removes containers whose app no
// longer exists. ShutdownCleanup is the graceful-shutdown counterpart: it
// removes service containers owned by this worker only; startup convergence
// remains the crash-recovery path when shutdown cleanup did not execute.
//
// Node identity: the production worker wires WithNodeID, which scopes every
// operation to containers whose relay.node_id equals its stable NODE_ID. A
// ServiceReconciler built without it (and the exported direct Reconcile API)
// keeps the historical unscoped, same-host behavior. The worker scope is what
// keeps two Relay nodes sharing one Docker daemon from changing each other's
// service containers, while a restart of the SAME NODE_ID still adopts the
// previous process's running, converged containers instead of recreating them.
//
// Identity is the service's stable template name (app.Service.Name) and the
// replica slot is relay.replica; both are label-derived and are the only
// grouping keys. The configured source descriptor (relay.identity) is compared
// as the desired IMPLEMENTATION, never as identity: a source change under the
// same name (a new entrypoint file or image reference) is the SAME logical
// service, so it replaces the same replica slots through the normal
// start-before-stop path. A NAME change is a removal plus an addition at this
// layer (see Reconcile's comment on that edge), which is why removed-service
// cleanup is deferred until after every desired service has converged.
//
// Ordering guarantees relied on by the worker:
//   - Reconcile is idempotent: when already converged it lists containers once
//     and mutates nothing, so calling it on every periodic reconcile tick (as a
//     cheap no-op) is safe and gives crash replacement within the default 30s
//     cadence without a separate services-only loop.
//   - Service-container removal must settle BEFORE the app's images are
//     retired, because running service containers still reference those images.
//     The worker wires the deferred removal form (EnqueueRemoveWithSettlement),
//     so the serial app pump starts the removal without blocking and the app
//     retirement runs from the settlement callback; a removal superseded by a
//     re-add settles without retiring a live app's images. The runner-side
//     reference guard remains the second line of defense for a partial
//     supersession.
//   - Routing (Traefik) for routed services (a service declaring a host) is
//     validated before any container action for that service: the routing
//     config must be present, the per-service effective host (including any
//     TRAEFIK_HOST_OVERRIDE mapping) must be a valid hostname, and the routing
//     network must exist (Relay never creates it); a routed service failing
//     routing validation is reported and skipped, not half-reconciled.
//   - A source that cannot be resolved (a failed pull, a missing image, an
//     unlaunchable entrypoint), an unresolvable environment/secret, or a
//     missing routing network is reported and leaves the existing containers
//     untouched: nothing is stopped or started for that service.
//   - Reconcile takes the LIFECYCLE context and derives a FRESH normal-operation
//     bound (the injected reconcileTimeout) for each Docker operation itself. The
//     routing/source-resolution phase and the post-resolution phase (env
//     resolution, replacement starts, stale stops) run on separate bounds, so one
//     operation cannot consume another's budget. A caller must never wrap the
//     whole pass in one short deadline.
//
// A backend may report ErrServicePending from StartService when a replacement is
// accepted but not terminally converged (for example, it is running but its
// Docker healthcheck has not reported healthy yet). The pass then stops before
// any stale stop, preserves every usable old generation, and reports
// ServicePassPending. Pending is not a failure and never authorizes readiness or
// image retirement; the coordinator retains the logical request and resumes it
// on a notification, an eligible periodic observation, or a due deadline.
package reconciler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/moby/moby/api/types/container"

	"relay/internal/app"
	"relay/internal/observability/metrics"
	"relay/internal/routing"
	"relay/internal/runtime"
)

// Docker is the service-container subset of the runtime Manager. The Manager
// satisfies it; the structural assertion lives in the worker wiring (worker.go),
// not here.
type Docker interface {
	// ResolveServiceImage resolves one service's configured source to the image
	// a container should run: the app image for an `entrypoint` source, or
	// an inspected/pulled external image for an `image` source. It is resolved
	// BEFORE any container action, so a pull failure preserves healthy
	// containers.
	ResolveServiceImage(
		ctx context.Context,
		fnName string,
		tmpl *app.Template,
		svc app.Service,
		appImage string,
	) (runtime.ServiceImage, error)
	// StartService creates and starts one service replica and returns only
	// after Docker confirms it is RUNNING. A non-nil error (create/start
	// failure, or a started container that inspects as not running) means the
	// replacement is not viable: the reconciler preserves the old generation
	// for that slot. The runtime Manager implements the running gate; a fake
	// that returns nil without confirming running is asserting convergence it
	// has not proven.
	StartService(ctx context.Context, spec runtime.ServiceSpec, replica int) (string, error)
	ServiceContainerList(ctx context.Context) ([]runtime.ServiceContainer, error)
	StopServiceContainers(ctx context.Context, containers []runtime.ServiceContainer) error
	// RemoveAppServiceContainers stops and removes every service container
	// belonging to one app, returning how many were removed.
	RemoveAppServiceContainers(ctx context.Context, fnName string) (int, error)
	// NetworkExists reports whether a Docker network exists on the daemon. The
	// service reconciler uses it to refuse routed services whose routing
	// network is missing; Relay never creates networks.
	NetworkExists(ctx context.Context, network string) (bool, error)
}

type serviceStopResult interface {
	StopServiceContainersWithResult(
		context.Context,
		[]runtime.ServiceContainer,
	) (int, error)
}

func stopServiceContainers(
	ctx context.Context,
	docker Docker,
	containers []runtime.ServiceContainer,
) (int, bool, error) {
	if result, ok := docker.(serviceStopResult); ok {
		count, err := result.StopServiceContainersWithResult(ctx, containers)
		return count, true, err
	}
	err := docker.StopServiceContainers(ctx, containers)
	if err != nil {
		return 0, false, err
	}
	return len(containers), true, nil
}

func requestCancelled(ctx context.Context, err error) bool {
	return ctx != nil &&
		errors.Is(ctx.Err(), context.Canceled) &&
		errors.Is(err, context.Canceled)
}

// ServiceRestarter is the optional in-place restart capability of a Docker
// implementation. The production runtime Manager implements it; the service
// reconciler type-asserts the configured Docker for it, so a Docker that does
// not (a minimal fake, or an older implementation) silently keeps the
// historical replace-on-source-change behavior. It restarts the SAME container
// (same ID, same Config, same bind mounts), which is what lets a HOT_RELOAD
// source-only change retain the running container's identity while the live
// read-only source bind is re-read by the fresh process.
type ServiceRestarter interface {
	RestartServiceContainer(ctx context.Context, containerID string) error
}

// ShutdownDocker is the optional shutdown-scoped Docker capability of a Docker
// implementation. The production runtime Manager implements it; the reconciler
// type-asserts the configured Docker for it in the graceful-shutdown cleanup
// path only, so a minimal fake (or an implementation without the capability)
// keeps using the ordinary methods.
//
// It exists because the worker cancels its lifecycle BEFORE it runs the ordered
// shutdown steps: every ordinary runtime admission is already refused by the
// time the service cleanup runs. These methods are the same list/remove
// operations admitted through the Manager's narrow shutdown-only gate, so the
// work runs on the fresh shutdown step context (never the cancelled lifecycle)
// while still being tracked and joined by Manager.CloseContext.
type ShutdownDocker interface {
	// ShutdownServiceContainerList is the shutdown-only form of
	// ServiceContainerList.
	ShutdownServiceContainerList(ctx context.Context) ([]runtime.ServiceContainer, error)
	// ShutdownStopServiceContainers is the shutdown-only form of
	// StopServiceContainers, applied to the containers the shutdown discovery
	// captured.
	ShutdownStopServiceContainers(ctx context.Context, containers []runtime.ServiceContainer) error
}

// shutdownDockerOps selects the shutdown-scoped list/remove pair for d,
// falling back to the ordinary Docker methods when d does not implement
// ShutdownDocker (a minimal fake). Production always takes the shutdown pair.
func shutdownDockerOps(d Docker) (
	list func(context.Context) ([]runtime.ServiceContainer, error),
	remove func(context.Context, []runtime.ServiceContainer) error,
) {
	if sd, ok := d.(ShutdownDocker); ok {
		return sd.ShutdownServiceContainerList, sd.ShutdownStopServiceContainers
	}
	return d.ServiceContainerList, d.StopServiceContainers
}

// SecretResolver resolves a secret reference to its value. It is a structural
// alias for secrets.Provider so the Worker can pass its shared provider without
// a new concrete type.
type SecretResolver interface {
	Resolve(ctx context.Context, name string) (string, error)
}

// BuildEnv assembles a service replica's environment from the template, the
// runtime plan env (preparedEnv), and the desired internal port.
//
// Order is deliberate and a documented invariant:
//
//	preparedEnv (runtime plan env, e.g. PYTHONDONTWRITEBYTECODE)
//	template env  (Template.EnvList(), name-ordered)
//	resolved secrets (Template.SecretList(), name-ordered)
//	PORT=<port>   (LAST — so template env or a secret can never override it)
//
// A template that declares a secret binding while secrets is nil fails with the
// runner's error style ("no secret provider is configured").
func BuildEnv(
	ctx context.Context,
	tmpl *app.Template,
	port int,
	preparedEnv []string,
	secrets SecretResolver,
) ([]string, error) {
	env := make([]string, 0, 4)
	env = append(env, preparedEnv...)
	for _, ev := range tmpl.EnvList() {
		env = append(env, ev.Name+"="+ev.Value)
	}
	for _, sb := range tmpl.SecretList() {
		if secrets == nil {
			return nil, fmt.Errorf("app references secret %q but no secret provider is configured", sb.Ref)
		}
		val, err := secrets.Resolve(ctx, sb.Ref.String())
		if err != nil {
			// The provider's error carries the reference name only, never a value.
			return nil, fmt.Errorf("resolve secret %q: %w", sb.Ref, err)
		}
		env = append(env, sb.Name+"="+val)
	}
	// PORT is appended last so template env or a secret can never override it.
	env = append(env, fmt.Sprintf("PORT=%d", port))
	return env, nil
}

// reconcileAuthorityKey is the private context key carrying a supersession
// check. It is deliberately an unexported type so no other package can install
// a check under a colliding key.
type reconcileAuthorityKey struct{}

// withReconcileAuthority attaches a supersession check to ctx. current reports
// whether the request that owns ctx is still the authoritative desired state
// for its app. Reconcile consults it at the commit boundary — immediately
// before it would stop a superseded generation — so a request a newer desired
// state has already superseded does NOT stop the old generation it was about to
// replace (the last usable generation), and instead cleans only the provisional
// replacements it started. A nil current leaves ctx unchanged (no authority
// check), which is what a direct Apply caller gets; the coordinator wires an
// authority that consults its own desired-state token.
//
// It is package-private on purpose: the only producer is the coordinator in
// this package, and a direct Apply caller keeps the exact pre-existing
// behavior. The check belongs to the request's own lifecycle; Reconcile reads
// it at each commit boundary and never caches the answer.
func withReconcileAuthority(ctx context.Context, current func() bool) context.Context {
	if current == nil {
		return ctx
	}
	return context.WithValue(ctx, reconcileAuthorityKey{}, current)
}

// reconcileAuthorityFrom returns the supersession check carried by ctx, or nil
// when the caller attached none (a direct Apply).
func reconcileAuthorityFrom(ctx context.Context) func() bool {
	current, _ := ctx.Value(reconcileAuthorityKey{}).(func() bool)
	return current
}

// sourceApplyTracker records, per service container ID, the live source
// fingerprint an in-place restart has already applied. Docker labels are
// immutable, so a restarted container's relay.source label still names the
// fingerprint it was CREATED with; without this record convergence would see a
// permanent mismatch and restart the container on every pass. The record is
// in-memory and worker-lifetime only: after a worker restart the first pass
// restarts each stale container once more (idempotent) and re-records it. It is
// safe for concurrent use — different apps may reconcile concurrently.
type sourceApplyTracker struct {
	mu      sync.Mutex
	applied map[string]appliedSource
}

// appliedSource is one tracker record: the app that owns the container and the
// source fingerprint already applied to it. The app field scopes pruning to the
// reconciling app so a concurrent app's records are never touched.
type appliedSource struct {
	app    string
	source string
}

// newSourceApplyTracker returns an empty tracker.
func newSourceApplyTracker() *sourceApplyTracker {
	return &sourceApplyTracker{applied: map[string]appliedSource{}}
}

// appliedFor returns the fingerprint recorded for a container when the record
// belongs to app; the second result is false when there is no such record. The
// app guard keeps a recycled or foreign container ID from inheriting another
// app's applied source. It is nil-receiver-safe so a reconciler constructed
// without a tracker disables in-place reload without a nil check at every call.
func (t *sourceApplyTracker) appliedFor(id, app string) (string, bool) {
	if t == nil {
		return "", false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	rec, ok := t.applied[id]
	if !ok || rec.app != app {
		return "", false
	}
	return rec.source, true
}

// record stamps the fingerprint an in-place restart applied to a container.
func (t *sourceApplyTracker) record(id, app, source string) {
	if t == nil || id == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.applied == nil {
		t.applied = map[string]appliedSource{}
	}
	t.applied[id] = appliedSource{app: app, source: source}
}

// prune drops the records for app whose container IDs are not in live, so a
// removed or replaced container's record does not leak. Only this app's records
// are considered, so a concurrent app's records are never touched.
func (t *sourceApplyTracker) prune(app string, live map[string]bool) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, rec := range t.applied {
		if rec.app == app && !live[id] {
			delete(t.applied, id)
		}
	}
}

// forgetApp drops every record for app. It is called when an app is removed, so
// the tracker does not retain records for containers that no longer exist.
func (t *sourceApplyTracker) forgetApp(app string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, rec := range t.applied {
		if rec.app == app {
			delete(t.applied, id)
		}
	}
}

// ErrServicePending reports that a service operation was accepted but is not
// yet terminally converged. It is defined by the runtime backend seam so a
// Docker implementation can return it from StartService without importing this
// package; the reconciler re-exports the same sentinel.
var ErrServicePending = runtime.ErrServicePending

// ServicePassOutcome classifies one worker pass. The distinction matters to
// the coordinator: only a terminal outcome concludes a logical request, while
// pending releases the worker and image lease but retains the logical request.
type ServicePassOutcome uint8

const (
	// ServicePassSucceeded means the pass ran to a terminal successful result
	// (including a no-op verification or a superseded pass that issued work).
	ServicePassSucceeded ServicePassOutcome = iota
	// ServicePassPending means at least one desired service has not terminally
	// converged and a later wake should re-evaluate the current desired state.
	// A pending result may also carry an independent error (Err); pending is
	// still the governing outcome because convergence is explicitly incomplete.
	ServicePassPending
	// ServicePassFailed means the pass returned an error without a retained
	// pending request (for example a convergence-state load failure, or a
	// lifecycle cancellation).
	ServicePassFailed
)

// ServicePassResult is a worker-pass result. Outcome classifies the pass; Err is
// non-nil for a failed pass AND may be non-nil alongside a pending outcome
// (an independent failure that must not replace the retained pending
// candidates/deadlines). Changed reports whether the pass performed corrective
// work; Superseded reports that a newer desired state replaced its request.
type ServicePassResult struct {
	Err        error
	Outcome    ServicePassOutcome
	Changed    bool
	Superseded bool
	// StartupDeadline, when set with a pending outcome, is the candidate's exact
	// durable startup deadline. The coordinator schedules one soft wake for it; an
	// observer hint may wake earlier, and reconcile still refuses to duplicate a
	// starting candidate.
	StartupDeadline time.Time
	// RetryNotBefore, when set with a pending outcome, is the hard earliest time a
	// failed candidate/preparation may be retried. The coordinator installs it as
	// the wake deadline and refuses Notify before it, so an observer hint (for
	// example from a starting candidate in another slot) cannot bypass the
	// bounded retry backoff. It is normally backed by Docker-owned marker state;
	// when that write failed it is a PROCESS-LOCAL window only, and the
	// reconciler holds the same floor itself until a durable write succeeds.
	RetryNotBefore time.Time
	// Candidates are the pending healthcheck candidate containers to observe. They
	// are hints only and never authorize work.
	Candidates []runtime.ServiceHealthCandidate
}

// Pending reports whether the pass ended without terminal convergence.
func (r ServicePassResult) Pending() bool { return r.Outcome == ServicePassPending }

// Failed reports whether the pass returned an error.
func (r ServicePassResult) Failed() bool { return r.Outcome == ServicePassFailed }

// servicePass reports what one reconciliation pass actually did. changed
// preserves the historical "any corrective action was issued" signal used by
// callers, the no-op log-level decision, and the reconcile metric outcome;
// started, restarted, and stopped describe the committed container transitions
// for the operator-facing summary. superseded marks a pass that abandoned its
// remaining work because a newer desired state replaced its request: its
// committed counts are still real, but the summary must not present it as a
// plain changed pass. pending marks a pass in which at least one desired replica
// slot is not terminally converged; independent slots/services are still
// attempted, but no pending slot's fallback is stopped.
type servicePass struct {
	changed    bool
	superseded bool
	pending    bool
	started    int
	restarted  int
	// startupDeadline and retryNotBefore are the durable healthcheck wake
	// deadlines derived from Docker-owned candidate metadata. candidates are the
	// containers an observer should watch.
	startupDeadline time.Time
	retryNotBefore  time.Time
	candidates      []runtime.ServiceHealthCandidate
	// stopped counts containers whose stop/removal completed, as reported by
	// the runtime manager when a batch has partial failures.
	stopped      int
	stoppedKnown bool
}

// Reconcile converges the set of fnName's running service containers to
// tmpl.Services (with the freshly prepared image). It is a best-effort pass:
// per-operation failures are logged by the caller-facing style and the first
// error is returned (at least one error surfaces when anything failed), so a
// transient daemon error on one container does not abort convergence of the rest.
//
// serviceNetworks is the worker-global Docker network set (NETWORKS) every
// service container joins, exactly as it applies to execution containers; a
// routed service additionally joins its routing network (TRAEFIK_NETWORK). The
// networks are infrastructure owned OUTSIDE Relay — Relay never creates them —
// and are normalized into a set (see runtime.NetworkSet), so a container is
// joined/replaced based on the SET of networks, not the declaration order.
//
// ctx is the LIFECYCLE context, NOT a pre-bounded reconcile budget. Reconcile
// derives every normal-operation bound itself from ctx and reconcileTimeout, so a
// caller can never accidentally wrap a whole pass in one short deadline.
// Concretely, each service's pre-resolution phase (routing validation and source
// resolution) and its post-resolution phase (environment resolution, stale
// stops, replica starts) run on SEPARATE fresh bounds rooted in ctx. A
// non-positive reconcileTimeout leaves normal operations bounded only by the
// lifecycle context.
//
// The returned bool reports whether the pass performed any convergence action
// (stopped at least one removed-service or stale container, or attempted to
// start at least one replica). A fully-converged desired state — everything
// already running with the correct image, port, and replica count — returns
// (false, nil). Callers can use this to distinguish a no-op verification pass
// from a pass that actually changed state (e.g. for log-level selection).
//
// Replacement ordering: for each desired replica slot the reconciler attempts
// the replacement BEFORE stopping the slot's old generation, and stops that
// generation only once StartService has confirmed the replacement is running.
// Old generations of a slot whose replacement failed are preserved (the slot
// keeps serving) and retried on a later pass. Excess replicas (scale-down) and
// removed services are stopped only after every desired slot has converged, so
// convergence never removes every old replica before replacements exist.
//
// Identity edge — a NAME change is a removal plus an addition. A service's
// identity is its stable template name (relay.service), the sole grouping key;
// the source descriptor is compared as the desired implementation, not as
// identity. Renaming a service makes the old name absent from the desired set
// and the new name appear, so the old containers are treated as removed. The
// reconciler deliberately stops removed services LAST (after desired starts), so
// the old name's containers are preserved until the new name's service has fully
// converged; a FAILED new service preserves the old name's running containers
// (see the removal cleanup below). Changing only the source under the same name
// is NOT a rename: under HOT_RELOAD a source-only change is applied by an
// in-place restart of the same replica slots (retaining the container IDs, when
// the Docker implementation supports it), while any image/config/dependency
// change still replaces the same replica slots via start-before-stop.
//
// Container ownership is always label-derived. A container belongs to fnName
// when its App == fnName; containers of other apps are never touched.
// This exported entry point is deliberately NOT node-scoped, so a direct
// caller keeps the historical same-host behavior. The worker-wired
// ServiceReconciler adds its stable NODE_ID as a second predicate (see
// WithNodeID): containers stamped with another worker's relay.node_id are
// untouched, while a restart of the SAME NODE_ID still adopts and reconciles
// the previous process's service containers.
func Reconcile(
	ctx context.Context,
	reconcileTimeout time.Duration,
	docker Docker,
	fnName string,
	tmpl *app.Template,
	appImage string,
	preparedEnv []string,
	serviceNetworks []string,
	secrets SecretResolver,
	traefik routing.TraefikConfig,
	log *slog.Logger,
) (bool, error) {
	pass, err := reconcileWithObserver(
		ctx, reconcileTimeout, docker, fnName, tmpl, appImage, preparedEnv,
		serviceNetworks, secrets, traefik, log, "", nil, nil, nil,
	)
	if pass.pending && !requestCancelled(ctx, err) {
		// Pending is reported as ErrServicePending for the direct API. An
		// independent failure is preserved alongside it, so a caller never
		// mistakes a pending-with-error pass for a clean pending pass.
		err = errors.Join(ErrServicePending, err)
	}
	return pass.changed, err
}

func reconcileWithObserver(
	ctx context.Context,
	reconcileTimeout time.Duration,
	docker Docker,
	fnName string,
	tmpl *app.Template,
	appImage string,
	preparedEnv []string,
	serviceNetworks []string,
	secrets SecretResolver,
	traefik routing.TraefikConfig,
	log *slog.Logger,
	nodeID string,
	reconcileStarted func(),
	apply *sourceApplyTracker,
	stateTracker *ServiceReconciler,
) (servicePass, error) {
	// A fresh normal-operation bound for the container listing. reconcileTimeout
	// is always positive from the worker; a non-positive value means "no
	// normal-operation bound", so the lifecycle context bounds it directly rather
	// than deriving an already-expired child.
	listCtx, listCancel := ctx, func() {}
	if reconcileTimeout > 0 {
		listCtx, listCancel = context.WithTimeout(ctx, reconcileTimeout)
	}
	containers, err := docker.ServiceContainerList(listCtx)
	listCancel()
	if err != nil {
		// Without a listing we cannot know the running set; surface the error
		// rather than guessing whether to start/stop anything.
		return servicePass{}, fmt.Errorf("service: list containers: %w", err)
	}

	// owned reports whether a discovered container belongs to this pass: the
	// same app, and — when the reconciler is worker-wired — this worker's own
	// stable identity. A container another worker created (a different
	// relay.node_id on the shared daemon) is never converged, adopted, or
	// stopped here; the unscoped direct API (nodeID == "") keeps the
	// historical app-only predicate. Because NODE_ID is stable across
	// restarts, a replacement process still matches the containers its previous
	// incarnation created and adopts a converged one in place.
	owned := func(ctr runtime.ServiceContainer) bool {
		if ctr.App != fnName {
			return false
		}
		return nodeID == "" || ctr.NodeID == nodeID
	}

	// restarter is the optional in-place restart capability. When the configured
	// Docker implements it AND a source-apply tracker is present (the worker's
	// ServiceReconciler always supplies one), a source-only change restarts the
	// existing container instead of replacing it. A Docker without the capability
	// — or a direct Reconcile call with no tracker — keeps the historical
	// replace-on-source-change behavior.
	restarter, _ := docker.(ServiceRestarter)
	if apply == nil {
		restarter = nil
	}
	// Drop tracker records for containers of this app no longer present, so a
	// removed or replaced container's record does not accumulate.
	if apply != nil {
		live := make(map[string]bool, len(containers))
		for _, ctr := range containers {
			if owned(ctr) {
				live[ctr.ID] = true
			}
		}
		apply.prune(fnName, live)
	}

	// changed reports whether any convergence action (a stop or a start
	// attempt) was issued during this pass. It starts false and is set by the
	// removed-service cleanup below and by the classify/start work further down.
	// pass counts only COMMITTED container transitions for the summary line;
	// an issued-but-failed action leaves its count unchanged.
	changed := false
	pass := servicePass{stoppedKnown: true}

	// reconcileStarted publishes the reconciling status, but ONLY when this
	// pass actually has corrective container work to do: a fully-converged
	// verification pass (nothing to stop or start) must never claim
	// convergence work that is not happening. corrective is set by every branch
	// that is about to stop or start a container; notifyReconcile then fires
	// reconcileStarted at most once per pass, immediately before the first such
	// action. A pass with no corrective work never fires it.
	corrective := false
	reconcileNotified := false
	notifyReconcile := func() {
		if reconcileNotified || !corrective {
			return
		}
		reconcileNotified = true
		// The first real corrective operation is this pass's start transition.
		// A fully-converged verification pass has no corrective work, so it
		// never logs a start line.
		log.Info("Service: reconciling",
			"app", fnName,
			"services", len(tmpl.Services),
			"outcome", "started",
		)
		if reconcileStarted != nil {
			reconcileStarted()
		}
	}

	var firstErr error
	fail := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// Retry policy, in one place:
	//
	//   - A HEALTHCHECK CANDIDATE failure (Docker accepted a replacement but did
	//     not report it ready, or an existing candidate is observed unhealthy,
	//     exited, or past its startup grace) uses the durable Docker-owned
	//     exponential retry scheme: the attempt is an immutable container label
	//     plus a marker field, only the delay is capped, and the reconciler
	//     refuses to start a replacement before the persisted window elapses (see
	//     handlePendingStart and the failed-slot branch).
	//
	//   - A PREPARATION failure for a service with an EFFECTIVE Relay probe — an
	//     unavailable routing network, a source/image that cannot be resolved, an
	//     environment/secret that cannot be resolved, or a create/start error —
	//     uses the SAME bounded, durable exponential scheme, keyed by the
	//     preparation generation and persisted in the same Docker-owned marker
	//     (recordPreparationAttempt / recordPreparationFailure / retryStartFailure).
	//     The admitted attempt is written BEFORE any network/source/environment
	//     work; if that write fails, no preparation work is attempted. If the
	//     failure write is unavailable, the admitted attempt is still durable and a
	//     restart converts it into the SAME attempt's bounded failure before
	//     retrying — never an immediate retry or a reset attempt. It therefore does
	//     not depend on the app's periodic reconcile cadence, survives a restart,
	//     has no attempt cutoff, keeps the existing replicas serving, and resets
	//     when preparation succeeds or the desired generation changes. The gate is
	//     the EFFECTIVE probe, not merely a declared one: the app's prepared
	//     HOT_RELOAD source-mount record is consulted before resolution (it is a
	//     cheap in-memory predicate, the same one ResolveServiceImage uses), so a
	//     source-mounted service ignores Relay healthchecks entirely — it keeps
	//     the no-probe historical behavior below, and a healthcheck-only template
	//     edit cannot reset its preparation window.
	//
	//   - A service WITHOUT a declared healthcheck keeps the historical
	//     behavior: its preparation failures are per-pass operational failures
	//     retried by the ordinary app reconciliation cadence. There is no
	//     candidate readiness contract or Docker-owned attempt state to persist
	//     for it, so the bounded scheme is deliberately not applied.
	//
	//   - Routing CONFIG validation (missing/invalid worker Traefik config, an
	//     overlong effective host) is deterministic for the running process: the
	//     same inputs always fail until the template or worker config changes, so
	//     it stays a terminal per-pass failure rather than entering a retry loop
	//     that cannot succeed.
	//
	// A marker-persistence failure is never treated as DURABLE authority: it is
	// reported as an independent error while the pass keeps its pending
	// candidate contract, and the SAME hard eligibility/attempt is retained in a
	// process-local floor (and handed to the coordinator) so another slot's
	// notification cannot bypass the backoff or drive a retry loop. No start is
	// admitted on an unpersisted attempt. If an ATTEMPT ADMISSION (preparation or
	// pre-start candidate) survives without a recorded outcome — the process
	// stopped mid-operation, or the failure write was unavailable — a restart
	// converts it into the SAME attempt's durable window anchored at RECOVERY
	// time. The failure instant was never persisted and the operation may have run
	// longer than the backoff, so an admission-time anchor could already be
	// elapsed; recovery anchoring can only delay, never retry sooner than a full
	// backoff after the unrecorded outcome, and it never resets the attempt. A
	// preparation admission whose exact attempt is matched by a durable completion
	// record is NOT such an unknown outcome: its preparation already succeeded, so
	// it is dropped without installing a failure window.

	// bounded derives a fresh normal-operation bound rooted in the lifecycle
	// context, matching the per-phase bounds Reconcile already uses. Supersession
	// cleanup (removing this pass's provisional replacements) uses it so it never
	// inherits an exhausted phase budget.
	bounded := func() (context.Context, context.CancelFunc) {
		if reconcileTimeout > 0 {
			return context.WithTimeout(ctx, reconcileTimeout)
		}
		return context.WithCancel(ctx)
	}

	// Supersession: a request a newer desired state has replaced must not commit
	// (stop) the old generation it was about to supersede, or a newer request
	// could find its last usable generation already gone. authority is attached
	// only by the ServiceCoordinator; a direct Apply has none and its behavior is
	// unchanged. superseded is evaluated at EACH commit boundary because a newer
	// enqueue can arrive at any point during a long pass.
	authority := reconcileAuthorityFrom(ctx)
	superseded := func() bool { return authority != nil && !authority() }

	// desiredFailed records that at least one desired service did not converge
	// this pass (a routing/source/env resolution failure, or a replacement that
	// could not be confirmed running). A removed name's running containers may be
	// the ONLY usable generation for the service the new name replaces (a rename
	// is indistinguishable from a removal plus an addition); when the desired set
	// did not converge they are preserved rather than stopped (see the removal
	// cleanup below).
	desiredFailed := false

	// abortPass is set when the request is discovered to be superseded: no
	// further service is converged and no removed-service cleanup runs.
	abortPass := false

	// provisional collects the replacement containers THIS pass started that a
	// later slot failed to commit, or that a supersession abandoned before
	// commit. They are this pass's own creations (their IDs are known only here),
	// so removing them cannot touch another generation — in particular it never
	// stops a running old generation.
	var provisional []runtime.ServiceContainer

	desired := make(map[string]app.Service, len(tmpl.Services))
	for _, svc := range tmpl.Services {
		desired[svc.Name] = svc
	}

	// Classify this app's containers by service NAME. Containers whose
	// service name is no longer in the template (removed service, or an old name
	// after a rename) are collected and stopped only AFTER every desired service
	// has converged below: start-before-stop ordering means a service whose
	// source changed under the same name has already swapped in place, and a
	// renamed service's old containers are not torn down before the new name's
	// replicas are running.
	var removed []runtime.ServiceContainer
	byService := make(map[string][]runtime.ServiceContainer)
	for _, ctr := range containers {
		if !owned(ctr) {
			// Another app, or another worker's service container, owns this
			// container; this pass never touches it.
			continue
		}
		if _, ok := desired[ctr.Name]; !ok {
			removed = append(removed, ctr)
			continue
		}
		byService[ctr.Name] = append(byService[ctr.Name], ctr)
	}

	// Stale/excess stops are deferred until every desired service and replica
	// slot has been attempted. A pending candidate cannot block independent
	// convergence, and a slot that is still pending never contributes its old
	// generation to this list.
	var deferredStale []runtime.ServiceContainer
	type commitIntent struct {
		name       string
		replica    int
		generation string
		id         string
		attempt    int
	}
	var commitIntents []commitIntent
	for _, svc := range tmpl.Services {
		name := svc.Name
		sourceRef := svc.SourceRef()
		existing := byService[name]

		// Supersession check at the top of each service: if a newer desired
		// state replaced this request while a previous service was converging,
		// do no further container work for the remaining services. Every
		// earlier service already committed atomically (its old generations
		// stopped only after its replacements were confirmed), so abandoning
		// here cannot leave a service with no generation.
		if abortPass || superseded() {
			abortPass = true
			pass.superseded = true
			break
		}

		// Preparation retry (services with an EFFECTIVE Relay probe only): a
		// failure that happens BEFORE any candidate exists — an unavailable routing
		// network, a source/image that cannot be resolved, an environment/secret
		// that cannot be resolved, or a start that fails — is retried with the same
		// bounded, durable exponential window as a failed candidate. It is
		// persisted in the Docker-owned marker keyed by the PREPARATION generation
		// (the desired inputs available before resolution), so a restart
		// reconstructs the same eligibility, a new desired generation resets it,
		// and recovery does not depend on the app's periodic reconcile cadence.
		//
		// The gate is the EFFECTIVE probe. Relay's own probe is effective only when
		// the app is not source-mounted (HOT_RELOAD); sourceMounted is an in-memory
		// Prepare record — no inspect, pull, or fingerprint — and mirrors what
		// ResolveServiceImage reports below. Under HOT_RELOAD a declared
		// healthcheck is bypassed entirely: it must not drive the preparation retry
		// policy or the preparation identity, so a healthcheck-only template edit
		// cannot reset a retry window. A service whose probe is not applied (none
		// declared, or HOT_RELOAD) keeps the historical terminal per-pass behavior:
		// it has no candidate readiness contract or Docker-owned attempt state to
		// persist.
		sourceMounted := serviceSourceMounted(docker, fnName)
		preparationGeneration := servicePreparationIdentity(
			fnName, tmpl, svc, appImage, serviceNetworks, traefik, sourceMounted)
		preparationAttempt := 0
		preparationRetry := svc.Healthcheck != nil && !sourceMounted && stateTracker != nil
		// preparationAdmittedAt is the start instant of the admission this pass
		// writes. It identifies that admission's successful completion, so it is
		// zero when no admission was written (no effective probe, or a failed
		// admission write).
		preparationAdmittedAt := time.Time{}
		preparationFloorKey := preparationRetryFloorKey(fnName, name, preparationGeneration)
		if preparationRetry {
			// Resolve the attempt/window that must gate this process and any
			// restart, in order of authority:
			//   1. the process-local floor (covers a failed marker write that has
			//      not been superseded yet);
			//   2. a recorded failure window;
			//   3. an ADMITTED attempt whose outcome was never recorded (the
			//      process died during preparation, or the failure write failed).
			//      It is converted into the SAME attempt's durable bounded failure
			//      BEFORE any new preparation work, anchored at recovery time (the
			//      failure instant is unknown), so a restart can never retry
			//      immediately, retry early, or reset the attempt. An admission
			//      whose exact attempt has a durable completion record already
			//      succeeded and is dropped instead of converted.
			floor := stateTracker.retryFloorFor(preparationFloorKey)
			if time.Now().Before(floor.until) {
				pass.pending = true
				pass.retryNotBefore = earlierDeadline(pass.retryNotBefore, floor.until)
				continue
			}
			preparationAttempt = floor.attempt
			if recorded, ok := stateTracker.preparationState(
				fnName, name, 0, preparationGeneration,
			); ok {
				if time.Now().Before(recorded.RetryNotBefore) {
					pass.pending = true
					pass.retryNotBefore = earlierDeadline(
						pass.retryNotBefore, recorded.RetryNotBefore)
					continue
				}
				if recorded.CandidateAttempt > preparationAttempt {
					preparationAttempt = recorded.CandidateAttempt
				}
			} else if unfinished, ok := stateTracker.preparationAdmission(
				fnName, name, 0, preparationGeneration,
			); ok {
				if stateTracker.preparationCompleted(
					fnName, name, 0, preparationGeneration,
					unfinished.CandidateAttempt, unfinished.CandidateStartedAt,
				) {
					// This admission's preparation COMPLETED successfully; the
					// marker is a leftover from a process that stopped before the
					// retry-family cleanup, or from a cleanup write that failed. It
					// records no failure, so converting it would delay an
					// already-prepared (and possibly already-serving) generation by
					// a full backoff. Drop it (best effort) and admit a fresh
					// attempt below.
					if err := stateTracker.clearPreparationState(
						ctx, fnName, name, 0, preparationGeneration,
					); err != nil {
						log.Debug("Service: clear completed preparation state failed",
							"app", fnName, "service", name, "error", err)
					}
					preparationAttempt = 0
				} else {
					attempt := unfinished.CandidateAttempt
					if preparationAttempt > attempt {
						attempt = preparationAttempt
					}
					if attempt < 1 {
						attempt = 1
					}
					// The FAILURE instant was never persisted — that is exactly why
					// this record is unfinished (the process died during preparation,
					// or the failure write was unavailable). The preparation operation
					// may also have run longer than the backoff (a slow pull), so
					// anchoring at CandidateStartedAt could already be in the past and
					// retry immediately after the unrecorded outcome. Convert it into
					// the SAME attempt's window anchored at RECOVERY time instead: this
					// can only delay, never retry sooner than a full backoff after the
					// outcome, and it preserves the attempt number.
					eligibleAt := time.Now().Add(servicePreparationBackoff(
						svc.Healthcheck.Interval, attempt))
					stateTracker.noteRetryFloor(preparationFloorKey, eligibleAt, attempt)
					if err := stateTracker.recordPreparationFailure(
						ctx, fnName, name, 0, preparationGeneration, attempt, eligibleAt,
					); err != nil {
						fail(fmt.Errorf("service %q: %w", name, err))
						desiredFailed = true
						pass.pending = true
						pass.retryNotBefore = earlierDeadline(pass.retryNotBefore, eligibleAt)
						continue
					}
					if time.Now().Before(eligibleAt) {
						pass.pending = true
						pass.retryNotBefore = earlierDeadline(pass.retryNotBefore, eligibleAt)
						continue
					}
					preparationAttempt = attempt
				}
			}
		}
		// admitPreparation persists the admitted preparation attempt DURABLY
		// before any network/source/environment work. If that write fails, no
		// preparation work is attempted: a restart then finds the admitted
		// attempt and converts it into the same bounded failure instead of
		// retrying immediately with a reset attempt. It is called only after the
		// deterministic routing-config validation, so a terminal config failure
		// never leaves an admission to be converted.
		admitPreparation := func() bool {
			if !preparationRetry {
				return true
			}
			admitted := preparationAttempt + 1
			admittedAt := time.Now()
			if err := stateTracker.recordPreparationAttempt(
				ctx, fnName, name, 0, preparationGeneration, admitted, admittedAt,
			); err != nil {
				fail(fmt.Errorf("service %q: %w", name, err))
				desiredFailed = true
				pass.pending = true
				floorAt := time.Now().Add(servicePreparationBackoff(
					svc.Healthcheck.Interval, admitted))
				stateTracker.noteRetryFloor(preparationFloorKey, floorAt, admitted)
				pass.retryNotBefore = earlierDeadline(pass.retryNotBefore, floorAt)
				return false
			}
			preparationAttempt = admitted
			preparationAdmittedAt = admittedAt
			return true
		}
		// retryPreparation installs the durable window for the ADMITTED attempt's
		// failure. The window is also recorded in the process-local floor BEFORE
		// the marker write, so a failed write still holds the same hard
		// eligibility and attempt in this process; the admitted marker (written
		// above) keeps the attempt durable for a restart, which converts it into
		// the same bounded failure. A successful preparation prunes the family.
		retryPreparation := func() bool {
			if !preparationRetry {
				return false
			}
			attempt := preparationAttempt
			if attempt < 1 {
				attempt = 1
			}
			eligibleAt := time.Now().Add(servicePreparationBackoff(
				svc.Healthcheck.Interval, attempt))
			stateTracker.noteRetryFloor(preparationFloorKey, eligibleAt, attempt)
			if err := stateTracker.recordPreparationFailure(
				ctx, fnName, name, 0, preparationGeneration, attempt, eligibleAt,
			); err != nil {
				fail(fmt.Errorf("service %q: %w", name, err))
				desiredFailed = true
				pass.pending = true
				// The failure window is not durable, but the ADMITTED attempt is;
				// a restart converts it into the same bounded window. Keep the hard
				// eligibility in this process so notifications cannot retry early.
				pass.retryNotBefore = earlierDeadline(pass.retryNotBefore, eligibleAt)
				return true
			}
			pass.pending = true
			pass.retryNotBefore = earlierDeadline(pass.retryNotBefore, eligibleAt)
			return true
		}

		// The pre-resolution phase runs on its own fresh normal-operation bound
		// rooted in the lifecycle context (never the caller's ctx), so a caller
		// cannot wrap a whole pass in one short deadline and one operation cannot
		// consume another's budget. Routing validation and source resolution
		// belong here.
		preCtx, preCancel := ctx, func() {}
		if reconcileTimeout > 0 {
			preCtx, preCancel = context.WithTimeout(ctx, reconcileTimeout)
		}

		// Routing validation runs FIRST, before any classification or stops:
		// a routed service (one declaring a host) whose routing config is
		// unusable must be reported and skipped entirely — no stops, no
		// starts — replacing it half-way would break the existing container
		// for nothing. The deterministic config checks run before the preparation
		// admission so a terminal config failure cannot leave an admitted attempt
		// behind.
		var routeLabels map[string]string
		routeNetwork := ""
		if svc.Host != "" {
			if err := traefik.Validate(); err != nil {
				preCancel()
				err := fmt.Errorf("service %q: %w", name, err)
				fail(err)
				desiredFailed = true
				log.Warn("Service: routing validation failed", "service", name, "error", err)
				continue
			}
			// The per-service effective host (declared host, or the override
			// mapping) is validated next: the combined length is only knowable
			// with the template host in hand, and an overlong host would
			// silently never match. This runs before network/container work,
			// preserving the routing-first ordering; with no override it is a
			// no-op on an already-validated template host.
			if err := traefik.ValidateHost(svc.Host); err != nil {
				preCancel()
				err := fmt.Errorf("service %q: %w", name, err)
				fail(err)
				desiredFailed = true
				log.Warn("Service: routing validation failed", "service", name, "error", err)
				continue
			}
		}
		if !admitPreparation() {
			preCancel()
			continue
		}
		if svc.Host != "" {
			// The routing network (e.g. the Traefik network) is infrastructure
			// owned outside Relay; Relay verifies it exists and refuses to
			// start routed containers otherwise — it never creates it.
			ok, err := docker.NetworkExists(preCtx, traefik.Network)
			if err != nil {
				preCancel()
				err := fmt.Errorf("service %q: check routing network: %w", name, err)
				fail(err)
				if !requestCancelled(ctx, err) {
					log.Warn("Service: routing validation failed", "service", name, "error", err)
				}
				if retryPreparation() {
					continue
				}
				desiredFailed = true
				continue
			}
			if !ok {
				preCancel()
				err := fmt.Errorf("service %q: %w", name, routing.MissingNetwork(traefik.Network))
				fail(err)
				log.Warn("Service: routing validation failed", "service", name, "error", err)
				if retryPreparation() {
					continue
				}
				desiredFailed = true
				continue
			}
			routeLabels = routing.TraefikLabels(fnName, name, svc.Host, svc.Path, svc.Port, traefik)
			routeNetwork = traefik.Network
			// Optional routing values log only when set, omitting empty
			// ones; the generated labels themselves are never logged.
			var attrs []any
			if svc.Path != "" {
				attrs = append(attrs, "path", svc.Path)
			}
			if traefik.EntryPoints != "" {
				attrs = append(attrs, "entrypoints", traefik.EntryPoints)
			}
			if traefik.CertResolver != "" {
				attrs = append(attrs, "certresolver", traefik.CertResolver)
			}
			if traefik.Priority != nil {
				attrs = append(attrs, "priority", *traefik.Priority)
			}
			if traefik.HostOverride != "" {
				attrs = append(attrs, "host_override", traefik.HostOverride)
			}
			log.Debug("Service: routing configured",
				append([]any{
					"app", fnName,
					"service", name,
					"host", svc.Host,
					"network", routeNetwork,
				}, attrs...)...)
		}

		// Resolve the source's image BEFORE any container action. A source that
		// cannot be resolved — a failed pull, a missing image, or an
		// unlaunchable entrypoint — is reported and this service is skipped
		// entirely: its existing (healthy) containers are preserved rather than
		// replaced on a failed resolution. This is the ordering guarantee that a
		// transient registry outage never tears down a working service.
		resolved, err := docker.ResolveServiceImage(preCtx, fnName, tmpl, svc, appImage)
		preCancel()
		if err != nil {
			fail(fmt.Errorf("service %q: %w", name, err))
			if !requestCancelled(ctx, err) {
				log.Warn("Service: cannot resolve source; keeping existing containers",
					"service", name, "error", err)
			}
			if retryPreparation() {
				continue
			}
			desiredFailed = true
			continue
		}

		// The post-resolution phase runs on a FRESH normal-operation bound rooted
		// in the lifecycle context, separate from every bound above, so the
		// environment resolution, stale stops, and replica starts that follow
		// each get their own budget. A non-positive reconcileTimeout leaves
		// these operations bounded only by the lifecycle context.
		postCtx, postCancel := ctx, func() {}
		if reconcileTimeout > 0 {
			postCtx, postCancel = context.WithTimeout(ctx, reconcileTimeout)
		}
		env, err := BuildEnv(postCtx, tmpl, svc.Port, preparedEnv, secrets)
		if err != nil {
			postCancel()
			fail(fmt.Errorf("service %q: %w", name, err))
			if !requestCancelled(ctx, err) {
				log.Warn("Service: cannot start replicas", "service", name, "error", err)
			}
			if retryPreparation() {
				continue
			}
			desiredFailed = true
			continue
		}
		// Preparation succeeded, so the admitted attempt's OUTCOME is now known.
		// Record that durably BEFORE pruning the retry family: a restart (or a
		// failed cleanup write) can then never mistake this admission for an
		// interrupted, unknown preparation and install a failure backoff for a
		// generation that already prepared successfully. The completion record is
		// keyed by the admission's generation, attempt, and start instant, so a
		// LATER admission of the same generation never inherits this outcome.
		// Then prune the retry family (best-effort) so a later failure starts a
		// fresh attempt sequence and a stale generation (including after a
		// HOT_RELOAD/probe change) leaves no retry marker behind. This is a no-op
		// when no preparation marker is cached.
		if stateTracker != nil {
			completed := preparationAdmittedAt.IsZero()
			if !preparationAdmittedAt.IsZero() {
				if err := stateTracker.recordPreparationCompleted(
					ctx, fnName, name, 0, preparationGeneration,
					preparationAttempt, preparationAdmittedAt,
				); err != nil {
					log.Debug("Service: record preparation completion failed",
						"app", fnName, "service", name, "error", err)
				} else {
					completed = true
				}
			}
			if err := stateTracker.clearPreparationState(
				ctx, fnName, name, 0, preparationGeneration,
			); err != nil {
				log.Debug("Service: clear preparation retry state failed",
					"app", fnName, "service", name, "error", err)
				if !completed {
					// Neither the completion record nor the cleanup is durable, but
					// THIS process knows the preparation succeeded. Drop the cached
					// admission so the next pass does not convert its own successful
					// outcome into a failure backoff; a restart before the next
					// admission write stays conservative (no durable completion).
					stateTracker.forgetPreparationState(fnName, name)
				}
			}
		}
		preparationAttempt = 0
		// environmentFingerprint is the desired effective environment identity. It is
		// compared against each container's relay.environment label below: the
		// environment is the one piece of configuration the image reference
		// cannot carry (a template env change on an `image` source leaves the
		// reference unchanged; a rotated secret value never changes the source
		// fingerprint), so without this comparison a stale container would keep
		// serving its old env/secrets indefinitely. It covers the exact slice
		// StartService applies, preserving its order.
		environmentFingerprint := runtime.EnvironmentFingerprint(env)
		// resources are the app's effective per-container limits; their
		// fingerprint is compared against each container's relay.resources label
		// below, so a resource-only template change replaces the running service
		// even though the image reference (and image fingerprint) are unchanged.
		resources := tmpl.ResourceLimits()
		resourceHash := resources.Fingerprint()
		// desiredTmpfs is the canonical identity of the service's configured
		// additional tmpfs destinations. It is compared against each container's
		// relay.tmpfs label below, so a tmpfs-configuration change replaces the
		// running service even though neither the image nor any other label
		// changes. The label is the only observable identity here: ContainerList
		// summaries omit HostConfig.Tmpfs and inspected Mounts omit tmpfs entries
		// on some Engines, so comparing the live mount table would add an inspect
		// per discovered container each pass. The always-present /tmp mount is
		// constant and not encoded.
		desiredTmpfs := runtime.ServiceTmpfsLabel(svc.Tmpfs)
		// A HOT_RELOAD source mount bypasses Relay healthcheck behavior entirely.
		// No Docker probe, no healthcheck/attempt labels, and no readiness gate are
		// applied, so a healthcheck-only template edit cannot force a replacement
		// and source-only restarts keep their in-place semantics.
		effectiveHealthcheck := svc.Healthcheck
		if resolved.Mount != nil {
			effectiveHealthcheck = nil
		}
		// desiredHealthcheck is the canonical identity of the service's desired
		// healthcheck configuration. "" (no healthcheck, including HOT_RELOAD)
		// matches an unlabeled container, preserving the historical behavior.
		desiredHealthcheck := runtime.ServiceHealthcheckLabel(effectiveHealthcheck)
		// desiredNetworks / desiredNetworkSet is the canonical network set the
		// container must carry and join: the worker-global NETWORKS set every
		// service container joins, plus the routing network for a routed
		// service (none otherwise). Normalizing to a sorted, de-duplicated set
		// makes the comparison order- and duplicate-invariant, so a
		// relay.networks label discovered on a container matches whenever the
		// SET is the same — a reordered or repeated declaration never replaces a
		// container. A routing-network change (or a global change) makes the
		// running container stale and replaces it.
		desiredNetworkSet := runtime.NetworkSet(append(append([]string(nil), serviceNetworks...), routeNetwork))
		desiredNetworks := runtime.NetworksLabel(desiredNetworkSet...)

		// A container is CONVERGED (a keep candidate) only when it is both
		// healthy (running) and currently configured correctly (source reference,
		// image, image content, port, effective environment, resources, additional
		// tmpfs destinations, source mount, and Docker networks all match the
		// desired values) and carries a real replica label. Anything else —
		// exited/dead/removing, a changed source, a changed image, a moved
		// external tag (image content changed), a changed port, a changed
		// env/secret (environment fingerprint mismatch), changed resource
		// configuration, tmpfs list, network set, mounted source
		// (relay.source), or an unlabeled legacy container (Replica == -1) — is a
		// non-converged generation that a converged replacement supersedes. In
		// addition, the container's labels must match the desired routing label
		// set exactly: a changed host/path/port leaves
		// stale Traefik labels pointing traffic at whatever the old container
		// served, so the container is replaced. SourceRef comparison matters for
		// an `image` source whose two references resolve to the same bytes: the
		// container still carries the old relay.identity and must be replaced so
		// its label reflects the configured source.
		//
		// desiredSourceID is the live source fingerprint for a HOT_RELOAD
		// (source-mounted) entrypoint service ("" otherwise). It never changes the
		// image reference, so without this comparison a source-only edit would
		// leave the running container serving stale code.
		desiredSourceID := ""
		if resolved.Mount != nil {
			desiredSourceID = resolved.Mount.Identity
		}
		// desiredGeneration is the committed-marker generation identity. It is the
		// exact desired implementation (including the effective healthcheck, which
		// is empty under HOT_RELOAD). A marker only designates this container as
		// active while this identity still matches.
		desiredGeneration := serviceGenerationIdentity(
			sourceRef, resolved.Ref, resolved.ID, svc.Port,
			environmentFingerprint, resourceHash, desiredTmpfs, desiredNetworks,
			desiredHealthcheck, desiredSourceID, resolved.Entry, routeLabels,
		)
		// configured reports every immutable configuration comparison except
		// the live source fingerprint and runtime readiness, which the
		// caller-specific checks below handle separately.
		configured := func(ctr runtime.ServiceContainer) bool {
			// Under HOT_RELOAD healthchecks are ignored entirely: an old
			// relay.healthcheck label must not make the container stale and turn an
			// in-place source restart into a replacement.
			healthcheckMatches := true
			if resolved.Mount == nil {
				healthcheckMatches = ctr.Healthcheck == desiredHealthcheck
			}
			return ctr.SourceRef == sourceRef &&
				ctr.Image == resolved.Ref &&
				ctr.ImageID == resolved.ID &&
				ctr.Port == svc.Port &&
				ctr.EnvironmentFingerprint == environmentFingerprint &&
				ctr.Resources == resourceHash &&
				ctr.Tmpfs == desiredTmpfs &&
				healthcheckMatches &&
				ctr.Networks == desiredNetworks &&
				ctr.Replica >= 0 &&
				routingLabelsMatch(routeLabels, ctr.Labels)
		}
		// sourceOK accepts a container whose immutable relay.source label
		// equals the desired fingerprint, OR one already reloaded in place onto
		// it (the applied-source tracker records the restarted container's ID,
		// since its label can never be updated).
		sourceOK := func(ctr runtime.ServiceContainer) bool {
			if ctr.SourceID == desiredSourceID {
				return true
			}
			if desiredSourceID == "" {
				return false
			}
			applied, ok := apply.appliedFor(ctr.ID, fnName)
			return ok && applied == desiredSourceID
		}
		// serviceReady is the pass-local readiness view. Under HOT_RELOAD, Relay's
		// own healthcheck is ignored entirely — including a Ready=false observation
		// left on an older container that still carries relay.healthcheck — so a
		// previously unhealthy/starting probe cannot gate an in-place source restart
		// as generic pending. Backend readiness on containers WITHOUT Relay's
		// healthcheck label is still honored.
		serviceReady := func(ctr runtime.ServiceContainer) bool {
			if resolved.Mount != nil && ctr.Healthcheck != "" {
				return true
			}
			return serviceContainerReady(ctr)
		}
		pickServiceFallback := func(containers []runtime.ServiceContainer) *runtime.ServiceContainer {
			return pickFallbackWith(containers, serviceReady)
		}
		// sourceOnlyStale reports whether ctr is running and correctly configured
		// EXCEPT for its live source fingerprint: the exact case an in-place
		// restart resolves under HOT_RELOAD. It requires a real source mount
		// (desiredSourceID != "") and an immutable relay.source label that differs;
		// every other field — image, image content, port, env, resources, tmpfs,
		// networks, replica slot, routing labels — must already match, so a
		// configuration or dependency change still replaces rather than restarts.
		// Under HOT_RELOAD effectiveHealthcheck is nil, so health never gates the
		// in-place source restart.
		sourceOnlyStale := func(ctr runtime.ServiceContainer) bool {
			return desiredSourceID != "" &&
				ctr.SourceID != desiredSourceID &&
				ctr.State == container.StateRunning &&
				serviceReady(ctr) &&
				configured(ctr)
		}

		// The replacement spec is built per start so the durable retry attempt
		// stamped on a healthcheck candidate is the current generation.
		newSpec := func(attempt int) runtime.ServiceSpec {
			return runtime.ServiceSpec{
				App:                fnName,
				Name:               name,
				SourceRef:          sourceRef,
				Port:               svc.Port,
				Image:              resolved.Ref,
				ImageID:            resolved.ID,
				Entry:              resolved.Entry,
				Env:                env,
				Resources:          resources,
				Tmpfs:              svc.Tmpfs,
				Labels:             routeLabels,
				Networks:           desiredNetworkSet,
				SourceMount:        resolved.Mount,
				Runtime:            tmpl.Runtime,
				Healthcheck:        effectiveHealthcheck,
				HealthcheckAttempt: attempt,
			}
		}

		// Group this service's existing containers by their logical replica
		// slot. A container with no valid replica label (Replica < 0, e.g. a
		// pre-label legacy container) is never a slot occupant: it is always
		// replaced and is cleaned up after the desired slots converge.
		slotGroups := make(map[int][]runtime.ServiceContainer)
		var legacy []runtime.ServiceContainer
		for _, ctr := range existing {
			if ctr.Replica < 0 {
				legacy = append(legacy, ctr)
				continue
			}
			slotGroups[ctr.Replica] = append(slotGroups[ctr.Replica], ctr)
		}

		// stale collects every container to stop AFTER the desired slots have
		// been converged (start-before-stop). A running fallback whose
		// replacement failed is simply never added to stale, so the slot keeps
		// serving until the next reconcile retries.
		var stale []runtime.ServiceContainer
		started := 0

		// For each desired replica slot, ensure a converged replacement is
		// running BEFORE the old generation for that same logical slot is
		// stopped. Group members are ordered deterministically by container ID
		// (never by Docker list order), so a pass with duplicate A+B
		// generations converges identically every time.
		// A pending candidate in one slot must not stop unrelated slots from
		// progressing. Every slot is attempted; only commits/stops that would
		// compromise a pending slot are deferred.
		addCandidate := func(ctr runtime.ServiceContainer) {
			for _, existing := range pass.candidates {
				if existing.ContainerID == ctr.ID {
					return
				}
			}
			pass.candidates = append(pass.candidates, runtime.ServiceHealthCandidate{
				App: fnName, ContainerID: ctr.ID,
			})
		}
		addCandidateID := func(id string) {
			if id == "" {
				return
			}
			for _, existing := range pass.candidates {
				if existing.ContainerID == id {
					return
				}
			}
			pass.candidates = append(pass.candidates, runtime.ServiceHealthCandidate{
				App: fnName, ContainerID: id,
			})
		}
		// handlePendingStart classifies a StartService ErrServicePending result. A
		// health-gated container that already exited/unhealthy enters the SAME hard
		// bounded-retry lifecycle as an observed failure, and its durable failure
		// instant must be recorded before the pass returns pending. A merely
		// starting candidate persists its attempt/start record before returning
		// pending, so a candidate that disappears before its failure is recorded —
		// even across a restart — is still converted into the SAME attempt's
		// bounded backoff instead of restarting at attempt one. The failed/old
		// generation is always preserved.
		//
		// A marker write failure is NOT swallowed: the pass records it as an
		// independent error and keeps the in-memory wake window only to avoid a
		// hot retry loop in THIS process. The window is never durable authority —
		// the next pass re-derives eligibility from the marker, and a later
		// attempt cannot start a replacement unless that durable record exists.
		handlePendingStart := func(slot int, id string, attempt int, failed []runtime.ServiceContainer) {
			pass.pending = true
			if effectiveHealthcheck == nil {
				return
			}
			observation, outcome := observeServiceContainer(postCtx, docker, id)
			failure := outcome == serviceProbeObserved &&
				(observation.Missing ||
					observation.State != container.StateRunning ||
					observation.Health == container.Unhealthy)
			if failure {
				if attempt < 1 {
					attempt = 1
				}
				eligibleAt := time.Now().Add(serviceHealthBackoff(
					effectiveHealthcheck.Interval, attempt))
				pass.retryNotBefore = earlierDeadline(pass.retryNotBefore, eligibleAt)
				if stateTracker != nil {
					stateTracker.noteRetryFloor(
						candidateRetryFloorKey(fnName, name, slot, desiredGeneration),
						eligibleAt, attempt)
					if err := stateTracker.recordCandidateFailure(
						ctx, fnName, name, slot, desiredGeneration, id, attempt, eligibleAt,
					); err != nil {
						fail(fmt.Errorf("service %q replica %d: %w", name, slot, err))
						desiredFailed = true
						// The hard window and floor were installed above and are
						// retained: losing the durable write must not let a
						// notification retry early in this process.
					}
				}
			} else {
				pass.startupDeadline = earlierDeadline(pass.startupDeadline,
					time.Now().Add(effectiveHealthcheck.Interval))
				if stateTracker != nil {
					startedAt := time.Now()
					if outcome == serviceProbeObserved && !observation.StartedAt.IsZero() {
						startedAt = observation.StartedAt
					}
					if err := stateTracker.recordCandidateStarted(
						ctx, fnName, name, slot, desiredGeneration, id, attempt, startedAt,
					); err != nil {
						fail(fmt.Errorf("service %q replica %d: %w", name, slot, err))
						desiredFailed = true
						pass.startupDeadline = earlierDeadline(pass.startupDeadline,
							time.Now().Add(servicePreparationBackoff(
								effectiveHealthcheck.Interval, attempt)))
					}
				}
			}
			addHealthCandidate(&pass, fnName, id)
			for _, ctr := range failed {
				addHealthCandidate(&pass, fnName, ctr.ID)
			}
		}
		// beginCandidateAttempt persists the admitted attempt BEFORE any
		// StartService. The attempt/start anchor is then durable even if the
		// started record cannot be written or the candidate disappears before its
		// failure is observed; a failed write refuses the start instead of
		// admitting an attempt that is not durably recorded.
		beginCandidateAttempt := func(slot, attempt int) bool {
			if effectiveHealthcheck == nil || stateTracker == nil {
				return true
			}
			if err := stateTracker.recordCandidateStarted(
				ctx, fnName, name, slot, desiredGeneration, "", attempt, time.Now(),
			); err != nil {
				fail(fmt.Errorf("service %q replica %d: %w", name, slot, err))
				desiredFailed = true
				pass.pending = true
				// The attempt is NOT durable, so the start is refused. Record the
				// same hard window in the process-local floor (and hand it to the
				// coordinator) so notifications cannot retry early; a restart
				// re-derives conservatively.
				floorAt := time.Now().Add(servicePreparationBackoff(
					effectiveHealthcheck.Interval, attempt))
				stateTracker.noteRetryFloor(
					candidateRetryFloorKey(fnName, name, slot, desiredGeneration),
					floorAt, attempt)
				pass.retryNotBefore = earlierDeadline(pass.retryNotBefore, floorAt)
				return false
			}
			stateTracker.clearRetryFloor(
				candidateRetryFloorKey(fnName, name, slot, desiredGeneration))
			return true
		}
		// retryStartFailure turns a start that failed before any candidate
		// existed into a durable slot-level retry window (no container ID), so
		// the replacement is retried with the bounded exponential backoff rather
		// than on the app's periodic cadence. A write failure keeps the pass
		// pending and explicit without claiming durable eligibility. A service
		// without a declared probe keeps the historical terminal failure.
		retryStartFailure := func(slot, attempt int) {
			if effectiveHealthcheck == nil || stateTracker == nil {
				return
			}
			pass.pending = true
			if attempt < 1 {
				attempt = 1
			}
			eligibleAt := time.Now().Add(servicePreparationBackoff(
				effectiveHealthcheck.Interval, attempt))
			stateTracker.noteRetryFloor(
				candidateRetryFloorKey(fnName, name, slot, desiredGeneration),
				eligibleAt, attempt)
			if err := stateTracker.recordCandidateFailure(
				ctx, fnName, name, slot, desiredGeneration, "", attempt, eligibleAt,
			); err != nil {
				fail(fmt.Errorf("service %q replica %d: %w", name, slot, err))
				desiredFailed = true
				// Not durable: the floor above retains the hard window in memory
				// for this process, so another slot's notification cannot retry
				// the start early.
				pass.retryNotBefore = earlierDeadline(pass.retryNotBefore, eligibleAt)
				return
			}
			pass.retryNotBefore = earlierDeadline(pass.retryNotBefore, eligibleAt)
		}
		// recordStartedCandidate refines the durable attempt with the accepted
		// container ID and its start instant immediately after StartService. The
		// pre-start write already made the ATTEMPT durable; this makes the
		// candidate's identity durable too, so a candidate that disappears before
		// its failure is recorded is converted with the same attempt, and a
		// superseded pass can drop exactly the record it abandoned. A write
		// failure keeps the pre-start attempt record and the candidate pending.
		recordStartedCandidate := func(slot int, id string, attempt int) bool {
			if effectiveHealthcheck == nil || stateTracker == nil {
				return true
			}
			if err := stateTracker.recordCandidateStarted(
				ctx, fnName, name, slot, desiredGeneration, id, attempt, time.Now(),
			); err != nil {
				fail(fmt.Errorf("service %q replica %d: %w", name, slot, err))
				desiredFailed = true
				pass.pending = true
				// The pre-start attempt record is still durable; only the
				// container identity refinement failed. Hold the same hard window
				// in the process-local floor so notifications cannot retry early.
				floorAt := time.Now().Add(servicePreparationBackoff(
					effectiveHealthcheck.Interval, attempt))
				stateTracker.noteRetryFloor(
					candidateRetryFloorKey(fnName, name, slot, desiredGeneration),
					floorAt, attempt)
				pass.retryNotBefore = earlierDeadline(pass.retryNotBefore, floorAt)
				return false
			}
			return true
		}
		for slot := 0; slot < svc.Replicas; slot++ {
			if superseded() {
				pass.superseded = true
				abortPass = true
				break
			}
			group := slotGroups[slot]
			sort.Slice(group, func(i, j int) bool { return group[i].ID < group[j].ID })

			// A committed marker for THIS desired generation designates an already
			// active replica. An active replica that later becomes unhealthy remains
			// active: health alone must never replace it. Only its exit/death or a
			// changed desired generation makes the marker inapplicable.
			committed, hasCommitted := runtime.ServiceConvergenceState{}, false
			if stateTracker != nil {
				committed, hasCommitted = stateTracker.committedState(fnName, name, slot)
			}
			activeID := ""
			if hasCommitted && committed.Generation == desiredGeneration {
				for _, ctr := range group {
					if ctr.ID == committed.CommittedContainerID &&
						usableServiceContainer(ctr) &&
						configured(ctr) && sourceOK(ctr) {
						activeID = ctr.ID
						break
					}
				}
			}

			var matches, olds, starting, failed []runtime.ServiceContainer
			genericPending := false
			for _, ctr := range group {
				if ctr.ID == activeID {
					matches = append(matches, ctr)
					continue
				}
				if hasCommitted && committed.Generation == desiredGeneration &&
					ctr.ID == committed.CommittedContainerID {
					// The committed active replica is no longer usable (exited/dead).
					// This is crash recovery, not a health transition and not a retry
					// attempt: keep it as a non-serving old generation and start a fresh
					// first-attempt replacement.
					olds = append(olds, ctr)
					continue
				}
				if effectiveHealthcheck != nil && configured(ctr) && sourceOK(ctr) {
					switch {
					case ctr.State == container.StateRunning && ctr.Health == container.Healthy:
						// A healthy config-matching container without a committed
						// marker is an uncommitted candidate ready to be adopted or
						// committed in this pass.
						matches = append(matches, ctr)
					case ctr.State == container.StateRunning &&
						(ctr.Health == container.Starting || ctr.Health == "") &&
						!runtime.ServiceHealthCandidateExpired(ctr, effectiveHealthcheck):
						starting = append(starting, ctr)
					default:
						// Unhealthy, exited/dead, or starting past its durable
						// deadline: a failed candidate that must wait for bounded
						// retry eligibility.
						failed = append(failed, ctr)
					}
					continue
				}
				if configured(ctr) && sourceOK(ctr) && ctr.State == container.StateRunning {
					if serviceReady(ctr) {
						matches = append(matches, ctr)
					} else {
						// A backend readiness signal without a healthcheck keeps the
						// historical pending-until-ready behavior.
						genericPending = true
						olds = append(olds, ctr)
					}
					continue
				}
				olds = append(olds, ctr)
			}

			if len(matches) > 0 {
				keep := matches[0]
				if activeID != "" {
					// A committed active replica always wins over an uncommitted
					// healthy candidate in the same slot.
					for _, candidate := range matches {
						if candidate.ID == activeID {
							keep = candidate
							break
						}
					}
				}
				// Revalidate the commit candidates immediately before writing the
				// durable marker and stopping superseded generations. A stale list
				// snapshot is never sufficient authority for a health-gated commit.
				// Every already-listed healthy candidate is tried in deterministic
				// (container ID) order: rejecting the first one must not discard the
				// others and start a needless replacement while a sibling candidate
				// is still viable.
				if effectiveHealthcheck != nil && keep.ID != activeID {
					var remaining []runtime.ServiceContainer
					authorized := false
					for i, candidate := range matches {
						observation, outcome := observeServiceContainer(postCtx, docker, candidate.ID)
						if outcome == serviceProbeUnavailable {
							keep = candidate
							authorized = true
							remaining = append(remaining, matches[i+1:]...)
							break
						}
						if outcome == serviceProbeFailed {
							// An ambiguous probe must never authorize a commit, retire
							// the old generation, or start a duplicate candidate. Keep
							// observing this candidate and try the next match.
							starting = append(starting, candidate)
							continue
						}
						if observation.Missing || observation.State != container.StateRunning ||
							observation.Health != container.Healthy {
							candidate.Health = observation.Health
							candidate.State = observation.State
							if observation.Missing || observation.State != container.StateRunning ||
								observation.Health == container.Unhealthy ||
								runtime.ServiceHealthCandidateExpired(candidate, effectiveHealthcheck) {
								failed = append(failed, candidate)
							} else {
								starting = append(starting, candidate)
							}
							continue
						}
						keep = candidate
						authorized = true
						remaining = append(remaining, matches[i+1:]...)
						break
					}
					if !authorized {
						matches = nil
					} else {
						// Only the authorized candidate holds the slot. The others
						// were classified into starting/failed above, or are extra
						// healthy duplicates queued here. Reducing `matches` to just
						// the kept candidate means the stop loop below (which also
						// skips the kept ID) cannot list any candidate twice.
						matches = []runtime.ServiceContainer{keep}
						stale = append(stale, remaining...)
					}
				}
				if len(matches) > 0 {
					if effectiveHealthcheck != nil && keep.ID != activeID {
						if superseded() {
							pass.superseded = true
							abortPass = true
							break
						}
						// Defer the durable marker write until the final readiness
						// revalidation below. Writing it here would authorize retirement
						// if the candidate later loses readiness before the deferred stop.
						commitIntents = append(commitIntents, commitIntent{
							name: name, replica: slot, generation: desiredGeneration,
							id: keep.ID, attempt: serviceHealthAttemptNumber(keep),
						})
					}
					// A committed/healthy replica holds the slot. Everything else in the
					// slot is safe to stop. Exclude the kept candidate itself: with an
					// active replica chosen out of list order (committed precedence),
					// a plain matches[1:] could stop the replica being kept.
					for _, candidate := range matches {
						if candidate.ID != keep.ID {
							stale = append(stale, candidate)
						}
					}
					stale = append(stale, olds...)
					stale = append(stale, starting...)
					stale = append(stale, failed...)
					continue
				}
			}

			if len(starting) > 0 || genericPending {
				pass.pending = true
				for _, ctr := range starting {
					addCandidate(ctr)
					if effectiveHealthcheck == nil {
						continue
					}
					if deadline, ok := serviceHealthStartupDeadline(ctr, effectiveHealthcheck); ok {
						pass.startupDeadline = earlierDeadline(pass.startupDeadline, deadline)
					} else {
						pass.startupDeadline = earlierDeadline(pass.startupDeadline,
							time.Now().Add(effectiveHealthcheck.Interval))
					}
					// Ensure the attempt/start record exists for a starting
					// candidate this process did not just create (a previous
					// process, or an earlier write that failed): a candidate that
					// disappears before its failure is observed must still be
					// converted into the SAME attempt's backoff after a restart.
					if stateTracker == nil {
						continue
					}
					if _, ok := stateTracker.startedCandidateState(
						fnName, name, slot, desiredGeneration, ctr.ID,
					); ok {
						continue
					}
					startedAt := ctr.StartedAt
					if startedAt.IsZero() {
						startedAt = time.Now()
					}
					if err := stateTracker.recordCandidateStarted(
						ctx, fnName, name, slot, desiredGeneration,
						ctr.ID, serviceHealthAttemptNumber(ctr), startedAt,
					); err != nil {
						fail(fmt.Errorf("service %q replica %d: %w", name, slot, err))
						desiredFailed = true
					}
				}
				continue
			}

			if len(failed) > 0 {
				nextAttempt := runtime.ServiceHealthAttempt(failed)
				retryFrom := failed[0]
				for _, ctr := range failed[1:] {
					if serviceHealthAttemptNumber(ctr) > serviceHealthAttemptNumber(retryFrom) {
						retryFrom = ctr
					}
				}
				attempt := serviceHealthAttemptNumber(retryFrom)
				// The durable marker is the floor, not only the exact candidate
				// label: an attempt whose container vanished keeps its number in
				// the marker, and a later failure in the same generation must not
				// regress below it.
				if stateTracker != nil {
					if recorded, ok := stateTracker.committedState(
						fnName, name, slot,
					); ok && recorded.CandidateGeneration == desiredGeneration &&
						recorded.CandidateAttempt > attempt {
						attempt = recorded.CandidateAttempt
						if attempt+1 > nextAttempt {
							nextAttempt = attempt + 1
						}
					}
				}
				// Failure eligibility is durable: use the recorded failure instant
				// when this exact candidate already persisted one, or the recorded
				// slot-level preparation window when the last start failed before a
				// candidate existed. Otherwise this is the first observation of the
				// failure, so anchor the backoff at NOW and record it before
				// returning pending. Container create time alone cannot represent a
				// failure that happened after several probe intervals.
				recordedRetryAt := time.Time{}
				if stateTracker != nil {
					if recorded, ok := stateTracker.candidateState(
						fnName, name, slot, desiredGeneration, retryFrom.ID,
					); ok {
						recordedRetryAt = recorded.RetryNotBefore
					} else if recorded, ok := stateTracker.slotPreparationState(
						fnName, name, slot, desiredGeneration,
					); ok {
						// The preparation attempt already consumed this number, so
						// the next start continues from it rather than resetting.
						if recorded.CandidateAttempt >= attempt {
							attempt = recorded.CandidateAttempt
							nextAttempt = attempt + 1
						}
						recordedRetryAt = recorded.RetryNotBefore
					}
				}
				eligibleAt := time.Now().Add(serviceHealthBackoff(
					effectiveHealthcheck.Interval, attempt))
				if !recordedRetryAt.IsZero() {
					eligibleAt = recordedRetryAt
				}
				if stateTracker != nil {
					floorKey := candidateRetryFloorKey(fnName, name, slot, desiredGeneration)
					floor := stateTracker.retryFloorFor(floorKey)
					if floor.until.After(eligibleAt) {
						// The process-local floor can be the only record when a
						// marker write failed; it must still gate this process.
						eligibleAt = floor.until
					}
					if floor.attempt > attempt {
						attempt = floor.attempt
						nextAttempt = attempt + 1
					}
					if recordedRetryAt.IsZero() {
						stateTracker.noteRetryFloor(floorKey, eligibleAt, attempt)
						if err := stateTracker.recordCandidateFailure(
							ctx, fnName, name, slot, desiredGeneration, retryFrom.ID,
							attempt, eligibleAt,
						); err != nil {
							fail(fmt.Errorf("service %q replica %d: %w", name, slot, err))
							desiredFailed = true
							// The failed generation is retained and the slot is
							// reported pending: no replacement can start before the
							// window. The write failed, so the floor above keeps the
							// SAME hard window in this process.
							pass.pending = true
							pass.retryNotBefore = earlierDeadline(
								pass.retryNotBefore, eligibleAt)
							for _, ctr := range failed {
								addCandidate(ctr)
							}
							continue
						}
					}
				}
				if time.Now().Before(eligibleAt) {
					// Bounded retry eligibility is authoritative. No start and no stop
					// of the slot's fallback may run before it; Notify cannot bypass
					// this because the coordinator holds the same hard deadline.
					pass.pending = true
					pass.retryNotBefore = earlierDeadline(pass.retryNotBefore, eligibleAt)
					for _, ctr := range failed {
						addCandidate(ctr)
					}
					continue
				}
				// Eligible: start exactly one new candidate with the next durable
				// attempt. Keep only the highest-attempt failed generation as the
				// retry-metadata/fallback carrier and remove additional failed
				// generations before starting, so repeated failures cannot accumulate
				// duplicate candidates. Failed generations stay until a viable
				// replacement exists.
				if len(failed) > 1 {
					var extra []runtime.ServiceContainer
					for _, ctr := range failed {
						if ctr.ID != retryFrom.ID {
							extra = append(extra, ctr)
						}
					}
					if superseded() {
						pass.superseded = true
						abortPass = true
						break
					}
					changed = true
					corrective = true
					notifyReconcile()
					cleanupCtx, cleanupCancel := bounded()
					stopped, known, err := stopServiceContainers(cleanupCtx, docker, extra)
					cleanupCancel()
					pass.stopped += stopped
					pass.stoppedKnown = pass.stoppedKnown && known
					if err != nil {
						fail(fmt.Errorf("service %q failed candidates: %w", name, err))
						desiredFailed = true
						continue
					}
					failed = []runtime.ServiceContainer{retryFrom}
				}
				fallback := pickServiceFallback(olds)
				changed = true
				corrective = true
				notifyReconcile()
				if superseded() {
					pass.superseded = true
					abortPass = true
					break
				}
				if !beginCandidateAttempt(slot, nextAttempt) {
					continue
				}
				startedID, err := docker.StartService(postCtx, newSpec(nextAttempt), slot)
				if err != nil {
					if errors.Is(err, ErrServicePending) {
						handlePendingStart(slot, startedID, nextAttempt, failed)
						continue
					}
					fail(fmt.Errorf("service %q replica %d: %w", name, slot, err))
					desiredFailed = true
					if !requestCancelled(ctx, err) {
						log.Warn("Service: replacement failed; keeping old replica",
							"app", fnName, "service", name, "replica", slot, "error", err)
					}
					retryStartFailure(slot, nextAttempt)
					for i := range olds {
						if !usableServiceContainer(olds[i]) {
							stale = append(stale, olds[i])
						}
					}
					continue
				}
				if effectiveHealthcheck != nil {
					if superseded() {
						pass.superseded = true
						abortPass = true
						break
					}
					if !recordStartedCandidate(slot, startedID, nextAttempt) {
						addCandidateID(startedID)
						for _, ctr := range failed {
							addCandidate(ctr)
						}
						continue
					}
					observation, outcome := observeServiceContainer(postCtx, docker, startedID)
					if outcome == serviceProbeFailed ||
						(outcome == serviceProbeObserved &&
							(observation.Missing || observation.State != container.StateRunning ||
								observation.Health != container.Healthy)) {
						pass.pending = true
						pass.startupDeadline = earlierDeadline(pass.startupDeadline,
							time.Now().Add(effectiveHealthcheck.Interval))
						addCandidateID(startedID)
						for _, ctr := range failed {
							addCandidate(ctr)
						}
						continue
					}
					commitIntents = append(commitIntents, commitIntent{
						name: name, replica: slot, generation: desiredGeneration,
						id: startedID, attempt: nextAttempt,
					})
				}
				started++
				pass.started++
				if effectiveHealthcheck == nil && fallback != nil && startedID != "" {
					provisional = append(provisional, runtime.ServiceContainer{
						ID: startedID, App: fnName, Name: name,
						State: container.StateRunning,
					})
				}
				stale = append(stale, failed...)
				stale = append(stale, olds...)
				continue
			}

			// A source-only change under an unchanged image and configuration is
			// applied by restarting the SAME container in place (Docker
			// ContainerRestart), when the Docker implementation supports it: the
			// live read-only source bind is re-read by the fresh process, and the
			// container's identity is retained. The applied fingerprint is
			// recorded so later passes recognize the restarted container as
			// converged despite its immutable relay.source label. Every other
			// generation in the slot is superseded once the restart is confirmed
			// running.
			if restarter != nil {
				var restartTarget *runtime.ServiceContainer
				for i := range olds {
					if sourceOnlyStale(olds[i]) {
						restartTarget = &olds[i]
						break
					}
				}
				if restartTarget != nil {
					changed = true
					corrective = true
					notifyReconcile()
					if err := restarter.RestartServiceContainer(postCtx, restartTarget.ID); err != nil {
						fail(fmt.Errorf("service %q replica %d: source reload restart: %w", name, slot, err))
						desiredFailed = true
						if !requestCancelled(ctx, err) {
							log.Warn("Service: source reload restart failed; keeping generation",
								"app", fnName, "service", name, "replica", slot, "error", err)
						}
						for i := range olds {
							if !usableServiceContainer(olds[i]) {
								stale = append(stale, olds[i])
							}
						}
						continue
					}
					apply.record(restartTarget.ID, fnName, desiredSourceID)
					pass.restarted++
					for i := range olds {
						if olds[i].ID != restartTarget.ID {
							stale = append(stale, olds[i])
						}
					}
					continue
				}
			}

			// No usable generation holds the slot. The fallback is started first; it
			// is stopped only after a viable replacement exists.
			fallback := pickServiceFallback(olds)
			// A durable marker for THIS desired generation is the only remaining
			// record of an attempt whose container is gone: a failed candidate, a
			// STARTED candidate that disappeared before its failure was observed, or
			// a preparation failure that never produced a container. Honor it (or
			// convert it into a durable failure window anchored at its recorded
			// start instant) BEFORE starting anything, so a restart can never reset
			// the attempt or admit a retry that is not durably eligible.
			durableAttempt := 0
			if effectiveHealthcheck != nil && stateTracker != nil {
				floorKey := candidateRetryFloorKey(fnName, name, slot, desiredGeneration)
				floor := stateTracker.retryFloorFor(floorKey)
				if time.Now().Before(floor.until) {
					// A failed marker write can leave the process-local floor as
					// the ONLY eligibility record. Honor it before consulting any
					// marker or starting anything.
					pass.pending = true
					pass.retryNotBefore = earlierDeadline(pass.retryNotBefore, floor.until)
					continue
				}
				if recorded, ok := stateTracker.committedState(fnName, name, slot); ok &&
					recorded.CandidateGeneration == desiredGeneration {
					present := false
					if recorded.CandidateContainerID != "" {
						for _, ctr := range group {
							if ctr.ID == recorded.CandidateContainerID {
								present = true
								break
							}
						}
					}
					if !present {
						switch {
						case !recorded.RetryNotBefore.IsZero():
							if time.Now().Before(recorded.RetryNotBefore) {
								pass.pending = true
								pass.retryNotBefore = earlierDeadline(
									pass.retryNotBefore, recorded.RetryNotBefore)
								continue
							}
							durableAttempt = recorded.CandidateAttempt
						default:
							// The persisted attempt/start record has no container
							// (the start never completed) or its container vanished
							// before its failure was recorded. Turn it into the SAME
							// attempt's durable failed window before any new start.
							attempt := recorded.CandidateAttempt
							if floor.attempt > attempt {
								attempt = floor.attempt
							}
							if attempt < 1 {
								attempt = 1
							}
							// The FAILURE instant was never persisted — that is why
							// this record has no RetryNotBefore. The start operation
							// may also have run longer than the backoff, so anchoring
							// at CandidateStartedAt could already be elapsed and start
							// attempt N+1 immediately after the unrecorded outcome.
							// Anchor at RECOVERY time instead: this can only delay,
							// never start sooner than a full backoff after the
							// outcome, and preserves the attempt number.
							eligibleAt := time.Now().Add(serviceHealthBackoff(
								effectiveHealthcheck.Interval, attempt))
							stateTracker.noteRetryFloor(floorKey, eligibleAt, attempt)
							if err := stateTracker.recordCandidateFailure(
								ctx, fnName, name, slot, desiredGeneration,
								recorded.CandidateContainerID, attempt, eligibleAt,
							); err != nil {
								fail(fmt.Errorf("service %q replica %d: %w", name, slot, err))
								desiredFailed = true
								pass.pending = true
								// Not durable: the floor above keeps the same hard
								// window in this process so notifications cannot
								// start a replacement early. A restart re-derives.
								pass.retryNotBefore = earlierDeadline(
									pass.retryNotBefore, eligibleAt)
								continue
							}
							if time.Now().Before(eligibleAt) {
								pass.pending = true
								pass.retryNotBefore = earlierDeadline(
									pass.retryNotBefore, eligibleAt)
								continue
							}
							durableAttempt = attempt
						}
					}
				}
			}
			attempt := 1
			if durableAttempt > 0 {
				attempt = durableAttempt + 1
			} else if effectiveHealthcheck != nil {
				attempt = runtime.ServiceHealthAttempt(failed)
			}
			changed = true
			corrective = true
			notifyReconcile()
			if superseded() {
				pass.superseded = true
				abortPass = true
				break
			}
			if !beginCandidateAttempt(slot, attempt) {
				continue
			}
			startedID, err := docker.StartService(postCtx, newSpec(attempt), slot)
			if err != nil {
				if errors.Is(err, ErrServicePending) {
					handlePendingStart(slot, startedID, attempt, nil)
					continue
				}
				fail(fmt.Errorf("service %q replica %d: %w", name, slot, err))
				desiredFailed = true
				if !requestCancelled(ctx, err) {
					if fallback != nil {
						log.Warn("Service: replacement failed; keeping old replica",
							"app", fnName, "service", name, "replica", slot, "error", err)
					} else {
						log.Warn("Service: replica unavailable",
							"app", fnName, "service", name, "replica", slot, "error", err)
					}
				}
				retryStartFailure(slot, attempt)
				for i := range olds {
					if !usableServiceContainer(olds[i]) {
						stale = append(stale, olds[i])
					}
				}
				continue
			}
			if effectiveHealthcheck != nil {
				if superseded() {
					pass.superseded = true
					abortPass = true
					break
				}
				if !recordStartedCandidate(slot, startedID, attempt) {
					addCandidateID(startedID)
					continue
				}
				observation, outcome := observeServiceContainer(postCtx, docker, startedID)
				if outcome == serviceProbeFailed ||
					(outcome == serviceProbeObserved &&
						(observation.Missing || observation.State != container.StateRunning ||
							observation.Health != container.Healthy)) {
					pass.pending = true
					pass.startupDeadline = earlierDeadline(pass.startupDeadline,
						time.Now().Add(effectiveHealthcheck.Interval))
					addCandidateID(startedID)
					continue
				}
				commitIntents = append(commitIntents, commitIntent{
					name: name, replica: slot, generation: desiredGeneration,
					id: startedID, attempt: attempt,
				})
			}
			started++
			pass.started++
			if fallback != nil && startedID != "" {
				provisional = append(provisional, runtime.ServiceContainer{
					ID: startedID, App: fnName, Name: name,
					State: container.StateRunning,
				})
			}
			stale = append(stale, olds...)
		}

		if abortPass {
			postCancel()
			break
		}

		// Every slot at or above the desired replica count is excess: a
		// scale-down replica or a duplicate slot. They are cleaned up only
		// after the desired slots above converged, so a replacement is never
		// preempted by removing old replicas first. Slots are walked in
		// ascending order for deterministic stop ordering.
		var excessSlots []int
		for slot := range slotGroups {
			if slot >= svc.Replicas {
				excessSlots = append(excessSlots, slot)
			}
		}
		sort.Ints(excessSlots)
		for _, slot := range excessSlots {
			stale = append(stale, slotGroups[slot]...)
		}
		sort.Slice(legacy, func(i, j int) bool { return legacy[i].ID < legacy[j].ID })
		stale = append(stale, legacy...)

		// Safety net: never take a desired service from at least one actually
		// ready replica to none because every ready generation would be stopped.
		// Survivors are the replacements confirmed running this pass plus every
		// existing ready (running/restarting and not explicitly unready)
		// container not scheduled for a stop. An explicit Ready=false container
		// is only a last-resort running fallback, never a survivor that
		// authorizes stopping the last ready generation. If that count is zero,
		// keep the lowest-ID ready stale container (or, when none is ready, the
		// lowest-ID usable one) and retry next pass.
		surviving := started
		for _, ctr := range existing {
			if serviceReady(ctr) && !containsStaleID(stale, ctr.ID) {
				surviving++
			}
		}
		if svc.Replicas > 0 && surviving == 0 {
			if keep, ok := lowestUsableStaleWith(stale, serviceReady); ok {
				stale = removeStaleID(stale, keep.ID)
				if !requestCancelled(ctx, firstErr) {
					log.Warn("Service: no replacement could be confirmed; keeping a running replica",
						"app", fnName, "service", name, "replica", keep.Replica)
				}
			}
		}

		// Supersession commit boundary: this is the first point at which this
		// service would stop its old generations. If a newer desired state has
		// replaced this request in the meantime, do NOT stop them — the newer
		// request must find the old generation still running. Clean up only the
		// provisional replacements THIS pass started (their IDs are known only
		// here, so no other generation can be touched), then abandon the rest of
		// the pass for the newer request to converge.
		if superseded() {
			log.Info("Service: pass superseded; preserving old generation",
				"app", fnName, "service", name)
			pass.superseded = true
			// The provisional replacements are neither a committed start nor a
			// committed convergence outcome: this pass removes them and leaves
			// the old generation serving, so they must not inflate the summary.
			pass.started -= len(provisional)
			cleanupProvisional(ctx, bounded, docker, fnName, name, provisional, log)
			// The abandoned attempt belongs to no serving generation and the
			// newer request owns a fresh attempt sequence, so drop its durable
			// record instead of making that request wait out its backoff.
			if stateTracker != nil {
				for _, ctr := range provisional {
					stateTracker.clearAttemptForContainer(
						ctx, fnName, name, svc.Replicas, desiredGeneration, ctr.ID)
				}
			}
			provisional = nil
			abortPass = true
			postCancel()
			break
		}

		// Defer the stale/excess generations until every desired service and slot
		// has been attempted below.
		deferredStale = append(deferredStale, stale...)
		// The service committed: its provisional replacements are now the
		// running generation and are no longer removable by a later supersession.
		provisional = nil
		postCancel()
	}

	// Final readiness/commit boundary: every candidate that would become the
	// committed generation is revalidated ONE last time immediately before its
	// marker is written and its superseded generations are retired. A marker is
	// never written for a candidate that lost readiness here, so the previous
	// marker (if any) remains authoritative and the fallback is preserved across
	// this pass and future restarts.
	if len(commitIntents) > 0 {
		checkCtx, checkCancel := bounded()
		for _, intent := range commitIntents {
			if superseded() {
				pass.superseded = true
				abortPass = true
				break
			}
			observation, outcome := observeServiceContainer(checkCtx, docker, intent.id)
			authorized := outcome == serviceProbeUnavailable
			if outcome == serviceProbeObserved {
				authorized = !observation.Missing &&
					observation.State == container.StateRunning &&
					observation.Health == container.Healthy
			}
			if authorized {
				if superseded() {
					pass.superseded = true
					abortPass = true
					break
				}
				// The started record is normally written immediately after
				// StartService. An ADOPTED (already healthy) candidate has none, so
				// record it at this commit boundary too: the durable attempt/start
				// record is what a later failure (or a restarted worker) derives its
				// retry window from, so an unrecorded candidate is not committed and
				// the old marker stays authoritative.
				if stateTracker != nil {
					if _, recorded := stateTracker.startedCandidateState(
						fnName, intent.name, intent.replica,
						intent.generation, intent.id,
					); !recorded {
						if err := stateTracker.recordCandidateStarted(
							ctx, fnName, intent.name, intent.replica,
							intent.generation, intent.id, intent.attempt, time.Now(),
						); err != nil {
							fail(fmt.Errorf("service %q replica %d: %w",
								intent.name, intent.replica, err))
							desiredFailed = true
							deferredStale = removeStaleSlot(
								deferredStale, intent.name, intent.replica)
							pass.pending = true
							addHealthCandidate(&pass, fnName, intent.id)
							if hc := desired[intent.name].Healthcheck; hc != nil {
								pass.startupDeadline = earlierDeadline(
									pass.startupDeadline,
									time.Now().Add(hc.Interval))
							}
							continue
						}
					}
				}
				if stateTracker != nil && stateTracker.stateStore != nil {
					if err := stateTracker.commitServiceState(
						ctx, fnName, intent.name, intent.replica,
						intent.generation, intent.id,
					); err != nil {
						fail(fmt.Errorf("service %q replica %d: %w",
							intent.name, intent.replica, err))
						desiredFailed = true
						deferredStale = removeStaleSlot(
							deferredStale, intent.name, intent.replica)
						pass.pending = true
						addHealthCandidate(&pass, fnName, intent.id)
					}
				}
				continue
			}
			// The candidate did not survive the final readiness check (or the
			// probe was ambiguous/missing). Do not write a marker and do not
			// retire its fallback.
			deferredStale = removeStaleSlot(
				deferredStale, intent.name, intent.replica)
			pass.pending = true
			addHealthCandidate(&pass, fnName, intent.id)
			hc := desired[intent.name].Healthcheck
			attempt := intent.attempt
			if attempt < 1 {
				attempt = 1
			}
			switch {
			case outcome == serviceProbeFailed:
				// Ambiguous probe: preserve the fallback and re-observe shortly.
				if hc != nil {
					pass.startupDeadline = earlierDeadline(pass.startupDeadline,
						time.Now().Add(hc.Interval))
				}
			case observation.Missing || observation.State != container.StateRunning ||
				observation.Health == container.Unhealthy:
				interval := time.Second
				if hc != nil {
					interval = hc.Interval
				}
				eligibleAt := time.Now().Add(serviceHealthBackoff(interval, attempt))
				pass.retryNotBefore = earlierDeadline(pass.retryNotBefore, eligibleAt)
				if stateTracker != nil {
					if err := stateTracker.recordCandidateFailure(
						ctx, fnName, intent.name, intent.replica,
						intent.generation, intent.id, attempt, eligibleAt,
					); err != nil {
						fail(fmt.Errorf("service %q replica %d: %w",
							intent.name, intent.replica, err))
						desiredFailed = true
					}
				}
			default:
				if hc != nil {
					pass.startupDeadline = earlierDeadline(pass.startupDeadline,
						time.Now().Add(hc.Interval))
				}
			}
		}
		checkCancel()
	}
	// Every desired service and replica slot has now been attempted (converged,
	// pending, or failed). Stop the safe stale/excess generations collected from
	// converged slots; a pending slot never contributed its fallback here.
	if !abortPass && !superseded() && len(deferredStale) > 0 {
		changed = true
		corrective = true
		notifyReconcile()
		stopCtx, stopCancel := bounded()
		stopped, known, err := stopServiceContainers(stopCtx, docker, deferredStale)
		stopCancel()
		pass.stopped += stopped
		pass.stoppedKnown = pass.stoppedKnown && known
		if err != nil {
			fail(fmt.Errorf("service stale: %w", err))
		}
	}

	// Removed services are stopped LAST: their containers are no longer desired,
	// but deferring the stop until every desired service has converged means a
	// RENAME (which is indistinguishable from a removal plus an addition under
	// name grouping) cannot have its old generation torn down before the new
	// name's replicas are running. Removed-service and removed-app cleanup
	// still removes every generation.
	//
	// They are skipped entirely when the pass was superseded or a desired service
	// did not converge. A removed name's running containers can be the ONLY
	// usable generation for the service the new name replaces (a rename), or, for
	// an ordinary removal, a failed desired service may be transient; stopping
	// them then would destroy the last usable generation. A subsequent reconcile
	// (the coalesced newer desired state, or the next periodic pass) retries the
	// removal once the desired set converges.
	if pass.pending {
		// A pending desired service preserves every removed name's containers
		// too: they can be the last usable generation while the replacement is
		// not terminally converged.
	} else if abortPass || superseded() {
		// A superseded pass preserves a removed name's running containers: they
		// may be the only usable generation for the service a rename replaces.
		// With nothing to preserve this phase did no work, so it is neither
		// logged nor allowed to mark an otherwise completed pass superseded.
		if len(removed) > 0 {
			pass.superseded = true
			log.Info("Service: pass superseded; preserving removed services",
				"app", fnName, "removed", len(removed))
		}
	} else if len(removed) > 0 && desiredFailed {
		if !requestCancelled(ctx, firstErr) {
			log.Warn("Service: desired services did not converge; preserving removed services",
				"app", fnName, "removed", len(removed))
		}
	} else if len(removed) > 0 {
		sort.Slice(removed, func(i, j int) bool { return removed[i].ID < removed[j].ID })
		changed = true
		corrective = true
		notifyReconcile()
		stopCtx, stopCancel := ctx, func() {}
		if reconcileTimeout > 0 {
			stopCtx, stopCancel = context.WithTimeout(ctx, reconcileTimeout)
		}
		stopped, known, err := stopServiceContainers(stopCtx, docker, removed)
		pass.stopped += stopped
		pass.stoppedKnown = pass.stoppedKnown && known
		if err != nil {
			fail(fmt.Errorf("service removed: %w", err))
		} else if stateTracker != nil && stateTracker.stateStore != nil {
			names := make(map[string]bool, len(removed))
			for _, ctr := range removed {
				names[ctr.Name] = true
			}
			for removedName := range names {
				if err := stateTracker.deleteServiceStates(
					ctx, fnName, removedName,
				); err != nil {
					log.Debug("Service: remove convergence state failed",
						"app", fnName, "service", removedName, "error", err)
				}
			}
		}
		stopCancel()
	}

	pass.changed = changed
	return pass, firstErr
}

// cleanupProvisional removes the replacement containers a single pass started
// but never committed (a slot whose later cleanup found the request superseded).
// It removes ONLY the listed ids — this pass's own creations — so it can never
// stop a running old generation. Failures are logged, never fatal: a leftover
// provisional container is a non-converged generation the next reconcile
// replaces, exactly like any other stale candidate.
func cleanupProvisional(
	lifecycleCtx context.Context,
	bounded func() (context.Context, context.CancelFunc),
	docker Docker,
	fnName, name string,
	provisional []runtime.ServiceContainer,
	log *slog.Logger,
) {
	if len(provisional) == 0 {
		return
	}
	stopCtx, stopCancel := bounded()
	defer stopCancel()
	if err := docker.StopServiceContainers(stopCtx, provisional); err != nil {
		if !requestCancelled(lifecycleCtx, err) {
			log.Warn("Service: remove provisional replacement failed",
				"app", fnName, "service", name, "count", len(provisional), "error", err)
		}
	}
}

// serviceContainerReady reports the backend's CURRENT readiness observation for
// a service container. Unknown (nil) preserves Docker's historical behavior: a
// running container is treated as converged. A backend that reports an explicit
// false keeps an otherwise config-matching candidate pending.
func serviceContainerReady(ctr runtime.ServiceContainer) bool {
	return ctr.Ready == nil || *ctr.Ready
}

// pickFallbackWith chooses the deterministic last-resort serving generation for
// a slot using the caller's readiness view: the lowest-ID actually-ready
// container, else the lowest-ID usable (running/restarting) one.
func pickFallbackWith(
	containers []runtime.ServiceContainer,
	ready func(runtime.ServiceContainer) bool,
) *runtime.ServiceContainer {
	for i := range containers {
		if ready(containers[i]) {
			return &containers[i]
		}
	}
	for i := range containers {
		if usableServiceContainer(containers[i]) {
			return &containers[i]
		}
	}
	return nil
}

// pickFallback chooses the deterministic last-resort serving generation for a
// slot: the lowest-ID actually-ready container, else the lowest-ID usable
// (running/restarting) one. Ordering is by container ID, never Docker list order.
func pickFallback(containers []runtime.ServiceContainer) *runtime.ServiceContainer {
	return pickFallbackWith(containers, readyServiceContainer)
}

func serviceHealthAttemptNumber(ctr runtime.ServiceContainer) int {
	if ctr.HealthcheckAttempt < 1 {
		return 1
	}
	return ctr.HealthcheckAttempt
}

// addHealthCandidate appends one unique pending healthcheck candidate hint to a
// pass result.
func addHealthCandidate(pass *servicePass, app, id string) {
	if id == "" {
		return
	}
	for _, existing := range pass.candidates {
		if existing.ContainerID == id {
			return
		}
	}
	pass.candidates = append(pass.candidates, runtime.ServiceHealthCandidate{
		App: app, ContainerID: id,
	})
}

// removeStaleSlot returns stale without containers belonging to one service
// replica slot. It may reuse the backing array; the caller must not alias the
// original.
func removeStaleSlot(
	stale []runtime.ServiceContainer, name string, replica int,
) []runtime.ServiceContainer {
	out := stale[:0]
	for _, ctr := range stale {
		if ctr.Name == name && ctr.Replica == replica {
			continue
		}
		out = append(out, ctr)
	}
	return out
}

// serviceGenerationIdentity is the desired implementation identity of one
// service replica: every input that makes a container config-current. The
// committed-generation marker compares this value so an active replica whose
// desired configuration changed is never mistaken for the current active
// generation. Routing labels are included sorted; nothing secret is part of it.
func serviceGenerationIdentity(
	sourceRef, image, imageID string,
	port int,
	environment, resources, tmpfs, networks, healthcheck, sourceID string,
	entry []string,
	labels map[string]string,
) string {
	fields := []string{
		sourceRef,
		image,
		imageID,
		fmt.Sprintf("%d", port),
		environment,
		resources,
		tmpfs,
		networks,
		healthcheck,
		sourceID,
	}
	fields = append(fields, entry...)
	return serviceIdentityHash(fields, labels)
}

// servicePreparationIdentity is the generation identity available BEFORE the
// service's source resolves. It covers every desired input a template or app
// generation can change at that point: source descriptor, the app image an
// `entrypoint` service runs (a Git source update changes it while the service
// template stays byte-identical), port, replica count, routing host/path, the
// FULL routing configuration identity (the generated Traefik labels, which embed
// the network, entrypoints, cert resolver, priority, and host override), the
// EFFECTIVE probe, tmpfs, resources, runtime, env/secret bindings, and the desired
// network set. A new desired generation therefore resets a persisted preparation
// retry window instead of inheriting an obsolete pre-candidate wait. The resolved
// image content and resolved secret VALUES are deliberately absent: they cannot be
// known before preparation succeeds, and the bounded window is capped regardless.
//
// sourceMounted mirrors what ResolveServiceImage is about to report: under
// HOT_RELOAD Relay's probe is bypassed entirely, so the declared healthcheck is
// NOT part of the identity (a healthcheck-only edit must not reset the retry
// window) and the retry policy is the no-probe historical behavior.
func servicePreparationIdentity(
	fnName string, tmpl *app.Template, svc app.Service, appImage string,
	serviceNetworks []string, traefik routing.TraefikConfig, sourceMounted bool,
) string {
	routeNetwork := ""
	if svc.Host != "" {
		routeNetwork = traefik.Network
	}
	networkSet := runtime.NetworkSet(
		append(append([]string(nil), serviceNetworks...), routeNetwork))
	fields := []string{
		svc.SourceRef(),
		fmt.Sprintf("%d", svc.Port),
		fmt.Sprintf("%d", svc.Replicas),
		svc.Host,
		svc.Path,
		runtime.ServiceTmpfsLabel(svc.Tmpfs),
		tmpl.ResourceLimits().Fingerprint(),
		tmpl.Runtime,
		runtime.NetworksLabel(networkSet...),
	}
	if sourceMounted {
		fields = append(fields, "source-mounted")
	} else {
		fields = append(fields, runtime.ServiceHealthcheckLabel(svc.Healthcheck))
	}
	if svc.Entrypoint != "" {
		// An entrypoint service runs the app's own image; an image source runs
		// its configured image reference, already covered by SourceRef.
		fields = append(fields, "app-image", appImage)
	}
	for _, ev := range tmpl.EnvList() {
		fields = append(fields, ev.Name+"="+ev.Value)
	}
	for _, sb := range tmpl.SecretList() {
		fields = append(fields, sb.Name+"="+sb.Ref.String())
	}
	routingLabels := routing.TraefikLabels(fnName, svc.Name, svc.Host, svc.Path, svc.Port, traefik)
	return serviceIdentityHash(fields, routingLabels)
}

// serviceIdentityHash hashes an ordered field list (and, when non-nil, a label
// map's sorted key/value pairs) into the short identity used by service state.
// The field order is part of the identity and must stay stable.
func serviceIdentityHash(fields []string, labels map[string]string) string {
	h := sha256.New()
	write := func(field string) {
		_, _ = h.Write([]byte(field))
		_, _ = h.Write([]byte{0})
	}
	for _, field := range fields {
		write(field)
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		write(key)
		write(labels[key])
	}
	return hex.EncodeToString(h.Sum(nil))[:runtimeServiceHashLen]
}

const runtimeServiceHashLen = 16

// maxServiceHealthRetryDelay caps one retry backoff interval. It is a polling
// cap, not an attempt limit: the durable attempt and recorded failure instant keep
// growing the eligibility window across restarts without ever giving up.
const maxServiceHealthRetryDelay = time.Minute

// serviceHealthBackoff is the bounded exponential retry delay for one failed
// candidate: the configured probe interval doubled once per failed attempt, from
// the interval up to max(one minute, interval). The cap can never be shorter
// than the configured interval, so a template that probes less often than once a
// minute (the config allows intervals up to one hour) is never retried faster
// than its own cadence; a template that probes faster backs off to one minute.
// There is no attempt cutoff — only the delay is capped.
func serviceHealthBackoff(interval time.Duration, attempt int) time.Duration {
	if interval <= 0 {
		return 0
	}
	limit := maxServiceHealthRetryDelay
	if interval > limit {
		limit = interval
	}
	delay := interval
	for i := 1; i < attempt && delay < limit; i++ {
		delay *= 2
		if delay > limit {
			delay = limit
		}
	}
	return delay
}

// servicePreparationBackoff is the bounded exponential delay for retrying a
// PREPARATION failure of a healthcheck-enabled service (a source/image,
// environment/secret, routing-network, or create/start failure before any
// candidate exists). The probe interval is a readiness cadence, not a
// preparation cadence, so the first retry is never slower than one minute: a
// short interval keeps its own cadence, while a long interval (up to one hour)
// starts at one minute and doubles toward that interval. Only the delay is
// capped; there is no attempt cutoff.
func servicePreparationBackoff(interval time.Duration, attempt int) time.Duration {
	limit := maxServiceHealthRetryDelay
	if interval > limit {
		limit = interval
	}
	delay := interval
	if delay <= 0 || delay > maxServiceHealthRetryDelay {
		delay = maxServiceHealthRetryDelay
	}
	for i := 1; i < attempt && delay < limit; i++ {
		delay *= 2
		if delay > limit {
			delay = limit
		}
	}
	return delay
}

// serviceHealthStartupDeadline is the durable point after which a still-starting
// candidate is a failed attempt. It is derived from the instant Docker actually
// STARTED the container plus the desired startup grace and retry window, so the
// coordinator can install the exact wake deadline and a restart reconstructs the
// same instant. Create time is not a substitute: create→start can be delayed,
// and a create-time deadline would expire a freshly started candidate early.
// When no durable start instant is available the caller falls back to a
// bounded re-observation from now, and expiry waits for Docker's own unhealthy
// transition (see runtime.ServiceHealthCandidateExpired).
func serviceHealthStartupDeadline(
	ctr runtime.ServiceContainer, hc *app.ServiceHealthcheck,
) (time.Time, bool) {
	if hc == nil || ctr.StartedAt.IsZero() {
		return time.Time{}, false
	}
	return ctr.StartedAt.Add(
		hc.StartPeriod + time.Duration(hc.Retries+1)*hc.Interval), true
}

// serviceHealthProbe is the optional read-through readiness capability. The
// production runtime Manager implements it; a minimal fake keeps using the list
// snapshot.
type serviceHealthProbe interface {
	ObserveServiceHealth(
		context.Context, []string,
	) (map[string]runtime.ServiceHealthObservation, error)
}

// serviceSourceMountReporter is the optional capability reporting whether an
// app's current prepared generation runs from a live source mount (HOT_RELOAD).
// The production runtime Manager implements it; a Docker that does not (a
// minimal fake) reports no mount, which matches the historical effective-probe
// behavior (ResolveServiceImage would yield no mount in the same situation).
type serviceSourceMountReporter interface {
	ServiceSourceMounted(fnName string) bool
}

// serviceSourceMounted reports whether fnName's prepared generation bypasses
// Relay's own Docker probe via a HOT_RELOAD source mount. It is deliberately
// queried BEFORE source resolution (it is an in-memory record published by
// Prepare), so preparation retries and the preparation identity follow the
// EFFECTIVE probe: a HOT_RELOAD service ignores Relay healthchecks entirely,
// including a healthcheck-only template edit.
func serviceSourceMounted(docker Docker, fnName string) bool {
	reporter, ok := docker.(serviceSourceMountReporter)
	return ok && reporter.ServiceSourceMounted(fnName)
}

// serviceProbeOutcome distinguishes a Docker implementation that cannot probe
// (a minimal fake, keeping the historical snapshot behavior) from a probe that
// ran and failed to give an authoritative answer.
type serviceProbeOutcome uint8

const (
	serviceProbeUnavailable serviceProbeOutcome = iota
	serviceProbeObserved
	serviceProbeFailed
)

// observeServiceContainer revalidates one candidate immediately before a commit
// or stop. Unavailable means the seam does not implement probing (tests keep the
// list snapshot). Observed carries a definitive Docker observation, including
// Missing. Failed means the probe was attempted but could not produce an
// authoritative answer; callers must fail closed.
func observeServiceContainer(
	ctx context.Context, docker Docker, id string,
) (runtime.ServiceHealthObservation, serviceProbeOutcome) {
	probe, available := docker.(serviceHealthProbe)
	if !available || id == "" {
		return runtime.ServiceHealthObservation{}, serviceProbeUnavailable
	}
	observations, err := probe.ObserveServiceHealth(ctx, []string{id})
	if observation, ok := observations[id]; ok {
		return observation, serviceProbeObserved
	}
	if err != nil {
		return runtime.ServiceHealthObservation{}, serviceProbeFailed
	}
	return runtime.ServiceHealthObservation{}, serviceProbeFailed
}

// readyServiceContainer reports whether a container may be counted as an
// actually-ready occupant: it is running/restarting and the backend does not
// explicitly observe it as unready. It is deliberately distinct from
// usableServiceContainer, which also preserves a running-but-unready
// generation as a fallback while a replacement is retried.
func readyServiceContainer(ctr runtime.ServiceContainer) bool {
	return usableServiceContainer(ctr) && serviceContainerReady(ctr)
}

// usableServiceContainer reports whether a non-converged container may still be
// serving and can therefore back a slot while a replacement is retried: running,
// or restarting (Docker is bringing it back). Exited/dead/removing/created
// containers are not usable and are safe to clean even when a replacement
// failed. An explicit Ready=false container is still usable for preservation
// (it must not be stopped on a failed replacement), but it is not counted as a
// ready survivor; see readyServiceContainer.
func usableServiceContainer(ctr runtime.ServiceContainer) bool {
	return ctr.State == container.StateRunning || ctr.State == container.StateRestarting
}

// containsStaleID reports whether stale already holds the container id.
func containsStaleID(stale []runtime.ServiceContainer, id string) bool {
	for _, c := range stale {
		if c.ID == id {
			return true
		}
	}
	return false
}

// removeStaleID returns stale without the container id (a no-op when absent).
// It may reuse the backing array; the caller must not alias the original.
func removeStaleID(stale []runtime.ServiceContainer, id string) []runtime.ServiceContainer {
	out := stale[:0]
	for _, c := range stale {
		if c.ID == id {
			continue
		}
		out = append(out, c)
	}
	return out
}

// lowestUsableStale returns the deterministic last-resort fallback from stale.
// It prefers the lowest-ID actually-ready (running/restarting and not
// explicitly unready) container, so an unready candidate can never displace the
// last ready generation. Only when no ready candidate exists does it fall back
// to the lowest-ID usable running/restarting container.
func lowestUsableStaleWith(
	stale []runtime.ServiceContainer,
	ready func(runtime.ServiceContainer) bool,
) (runtime.ServiceContainer, bool) {
	if best, ok := lowestStaleMatching(stale, ready); ok {
		return best, ok
	}
	return lowestStaleMatching(stale, usableServiceContainer)
}

func lowestUsableStale(stale []runtime.ServiceContainer) (runtime.ServiceContainer, bool) {
	return lowestUsableStaleWith(stale, readyServiceContainer)
}

func lowestStaleMatching(
	stale []runtime.ServiceContainer, usable func(runtime.ServiceContainer) bool,
) (runtime.ServiceContainer, bool) {
	var best runtime.ServiceContainer
	found := false
	for i := range stale {
		if !usable(stale[i]) {
			continue
		}
		if !found || stale[i].ID < best.ID {
			best = stale[i]
			found = true
		}
	}
	return best, found
}

// routingLabelsMatch reports whether a container's actual label set matches the
// desired routing label set: every desired key/value must be present equal in
// the actual set, and the actual set must carry NO extra Traefik-owned key
// (routing.IsTraefikLabel) — stale routing labels from a previous host/config
// would keep routing old traffic, so they make the container stale. nil-vs-nil
// (an unrouted service and an unlabeled container) matches. Non-Traefik extra
// labels (Relay ownership itself, future additions) are ignored here — Relay
// ownership matching happens via the structured fields above.
func routingLabelsMatch(desired, actual map[string]string) bool {
	for k, want := range desired {
		if actual[k] != want {
			return false
		}
	}
	for k := range actual {
		if _, isDesired := desired[k]; isDesired {
			continue
		}
		if routing.IsTraefikLabel(k) {
			return false
		}
	}
	return true
}

// RemoveAll stops and removes every service container belonging to fnName,
// delegating to the Docker implementation's RemoveAppServiceContainers.
// Used when an app is removed: its service containers must be stopped before
// its images are retired (see the reconciler's deferred RemoveServices
// settlement ordering).
func RemoveAll(ctx context.Context, docker Docker, fnName string, log *slog.Logger) {
	n, err := docker.RemoveAppServiceContainers(ctx, fnName)
	if err != nil {
		log.Warn("Service: remove app containers failed", "app", fnName, "error", err)
		return
	}
	if n > 0 {
		log.Info("Service: removed app containers", "app", fnName, "count", n)
	}
}

// ShutdownCleanup stops and removes every persistent service container owned
// by THIS worker: relay.node_id == nodeID (the node identity, config NODE_ID).
// Graceful Relay shutdown removes this worker's persistent service containers;
// crash recovery remains handled by startup reconciliation.
//
// It exists as its own smallest operation because RemoveAll is app-scoped and
// SweepOrphans is cross-app (the latter is node-scoped only when the
// reconciler is wired with WithNodeID; the exported direct API stays
// unscoped); here ownership is node-scoped only. The returned count is the
// number of this worker's containers selected for removal.
//
// A container with an empty NodeID is NOT claimed by any worker: an empty node
// identity is unmatchable, so ownership is unknowable — startup reconciliation
// handles such strays. As a defensive guard, an empty nodeID argument selects
// nothing.
//
// It is the convenience form of the split cleanup (DiscoverShutdownContainers
// then RemoveShutdownContainers) under ONE lock hold, preserving the historical
// list-and-stop atomicity. Callers that must prove ownership BETWEEN discovery
// and removal (the worker's shutdown step) call the two halves directly, so the
// ownership proof runs after the snapshot and before any removal.
func (c *ServiceReconciler) ShutdownCleanup(ctx context.Context, nodeID string) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	own, err := c.discoverShutdownLocked(ctx, nodeID)
	if err != nil {
		return 0, err
	}
	if len(own) == 0 {
		return 0, nil
	}
	return len(own), c.removeShutdownLocked(ctx, own)
}

// discoverShutdownLocked selects THIS worker's persistent service containers:
// every discovered service container whose relay.node_id label equals nodeID.
// It is the DISCOVERY half of ShutdownCleanup, split out so the caller can prove
// ownership before any removal, and it must be called with c.mu held (see the
// locked wrappers DiscoverShutdownContainers and ShutdownCleanup).
//
// Strict equality: a container without a node_id label is not claimed by any
// worker's shutdown (ownership unknowable; startup reconciliation handles
// strays). An empty nodeID is unmatchable, so nothing is selected — a
// defensive guard for a caller that somehow has no node identity.
func (c *ServiceReconciler) discoverShutdownLocked(ctx context.Context, nodeID string) ([]runtime.ServiceContainer, error) {
	if nodeID == "" {
		return nil, nil
	}
	// The shutdown-scoped listener runs the lookup on the fresh shutdown step
	// context through the Manager's narrow shutdown-only gate: the worker
	// lifecycle is already cancelled when this step runs, so the ordinary method
	// would be refused. A Docker without the capability falls back to the
	// ordinary method.
	listContainers, _ := shutdownDockerOps(c.docker)
	containers, err := listContainers(ctx)
	if err != nil {
		logErr := fmt.Errorf("service: list containers: %w", err)
		c.log.Warn("Service: shutdown cleanup failed", "error", logErr)
		return nil, logErr
	}
	var own []runtime.ServiceContainer
	for _, ct := range containers {
		if ct.NodeID == nodeID {
			own = append(own, ct)
		}
	}
	if len(own) == 0 {
		c.log.Debug("Service: shutdown cleanup: nothing to clean", "node_id", nodeID)
	}
	return own, nil
}

// removeShutdownLocked stops and removes exactly the given service containers.
// It is the REMOVAL half of ShutdownCleanup and must be called with c.mu held.
// It never re-lists the daemon: the caller passes the containers captured by
// discoverShutdownLocked, so a replacement container created AFTER that snapshot
// is never stopped (its id is not in the captured set).
func (c *ServiceReconciler) removeShutdownLocked(ctx context.Context, own []runtime.ServiceContainer) error {
	if len(own) == 0 {
		return nil
	}
	// The shutdown-scoped removal runs on the fresh shutdown step context through
	// the Manager's narrow shutdown-only gate (the ordinary method is refused once
	// the worker lifecycle is cancelled). The order is preserved: discovery, the
	// caller's fail-closed ownership proof, then this removal of exactly the
	// captured IDs.
	_, removeContainers := shutdownDockerOps(c.docker)
	err := removeContainers(ctx, own)
	if err != nil {
		// StopServiceContainers keeps the partial progress (everything it
		// could stop/remove is gone); the error is still returned to the
		// caller — cleanup is never fatal to shutdown itself.
		c.log.Warn("Service: shutdown cleanup failed", "error", err)
	} else {
		c.log.Info("Service: shutdown cleanup complete", "containers", len(own))
	}
	return err
}

// DiscoverShutdownContainers returns THIS worker's persistent service containers
// (relay.node_id == nodeID) for graceful-shutdown removal. It is the
// discovery half of ShutdownCleanup, exported so the worker can snapshot the
// candidates, prove ownership on its pinned lock, and only then remove the
// captured containers (RemoveShutdownContainers) — never re-listing after the
// proof, which is what keeps a replacement owner's containers safe.
//
// When the configured Docker implements ShutdownDocker (the production runtime
// Manager), discovery is admitted through the shutdown-only gate on the
// caller's fresh step context; otherwise the ordinary method is used.
func (c *ServiceReconciler) DiscoverShutdownContainers(
	ctx context.Context, nodeID string) ([]runtime.ServiceContainer, error,
) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.discoverShutdownLocked(ctx, nodeID)
}

// RemoveShutdownContainers stops and removes exactly the given containers. It is
// the removal half of ShutdownCleanup, exported for the worker's snapshot →
// ownership-proof → remove ordering, and by construction only ever touches the
// containers it is given (immutable IDs captured before the proof).
//
// When the configured Docker implements ShutdownDocker (the production runtime
// Manager), the removal is admitted through the shutdown-only gate on the
// caller's fresh step context; otherwise the ordinary method is used.
func (c *ServiceReconciler) RemoveShutdownContainers(
	ctx context.Context, containers []runtime.ServiceContainer,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.removeShutdownLocked(ctx, containers)
}

// serviceStateKey identifies one Docker-owned marker: one replica's
// committed/candidate record on one node (kind ""), or the service-level
// preparation retry record (kind runtime.ServiceStateKindPreparation). The kind
// makes the two families independent, so a preparation failure and a failed
// replica candidate never overwrite each other's attempt/window. Node scoping
// keeps two workers sharing a daemon from consulting each other's markers.
type serviceStateKey struct {
	kind    string
	nodeID  string
	app     string
	service string
	replica int
}

// StateMarkerStore is the optional Docker-owned committed-generation store. The
// production runtime Manager implements it; a minimal fake without it keeps the
// historical in-memory behavior (every config-matching non-healthy container is
// treated as an uncommitted candidate).
type StateMarkerStore interface {
	ListServiceConvergenceStates(ctx context.Context) ([]runtime.ServiceConvergenceState, error)
	WriteServiceConvergenceState(ctx context.Context, state runtime.ServiceConvergenceState) error
	DeleteServiceConvergenceStates(ctx context.Context, app, service, nodeID string) error
}

// stateMarkerKindDeleter is the optional marker-family delete capability. The
// production runtime Manager implements it; a store that does not falls back to
// a kind-scoped cleared-record write (whose write cleanup removes older markers
// of the same family). It exists so a service's obsolete preparation-retry
// markers can be removed without touching the replica's committed/candidate
// markers.
type stateMarkerKindDeleter interface {
	DeleteServiceConvergenceStatesForKind(
		ctx context.Context, app, service, nodeID, kind string,
	) error
}

// serviceStateLoadDeletion records a cache deletion (one app, or one service
// when service is non-empty) that raced the one-time marker load. It is the
// tombstone that keeps a snapshot taken before the deletion from resurrecting
// the deleted markers in the cache. anyKind records an app/service removal
// (every marker family); otherwise it is scoped to one marker family (kind).
type serviceStateLoadDeletion struct {
	anyKind bool
	kind    string
	app     string
	service string
}

// matches reports whether the deletion covers a discovered marker. Node identity
// is deliberately not part of the match: forgetServiceState drops every cached
// key for the app/service regardless of node, so the tombstone mirrors it.
func (d serviceStateLoadDeletion) matches(state runtime.ServiceConvergenceState) bool {
	if state.App != d.app {
		return false
	}
	if d.service != "" && state.Service != d.service {
		return false
	}
	return d.anyKind || state.Kind == d.kind
}

// ensureConvergenceStates loads the newest committed marker for every service
// replica once per reconciler. A load failure is returned so the pass fails
// closed rather than mistaking an active replica for an uncommitted candidate.
//
// Initialization is serialized by stateInitMu, NOT by the cache mutex: the
// marker list is a Docker call, and holding the cache lock across network I/O
// would block every concurrent pass of every app behind one daemon round trip.
// stateMu only guards the in-memory cache. The list runs on its own bounded
// operation context (see stateOpContext), so a hung daemon cannot pin a service
// worker forever. A failed load leaves the reconciler uninitialized: the next
// caller retries instead of treating an empty cache as authoritative.
//
// The load is a SNAPSHOT, and other operations mutate the cache (and Docker)
// while it is in flight: Remove/deleteServiceStates and pruning delete markers,
// and a marker write can update the cache. Replacing the cache with the raw
// snapshot would resurrect a marker deleted from Docker during the load (for
// example a removed app's preparation-retry window, which a re-added app with
// the same desired identity would then wrongly inherit) or discard a newer
// in-memory write. The merge below therefore starts the load as a tracked
// revision: every cache write and every forget during the load is recorded, a
// discovered entry whose key was written or deleted meanwhile is ignored (the
// in-memory result is newer), and a written entry is re-applied from the cache.
func (c *ServiceReconciler) ensureConvergenceStates(ctx context.Context) error {
	c.stateInitMu.Lock()
	defer c.stateInitMu.Unlock()
	c.stateMu.Lock()
	loaded := c.stateLoaded
	c.stateMu.Unlock()
	if loaded {
		return nil
	}
	if c.stateStore == nil {
		c.stateMu.Lock()
		c.stateLoaded = true
		c.stateMu.Unlock()
		return nil
	}
	c.stateMu.Lock()
	c.stateLoadActive = true
	c.stateLoadTouched = nil
	c.stateLoadDeletions = nil
	c.stateMu.Unlock()

	loadCtx, cancel := c.stateOpContext(ctx)
	states, err := c.stateStore.ListServiceConvergenceStates(loadCtx)
	cancel()
	if err != nil {
		c.stateMu.Lock()
		c.stateLoadActive = false
		c.stateLoadTouched = nil
		c.stateLoadDeletions = nil
		c.stateMu.Unlock()
		return fmt.Errorf("service: load convergence state: %w", err)
	}

	c.stateMu.Lock()
	merged := make(map[serviceStateKey]runtime.ServiceConvergenceState, len(states))
	for _, state := range states {
		if c.nodeID != "" && state.NodeID != c.nodeID {
			continue
		}
		key := serviceStateKey{
			kind: state.Kind, nodeID: state.NodeID, app: state.App,
			service: state.Service, replica: state.Replica,
		}
		if c.stateLoadRaceLocked(key, state) {
			continue
		}
		existing, ok := merged[key]
		if ok && existing.UpdatedAt.After(state.UpdatedAt) {
			continue
		}
		merged[key] = state
	}
	// Re-apply writes that landed while the list was in flight: they are newer
	// than the snapshot. A key written and then forgotten has no cache entry and
	// stays absent (the deletion tombstone already skipped the snapshot entry).
	for key := range c.stateLoadTouched {
		if state, ok := c.committed[key]; ok {
			merged[key] = state
		}
	}
	c.committed = merged
	c.stateLoaded = true
	c.stateLoadActive = false
	c.stateLoadTouched = nil
	c.stateLoadDeletions = nil
	c.stateMu.Unlock()
	return nil
}

// stateLoadRaceLocked reports whether a marker discovered by the in-flight load
// must be ignored because the cache was mutated for it meanwhile. The caller must
// hold stateMu.
func (c *ServiceReconciler) stateLoadRaceLocked(
	key serviceStateKey, state runtime.ServiceConvergenceState,
) bool {
	if !c.stateLoadActive {
		return false
	}
	if _, touched := c.stateLoadTouched[key]; touched {
		return true
	}
	for _, deletion := range c.stateLoadDeletions {
		if deletion.matches(state) {
			return true
		}
	}
	return false
}

// serviceRetryFloorKey identifies one process-local retry window. It mirrors the
// marker family key plus the generation, so a floor never leaks into a different
// desired generation.
type serviceRetryFloorKey struct {
	kind       string
	app        string
	service    string
	replica    int
	generation string
}

// serviceRetryFloor is one process-local retry window: the earliest eligible
// instant and the attempt it belongs to.
type serviceRetryFloor struct {
	until   time.Time
	attempt int
}

// noteRetryFloor records the process-local eligibility for one key. It is called
// whenever a retry window is installed, BEFORE the marker write is attempted, so
// a failed write still holds the same hard eligibility and attempt in this
// process.
func (c *ServiceReconciler) noteRetryFloor(
	key serviceRetryFloorKey, at time.Time, attempt int,
) {
	if at.IsZero() {
		return
	}
	c.retryMu.Lock()
	if c.retryFloor == nil {
		c.retryFloor = map[serviceRetryFloorKey]serviceRetryFloor{}
	}
	if current, ok := c.retryFloor[key]; !ok || at.After(current.until) {
		c.retryFloor[key] = serviceRetryFloor{until: at, attempt: attempt}
	}
	c.retryMu.Unlock()
}

// candidateRetryFloorKey identifies the per-replica candidate retry floor
// (including slot-level start failures, which have no container yet).
func candidateRetryFloorKey(app, service string, replica int, generation string) serviceRetryFloorKey {
	return serviceRetryFloorKey{
		app: app, service: service, replica: replica, generation: generation,
	}
}

// preparationRetryFloorKey identifies the service-level preparation retry floor.
func preparationRetryFloorKey(app, service, generation string) serviceRetryFloorKey {
	return serviceRetryFloorKey{
		kind:       runtime.ServiceStateKindPreparation,
		app:        app,
		service:    service,
		generation: generation,
	}
}

// retryFloorFor returns the process-local eligibility/attempt for one key (zero
// when none is recorded).
func (c *ServiceReconciler) retryFloorFor(key serviceRetryFloorKey) serviceRetryFloor {
	c.retryMu.Lock()
	defer c.retryMu.Unlock()
	return c.retryFloor[key]
}

// clearRetryFloor drops one process-local window after its durable state became
// authoritative or the attempt concluded.
func (c *ServiceReconciler) clearRetryFloor(key serviceRetryFloorKey) {
	c.retryMu.Lock()
	delete(c.retryFloor, key)
	c.retryMu.Unlock()
}

// clearRetryFloors drops every process-local window for one app (and, when
// service is non-empty, one service). It is called when the app/service is
// removed so the map cannot grow across generations.
func (c *ServiceReconciler) clearRetryFloors(app, service string) {
	c.retryMu.Lock()
	defer c.retryMu.Unlock()
	for key := range c.retryFloor {
		if key.app == app && (service == "" || key.service == service) {
			delete(c.retryFloor, key)
		}
	}
}

// rememberStateWriteLocked records a cache write that raced the one-time marker
// load, so the snapshot cannot overwrite the newer in-memory value. The caller
// must hold stateMu.
func (c *ServiceReconciler) rememberStateWriteLocked(key serviceStateKey) {
	if !c.stateLoadActive {
		return
	}
	if c.stateLoadTouched == nil {
		c.stateLoadTouched = map[serviceStateKey]struct{}{}
	}
	c.stateLoadTouched[key] = struct{}{}
}

// stateOpContext derives the bounded context for ONE Docker-owned marker
// operation (list, write, or delete) from the pass's lifecycle context. The
// marker store is a Docker API surface like any other, so a daemon that stops
// answering must not hold a coordinator worker or a startup step indefinitely;
// reconcileTimeout is the reconciler's normal-operation bound, and a
// non-positive value leaves the operation bounded only by the lifecycle.
// Bounding here covers every caller (pass-time writes, startup recovery, and
// pruning) uniformly.
func (c *ServiceReconciler) stateOpContext(
	ctx context.Context,
) (context.Context, context.CancelFunc) {
	if c.reconcileTimeout > 0 {
		return context.WithTimeout(ctx, c.reconcileTimeout)
	}
	return context.WithCancel(ctx)
}

func (c *ServiceReconciler) committedState(
	app, service string, replica int,
) (runtime.ServiceConvergenceState, bool) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	state, ok := c.committed[serviceStateKey{
		nodeID: c.nodeID, app: app, service: service, replica: replica,
	}]
	return state, ok
}

// candidateState returns the durable failure metadata for the CURRENT desired
// generation, if it belongs to the same candidate container. It is used to
// reconstruct the exact retryNotBefore after a restart.
func (c *ServiceReconciler) candidateState(
	app, service string, replica int, generation, containerID string,
) (runtime.ServiceConvergenceState, bool) {
	state, ok := c.committedState(app, service, replica)
	if !ok || state.CandidateGeneration != generation ||
		state.CandidateContainerID != containerID ||
		state.RetryNotBefore.IsZero() {
		return runtime.ServiceConvergenceState{}, false
	}
	return state, true
}

// recordCandidateFailure persists the observed failure time and attempt in the
// Docker-owned marker BEFORE the pass returns pending. It preserves the committed
// generation fields so an old active replica remains distinguishable. Nothing is
// cached on a failed write: the durable state is the authority a later attempt
// (or a restarted worker) re-derives its eligibility from, so an unpersisted
// window must never look recorded in memory.
func (c *ServiceReconciler) recordCandidateFailure(
	ctx context.Context, app, service string, replica int,
	generation, containerID string, attempt int, retryAt time.Time,
) error {
	if c.stateStore == nil {
		return nil
	}
	key := serviceStateKey{
		nodeID: c.nodeID, app: app, service: service, replica: replica,
	}
	c.stateMu.Lock()
	state := c.committed[key]
	c.stateMu.Unlock()
	state.App = app
	state.Service = service
	state.NodeID = c.nodeID
	state.Replica = replica
	state.CandidateGeneration = generation
	state.CandidateContainerID = containerID
	state.CandidateAttempt = attempt
	state.RetryNotBefore = retryAt
	state.UpdatedAt = time.Now()
	writeCtx, cancel := c.stateOpContext(ctx)
	err := c.stateStore.WriteServiceConvergenceState(writeCtx, state)
	cancel()
	if err != nil {
		return fmt.Errorf("service: record retry state: %w", err)
	}
	c.stateMu.Lock()
	if c.committed == nil {
		c.committed = map[serviceStateKey]runtime.ServiceConvergenceState{}
	}
	c.rememberStateWriteLocked(key)
	c.committed[key] = state
	c.stateMu.Unlock()
	return nil
}

// recordCandidateStarted records a newly admitted candidate attempt: its
// generation, attempt, container ID (empty while the attempt is admitted but no
// container exists yet), and the instant the attempt began. It clears any prior
// failure eligibility. The immutable attempt label is already on the container;
// this marker makes the attempt durable BEFORE the container's outcome is known.
// If the process then dies (or the failure write is unavailable), the attempt
// record has no failure instant, so a restart converts it into the SAME attempt's
// bounded window anchored at RECOVERY time — it may wait longer, but it never
// retries sooner or resets the attempt. A write failure is returned: the caller
// must not start or commit on an unrecorded attempt, and a failed write is never
// cached.
func (c *ServiceReconciler) recordCandidateStarted(
	ctx context.Context, app, service string, replica int,
	generation, containerID string, attempt int, startedAt time.Time,
) error {
	if c.stateStore == nil {
		return nil
	}
	if attempt < 1 {
		attempt = 1
	}
	if startedAt.IsZero() {
		startedAt = time.Now()
	}
	key := serviceStateKey{
		nodeID: c.nodeID, app: app, service: service, replica: replica,
	}
	c.stateMu.Lock()
	state := c.committed[key]
	c.stateMu.Unlock()
	state.App = app
	state.Service = service
	state.NodeID = c.nodeID
	state.Replica = replica
	state.CandidateGeneration = generation
	state.CandidateContainerID = containerID
	state.CandidateAttempt = attempt
	state.CandidateStartedAt = startedAt
	state.RetryNotBefore = time.Time{}
	state.UpdatedAt = time.Now()
	writeCtx, cancel := c.stateOpContext(ctx)
	err := c.stateStore.WriteServiceConvergenceState(writeCtx, state)
	cancel()
	if err != nil {
		return fmt.Errorf("service: record candidate state: %w", err)
	}
	c.stateMu.Lock()
	if c.committed == nil {
		c.committed = map[serviceStateKey]runtime.ServiceConvergenceState{}
	}
	c.rememberStateWriteLocked(key)
	c.committed[key] = state
	c.stateMu.Unlock()
	return nil
}

// recordPreparationFailure persists a bounded retry window for a service whose
// PREPARATION failed before any candidate existed (routing network unavailable,
// source/image resolution, environment/secret resolution). It uses the DISTINCT
// preparation marker family (ServiceStateKindPreparation), generation-fenced by
// the preparation identity so a new desired generation starts a fresh attempt
// sequence. It records no container: the attempt is admitted but has not produced
// one. Because the family is separate, this write can never overwrite a replica's
// candidate attempt/window (or a committed generation). The caller must not retry
// before the returned window elapsed.
func (c *ServiceReconciler) recordPreparationFailure(
	ctx context.Context, app, service string, replica int,
	generation string, attempt int, retryAt time.Time,
) error {
	if c.stateStore == nil {
		return nil
	}
	if attempt < 1 {
		attempt = 1
	}
	key := serviceStateKey{
		kind:   runtime.ServiceStateKindPreparation,
		nodeID: c.nodeID, app: app, service: service, replica: replica,
	}
	state := runtime.ServiceConvergenceState{
		Kind:                runtime.ServiceStateKindPreparation,
		App:                 app,
		Service:             service,
		NodeID:              c.nodeID,
		Replica:             replica,
		CandidateGeneration: generation,
		CandidateAttempt:    attempt,
		RetryNotBefore:      retryAt,
		UpdatedAt:           time.Now(),
	}
	writeCtx, cancel := c.stateOpContext(ctx)
	err := c.stateStore.WriteServiceConvergenceState(writeCtx, state)
	cancel()
	if err != nil {
		return fmt.Errorf("service: record preparation retry state: %w", err)
	}
	c.stateMu.Lock()
	if c.committed == nil {
		c.committed = map[serviceStateKey]runtime.ServiceConvergenceState{}
	}
	c.rememberStateWriteLocked(key)
	c.committed[key] = state
	c.stateMu.Unlock()
	return nil
}

// recordPreparationAttempt persists an ADMITTED preparation attempt in the
// preparation marker family BEFORE any source/network/env work. It records the
// attempt number and its start instant with no retry window; a successful
// preparation records its completion (see recordPreparationCompleted) and prunes
// this family, while a failed attempt overwrites this record with its bounded
// window. If the process dies in between (or the failure write is unavailable),
// the admitted record lets a restart convert the unfinished attempt into the SAME
// attempt's bounded failure anchored at RECOVERY time (the failure instant was
// never persisted), so it never retries sooner or resets the attempt. An
// admission whose exact start instant is matched by a completed record is instead
// a leftover from a preparation that SUCCEEDED, and is never converted. The write
// never touches a replica's committed/candidate marker.
func (c *ServiceReconciler) recordPreparationAttempt(
	ctx context.Context, app, service string, replica int,
	generation string, attempt int, startedAt time.Time,
) error {
	if c.stateStore == nil {
		return nil
	}
	if attempt < 1 {
		attempt = 1
	}
	if startedAt.IsZero() {
		startedAt = time.Now()
	}
	state := runtime.ServiceConvergenceState{
		Kind:                runtime.ServiceStateKindPreparation,
		App:                 app,
		Service:             service,
		NodeID:              c.nodeID,
		Replica:             replica,
		CandidateGeneration: generation,
		CandidateAttempt:    attempt,
		CandidateStartedAt:  startedAt,
		UpdatedAt:           time.Now(),
	}
	writeCtx, cancel := c.stateOpContext(ctx)
	err := c.stateStore.WriteServiceConvergenceState(writeCtx, state)
	cancel()
	if err != nil {
		return fmt.Errorf("service: record preparation attempt: %w", err)
	}
	key := serviceStateKey{
		kind:   runtime.ServiceStateKindPreparation,
		nodeID: c.nodeID, app: app, service: service, replica: replica,
	}
	c.stateMu.Lock()
	if c.committed == nil {
		c.committed = map[serviceStateKey]runtime.ServiceConvergenceState{}
	}
	c.rememberStateWriteLocked(key)
	c.committed[key] = state
	c.stateMu.Unlock()
	return nil
}

// recordPreparationCompleted persists the successful OUTCOME of one admitted
// preparation attempt, identified by its generation, attempt number, and start
// instant. It is written immediately after preparation succeeds and before the
// retry family is pruned, so a process that stops before cleanup (or a failed
// cleanup write) leaves a durable record that the admission did not fail. The
// completion lives in its own marker family: it can never be read as a retry
// window, and pruning the retry family does not erase the outcome. It records no
// container and no retry window. A later admission carries a different start
// instant, so it never inherits this outcome.
func (c *ServiceReconciler) recordPreparationCompleted(
	ctx context.Context, app, service string, replica int,
	generation string, attempt int, admittedAt time.Time,
) error {
	if c.stateStore == nil {
		return nil
	}
	if attempt < 1 {
		attempt = 1
	}
	state := runtime.ServiceConvergenceState{
		Kind:                runtime.ServiceStateKindPreparationCompleted,
		App:                 app,
		Service:             service,
		NodeID:              c.nodeID,
		Replica:             replica,
		CandidateGeneration: generation,
		CandidateAttempt:    attempt,
		CandidateStartedAt:  admittedAt,
		UpdatedAt:           time.Now(),
	}
	writeCtx, cancel := c.stateOpContext(ctx)
	err := c.stateStore.WriteServiceConvergenceState(writeCtx, state)
	cancel()
	if err != nil {
		return fmt.Errorf("service: record preparation completion: %w", err)
	}
	key := serviceStateKey{
		kind:   runtime.ServiceStateKindPreparationCompleted,
		nodeID: c.nodeID, app: app, service: service, replica: replica,
	}
	c.stateMu.Lock()
	if c.committed == nil {
		c.committed = map[serviceStateKey]runtime.ServiceConvergenceState{}
	}
	c.rememberStateWriteLocked(key)
	c.committed[key] = state
	c.stateMu.Unlock()
	return nil
}

// preparationCompleted reports whether the exact admitted preparation attempt
// (generation, attempt, and start instant) is durably known to have completed
// successfully. Only that admission's outcome is matched: a different attempt or
// a later admission of the same generation does not inherit it.
func (c *ServiceReconciler) preparationCompleted(
	app, service string, replica int, generation string, attempt int, admittedAt time.Time,
) bool {
	c.stateMu.Lock()
	state, ok := c.committed[serviceStateKey{
		kind:   runtime.ServiceStateKindPreparationCompleted,
		nodeID: c.nodeID, app: app, service: service, replica: replica,
	}]
	c.stateMu.Unlock()
	return ok && state.CandidateGeneration == generation &&
		state.CandidateAttempt == attempt &&
		state.CandidateStartedAt.Equal(admittedAt) &&
		state.CandidateContainerID == "" && state.RetryNotBefore.IsZero()
}

// preparationAdmission returns the durable preparation-family record for one
// replica when it is an ADMITTED attempt whose outcome was never recorded (no
// retry window yet) for the given preparation generation.
func (c *ServiceReconciler) preparationAdmission(
	app, service string, replica int, generation string,
) (runtime.ServiceConvergenceState, bool) {
	c.stateMu.Lock()
	state, ok := c.committed[serviceStateKey{
		kind:   runtime.ServiceStateKindPreparation,
		nodeID: c.nodeID, app: app, service: service, replica: replica,
	}]
	c.stateMu.Unlock()
	if !ok || state.CandidateGeneration != generation ||
		state.CandidateContainerID != "" || !state.RetryNotBefore.IsZero() ||
		state.CandidateStartedAt.IsZero() {
		return runtime.ServiceConvergenceState{}, false
	}
	return state, true
}

// preparationState returns the durable preparation retry state for one replica
// when it belongs to the given preparation generation and records no container.
// It reads only the preparation marker family, never a replica candidate record.
func (c *ServiceReconciler) preparationState(
	app, service string, replica int, generation string,
) (runtime.ServiceConvergenceState, bool) {
	c.stateMu.Lock()
	state, ok := c.committed[serviceStateKey{
		kind:   runtime.ServiceStateKindPreparation,
		nodeID: c.nodeID, app: app, service: service, replica: replica,
	}]
	c.stateMu.Unlock()
	if !ok || state.CandidateGeneration != generation ||
		state.CandidateContainerID != "" || state.RetryNotBefore.IsZero() {
		return runtime.ServiceConvergenceState{}, false
	}
	return state, true
}

// slotPreparationState returns the durable slot-level retry window recorded when
// a start failed before any candidate existed. It lives in the CANDIDATE family
// (kind "", no container ID) and is distinct from the service-level preparation
// family, so a preparation failure can never overwrite a slot's candidate
// attempt/window (and vice versa).
func (c *ServiceReconciler) slotPreparationState(
	app, service string, replica int, generation string,
) (runtime.ServiceConvergenceState, bool) {
	state, ok := c.committedState(app, service, replica)
	if !ok || state.CandidateGeneration != generation ||
		state.CandidateContainerID != "" || state.RetryNotBefore.IsZero() {
		return runtime.ServiceConvergenceState{}, false
	}
	return state, true
}

// startedCandidateState returns the durable marker for one replica when it
// describes the EXACT candidate container ID of the given generation (a started,
// not-yet-failed attempt).
func (c *ServiceReconciler) startedCandidateState(
	app, service string, replica int, generation, containerID string,
) (runtime.ServiceConvergenceState, bool) {
	state, ok := c.committedState(app, service, replica)
	if !ok || state.CandidateGeneration != generation ||
		state.CandidateContainerID != containerID ||
		state.CandidateContainerID == "" || !state.RetryNotBefore.IsZero() {
		return runtime.ServiceConvergenceState{}, false
	}
	return state, true
}

// clearCandidateState drops a replica's persisted CANDIDATE retry record when it
// belongs to the given generation, preserving that record's committed
// generation fields. It never touches the service-level preparation family. It
// is best-effort: a bounded stale window can only delay the next retry, never
// shorten it, so callers log a failure at debug.
func (c *ServiceReconciler) clearCandidateState(
	ctx context.Context, app, service string, replica int, generation string,
) error {
	if c.stateStore == nil {
		return nil
	}
	key := serviceStateKey{
		nodeID: c.nodeID, app: app, service: service, replica: replica,
	}
	c.stateMu.Lock()
	state := c.committed[key]
	c.stateMu.Unlock()
	if state.Kind != "" || state.CandidateGeneration != generation {
		return nil
	}
	state.CandidateGeneration = ""
	state.CandidateContainerID = ""
	state.CandidateAttempt = 0
	state.CandidateStartedAt = time.Time{}
	state.RetryNotBefore = time.Time{}
	state.UpdatedAt = time.Now()
	writeCtx, cancel := c.stateOpContext(ctx)
	err := c.stateStore.WriteServiceConvergenceState(writeCtx, state)
	cancel()
	if err != nil {
		return fmt.Errorf("service: clear candidate state: %w", err)
	}
	c.stateMu.Lock()
	if c.committed == nil {
		c.committed = map[serviceStateKey]runtime.ServiceConvergenceState{}
	}
	c.rememberStateWriteLocked(key)
	c.committed[key] = state
	c.stateMu.Unlock()
	c.clearRetryFloor(candidateRetryFloorKey(app, service, replica, generation))
	return nil
}

// clearPreparationState drops EVERY persisted preparation-RETRY marker for one
// service after the service prepared successfully, so a later failure starts a
// fresh attempt sequence. It is deliberately not generation-scoped: a marker
// recorded under a previous desired identity (including a declared probe that is
// no longer effective, e.g. after a HOT_RELOAD switch) no longer matches the
// current preparation generation and would otherwise remain forever, because the
// candidate-family write cleanup cannot remove another marker family.
//
// It never touches the completion family: the durable outcome of a successful
// preparation is what lets a later pass tell a resolved admission apart from an
// interrupted one. Dropping this retry family therefore cannot lose that outcome.
//
// The cleanup is bounded: the preparation family holds at most one volume per
// service (write cleanup keeps it that way), so at most one list/delete round
// trip runs, and only when a preparation marker is actually cached. It never
// touches a replica's committed/candidate markers, and a cleanup failure is
// returned for the caller to log at Debug: it can neither fail the pass nor
// change what is served, and the cache is left consistent (the stale entries are
// retained until a delete or overwrite succeeds; every reader ignores a
// non-current preparation generation).
func (c *ServiceReconciler) clearPreparationState(
	ctx context.Context, app, service string, replica int, generation string,
) error {
	defer c.clearPreparationRetryFloors(app, service)
	if c.stateStore == nil || !c.hasPreparationState(app, service) {
		return nil
	}
	if deleter, ok := c.stateStore.(stateMarkerKindDeleter); ok {
		deleteCtx, cancel := c.stateOpContext(ctx)
		err := deleter.DeleteServiceConvergenceStatesForKind(
			deleteCtx, app, service, c.nodeID, runtime.ServiceStateKindPreparation)
		cancel()
		if err != nil {
			return fmt.Errorf("service: clear preparation state: %w", err)
		}
		c.forgetPreparationState(app, service)
		return nil
	}
	// Fallback for a store without the kind-scoped delete: write a cleared
	// current-generation preparation record. The marker store's write cleanup is
	// scoped to the same family, so it removes every older preparation volume for
	// this service in the same bounded call. The cleared record itself carries no
	// retry window and is dropped from the cache afterwards so a successful
	// preparation does not re-write it on every pass.
	writeCtx, cancel := c.stateOpContext(ctx)
	err := c.stateStore.WriteServiceConvergenceState(writeCtx, runtime.ServiceConvergenceState{
		Kind:                runtime.ServiceStateKindPreparation,
		App:                 app,
		Service:             service,
		NodeID:              c.nodeID,
		Replica:             replica,
		CandidateGeneration: generation,
		RetryNotBefore:      time.Time{},
		CandidateStartedAt:  time.Time{},
		UpdatedAt:           time.Now(),
	})
	cancel()
	if err != nil {
		return fmt.Errorf("service: clear preparation state: %w", err)
	}
	c.forgetPreparationState(app, service)
	return nil
}

// hasPreparationState reports whether any preparation-family marker for the
// service is cached. The cache is populated from Docker at initialization, so a
// stale generation is visible here after a restart as well. A nil store has no
// cached preparation state.
func (c *ServiceReconciler) hasPreparationState(app, service string) bool {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	for key := range c.committed {
		if key.kind == runtime.ServiceStateKindPreparation &&
			key.app == app && key.service == service {
			return true
		}
	}
	return false
}

// forgetPreparationState drops every cached preparation-family marker for one
// service. While the one-time marker load is in flight it records a
// kind-scoped tombstone, so a snapshot taken before the deletion cannot
// resurrect the obsolete marker (and no replica candidate marker is affected).
func (c *ServiceReconciler) forgetPreparationState(app, service string) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	for key := range c.committed {
		if key.kind == runtime.ServiceStateKindPreparation &&
			key.app == app && key.service == service {
			delete(c.committed, key)
		}
	}
	if c.stateLoadActive {
		c.stateLoadDeletions = append(c.stateLoadDeletions, serviceStateLoadDeletion{
			kind:    runtime.ServiceStateKindPreparation,
			app:     app,
			service: service,
		})
	}
}

// clearPreparationRetryFloors drops the process-local preparation floors for one
// service. A successful preparation resets the attempt sequence regardless of
// whether a durable marker existed, while replica candidate floors are left
// untouched.
func (c *ServiceReconciler) clearPreparationRetryFloors(app, service string) {
	c.retryMu.Lock()
	defer c.retryMu.Unlock()
	for key := range c.retryFloor {
		if key.kind == runtime.ServiceStateKindPreparation &&
			key.app == app && key.service == service {
			delete(c.retryFloor, key)
		}
	}
}

// clearAttemptForContainer drops the candidate record of the container a
// SUPERSEDED pass started and then removed as provisional. The abandoned
// attempt belongs to no serving generation, and the newer desired request owns a
// fresh attempt sequence; leaving the record would make that newer request wait
// out a backoff for an attempt it never made. The scan is bounded by the
// desired replica count and only matches the exact generation and container.
func (c *ServiceReconciler) clearAttemptForContainer(
	ctx context.Context, app, service string, replicas int,
	generation, containerID string,
) {
	if c.stateStore == nil || containerID == "" {
		return
	}
	for slot := 0; slot < replicas; slot++ {
		state, ok := c.committedState(app, service, slot)
		if !ok || state.CandidateGeneration != generation ||
			state.CandidateContainerID != containerID {
			continue
		}
		if err := c.clearCandidateState(ctx, app, service, slot, generation); err != nil {
			c.log.Debug("Service: clear abandoned candidate state failed",
				"app", app, "service", service, "replica", slot, "error", err)
		}
	}
}

// commitServiceState records the committed generation BEFORE any superseded
// generation is stopped. A failed write is returned so the caller preserves the
// old generation rather than committing without a durable marker.
func (c *ServiceReconciler) commitServiceState(
	ctx context.Context, app, service string, replica int, generation, containerID string,
) error {
	if c.stateStore == nil {
		return nil
	}
	state := runtime.ServiceConvergenceState{
		App: app, Service: service, NodeID: c.nodeID, Replica: replica,
		CommittedContainerID: containerID,
		Generation:           generation,
		UpdatedAt:            time.Now(),
	}
	writeCtx, cancel := c.stateOpContext(ctx)
	err := c.stateStore.WriteServiceConvergenceState(writeCtx, state)
	cancel()
	if err != nil {
		return fmt.Errorf("service: commit convergence state: %w", err)
	}
	c.stateMu.Lock()
	if c.committed == nil {
		c.committed = map[serviceStateKey]runtime.ServiceConvergenceState{}
	}
	key := serviceStateKey{
		nodeID: c.nodeID, app: app, service: service, replica: replica,
	}
	c.rememberStateWriteLocked(key)
	c.committed[key] = state
	c.stateMu.Unlock()
	// The committed generation has no pending retry; drop this slot's
	// process-local floor so it cannot linger across generations.
	c.clearRetryFloor(candidateRetryFloorKey(app, service, replica, generation))
	return nil
}

// deleteServiceStates removes the durable markers for one app or service after
// its containers are gone. The delete runs on its own bounded operation
// context; the cache entry is dropped only when the durable delete succeeded, so
// a failed delete does not make the in-memory view disagree with Docker.
func (c *ServiceReconciler) deleteServiceStates(
	ctx context.Context, app, service string,
) error {
	if c.stateStore == nil {
		c.forgetServiceState(app, service)
		return nil
	}
	deleteCtx, cancel := c.stateOpContext(ctx)
	err := c.stateStore.DeleteServiceConvergenceStates(
		deleteCtx, app, service, c.nodeID)
	cancel()
	if err != nil {
		return fmt.Errorf("service: remove convergence state: %w", err)
	}
	c.forgetServiceState(app, service)
	return nil
}

// forgetServiceState removes one service's markers from the cache after the
// service's containers are removed. While the one-time marker load is in
// flight it also records a deletion tombstone for the app/service scope — even
// when no cache entry exists yet — so the load snapshot cannot resurrect a
// marker that was deleted from Docker meanwhile.
func (c *ServiceReconciler) forgetServiceState(app, service string) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	for key := range c.committed {
		if key.app == app && (service == "" || key.service == service) {
			delete(c.committed, key)
		}
	}
	if c.stateLoadActive {
		c.stateLoadDeletions = append(c.stateLoadDeletions,
			serviceStateLoadDeletion{anyKind: true, app: app, service: service})
	}
	// Forget the process-local retry floors for the same scope, so the map does
	// not grow across generations and a removed service cannot be gated by a
	// stale floor if it reappears with the same identity.
	c.clearRetryFloors(app, service)
}

// RecoverServiceHealthCandidates reconstructs the uncommitted healthcheck
// candidates that existed before this worker started. A container whose ID is a
// committed active replica for this node is excluded: an active replica that
// later becomes unhealthy is not a candidate and must not be replaced for health
// alone. With no state store, every non-healthy healthcheck container is returned
// (the historical best effort).
func (c *ServiceReconciler) RecoverServiceHealthCandidates(
	ctx context.Context,
) ([]runtime.ServiceHealthCandidate, error) {
	if err := c.ensureConvergenceStates(ctx); err != nil {
		return nil, err
	}
	containers, err := c.docker.ServiceContainerList(ctx)
	if err != nil {
		return nil, fmt.Errorf("service: recover health candidates: %w", err)
	}
	committedIDs := map[string]bool{}
	c.stateMu.Lock()
	for key, state := range c.committed {
		if key.nodeID == c.nodeID || c.nodeID == "" {
			committedIDs[state.CommittedContainerID] = true
		}
	}
	c.stateMu.Unlock()
	var out []runtime.ServiceHealthCandidate
	for _, ctr := range containers {
		if c.nodeID != "" && ctr.NodeID != c.nodeID {
			continue
		}
		if ctr.Healthcheck == "" || committedIDs[ctr.ID] {
			continue
		}
		if ctr.Health == container.Healthy {
			continue
		}
		out = append(out, runtime.ServiceHealthCandidate{
			App: ctr.App, ContainerID: ctr.ID,
		})
	}
	return out, nil
}

// PruneServiceConvergenceStates removes committed-generation markers for apps
// that no longer exist. It is a best-effort startup/periodic cleanup companion to
// container orphan sweeping; only Relay service-state volumes for this node are
// considered.
func (c *ServiceReconciler) PruneServiceConvergenceStates(
	ctx context.Context, liveApps map[string]bool,
) {
	if c.stateStore == nil {
		return
	}
	listCtx, listCancel := c.stateOpContext(ctx)
	states, err := c.stateStore.ListServiceConvergenceStates(listCtx)
	listCancel()
	if err != nil {
		c.log.Debug("Service: list convergence state for pruning failed", "error", err)
		return
	}
	seen := map[string]bool{}
	for _, state := range states {
		if c.nodeID != "" && state.NodeID != c.nodeID {
			continue
		}
		if liveApps[state.App] || seen[state.App] {
			continue
		}
		seen[state.App] = true
		if err := c.deleteServiceStates(ctx, state.App, ""); err != nil {
			c.log.Debug("Service: prune convergence state failed",
				"app", state.App, "error", err)
		}
	}
}

// ServiceReconciler ties Reconcile, RemoveAll, SweepOrphans, and ShutdownCleanup
// to a single
// Docker implementation, secret resolver, and logger. App-scoped Apply
// calls are serialized by ServiceCoordinator in the worker; the mutex remains
// around cross-app cleanup operations. A reconciler constructed with
// WithNodeID scopes Reconcile, app removal, and the orphan sweep to its
// stable relay.node_id (see that option); without it the operations keep the
// historical unscoped, same-host behavior.
type ServiceReconciler struct {
	docker  Docker
	secrets SecretResolver
	traefik routing.TraefikConfig
	// networks is the worker-global Docker network set (NETWORKS) every service
	// container joins, exactly as it applies to execution containers. It is set
	// through WithNetworks (the worker wires config's resolved value) and is
	// nil for a direct call without the option, preserving pre-existing
	// behavior (no global networks). It is copied at construction and is
	// startup configuration: changing it requires a worker restart.
	networks []string
	log      *slog.Logger

	// reconcileTimeout is the bound Reconcile derives for each normal
	// service-operation context, rooted in the lifecycle context its caller
	// passes. It is injected by the worker (its reconcileTimeout) so the policy
	// lives with the worker that owns the value; a non-positive value means
	// "no normal-operation bound", leaving operations bounded only by the
	// lifecycle.
	reconcileTimeout time.Duration

	// metrics is the optional observability registry. When nil every metric call
	// is a no-op; it is set through NewServiceReconciler's options (the worker
	// passes WithMetrics). It is never used to gate behavior.
	metrics *metrics.Registry

	// nodeID, when non-empty, is this worker's stable node identity (NODE_ID,
	// stamped as relay.node_id on every service container) and SCOPES every
	// container operation: Reconcile converges/adopts, Remove removes, and the
	// orphan sweep selects only containers carrying it. The worker wires
	// WithNodeID so nodes sharing a Docker daemon cannot change each
	// other's services; because NODE_ID is stable, a restart still adopts its
	// own converged containers. Empty (a directly-constructed reconciler or the
	// exported Reconcile) keeps the historical unscoped, same-host behavior.
	nodeID string

	// sourceApply tracks source-only in-place restarts (HOT_RELOAD). It is
	// constructed by NewServiceReconciler and shared across this reconciler's
	// passes so a restarted container is recognized as converged on later passes
	// despite its immutable relay.source label. A directly-constructed
	// ServiceReconciler leaves it nil, which disables in-place reload (source
	// changes then replace, the historical behavior).
	sourceApply *sourceApplyTracker

	// stateStore, when implemented by the Docker seam, persists the committed
	// generation marker in Docker-owned volume labels so an active replica can be
	// distinguished from an uncommitted healthcheck candidate across restarts.
	// stateMu guards only the committed cache; stateInitMu serializes the
	// one-time marker load without holding the cache lock across the Docker call.
	// Every marker operation is bounded by stateOpContext.
	stateStore  StateMarkerStore
	stateMu     sync.Mutex
	stateInitMu sync.Mutex
	committed   map[serviceStateKey]runtime.ServiceConvergenceState
	stateLoaded bool
	// stateLoadActive is true only while the one-time marker load is in flight.
	// stateLoadTouched and stateLoadDeletions record the cache mutations that
	// raced it, so the snapshot merge neither resurrects a deleted marker nor
	// discards a newer write (see ensureConvergenceStates). All three are guarded
	// by stateMu.
	stateLoadActive    bool
	stateLoadTouched   map[serviceStateKey]struct{}
	stateLoadDeletions []serviceStateLoadDeletion

	// retryMu guards retryFloor, the PROCESS-LOCAL copy of the retry windows this
	// reconciler installed (preparation and candidate failures). It is not durable
	// state: it exists so that a marker write which failed still holds the same
	// hard eligibility and attempt in this process, and another slot's
	// notification cannot bypass the backoff or drive a retry loop. A restart
	// loses it and re-derives conservatively from the (possibly absent) marker; a
	// replacement start is independently gated by the pre-start attempt write, so
	// neither path can start earlier than safe.
	retryMu    sync.Mutex
	retryFloor map[serviceRetryFloorKey]serviceRetryFloor

	mu sync.Mutex
}

// ServiceReconcilerOption configures optional ServiceReconciler dependencies.
// Options keep existing callers (and tests) source-compatible: a call without
// options behaves exactly as before, with observability disabled.
type ServiceReconcilerOption func(*ServiceReconciler)

// WithMetrics wires the metrics registry that records service reconcile
// outcomes and durations. A nil registry leaves observability disabled.
func WithMetrics(reg *metrics.Registry) ServiceReconcilerOption {
	return func(c *ServiceReconciler) { c.metrics = reg }
}

// WithNetworks wires the worker-global Docker network set (NETWORKS) every
// service container joins, exactly as it applies to execution containers. The
// worker passes config's resolved list; omitting the option (or passing nil)
// leaves services with no global networks. The list is copied so a caller
// cannot mutate the reconciler's configuration after construction, and it is
// startup configuration: changing it requires a worker restart.
func WithNetworks(networks []string) ServiceReconcilerOption {
	return func(c *ServiceReconciler) { c.networks = append([]string(nil), networks...) }
}

// WithNodeID scopes every service-container operation of this reconciler to
// one stable node identity: only containers whose relay.node_id label equals
// nodeID are reconciled, adopted, removed, or swept. The worker wires its
// NODE_ID (the same value Manager stamps on service containers), so two
// nodes sharing a Docker daemon never change each other's services while a
// restart of the SAME identity still adopts the previous process's running,
// converged containers. Omitting the option (or passing "") leaves the
// historical unscoped, same-host behavior for direct reconciler use.
func WithNodeID(nodeID string) ServiceReconcilerOption {
	return func(c *ServiceReconciler) { c.nodeID = nodeID }
}

// NewServiceReconciler builds a ServiceReconciler. traefik is the worker-level
// Traefik routing config (empty = routing not configured; required only for
// services whose template declares a host). reconcileTimeout is the worker's
// normal-service-operation budget, applied by Reconcile to every pre-resolution
// and post-resolution Docker operation (a non-positive value leaves them bounded
// only by the lifecycle context). Options are optional (e.g. WithMetrics for
// observability).
func NewServiceReconciler(
	docker Docker,
	secrets SecretResolver,
	traefik routing.TraefikConfig,
	log *slog.Logger,
	reconcileTimeout time.Duration,
	opts ...ServiceReconcilerOption,
) *ServiceReconciler {
	c := &ServiceReconciler{
		docker:           docker,
		secrets:          secrets,
		traefik:          traefik,
		log:              log,
		reconcileTimeout: reconcileTimeout,
		sourceApply:      newSourceApplyTracker(),
		committed:        map[serviceStateKey]runtime.ServiceConvergenceState{},
	}
	if store, ok := docker.(StateMarkerStore); ok {
		c.stateStore = store
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	return c
}

// ApplyResult converges fnName's services to tmpl+image and logs the outcome —
// Info when the pass changed state, was superseded after issuing work, or ended
// pending, Debug when it was a no-op verification pass (or a superseded pass
// that issued nothing), Warn when it failed or ended pending with an independent
// error. It is intentionally non-fatal: a service-convergence failure must not
// fail the app's reconcile.
//
// It distinguishes a terminal success from a pending pass (a backend accepted a
// replacement but has not terminally converged it) and a failure. A pending
// result ends the worker pass, releases the image lease, and retains the logical
// request — including its candidates and wake deadlines — for a later wake.
//
// PENDING WINS over an independent failure. A pass can converge most slots,
// leave another slot's candidate pending, and still record a real error (for
// example a marker write that could not be persisted for a different slot or
// service). Reporting that as a terminal failure would discard the retained
// candidates and deadlines and let the coordinator conclude a deployment that
// has not converged; instead the result is pending WITH Err set, so the failure
// is explicit in the result, the log, and the metric while convergence remains
// explicitly incomplete. A lifecycle cancellation is never reported as pending.
//
// The Info/Debug distinction means an unchanged app (periodic self-healing
// tick) does not log at Info; only converges that actually changed or failed do.
// A superseded pass reports outcome=superseded with its committed counts
// instead of a plain changed outcome, because a newer desired state owns the
// remaining convergence.
//
// ctx is the LIFECYCLE context, NOT a pre-bounded pass budget: Reconcile
// derives its own fresh per-operation bounds from it and the injected
// reconcileTimeout, so a caller must pass a lifecycle-rooted context (not a
// short deadline wrapped around the whole pass). image is the app's own
// prepared image, used only by `entrypoint` sources. Callers that run it
// concurrently must provide app-level serialization (ServiceCoordinator
// does this for the worker).
func (c *ServiceReconciler) ApplyResult(
	ctx context.Context, fnName string, tmpl *app.Template, image string,
	preparedEnv []string, reconcileStarted func(),
) ServicePassResult {
	replicas := 0
	for _, svc := range tmpl.Services {
		replicas += svc.Replicas
	}

	// Observe every pass exactly once, including the periodic no-op
	// verification: the duration covers the whole Reconcile call and the outcome
	// is the closed changed/unchanged/pending/error set. A nil registry is a
	// no-op. The app label is the existing bounded dimension.
	start := time.Now()
	if err := c.ensureConvergenceStates(ctx); err != nil {
		c.observeReconcile(fnName, false, false, err, 0)
		c.log.Warn("Service: convergence state unavailable",
			"app", fnName, "error", err)
		return ServicePassResult{Outcome: ServicePassFailed, Err: err}
	}
	pass, err := reconcileWithObserver(
		ctx, c.reconcileTimeout, c.docker, fnName, tmpl, image, preparedEnv,
		c.networks, c.secrets, c.traefik, c.log, c.nodeID, reconcileStarted,
		c.sourceApply, c,
	)
	duration := time.Since(start)
	c.observeReconcile(fnName, pass.changed, pass.pending, err, duration)
	if pass.pending && !requestCancelled(ctx, err) {
		// Pending is not a failure and must not mark readiness. It ends the
		// worker pass while the coordinator retains the logical request, its
		// candidates, and its deadlines, and resumes it on a notification, an
		// eligible observation, or a due deadline. An independent error is
		// carried alongside instead of replacing the pending contract.
		attrs := []any{
			"app", fnName,
			"services", len(tmpl.Services),
			"replicas", replicas,
			"started", pass.started,
			"restarted", pass.restarted,
			"duration", duration,
			"outcome", "pending",
		}
		if pass.stoppedKnown {
			attrs = append(attrs, "stopped", pass.stopped)
		}
		if !pass.retryNotBefore.IsZero() {
			attrs = append(attrs, "retry_not_before", pass.retryNotBefore)
		}
		if !pass.startupDeadline.IsZero() {
			attrs = append(attrs, "startup_deadline", pass.startupDeadline)
		}
		if err != nil {
			attrs = append(attrs, "error", err)
			c.log.Warn("Service: convergence pending with errors", attrs...)
		} else {
			c.log.Info("Service: convergence pending", attrs...)
		}
		return ServicePassResult{
			Outcome: ServicePassPending, Changed: pass.changed,
			Superseded:      pass.superseded,
			Err:             err,
			StartupDeadline: pass.startupDeadline,
			RetryNotBefore:  pass.retryNotBefore,
			Candidates:      append([]runtime.ServiceHealthCandidate(nil), pass.candidates...),
		}
	}
	if err != nil {
		if requestCancelled(ctx, err) {
			attrs := []any{
				"app", fnName,
				"services", len(tmpl.Services),
				"replicas", replicas,
				"started", pass.started,
				"restarted", pass.restarted,
				"duration", duration,
				"outcome", "cancelled",
				"error", err,
			}
			if pass.stoppedKnown {
				attrs = append(attrs, "stopped", pass.stopped)
			}
			c.log.Debug("Service: reconciliation cancelled", attrs...)
			return ServicePassResult{
				Outcome: ServicePassFailed, Changed: pass.changed,
				Superseded: pass.superseded, Err: err,
			}
		}
		// The error stays the operator-relevant outcome (and the metric), with
		// superseded carried alongside so an abandoned pass is still visible.
		attrs := []any{
			"app", fnName,
			"services", len(tmpl.Services),
			"replicas", replicas,
			"started", pass.started,
			"restarted", pass.restarted,
			"superseded", pass.superseded,
			"duration", duration,
			"outcome", "error",
			"error", err,
		}
		if pass.stoppedKnown {
			attrs = append(attrs, "stopped", pass.stopped)
		}
		c.log.Warn("Service: reconciled with errors", attrs...)
		return ServicePassResult{
			Outcome: ServicePassFailed, Changed: pass.changed,
			Superseded: pass.superseded, Err: err,
		}
	}
	if pass.superseded {
		// The request was replaced by a newer desired state, so the pass may have
		// committed some transitions while deliberately leaving the rest to the
		// newer pass. Report the actual outcome instead of a plain changed pass;
		// the level follows the same rule as changed, so a pass that issued no
		// corrective work stays at Debug rather than adding an Info line.
		logAt := c.log.Debug
		if pass.changed {
			logAt = c.log.Info
		}
		logAt("Service: superseded",
			"app", fnName,
			"services", len(tmpl.Services),
			"replicas", replicas,
			"started", pass.started,
			"restarted", pass.restarted,
			"stopped", pass.stopped,
			"duration", duration,
			"outcome", "superseded",
		)
		return ServicePassResult{
			Outcome: ServicePassSucceeded, Changed: pass.changed, Superseded: true,
		}
	}
	if pass.changed {
		c.log.Info("Service: reconciled",
			"app", fnName,
			"services", len(tmpl.Services),
			"replicas", replicas,
			"started", pass.started,
			"restarted", pass.restarted,
			"stopped", pass.stopped,
			"duration", duration,
			"outcome", "changed",
		)
		return ServicePassResult{Outcome: ServicePassSucceeded, Changed: true}
	}
	c.log.Debug("Service: unchanged",
		"app", fnName,
		"services", len(tmpl.Services),
		"replicas", replicas,
		"duration", duration,
		"outcome", "unchanged",
	)
	return ServicePassResult{Outcome: ServicePassSucceeded}
}

// observeReconcile records one service pass's outcome and duration. The outcome
// is error when the pass returned an error (even if it also changed some
// containers — the error is the operator-relevant signal), pending when a
// backend accepted work that has not terminally converged, changed when it
// converged container state, and unchanged for a fully-converged verification
// pass. It is nil-registry-safe.
func (c *ServiceReconciler) observeReconcile(
	fnName string, changed, pending bool, err error, d time.Duration,
) {
	if c.metrics == nil {
		return
	}
	outcome := metrics.ServiceOutcomeUnchanged
	switch {
	case err != nil:
		outcome = metrics.ServiceOutcomeError
	case pending:
		outcome = metrics.ServiceOutcomePending
	case changed:
		outcome = metrics.ServiceOutcomeChanged
	}
	c.metrics.IncLabels(metrics.MetricServiceReconciles, []metrics.Label{
		{Name: "app", Value: fnName},
		{Name: "outcome", Value: outcome},
	})
	c.metrics.ObserveDurationLabels(metrics.MetricServiceReconcileDuration, []metrics.Label{
		{Name: "app", Value: fnName},
	}, d)
}

// Remove stops and removes every service container belonging to fnName that
// THIS reconciler owns. It is the operation behind the coordinator's removal
// request; the app's images are retired only from that request's settlement
// callback, i.e. after this returns (or after the request was
// superseded/cancelled). An unscoped reconciler (no WithNodeID)
// delegates to RemoveAll and therefore removes every container of the app on
// the daemon, preserving the historical behavior; a worker-wired reconciler
// additionally requires relay.node_id == its NODE_ID, so removing an app
// never takes another worker's service containers down.
func (c *ServiceReconciler) Remove(ctx context.Context, fnName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sourceApply.forgetApp(fnName)
	deleteState := func() {
		if err := c.deleteServiceStates(ctx, fnName, ""); err != nil {
			c.log.Warn("Service: remove convergence state failed", "app", fnName, "error", err)
		}
	}
	if c.nodeID == "" {
		RemoveAll(ctx, c.docker, fnName, c.log)
		deleteState()
		return
	}
	c.removeAppForWorkerLocked(ctx, fnName)
	deleteState()
}

// removeAppForWorkerLocked removes this worker's service containers for fnName.
// It is the node-identity-scoped counterpart of RemoveAll and must be called
// with c.mu held. It uses the same two Docker operations
// (ServiceContainerList then StopServiceContainers) RemoveAppServiceContainers
// uses, adding the relay.node_id scope. It never touches another worker's
// containers for the same app: those are that worker's to converge or remove.
func (c *ServiceReconciler) removeAppForWorkerLocked(
	ctx context.Context, fnName string,
) {
	containers, err := c.docker.ServiceContainerList(ctx)
	if err != nil {
		c.log.Warn("Service: remove app containers failed",
			"app", fnName, "error", err)
		return
	}
	var own []runtime.ServiceContainer
	for _, ctr := range containers {
		if ctr.App == fnName && ctr.NodeID == c.nodeID {
			own = append(own, ctr)
		}
	}
	if len(own) == 0 {
		return
	}
	if err := c.docker.StopServiceContainers(ctx, own); err != nil {
		c.log.Warn("Service: remove app containers failed",
			"app", fnName, "count", len(own), "error", err)
		return
	}
	c.log.Info("Service: removed app containers", "app", fnName, "count", len(own))
}

// SweepOrphans stops and removes every service container whose app is not
// in liveApps (an app removed while Relay was down, or stale containers
// from a previous boot on this host). A worker-wired reconciler additionally
// requires relay.node_id == its NODE_ID, so it never sweeps another worker's
// service containers; an unscoped reconciler keeps the historical app-only
// predicate (the runtime's documented same-host-restart limitation).
// Called once at startup after the per-app Applys.
//
// It is the convenience form of the split cleanup (DiscoverOrphanContainers
// then RemoveOrphanContainers) under ONE lock hold, preserving the historical
// list-and-stop atomicity. Callers that must prove ownership BETWEEN discovery
// and removal (the worker's startup housekeeping) call the two halves directly,
// so the ownership proof runs after the candidate snapshot and before any
// removal.
func (c *ServiceReconciler) SweepOrphans(ctx context.Context, liveApps map[string]bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.PruneServiceConvergenceStates(ctx, liveApps)
	stale, err := c.discoverOrphansLocked(ctx, liveApps)
	if err != nil {
		// discoverOrphansLocked already logged the failure.
		return
	}
	if len(stale) == 0 {
		return
	}
	// removeOrphansLocked already logged the outcome (failure or success).
	_ = c.removeOrphansLocked(ctx, stale)
}

// DiscoverOrphanContainers returns every discovered service container whose app
// is not in liveApps (an app removed while Relay was down, or a stale container
// from a previous boot on this host). It is the DISCOVERY half of SweepOrphans,
// split out so the caller can snapshot the candidates, prove ownership on its
// pinned lock, and only then remove the captured containers
// (RemoveOrphanContainers) — never re-listing after the proof, which is what
// keeps a replacement owner's containers safe. It stops nothing.
//
// The app-live predicate is the same one SweepOrphans has always used. A
// worker-wired reconciler additionally requires relay.node_id == its
// NODE_ID, so the startup sweep never captures another worker's service
// containers; an unscoped reconciler keeps the historical app-only predicate
// (the runtime's documented same-host-restart limitation). A list failure is
// returned unchanged (and logged) so the caller can surface it with its own
// context.
func (c *ServiceReconciler) DiscoverOrphanContainers(
	ctx context.Context, liveApps map[string]bool) ([]runtime.ServiceContainer, error,
) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.discoverOrphansLocked(ctx, liveApps)
}

// discoverOrphansLocked selects the containers SweepOrphans would remove: every
// discovered service container whose app is not in liveApps, restricted to this
// node's identity when the reconciler is worker-wired. It must be called
// with c.mu held (see the locked wrappers DiscoverOrphanContainers and
// SweepOrphans). A list failure is logged ("Service: orphan sweep list failed")
// and returned unchanged.
func (c *ServiceReconciler) discoverOrphansLocked(
	ctx context.Context, liveApps map[string]bool,
) ([]runtime.ServiceContainer, error) {
	containers, err := c.docker.ServiceContainerList(ctx)
	if err != nil {
		c.log.Warn("Service: orphan sweep list failed", "error", err)
		return nil, err
	}
	var stale []runtime.ServiceContainer
	for _, ct := range containers {
		if c.nodeID != "" && ct.NodeID != c.nodeID {
			// Another worker's service container: another identity's property,
			// never swept here.
			continue
		}
		if !liveApps[ct.App] {
			stale = append(stale, ct)
		}
	}
	return stale, nil
}

// RemoveOrphanContainers stops and removes exactly the given service containers.
// It is the REMOVAL half of SweepOrphans, exported for the worker's snapshot →
// ownership-proof → remove ordering, and by construction only ever touches the
// containers it is given (the IDs captured before the proof).
func (c *ServiceReconciler) RemoveOrphanContainers(
	ctx context.Context, stale []runtime.ServiceContainer,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.removeOrphansLocked(ctx, stale)
}

// removeOrphansLocked stops and removes exactly the given containers and must be
// called with c.mu held (see RemoveOrphanContainers and SweepOrphans). It never
// re-lists the daemon: the caller passes the containers captured by
// discoverOrphansLocked, so a replacement container created AFTER that snapshot
// is never stopped (its id is not in the captured set). A stop failure is logged
// ("Service: orphan sweep stop failed") and returned unchanged; a success is
// logged ("Service: swept orphan containers") with the removed count. An empty
// set is a no-op.
func (c *ServiceReconciler) removeOrphansLocked(
	ctx context.Context, stale []runtime.ServiceContainer,
) error {
	if len(stale) == 0 {
		return nil
	}
	if err := c.docker.StopServiceContainers(ctx, stale); err != nil {
		c.log.Warn("Service: orphan sweep stop failed", "count", len(stale), "error", err)
		return err
	}
	c.log.Info("Service: swept orphan containers",
		"count", len(stale),
	)
	return nil
}

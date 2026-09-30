// The service reconciler (services.go) reconciles a function's persistent
// service containers to its template. At startup and on every reconcile of the
// owning function, it lists the daemon's service containers, classifies each
// against the desired configuration, and converges the running set to the
// template's declared services and replica counts.
//
// Replacement is ZERO-DOWNTIME-BY-CONSTRUCTION: for every desired replica slot
// the reconciler starts the replacement and requires StartService to confirm it
// RUNNING before stopping the superseded generation for that same logical slot.
// A failed create/start/inspect leaves the old generation running and the slot
// keeps serving; the next reconcile retries. Old generations, excess replicas,
// and removed services are stopped only after the desired slots have converged,
// so a service is never taken to zero replicas by a failed replacement.
//
// The component is deliberately small and the owning layer (worker wiring) is
// what decides when to invoke it. Containers owned by OTHER functions are never
// touched here: each function's reconcile owns only its own containers, and
// SweepOrphans is the single cross-function pass (called once at startup with
// the set of live function names) that removes containers whose function no
// longer exists. ShutdownCleanup is the graceful-shutdown counterpart: it
// removes service containers owned by this worker only; startup convergence
// remains the crash-recovery path when shutdown cleanup did not execute.
//
// Identity is the configured source descriptor (function.Service.SourceRef) and
// the replica slot is relay.replica; both are label-derived and are the only
// grouping keys. An identity change is therefore indistinguishable from a
// removal plus an addition at this layer (see Reconcile's comment on that
// edge), which is why removed-service cleanup is deferred until after every
// desired service has converged.
//
// Ordering guarantees relied on by the worker:
//   - Reconcile is idempotent: when already converged it lists containers once
//     and mutates nothing, so calling it on every periodic reconcile tick (as a
//     cheap no-op) is safe and gives crash replacement within the default 30s
//     cadence without a separate services-only loop.
//   - RemoveAll (via the reconciler's RemoveServices hook) must run BEFORE the
//     function's images are retired, because running service containers still
//     reference those images.
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
package reconciler

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/moby/moby/api/types/container"

	"relay/internal/function"
	"relay/internal/observability/metrics"
	"relay/internal/routing"
	"relay/internal/runtime"
)

// Docker is the service-container subset of the runtime Manager. The Manager
// satisfies it; the structural assertion lives in the worker wiring (worker.go),
// not here.
type Docker interface {
	// ResolveServiceImage resolves one service's configured source to the image
	// a container should run: the function image for an `entrypoint` source, or
	// an inspected/pulled external image for an `image` source. It is resolved
	// BEFORE any container action, so a pull failure preserves healthy
	// containers.
	ResolveServiceImage(
		ctx context.Context,
		fnName string,
		tmpl *function.Template,
		svc function.Service,
		functionImage string,
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
	// RemoveFunctionServiceContainers stops and removes every service container
	// belonging to one function, returning how many were removed.
	RemoveFunctionServiceContainers(ctx context.Context, fnName string) (int, error)
	// NetworkExists reports whether a Docker network exists on the daemon. The
	// service reconciler uses it to refuse routed services whose routing
	// network is missing; Relay never creates networks.
	NetworkExists(ctx context.Context, network string) (bool, error)
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
	tmpl *function.Template,
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
			return nil, fmt.Errorf("function references secret %q but no secret provider is configured", sb.Ref)
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
// for its function. Reconcile consults it at the commit boundary — immediately
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
// Identity edge — an identity CHANGE is a removal plus an addition. A service's
// identity is its SourceRef (entrypoint file or image reference), the sole
// grouping key; the template has no separate stable service name. Changing a
// source descriptor (e.g. an external image tag, or an entrypoint path) makes
// the old identity absent from the desired set and the new identity appear, so
// the old containers are treated as removed. The reconciler deliberately stops
// removed services LAST (after desired starts), which gives the new identity and
// the old one a brief overlap, but it cannot pair them: it has no evidence the
// two identities are "the same service". Reliable cross-SourceRef pairing would
// require a stable service identity (an index or explicit name) that the
// template model does not have, so it is out of scope here.
//
// Container ownership is always label-derived. A container belongs to fnName
// when its Function == fnName; containers of other functions are never touched.
// The ownership predicate is NOT hostname-scoped — services must be
// reconcilable across worker restarts on the same host (same as the runtime).
func Reconcile(
	ctx context.Context,
	reconcileTimeout time.Duration,
	docker Docker,
	fnName string,
	tmpl *function.Template,
	functionImage string,
	preparedEnv []string,
	serviceNetworks []string,
	secrets SecretResolver,
	traefik routing.TraefikConfig,
	log *slog.Logger,
) (bool, error) {
	return reconcileWithObserver(
		ctx, reconcileTimeout, docker, fnName, tmpl, functionImage, preparedEnv,
		serviceNetworks, secrets, traefik, log, nil,
	)
}

func reconcileWithObserver(
	ctx context.Context,
	reconcileTimeout time.Duration,
	docker Docker,
	fnName string,
	tmpl *function.Template,
	functionImage string,
	preparedEnv []string,
	serviceNetworks []string,
	secrets SecretResolver,
	traefik routing.TraefikConfig,
	log *slog.Logger,
	reconcileStarted func(),
) (bool, error) {
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
		return false, fmt.Errorf("service: list containers: %w", err)
	}
	// changed reports whether any convergence action (a stop or a start
	// attempt) was issued during this pass. It starts false and is set by the
	// removed-service cleanup below and by the classify/start work further down.
	changed := false

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
		if reconcileStarted == nil || reconcileNotified || !corrective {
			return
		}
		reconcileNotified = true
		reconcileStarted()
	}

	var firstErr error
	fail := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

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
	// could not be confirmed running). Under SourceRef grouping an identity
	// change is a removal plus an addition, so a removed identity's running
	// containers may be the ONLY usable generation for the service the new
	// identity replaces; when the desired set did not converge they are preserved
	// rather than stopped (see the removal cleanup below).
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

	desired := make(map[string]function.Service, len(tmpl.Services))
	for _, svc := range tmpl.Services {
		desired[svc.SourceRef()] = svc
	}

	// Classify this function's containers by service. Containers whose service
	// is no longer in the template (removed service) are collected and stopped
	// only AFTER every desired service has converged below: start-before-stop
	// ordering means a service whose identity changed (an identity change is
	// indistinguishable from a removal plus an addition under SourceRef
	// grouping) does not have its old generation torn down before the new
	// service's replicas are running.
	var removed []runtime.ServiceContainer
	byService := make(map[string][]runtime.ServiceContainer)
	for _, ctr := range containers {
		if ctr.Function != fnName {
			// Another function owns this container; its reconcile handles it.
			continue
		}
		if _, ok := desired[ctr.Identity]; !ok {
			removed = append(removed, ctr)
			continue
		}
		byService[ctr.Identity] = append(byService[ctr.Identity], ctr)
	}

	for _, svc := range tmpl.Services {
		identity := svc.SourceRef()
		existing := byService[identity]

		// Supersession check at the top of each service: if a newer desired
		// state replaced this request while a previous service was converging,
		// do no further container work for the remaining services. Every
		// earlier service already committed atomically (its old generations
		// stopped only after its replacements were confirmed), so abandoning
		// here cannot leave a service with no generation.
		if abortPass || superseded() {
			abortPass = true
			break
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
		// for nothing.
		var routeLabels map[string]string
		routeNetwork := ""
		if svc.Host != "" {
			if err := traefik.Validate(); err != nil {
				preCancel()
				err := fmt.Errorf("service %q: %w", identity, err)
				fail(err)
				desiredFailed = true
				log.Warn("Service: routing validation failed", "service", identity, "error", err)
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
				err := fmt.Errorf("service %q: %w", identity, err)
				fail(err)
				desiredFailed = true
				log.Warn("Service: routing validation failed", "service", identity, "error", err)
				continue
			}
			// The routing network (e.g. the Traefik network) is infrastructure
			// owned outside Relay; Relay verifies it exists and refuses to
			// start routed containers otherwise — it never creates it.
			ok, err := docker.NetworkExists(preCtx, traefik.Network)
			if err != nil {
				preCancel()
				err := fmt.Errorf("service %q: check routing network: %w", identity, err)
				fail(err)
				desiredFailed = true
				log.Warn("Service: routing validation failed", "service", identity, "error", err)
				continue
			}
			if !ok {
				preCancel()
				err := fmt.Errorf("service %q: %w", identity, routing.MissingNetwork(traefik.Network))
				fail(err)
				desiredFailed = true
				log.Warn("Service: routing validation failed", "service", identity, "error", err)
				continue
			}
			routeLabels = routing.TraefikLabels(fnName, identity, svc.Host, svc.Path, svc.Port, traefik)
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
					"function", fnName,
					"service", identity,
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
		resolved, err := docker.ResolveServiceImage(preCtx, fnName, tmpl, svc, functionImage)
		preCancel()
		if err != nil {
			fail(fmt.Errorf("service %q: %w", identity, err))
			desiredFailed = true
			log.Warn("Service: cannot resolve source; keeping existing containers",
				"service", identity, "error", err)
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
			fail(fmt.Errorf("service %q: %w", identity, err))
			desiredFailed = true
			log.Warn("Service: cannot start replicas", "service", identity, "error", err)
			continue
		}
		// envHash is the desired effective environment's content hash. It is
		// compared against each container's relay.env_hash label below: the
		// environment is the one piece of configuration the image reference
		// cannot carry (a template env change on an `image` source leaves the
		// reference unchanged; a rotated secret value never changes the source
		// fingerprint), so without this comparison a stale container would keep
		// serving its old env/secrets indefinitely. The hash is order-sensitive
		// and covers the exact slice StartService applies.
		envHash := runtime.EnvHash(env)
		// resources are the function's effective per-container limits; their
		// fingerprint is compared against each container's relay.resources label
		// below, so a resource-only template change replaces the running service
		// even though the image reference (and image fingerprint) are unchanged.
		resources := tmpl.ResourceLimits()
		resourceHash := resources.Fingerprint()
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
		// healthy (running) and currently configured correctly (image, image
		// content, port, effective environment, and Docker networks all match
		// the desired values) and carries a real replica label. Anything else —
		// exited/dead/removing, a changed image, a moved external tag (image
		// content changed), a changed port, a changed env/secret (env hash
		// mismatch), a changed network set, or an unlabeled legacy container
		// (Replica == -1) — is a non-converged generation that a converged
		// replacement supersedes. In addition, the container's labels must
		// match the desired routing label set exactly: a changed host/path/port
		// leaves stale Traefik labels pointing traffic at whatever the old
		// container served, so the container is replaced.
		converged := func(ctr runtime.ServiceContainer) bool {
			return ctr.State == container.StateRunning &&
				ctr.Image == resolved.Ref &&
				ctr.ImageID == resolved.ID &&
				ctr.Port == svc.Port &&
				ctr.EnvHash == envHash &&
				ctr.Resources == resourceHash &&
				ctr.Networks == desiredNetworks &&
				ctr.Replica >= 0 &&
				routingLabelsMatch(routeLabels, ctr.Labels)
		}

		// The replacement spec is built once per service and reused for every
		// deficit slot below.
		newSpec := func() runtime.ServiceSpec {
			return runtime.ServiceSpec{
				Function:  fnName,
				Identity:  identity,
				Port:      svc.Port,
				Image:     resolved.Ref,
				ImageID:   resolved.ID,
				Entry:     resolved.Entry,
				Env:       env,
				Resources: resources,
				Labels:    routeLabels,
				Networks:  desiredNetworkSet,
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
		for slot := 0; slot < svc.Replicas; slot++ {
			group := slotGroups[slot]
			sort.Slice(group, func(i, j int) bool { return group[i].ID < group[j].ID })
			var matches, olds []runtime.ServiceContainer
			for _, ctr := range group {
				if converged(ctr) {
					matches = append(matches, ctr)
				} else {
					olds = append(olds, ctr)
				}
			}

			if len(matches) > 0 {
				// A converged replica already holds the slot: keep the lowest-ID
				// one (stable, list-order independent) and clean up every other
				// matching duplicate plus every superseded generation in the
				// slot. The desired replica is already running, so stopping the
				// rest cannot take the slot down.
				stale = append(stale, matches[1:]...)
				stale = append(stale, olds...)
				continue
			}

			// No converged replica holds the slot. The lowest-ID usable
			// (running or restarting) non-converged generation in the slot is
			// the fallback: the replacement is started first, and the fallback
			// is stopped only once StartService has confirmed the replacement
			// is running.
			var fallback *runtime.ServiceContainer
			for i := range olds {
				if usableServiceContainer(olds[i]) {
					fallback = &olds[i]
					break
				}
			}

			changed = true
			corrective = true
			notifyReconcile()
			startedID, err := docker.StartService(postCtx, newSpec(), slot)
			if err != nil {
				fail(fmt.Errorf("service %q replica %d: %w", identity, slot, err))
				desiredFailed = true
				if fallback != nil {
					// The replacement failed: preserve the slot's usable old
					// generation(s) so the service keeps serving, and retry on
					// the next reconcile. Leaving them out of stale is the
					// suppression: they are not stopped this pass.
					log.Warn("Service: replacement failed; keeping old replica",
						"function", fnName, "service", identity, "replica", slot, "error", err)
				} else {
					// No old fallback for this slot: retain unavailable
					// semantics and retry on the next reconcile.
					log.Warn("Service: replica unavailable",
						"function", fnName, "service", identity, "replica", slot, "error", err)
				}
				// Usable olds stay as fallbacks; a non-usable old can never
				// serve, so it is cleaned up even when the replacement failed.
				for i := range olds {
					if !usableServiceContainer(olds[i]) {
						stale = append(stale, olds[i])
					}
				}
				continue
			}
			// StartService confirmed the replacement is running: the old
			// generations in this slot are now safe to stop.
			started++
			// A replacement that superseded a usable old generation is
			// uncommitted until this service's stale stop runs. Record it so a
			// supersession discovered at that commit boundary can remove this
			// pass's own provisional container while preserving the old one; a
			// slot with NO usable fallback is deliberately not recorded (there
			// the replacement is the only running generation).
			if fallback != nil && startedID != "" {
				provisional = append(provisional, runtime.ServiceContainer{
					ID: startedID, Function: fnName, Identity: identity,
					State: container.StateRunning,
				})
			}
			stale = append(stale, olds...)
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

		// Safety net: never take a desired service from at least one running
		// replica to none because every running generation would be stopped.
		// Survivors are the replacements confirmed running this pass plus every
		// existing usable (running/restarting) container not scheduled for a
		// stop (a kept converged replica or an unclaimed generation). If that
		// count is zero, keep the lowest-ID usable stale container as a fallback
		// and retry next pass. It is deterministic and a no-op whenever a
		// replacement succeeded or any converged replica remains.
		surviving := started
		for _, ctr := range existing {
			if usableServiceContainer(ctr) && !containsStaleID(stale, ctr.ID) {
				surviving++
			}
		}
		if svc.Replicas > 0 && surviving == 0 {
			if keep, ok := lowestUsableStale(stale); ok {
				stale = removeStaleID(stale, keep.ID)
				log.Warn("Service: no replacement could be confirmed; keeping a running replica",
					"function", fnName, "service", identity, "replica", keep.Replica)
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
				"function", fnName, "service", identity)
			cleanupProvisional(bounded, docker, fnName, identity, provisional, log)
			provisional = nil
			abortPass = true
			postCancel()
			break
		}

		// Stop the stale/excess generations now that the desired slots are
		// running.
		if len(stale) > 0 {
			changed = true
			corrective = true
			notifyReconcile()
			if err := docker.StopServiceContainers(postCtx, stale); err != nil {
				fail(fmt.Errorf("service %q stale: %w", identity, err))
			}
		}
		// The service committed: its provisional replacements are now the
		// running generation and are no longer removable by a later supersession.
		provisional = nil
		postCancel()
	}

	// Removed services are stopped LAST: their containers are no longer desired,
	// but deferring the stop until every desired service has converged means an
	// identity change (which is indistinguishable from a removal plus an
	// addition under SourceRef grouping) cannot have its old generation torn
	// down before the new service's replicas are running. Removed-service and
	// removed-function cleanup still removes every generation.
	//
	// They are skipped entirely when the pass was superseded or a desired service
	// did not converge. Under SourceRef grouping a changed source is a removal
	// plus an addition, so a removed identity's running containers can be the
	// ONLY usable generation for the service the new identity replaces; stopping
	// them then would destroy the last usable generation. A subsequent reconcile
	// (the coalesced newer desired state, or the next periodic pass) retries the
	// removal once the desired set converges.
	if abortPass || superseded() {
		log.Info("Service: pass superseded; preserving removed services",
			"function", fnName, "removed", len(removed))
	} else if len(removed) > 0 && desiredFailed {
		log.Warn("Service: desired services did not converge; preserving removed services",
			"function", fnName, "removed", len(removed))
	} else if len(removed) > 0 {
		sort.Slice(removed, func(i, j int) bool { return removed[i].ID < removed[j].ID })
		changed = true
		corrective = true
		notifyReconcile()
		stopCtx, stopCancel := ctx, func() {}
		if reconcileTimeout > 0 {
			stopCtx, stopCancel = context.WithTimeout(ctx, reconcileTimeout)
		}
		if err := docker.StopServiceContainers(stopCtx, removed); err != nil {
			fail(fmt.Errorf("service removed: %w", err))
		}
		stopCancel()
	}

	return changed, firstErr
}

// cleanupProvisional removes the replacement containers a single pass started
// but never committed (a slot whose later cleanup found the request superseded).
// It removes ONLY the listed ids — this pass's own creations — so it can never
// stop a running old generation. Failures are logged, never fatal: a leftover
// provisional container is a non-converged generation the next reconcile
// replaces, exactly like any other stale candidate.
func cleanupProvisional(
	bounded func() (context.Context, context.CancelFunc),
	docker Docker,
	fnName, identity string,
	provisional []runtime.ServiceContainer,
	log *slog.Logger,
) {
	if len(provisional) == 0 {
		return
	}
	stopCtx, stopCancel := bounded()
	defer stopCancel()
	if err := docker.StopServiceContainers(stopCtx, provisional); err != nil {
		log.Warn("Service: remove provisional replacement failed",
			"function", fnName, "service", identity, "count", len(provisional), "error", err)
	}
}

// usableServiceContainer reports whether a non-converged container may still be
// serving and can therefore back a slot while a replacement is retried: running,
// or restarting (Docker is bringing it back). Exited/dead/removing/created
// containers are not usable and are safe to clean even when a replacement
// failed.
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

// lowestUsableStale returns the lowest-ID usable (running or restarting)
// container in stale and whether one was found. It is the deterministic pick for
// the last-resort fallback guard.
func lowestUsableStale(stale []runtime.ServiceContainer) (runtime.ServiceContainer, bool) {
	var best runtime.ServiceContainer
	found := false
	for i := range stale {
		st := stale[i].State
		if st != container.StateRunning && st != container.StateRestarting {
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
// delegating to the Docker implementation's RemoveFunctionServiceContainers.
// Used when a function is removed: its service containers must be stopped before
// its images are retired (see the reconciler's RemoveServices hook ordering).
func RemoveAll(ctx context.Context, docker Docker, fnName string, log *slog.Logger) {
	n, err := docker.RemoveFunctionServiceContainers(ctx, fnName)
	if err != nil {
		log.Warn("Service: remove function containers failed", "function", fnName, "error", err)
		return
	}
	if n > 0 {
		log.Info("Service: removed function containers", "function", fnName, "count", n)
	}
}

// ShutdownCleanup stops and removes every persistent service container owned
// by THIS worker: relay.hostname == hostname (the consumer identity). Graceful
// Relay shutdown removes this worker's persistent service containers; crash
// recovery remains handled by startup reconciliation.
//
// It exists as its own smallest operation because RemoveAll is function-scoped
// and SweepOrphans is cross-function (neither is hostname-scoped by design);
// here ownership is hostname-scoped only. The returned count is the number of
// this worker's containers selected for removal.
//
// A container with an empty Hostname is NOT claimed by any worker: the hostname
// is unmatchable, so ownership is unknowable — startup reconciliation handles
// such strays. As a defensive guard, an empty hostname argument selects nothing.
func (c *ServiceReconciler) ShutdownCleanup(ctx context.Context, hostname string) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if hostname == "" {
		return 0, nil
	}
	containers, err := c.docker.ServiceContainerList(ctx)
	if err != nil {
		logErr := fmt.Errorf("service: list containers: %w", err)
		c.log.Warn("Service: shutdown cleanup failed", "error", logErr)
		return 0, logErr
	}
	var own []runtime.ServiceContainer
	for _, ct := range containers {
		// Strict equality: a container without a hostname label is not claimed
		// by any worker's shutdown (ownership unknowable; startup reconciliation
		// handles strays).
		if ct.Hostname == hostname {
			own = append(own, ct)
		}
	}
	if len(own) == 0 {
		c.log.Debug("Service: shutdown cleanup: nothing to clean", "hostname", hostname)
		return 0, nil
	}
	err = c.docker.StopServiceContainers(ctx, own)
	if err != nil {
		// StopServiceContainers keeps the partial progress (everything it
		// could stop/remove is gone); the error is still returned to the
		// caller — cleanup is never fatal to shutdown itself.
		c.log.Warn("Service: shutdown cleanup failed", "error", err)
	} else {
		c.log.Info("Service: shutdown cleanup complete", "containers", len(own))
	}
	return len(own), err
}

// ServiceReconciler ties Reconcile, RemoveAll, SweepOrphans, and ShutdownCleanup
// to a single
// Docker implementation, secret resolver, and logger. Function-scoped Apply
// calls are serialized by ServiceCoordinator in the worker; the mutex remains
// around cross-function cleanup operations.
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
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	return c
}

// Apply converges fnName's services to tmpl+image: it runs Reconcile and logs
// the outcome — Info when the pass changed state, Debug when it was a no-op
// verification pass, Warn when it errored. It is intentionally non-fatal: a
// service-convergence failure must not fail the function's reconcile. The
// Info/Debug distinction means an unchanged function (periodic self-healing
// tick) does not log at Info; only converges that actually changed or failed do.
//
// ctx is the LIFECYCLE context, NOT a pre-bounded pass budget: Reconcile
// derives its own fresh per-operation bounds from it and the injected
// reconcileTimeout, so a caller must pass a lifecycle-rooted context (not a
// short deadline wrapped around the whole pass). image is the function's own
// prepared image, used only by `entrypoint` sources. Callers that run Apply
// concurrently must provide function-level serialization (ServiceCoordinator
// does this for the worker).
func (c *ServiceReconciler) Apply(
	ctx context.Context,
	fnName string,
	tmpl *function.Template,
	image string,
	preparedEnv []string,
) error {
	return c.apply(ctx, fnName, tmpl, image, preparedEnv, nil)
}

func (c *ServiceReconciler) ApplyWithStatus(
	ctx context.Context, fnName string, tmpl *function.Template, image string,
	preparedEnv []string, reconcileStarted func(),
) error {
	return c.apply(ctx, fnName, tmpl, image, preparedEnv, reconcileStarted)
}

func (c *ServiceReconciler) apply(
	ctx context.Context, fnName string, tmpl *function.Template, image string,
	preparedEnv []string, reconcileStarted func(),
) error {
	replicas := 0
	for _, svc := range tmpl.Services {
		replicas += svc.Replicas
	}

	// Observe every pass exactly once, including the periodic no-op
	// verification: the duration covers the whole Reconcile call and the outcome
	// is the closed changed/unchanged/error set. A nil registry is a no-op. The
	// function label is the existing bounded dimension.
	start := time.Now()
	changed, err := reconcileWithObserver(
		ctx, c.reconcileTimeout, c.docker, fnName, tmpl, image, preparedEnv,
		c.networks, c.secrets, c.traefik, c.log, reconcileStarted,
	)
	c.observeReconcile(fnName, changed, err, time.Since(start))
	if err != nil {
		c.log.Warn("Service: reconciled with errors",
			"function", fnName,
			"replicas", replicas,
			"error", err,
		)
		return err
	}
	if changed {
		c.log.Info("Service: reconciled",
			"function", fnName,
			"services", len(tmpl.Services),
			"replicas", replicas,
		)
		return nil
	}
	c.log.Debug("Service: unchanged",
		"function", fnName,
		"services", len(tmpl.Services),
		"replicas", replicas,
	)
	return nil
}

// observeReconcile records one service pass's outcome and duration. The outcome
// is error when the pass returned an error (even if it also changed some
// containers — the error is the operator-relevant signal), changed when it
// converged container state, and unchanged for a fully-converged verification
// pass. It is nil-registry-safe.
func (c *ServiceReconciler) observeReconcile(
	fnName string, changed bool, err error, d time.Duration,
) {
	if c.metrics == nil {
		return
	}
	outcome := metrics.ServiceOutcomeUnchanged
	switch {
	case err != nil:
		outcome = metrics.ServiceOutcomeError
	case changed:
		outcome = metrics.ServiceOutcomeChanged
	}
	c.metrics.IncLabels(metrics.MetricServiceReconciles, []metrics.Label{
		{Name: "function", Value: fnName},
		{Name: "outcome", Value: outcome},
	})
	c.metrics.ObserveDurationLabels(metrics.MetricServiceReconcileDuration, []metrics.Label{
		{Name: "function", Value: fnName},
	}, d)
}

// Remove stops and removes every service container belonging to fnName. Called
// by the reconciler's RemoveServices hook BEFORE the function's images are
// retired.
func (c *ServiceReconciler) Remove(ctx context.Context, fnName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	RemoveAll(ctx, c.docker, fnName, c.log)
}

// SweepOrphans stops and removes every service container whose function is not
// in liveFunctions (a function removed while Relay was down, or stale containers
// from a previous boot on this host). hostname is deliberately NOT part of the
// predicate, matching the runtime's documented same-host-restart limitation.
// Called once at startup after the per-function Applys.
func (c *ServiceReconciler) SweepOrphans(ctx context.Context, liveFunctions map[string]bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	containers, err := c.docker.ServiceContainerList(ctx)
	if err != nil {
		c.log.Warn("Service: orphan sweep list failed", "error", err)
		return
	}
	var stale []runtime.ServiceContainer
	for _, ct := range containers {
		if !liveFunctions[ct.Function] {
			stale = append(stale, ct)
		}
	}
	if len(stale) == 0 {
		return
	}
	if err := c.docker.StopServiceContainers(ctx, stale); err != nil {
		c.log.Warn("Service: orphan sweep stop failed", "count", len(stale), "error", err)
		return
	}
	c.log.Info("Service: swept orphan containers",
		"count", len(stale),
	)
}

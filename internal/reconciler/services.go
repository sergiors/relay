// The service reconciler (services.go) reconciles a function's persistent
// service containers to its template. At startup and on every reconcile of the
// owning function, it lists the daemon's service containers, classifies each as
// desired/stale, stops the stale (and any removed services' and excess
// replicas') containers, and starts whatever replicas are missing — converging
// the running set to the template's declared services and replica counts.
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
// Ordering guarantees relied on by the worker:
//   - Reconcile is idempotent: when already converged it lists containers once
//     and mutates nothing, so calling it on every periodic reconcile tick (as a
//     cheap no-op) is safe and gives crash replacement within the default 30s
//     cadence without a separate services-only loop.
//   - Removed-service cleanup runs BEFORE the template-networks pre-flight, so
//     a missing network (which preserves the desired services' healthy
//     containers) never prevents a service removed from the template from being
//     stopped. The pre-flight gates only the start/replacement of DESIRED
//     services.
//   - RemoveAll (via the reconciler's RemoveServices hook) must run BEFORE the
//     function's images are retired, because running service containers still
//     reference those images.
//   - Routing (Traefik) for routed services (a service declaring a host) is
//     validated before any container action for that service: the routing
//     config must be present, the per-service effective host (including any
//     TRAEFIK_HOST_OVERRIDE mapping) must be a valid hostname, and the routing
//     network must exist (Relay never creates it); a routed service failing
//     routing validation is reported and skipped, not half-reconciled.
//   - Reconcile takes the LIFECYCLE context and derives a FRESH normal-operation
//     bound (the injected reconcileTimeout) for each Docker operation itself. The
//     pre-build phase (routing validation and source resolution) and the
//     post-build phase (env resolution, stale stops, replica starts) run on
//     separate bounds, so a long Dockerfile build — bounded by the runtime's
//     lifecycle-rooted buildTimeout, never by this budget — cannot consume the
//     post-build deadline. A caller must never wrap the whole pass in one short
//     deadline.
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
	"relay/internal/routing"
	"relay/internal/runtime"
)

// Docker is the service-container subset of the runtime Manager. The Manager
// satisfies it; the structural assertion lives in the worker wiring (worker.go),
// not here.
type Docker interface {
	// ResolveServiceImage resolves one service's configured source to the image
	// a container should run: the function image for an `entrypoint` source, a
	// content-addressed Relay-built image for a `build` source, or an inspected/
	// pulled external image for an `image` source. It is resolved BEFORE any
	// container action, so a build or pull failure preserves healthy containers.
	ResolveServiceImage(
		ctx context.Context,
		fnName, fnDir string,
		tmpl *function.Template,
		svc function.Service,
		functionImage string,
	) (runtime.ServiceImage, error)
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
	// VerifyNetworks checks that every named network exists, returning the first
	// missing one (ok=false). It exists so the start/replacement of a template's
	// DESIRED services is refused when one of its top-level `networks` is
	// missing; removed-service cleanup still runs (it does not depend on the
	// networks). Relay never creates networks.
	VerifyNetworks(ctx context.Context, networks []string) (missing string, ok bool, err error)
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

// Reconcile converges the set of fnName's running service containers to
// tmpl.Services (with the freshly prepared image). It is a best-effort pass:
// per-operation failures are logged by the caller-facing style and the first
// error is returned (at least one error surfaces when anything failed), so a
// transient daemon error on one container does not abort convergence of the rest.
//
// ctx is the LIFECYCLE context, NOT a pre-bounded reconcile budget. Reconcile
// derives every normal-operation bound itself from ctx and reconcileTimeout, so a
// caller can never accidentally wrap a whole pass — a long Dockerfile build
// included — in one short deadline. Concretely, each service's pre-build phase
// (routing validation and source resolution) and its post-build phase
// (environment resolution, stale stops, replica starts) run on SEPARATE fresh
// bounds rooted in ctx: a long build issued by the runtime under its own
// lifecycle-rooted buildTimeout can never consume the deadline of the post-build
// work that follows. A non-positive reconcileTimeout leaves normal operations
// bounded only by the lifecycle context.
//
// The returned bool reports whether the pass performed any convergence action
// (stopped at least one removed-service or stale container, or attempted to
// start at least one replica). A fully-converged desired state — everything
// already running with the correct image, port, and replica count — returns
// (false, nil). Callers can use this to distinguish a no-op verification pass
// from a pass that actually changed state (e.g. for log-level selection).
//
// Container ownership is always label-derived. A container belongs to fnName
// when its Function == fnName; containers of other functions are never touched.
// The ownership predicate is NOT hostname-scoped — services must be
// reconcilable across worker restarts on the same host (same as the runtime).
func Reconcile(
	ctx context.Context,
	reconcileTimeout time.Duration,
	d Docker,
	fnName, fnDir string,
	tmpl *function.Template,
	functionImage string,
	preparedEnv []string,
	secrets SecretResolver,
	traefik routing.TraefikConfig,
	log *slog.Logger,
) (bool, error) {
	// A fresh normal-operation bound for the container listing. reconcileTimeout
	// is always positive from the worker; a non-positive value means "no
	// normal-operation bound", so the lifecycle context bounds it directly rather
	// than deriving an already-expired child.
	listCtx, listCancel := ctx, func() {}
	if reconcileTimeout > 0 {
		listCtx, listCancel = context.WithTimeout(ctx, reconcileTimeout)
	}
	containers, err := d.ServiceContainerList(listCtx)
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

	var firstErr error
	fail := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	desired := make(map[string]function.Service, len(tmpl.Services))
	for _, svc := range tmpl.Services {
		desired[svc.SourceRef()] = svc
	}

	// Classify this function's containers by service. Containers whose service
	// is no longer in the template (removed service) are stopped OUTRIGHT and
	// BEFORE the network pre-flight below: removed-service cleanup does not
	// depend on the template's networks — the container is not part of the
	// desired set regardless of network availability — so a missing network must
	// never leave a removed service's container running. Desired services'
	// containers are grouped for the per-service pass further down.
	byService := make(map[string][]runtime.ServiceContainer)
	for _, c := range containers {
		if c.Function != fnName {
			// Another function owns this container; its reconcile handles it.
			continue
		}
		if _, ok := desired[c.Identity]; !ok {
			changed = true
			stopCtx, stopCancel := ctx, func() {}
			if reconcileTimeout > 0 {
				stopCtx, stopCancel = context.WithTimeout(ctx, reconcileTimeout)
			}
			err := d.StopServiceContainers(stopCtx, []runtime.ServiceContainer{c})
			stopCancel()
			if err != nil {
				fail(fmt.Errorf("service %q removed: %w", c.Identity, err))
			}
			continue
		}
		byService[c.Identity] = append(byService[c.Identity], c)
	}

	// The template's top-level `networks` are joined by every service container
	// (and every execution container). They are infrastructure owned OUTSIDE
	// Relay, so Relay verifies them before any START or STALE REPLACEMENT and
	// refuses to converge the desired services otherwise: a missing network must
	// preserve the function's healthy current service containers and be retried
	// on the next pass, never tear them down. The warning is structured
	// (function + network) and scoped to THIS function — it never affects any
	// other function. The check is skipped when the template declares no
	// services (nothing to start; removed services were already cleaned up
	// above), so a services-less function never logs a spurious network warning.
	// Removed-service cleanup above has already run and is reported through
	// `changed`.
	if len(tmpl.Services) > 0 {
		netCtx, netCancel := ctx, func() {}
		if reconcileTimeout > 0 {
			netCtx, netCancel = context.WithTimeout(ctx, reconcileTimeout)
		}
		missing, ok, err := d.VerifyNetworks(netCtx, tmpl.Networks)
		netCancel()
		if err != nil {
			fail(fmt.Errorf("function networks: %w", err))
			log.Warn("Service: cannot verify networks; keeping existing containers",
				"function", fnName, "error", err)
			return changed, firstErr
		} else if !ok {
			err := fmt.Errorf("network %q does not exist", missing)
			fail(err)
			log.Warn("Service: required Docker network is missing; keeping existing containers",
				"function", fnName,
				"network", missing,
				"effect", "desired services skipped; removed-service cleanup still applied; relay never creates networks",
			)
			return changed, firstErr
		}
	}

	for _, svc := range tmpl.Services {
		identity := svc.SourceRef()
		existing := byService[identity]

		// The pre-build phase runs on its own fresh normal-operation bound rooted
		// in the lifecycle context (never the caller's ctx), so a caller cannot
		// wrap a whole pass in one short deadline and one operation cannot
		// consume another's budget. Routing validation and source resolution
		// belong here. A `build` source's Dockerfile build does NOT run on this
		// bound — the runtime roots it in the manager lifecycle under its own
		// buildTimeout (see runtime.buildContext) — but the quick probes around
		// it (network existence, image existence, inspect) do.
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
				log.Warn("Service: routing validation failed", "service", identity, "error", err)
				continue
			}
			// The routing network (e.g. the Traefik network) is infrastructure
			// owned outside Relay; Relay verifies it exists and refuses to
			// start routed containers otherwise — it never creates it.
			ok, err := d.NetworkExists(preCtx, traefik.Network)
			if err != nil {
				preCancel()
				err := fmt.Errorf("service %q: check routing network: %w", identity, err)
				fail(err)
				log.Warn("Service: routing validation failed", "service", identity, "error", err)
				continue
			}
			if !ok {
				preCancel()
				err := fmt.Errorf("service %q: %w", identity, routing.MissingNetwork(traefik.Network))
				fail(err)
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
		// cannot be resolved — a failed build, a failed pull, a missing image, or
		// an unlaunchable entrypoint — is reported and this service is skipped
		// entirely: its existing (healthy) containers are preserved rather than
		// replaced on a failed resolution. This is the ordering guarantee that a
		// transient registry outage never tears down a working service. The build
		// itself does not use preCtx: the runtime roots it in the manager
		// lifecycle under buildTimeout, so a long build cannot outlive the
		// pre-build budget here either.
		resolved, err := d.ResolveServiceImage(preCtx, fnName, fnDir, tmpl, svc, functionImage)
		preCancel()
		if err != nil {
			fail(fmt.Errorf("service %q: %w", identity, err))
			log.Warn("Service: cannot resolve source; keeping existing containers",
				"service", identity, "error", err)
			continue
		}

		// The post-build phase runs on a FRESH normal-operation bound rooted in
		// the lifecycle context, separate from every bound above. This is the
		// core ordering guarantee: a long Dockerfile build in the pre-build
		// phase (bounded by the runtime's buildTimeout, not by preCtx) can never
		// consume the deadline of the environment resolution, stale stops, and
		// replica starts that follow. A non-positive reconcileTimeout leaves
		// these operations bounded only by the lifecycle context.
		postCtx, postCancel := ctx, func() {}
		if reconcileTimeout > 0 {
			postCtx, postCancel = context.WithTimeout(ctx, reconcileTimeout)
		}
		env, err := BuildEnv(postCtx, tmpl, svc.Port, preparedEnv, secrets)
		if err != nil {
			postCancel()
			fail(fmt.Errorf("service %q: %w", identity, err))
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
		// desiredNetworks is the canonical relay.networks value the container
		// must carry: the union of the routing network (if routed) and the
		// template's top-level `networks`, sorted and de-duplicated. A network
		// change — adding, removing, or renaming a template network — makes the
		// running container stale and replaces it WITHOUT rebuilding the image
		// (the image fingerprint ignores runtime-only config).
		desiredNetworks := runtime.NetworksLabel(routeNetwork, tmpl.Networks)

		// A container is a keep candidate only when it is both healthy (running)
		// and currently configured correctly (image, image content, port,
		// effective environment, and Docker networks all match the desired
		// values) and carries a real replica label. Anything else —
		// exited/dead/removing, a changed image (rebuild), a moved external tag
		// (image content changed), a changed port, a changed env/secret (env hash
		// mismatch), a changed network set, or an unlabeled legacy container
		// (Replica == -1) — is stale and must be replaced. In addition, the
		// container's labels must match the desired routing label set exactly: a
		// changed host/path/port leaves stale Traefik labels pointing traffic at
		// whatever the old container served, so the container is replaced.
		var candidates []runtime.ServiceContainer
		var stale []runtime.ServiceContainer
		for _, c := range existing {
			if c.State == container.StateRunning &&
				c.Image == resolved.Ref &&
				c.ImageID == resolved.ID &&
				c.Port == svc.Port &&
				c.EnvHash == envHash &&
				c.Networks == desiredNetworks &&
				c.Replica >= 0 &&
				routingLabelsMatch(routeLabels, c.Labels) {
				candidates = append(candidates, c)
			} else {
				stale = append(stale, c)
			}
		}

		// Prefer keeping the LOWEST replica indexes, so scale-down retains the
		// longest-running lowest-numbered replicas. A candidate already occupies
		// its own slot (Replica), and we only keep candidates whose slot is a
		// desired replica slot in [0, desired); any candidate with a slot >=
		// desired, or a duplicate slot, is excess and becomes stale. Kept
		// candidates are recorded in occupied; the start loop below starts every
		// desired slot that no candidate holds.
		sort.Slice(candidates, func(i, j int) bool { return candidates[i].Replica < candidates[j].Replica })
		occupied := make(map[int]bool, svc.Replicas)
		for _, c := range candidates {
			if c.Replica < svc.Replicas && !occupied[c.Replica] {
				occupied[c.Replica] = true
				continue
			}
			stale = append(stale, c)
		}

		// Stop every stale/excess container for this service.
		if len(stale) > 0 {
			changed = true
			if err := d.StopServiceContainers(postCtx, stale); err != nil {
				fail(fmt.Errorf("service %q stale: %w", identity, err))
			}
		}

		// Start the deficit: every desired slot 0..Replicas-1 that no kept
		// container occupies.
		for slot := 0; slot < svc.Replicas; slot++ {
			if occupied[slot] {
				continue
			}
			changed = true
			spec := runtime.ServiceSpec{
				Function: fnName,
				Identity: identity,
				Port:     svc.Port,
				Image:    resolved.Ref,
				ImageID:  resolved.ID,
				Entry:    resolved.Entry,
				Env:      env,
				Labels:   routeLabels,
				Network:  routeNetwork,
				Networks: tmpl.Networks,
			}
			if _, err := d.StartService(postCtx, spec, slot); err != nil {
				fail(fmt.Errorf("service %q replica %d: %w", identity, slot, err))
			}
		}
		postCancel()
	}

	return changed, firstErr
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
func RemoveAll(ctx context.Context, d Docker, fnName string, log *slog.Logger) {
	n, err := d.RemoveFunctionServiceContainers(ctx, fnName)
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
// Docker implementation, secret resolver, and logger, serializing all service
// mutations with one mutex so concurrent reconciler ticks and startup sweeps
// cannot interleave container operations.
type ServiceReconciler struct {
	docker  Docker
	secrets SecretResolver
	traefik routing.TraefikConfig
	log     *slog.Logger

	// reconcileTimeout is the bound Reconcile derives for each normal
	// service-operation context, rooted in the lifecycle context its caller
	// passes. It is injected by the worker (its reconcileTimeout) so the policy
	// lives with the worker that owns the value; a non-positive value means
	// "no normal-operation bound", leaving operations bounded only by the
	// lifecycle. It deliberately does NOT bound Dockerfile builds — those are
	// rooted in the runtime manager's lifecycle under buildTimeout.
	reconcileTimeout time.Duration

	mu sync.Mutex
}

// NewServiceReconciler builds a ServiceReconciler. traefik is the worker-level
// Traefik routing config (empty = routing not configured; required only for
// services whose template declares a host). reconcileTimeout is the worker's
// normal-service-operation budget, applied by Reconcile to every pre-build and
// post-build Docker operation (a non-positive value leaves them bounded only by
// the lifecycle context; builds are never bounded by it).
func NewServiceReconciler(
	d Docker,
	secrets SecretResolver,
	traefik routing.TraefikConfig,
	log *slog.Logger,
	reconcileTimeout time.Duration,
) *ServiceReconciler {
	return &ServiceReconciler{
		docker:           d,
		secrets:          secrets,
		traefik:          traefik,
		log:              log,
		reconcileTimeout: reconcileTimeout,
	}
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
// short deadline wrapped around the whole pass) and a long build can never
// consume the post-build deadline.
//
// fnDir is the function's directory, needed to resolve `build` sources (their
// Dockerfile is read relative to it) and to fingerprint their selected source.
// image is the function's own prepared image, used only by `entrypoint`
// sources. fnDir may be empty when the template declares no build service.
func (c *ServiceReconciler) Apply(
	ctx context.Context,
	fnName, fnDir string,
	tmpl *function.Template,
	image string,
	preparedEnv []string,
) {
	c.mu.Lock()
	defer c.mu.Unlock()

	replicas := 0
	for _, svc := range tmpl.Services {
		replicas += svc.Replicas
	}

	changed, err := Reconcile(ctx, c.reconcileTimeout, c.docker, fnName, fnDir, tmpl, image, preparedEnv, c.secrets, c.traefik, c.log)
	if err != nil {
		c.log.Warn("Service: reconciled with errors",
			"function", fnName,
			"replicas", replicas,
			"error", err,
		)
		return
	}
	if changed {
		c.log.Info("Service: reconciled",
			"function", fnName,
			"services", len(tmpl.Services),
			"replicas", replicas,
		)
		return
	}
	c.log.Debug("Service: unchanged",
		"function", fnName,
		"services", len(tmpl.Services),
		"replicas", replicas,
	)
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

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
//   - RemoveAll (via the reconciler's RemoveServices hook) must run BEFORE the
//     function's images are retired, because running service containers still
//     reference those images.
//   - Routing (Traefik) for routed services (a service declaring a host) is
//     validated before any container action for that service: the routing
//     config must be present, the per-service effective host (including any
//     TRAEFIK_HOST_OVERRIDE mapping) must be a valid hostname, and the routing
//     network must exist (Relay never creates it); a routed service failing
//     routing validation is reported and skipped, not half-reconciled.
package reconciler

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/moby/moby/api/types/container"

	"relay/internal/function"
	"relay/internal/routing"
	"relay/internal/runtime"
)

// Docker is the service-container subset of the runtime Manager. The Manager
// satisfies it; the structural assertion lives in the worker wiring (worker.go),
// not here.
type Docker interface {
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

// Reconcile converges the set of fnName's running service containers to
// tmpl.Services (with the freshly prepared image). It is a best-effort pass:
// per-operation failures are logged by the caller-facing style and the first
// error is returned (at least one error surfaces when anything failed), so a
// transient daemon error on one container does not abort convergence of the rest.
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
	d Docker,
	fnName string,
	tmpl *function.Template,
	image string,
	preparedEnv []string,
	secrets SecretResolver,
	traefik routing.TraefikConfig,
	log *slog.Logger,
) (bool, error) {
	containers, err := d.ServiceContainerList(ctx)
	if err != nil {
		// Without a listing we cannot know the running set; surface the error
		// rather than guessing whether to start/stop anything.
		return false, fmt.Errorf("service: list containers: %w", err)
	}
	// changed reports whether any convergence action (a stop or a start
	// attempt) was issued during this pass. It starts false and is only set by
	// the classify/start work below.
	changed := false

	desired := make(map[string]function.Service, len(tmpl.Services))
	for _, svc := range tmpl.Services {
		desired[svc.Entrypoint] = svc
	}

	var firstErr error
	fail := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// Group this function's containers by service; containers whose service is
	// no longer in the template (removed service) are stopped outright.
	byService := make(map[string][]runtime.ServiceContainer)
	for _, c := range containers {
		if c.Function != fnName {
			// Another function owns this container; its reconcile handles it.
			continue
		}
		if _, ok := desired[c.Entrypoint]; !ok {
			changed = true
			if err := d.StopServiceContainers(ctx, []runtime.ServiceContainer{c}); err != nil {
				fail(fmt.Errorf("service %q removed: %w", c.Entrypoint, err))
			}
			continue
		}
		byService[c.Entrypoint] = append(byService[c.Entrypoint], c)
	}

	for _, svc := range tmpl.Services {
		existing := byService[svc.Entrypoint]

		// Routing validation runs FIRST, before any classification or stops:
		// a routed service (one declaring a host) whose routing config is
		// unusable must be reported and skipped entirely — no stops, no
		// starts — replacing it half-way would break the existing container
		// for nothing.
		var routeLabels map[string]string
		routeNetwork := ""
		if svc.Host != "" {
			if err := traefik.Validate(); err != nil {
				err := fmt.Errorf("service %q: %w", svc.Entrypoint, err)
				fail(err)
				log.Warn("Service: routing validation failed", "service", svc.Entrypoint, "error", err)
				continue
			}
			// The per-service effective host (declared host, or the override
			// mapping) is validated next: the combined length is only knowable
			// with the template host in hand, and an overlong host would
			// silently never match. This runs before network/container work,
			// preserving the routing-first ordering; with no override it is a
			// no-op on an already-validated template host.
			if err := traefik.ValidateHost(svc.Host); err != nil {
				err := fmt.Errorf("service %q: %w", svc.Entrypoint, err)
				fail(err)
				log.Warn("Service: routing validation failed", "service", svc.Entrypoint, "error", err)
				continue
			}
			// The routing network (e.g. the Traefik network) is infrastructure
			// owned outside Relay; Relay verifies it exists and refuses to
			// start routed containers otherwise — it never creates it.
			ok, err := d.NetworkExists(ctx, traefik.Network)
			if err != nil {
				err := fmt.Errorf("service %q: check routing network: %w", svc.Entrypoint, err)
				fail(err)
				log.Warn("Service: routing validation failed", "service", svc.Entrypoint, "error", err)
				continue
			}
			if !ok {
				err := fmt.Errorf("service %q: %w", svc.Entrypoint, routing.MissingNetwork(traefik.Network))
				fail(err)
				log.Warn("Service: routing validation failed", "service", svc.Entrypoint, "error", err)
				continue
			}
			routeLabels = routing.TraefikLabels(fnName, svc.Entrypoint, svc.Host, svc.Path, svc.Port, traefik)
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
					"service", svc.Entrypoint,
					"host", svc.Host,
					"network", routeNetwork,
				}, attrs...)...)
		}

		// A container is a keep candidate only when it is both healthy (running)
		// and currently configured correctly (image and port match the desired
		// value) and carries a real replica label. Anything else — exited/dead/
		// removing, a changed image (rebuild), a changed port, or an unlabeled
		// legacy container (Replica == -1) — is stale and must be replaced. In
		// addition, the container's labels must match the desired routing label
		// set exactly: a changed host/path/port/network leaves stale Traefik
		// labels pointing traffic at whatever the old container served, so the
		// container is replaced.
		var candidates []runtime.ServiceContainer
		var stale []runtime.ServiceContainer
		for _, c := range existing {
			if c.State == container.StateRunning &&
				c.Image == image &&
				c.Port == svc.Port &&
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
			if err := d.StopServiceContainers(ctx, stale); err != nil {
				fail(fmt.Errorf("service %q stale: %w", svc.Entrypoint, err))
			}
		}

		// Resolve the entrypoint and environment ONCE per service; when either
		// fails we cannot start replicas, but the stops above already happened.
		// Log the reason before continuing so the missing-replica condition is
		// diagnosable rather than silent.
		entry, err := runtime.ServiceEntry(tmpl.Runtime, svc.Entrypoint)
		if err != nil {
			fail(fmt.Errorf("service %q: %w", svc.Entrypoint, err))
			log.Warn("Service: cannot start replicas", "service", svc.Entrypoint, "error", err)
			continue
		}
		env, err := BuildEnv(ctx, tmpl, svc.Port, preparedEnv, secrets)
		if err != nil {
			fail(fmt.Errorf("service %q: %w", svc.Entrypoint, err))
			log.Warn("Service: cannot start replicas", "service", svc.Entrypoint, "error", err)
			continue
		}

		// Start the deficit: every desired slot 0..Replicas-1 that no kept
		// container occupies.
		for slot := 0; slot < svc.Replicas; slot++ {
			if occupied[slot] {
				continue
			}
			changed = true
			spec := runtime.ServiceSpec{
				Function:   fnName,
				Entrypoint: svc.Entrypoint,
				Port:       svc.Port,
				Image:      image,
				Entry:      entry,
				Env:        env,
				Labels:     routeLabels,
				Network:    routeNetwork,
			}
			if _, err := d.StartService(ctx, spec, slot); err != nil {
				fail(fmt.Errorf("service %q replica %d: %w", svc.Entrypoint, slot, err))
			}
		}
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
	mu      sync.Mutex
}

// NewServiceReconciler builds a ServiceReconciler. traefik is the worker-level
// Traefik routing config (empty = routing not configured; required only for
// services whose template declares a host).
func NewServiceReconciler(
	d Docker,
	secrets SecretResolver,
	traefik routing.TraefikConfig,
	log *slog.Logger,
) *ServiceReconciler {
	return &ServiceReconciler{docker: d, secrets: secrets, traefik: traefik, log: log}
}

// Apply converges fnName's services to tmpl+image: it runs Reconcile and logs
// the outcome — Info when the pass changed state, Debug when it was a no-op
// verification pass, Warn when it errored. It is intentionally non-fatal: a
// service-convergence failure must not fail the function's reconcile. The
// Info/Debug distinction means an unchanged function (periodic self-healing
// tick) does not log at Info; only converges that actually changed or failed do.
func (c *ServiceReconciler) Apply(
	ctx context.Context,
	fnName string,
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

	changed, err := Reconcile(ctx, c.docker, fnName, tmpl, image, preparedEnv, c.secrets, c.traefik, c.log)
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

package worker

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"relay/internal/config"
	"relay/internal/routing"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/state"
)

// Service endpoint publication
//
// When host-port publication is enabled (SERVICE_BIND_ADDR), the worker
// maintains a PostgreSQL batch of routable endpoints it has VERIFIED on its own
// Docker daemon. Publication consumes the GLOBAL node lease maintained by the
// node registry; it never owns an independent heartbeat or lease. The registry
// is a materialized view for discovery: it never authorizes matching, builds,
// reconciliation, or execution, and a publication outage only delays discovery.
//
// A single bounded loop re-collects the eligible endpoints and publishes the
// exact batch. The standalone node registry owns the lease heartbeat; endpoint
// publication never renews the lease. It does refresh each endpoint's OWN
// expiry, so a snapshot that can no longer be verified disappears while the
// node lease stays alive.
//
// Every write is fenced in PostgreSQL by the pinned lock backend and the
// current database incarnation (see state's node registry methods), so a
// superseded process cannot publish rows even in the interval between losing
// its advisory lock and a successor's claim.
const (
	// serviceEndpointSyncInterval is the Docker collection/publication cadence.
	// It is also how quickly a health transition (a committed replica becoming
	// unhealthy, or recovering) is reflected without rebuilding anything.
	serviceEndpointSyncInterval = 5 * time.Second
	// serviceEndpointSyncTimeout bounds one Docker collection plus publication.
	serviceEndpointSyncTimeout = 20 * time.Second
)

// serviceEndpointStore is the consumer-owned persistence seam for endpoint
// publication. *state.State implements it.
type serviceEndpointStore interface {
	PublishServiceRegistryEndpoints(
		ctx context.Context,
		lease state.NodeLease,
		endpoints []state.ServiceRegistryEndpoint,
	) error
}

// serviceEndpointSource is the Docker seam the publisher reads. The production
// *runtime.Manager implements it; a fake can drive collection tests without
// Docker.
type serviceEndpointSource interface {
	RegistryServiceEndpoints(
		ctx context.Context, expectedBindAddr string,
	) ([]runtime.RegistryServiceEndpoint, error)
}

// nodeLeaseSource supplies the global node lease owned by the node registry.
type nodeLeaseSource interface {
	Lease() *state.NodeLease
}

// serviceEndpointPublisher owns this worker's published endpoint batch.
type serviceEndpointPublisher struct {
	st       serviceEndpointStore
	manager  serviceEndpointSource
	apps     *runner.Registry
	nodes    nodeLeaseSource
	traefik  routing.TraefikConfig
	log      *slog.Logger
	reporter *registryFailureReporter

	bindAddr     string
	hostOverride string

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	wg     sync.WaitGroup
}

// startServiceEndpointPublisher starts endpoint publication and registers its
// shutdown step. It returns nil (and registers nothing) when host-port
// publication is disabled or the required dependencies are absent, so every
// other deployment keeps its existing behavior.
func startServiceEndpointPublisher(
	lifecycle context.Context,
	logger *slog.Logger,
	cfg config.Config,
	st *state.State,
	manager *runtime.Manager,
	apps *runner.Registry,
	nodes *nodeRegistry,
	shutdown *shutdownRegistry,
) *serviceEndpointPublisher {
	if cfg.ServiceBindAddr == "" || st == nil || manager == nil || apps == nil || nodes == nil {
		return nil
	}
	r := &serviceEndpointPublisher{
		st:      st,
		manager: manager,
		apps:    apps,
		nodes:   nodes,
		traefik: routing.TraefikConfig{
			Network:      cfg.TraefikNetwork,
			EntryPoints:  cfg.TraefikEntryPoints,
			CertResolver: cfg.TraefikCertResolver,
			Priority:     cfg.TraefikPriority,
		},
		log:          logger,
		reporter:     nodes.reporter,
		bindAddr:     cfg.ServiceBindAddr,
		hostOverride: cfg.RoutingHostOverride,
	}
	r.Start(lifecycle)
	if shutdown != nil {
		shutdown.register(serviceEndpointPublisherStep(r))
	}
	return r
}

// Start runs the publication loop under lifecycle. done closes once it has
// returned so shutdown can strictly join it before the node registry releases
// the lease.
func (r *serviceEndpointPublisher) Start(lifecycle context.Context) {
	r.ctx, r.cancel = context.WithCancel(lifecycle)
	r.done = make(chan struct{})
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.syncLoop()
	}()
	go func() {
		r.wg.Wait()
		close(r.done)
	}()
}

func (r *serviceEndpointPublisher) syncLoop() {
	r.syncOnce()
	ticker := time.NewTicker(serviceEndpointSyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.syncOnce()
		}
	}
}

// stopping reports lifecycle cancellation. An operation canceled by shutdown is
// normal termination, not a publication failure, and must not emit a failure
// warning.
func (r *serviceEndpointPublisher) stopping() bool {
	return r.ctx != nil && r.ctx.Err() != nil
}

func (r *serviceEndpointPublisher) syncOnce() {
	ctx, cancel := context.WithTimeout(r.ctx, serviceEndpointSyncTimeout)
	defer cancel()
	lease := r.nodes.Lease()
	if lease == nil {
		return
	}
	endpoints, err := r.collectEndpoints(ctx)
	if err != nil {
		if endpoints == nil {
			// Keep the previous snapshot: a failed discovery must not be
			// mistaken for "nothing is serving". Endpoint expiry still bounds
			// staleness.
			if !r.stopping() {
				r.reporter.failure("publish", err)
			}
			return
		}
		// A partially verified batch is still authoritative for the endpoints
		// it contains; the unreadable container is retried on the next sync.
		r.log.Debug("Service registry: publishing partially verified endpoints",
			"error", err)
	}
	if err := r.st.PublishServiceRegistryEndpoints(ctx, *lease, endpoints); err != nil {
		if errors.Is(err, state.ErrNodeLeaseFence) {
			// The node registry owns lease recovery; this publication is simply
			// stale and the next sync uses the fresh lease.
			r.log.Debug("Service registry: publish fenced",
				"incarnation", lease.Incarnation, "error", err)
			return
		}
		if r.stopping() {
			return
		}
		r.reporter.failure("publish", err)
		return
	}
	r.reporter.recovery("publish")
}

// collectEndpoints builds the exact batch this node may advertise: each
// Docker-verified committed replica must also match the CURRENT template's
// route (effective host, path, port, and full Traefik label set). A container
// left from a superseded template or an earlier host override therefore drops
// out instead of advertising a stale route. The published host is the effective
// hostname used by the container's routing labels (see routing.EffectiveHost),
// so registry consumers and Traefik agree on the advertised name.
func (r *serviceEndpointPublisher) collectEndpoints(
	ctx context.Context,
) ([]state.ServiceRegistryEndpoint, error) {
	candidates, collectErr := r.manager.RegistryServiceEndpoints(ctx, r.bindAddr)
	if len(candidates) == 0 {
		return nil, collectErr
	}

	routes := r.desiredRoutes()
	out := make([]state.ServiceRegistryEndpoint, 0, len(candidates))
	seen := make(map[serviceRegistryEndpointKey]struct{}, len(candidates))
	for _, candidate := range candidates {
		route, ok := routes[serviceRegistryRouteKey{
			app: candidate.App, service: candidate.Service,
		}]
		if !ok {
			continue
		}
		if !registryRoutingLabelsMatch(route.labels, candidate.Labels) {
			continue
		}
		key := keyForEndpoint(candidate)
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, state.ServiceRegistryEndpoint{
			App:          candidate.App,
			Service:      candidate.Service,
			Replica:      candidate.Replica,
			Host:         route.host,
			Path:         route.path,
			HostIP:       candidate.HostIP,
			HostPort:     candidate.HostPort,
			InternalPort: candidate.InternalPort,
			ContainerID:  candidate.ContainerID,
			Generation:   candidate.Generation,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Host != out[j].Host {
			return out[i].Host < out[j].Host
		}
		if out[i].Path != out[j].Path {
			return out[i].Path < out[j].Path
		}
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
	return out, collectErr
}

type serviceRegistryRouteKey struct {
	app     string
	service string
}

type serviceRegistryEndpointKey struct {
	app         string
	service     string
	replica     int
	containerID string
}

func keyForEndpoint(endpoint runtime.RegistryServiceEndpoint) serviceRegistryEndpointKey {
	return serviceRegistryEndpointKey{
		app:         endpoint.App,
		service:     endpoint.Service,
		replica:     endpoint.Replica,
		containerID: endpoint.ContainerID,
	}
}

func (r *serviceEndpointPublisher) desiredRoutes() map[serviceRegistryRouteKey]serviceRegistryRoute {
	names := r.apps.Names()
	sort.Strings(names)
	routes := make(map[serviceRegistryRouteKey]serviceRegistryRoute, len(names))
	for _, name := range names {
		pf := r.apps.GetByName(name)
		if pf == nil {
			continue
		}
		tmpl := pf.App().Template
		if tmpl == nil {
			continue
		}
		for _, svc := range tmpl.Services {
			if svc.Host == "" {
				continue
			}
			host := routing.EffectiveHost(svc.Host, r.hostOverride)
			routes[serviceRegistryRouteKey{app: name, service: svc.Name}] = serviceRegistryRoute{
				host: host,
				path: svc.Path,
				labels: routing.TraefikLabels(
					name, svc.Name, host, svc.Path, svc.Port, r.traefik),
			}
		}
	}
	return routes
}

type serviceRegistryRoute struct {
	host   string
	path   string
	labels map[string]string
}

// registryRoutingLabelsMatch mirrors the service reconciler's routing-label
// comparison: every desired Traefik key/value must match and the container must
// carry no extra Traefik-owned key. Non-Traefik labels are ignored.
func registryRoutingLabelsMatch(desired, actual map[string]string) bool {
	for key, want := range desired {
		if actual[key] != want {
			return false
		}
	}
	for key := range actual {
		if _, ok := desired[key]; ok {
			continue
		}
		if routing.IsTraefikLabel(key) {
			return false
		}
	}
	return true
}

// shutdown stops the publication loop and strictly joins it through the
// shutdown registry's barrier semantics. It does NOT withdraw rows: the node
// registry step runs afterwards and releases this incarnation's endpoints and
// lease atomically.
func (r *serviceEndpointPublisher) shutdown(ctx context.Context) error {
	r.cancel()
	return barrierJoin(ctx, func(joinCtx context.Context) error {
		return waitSignal(joinCtx, r.done)
	})
}

func serviceEndpointPublisherStep(r *serviceEndpointPublisher) shutdownStep {
	return shutdownStep{
		name:    shutdownStepServiceEndpoints,
		timeout: shutdownServiceTimeout,
		barrier: true,
		run:     r.shutdown,
	}
}

package worker

import (
	"context"
	"log/slog"

	"relay/internal/config"
	"relay/internal/observability/metrics"
	"relay/internal/reconciler"
	"relay/internal/routing"
	"relay/internal/runtime"
	"relay/internal/secrets"
)

// startServiceConvergence constructs and starts the persistent service
// controller/coordinator and registers its shutdown steps. It returns both
// because Run still drives startup/live service enqueue through the coordinator
// and the startup housekeeping sweep through the controller.
//
// The service reconciler converges each app's persistent service containers to
// its template (manager is the Docker seam; secretProvider is the shared
// secrets resolver). It is wired with THIS worker's stable identity
// (WithWorkerID), so every service pass, app removal, and orphan sweep only
// ever converges, adopts, or removes containers stamped with this worker's
// relay.hostname: another worker's services on the shared Docker daemon are
// never touched. Because WORKER_ID is stable across restarts, a replacement
// process still ADOPTS the previous process's converged service containers
// instead of recreating them. Service containers stop on graceful shutdown: the
// shutdown tail runs ShutdownCleanup for this worker's identity (cfg.WorkerID).
// The coordinator runs the per-app Applys asynchronously (bounded workers,
// latest-desired-state coalescing), so startup never blocks on service
// convergence (e.g. an external image pull); the startup orphan sweep + image
// GC run inside the coordinator's exclusive housekeeping window in the
// background pass. Startup reconciliation remains the crash-recovery path when
// shutdown cleanup did not execute. The service reconciler itself decides
// whether routing applies (only services declaring a host are routed) and
// validates TRAEFIK_NETWORK per routed service; wiring only forwards the
// configured value. The same worker-global NETWORKS set applied to execution
// containers is forwarded too, so every service container joins it.
func startServiceConvergence(
	ctx context.Context,
	logger *slog.Logger,
	cfg config.Config,
	manager *runtime.Manager,
	secretProvider secrets.Provider,
	proof ownershipProof,
	metricsInstance *metrics.Registry,
	shutdown *shutdownRegistry,
) (*reconciler.ServiceReconciler, *reconciler.ServiceCoordinator) {
	svcCtrl := reconciler.NewServiceReconciler(
		manager,
		secretProvider,
		routing.TraefikConfig{
			Network:      cfg.TraefikNetwork,
			EntryPoints:  cfg.TraefikEntryPoints,
			CertResolver: cfg.TraefikCertResolver,
			Priority:     cfg.TraefikPriority,
			HostOverride: cfg.TraefikHostOverride,
		},
		logger,
		reconcileTimeout,
		reconciler.WithMetrics(metricsInstance),
		reconciler.WithNetworks(cfg.Networks),
		// Scope every service-container operation to this worker's stable
		// identity (relay.hostname). A same-WORKER_ID restart still adopts its
		// own running services; a different worker's containers are untouched.
		reconciler.WithWorkerID(cfg.WorkerID),
	)
	services := reconciler.NewServiceCoordinator(svcCtrl)
	services.Start(ctx)
	// Joining the coordinator releases its workers and waiters and drains
	// in-flight Applys; the join is a strict barrier whose per-step bound is only
	// the wait-before-diagnostic threshold, while the hostname-scoped container
	// cleanup that runs right after is a separate bounded best-effort step.
	shutdown.register(servicesJoinBarrierStep(services.Join))
	shutdown.register(shutdownStep{
		name:    shutdownStepServiceCleanup,
		timeout: shutdownServiceTimeout,
		// Service cleanup stops and removes THIS worker's service containers
		// (scoped by WORKER_ID), so it is skipped once ownership is lost: a
		// replacement process may already own the identity and its containers.
		// Unlike execution containers, a replacement presenting the SAME
		// WORKER_ID CAN adopt the SAME physical service container id (the
		// reconciler is label-derived and the worker scopes it by relay.hostname,
		// not by process), so the safety here rests on the fail-closed
		// ownership proof rather than on id freshness. It snapshots the
		// candidates FIRST, re-proves the lock on the pinned connection SECOND,
		// and removes exactly the captured containers THIRD
		// (guardIdentitySnapshotCleanup): discovery before the proof means a
		// candidate was observed while the lock was ours, the proof gates the
		// whole removal, and a latched or failed proof removes nothing. This is
		// not atomic fencing; see guardIdentitySnapshotCleanup for the bounded
		// residual.
		run: shutdownServices(svcCtrl, cfg.WorkerID, proof, logger),
	})
	return svcCtrl, services
}

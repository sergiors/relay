package worker

import (
	"context"
	"log/slog"
	"time"

	"relay/internal/app"
	"relay/internal/config"
	"relay/internal/observability/metrics"
	"relay/internal/reconciler"
	"relay/internal/routing"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/secrets"
	"relay/internal/state"
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

// enqueueLiveServices snapshots the current prepared environment and publishes
// the desired service state without blocking the app reconciler pump. The
// coordinator coalesces updates to the latest desired state per app and its
// bounded workers converge it with the worker LIFECYCLE context. A nil Prepared
// (unavailable) entry falls back to no plan env, mirroring the runner's
// nil-safe behavior.
//
// The managed app image lease is acquired BEFORE the enqueue and handed to
// the coordinator, which holds it across pending/coalesced/running service
// reconciliation through StartService completion, so a concurrent retirement
// cannot remove the image a service pass is still converging. When the image is
// already retiring (a superseded desired state), the enqueue is skipped: a newer
// desired state will follow.
func enqueueLiveServices(
	services *reconciler.ServiceCoordinator,
	manager *runtime.Manager,
	reg *runner.Registry,
	name string,
	tmpl *app.Template,
	image string,
) {
	var preparedEnv []string
	if cur := reg.GetByName(name); cur != nil && cur.Prepared() != nil {
		preparedEnv = cur.Prepared().Env
	}
	lease := acquireServiceLease(manager, name, image)
	services.EnqueueLeased(name, tmpl, image, preparedEnv, lease)
}

func enqueueLiveServicesWithStatus(
	services *reconciler.ServiceCoordinator,
	manager *runtime.Manager,
	reg *runner.Registry,
	name string,
	tmpl *app.Template,
	image string,
	onReconcileStart func(),
	onComplete func(error),
) {
	var preparedEnv []string
	if cur := reg.GetByName(name); cur != nil && cur.Prepared() != nil {
		preparedEnv = cur.Prepared().Env
	}
	lease := acquireServiceLease(manager, name, image)
	services.EnqueueWithStatusLeased(
		name,
		tmpl,
		image,
		preparedEnv,
		lease,
		onReconcileStart,
		onComplete,
	)
}

func enqueueLiveServiceObservationWithStatus(
	services *reconciler.ServiceCoordinator,
	manager *runtime.Manager,
	reg *runner.Registry,
	name string,
	tmpl *app.Template,
	image string,
	onReconcileStart func(),
	onComplete func(error),
) {
	var preparedEnv []string
	if cur := reg.GetByName(name); cur != nil && cur.Prepared() != nil {
		preparedEnv = cur.Prepared().Env
	}
	lease := acquireServiceLease(manager, name, image)
	services.EnqueueStatusObservationLeased(
		name,
		tmpl,
		image,
		preparedEnv,
		lease,
		onReconcileStart,
		onComplete,
	)
}

// acquireServiceLease admits a managed image lease for a service pass, or nil
// when the image is not Relay-owned (an external image service) or a manager is
// absent (tests). A retirement in progress yields nil: the enqueue proceeds
// unleased, and a newer desired state will replace it. It never fails the
// caller — service convergence is best-effort and the lease only narrows a
// removal race.
func acquireServiceLease(manager *runtime.Manager, name, image string) *runtime.ImageLease {
	if manager == nil || !runtime.IsRelayImage(image) {
		return nil
	}
	lease, err := manager.AcquireImageLease(image)
	if err != nil {
		manager.Logger().Debug("Service: image retiring; enqueue unleased",
			"app", name, "image", image)
		return nil
	}
	return lease
}

// enqueueStartupServices publishes each prepared app's initial desired
// service state and returns immediately; the coordinator converges them in the
// background, so startup never blocks on service convergence (e.g. an external
// image pull). The
// unavailable/no-services removals are published the same nonblocking way (see
// the special case below), so startup never blocks on Docker work at all; the
// background housekeeping pass' barrier waits for every published operation to
// settle before touching containers or images.
//
// Prepared (available) apps are enqueued UNCONDITIONALLY — including
// templates that now declare no services: Reconcile with an empty desired set
// stops any containers a previous boot left behind when services were removed
// while Relay was down (the fingerprint was re-seeded from changed content, so
// the reconciler would take the skip path and never converge them otherwise).
//
// An unavailable app (no image this boot) is left alone when its template
// still declares services (its stale containers may still be serving the old
// image, until a later successful reconcile or Remove replaces them); when its
// template no longer declares services, lingering containers are stale by
// definition and are removed via the nonblocking EnqueueRemove. The coordinator
// derives the removal's own fresh reconcileTimeout bound; the housekeeping
// barrier serializes the image sweep behind it exactly as it does for Applys.
func enqueueStartupServices(prepared []*runner.PreparedApp, services *reconciler.ServiceCoordinator, logger *slog.Logger) {
	enqueueStartupServicesWithState(context.Background(), prepared, services, nil, nil, logger)
}

// enqueueStartupServicesWithState is enqueueStartupServices with the state sink
// and the exact startup discovery fencing tokens. The service status
// callbacks are conditioned on those tokens, so a convergence superseded by a
// newer generation cannot rewrite state. ctx is the worker lifecycle context:
// callbacks that complete after shutdown attempt their state write with a
// cancelled context, which is treated as normal shutdown (the write fails
// closed and is logged at Debug) rather than a reconcile failure.
func enqueueStartupServicesWithState(
	ctx context.Context,
	prepared []*runner.PreparedApp,
	services *reconciler.ServiceCoordinator,
	st *state.State,
	fencingTokens map[string]state.FencingToken,
	logger *slog.Logger,
) {
	for _, pf := range prepared {
		fn := pf.App()
		if st != nil && fencingTokens[fn.Name] <= 0 {
			// Defense in depth: startup preparation already refuses a token-less
			// app. Publishing service state without the discovery token would let
			// container convergence proceed with no state authority, so fail
			// closed here too.
			logger.Error(
				"Service: startup publication skipped without fencing token",
				"app", fn.Name,
			)
			continue
		}
		if prep := pf.Prepared(); prep != nil {
			// Share the app's publication lease into the startup service
			// request, so a concurrent retirement cannot remove the image while
			// the initial service convergence still needs it. The share is
			// admitted from the SAME lease the registry will publish, and is
			// released when the request concludes. A no-runtime app has no
			// lease and enqueues unleased.
			lease := pf.SharePublication()
			if st != nil && len(fn.Template.Services) > 0 {
				fencingToken := fencingTokens[fn.Name]
				services.EnqueueWithStatusLeased(fn.Name, fn.Template, prep.Image, prep.Env, lease,
					func() { st.RecordReconciling(ctx, fn.Name, fencingToken) },
					func(err error) {
						if err != nil {
							st.RecordServiceFailure(ctx, fn.Name, err, fencingToken)
							return
						}
						// Persist the fingerprint the image was ACTUALLY built
						// from, never a later live rescan: a post-convergence scan
						// could record content the active image does not serve, and
						// the next reconcile/audit detects any drift by comparing the
						// on-disk digest against this built identity.
						st.RecordReconcileSuccess(ctx, fn.Name, prep.Image, prep.Fingerprint, time.Now(), fn, fencingToken)
					})
			} else {
				services.EnqueueLeased(fn.Name, fn.Template, prep.Image, prep.Env, lease)
			}
			continue
		}
		if len(fn.Template.Services) == 0 {
			services.EnqueueRemove(fn.Name)
		} else {
			logger.Warn("Service: app unavailable; skipping service reconcile", "app", fn.Name)
		}
	}
}

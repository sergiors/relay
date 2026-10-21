package worker

import (
	"context"
	"log/slog"
	"time"

	"relay/internal/observability/tracing"
	"relay/internal/reconciler"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/state"
)

// sweepStartupExecutionOrphans removes execution containers a previous Relay
// process on THIS worker identity left behind (a crash mid-invocation), before
// any app is prepared or any container is created. It is label- and
// identity-scoped, so other workers' and non-Relay containers are untouched.
//
// The sweep is bounded and re-driven a bounded number of times
// (retryIdentitySnapshotCleanup), so a transient list/remove error does not
// leave this identity's orphans behind for the whole process lifetime. When the
// bound is exhausted it logs and continues, leaving them for a later restart.
// Each attempt SNAPSHOTS this identity's candidates FIRST, runs ONE fresh
// bounded ownership proof, then force-removes exactly the captured IDs. A
// latched loss or a failed/ambiguous proof removes nothing: a replacement
// process may already own WORKER_ID and its containers must not be disturbed.
// Execution containers are never adopted by a replacement (the runtime pool is
// in-process, so a replacement always creates new ids), which is what makes
// capturing the ids before the proof safe. The bounded residual race after a
// passing proof is documented on guardIdentitySnapshotCleanup.
func sweepStartupExecutionOrphans(
	lifecycle context.Context,
	startupCtx context.Context,
	logger *slog.Logger,
	manager *runtime.Manager,
	workerID string,
	proof ownershipProof,
) {
	orphanCtx, orphanSpan := tracing.Start(startupCtx, "orphan.sweep")
	sweepCtx, sweepCancel := context.WithTimeout(orphanCtx, reconcileTimeout)
	sweepResult, sweepErr := retryIdentitySnapshotCleanup(
		sweepCtx, proof, logger, "orphan-sweep",
		func(stepCtx context.Context) ([]runtime.OrphanContainer, error) {
			return manager.DiscoverOrphanContainers(stepCtx, workerID)
		},
		func(
			stepCtx context.Context,
			orphans []runtime.OrphanContainer,
		) (int, error) {
			return manager.RemoveOrphanContainers(stepCtx, orphans)
		},
	)
	sweepCancel()
	if sweepErr != nil {
		// A cancelled lifecycle is a normal shutdown, not a sweep failure.
		orphanSpan.RecordError(sweepErr)
		logStartupCleanupFailure(
			lifecycle, logger, "Startup: orphan container sweep failed", sweepErr,
		)
	} else if sweepResult.removed > 0 {
		logger.Info(
			"Startup: removed orphan containers from a previous relay process",
			"removed", sweepResult.removed,
			"discovered", sweepResult.discovered,
			"attempts", sweepResult.attempts,
		)
	} else {
		logger.Debug("Startup: orphan container sweep complete",
			"discovered", sweepResult.discovered,
			"attempts", sweepResult.attempts,
		)
	}
	orphanSpan.End()
}

// prepareAndStartApps builds every app's image, publishes the initial desired
// service state, and starts the background startup housekeeping that runs once
// the coordinator's initial service attempts settle. It returns the prepared
// set the runner and reconciler seed consume.
//
// Ordering is part of the contract: preparation completes before any service
// state is published, the enqueue returns before housekeeping starts, and the
// housekeeping barrier is registered immediately after it starts. The
// housekeeping lifecycle is the worker lifecycle, so shutdown cancels an
// in-flight wait or sweep promptly, and the barrier keeps the runtime manager
// and state DB open until the pass actually exits.
func prepareAndStartApps(
	lifecycle context.Context,
	startupCtx context.Context,
	logger *slog.Logger,
	manager *runtime.Manager,
	st *state.State,
	startup startupAppSet,
	services *reconciler.ServiceCoordinator,
	svcCtrl *reconciler.ServiceReconciler,
	proof ownershipProof,
	shutdown *shutdownRegistry,
) []*runner.PreparedApp {
	prepareCtx, prepareSpan := tracing.Start(startupCtx, "apps.prepare")
	prepareStart := time.Now()
	prepared := prepareApps(prepareCtx, manager, startup.records, st, startup.tokens, logger)
	logger.Debug("Startup: prepared apps",
		"count", len(prepared),
		"duration", time.Since(prepareStart),
	)
	prepareSpan.End()

	// Publish each app's initial desired service state and return immediately.
	// The coordinator's bounded workers converge the states in the background,
	// so startup never blocks on service convergence (e.g. an external image
	// pull) — nor on the unavailable/no-services removals, which are published
	// the same nonblocking way (the coordinator derives their own fresh bound).
	_, enqueueSpan := tracing.Start(startupCtx, "services.enqueue")
	enqueueStartupServicesWithState(lifecycle, prepared, services, st, startup.tokens, logger)
	enqueueSpan.End()

	// Lifecycle-aware background housekeeping. It waits behind the coordinator's
	// barrier for the initial service attempts to settle, then runs the startup
	// cleanup passes in their safe order: orphan container sweep, image sweep
	// (whose keep-set must observe the settled service containers), then
	// dependency GC (which prunes dependency images the image sweep orphaned).
	// Startup does NOT call image GC synchronously; the returned done channel is
	// joined in the shutdown tail so no sweep overlaps shutdown cleanup or the
	// closing state DB and Docker client.
	//
	// The service-orphan sweep's present-app set: every VALID loaded app plus
	// every PRESENT app candidate the loader diagnosed as invalid. A directory
	// that is present but unloadable (an invalid/unreadable template) is still an
	// app on disk, so its service containers must never be treated as belonging
	// to an app that was removed while Relay was down. A directory without
	// template.yaml is not an app candidate and is never an issue, so its stale
	// containers are swept. The loader's diagnostics — not PostgreSQL state — are
	// the source of truth for what is present. A directory that VANISHED is not
	// in either set, so its containers are (correctly) swept as orphans.
	liveNames := liveAppNames(startup.loaded, startup.issues)
	housekeepingDone := startStartupHousekeeping(
		lifecycle, logger, startupHousekeeper{
			exclusive: services.RunExclusive,
			sweep: func(hctx context.Context) {
				// The service-orphan sweep is identity-scoped: it lists and stops
				// stale service containers for THIS worker's identity on the
				// shared daemon (the ServiceReconciler is wired with
				// WithWorkerID, so another worker's service containers are never
				// captured). Like the orphan execution sweep it must run in the
				// snapshot-first, proof-second, remove-third order
				// (guardIdentitySnapshotCleanup) so discovery never follows the
				// proof: SNAPSHOT the stale candidates (absent-app filtered with
				// the loader's present set), re-prove the pinned lock, then
				// remove exactly the captured containers. A latched or failed
				// proof removes nothing, so a replacement owner's containers are
				// not disturbed. The sweep is re-driven a bounded number of times
				// so a transient list/stop error is not abandoned.
				sweepStartupServiceOrphans(hctx, svcCtrl, liveNames, proof, logger)
			},
			images: func(hctx context.Context) {
				sweepStartupImages(hctx, manager, prepared, st, logger)
			},
			deps: func(hctx context.Context) {
				cleanupStartupDependencies(hctx, manager, logger)
			},
		},
	)
	// barrier: the startup sweeps use the runtime manager and the state DB, so
	// the manager/state teardown must wait for the pass to actually exit (not
	// merely for this step's bound) before closing either.
	shutdown.register(housekeepingBarrierStep(housekeepingDone))

	return prepared
}

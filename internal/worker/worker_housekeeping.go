package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"relay/internal/app"
	"relay/internal/observability/tracing"
	"relay/internal/reconciler"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/state"
)

// sweepStartupExecutionOrphans removes execution containers a previous Relay
// process on THIS node identity left behind (a crash mid-invocation), before
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
// process may already own NODE_ID and its containers must not be disturbed.
// Execution containers are never adopted by a replacement (the runtime pool is
// in-process, so a replacement always creates new ids), which is what makes
// capturing the ids before the proof safe. The bounded residual race after a
// passing proof is documented on guardIdentitySnapshotCleanup.
func sweepStartupExecutionOrphans(
	lifecycle context.Context,
	startupCtx context.Context,
	logger *slog.Logger,
	manager *runtime.Manager,
	nodeID string,
	proof ownershipProof,
) {
	orphanCtx, orphanSpan := tracing.Start(startupCtx, "orphan.sweep")
	sweepCtx, sweepCancel := context.WithTimeout(orphanCtx, reconcileTimeout)
	sweepResult, sweepErr := retryIdentitySnapshotCleanup(
		sweepCtx, proof, logger, "orphan-sweep",
		func(stepCtx context.Context) ([]runtime.OrphanContainer, error) {
			return manager.DiscoverOrphanContainers(stepCtx, nodeID)
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

// startupHousekeeper groups the background startup cleanup passes so their
// ordering and lifecycle behavior are deterministic to test without Docker.
// exclusive is the coordinator's housekeeping seam (see
// ServiceCoordinator.RunExclusive): it atomically waits for current service work
// to settle, PAUSES scheduling of new requests for the duration of the callback,
// and resumes/schedules the latest coalesced desired states afterward. The three
// passes run inside that exclusive window in the safe order — orphan container
// sweep, then image sweep (whose keep-set must observe the settled service
// containers), then dependency GC (which prunes dependency images the image
// sweep orphaned) — so no live UpdateServices can race them.
type startupHousekeeper struct {
	exclusive func(context.Context, func(context.Context)) error
	sweep     func(context.Context)
	images    func(context.Context)
	deps      func(context.Context)
}

// startStartupHousekeeping launches the lifecycle-aware startup housekeeping in a
// background goroutine and returns a channel closed when it completes. Startup
// must NOT block on it: Run publishes the initial desired states and returns
// immediately, and only this pass waits at the coordinator barrier for the
// initial service attempts to settle before touching containers or images. Its
// lifecycle is the worker lifecycle, so shutdown cancels an in-flight wait/sweep
// promptly. A barrier failure (lifecycle cancellation or a bounded wait that did
// not clear) skips the sweeps: image/orphan cleanup must never run against an
// unconverged or shutting-down world. An update arriving while the exclusive
// callback runs is coalesced as a pending desired state and scheduled only once
// the callback returns, so it can never run concurrently with a sweep.
func startStartupHousekeeping(
	lifecycle context.Context,
	logger *slog.Logger,
	h startupHousekeeper,
) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		err := h.exclusive(lifecycle, func(hctx context.Context) {
			h.sweep(hctx)
			h.images(hctx)
			h.deps(hctx)
		})
		if err == nil {
			return
		}
		// A cancelled lifecycle is the normal shutdown path, not a barrier
		// failure: log it at Debug and note the sweeps were skipped (they must
		// never run against a shutting-down world).
		if lifecycle.Err() != nil {
			logger.Debug("Startup: housekeeping stopped by shutdown", "error", err)
		} else {
			logger.Warn("Startup: service reconciliation barrier failed", "error", err)
		}
	}()
	return done
}

// sweepStartupImages removes Relay-owned images that no longer correspond to a
// live app version, after every current image is built (reused if
// unchanged) and services are converged. The keep-set holds (a) the exact image
// each app was actually prepared with this boot (Prepared.Image, not a
// re-derived expected tag), (b) images the running service containers
// reference. It also keeps (c) every last-active image recorded in state — the
// crash guard for a swap that started but whose RecordReconcileSuccess never
// landed, where the recorded image may still be the one serving.
//
// The recorded-image gather is the full state listing rather than only the
// loaded set: an app whose desired definition is PRESENT but INVALID is not
// part of the loaded set (it is never prepared or registered), yet it may still
// have a previously-serving active image that must not be swept. PruneRemoved
// has already dropped rows for genuinely absent apps, so every remaining
// row corresponds to an app present on disk (or one whose stat transiently
// failed, which is conservatively kept); keeping its recorded image is the safe
// choice. This affects GC only — never execution, which uses the runtime
// registry of valid apps.
//
// Keeping Prepared.Image is deliberate: the image each app is actually
// serving this boot is the authoritative answer, and it needs no tree scan — a
// app whose build failed (unavailable) contributes no prepared image, and
// its previous serving image is covered by the recorded state image. This
// replaces a re-hash of every app tree purely to reconstruct an expected
// tag, so a build that reused an image or a no-runtime app costs nothing
// here.
//
// When state is nil (DB failed to open) the recorded images are unavailable, so
// the sweep below is skipped entirely: we cannot distinguish a removed app
// from a live one, and orphan removal must never run against an unknown world.
// Conservative: nothing that might still serve is ever removed.
func sweepStartupImages(
	lifecycle context.Context,
	manager *runtime.Manager,
	prepared []*runner.PreparedApp,
	st *state.State,
	logger *slog.Logger,
) {
	// The exact image each prepared app is serving this boot. This is the
	// authoritative keep input: no fingerprint re-scan, no expected-tag
	// reconstruction.
	preparedImages := make([]string, 0, len(prepared))
	for _, pf := range prepared {
		if p := pf.Prepared(); p != nil && p.Image != "" {
			preparedImages = append(preparedImages, p.Image)
		}
	}

	// Images referenced by any Relay-owned service container are kept too: a
	// container kept by the Applys above (unchanged image) or left over from a
	// previous boot that this boot has not yet replaced references its image by
	// label, and the sweep must not remove an image a running container depends
	// on. Removal is deferred to the owning app's reconcile, which replaces
	// the container first and only then retires the image.
	svcCtx, cancel := context.WithTimeout(lifecycle, reconcileTimeout)
	svcContainers, err := manager.ServiceContainerList(svcCtx)
	cancel()
	var serviceImages []string
	if err == nil {
		serviceImages = make([]string, 0, len(svcContainers))
		for _, container := range svcContainers {
			serviceImages = append(serviceImages, container.Image)
		}
	} else {
		logStartupCleanupFailure(lifecycle, logger, "Service: keep-set list failed; continuing without", err)
	}
	// The state keep-set is only armed when the DB was available. When st is
	// nil recordedImages stays empty, so the keep-set falls back to the prepared
	// and service images alone (and the sweep below is skipped entirely).
	var recordedImages []string
	if st != nil {
		for _, img := range st.ActiveImages() {
			if img != "" {
				recordedImages = append(recordedImages, img)
			}
		}
	}

	keep := startupImageKeepSet(preparedImages, serviceImages, recordedImages)
	if st != nil {
		// The image removal pass is a Docker listing plus one removal per
		// orphaned image; bound it with its own fresh reconcileTimeout so a slow
		// daemon cannot make the exclusive housekeeping window — and every
		// service update coalesced behind it — wait unbounded.
		removeCtx, cancel := context.WithTimeout(lifecycle, reconcileTimeout)
		_, err := manager.RemoveImagesExcept(removeCtx, keep)
		cancel()
		if err != nil {
			logStartupCleanupFailure(lifecycle, logger, "Image cleanup: startup sweep failed", err)
		}
	}
}

// liveAppNames builds the startup service-orphan sweep's present-app set from
// the loader's output: every valid app plus every PRESENT app candidate reported
// as a LoadIssue (an invalid path or an invalid/unreadable template). A
// present-but-invalid candidate is still an app on disk, so its service
// containers must be preserved rather than treated as an absent app's
// leftovers. A directory without template.yaml is not an app candidate and is
// never an issue, so its containers are swept like a removed app's. Relay's
// staging directories and directories that vanished mid-load are never issues
// either. Loader diagnostics are the only input: the persisted state view must
// not decide container cleanup.
func liveAppNames(apps []app.App, issues []app.LoadIssue) map[string]bool {
	live := make(map[string]bool, len(apps)+len(issues))
	for _, fn := range apps {
		live[fn.Name] = true
	}
	for _, issue := range issues {
		live[issue.Name] = true
	}
	return live
}

// sweepStartupServiceOrphans runs the startup service-orphan sweep under its own
// fresh reconcileTimeout rooted in the housekeeping lifecycle, so a slow daemon
// cannot make the exclusive housekeeping window (and every service update
// coalesced behind it) wait unbounded. It preserves the ServiceReconciler's
// behavior (app-live-filtered stale containers, list-then-stop) but changes the
// ORDER to the snapshot-first, proof-second, remove-third sequence
// (guardIdentitySnapshotCleanup): it SNAPSHOTS the stale candidates, re-proves
// the pinned ownership lock, then stops/removes exactly the captured
// containers — never re-listing after the proof. A loss latched by the monitor
// (or a failed/ambiguous fresh proof) skips the removal entirely, so a
// replacement owner's containers are never disturbed. The bound matches the
// per-app Applys; the reconciler logs a list/stop failure itself, and any error
// returned by the guard is surfaced here with the startup cleanup convention.
//
// A transient list/stop failure is re-driven a bounded number of times through
// retryIdentitySnapshotCleanup: each attempt rediscovers a fresh snapshot,
// re-proves ownership, and removes exactly that attempt's captured IDs, all
// within the same bound and never after a loss or cancellation. The aggregate
// counts are logged once per sweep (the reconciler's own per-pass log remains
// the authoritative removal detail), so startup gains a summary rather than
// per-container success spam. The reconciler itself is worker-wired with
// WithNodeID, so discovery only ever captures THIS worker's service
// containers.
func sweepStartupServiceOrphans(
	hctx context.Context, svcCtrl *reconciler.ServiceReconciler, liveNames map[string]bool,
	proof ownershipProof, logger *slog.Logger,
) {
	sweepCtx, cancel := context.WithTimeout(hctx, reconcileTimeout)
	defer cancel()
	result, err := retryIdentitySnapshotCleanup(
		sweepCtx, proof, logger, "startup-service-orphan-sweep",
		func(stepCtx context.Context) ([]runtime.ServiceContainer, error) {
			return svcCtrl.DiscoverOrphanContainers(stepCtx, liveNames)
		},
		func(stepCtx context.Context, stale []runtime.ServiceContainer) (int, error) {
			if rerr := svcCtrl.RemoveOrphanContainers(stepCtx, stale); rerr != nil {
				// The reconciler already logged the failure; report the count
				// only for a fully successful removal so the aggregate never
				// inflates a partial pass. The next attempt rediscovers the
				// leftovers.
				return 0, rerr
			}
			return len(stale), nil
		},
	)
	if err != nil {
		// The reconciler already emitted its existing Warn for the underlying
		// list/stop failure. This worker-level classification only downgrades a
		// cancellation-driven error to Debug, matching the sibling startup
		// sweeps, so shutdown does not gain a second warning; it does not imply
		// that cancellation logging is Debug everywhere (the reconciler's Warn
		// above is unconditional).
		logStartupCleanupFailure(hctx, logger, "Startup: service orphan sweep failed", err)
		return
	}
	if result.removed > 0 {
		logger.Info("Startup: service orphan sweep complete",
			"discovered", result.discovered,
			"removed", result.removed,
			"attempts", result.attempts,
		)
		return
	}
	logger.Debug("Startup: service orphan sweep complete",
		"discovered", result.discovered,
		"attempts", result.attempts,
	)
}

// networkVerifier is the narrow view of the runtime Manager that the startup
// NETWORKS pre-flight needs. *runtime.Manager satisfies it; a test fake can
// implement it, so the verification error semantics are unit-testable without a
// Docker daemon (matching the preflight seam's shape).
type networkVerifier interface {
	VerifyNetworks(ctx context.Context, networks []string) (string, bool, error)
}

// verifyConfiguredNetworks verifies every network in the worker-global NETWORKS
// set exists on the Docker daemon before any app is prepared or any
// container created. A missing network, or a verify error (a broken daemon), is
// a fatal startup failure: Relay never creates networks, and an execution
// container silently created on the wrong network would be a latent runtime
// fault. It is a no-op when networks is empty, so an unset NETWORKS keeps the
// default bridge behavior. The check is bounded by the worker lifecycle and a
// reconcileTimeout so a hung daemon cannot stall startup forever.
func verifyConfiguredNetworks(ctx context.Context, manager networkVerifier, networks []string) error {
	if len(networks) == 0 {
		return nil
	}
	verifyCtx, cancel := context.WithTimeout(ctx, reconcileTimeout)
	defer cancel()
	missing, ok, err := manager.VerifyNetworks(verifyCtx, networks)
	if err != nil {
		return fmt.Errorf("verify NETWORKS: %w", err)
	}
	if !ok {
		return fmt.Errorf("NETWORKS network %q does not exist (relay never creates networks)", missing)
	}
	return nil
}

// startupImageKeepSet computes the set of image references the startup sweep
// must keep: (a) the exact image each app was actually prepared with this
// boot (Prepared.Image), (b) every image a running service container
// references, and (c) every last-active image recorded for an app still on
// disk — the crash guard for a swap that started but whose
// RecordReconcileSuccess never landed, where the recorded image may still be the
// one serving.
//
// It takes the ALREADY-RESOLVED prepared image references rather than
// re-hashing each app tree to reconstruct an expected tag: the prepared
// refs are the authoritative "what is live right now" and cost no filesystem
// work, whereas a fingerprint re-scan would duplicate the startup hash and could
// even disagree with what was actually built. A no-runtime app has no
// prepared image and is naturally absent; any image a previous (runtime) version
// built is covered by its recorded last-active image. It is a pure function so
// the keep-set policy is unit testable without Docker or a state DB; blank image
// entries are ignored.
func startupImageKeepSet(preparedImages, serviceImages, recordedImages []string) map[string]bool {
	keep := make(map[string]bool)
	for _, img := range preparedImages {
		if img != "" {
			keep[img] = true
		}
	}

	for _, img := range serviceImages {
		if img != "" {
			keep[img] = true
		}
	}

	for _, img := range recordedImages {
		if img != "" {
			keep[img] = true
		}
	}

	return keep
}

// cleanupStartupDependencies runs dependency GC at startup. The startup image
// sweep may remove superseded app images; dependency cleanup can then
// remove dependency images that no managed app image references anymore.
// It runs OUTSIDE the st != nil gate: unlike RemoveImagesExcept, it needs no
// state keep-set — ownership is derived from the managed-image labels the builds
// just stamped. Best-effort and single-shot: an error is logged and left for the
// next natural lifecycle point; it never retries in a loop.
func cleanupStartupDependencies(lifecycle context.Context, manager *runtime.Manager, logger *slog.Logger) {
	// A fresh reconcileTimeout bound per pass, rooted in the housekeeping
	// lifecycle: like the orphan/image sweeps it runs inside the exclusive
	// window, so an unbounded dependency GC would hold every coalesced service
	// update behind it. It is still rooted in the lifecycle, so shutdown cancels
	// it promptly as well.
	depCtx, cancel := context.WithTimeout(lifecycle, reconcileTimeout)
	defer cancel()
	if _, err := manager.CleanupUnusedDependencies(depCtx); err != nil {
		logStartupCleanupFailure(lifecycle, logger, "Dependency image cleanup failed", err)
	}
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
	prepared := prepareApps(prepareCtx, manager, startup.records, st, startup.fencingTokens, logger)
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
	enqueueStartupServicesWithState(lifecycle, prepared, services, st, startup.fencingTokens, logger)
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
				// stale service containers for THIS node's identity on the
				// shared daemon (the ServiceReconciler is wired with
				// WithNodeID, so another worker's service containers are never
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

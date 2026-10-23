package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/trace"

	"relay/internal/app"
	"relay/internal/cron"
	"relay/internal/observability/metrics"
	"relay/internal/reconciler"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/state"
)

// startAppReconciler establishes app change detection, seeds the startup
// fingerprints, and starts the reconciler plus its shutdown barrier. The caller
// owns the phase span, so it can close at the same boundary as before.
//
// PrepareWatch runs BEFORE any seed: a change after the watch is installed is
// observed as an fsnotify event, and a change between the fingerprint scan and
// the watch installation is still caught because the seeded value is the OLDER
// one. If the watcher cannot be created, the supplied fingerprints are NOT
// trusted: seeding is skipped entirely and Start is not retried, so a later
// successful watcher construction cannot run against an unseeded reconciler.
//
// Seeding uses the identity each app's image was ACTUALLY built from
// (startupSeedFingerprints), so the first reconcile compares the live tree
// against what is really baked. Start runs in its own goroutine and exits on
// lifecycle cancellation; reconcilerDone is the shutdown barrier that
// guarantees no reconcile can still use the runtime manager or state DB when
// those are closed.
func startAppReconciler(
	lifecycle context.Context,
	logger *slog.Logger,
	span trace.Span,
	rec *reconciler.Reconciler,
	apps []app.App,
	fingerprints map[string]string,
	fencingTokens map[string]state.FencingToken,
	prepared []*runner.PreparedApp,
	shutdown *shutdownRegistry,
	onFailure func(error),
) {
	reconcilerDone := make(chan struct{})
	if err := rec.PrepareWatch(lifecycle); err != nil {
		// Do not let Start retry watcher construction: if that retry succeeded,
		// the reconciler would run without the startup identities that are only
		// safe to seed after the watch is installed.
		close(reconcilerDone)
		shutdown.register(reconcilerBarrierStep(reconcilerDone))
		if lifecycle.Err() == nil {
			logger.Error("Reconciler: watch setup failed", "error", err)
			span.RecordError(err)
			onFailure(fmt.Errorf("prepare app watch: %w", err))
		}
		return
	}

	seedStart := time.Now()
	seeds := startupSeedFingerprints(apps, fingerprints, prepared)
	for _, fn := range apps {
		rec.Seed(fn, seeds[fn.Name])
	}
	// Seed the EXACT fencing token each startup discovery/invalid write
	// returned. This lets the first live observation reuse that generation
	// instead of allocating a new one (or reading back another worker's
	// token), and it includes present-but-invalid desired definitions so their
	// live removal can still be fencing-token-guarded.
	for name, fencingToken := range fencingTokens {
		rec.SeedFencingToken(name, fencingToken)
	}
	logger.Debug("Startup: seeded startup fingerprints",
		"count", len(apps),
		"duration", time.Since(seedStart),
	)
	logger.Info("Watching apps for changes", "root", app.Dir)

	go func() {
		defer close(reconcilerDone)
		// A normal return is lifecycle cancellation. A non-nil error is either
		// watcher construction failure or a terminal watcher/channel failure
		// after the loops started; Start has already joined them, so no
		// reconcile can still use the runtime manager or state DB. Surface it
		// loudly rather than leaving a worker that believes it is watching apps
		// while live reload has silently stopped.
		runReconcilerStart(lifecycle, rec.Start, func(err error) {
			logger.Error(
				"Reconciler: watch loop stopped",
				"error", err,
			)
			onFailure(err)
		})
	}()
	shutdown.register(reconcilerBarrierStep(reconcilerDone))
}

// runReconcilerStart treats lifecycle cancellation as an ordinary shutdown and
// reports only a terminal Start failure. Start joins its loops before returning,
// so onFailure runs after the reconciler has stopped using shared dependencies.
func runReconcilerStart(
	lifecycle context.Context,
	start func(context.Context) error,
	onFailure func(error),
) {
	err := start(lifecycle)
	if err == nil || lifecycle.Err() != nil ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	onFailure(err)
}

// reportReconcilerFailure latches a terminal reconciler error before cancelling
// the lifecycle, so Run can distinguish that cancellation from normal shutdown.
func reportReconcilerFailure(
	lifecycle context.Context,
	cancel context.CancelFunc,
	failures chan<- error,
	err error,
) {
	if lifecycle.Err() != nil {
		return
	}
	failures <- fmt.Errorf("reconciler failed: %w", err)
	cancel()
}

// reconcilerRunResult preserves an independent Consume failure while surfacing
// a terminal reconciler failure after shutdown has joined Start.
func reconcilerRunResult(runErr error, failures <-chan error) error {
	select {
	case err := <-failures:
		if runErr == nil {
			return err
		}
		return errors.Join(runErr, err)
	default:
		return runErr
	}
}

// appReconcilerConfig builds the reconciler's callback wiring: the live
// registry swap and image retirement hooks, the per-app
// metrics/baseline/warm-pool/schedule convergence on removal, the schedule and
// service convergence hooks used on discovery and update, and the
// remove-services-before-image-retirement ordering. It only builds the Config;
// startAppReconciler owns PrepareWatch, the conditional startup seeding, and
// the Start goroutine/barrier so the watch/seeding ordering stays visible in
// one place.
func appReconcilerConfig(
	root string,
	st *state.State,
	runWorker *runner.Runner,
	metricsInstance *metrics.Registry,
	statsFlusher *statsFlusher,
	manager *runtime.Manager,
	sched *cron.Scheduler,
	services *reconciler.ServiceCoordinator,
) reconciler.Config {
	return reconciler.Config{
		Root:   root,
		State:  st,
		Retire: func(_ string, oldImage string) { runWorker.RetireImage(oldImage) },
		// The periodic audit re-drives deferred image cleanup: an image
		// whose bounded removal retries were exhausted while a service
		// container still referenced it (or whose removal failed) is retried
		// here once the reference state may have changed, so it is not left
		// behind when the initial retry window elapsed. Coalesced and
		// idempotent, and a no-op when nothing is deferred.
		Audit: func() { runWorker.RetryDeferredImageRemovals() },
		RemoveApp: func(name string) {
			metricsInstance.RemoveApp(name)
			// Drop the app's reset baseline too, so a re-added app
			// is not offset by a stale pre-removal total.
			statsFlusher.dropAppBaseline(name)
			// Drop the app's warm container state first: no new acquire
			// may warm a removed app, idle containers are discarded now,
			// and busy ones are discarded on release. Then retire every image
			// version once idle (the runner's reference guard keeps an image
			// an in-flight execution still needs).
			manager.RemoveApp(name)
			runWorker.RemoveAppSemaphore(name)
			runWorker.RemoveAppImages(name)
			sched.RemoveApp(name) // a removed app never keeps firing
		},
		UpdateSchedules: func(name string, tmpl *app.Template) {
			sched.ReplaceApp(name, tmpl)
		},
		// Converge the app's persistent service containers whenever its
		// new version is swapped in (and on the skip path when it declares
		// services, so crashed replicas self-heal on the periodic tick). The
		// hook runs synchronously in the reconciler pump goroutine.
		//
		// Apply receives the LIFECYCLE context, NOT a 30s budget wrapped
		// around the whole pass: the ServiceReconciler injects the worker's
		// reconcileTimeout into Reconcile, which derives a fresh bound for
		// each normal Docker operation itself. Shutdown still
		// cancels the operation promptly because ctx is the signal context.
		//
		// The prepared env comes from the current registry entry (the runtime
		// plan env); a nil Prepared (unavailable) falls back to no plan env,
		// mirroring the runner's nil-safe behavior.
		UpdateServices: func(name string, tmpl *app.Template, image string) {
			enqueueLiveServices(
				services,
				manager,
				runWorker.Registry(),
				name,
				tmpl,
				image,
			)
		},
		UpdateServicesWithStatus: func(
			name string,
			tmpl *app.Template,
			image string,
			onReconcileStart func(),
			onComplete func(error),
		) {
			enqueueLiveServicesWithStatus(
				services,
				manager,
				runWorker.Registry(),
				name,
				tmpl,
				image,
				onReconcileStart,
				// A service convergence completing is exactly the lifecycle
				// moment an old image's last service container may have been
				// replaced, so it is the precise re-drive point for a
				// deferred image retirement. It runs after the caller's own
				// completion bookkeeping and only admits async cleanup, so it
				// never blocks or changes the completion outcome.
				func(err error) {
					if onComplete != nil {
						onComplete(err)
					}
					runWorker.RetryDeferredImageRemovals()
				},
			)
		},
		UpdateServicesObservationWithStatus: func(
			name string,
			tmpl *app.Template,
			image string,
			onReconcileStart func(),
			onComplete func(error),
		) {
			enqueueLiveServiceObservationWithStatus(
				services,
				manager,
				runWorker.Registry(),
				name,
				tmpl,
				image,
				onReconcileStart,
				// See UpdateServicesWithStatus: a converged service pass is
				// the re-drive point for a deferred image retirement. The
				// periodic audit path converges services too, so a deferred
				// image is retried on that cadence as well.
				func(err error) {
					if onComplete != nil {
						onComplete(err)
					}
					runWorker.RetryDeferredImageRemovals()
				},
			)
		},
		// On removal, stop the app's service containers BEFORE the images
		// are retired (reconciler calls RemoveServices before RemoveApp):
		// running service containers reference those images. RemoveAndWait
		// waits DETERMINISTICALLY for the queued removal to complete; the
		// coordinator derives the operation's own fresh 30s bound rooted in
		// the lifecycle context, and shutdown releases the wait via the
		// lifecycle, so the hook can never let image retirement race the
		// removal. The hook runs in the single pump goroutine, so it never
		// blocks a reconcile of another app.
		RemoveServices: func(name string) {
			services.RemoveAndWait(name)
		},
	}
}

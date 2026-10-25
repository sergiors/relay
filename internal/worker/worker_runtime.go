package worker

import (
	"context"
	"log/slog"
	"time"

	"relay/internal/config"
	"relay/internal/observability/metrics"
	"relay/internal/runtime"
	"relay/internal/state"
)

// openRuntimeManager constructs the Docker runtime manager and registers its
// context-aware cleanup immediately on a successful open. It is the
// preflight's OpenManager step: the manager is opened with deferred maintenance
// so no manager background loop runs before NETWORKS verification, and its
// cleanup step is registered BEFORE runExternalPreflight returns so a later
// preflight failure (NETWORKS verification) still converges through the
// deferred shutdown.
//
// st is the REQUIRED state handle opened earlier in Run. The manager's
// cross-process dependency-image coordination is built on it: dependency layers
// live on the Docker daemon, which may be shared by several Relay processes, so
// the only authority that can make a layer unremovable across them is the
// database they share. A nil handle leaves the manager uncoordinated (a build
// that consumes a dependency image and GC fail closed); Run always passes the
// opened handle.
//
// lifecycle is the worker's signal context, not the preflight step's span
// context: builds and the startup ping must be cancelled by SIGTERM/SIGINT, and
// the startup span is used only for the preflight's phase span.
func openRuntimeManager(
	lifecycle context.Context,
	logger *slog.Logger,
	metricsInstance *metrics.Registry,
	cfg config.Config,
	st *state.State,
	shutdown *shutdownRegistry,
) (*runtime.Manager, error) {
	manager, err := runtime.NewManager(
		logger,
		metricsInstance,
		cfg.WorkerID,
		runtime.WithWarmContainerIdleTimeout(cfg.WarmContainerIdleTimeout),
		// The SAME MAX_CONCURRENT_INVOCATIONS the runner's global semaphore uses:
		// the runtime clips each app's effective per-app
		// concurrency to it, so a template asking for more than the
		// worker-global cap (e.g. 15 with MAX_CONCURRENT_INVOCATIONS=8) warms,
		// reports, and admits only the cap's worth. It is startup
		// configuration; a global change requires a worker restart.
		runtime.WithMaxConcurrentInvocations(cfg.MaxConcurrentInvocations),
		// The per-worker cap (MAX_CONCURRENT_BUILDS) on dependency and app
		// image builds running concurrently. It is held only while a build
		// actually runs, so non-build preparation (source
		// selection/snapshot, dependency snapshot, reuse probes) does not
		// consume a slot. The same Manager limiter bounds both startup and
		// live reconcile preparations, independent of invocation concurrency
		// and of the local event buffer. It is startup configuration; a
		// change requires a worker restart.
		runtime.WithMaxConcurrentBuilds(cfg.MaxConcurrentBuilds),
		// The worker-global hard bound (MAX_WARM_CONTAINERS) on warm
		// EXECUTION containers this worker keeps across all apps. It
		// bounds the pooled execution-container population (idle + busy),
		// independent of invocation concurrency; persistent service
		// containers are outside it. It is startup configuration; a
		// change requires a worker restart.
		runtime.WithMaxWarmContainers(cfg.MaxWarmContainers),
		// The worker-global Docker networks (NETWORKS) every execution
		// container joins at create time. They are verified by the next
		// preflight step before any app is prepared or any container
		// created.
		runtime.WithNetworks(cfg.Networks),
		// HOT_RELOAD: bind-mount the live app source read-only for a
		// runtime whose dependency layout permits it (Python and Node),
		// instead of baking it into the image, so a source-only edit
		// takes effect without rebuilding. It is the public trigger for
		// the runtime's internal source-mount mechanism. Startup
		// configuration; a change requires a worker restart.
		runtime.WithSourceMount(cfg.HotReload),
		// Root Dockerfile builds in the WORKER LIFECYCLE (lifecycle), not the
		// startup-span context: they get an independent 10m bound
		// (runtime.buildTimeout) but are still cancelled when Relay shuts
		// down. Builds must NOT inherit the short 30s reconcile budget
		// the worker uses for normal service operations.
		runtime.WithLifecycleContext(lifecycle),
		// Cross-process dependency-image coordination. The per-Manager lease
		// coordinator only protects this process's own uses; a dependency layer
		// lives on the Docker daemon, which several Relay processes may share, so
		// the shared state database is the only place a cross-process shared
		// use / exclusive GC decision can be made. Any Relay processes sharing one
		// Docker daemon must therefore use the same DATABASE_URL database (see
		// README/docs/operations.md).
		runtime.WithDependencyImageCoordinator(newStateDependencyImageCoordinator(st)),
		// Do NOT start the warm-container maintenance loop in the
		// constructor: the manager is opened before NETWORKS is
		// verified, and the request is that all configured networks
		// validate before ANY background loop starts. The loop is
		// started by the final preflight step (StartMaintenance) only
		// after verification succeeds; if it fails, the manager (and its
		// Docker client) is still torn down by the registered shutdown
		// step with no loop ever started.
		runtime.WithDeferredMaintenance(),
	)
	if err != nil {
		return nil, err
	}
	// Manager cleanup is context-aware, so the Docker client is only
	// closed after every worker-owned goroutine has stopped and every
	// warm container has been torn down (in parallel, bounded) or the
	// step's finite bound expires.
	shutdown.register(shutdownStep{
		name:    shutdownStepManager,
		timeout: 30 * time.Second,
		run:     manager.CloseContext,
	})
	return manager, nil
}

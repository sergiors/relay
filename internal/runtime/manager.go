package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/moby/moby/client"

	"relay/internal/function"
	"relay/internal/metrics"
	"relay/internal/runtime/node"
	"relay/internal/runtime/plan"
	"relay/internal/runtime/python"
	"relay/internal/source"
)

// arch and platform are the build-host architecture keys for the dependency
// fingerprint. They default to the running process's GOARCH/GOOS (the daemon
// this process builds on is the daemon it executes on, so the build host is the
// correct key). Making them variables lets tests inject synthetic architectures.
var (
	arch     = runtime.GOARCH
	platform = runtime.GOOS
)

// engineFor returns the engine that prepares a spec. A single engine serves
// every version registered for its language. The handler-module list is passed
// to Plan so the Node engine can transpile TypeScript handlers at build time;
// the Python engine ignores it.
func engineFor(spec plan.Spec) (interface {
	Plan(plan.Spec, string, []string) (plan.BuildPlan, error)
}, error) {
	switch spec.Engine {
	case plan.EnginePython:
		return python.Engine{}, nil
	case plan.EngineNode:
		return node.Engine{}, nil
	default:
		return nil, fmt.Errorf("runtime %q: no engine for %q", spec.Name, spec.Engine)
	}
}

// DefaultWarmContainerIdleTimeout is the idle-eviction window applied by
// NewManager when no WithWarmContainerIdleTimeout option is given. It mirrors
// config.DefaultWarmContainerIdleTimeout (this leaf package cannot import
// config); the worker always passes config's resolved value explicitly.
const DefaultWarmContainerIdleTimeout = 5 * time.Minute

// DefaultMaxConcurrency is the worker-global concurrency cap applied by
// NewManager when no WithMaxConcurrency option is given. It mirrors
// config.DefaultMaxConcurrency and runner.DefaultMaxConcurrency (this leaf
// package cannot import either); the worker always passes config's resolved
// value explicitly, and a Manager constructed directly by tests leaves
// maxConcurrency zero (treated as this default, never "uncapped").
const DefaultMaxConcurrency = 8

// Manager prepares function images and executes handler invocations. It owns a
// single Docker Engine client, reused for every build and invocation, and a
// per-function warm container pool: each function keeps up to its resolved
// concurrency reused containers per image version (see container_cache.go),
// kept alive between invocations and leased one-per-invocation, and discarded
// on timeout/process exit/protocol error/image change/function removal/shutdown
// or evicted when it has been idle longer than the configured idle timeout.
type Manager struct {
	log *slog.Logger
	cli *client.Client
	// metrics is an optional observability registry. A nil registry disables
	// all metric recording; every call is a no-op.
	metrics *metrics.Registry
	// hostname identifies this worker for container ownership. It is the same
	// value as the Redis consumer identity (config.ConsumerName), so Relay's
	// container-label hostname and its stream consumer identity are one and the
	// same. The startup orphan sweep uses it to distinguish this worker's
	// stalled containers from those of every other worker sharing the daemon.
	hostname string
	// maxConcurrency is the worker-global concurrency cap (MAX_CONCURRENCY): the
	// SAME value the runner uses for its global semaphore. It clips every
	// function's effective per-function concurrency to
	// min(template concurrency, maxConcurrency) in Prepare/Execute, so the warm
	// pool, its capacity gauge, PoolSnapshot/the CLI, and the runner's
	// per-function semaphore all agree on one effective bound. It is set once at
	// construction (WithMaxConcurrency; the worker wires config's resolved
	// value) and read without the lock: MAX_CONCURRENCY is startup
	// configuration and is NOT hot-reloadable, so a global change requires a
	// worker restart. A zero value falls back to DefaultMaxConcurrency, never
	// "uncapped" (a Manager constructed directly by tests behaves like the
	// default runner).
	maxConcurrency int
	// containers caches the per-function reusable execution containers.
	containers *containerCache
	// lifecycle is the manager's lifecycle context: Dockerfile builds are
	// rooted here (see buildContext), so a long build is bounded by buildTimeout
	// but still cancelled when Relay shuts down, without ever inheriting a
	// caller's short reconcile budget. It is derived at construction from
	// WithLifecycleContext (the worker's signal ctx); lifecycleCancel is owned
	// by Close so a build in flight during Close is cancelled too. A Manager
	// constructed directly by tests may leave both nil, in which case
	// buildContext roots builds at context.Background.
	lifecycle       context.Context
	lifecycleCancel context.CancelFunc
	// now is the injectable clock seam. It defaults to time.Now and is used by
	// the external-image pull throttle (see service_source.go). It is a Manager
	// field, never a package global, so a test can advance time deterministically
	// without mutating shared state.
	now func() time.Time
	// pullChecks records, per service image identity, the instant of the last
	// SUCCESSFUL remote pull check. It enforces the hourly-at-most cadence for
	// external service images and is in-memory only (never persisted). Guarded by
	// pullMu.
	pullMu     sync.Mutex
	pullChecks map[string]time.Time

	// done is closed by Close to stop the single maintenance loop (the only
	// eviction driver; there is never a ticker or goroutine per container).
	done chan struct{}
	// maintDone is closed by the maintenance loop when it exits.
	maintDone chan struct{}
	// closeOnce makes Close idempotent and keeps the maintenance loop's stop
	// handshake single-fire.
	closeOnce sync.Once
	// closeErr stores the client-close result so repeated Close calls are
	// idempotent.
	closeErr error
}

// ManagerOption tunes NewManager. Options keep the three-argument constructor
// backward-compatible for every existing caller while letting the worker pass
// its resolved environment configuration without the runtime reading env.
type ManagerOption func(*managerOptions)

// managerOptions is the resolved configuration applied by NewManager.
type managerOptions struct {
	// idleTimeout is the warm-container idle-eviction window. Zero means the
	// package default (DefaultWarmContainerIdleTimeout).
	idleTimeout time.Duration
	// maxConcurrency is the worker-global concurrency cap. Zero means the
	// package default (DefaultMaxConcurrency), matching the runner's
	// normalization.
	maxConcurrency int
	// now is the injectable clock seam for deterministic tests. Nil means the
	// wall clock.
	now func() time.Time
	// lifecycle roots the manager's lifecycle context (see WithLifecycleContext).
	// Nil means the manager starts its own Close-cancelled root.
	lifecycle context.Context
}

// WithWarmContainerIdleTimeout sets how long a healthy idle warm execution
// container is kept before the maintenance loop evicts it. A non-positive value
// is treated as the package default, so a caller cannot accidentally disable
// eviction; config.Load rejects non-positive values before they reach here.
func WithWarmContainerIdleTimeout(idleTimeout time.Duration) ManagerOption {
	return func(o *managerOptions) { o.idleTimeout = idleTimeout }
}

// WithMaxConcurrency sets the worker-global concurrency cap (MAX_CONCURRENCY).
// It is the SAME value the worker passes to runner.SetMaxConcurrency, so the
// warm pool's effective bound and the runner's per-function semaphore agree.
// A non-positive value is treated as DefaultMaxConcurrency (never "uncapped"),
// matching the runner's normalization. It is startup configuration: changing it
// requires a worker restart (there is no live setter; the runner's global
// semaphore is likewise built once at startup). The worker wires it from
// config; a direct NewManager caller that omits it gets DefaultMaxConcurrency.
func WithMaxConcurrency(n int) ManagerOption {
	return func(o *managerOptions) { o.maxConcurrency = n }
}

// WithLifecycleContext roots the manager's build lifecycle at lifecycle (the
// worker's signal context). Dockerfile builds are bounded by buildTimeout but
// are ALSO cancelled when this context is cancelled, so Relay shutdown stops an
// in-flight build. The manager derives a child of lifecycle, so Close cancels
// builds as well. Omitting the option (or passing nil) roots the lifecycle at
// context.Background, so a direct caller still gets Close-cancellable builds
// without wiring a signal context.
func WithLifecycleContext(lifecycle context.Context) ManagerOption {
	return func(o *managerOptions) { o.lifecycle = lifecycle }
}

// withClock injects a deterministic clock for tests. It is unexported because
// only in-package tests need it; production always uses the wall clock.
func withClock(now func() time.Time) ManagerOption {
	return func(o *managerOptions) { o.now = now }
}

// NewManager connects to the Docker daemon so failures surface at startup
// rather than per event. The client is configured from the environment
// (DOCKER_HOST / DOCKER_TLS_VERIFY / DOCKER_CERT_PATH) and negotiates the API
// version automatically. registry is an optional observability registry; a nil registry
// disables metric recording (every call is a no-op). hostname is this worker's
// hostname-scoped container ownership identity (e.g. config.ConsumerName()); it
// is stamped as the relay.hostname label on every execution container and gates
// the startup orphan sweep.
//
// Options are variadic so the original three-argument call remains valid; the
// worker passes WithWarmContainerIdleTimeout(cfg.WarmContainerIdleTimeout).
// NewManager starts the single warm-container maintenance loop, stopped by
// Close.
func NewManager(
	logger *slog.Logger,
	registry *metrics.Registry,
	hostname string,
	opts ...ManagerOption,
) (*Manager, error) {
	resolved := resolveManagerOptions(opts)

	cli, err := client.New(client.FromEnv)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to Docker daemon: %w", err)
	}
	if _, err := cli.Ping(context.Background(), client.PingOptions{}); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("cannot connect to Docker daemon: %w", err)
	}
	mgr := &Manager{
		log:            logger,
		cli:            cli,
		metrics:        registry,
		hostname:       hostname,
		maxConcurrency: resolved.maxConcurrency,
		done:           make(chan struct{}),
		maintDone:      make(chan struct{}),
		now:            resolved.now,
		pullChecks:     map[string]time.Time{},
	}
	if mgr.now == nil {
		mgr.now = time.Now
	}
	// The build lifecycle: a manager-owned child of the caller's lifecycle (the
	// worker passes its signal ctx via WithLifecycleContext) or of
	// context.Background when none was supplied. Owning a child means Close
	// always cancels in-flight Dockerfile builds, while a cancelled parent (Relay
	// shutdown) propagates too. Builds are bounded by buildTimeout on top of
	// this; they are never rooted in a caller's short reconcile context.
	parent := resolved.lifecycle
	if parent == nil {
		parent = context.Background()
	}
	mgr.lifecycle, mgr.lifecycleCancel = context.WithCancel(parent)
	mgr.containers = newContainerCache()
	mgr.containers.idleTimeout = resolved.idleTimeout
	mgr.containers.now = resolved.now
	mgr.containers.metrics = registry
	mgr.startMaintenance(maintenanceInterval(resolved.idleTimeout))
	return mgr, nil
}

// clock returns the Manager's injectable clock, defaulting to the wall clock
// when a Manager was constructed directly (tests) without an option. It is the
// single time source for the external-image pull throttle, so the cadence is
// deterministic under an injected clock and never consults a package global.
func (m *Manager) clock() time.Time {
	if m.now == nil {
		return time.Now()
	}
	return m.now()
}

// resolveManagerOptions applies the options in order and normalizes a
// non-positive idle timeout to the package default, so NewManager never starts
// the maintenance loop with a disabled eviction window. It is a pure function
// so the resolution logic is unit-testable without Docker.
func resolveManagerOptions(opts []ManagerOption) managerOptions {
	resolved := managerOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&resolved)
		}
	}
	if resolved.idleTimeout <= 0 {
		resolved.idleTimeout = DefaultWarmContainerIdleTimeout
	}
	if resolved.maxConcurrency < 1 {
		resolved.maxConcurrency = DefaultMaxConcurrency
	}
	return resolved
}

// maintenanceInterval derives the single maintenance-loop tick from the idle
// timeout: half the window (so an idle container is evicted within one half
// window of crossing the threshold), capped at one minute so a very large
// timeout still gets prompt eviction, and floored at 10ms so a tiny test
// timeout does not busy-loop. This is the ONLY ticker driving eviction.
func maintenanceInterval(idle time.Duration) time.Duration {
	interval := idle / 2
	if interval > time.Minute {
		interval = time.Minute
	}
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	return interval
}

// startMaintenance launches the single maintenance loop. It is a method so
// tests that construct a Manager directly (bypassing NewManager/Docker) can
// start the loop with a test clock.
func (m *Manager) startMaintenance(interval time.Duration) {
	if m.done == nil {
		m.done = make(chan struct{})
	}
	if m.maintDone == nil {
		m.maintDone = make(chan struct{})
	}
	go m.maintenanceLoop(interval)
}

// maintenanceLoop runs evictIdle on a single ticker until Close, then closes
// maintDone so Close can join it before tearing the cache down. It is the only
// eviction driver: there is no per-container goroutine or ticker.
func (m *Manager) maintenanceLoop(interval time.Duration) {
	defer close(m.maintDone)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.done:
			return
		case <-ticker.C:
			m.containers.evictIdle()
		}
	}
}

// Close stops the maintenance loop, discards every cached execution container
// (reason "shutdown"; each kill/remove runs on bounded, detached contexts so a
// cancelled shutdown ctx cannot strand them), then releases the Docker Engine
// client. It is idempotent and safe to call more than once during shutdown; the
// worker defers it at startup. The loop is joined before the cache is closed so
// a concurrent eviction can never race the shutdown discard.
func (m *Manager) Close() error {
	m.closeOnce.Do(func() {
		// Cancel the manager-owned build lifecycle first: an in-flight
		// Dockerfile build rooted here is cancelled promptly rather than running
		// until buildTimeout. The lifecycle is a manager-owned child of any
		// caller-supplied parent, so this is correct both for a worker build
		// (whose signal ctx is the parent) and for a direct caller.
		if m.lifecycleCancel != nil {
			m.lifecycleCancel()
		}
		if m.done != nil {
			close(m.done)
			if m.maintDone != nil {
				<-m.maintDone
			}
		}
		if m.containers != nil {
			m.containers.close()
		}
		if m.cli == nil {
			// A Manager constructed directly by a test (no Docker) still owns a
			// cache and maintenance loop, so Close must be safe without a client.
			return
		}
		m.closeErr = m.cli.Close()
	})
	return m.closeErr
}

// startContainer builds one fresh execution container for a function version.
// It is the containerCache factory, called with the creating invocation's
// parameters: env is the function's plan env (per-function, applied at
// container create), networks is the template's top-level `networks` list
// (joined at create time), and meta is the creation-time identity RunMeta
// stamped as labels (per-invocation fields left empty — labels are immutable
// while the container outlives invocations).
func (m *Manager) startContainer(
	ctx context.Context,
	fnName, image string,
	env []string,
	networks []string,
	meta RunMeta,
) (reusableContainer, error) {
	return startExecutionContainer(ctx, m.cli, m.log, fnName, image, env, networks, meta)
}

// Prepared is a function whose image has been built.
type Prepared struct {
	Name        string
	Image       string
	Fingerprint string
	// Env are the runtime environment variables the function's engine requires
	// (e.g. PYTHONDONTWRITEBYTECODE for Python). They are applied to every
	// execution container for this function, after the base RELAY_HANDLER var.
	Env []string
	// Concurrency is the function's EFFECTIVE per-function concurrency: the
	// template's resolved `concurrency` clipped to the worker-global
	// MAX_CONCURRENCY (see Manager.effectiveConcurrency). It is the bound on the
	// function's warm container pool: at most this many containers are kept and
	// leased concurrently. It intentionally matches the runner's per-function
	// semaphore (also clipped to the global cap) so the pool is not a second
	// limiter in the runner path; direct Execute callers that bypass the runner
	// are bounded by it. Because MAX_CONCURRENCY is startup configuration, a
	// global change requires a worker restart; a hot-swapped template
	// `concurrency` is re-clipped live on each successful Prepare.
	Concurrency int
	// Dependency is the full "relay-dep-*" reference this function image was
	// built FROM, or "" when the function declares no dependency layer. It is
	// the function image's parent, so a caller (the runner) knows which
	// dependency image this function version pulls its payload from — the input
	// to dependency garbage collection. It is populated on BOTH the build and
	// reuse paths.
	Dependency string
	// Networks is the template's normalized top-level `networks` list: the
	// Docker networks every execution container for this function joins at
	// create time. It is runtime-only configuration: it does NOT affect the
	// image, so changing it never rebuilds (see function.ImageFingerprint) but
	// DOES invalidate warm and service containers for the new networks.
	Networks []string
	// RuntimeGeneration is a digest of the runtime-only configuration (today
	// the Networks list). The warm pool keys container reuse on
	// (Image, RuntimeGeneration), so a runtime-only change replaces warm
	// containers without a rebuild. It is empty when there is no runtime-only
	// configuration.
	RuntimeGeneration string
}

// Prepare builds exactly ONE image for the function's current content (never per
// handler or event), then returns a handle for executing invocations against it.
//
// The image reference is derived from a content fingerprint computed here, so
// the same source always maps to the same fingerprinted image. If that image is
// already present locally (an earlier build or previous boot produced it), the
// build is skipped and the existing image reused — restart-without-changes is
// cheap. A fingerprint error fails Prepare: the reconciler already computes the
// fingerprint before calling Prepare and retains the previous version on error,
// and for startup a fingerprint failure marks the function unavailable, which is
// consistent with the existing build-failure handling.
//
// A successful Prepare (re)activates the function in the warm-container cache,
// clearing any prior removal mark and un-retiring THIS exact image so a
// removed-then-recreated function warms again. Activation is deliberately NOT
// done up front: a failed prepare must not lift a removal, or a stale acquire
// could warm a function the reconciler has not actually reconciled.
func (m *Manager) Prepare(ctx context.Context, fn function.Function) (*Prepared, error) {
	// Resolve the source-selection policy ONCE and share it with the
	// fingerprint and the build context: both must select exactly the same files
	// (the function's .gitignore rules), and resolving a single Selection keeps
	// them from disagreeing if a rule file is edited concurrently.
	selection, err := source.ForDir(fn.Dir)
	if err != nil {
		return nil, fmt.Errorf("function %q: select sources: %w", fn.Name, err)
	}
	fp, err := function.FingerprintSelection(selection)
	if err != nil {
		return nil, fmt.Errorf("function %q: fingerprint: %w", fn.Name, err)
	}
	// imageFP is the IMAGE fingerprint: the same digest with runtime-only template
	// keys (the `networks` list) removed. The image tag is derived from imageFP so
	// a runtime-only edit never yields a new tag and never forces a rebuild; the
	// full fp still gates reconciliation. See function.ImageFingerprint.
	imageFP, err := function.ImageFingerprintSelection(selection)
	if err != nil {
		return nil, fmt.Errorf("function %q: image fingerprint: %w", fn.Name, err)
	}

	// A template that needs no runtime (all of its services use build or image
	// sources, and it has no events or schedules) has no function image to
	// build: the services bring their own images. Prepare still succeeds so the
	// function is available for service convergence, returning a handle with no
	// image — no entrypoint service exists to consume it. The fingerprint is
	// still computed (above) and recorded so content changes gate reconciliation
	// exactly as for a runtime-backed function.
	if !fn.Template.NeedsRuntime() {
		m.log.Debug("Function: no runtime required; services bring their own images",
			"function", fn.Name)
		prepared := &Prepared{
			Name:              fn.Name,
			Fingerprint:       fp,
			Concurrency:       m.effectiveConcurrency(fn),
			Networks:          append([]string(nil), fn.Template.Networks...),
			RuntimeGeneration: fn.Template.RuntimeGeneration(),
		}
		m.containers.activateFunction(fn.Name, "")
		m.containers.setFunctionConcurrency(fn.Name, prepared.Concurrency)
		return prepared, nil
	}

	spec, err := lookup(fn.Template.Runtime)
	if err != nil {
		return nil, fmt.Errorf("function %q: %w", fn.Name, err)
	}

	eng, err := engineFor(spec)
	if err != nil {
		return nil, fmt.Errorf("function %q: %w", fn.Name, err)
	}

	planResult, err := eng.Plan(spec, fn.Dir, templateHandlers(fn))
	if err != nil {
		return nil, fmt.Errorf("function %q: plan: %w", fn.Name, err)
	}

	image := ImageRef(fn.Name, imageFP)

	// bootstrapHash pins the runtime-injected bootstrap content (the engine's
	// embedded plan files) plus the entrypoint onto the image as a label. The
	// fingerprint above covers ONLY the function dir, so the label is what
	// lets the reuse path below detect an image built with a stale bootstrap
	// (e.g. by an older Relay version) under the exact same tag.
	bootstrapLabelHash := bootstrapHash(planResult)

	// Prepare the dependency label reference for the return value on both paths.
	// On the build path it is the dependency image built FROM; on the reuse path
	// it is computed WITHOUT building (the dependency image obviously exists, or
	// the existing function image — which inherits its layers — would never have
	// built). Computing the dependency fingerprint needs the same fnDir reads the
	// function fingerprint above already performed, so it stays cheap.
	prepared := &Prepared{
		Name:              fn.Name,
		Image:             image,
		Fingerprint:       fp,
		Env:               planResult.Env,
		Concurrency:       m.effectiveConcurrency(fn),
		Networks:          append([]string(nil), fn.Template.Networks...),
		RuntimeGeneration: fn.Template.RuntimeGeneration(),
	}
	if !planResult.Deps.IsZero() {
		// Split out the pure fingerprint computation so the reuse path below can
		// name the function image's dependency without touching the daemon.
		depFP, err := DependencyFingerprint(arch, platform, spec, fn.Dir, planResult.Deps)
		if err != nil {
			return nil, fmt.Errorf("function %q: %w", fn.Name, fmt.Errorf("dependency fingerprint: %w", err))
		}
		prepared.Dependency = depImageRef(depFP)
	}

	// Reuse an existing local image when present. The fingerprinted reference is
	// the identity: an image carrying this exact tag was necessarily built from
	// identical source (the tag embeds the fingerprint prefix), so no content
	// comparison is needed.
	if m.imageExists(ctx, image) && m.bootstrapLabelMatches(ctx, image, bootstrapLabelHash) {
		m.log.Debug(
			"Function: image exists; reusing",
			"function", fn.Name,
			"image", image,
		)
		// The prepare succeeded (the image is present and current), so activate
		// the exact image: a previously removed function warms again, and a
		// reverted same-source image is no longer treated as retired.
		m.containers.activateFunction(fn.Name, image)
		// Propagate the reconciled concurrency to the live pool: the effective
		// bound must follow a successful Prepare even when the image was reused
		// (a concurrency-only change rebuilds the same fingerprinted image).
		m.containers.setFunctionConcurrency(fn.Name, prepared.Concurrency)
		return prepared, nil
	}

	// When the function declares a dependency layer, ensure the dependency image
	// exists first and build the function image FROM it. The dependency image is
	// content-addressed (no function name): it is shared across every function
	// and every version with identical (runtime + arch + manifest + install), so
	// a changed requirements.txt yields a NEW tag and an unchanged one reuses the
	// existing layer with no rebuild (even when the function's source changed).
	depRef := prepared.Dependency
	if !planResult.Deps.IsZero() {
		depRef, err = m.ensureDependencyImage(ctx, fn, spec, planResult.Deps, depRef)
		if err != nil {
			return nil, fmt.Errorf("function %q: %w", fn.Name, err)
		}
		// The function image inherits every layer of the dependency image, so
		// its Dockerfile's FROM is the dependency reference rather than the raw
		// base image. The engine moved the install into Deps, so the function
		// image carries no install RUN of its own — only UserSetup/User/Env/
		// Entrypoint on top of the dependency layer. The dependency image was
		// built with the runtime's external tools (e.g. uv), so the function
		// image inherits them via FROM and does not need to copy them again.
		planResult.BaseImage = depRef
		planResult.ToolCopies = nil
	}

	start := time.Now()
	// The build runs on an independent, lifecycle-rooted buildTimeout context,
	// NOT on the caller's ctx: Prepare is a seam for the reconciler, whose ctx
	// may be a short service-reconcile budget, and a slow image build must not
	// be cut off by it (while still being cancelled at Relay shutdown via the
	// manager lifecycle). The reuse probes above deliberately keep using the
	// caller's ctx — they are quick and must honor its cancellation.
	buildCtx, buildCancel := m.buildContext()
	defer buildCancel()
	if err := buildImage(buildCtx, m.cli, fn.Name, fn, planResult, image, functionImageLabels(fn.Name, fp, depRef, bootstrapLabelHash), selection); err != nil {
		elapsed := time.Since(start)
		m.metrics.ObserveDurationLabels(metrics.MetricFunctionBuild, []metrics.Label{
			{Name: "function", Value: fn.Name},
		}, elapsed)
		m.metrics.IncLabels(metrics.MetricBuildFailures, []metrics.Label{
			{Name: "function", Value: fn.Name},
		})
		// Function names are validated to [a-z0-9][a-z0-9._-]* (bounded by
		// function count), so using them as labels is low-cardinality.
		m.log.Error("Function: build failed",
			"function", fn.Name,
			"duration", elapsed,
			"result", "failed",
		)
		return nil, err
	}
	elapsed := time.Since(start)
	m.metrics.ObserveDurationLabels(metrics.MetricFunctionBuild,
		[]metrics.Label{{Name: "function", Value: fn.Name}}, elapsed)
	m.log.Info("Function: built",
		"function", fn.Name,
		"duration", elapsed,
		"result", "success",
	)
	// The build succeeded: activate the exact image so a removed-then-recreated
	// function warms again and a same-content rebuild is not left retired, then
	// propagate the reconciled concurrency to the live pool (a hot-swapped
	// concurrency takes effect without a worker restart).
	m.containers.activateFunction(fn.Name, image)
	m.containers.setFunctionConcurrency(fn.Name, prepared.Concurrency)
	return &Prepared{
		Name:              fn.Name,
		Image:             image,
		Fingerprint:       fp,
		Env:               planResult.Env,
		Concurrency:       prepared.Concurrency,
		Dependency:        prepared.Dependency,
		Networks:          prepared.Networks,
		RuntimeGeneration: prepared.RuntimeGeneration,
	}, nil
}

// templateHandlers returns the function's handler MODULE parts (the portion of
// each `module.function` handler before the LAST dot), collected from the
// template's event rules and cron schedules, sorted and deduped. Only the module
// part is needed: it identifies the source file the engine must resolve (and, for
// the Node engine, transpile when it is TypeScript). A nil template, a malformed
// handler without a dot, and an empty module are skipped: template validation
// already rejects them on the parse path, and this keeps Prepare total for
// hand-built templates used by tests and direct callers.
func templateHandlers(fn function.Function) []string {
	if fn.Template == nil {
		return nil
	}
	seen := make(map[string]bool)
	var out []string
	add := func(handler string) {
		idx := strings.LastIndex(handler, ".")
		if idx <= 0 {
			return
		}
		module := handler[:idx]
		if module == "" || seen[module] {
			return
		}
		seen[module] = true
		out = append(out, module)
	}
	for _, ev := range fn.Template.Events {
		add(ev.Handler)
	}
	for _, s := range fn.Template.Schedules {
		add(s.Handler)
	}
	sort.Strings(out)
	return out
}

// resolveConcurrency returns the function's resolved per-function concurrency
// for the warm container pool. It reads the template's parsed value, defaulting
// a zero value (a function built without parsing, or a nil template) to
// function.DefaultConcurrency — the same fallback the runner applies to its
// per-function semaphore, so the pool's bound and the runner's bound agree.
func resolveConcurrency(fn function.Function) int {
	if fn.Template == nil || fn.Template.Concurrency < 1 {
		return function.DefaultConcurrency
	}
	return fn.Template.Concurrency
}

// effectiveConcurrency returns the function's EFFECTIVE per-function
// concurrency for the warm container pool: the template's resolved concurrency
// clipped to the worker-global MAX_CONCURRENCY. When the template asks for more
// than the global cap (e.g. concurrency 15 with MAX_CONCURRENCY=8), the pool
// warms, reports, and admits only the cap's worth — the effective intersection
// of the two limits the README documents, matching the runner's clipped
// per-function semaphore.
func (m *Manager) effectiveConcurrency(fn function.Function) int {
	return m.clipConcurrency(resolveConcurrency(fn))
}

// clipConcurrency clips an already-resolved per-function concurrency to the
// worker-global MAX_CONCURRENCY. A value below 1 is treated as
// function.DefaultConcurrency first, and a zero Manager.maxConcurrency (a
// Manager constructed directly by tests) is treated as DefaultMaxConcurrency,
// never "uncapped". It is the single clipping rule applied by Prepare and
// Execute, so a hand-built Prepared (direct/integration callers) can never
// warm a pool larger than the worker-global cap.
func (m *Manager) clipConcurrency(n int) int {
	if n < 1 {
		n = function.DefaultConcurrency
	}
	limit := m.maxConcurrency
	if limit < 1 {
		limit = DefaultMaxConcurrency
	}
	if n > limit {
		return limit
	}
	return n
}

// ensureDependencyImage builds the dependency image for the function's
// dependency manifest set, returning the dependency image reference. It is a
// no-op (returns the existing reference) when the dependency image is already
// present locally — the content address makes existence the correctness test,
// since the tag embeds the fingerprint over every relevant input. If the
// dependency build fails, Prepare fails: there is no fallback to the old
// single-stage build, because the function image's Dockerfile inherits its
// dependency layers via FROM and cannot be built without them.
//
// depRef is the reference the caller already derived for the dependency
// fingerprint (so a build failure is attributable, and the caller has it in hand
// even for the reuse case). It must be the reference for depFingerprint; the
// two travel together to keep "which dependency was this built for" exact.
func (m *Manager) ensureDependencyImage(
	ctx context.Context,
	fn function.Function,
	spec plan.Spec,
	deps plan.Deps,
	depRef string,
) (string, error) {
	// Derive the dependency fingerprint so the built image is stamped with the
	// exact content address it encodes (see dependencyImageLabels). The
	// fingerprint has already been computed by Prepare's split above, but
	// recomputing here keeps this method self-contained and cheap (the same
	// manifest reads); the reference passed in is what identifies the image.
	fp, err := DependencyFingerprint(arch, platform, spec, fn.Dir, deps)
	if err != nil {
		return "", fmt.Errorf("dependency fingerprint: %w", err)
	}
	if depRef == "" {
		depRef = depImageRef(fp)
	}
	if m.imageExists(ctx, depRef) {
		m.log.Debug("Dependency image exists; reusing", "dep_image", depRef)
		return depRef, nil
	}

	start := time.Now()
	// As in Prepare's function-image build, the dependency build uses an
	// independent lifecycle-rooted buildTimeout context rather than the caller's
	// ctx, so a slow install step is never cut off by a short reconcile budget
	// while still being cancelled at Relay shutdown. The imageExists probe above
	// keeps the caller's ctx.
	buildCtx, buildCancel := m.buildContext()
	defer buildCancel()
	if err := buildDependencyImage(buildCtx, m.cli, spec, fn.Dir, deps, depRef, fp); err != nil {
		elapsed := time.Since(start)
		// Dependency-image build failures count as function build failures so the
		// existing failure metric/label surface stays the single observability
		// contract for "this function could not be prepared".
		m.metrics.IncLabels(metrics.MetricBuildFailures, []metrics.Label{
			{Name: "function", Value: fn.Name},
		})
		m.log.Error("Function: dependency build failed",
			"function", fn.Name,
			"duration", elapsed,
			"dep_image", depRef,
			"result", "failed",
		)
		return "", err
	}
	elapsed := time.Since(start)
	m.metrics.ObserveDurationLabels(metrics.MetricFunctionBuild,
		[]metrics.Label{{Name: "function", Value: fn.Name}}, elapsed)
	m.log.Info("Function: dependency layer built",
		"function", fn.Name,
		"duration", elapsed,
		"dep_image", depRef,
		"result", "success",
	)
	return depRef, nil
}

// Execute runs the given handler invocation against a REUSED execution
// container leased from the function's warm pool (up to Prepared.Concurrency
// containers per function per image version; see container_cache.go and
// execution_container.go). The first invocation for a function starts a
// container (stamping its creation-time identity labels from the RunMeta in
// ctx); concurrent invocations of the same function lease distinct containers,
// and each container serves one invocation at a time over the line-JSON
// invocation protocol until it is discarded (timeout, process exit, protocol
// error, image change). A handler failure (ok:false) does NOT discard it.
//
// The pool's bound is Prepared.Concurrency (the effective value, already
// clipped to MAX_CONCURRENCY by Prepare and re-clipped here), the same value
// the runner's per-function semaphore uses, so in the runner path the pool
// never blocks (the semaphore already admits at most that many concurrent
// calls). Direct callers that bypass the runner are bounded by the pool itself;
// when the pool is at capacity, Execute blocks until a lease is released, ctx
// is done, or the Manager is closed (errPoolClosed).
//
// The context must carry the per-invocation timeout; a timeout discards the
// container (kill + remove, reason "timeout") and is treated as an invocation
// failure. The invocation's diagnostic RunMeta is read from ctx (see
// WithRunMeta); when absent the labels are empty except the hostname fallback
// below, which is harmless (labels are diagnostic-only beyond the sweep
// predicate).
//
// extraEnv are additional environment variables carried INTO the request frame
// (see invokeRequest.Env) so per-invocation values — template env values and
// resolved secrets — take effect per invocation on the reused container.
// Later entries win on duplicate names ("K=V" parse; later entries overwrite),
// matching the old container-env semantics, so rotating a secret value never
// requires a rebuild.
func (m *Manager) Execute(
	ctx context.Context,
	prepared *Prepared,
	handler string,
	eventJSON []byte,
	extraEnv []string,
) error {
	meta := RunMetaFrom(ctx)
	if meta.Hostname == "" {
		// Fall back to the manager's worker identity so a direct caller that
		// did not inject RunMeta still stamps the container's owner (and so an
		// orphaned container from this worker is still attributable at sweep
		// time). The runner always injects the full meta; this is the safety
		// net for direct/integration callers.
		meta.Hostname = m.hostname
	}
	if meta.Image == "" {
		// Same safety net for the image identity: the owning function image is
		// known here, so the label (and RemoveImage's in-use guard) stays
		// accurate for direct callers that never set it.
		meta.Image = prepared.Image
	}
	if meta.Function == "" {
		meta.Function = prepared.Name
	}

	// Creation-time identity meta: per-invocation fields (Handler, MessageID,
	// EventID, EventName) are EMPTY because container labels are immutable at
	// creation while the reused container outlives individual invocations;
	// per-invocation attribution lives only in the request frame and the
	// in-flight output prefix.
	idMeta := meta
	idMeta.Handler = ""
	idMeta.MessageID = ""
	idMeta.EventID = ""
	idMeta.EventName = ""
	start := func() (reusableContainer, error) {
		// Verify the template's networks exist BEFORE creating the container.
		// The networks are infrastructure owned OUTSIDE Relay — never created
		// here — so a missing one is a structured operator warning, not a
		// chaos fix-up. This runs only on the cold-create path (a warm reuse
		// creates no container), so it adds no per-invocation Docker round
		// trip. A missing network fails THIS function's invocation only.
		if missing, ok, err := m.VerifyNetworks(ctx, prepared.Networks); err != nil {
			return nil, fmt.Errorf("verify networks: %w", err)
		} else if !ok {
			m.log.Warn("Runtime: required Docker network is missing",
				"function", prepared.Name,
				"network", missing,
				"effect", "function execution skipped; relay never creates networks",
			)
			return nil, fmt.Errorf("required Docker network %q does not exist", missing)
		}
		return m.startContainer(ctx, prepared.Name, prepared.Image, prepared.Env, prepared.Networks, idMeta)
	}
	// Prepared.Concurrency is populated by Prepare as the effective bound
	// (template concurrency clipped to MAX_CONCURRENCY). A hand-built Prepared
	// (direct/integration callers) may carry the raw template value or leave it
	// zero, so clip it here too: the pool bound can never exceed the
	// worker-global cap, and the same bound the runner's clipped per-function
	// semaphore uses is what the pool enforces, keeping the pool from ever being
	// a stricter limiter than the runner's per-function semaphore.
	max := m.clipConcurrency(prepared.Concurrency)
	return m.containers.executeVersion(
		ctx, prepared.Name, prepared.Image, prepared.RuntimeGeneration, max, start, handler, eventJSON, envMap(extraEnv),
	)
}

// InvalidateImage retires any cached execution container running the given
// image, WITHOUT blocking: idle containers are discarded immediately and busy
// ones are marked retired and discarded as soon as their invocation releases.
// No later acquire can be handed a pre-invalidation container for that image.
// It is called by the runner when retiring an image so the image's
// ErrImageInUse container-reference guard clears promptly.
func (m *Manager) InvalidateImage(image string) {
	m.containers.invalidateImage(image)
}

// PoolSnapshot returns a point-in-time view of name's live warm-container pool:
// capacity, container counts by lease state, and the cumulative acquire/discard
// counters. The gauges are read from the pool's authoritative in-memory state
// (no Docker round trip); the counters are read from the same metrics registry
// the worker exposes on /metrics. ok is false when the function has never warmed
// a pool (or its pool was already removed), so a caller can omit the section
// rather than render stale zeros. It is safe for concurrent use.
//
// This is a LIVE, worker-local view. It exists for in-process callers (an
// embedded CLI/provider, tests); the standalone `relay function inspect` process
// has no access to the worker's memory and therefore renders only the persisted
// cumulative counters with the live gauges marked unavailable (see
// internal/cli). The live gauges are deliberately NOT persisted to SQLite: a
// persisted live gauge would go stale between flushes.
func (m *Manager) PoolSnapshot(name string) (PoolSnapshot, bool) {
	if m == nil || m.containers == nil {
		return PoolSnapshot{}, false
	}
	return m.containers.snapshot(name, m.metrics)
}

// RemoveFunction discards a removed function's warm container state: new
// acquires for the function fail immediately, idle containers are discarded now,
// busy ones are retired and discarded when their invocation releases, and the
// pool's state is deleted once it is empty. A later release of a busy container
// can never recreate the state (the function name is remembered as removed until
// Prepared reactivates it). It is non-blocking, so the reconciler's removal hook
// is never stalled by an in-flight invocation, and it is the runtime half of
// function removal; the runner separately retires the function's images.
//
// The function's runtime-pool metric series are deleted by the cache inside the
// SAME critical section that installs the removal tombstone, so no concurrent
// acquire/discard can recreate them (see containerCache.removeFunction). A
// genuinely reactivated function gets a fresh pool (and fresh series) after that
// section. The worker's own metricsInstance.RemoveFunction and the flush-time
// SweepFunctionMetrics still cover the runner's series; the runtime no longer
// performs a second, racy delete here.
func (m *Manager) RemoveFunction(name string) {
	if name == "" {
		return
	}
	m.containers.removeFunction(name)
	// Drop the function's external-service pull-check records so a later
	// re-added function starts with an immediate remote check and the in-memory
	// map does not grow without bound across removals.
	m.forgetServicePullChecks(name)
}

// envMap parses "K=V" entries into a map, later entries winning on duplicate
// names (the same semantics the old container env argument had).
func envMap(extraEnv []string) map[string]string {
	if len(extraEnv) == 0 {
		return nil
	}
	env := make(map[string]string, len(extraEnv))
	for _, kv := range extraEnv {
		if k, v, ok := strings.Cut(kv, "="); ok {
			env[k] = v
		}
	}
	return env
}

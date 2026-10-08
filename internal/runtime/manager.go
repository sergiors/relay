package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/moby/moby/client"
	"go.opentelemetry.io/otel/codes"

	"relay/internal/app"
	"relay/internal/observability/metrics"
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

// DefaultMaxConcurrentInvocations is the worker-global concurrency cap applied by
// NewManager when no WithMaxConcurrentInvocations option is given. It mirrors
// config.DefaultMaxConcurrentInvocations and runner.DefaultMaxConcurrentInvocations (this leaf
// package cannot import either); the worker always passes config's resolved
// value explicitly, and a Manager constructed directly by tests leaves
// maxConcurrentInvocations zero (treated as this default, never "uncapped").
const DefaultMaxConcurrentInvocations = 8

// DefaultMaxConcurrentBuilds is the cap on runtime-backed image-preparation
// pipelines running concurrently in this worker, applied by NewManager when no
// WithMaxConcurrentBuilds option is given. It mirrors
// config.DefaultMaxConcurrentBuilds (this leaf package cannot import config);
// the worker always passes config's resolved value explicitly, and a Manager
// constructed directly by tests leaves maxConcurrentBuilds zero (treated as
// this default, never unbounded). The cap is per Manager (one per worker), so
// it bounds aggregate preparation — source selection/snapshot, dependency
// snapshot, reuse probes, dependency image build, and app image build — and is
// independent of the invocation-concurrency cap.
const DefaultMaxConcurrentBuilds = 2

// DefaultMaxWarmContainers is the worker-global hard bound on warm execution
// containers applied by NewManager when no WithMaxWarmContainers option is
// given. It mirrors config.DefaultMaxWarmContainers (this leaf package cannot
// import config); the worker always passes config's resolved value explicitly.
// A zero value falls back to this constant, never "unbounded".
const DefaultMaxWarmContainers = 8

// managerPingTimeout bounds the startup Docker daemon ping. The ping is rooted
// in the manager lifecycle (the worker's signal context) so it is cancelled at
// shutdown, but it must also be finite on its own: a wedged daemon must fail
// startup within a bounded time rather than hanging the worker before any
// resource exists to clean up. The bound is deliberately shorter than the
// shutdown step caps because it is not a teardown.
const managerPingTimeout = 10 * time.Second

// pingFunc pings the Docker daemon at startup. It is the constructor's seam:
// production uses pingDocker, while a test can inject a fake that blocks until
// its context is done, so the ping's finite bound and lifecycle ownership are
// unit-testable without a Docker daemon.
type pingFunc func(ctx context.Context, cli *client.Client) error

// closeClientFunc closes the manager's Docker client. It is the shutdown seam
// (analogous to pingFunc): production leaves it nil and CloseContext calls
// cli.Close, while a test can inject a wrapper that records the close instant so
// it can prove no Docker operation runs after the client closes.
type closeClientFunc func(cli *client.Client) error

// dependencyFingerprintFunc computes a dependency layer's content address from
// the immutable manifest snapshot Prepare captured. It is the Manager's seam
// (default: dependencyFingerprintFrom) so a test can inject a counter and prove
// Prepare computes the dependency digest exactly once and threads that single
// value through the tag, the label, and the staged bytes.
type dependencyFingerprintFunc func(arch, platform string, spec plan.Spec, deps plan.Deps, snap dependencySnapshot) (string, error)

// pingDocker is the production pingFunc: one version-negotiated daemon ping.
func pingDocker(ctx context.Context, cli *client.Client) error {
	_, err := cli.Ping(ctx, client.PingOptions{})
	return err
}

// Manager prepares app images and executes handler invocations. It owns a
// single Docker Engine client, reused for every build and invocation, and a
// per-app warm container pool: each app keeps up to its resolved
// concurrency reused containers per image version (see container_cache.go),
// kept alive between invocations and leased one-per-invocation, and discarded
// on timeout/process exit/protocol error/image change/app removal/shutdown
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
	// maxConcurrentInvocations is the worker-global concurrency cap (MAX_CONCURRENT_INVOCATIONS): the
	// SAME value the runner uses for its global semaphore. It clips every
	// app's effective per-app concurrency to
	// min(template concurrency, maxConcurrentInvocations) in Prepare/Execute, so the warm
	// pool, its capacity gauge, PoolSnapshot/the CLI, and the runner's
	// per-app semaphore all agree on one effective bound. It is set once at
	// construction (WithMaxConcurrentInvocations; the worker wires config's resolved
	// value) and read without the lock: MAX_CONCURRENT_INVOCATIONS is startup
	// configuration and is NOT hot-reloadable, so a global change requires a
	// worker restart. A zero value falls back to DefaultMaxConcurrentInvocations, never
	// "uncapped" (a Manager constructed directly by tests behaves like the
	// default runner).
	maxConcurrentInvocations int
	// maxConcurrentBuilds is the per-worker cap (MAX_CONCURRENT_BUILDS) on
	// runtime-backed image-preparation pipelines running concurrently: one
	// permit is held for a preparation's WHOLE duration — source selection and
	// snapshot, dependency snapshot, reuse probes, dependency image build, and
	// app image build — so dependency and app sub-builds are sequential within
	// one permit. It is set once at construction (WithMaxConcurrentBuilds; the
	// worker wires config's resolved value) and read without the lock: it is
	// startup configuration and is NOT hot-reloadable. A zero value falls back
	// to DefaultMaxConcurrentBuilds, never "unbounded". The limiter itself is
	// built lazily and race-safely (see buildLimiterFor) so a Manager
	// constructed directly by tests is bounded too.
	maxConcurrentBuilds int
	// maxWarmContainers is the worker-global hard bound (MAX_WARM_CONTAINERS) on
	// warm EXECUTION containers this worker keeps across all apps. It is set
	// once at construction (WithMaxWarmContainers; the worker wires config's
	// resolved value) and read without the lock: startup configuration, not
	// hot-reloadable. A zero value falls back to DefaultMaxWarmContainers, never
	// "unbounded". It bounds the pooled execution-container population, not
	// invocation concurrency; persistent service containers are outside it.
	maxWarmContainers int
	// buildLimit is the per-Manager preparation semaphore. It is created by
	// NewManager; a Manager constructed directly by tests lazily initializes it
	// on first use (buildLimiterFor), so every preparation is bounded even when
	// the option was never supplied. Capacity is immutable after creation. It is
	// an atomic pointer so the lazy fallback can be read concurrently with a
	// racing initialization without a data race.
	buildLimit     atomic.Pointer[buildLimiter]
	buildLimitInit sync.Once
	// networks is the worker-global Docker network set (NETWORKS) every
	// execution container this manager creates joins at create time. It is set
	// once at construction (WithNetworks; the worker wires config's resolved
	// value) and read without the lock: it is startup configuration and is NOT
	// hot-reloadable, so a global change requires a worker restart. Empty means
	// no NetworkingConfig is sent (default bridge behavior). The networks are
	// infrastructure owned OUTSIDE Relay — the worker verifies they exist at
	// startup and Relay never creates them.
	networks []string
	// containers caches the per-app reusable execution containers.
	containers *containerCache
	// leases is the single ownership authority for Relay-owned images: it
	// admits references (builds, executions, services, registry publication)
	// and gates removal, so no new use can slip between a reference check and
	// ImageRemove. It is created by NewManager; a Manager constructed directly
	// by tests lazily initializes it on first use (leaseInit).
	leases    *imageCoordinator
	leaseInit sync.Once
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
	// sourceMount is the worker-global SOURCE_MOUNT switch: when true, a runtime
	// whose dependency layout permits it (plan.Spec.MountableSource) builds a
	// source-independent app image and bind-mounts the app's live /apps source
	// read-only at runtime instead of baking it. It is set once at construction
	// (WithSourceMount; the worker wires config's resolved value) and read
	// without the lock: it is startup configuration and is NOT hot-reloadable.
	// False (the default) preserves the historical baked-source behavior exactly.
	sourceMount bool
	// mounts records, per app, the live source mount published by a successful
	// Prepare (see setSourceMount). It is read at execution-container create and
	// by ResolveServiceImage, and cleared on removal or when an app stops being
	// mountable. Guarded by mountMu.
	mountMu sync.RWMutex
	mounts  map[string]SourceMount
	// selfMounts caches the lazy resolution of Relay's OWN container's mounts,
	// used to translate a SOURCE_MOUNT app path into the path the Docker daemon
	// actually sees when Relay is itself a container (see daemonSourcePath). A
	// zero value resolves on first use.
	selfMounts containerMounts
	// containerDetect overrides the container self-identification precondition
	// used by daemonSourcePath. Nil uses the production default (the worker
	// hostname is container-ID-like). Set only by tests.
	containerDetect func() bool

	// done is closed by Close to stop the single maintenance loop (the only
	// eviction driver; there is never a ticker or goroutine per container).
	done chan struct{}
	// maintDone is closed by the maintenance loop when it exits.
	maintDone chan struct{}
	// maintInterval is the tick the maintenance loop is started with, derived
	// once at construction from the resolved idle timeout. It is what the
	// exported StartMaintenance uses, so a deferred-maintenance manager starts
	// the loop with exactly the interval NewManager would have used eagerly. It
	// is zero for a Manager constructed directly by tests (which call
	// startMaintenance with an explicit interval).
	maintInterval time.Duration
	// maintMu guards the start/close handshake for the maintenance loop, so
	// StartMaintenance is idempotent and Close only joins a loop that was
	// actually started (see startMaintenance and CloseContext).
	maintMu sync.Mutex
	// maintStarted reports whether the maintenance loop goroutine was launched.
	maintStarted bool
	// closed reports whether Close has begun; StartMaintenance refuses to start
	// a loop after that, so a close/start race can never leak a goroutine or
	// double-close a channel.
	closed bool
	// closeOnce makes Close idempotent and keeps the maintenance loop's stop
	// handshake single-fire.
	closeOnce sync.Once
	// closeErr stores the client-close result so repeated Close calls are
	// idempotent.
	closeErr error
	// closeClient closes the Docker client. It is nil in production (CloseContext
	// calls cli.Close directly) and set only by tests, which wrap it to record
	// the close instant so they can prove no Docker operation runs after close.
	// It is read once and never mutated after construction.
	closeClient closeClientFunc
	// depFingerprint computes a dependency layer's content address from the
	// snapshot Prepare captured. It is nil in production (dependencyFingerprintFrom
	// is used) and set only by tests, whose injected counter proves the digest is
	// computed exactly once per prepare.
	depFingerprint dependencyFingerprintFunc
	// startContainerFn creates a fresh execution container. It is nil in
	// production (startContainer is used) and set only by in-package tests so the
	// Execute path's resource resolution and generation identity can be exercised
	// without a Docker daemon. It receives exactly the arguments Execute would
	// pass to startContainer, including the resolved image identity so a test can
	// assert the container is created from the exact content that was leased.
	startContainerFn func(ctx context.Context, fnName string, img resolvedImage, env []string, limits app.ResourceLimits, meta RunMeta) (reusableContainer, error)
	// resolveImageIdentityFn resolves a managed image reference to its immutable
	// content identity. It is nil in production (resolveImageIdentity inspects
	// the daemon) and set only by in-package tests so the Execute path's
	// generation keying on image content can be exercised without a Docker
	// daemon. A returned zero value falls back to the reference identity.
	resolveImageIdentityFn func(ctx context.Context, ref, fingerprint string) (resolvedImage, error)
	// afterSourceSnapshot is the deterministic seam around the captured source
	// snapshot: Prepare calls it (test-only) ONCE, immediately after
	// CaptureSourceSnapshot and before the fingerprint is derived, passing the
	// live *app.SourceSnapshot. It is nil in production and never called
	// then. A test uses it either to mutate the on-disk tree at the exact
	// capture/build boundary (proving the tag and staged bytes still come from
	// the one capture) or to retain the snapshot and assert its bytes were
	// released when Prepare returns — including on the cancellation path. It is
	// read and never mutated after construction.
	afterSourceSnapshot func(*app.SourceSnapshot)
	// afterBuildPermit is the deterministic seam immediately after a
	// runtime-backed prepare acquires its preparation permit and before any
	// source selection, snapshot, or build work. It is nil in production and
	// never called then. A test uses it to hold a preparation inside the
	// bounded section (tracking how many run concurrently) and to prove the
	// per-worker build cap is enforced without sleeps.
	afterBuildPermit func()
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
	// maxConcurrentInvocations is the worker-global concurrency cap. Zero means the
	// package default (DefaultMaxConcurrentInvocations), matching the runner's
	// normalization.
	maxConcurrentInvocations int
	// maxConcurrentBuilds is the cap on concurrent runtime-backed preparation
	// pipelines. Zero means the package default (DefaultMaxConcurrentBuilds),
	// never "unbounded".
	maxConcurrentBuilds int
	// maxWarmContainers is the worker-global hard bound on warm execution
	// containers (MAX_WARM_CONTAINERS). Zero means the package default
	// (DefaultMaxWarmContainers), never "unbounded".
	maxWarmContainers int
	// networks is the worker-global Docker network set every execution container
	// joins at create time (see WithNetworks). Nil/empty means no extra
	// networks (default bridge behavior).
	networks []string
	// now is the injectable clock seam for deterministic tests. Nil means the
	// wall clock.
	now func() time.Time
	// lifecycle roots the manager's lifecycle context (see WithLifecycleContext).
	// Nil means the manager starts its own Close-cancelled root.
	lifecycle context.Context
	// ping is the startup Docker-daemon ping seam. Nil means the production
	// pingDocker. Tests inject a fake so the ping's finite bound and lifecycle
	// ownership are exercised without a daemon.
	ping pingFunc
	// deferredMaintenance, when set (WithDeferredMaintenance), makes NewManager
	// NOT start the warm-container maintenance loop. The caller must start it
	// explicitly with StartMaintenance once its own prerequisites (the worker's
	// NETWORKS verification) hold. False preserves the historical eager start.
	deferredMaintenance bool
	// sourceMount is the worker-global SOURCE_MOUNT switch (WithSourceMount).
	// False preserves the historical baked-source behavior.
	sourceMount bool
}

// WithWarmContainerIdleTimeout sets how long a healthy idle warm execution
// container is kept before the maintenance loop evicts it. A non-positive value
// is treated as the package default, so a caller cannot accidentally disable
// eviction; config.Load rejects non-positive values before they reach here.
func WithWarmContainerIdleTimeout(idleTimeout time.Duration) ManagerOption {
	return func(o *managerOptions) { o.idleTimeout = idleTimeout }
}

// WithMaxConcurrentInvocations sets the worker-global concurrency cap (MAX_CONCURRENT_INVOCATIONS).
// It is the SAME value the worker passes to runner.SetMaxConcurrentInvocations, so the
// warm pool's effective bound and the runner's per-app semaphore agree.
// A non-positive value is treated as DefaultMaxConcurrentInvocations (never "uncapped"),
// matching the runner's normalization. It is startup configuration: changing it
// requires a worker restart (there is no live setter; the runner's global
// semaphore is likewise built once at startup). The worker wires it from
// config; a direct NewManager caller that omits it gets DefaultMaxConcurrentInvocations.
func WithMaxConcurrentInvocations(n int) ManagerOption {
	return func(o *managerOptions) { o.maxConcurrentInvocations = n }
}

// WithMaxConcurrentBuilds sets the per-worker cap (MAX_CONCURRENT_BUILDS) on
// runtime-backed image-preparation pipelines running concurrently. A permit is
// held for a preparation's whole duration (source selection and snapshot,
// dependency snapshot, reuse probes, dependency image build, and app image
// build), so the cap bounds aggregate preparation, not just each Docker build
// call. It is independent of the invocation-concurrency cap and of the local
// event buffer, and it is NOT a host-wide resource quota: each worker enforces
// its own, so N workers may run up to N * capacity preparations. A
// non-positive value is treated as DefaultMaxConcurrentBuilds (never
// "unbounded"). It is startup configuration: changing it requires a worker
// restart (there is no live setter). The worker wires it from config; a direct
// NewManager caller that omits it gets DefaultMaxConcurrentBuilds.
func WithMaxConcurrentBuilds(n int) ManagerOption {
	return func(o *managerOptions) { o.maxConcurrentBuilds = n }
}

// WithMaxWarmContainers sets the worker-global hard bound (MAX_WARM_CONTAINERS)
// on warm EXECUTION containers this worker keeps across all apps. It is a count
// bound on the pooled execution-container population (idle + busy + in-flight
// creates), independent of MAX_CONCURRENT_INVOCATIONS; persistent service
// containers and stale-version throwaway containers are excluded. A
// non-positive value is treated as DefaultMaxWarmContainers (never "unbounded").
// It is startup configuration: changing it requires a worker restart. The worker
// wires it from config; a direct NewManager caller that omits it gets
// DefaultMaxWarmContainers.
func WithMaxWarmContainers(n int) ManagerOption {
	return func(o *managerOptions) { o.maxWarmContainers = n }
}

// WithNetworks sets the worker-global Docker network set (NETWORKS) that every
// execution container this manager creates joins at create time. The worker
// wires config's resolved list; a direct NewManager caller that omits it creates
// containers with no NetworkingConfig (default bridge behavior). The list is
// copied so a caller cannot mutate the manager's configuration after
// construction, and it is startup configuration: changing it requires a worker
// restart. The worker verifies the networks exist at startup; Relay never
// creates them.
func WithNetworks(networks []string) ManagerOption {
	return func(o *managerOptions) {
		o.networks = append([]string(nil), networks...)
	}
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

// WithSourceMount enables or disables SOURCE_MOUNT for this worker: when true,
// a runtime whose dependency layout permits it (plan.Spec.MountableSource, i.e.
// Python and Node) builds a source-independent app image and bind-mounts the
// app's live /apps source read-only into its execution and entrypoint-service
// containers instead of baking it. The worker wires config's resolved
// SOURCE_MOUNT value; a direct NewManager caller that omits it gets the
// historical baked-source behavior (false). It is startup configuration:
// changing it requires a worker restart.
func WithSourceMount(enabled bool) ManagerOption {
	return func(o *managerOptions) { o.sourceMount = enabled }
}

// WithDeferredMaintenance makes NewManager return WITHOUT starting the
// warm-container maintenance loop; the caller must start it explicitly with
// StartMaintenance once its own startup prerequisites hold. It exists for the
// worker, which opens the manager (and so pings Docker) as one preflight step
// but must verify every configured NETWORKS network BEFORE it starts any
// background loop: a manager whose eviction ticker started early would be an
// active background loop during a phase that is required to fail closed before
// later loops begin. Omitting the option (every other caller) preserves the
// historical behavior: NewManager starts the loop eagerly. Close is safe
// whether or not StartMaintenance was ever called.
func WithDeferredMaintenance() ManagerOption {
	return func(o *managerOptions) { o.deferredMaintenance = true }
}

// withClock injects a deterministic clock for tests. It is unexported because
// only in-package tests need it; production always uses the wall clock.
func withClock(now func() time.Time) ManagerOption {
	return func(o *managerOptions) { o.now = now }
}

// withPing injects the startup Docker-daemon ping seam for tests. It is
// unexported because only in-package tests need it; production always pings the
// real daemon.
func withPing(ping pingFunc) ManagerOption {
	return func(o *managerOptions) { o.ping = ping }
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
// Close. WithDeferredMaintenance suppresses that eager start so the caller can
// StartMaintenance it explicitly once its own startup prerequisites hold; Close
// remains safe in either case.
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
	// Create the manager-owned lifecycle BEFORE the ping so the ping is
	// cancelled by Close (and by the caller's signal context) as well as bounded
	// by managerPingTimeout. A cancelled lifecycle during startup is a shutdown,
	// not a daemon failure; the caller (worker) classifies the context error.
	parent := resolved.lifecycle
	if parent == nil {
		parent = context.Background()
	}
	lifecycle, lifecycleCancel := context.WithCancel(parent)
	ping := resolved.ping
	if ping == nil {
		ping = pingDocker
	}
	pingCtx, pingCancel := context.WithTimeout(lifecycle, managerPingTimeout)
	pingErr := ping(pingCtx, cli)
	pingCancel()
	if pingErr != nil {
		lifecycleCancel()
		_ = cli.Close()
		return nil, fmt.Errorf("cannot connect to Docker daemon: %w", pingErr)
	}
	mgr := &Manager{
		log:                      logger,
		cli:                      cli,
		metrics:                  registry,
		hostname:                 hostname,
		maxConcurrentInvocations: resolved.maxConcurrentInvocations,
		maxConcurrentBuilds:      resolved.maxConcurrentBuilds,
		maxWarmContainers:        resolved.maxWarmContainers,
		networks:                 resolved.networks,
		done:                     make(chan struct{}),
		maintDone:                make(chan struct{}),
		now:                      resolved.now,
		sourceMount:              resolved.sourceMount,
		pullChecks:               map[string]time.Time{},
	}
	// Construct the preparation limiter now with the resolved capacity, so the
	// lazy fallback in buildLimiterFor is never needed for a NewManager-built
	// Manager. A direct-construction Manager (tests) builds an equivalent one on
	// first use. Capacity is immutable after construction.
	mgr.buildLimit.Store(newBuildLimiter(resolved.maxConcurrentBuilds))
	if mgr.now == nil {
		mgr.now = time.Now
	}
	// The build lifecycle: a manager-owned child of the caller's lifecycle (the
	// worker passes its signal ctx via WithLifecycleContext) or of
	// context.Background when none was supplied. Owning a child means Close
	// always cancels in-flight Dockerfile builds, while a cancelled parent (Relay
	// shutdown) propagates too. Builds are bounded by buildTimeout on top of
	// this; they are never rooted in a caller's short reconcile context.
	mgr.lifecycle, mgr.lifecycleCancel = lifecycle, lifecycleCancel
	mgr.containers = newContainerCache()
	mgr.containers.idleTimeout = resolved.idleTimeout
	mgr.containers.now = resolved.now
	mgr.containers.metrics = registry
	mgr.containers.maxWarm = resolved.maxWarmContainers
	mgr.containers.publishWarmCapacity()
	mgr.leases = newImageCoordinator()
	mgr.maintInterval = maintenanceInterval(resolved.idleTimeout)
	if !resolved.deferredMaintenance {
		mgr.startMaintenance(mgr.maintInterval)
	}
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

// Logger returns the manager's structured logger, defaulting to slog.Default
// when a Manager was constructed directly (tests) without one.
func (m *Manager) Logger() *slog.Logger {
	if m == nil || m.log == nil {
		return slog.Default()
	}
	return m.log
}

// Ping performs one bounded Docker daemon ping through the manager's client,
// honoring ctx (the caller supplies its own deadline). It is the live
// dependency check the worker's readiness probe uses in steady state: the
// manager's startup ping happened once at construction, but readiness must
// reflect whether the daemon is still reachable. It deliberately does NOT use
// the manager lifecycle or an internal timeout — the caller owns the bound, so
// a readiness query can never stall and a cancelled query aborts promptly.
func (m *Manager) Ping(ctx context.Context) error {
	if m == nil || m.cli == nil {
		return fmt.Errorf("docker client unavailable")
	}
	_, err := m.cli.Ping(ctx, client.PingOptions{})
	return err
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
	if resolved.maxConcurrentInvocations < 1 {
		resolved.maxConcurrentInvocations = DefaultMaxConcurrentInvocations
	}
	if resolved.maxConcurrentBuilds < 1 {
		resolved.maxConcurrentBuilds = DefaultMaxConcurrentBuilds
	}
	if resolved.maxWarmContainers < 1 {
		resolved.maxWarmContainers = DefaultMaxWarmContainers
	}
	return resolved
}

// buildLimiter is the per-Manager counting semaphore that bounds how many
// runtime-backed image-preparation pipelines run concurrently. It is a plain
// buffered channel of permits, mirroring the runner's invocation semaphore: a
// preparation holds one permit for its WHOLE duration (source selection and
// snapshot, dependency snapshot, reuse probes, dependency image build, and app
// image build), so the cap bounds aggregate preparation and dependency/app
// sub-builds are sequential within one permit. Capacity is immutable after
// construction; a Manager owns exactly one limiter, so the bound is per worker,
// never global across workers.
type buildLimiter struct {
	permits chan struct{}
}

// newBuildLimiter returns a limiter with capacity n. The caller normalizes n to
// be positive (DefaultMaxConcurrentBuilds at minimum), so the channel is never
// zero-capacity/unbounded.
func newBuildLimiter(n int) *buildLimiter {
	return &buildLimiter{permits: make(chan struct{}, n)}
}

// acquire takes one preparation permit, blocking until a permit frees or ctx is
// done. On success the caller MUST release exactly once. On cancellation it
// returns ctx.Err() and holds no permit, so a cancelled preparation never enters
// the bounded section and never needs a release.
//
// Cancellation is preferred even when a permit is free: the pre-select ctx.Err
// check and the post-send re-check make a cancelled waiter lose deterministically
// instead of letting select choose pseudo-randomly between a ready permit and a
// ready ctx.Done. The re-check hands the just-taken permit straight back, so a
// cancellation racing a freed slot still leaks no capacity.
func (l *buildLimiter) acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case l.permits <- struct{}{}:
		if err := ctx.Err(); err != nil {
			// Both arms were ready and select picked the permit. Release it so
			// the cancellation path holds no slot and the caller never starts.
			l.release()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// release returns one preparation permit. It must be called exactly once per
// successful acquire.
func (l *buildLimiter) release() {
	<-l.permits
}

// buildLimiterFor returns the Manager's preparation limiter, lazily creating a
// default-capacity one only for a Manager constructed directly (tests) that
// never went through NewManager. The sync.Once makes the fallback race-safe, so
// concurrent preparations cannot each build their own limiter, and the limiter
// is stored in an atomic pointer so a racing initialization can also be read
// safely. Capacity is immutable once created.
func (m *Manager) buildLimiterFor() *buildLimiter {
	if l := m.buildLimit.Load(); l != nil {
		return l
	}
	m.buildLimitInit.Do(func() {
		capacity := m.maxConcurrentBuilds
		if capacity < 1 {
			capacity = DefaultMaxConcurrentBuilds
		}
		m.buildLimit.Store(newBuildLimiter(capacity))
	})
	return m.buildLimit.Load()
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

// startMaintenance launches the single maintenance loop. It is idempotent and
// safe to race with Close: the start/close handshake is serialized by maintMu,
// so at most one goroutine is launched, no loop is started after Close has
// begun, and Close joins exactly the loop that was actually started. It is a
// method so tests that construct a Manager directly (bypassing NewManager/Docker)
// can start the loop with a test clock; production goes through StartMaintenance.
func (m *Manager) startMaintenance(interval time.Duration) {
	m.maintMu.Lock()
	defer m.maintMu.Unlock()
	if m.maintStarted || m.closed {
		return
	}
	if m.done == nil {
		m.done = make(chan struct{})
	}
	if m.maintDone == nil {
		m.maintDone = make(chan struct{})
	}
	m.maintStarted = true
	go m.maintenanceLoop(interval)
}

// StartMaintenance starts the warm-container maintenance loop that NewManager
// suppressed under WithDeferredMaintenance. It is the explicit start the worker
// calls only after every configured NETWORKS network has been verified, so no
// background loop runs before the preflight fails closed. It is idempotent
// (a second call is a no-op) and refuses to start after Close, so a close/start
// race can neither leak a goroutine nor double-close the stop channel. Callers
// that did not request deferred maintenance need not call it: their loop is
// already running.
func (m *Manager) StartMaintenance() {
	interval := m.maintInterval
	if interval <= 0 {
		// A Manager constructed directly (tests) may not carry the resolved
		// interval; fall back to the default-derived tick rather than panic in
		// time.NewTicker.
		interval = maintenanceInterval(DefaultWarmContainerIdleTimeout)
	}
	m.startMaintenance(interval)
}

// maintenanceLoop runs evictIdle on a single ticker until Close, then closes
// maintDone so Close can join it before tearing the cache down. It is the only
// eviction driver: there is no per-container goroutine or ticker. Each pass
// runs on the manager lifecycle (not a detached context), so when Close
// cancels the lifecycle an in-flight eviction's container teardown observes
// cancellation and the pass — and therefore the maintDone join in CloseContext —
// returns promptly instead of running out the per-container operation caps
// serially. A Manager constructed directly by tests (no lifecycle) falls back to
// context.Background, preserving the historical detached behavior.
func (m *Manager) maintenanceLoop(interval time.Duration) {
	defer close(m.maintDone)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	evictCtx := m.lifecycle
	if evictCtx == nil {
		evictCtx = context.Background()
	}
	for {
		select {
		case <-m.done:
			return
		case <-ticker.C:
			m.containers.evictIdleContext(evictCtx)
		}
	}
}

// Close stops the maintenance loop, discards every cached execution container
// (reason "shutdown"), then releases the Docker Engine client. It is idempotent
// and safe to call more than once during shutdown. It is the unbounded form of
// CloseContext; callers that own a shutdown budget (the worker) use
// CloseContext.
func (m *Manager) Close() error {
	return m.CloseContext(context.Background())
}

// CloseContext is Close under a caller-supplied bound. It:
//
//  1. cancels the manager-owned build lifecycle (so an in-flight Dockerfile
//     build stops promptly); because the maintenance loop's eviction also runs
//     its teardowns on this lifecycle, an eviction pass already in flight is
//     cancelled too, so the join below returns promptly;
//  2. joins the maintenance loop, so no eviction races the teardown;
//  3. closes the image-removal gate (beginShutdown): after this point no NEW
//     manager-owned removal may begin (a runner async cleanup that fires after
//     lifecycle cancellation is refused with ErrManagerShuttingDown), and every
//     in-flight removal's derived context is cancelled;
//  4. joins every in-progress manager-owned removal (waitRetirements) — the
//     join is prompt because step 3 cancelled their contexts — so no removal can
//     still issue a Docker request after this point;
//  5. tears down every cached execution container through a bounded parallel
//     worker pool (containerShutdownConcurrency), each teardown context-aware,
//     so a large warm pool is not discarded with O(N) serial Docker delays and
//     an expired ctx stops waiting;
//  6. drops any outstanding image leases (reset); it no longer wakes removal
//     waiters (they were already joined, and waking them would let a remover
//     race the close);
//  7. closes the Docker Engine client only after every step above returns.
//
// Idempotent: a second call returns the stored client-close result without
// re-running the teardown, exactly like Close. Safe to call more than once
// during shutdown and from a Manager with no Docker client (direct tests).
func (m *Manager) CloseContext(ctx context.Context) error {
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
			// Serialize with StartMaintenance: once closed is set, a
			// concurrent or later StartMaintenance refuses to launch the loop,
			// so the close below and the (possibly absent) maintDone join are
			// race-free. A deferred-maintenance manager whose loop was never
			// started has nothing to join.
			m.maintMu.Lock()
			m.closed = true
			started := m.maintStarted
			close(m.done)
			m.maintMu.Unlock()
			if started && m.maintDone != nil {
				<-m.maintDone
			}
		} else {
			// A direct-construction manager with no stop channel: record the
			// close so a later StartMaintenance cannot start a loop that would
			// outlive the closed manager.
			m.maintMu.Lock()
			m.closed = true
			m.maintMu.Unlock()
		}
		// Make retirement/removal lifecycle-owned: close the removal gate BEFORE
		// joining, so no new manager-owned removal can begin once shutdown has
		// started and every in-flight one is cancelled. Then join them, so the
		// Docker client is only closed once no removal is still using it.
		// leaseCoord() (not a raw field read) is used so a coordinator lazily
		// created by a concurrent removal is the exact one gated and joined.
		coord := m.leaseCoord()
		coord.beginShutdown()
		coord.waitRetirements(ctx)
		if m.containers != nil {
			m.containers.closeContext(ctx)
		}
		// Drop any outstanding image leases: the manager is shutting down, so
		// every admitted reference ends here. This is defense against a caller
		// that never released a Prepared handle it did not publish. It does not
		// wake retirement waiters; those operations were already joined above.
		coord.reset()
		if m.cli == nil {
			// A Manager constructed directly by a test (no Docker) still owns a
			// cache and maintenance loop, so Close must be safe without a client.
			return
		}
		if m.closeClient != nil {
			m.closeErr = m.closeClient(m.cli)
		} else {
			m.closeErr = m.cli.Close()
		}
	})
	return m.closeErr
}

// startContainer builds one fresh execution container for an app version.
// It is the containerCache factory, called with the creating invocation's
// parameters: img is the resolved immutable image identity (its createImage is
// handed to Docker so the container is created from exactly the resolved bytes),
// env is the app's plan env (per-app, applied at container create),
// limits is the app's effective per-container resource configuration, and
// meta is the creation-time identity RunMeta stamped as labels (per-invocation
// fields left empty — labels are immutable while the container outlives
// invocations). Every container joins the worker-global network set
// (WithNetworks).
func (m *Manager) startContainer(
	ctx context.Context,
	fnName string,
	img resolvedImage,
	env []string,
	limits app.ResourceLimits,
	meta RunMeta,
) (reusableContainer, error) {
	if m.startContainerFn != nil {
		return m.startContainerFn(ctx, fnName, img, env, limits, meta)
	}
	workDir, mounts := m.executionSourceMount(fnName)
	return startExecutionContainer(ctx, m.cli, m.log, fnName, img.createImage(), env, m.networks, limits, meta, workDir, mounts...)
}

// Prepared is an app whose image has been built.
type Prepared struct {
	Name        string
	Image       string
	Fingerprint string
	// Env are the runtime environment variables the app's engine requires
	// (e.g. PYTHONDONTWRITEBYTECODE for Python). They are applied to every
	// execution container for this app, after the base RELAY_HANDLER var.
	Env []string
	// Concurrency is the app's EFFECTIVE per-app concurrency: the
	// template's resolved `concurrency` clipped to the worker-global
	// MAX_CONCURRENT_INVOCATIONS (see Manager.effectiveConcurrency). It is the bound on the
	// app's warm container pool: at most this many containers are kept and
	// leased concurrently. It intentionally matches the runner's per-app
	// semaphore (also clipped to the global cap) so the pool is not a second
	// limiter in the runner path; direct Execute callers that bypass the runner
	// are bounded by it. Because MAX_CONCURRENT_INVOCATIONS is startup configuration, a
	// global change requires a worker restart; a hot-swapped template
	// `concurrency` is re-clipped live on each successful Prepare.
	Concurrency int
	// Dependency is the full "relay-dep-*" reference this app image was
	// built FROM, or "" when the app declares no dependency layer. It is
	// the app image's parent, so a caller (the runner) knows which
	// dependency image this app version pulls its payload from — the input
	// to dependency garbage collection. It is populated on BOTH the build and
	// reuse paths.
	Dependency string
	// lease is the admitted reference to the app image (or dependency
	// image for a dependency-only handle) that Prepare acquired. It is the
	// ownership authority for the image: the caller must transfer it to the
	// registry publication (runner.NewPrepared → Registry) or release it. It is
	// nil for a no-runtime app and for hand-built Prepared values.
	lease *ImageLease
}

// Lease returns the admitted image lease Prepare acquired for this handle, or
// nil for a no-runtime app and hand-built handles. Ownership transfers to
// whoever publishes the handle (the runner registry); an unpublished handle
// must be released by calling ReleaseLease.
func (p *Prepared) Lease() *ImageLease {
	if p == nil {
		return nil
	}
	return p.lease
}

// ReleaseLease drops the handle's owned image lease, if any. It is idempotent
// and nil-safe, so a caller that discards a Prepared without publishing it
// (or publishes it and later supersedes it) never strands the image.
func (p *Prepared) ReleaseLease() {
	if p == nil || p.lease == nil {
		return
	}
	p.lease.Release()
}

// TakeLease transfers ownership of the handle's image lease to the caller and
// clears the handle's own reference, so the handle can no longer release it.
// It is the seam runner.NewPrepared uses to move the build's admitted reference
// into the registry publication without a double release. It returns nil for a
// no-runtime app or an already-transferred handle.
func (p *Prepared) TakeLease() *ImageLease {
	if p == nil || p.lease == nil {
		return nil
	}
	lease := p.lease
	p.lease = nil
	return lease
}

// Prepare builds exactly ONE image for the app's current content (never per
// handler or event), then returns a handle for executing invocations against it.
// It captures the app's selected source into one immutable snapshot and
// derives the content fingerprint, the image tag, and the staged build context
// from that single capture, so the tag can never describe one set of bytes while
// the image bakes another.
//
// The image reference is derived from that content fingerprint, so the same
// source always maps to the same fingerprinted image. If that image is already
// present locally (an earlier build or previous boot produced it), the build is
// skipped and the existing image reused — restart-without-changes is cheap. A
// fingerprint error fails Prepare: the reconciler already computes the
// fingerprint before calling Prepare and retains the previous version on error,
// and for startup a fingerprint failure marks the app unavailable, which is
// consistent with the existing build-failure handling.
//
// A successful Prepare (re)activates the app in the warm-container cache,
// clearing any prior removal mark and un-retiring THIS exact image so a
// removed-then-recreated app warms again. Activation is deliberately NOT
// done up front: a failed prepare must not lift a removal, or a stale acquire
// could warm an app the reconciler has not actually reconciled.
func (m *Manager) Prepare(ctx context.Context, fn app.App) (*Prepared, error) {
	return m.prepare(ctx, fn, "", nil)
}

// PrepareWithFingerprint is Prepare with a caller-supplied content fingerprint.
// The worker's startup path computes each loaded app's fingerprint exactly
// once (before the state phase) and passes it here so Prepare does not rescan the
// tree it already hashed. The supplied value is the identity the CALLER compared
// to decide a rebuild was needed; Prepare still captures the selected source once
// and tags the image from that capture, returning it as Prepared.Fingerprint. If
// the source mutated between the caller's scan and the capture, the capture wins
// (coherent tag and bytes) and the caller persists the RETURNED value, so the
// built generation is never mislabeled.
//
// An empty fingerprint means "not supplied" and falls back to computing one
// internally, so direct and test callers that have no ready fingerprint keep the
// exact Prepare behavior. For a runtime-backed app the source selection must
// still be resolved (the build context is staged from it). A no-runtime app
// takes no filesystem walk at all: its fingerprint is template-only by
// construction (see FingerprintApp).
//
// Correctness of the supplied value at startup is preserved by the caller's
// ordering: the reconciler's watcher is established BEFORE the supplied
// fingerprint is seeded (see reconciler.PrepareWatch/Seed), and a change landing
// between the fingerprint scan and the build is still caught by the first
// reconcile's own rescan, because the supplied seed is the older value.
func (m *Manager) PrepareWithFingerprint(
	ctx context.Context,
	fn app.App,
	fingerprint string,
) (*Prepared, error) {
	return m.prepare(ctx, fn, fingerprint, nil)
}

// PrepareWithFingerprintAndSelection is Prepare with BOTH a caller-supplied
// content fingerprint and the already-resolved source selection that fingerprint
// was computed from. The reconciler computes the fingerprint to decide whether a
// rebuild is needed, resolving the selection once (app.SelectAndFingerprintApp),
// and passes BOTH here so the build re-derives neither the policy nor the hash.
// Prepare then captures one immutable snapshot of that selection and derives the
// tag, the staged context, and the returned identity from that single capture, so
// a concurrent edit can no longer make the tag and the baked bytes disagree.
//
// A nil selection falls back to resolving one here (matching PrepareWithFingerprint).
// A no-runtime app ignores both: it builds no image and its template-only
// fingerprint is used verbatim when supplied.
func (m *Manager) PrepareWithFingerprintAndSelection(
	ctx context.Context,
	fn app.App,
	fingerprint string,
	selection *source.Selection,
) (*Prepared, error) {
	return m.prepare(ctx, fn, fingerprint, selection)
}

// prepare is the shared implementation behind Prepare,
// PrepareWithFingerprint, and PrepareWithFingerprintAndSelection. fingerprint is
// the caller-supplied content fingerprint, or "" to compute one here.
// suppliedSelection is the caller's already-resolved source selection for a
// runtime-backed app, or nil to resolve one here.
func (m *Manager) prepare(
	ctx context.Context,
	fn app.App,
	fingerprint string,
	suppliedSelection *source.Selection,
) (*Prepared, error) {
	// A template that needs no runtime (its services all use the external
	// `image` source, and it has no events or schedules) has no app image
	// to build: the services bring their own images. Prepare still succeeds so
	// the app is available for service convergence, returning a handle with
	// no image — no entrypoint service exists to consume it. The fingerprint is
	// still recorded so template changes gate reconciliation exactly as for a
	// runtime-backed app, but it is computed over template.yaml ALONE
	// (FingerprintApp): no app source is ever baked into an image, so
	// scanning the tree would read files nothing depends on. A caller-supplied
	// fingerprint (the worker's startup path) is used verbatim, so even the
	// template read is skipped.
	if !fn.Template.NeedsRuntime() {
		fp := fingerprint
		if fp == "" {
			var err error
			fp, err = app.FingerprintApp(fn.Dir, fn.Template)
			if err != nil {
				return nil, fmt.Errorf("app %q: fingerprint: %w", fn.Name, err)
			}
		}
		m.log.Debug("App: no runtime required; services bring their own images",
			"app", fn.Name)
		prepared := &Prepared{
			Name:        fn.Name,
			Fingerprint: fp,
			Concurrency: m.effectiveConcurrency(fn),
		}
		m.containers.activateApp(fn.Name, "")
		m.containers.setAppConcurrency(fn.Name, prepared.Concurrency)
		m.containers.setAppResources(fn.Name, fn.Template.ResourceLimits())
		// A no-runtime app has no app image and no source to mount; clear any
		// stale mount record from a previous runtime-backed version.
		m.clearSourceMount(fn.Name)
		return prepared, nil
	}

	// Acquire one preparation permit BEFORE any runtime-backed disk/metadata
	// work, so the per-worker cap (MAX_CONCURRENT_BUILDS) bounds the WHOLE
	// pipeline that follows — source selection (when resolved here), the source
	// snapshot, the dependency snapshot, the reuse probes, the dependency image
	// build, and the app image build — not merely the Docker call. Dependency
	// and app sub-builds are therefore sequential within one permit. The wait
	// selects on the caller's ctx and returns ctx.Err() WITHOUT taking the
	// snapshot, probing, building, or logging when cancelled; the build itself
	// stays independently bounded by the manager lifecycle once the permit is
	// held, since the limiter capacity is immutable and the permits channel is
	// never closed. The deferred release runs on every return, failure, and
	// panic. The limiter is per Manager (one per worker), so this bounds this
	// worker's preparations only.
	limit := m.buildLimiterFor()
	if err := limit.acquire(ctx); err != nil {
		return nil, fmt.Errorf("app %q: wait for build slot: %w", fn.Name, err)
	}
	defer limit.release()
	if m.afterBuildPermit != nil {
		// Test-only seam: fires immediately after the permit is acquired and
		// before any source/build work, so a test can hold a preparation inside
		// the bounded section without sleeps. Nil in production.
		m.afterBuildPermit()
	}

	// Resolve the source-selection policy ONCE. The policy (the app's
	// .gitignore rules) decides which files are source, and a single resolved
	// Selection keeps the capture below from disagreeing with the caller about
	// it. A caller that already resolved it for the fingerprint it supplies (the
	// reconciler, the worker startup pass) passes it in rather than making us
	// re-read the policy.
	selection := suppliedSelection
	if selection == nil {
		var err error
		selection, err = source.ForDir(fn.Dir)
		if err != nil {
			return nil, fmt.Errorf("app %q: select sources: %w", fn.Name, err)
		}
	}

	// Capture ONE immutable snapshot of the selected source. The fingerprint, the
	// image tag, and the staged build context ALL derive from this single read, so
	// the tag can never describe one set of bytes while the image bakes another
	// (the fingerprint-then-stage TOCTOU this replaces). The snapshot is owned by
	// this preparation and released by the deferred Discard below on every path:
	// success, error, and cancellation. A read failure is fatal to the prepare —
	// there is deliberately no fallback that would stage live files under an
	// identity that was never verified against them.
	sourceSnapshot, err := app.CaptureSourceSnapshot(selection)
	if err != nil {
		return nil, fmt.Errorf("app %q: snapshot source: %w", fn.Name, err)
	}
	defer sourceSnapshot.Discard()
	if m.afterSourceSnapshot != nil {
		// Test-only seam: a test can mutate the on-disk source at exactly this
		// boundary to prove the tag and the staged bytes still come from the one
		// captured snapshot, or retain the snapshot to observe its release. Nil in
		// production.
		m.afterSourceSnapshot(sourceSnapshot)
	}
	fp := sourceSnapshot.Fingerprint()
	if fp == "" {
		// CaptureSourceSnapshot always yields a 64-hex digest, so an empty value
		// would mean the identity is unavailable; never build under it.
		return nil, fmt.Errorf("app %q: empty source fingerprint", fn.Name)
	}
	if fingerprint != "" && fingerprint != fp {
		// The caller's pre-computed fingerprint (the value it compared to decide a
		// rebuild was needed) no longer matches the captured bytes: the source
		// mutated between the caller's read and this capture. The snapshot is
		// authoritative — the image is tagged with and built from exactly its
		// bytes — and the caller persists the RETURNED fingerprint, so the built
		// generation is never mislabeled. The next reconcile/audit observes the
		// caller's now-stale value and rebuilds.
		m.log.Debug("App: source changed before snapshot; using snapshot identity",
			"app", fn.Name,
			"supplied_fingerprint", fingerprint,
			"snapshot_fingerprint", fp,
		)
	}

	spec, err := lookup(fn.Template.Runtime)
	if err != nil {
		return nil, fmt.Errorf("app %q: %w", fn.Name, err)
	}

	// mountSource is the SOURCE_MOUNT decision for THIS app: enabled globally and
	// permitted by the runtime's dependency layout. It is resolved BEFORE Plan so
	// the engine can shape a source-mounted image (Node persists its pinned
	// esbuild for runtime TypeScript transpilation instead of baking generated
	// files). Only when it is true is the live source bind-mounted and the image
	// built without baking it.
	mountSource := m.sourceMount && spec.MountableSource
	spec.SourceMounted = mountSource

	// sourceHostPath is the path the Docker daemon must be given for the live
	// source bind. Under the bundled Compose layout Relay runs in a container
	// whose /apps is a host bind mount, so fn.Dir (/apps/foo) is NOT
	// daemon-visible; daemonSourcePath maps it to the inspected mount's Source
	// plus the relative path. Native-host Relay and every unmappable case keep
	// fn.Dir, and Docker's own source validation is still the final authority.
	sourceHostPath := fn.Dir
	if mountSource {
		sourceHostPath = m.daemonSourcePath(ctx, fn.Dir)
	}

	eng, err := engineFor(spec)
	if err != nil {
		return nil, fmt.Errorf("app %q: %w", fn.Name, err)
	}

	planResult, err := eng.Plan(spec, fn.Dir, templateHandlers(fn))
	if err != nil {
		return nil, fmt.Errorf("app %q: plan: %w", fn.Name, err)
	}

	// mountTarget is the in-container path the live source is mounted at. An
	// engine may place it OUTSIDE the workdir when dependencies or generated
	// files live under the workdir (Node mounts at /app/src so /app/node_modules
	// stays visible); an empty target falls back to the workdir (Python).
	mountTarget := planResult.SourceMountTarget
	if mountTarget == "" {
		mountTarget = planResult.WorkDir
	}

	// bootstrapHash pins the runtime-injected bootstrap content (the engine's
	// embedded plan files) plus the entrypoint onto the image as a label. The
	// fingerprint above covers ONLY the app dir, so the label is what
	// lets the reuse path below detect an image built with a stale bootstrap
	// (e.g. by an older Relay version) under the exact same tag.
	bootstrapLabelHash := bootstrapHash(planResult)

	// The dependency manifest snapshot is captured ONCE when the app declares
	// deps, so the dependency fingerprint and the bytes staged into the
	// dependency image come from the same read. It is computed BEFORE the app
	// image reference because the app image's FROM is the dependency reference:
	// a manifest-content change (a new dependency fingerprint/tag) must produce a
	// new app tag and rebuild the app image even when the app source is
	// unchanged, or the reuse probe would serve a stale app image.
	var depSnap dependencySnapshot
	// depFingerprint is the content address computed ONCE from the immutable
	// snapshot. It names the tag (depRef) and is stamped as the image's label,
	// so computing it once and passing it to ensureDependencyImage keeps the
	// tag, the label, and the staged bytes from ever disagreeing.
	var depFingerprint string
	// depRef names the dependency image this app image is built FROM ("" when
	// the app declares no deps).
	var depRef string
	// The dependency snapshot's private root (the relay-dep-build-* context) is
	// removed on success, failure, and cancellation. The release is deferred
	// BEFORE the block so a fingerprint failure after a successful capture still
	// removes it; release is nil-safe on the zero snapshot.
	defer depSnap.release()
	if !planResult.Deps.IsZero() {
		depSnap, err = snapshotDependency(fn.Dir, planResult.Deps)
		if err != nil {
			return nil, fmt.Errorf("app %q: %w", fn.Name, fmt.Errorf("dependency fingerprint: %w", err))
		}
		depFingerprint, err = m.dependencyFingerprint(arch, platform, spec, planResult.Deps, depSnap)
		if err != nil {
			return nil, fmt.Errorf("app %q: dependency fingerprint: %w", fn.Name, err)
		}
		depRef = depImageRef(depFingerprint)
	}

	// The app image reference. For the historical baked-source image it embeds
	// the source fingerprint, so every source version is a distinct image. Under
	// SOURCE_MOUNT the live source is bind-mounted instead of baked, so the tag
	// is derived from the non-source inputs (runtime, dependency, bootstrap) and
	// a source-only change reuses the same image; the reconciler advances the
	// warm/service generation from the source fingerprint instead (see
	// Prepared.Fingerprint).
	imageFingerprint := fp
	if mountSource {
		imageFingerprint = sourceMountImageFingerprint(spec.Name, depRef, renderDockerfileWithSource(planResult, false))
	}
	image := ImageRef(fn.Name, imageFingerprint)

	prepared := &Prepared{
		Name:        fn.Name,
		Image:       image,
		Fingerprint: fp,
		Env:         planResult.Env,
		Concurrency: m.effectiveConcurrency(fn),
		// The dependency image the app image is built FROM (both paths); it is
		// the input to dependency garbage collection.
		Dependency: depRef,
	}

	// Admit the app image reference BEFORE any probe or build. Holding this
	// lease from here until the handle is published (or discarded) closes the
	// TOCTOU window where an image could be committed to removal between the
	// existence probe and its use. A retirement in progress rejects the new
	// lease with ErrImageRetiring, which the caller retries.
	funcLease, err := m.AcquireImageLease(image)
	if err != nil {
		return nil, fmt.Errorf("app %q: %w", fn.Name, err)
	}
	// The app lease is transferred to the returned Prepared on success;
	// until then every failure path releases it so a failed Prepare never pins
	// the image.
	leaseTransferred := false
	defer func() {
		if !leaseTransferred {
			funcLease.Release()
		}
	}()
	prepared.lease = funcLease

	var depLease *ImageLease
	// releaseDep releases the dependency lease (if admitted). The dependency
	// snapshot root is released by the deferred depSnap.release above. It is
	// deferred so the lease is dropped on success, failure, and cancellation.
	releaseDep := func() {
		if depLease != nil {
			depLease.Release()
			depLease = nil
		}
	}
	// The dependency lease is held from its admission below through the ACTUAL
	// dependency build (ensureDependencyImage, when the layer is absent) and the
	// app image build that consumes the layer via FROM, so dependency GC
	// cannot remove the layer between its existence probe and its consumption.
	// The lease therefore spans the whole dependency use in Prepare, not merely
	// the probe. See TestPrepareDependencyLeaseSpansBuildVsGC.
	defer releaseDep()
	if depRef != "" {
		// Admit the dependency layer BEFORE its own reuse/existence probe, the
		// dependency image build, and the app image build below, so
		// dependency GC's retirement gate cannot remove the layer between the
		// probe and the FROM consumption.
		depLease, err = m.AcquireImageLease(depRef)
		if err != nil {
			return nil, fmt.Errorf("app %q: dependency %s: %w", fn.Name, depRef, err)
		}
	}

	// Reuse an existing local image when present. The fingerprinted reference is
	// the identity: an image carrying this exact tag was necessarily built from
	// identical source (the tag embeds the fingerprint prefix), so no content
	// comparison is needed. The inspect duration is logged at Debug on BOTH
	// outcomes below, so the reuse-probe cost (up to two daemon round trips) is
	// visible during startup triage whether the app reuses or builds,
	// without a benchmark and without a line per content file.
	reuseStart := time.Now()
	if m.imageExists(ctx, image) && m.bootstrapLabelMatches(ctx, image, bootstrapLabelHash) {
		m.log.Debug(
			"App: image exists; reusing",
			"app", fn.Name,
			"image", image,
			"inspect_duration", time.Since(reuseStart),
		)
		// The prepare succeeded (the image is present and current), so activate
		// the exact image: a previously removed app warms again, and a
		// reverted same-source image is no longer treated as retired.
		m.containers.activateApp(fn.Name, image)
		// Propagate the reconciled concurrency to the live pool: the effective
		// bound must follow a successful Prepare even when the image was reused
		// (a concurrency-only change rebuilds the same fingerprinted image).
		m.containers.setAppConcurrency(fn.Name, prepared.Concurrency)
		// Propagate the reconciled resource limits too: they never affect the
		// image fingerprint, so a resource-only change reaches the live pool
		// here (or via SetAppResources on the reconciler's skip path).
		m.containers.setAppResources(fn.Name, fn.Template.ResourceLimits())
		// Publish the live source mount (or clear a stale one) so a source-only
		// change — which reuses this exact image — still reaches execution and
		// entrypoint-service containers. Identity is the source fingerprint the
		// reconciler compared; the mount itself is the app's live /apps dir.
		if mountSource {
			m.setSourceMount(fn.Name, SourceMount{
				HostPath: sourceHostPath,
				Target:   mountTarget,
				WorkDir:  planResult.SourceMountWorkDir,
				Masks:    planResult.SourceMountMasks,
				Identity: fp,
			})
		} else {
			m.clearSourceMount(fn.Name)
		}
		leaseTransferred = true
		return prepared, nil
	}
	// The reuse probe missed: the tag is absent locally or its bootstrap label is
	// stale, so Prepare will (re)build. Log the miss with the SAME inspection
	// timing shape as the hit so both branches make the daemon probe cost
	// observable during startup triage; the following build has its own
	// duration/result logging.
	m.log.Debug(
		"App: image absent or stale; building",
		"app", fn.Name,
		"image", image,
		"inspect_duration", time.Since(reuseStart),
	)

	// When the app declares a dependency layer, ensure the dependency image
	// exists first and build the app image FROM it. The dependency image is
	// content-addressed (no app name): it is shared across every app
	// and every version with identical (runtime + arch + manifest + install), so
	// a changed requirements.txt yields a NEW tag and an unchanged one reuses the
	// existing layer with no rebuild (even when the app's source changed).
	if !planResult.Deps.IsZero() {
		depRef, err = m.ensureDependencyImage(
			ctx, fn, spec, planResult.Deps, depSnap, depFingerprint, depRef,
		)
		if err != nil {
			return nil, fmt.Errorf("app %q: %w", fn.Name, err)
		}
		// The app image inherits every layer of the dependency image, so
		// its Dockerfile's FROM is the dependency reference rather than the raw
		// base image. The engine moved the install into Deps, so the app
		// image carries no install RUN of its own — only UserSetup/User/Env/
		// Entrypoint on top of the dependency layer. The dependency image was
		// built with the runtime's external tools (e.g. uv), so the app
		// image inherits them via FROM and does not need to materialize them
		// again.
		planResult.BaseImage = depRef
		planResult.RuntimeTools = nil
	}

	start := time.Now()
	// The build runs on an independent, lifecycle-rooted buildTimeout context,
	// NOT on the caller's ctx. In production the reconciler and the startup
	// preparation both pass the worker LIFECYCLE context (Prepare is a seam
	// embedders and tests may also call), so a slow image build must not depend
	// on the caller's deadline being long, while still being cancelled at Relay
	// shutdown via the manager lifecycle. The reuse probes above deliberately
	// keep using the caller's ctx — they are quick and must honor its
	// cancellation.
	//
	// notifyAppBuild fires at this exact boundary — after every reuse probe,
	// immediately before buildImage — so the caller publishes the persisted
	// building status only when an image build is actually issued; a reused image
	// never flashes building.
	notifyAppBuild(ctx)
	buildCtx, buildCancel := m.buildContext()
	defer buildCancel()
	// The actual managed image build boundary (after every reuse probe). The span
	// is rooted in the caller's context so it nests under the preparing
	// app's span; the build itself still runs on the lifecycle-bounded
	// buildCtx. Only a real build is spanned; a reuse probe (above) is not.
	_, buildSpan := startRuntimeSpan(ctx, "runtime.build", fn.Name, image)
	// The managed-image relay.fingerprint label. For a baked-source image it is
	// the source fingerprint the tag embeds. For a SOURCE_MOUNT image it is
	// deliberately EMPTY: the image is source-independent, and Execute derives
	// the warm generation identity from Prepared.Fingerprint (the live source
	// fingerprint) by falling back to it when no label is present. Stamping the
	// build-time fingerprint would freeze the generation identity at build time
	// and a later source-only change would not recycle the warm containers.
	// The managed-image relay.fingerprint label. For a baked-source image it is
	// the source fingerprint the tag embeds. For a SOURCE_MOUNT image it is
	// deliberately EMPTY: the image is source-independent, and Execute derives
	// the warm generation identity from Prepared.Fingerprint (the live source
	// fingerprint) by falling back to it when no label is present. Stamping the
	// build-time fingerprint would freeze the generation identity at build time
	// and a later source-only change would not recycle the warm containers.
	//
	// The label is stamped as an explicit EMPTY string rather than omitted: a
	// source-mounted app image is built FROM a dependency image, and Docker
	// INHERITS labels through FROM, so an omitted label would surface the
	// dependency image's own relay.fingerprint (the constant dependency
	// fingerprint) and freeze the warm generation. The empty value CLEARS the
	// inherited label, so inspection yields "" and the Prepared fingerprint is
	// used.
	appLabels := appImageLabels(fn.Name, fp, depRef, bootstrapLabelHash)
	if mountSource {
		appLabels[labelFingerprint] = ""
	}
	if err := buildImage(buildCtx, m.cli, fn.Name, fn, planResult, image, appLabels, sourceSnapshot, m.metrics, !mountSource); err != nil {
		buildSpan.RecordError(err)
		buildSpan.SetStatus(codes.Error, err.Error())
		buildSpan.End()
		elapsed := time.Since(start)
		m.metrics.ObserveDurationLabels(metrics.MetricAppBuild, []metrics.Label{
			{Name: "app", Value: fn.Name},
		}, elapsed)
		m.metrics.IncLabels(metrics.MetricAppBuildFailures, []metrics.Label{
			{Name: "app", Value: fn.Name},
		})
		// App names are validated to [a-z0-9][a-z0-9._-]* (bounded by
		// app count), so using them as labels is low-cardinality.
		m.log.Error("App: build failed",
			"app", fn.Name,
			"duration", elapsed,
			"result", "failed",
		)
		return nil, err
	}
	buildSpan.End()
	elapsed := time.Since(start)
	m.metrics.ObserveDurationLabels(metrics.MetricAppBuild,
		[]metrics.Label{{Name: "app", Value: fn.Name}}, elapsed)
	m.log.Info("App: built",
		"app", fn.Name,
		"duration", elapsed,
		"result", "success",
	)
	// The build succeeded: activate the exact image so a removed-then-recreated
	// app warms again and a same-content rebuild is not left retired, then
	// propagate the reconciled concurrency to the live pool (a hot-swapped
	// concurrency takes effect without a worker restart).
	m.containers.activateApp(fn.Name, image)
	m.containers.setAppConcurrency(fn.Name, prepared.Concurrency)
	m.containers.setAppResources(fn.Name, fn.Template.ResourceLimits())
	// Publish the live source mount for this now-active version (or clear a
	// stale one), mirroring the reuse path above.
	if mountSource {
		m.setSourceMount(fn.Name, SourceMount{
			HostPath: sourceHostPath,
			Target:   mountTarget,
			WorkDir:  planResult.SourceMountWorkDir,
			Masks:    planResult.SourceMountMasks,
			Identity: fp,
		})
	} else {
		m.clearSourceMount(fn.Name)
	}
	leaseTransferred = true
	prepared.Dependency = depRef
	return prepared, nil
}

// templateHandlers returns the app's handler MODULE parts (the portion of
// each `module.function` handler before the LAST dot), collected from the
// template's event rules and cron schedules, sorted and deduped. Only the module
// part is needed: it identifies the source file the engine must resolve (and, for
// the Node engine, transpile when it is TypeScript). A nil template, a malformed
// handler without a dot, and an empty module are skipped: template validation
// already rejects them on the parse path, and this keeps Prepare total for
// hand-built templates used by tests and direct callers.
func templateHandlers(fn app.App) []string {
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

// resolveConcurrency returns the app's resolved per-app concurrency
// for the warm container pool. It reads the template's parsed value, defaulting
// a zero value (an app built without parsing, or a nil template) to
// app.DefaultConcurrency — the same fallback the runner applies to its
// per-app semaphore, so the pool's bound and the runner's bound agree.
func resolveConcurrency(fn app.App) int {
	if fn.Template == nil || fn.Template.Concurrency < 1 {
		return app.DefaultConcurrency
	}
	return fn.Template.Concurrency
}

// effectiveConcurrency returns the app's EFFECTIVE per-app
// concurrency for the warm container pool: the template's resolved concurrency
// clipped to the worker-global MAX_CONCURRENT_INVOCATIONS. When the template asks for more
// than the global cap (e.g. concurrency 15 with MAX_CONCURRENT_INVOCATIONS=8), the pool
// warms, reports, and admits only the cap's worth — the effective intersection
// of the two limits the README documents, matching the runner's clipped
// per-app semaphore.
func (m *Manager) effectiveConcurrency(fn app.App) int {
	return m.clipConcurrency(resolveConcurrency(fn))
}

// clipConcurrency clips an already-resolved per-app concurrency to the
// worker-global MAX_CONCURRENT_INVOCATIONS. A value below 1 is treated as
// app.DefaultConcurrency first, and a zero Manager.maxConcurrentInvocations (a
// Manager constructed directly by tests) is treated as DefaultMaxConcurrentInvocations,
// never "uncapped". It is the single clipping rule applied by Prepare and
// Execute, so a hand-built Prepared (direct/integration callers) can never
// warm a pool larger than the worker-global cap.
func (m *Manager) clipConcurrency(n int) int {
	if n < 1 {
		n = app.DefaultConcurrency
	}
	limit := m.maxConcurrentInvocations
	if limit < 1 {
		limit = DefaultMaxConcurrentInvocations
	}
	if n > limit {
		return limit
	}
	return n
}

// dependencyFingerprint computes a dependency layer's content address from the
// snapshot Prepare captured, using the injected seam when set (tests) and the
// production dependencyFingerprintFrom otherwise. Prepare calls it exactly once
// per dependency-bearing prepare.
func (m *Manager) dependencyFingerprint(arch, platform string, spec plan.Spec, deps plan.Deps, snap dependencySnapshot) (string, error) {
	if m.depFingerprint != nil {
		return m.depFingerprint(arch, platform, spec, deps, snap)
	}
	return dependencyFingerprintFrom(arch, platform, spec, deps, snap)
}

// ensureDependencyImage builds the dependency image for the app's
// dependency manifest set, returning the dependency image reference. It is a
// no-op (returns the existing reference) when the dependency image is already
// present locally — the content address makes existence the correctness test,
// since the tag embeds the fingerprint over every relevant input. If the
// dependency build fails, Prepare fails: there is no fallback to the old
// single-stage build, because the app image's Dockerfile inherits its
// dependency layers via FROM and cannot be built without them.
//
// depFingerprint is the content address the caller already computed from snap;
// it names the tag and is stamped as the image's label. depRef is the reference
// derived from it (so a build failure is attributable, and the caller has it in
// hand even for the reuse case). Both are passed in so this method never
// rehashes the snapshot: the tag, the label, and the staged bytes all come from
// the ONE immutable read.
func (m *Manager) ensureDependencyImage(
	ctx context.Context,
	fn app.App,
	spec plan.Spec,
	deps plan.Deps,
	snap dependencySnapshot,
	depFingerprint string,
	depRef string,
) (string, error) {
	if depRef == "" {
		depRef = depImageRef(depFingerprint)
	}
	// The inspect duration is logged at Debug on BOTH outcomes below, mirroring
	// Prepare's app-image reuse probe, so the dependency probe's daemon
	// round trip is visible during startup triage whether the layer is reused
	// or built — without a line per staged manifest.
	depReuseStart := time.Now()
	if m.imageExists(ctx, depRef) {
		m.log.Debug(
			"Dependency image exists; reusing",
			"dep_image", depRef,
			"inspect_duration", time.Since(depReuseStart),
		)
		return depRef, nil
	}
	m.log.Debug(
		"Dependency image absent; building",
		"dep_image", depRef,
		"inspect_duration", time.Since(depReuseStart),
	)

	start := time.Now()
	// As in Prepare's app-image build, the dependency build uses an
	// independent lifecycle-rooted buildTimeout context rather than the caller's
	// ctx, so a slow install step is never cut off by a short reconcile budget
	// while still being cancelled at Relay shutdown. The imageExists probe above
	// keeps the caller's ctx. notifyAppBuild fires at this exact boundary so
	// a dependency build also reports the building status.
	notifyAppBuild(ctx)
	buildCtx, buildCancel := m.buildContext()
	defer buildCancel()
	// The dependency layer is a real build, so it is spanned like the app
	// image build. The span nests under the preparing app's span.
	_, depBuildSpan := startRuntimeSpan(ctx, "runtime.build", fn.Name, depRef)
	if err := buildDependencyImage(buildCtx, m.cli, spec, deps, snap, depRef, depFingerprint, m.metrics); err != nil {
		depBuildSpan.RecordError(err)
		depBuildSpan.SetStatus(codes.Error, err.Error())
		depBuildSpan.End()
		elapsed := time.Since(start)
		// Dependency-image build failures count as app build failures so the
		// existing failure metric/label surface stays the single observability
		// contract for "this app could not be prepared".
		m.metrics.IncLabels(metrics.MetricAppBuildFailures, []metrics.Label{
			{Name: "app", Value: fn.Name},
		})
		m.log.Error("App: dependency build failed",
			"app", fn.Name,
			"duration", elapsed,
			"dep_image", depRef,
			"result", "failed",
		)
		return "", err
	}
	depBuildSpan.End()
	elapsed := time.Since(start)
	m.metrics.ObserveDurationLabels(metrics.MetricAppBuild,
		[]metrics.Label{{Name: "app", Value: fn.Name}}, elapsed)
	m.log.Info("App: dependency layer built",
		"app", fn.Name,
		"duration", elapsed,
		"dep_image", depRef,
		"result", "success",
	)
	return depRef, nil
}

// Execute runs the given handler invocation against a REUSED execution
// container leased from the app's warm pool (up to Prepared.Concurrency
// containers per app per image version; see container_cache.go and
// execution_container.go). The first invocation for an app starts a
// container (stamping its creation-time identity labels from the RunMeta in
// ctx); concurrent invocations of the same app lease distinct containers,
// and each container serves one invocation at a time over the line-JSON
// invocation protocol until it is discarded (timeout, process exit, protocol
// error, image change). A handler failure (ok:false) does NOT discard it.
//
// The pool's bound is Prepared.Concurrency (the effective value, already
// clipped to MAX_CONCURRENT_INVOCATIONS by Prepare and re-clipped here), the same value
// the runner's per-app semaphore uses, so in the runner path the pool
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
) (retErr error) {
	// The top-level execution span. It is a child of the invocation's context
	// (which the runner already instrumented with function.invoke), so the
	// runtime's acquire/invoke children nest under it. Payload and env values
	// are never attached.
	ctx, span := startRuntimeSpan(ctx, "runtime.execute", prepared.Name, prepared.Image)
	defer func() { finishRuntimeSpan(span, retErr) }()
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
		// Same safety net for the image identity: the owning app image is
		// known here, so the label (and RemoveImage's in-use guard) stays
		// accurate for direct callers that never set it.
		meta.Image = prepared.Image
	}
	if meta.App == "" {
		meta.App = prepared.Name
	}

	// Pin the image for the whole execution. An execution admitted before the
	// image's retirement carries that admitted lease on ctx (the runner passes a
	// registry snapshot's publication lease), so it holds admitted authority
	// even while retirement drains. admitLease validates that entitlement: the
	// carried lease must actually be live and pin THIS image, and the execution
	// takes its OWN share of it, so the reference is held for the execution's
	// whole duration even if the caller drops its lease concurrently. A direct
	// caller that carries no (or no matching) lease acquires its own independent
	// one, which a retirement in progress rejects with a retryable
	// ErrImageRetiring rather than letting the execution race ImageRemove. A nil
	// lease (no-runtime image, fake executor) is transparent.
	owned, err := m.admitLease(ctx, prepared.Image)
	if err != nil {
		return fmt.Errorf("execute %s: %w", prepared.Name, err)
	}
	if owned != nil {
		defer owned.Release()
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
	// Resolve the app's effective per-container resource limits ONCE for
	// this execution, from the cache's last published configuration, and derive
	// the config fingerprint from exactly those limits. Passing the same pair to
	// the create and to the pool means the HostConfig a container is created
	// with and the generation it is pooled under always agree, so a resource-only
	// hot change rotates containers without a rebuild.
	limits := m.containers.appResources(prepared.Name)
	config := limits.Fingerprint()
	// Resolve the managed image's IMMUTABLE content identity ONCE for this
	// execution, after the image lease above and BEFORE the pool lease and the
	// container create. The lease pins the reference for the whole inspection, so
	// a concurrent retirement/removal cannot delete the image between the
	// resolution and the create; the generation key uses this identity (reference
	// + content ID + fingerprint metadata), so the same tag resolving to new
	// bytes rotates its warm generation; and the create uses the resolved content
	// ID so the container runs exactly the bytes that were resolved, closing the
	// inspect-then-create TOCTOU window. The mutable reference stays
	// prepared.Image for labels, spans, and image retirement. Resolution failure
	// degrades to the reference identity, never failing the invocation.
	img := m.resolveImageIdentity(ctx, prepared.Image, prepared.Fingerprint)
	start := func() (reusableContainer, error) {
		// Every execution container joins the worker-global network set
		// (WithNetworks), which the worker has already verified exists at
		// startup. A network that disappears between verification and create
		// surfaces as a create error here; Relay never creates networks.
		return m.startContainer(ctx, prepared.Name, img, prepared.Env, limits, idMeta)
	}
	// Prepared.Concurrency is populated by Prepare as the effective bound
	// (template concurrency clipped to MAX_CONCURRENT_INVOCATIONS). A hand-built Prepared
	// (direct/integration callers) may carry the raw template value or leave it
	// zero, so clip it here too: the pool bound can never exceed the
	// worker-global cap, and the same bound the runner's clipped per-app
	// semaphore uses is what the pool enforces, keeping the pool from ever being
	// a stricter limiter than the runner's per-app semaphore.
	max := m.clipConcurrency(prepared.Concurrency)
	return m.containers.executeVersion(
		ctx, prepared.Name, img.identity(), config, max, start, handler, eventJSON, envMap(extraEnv),
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

// SetAppResources publishes an app's effective per-container resource
// limits to the live warm pool WITHOUT a rebuild. It is the resource half of a
// hot template change: resource limits intentionally do not participate in the
// image fingerprint, so the reconciler's unchanged-fingerprint skip path calls
// this (through the Builder's optional resourceSetter capability) when only the
// app's `resources` changed. A changed value supersedes the current
// container generation so old-config idle containers are discarded and busy ones
// drain, exactly like an image change but without touching the image reference
// or its ownership lease. It is idempotent for an unchanged value.
func (m *Manager) SetAppResources(name string, limits app.ResourceLimits) {
	if name == "" {
		return
	}
	m.containers.setAppResources(name, limits)
}

// PoolSnapshot returns a point-in-time view of name's live warm-container pool:
// capacity, container counts by lease state, and the cumulative acquire/discard
// counters. The gauges are read from the pool's authoritative in-memory state
// (no Docker round trip); the counters are read from the same metrics registry
// the worker exposes on /metrics. ok is false when the app has never warmed
// a pool (or its pool was already removed), so a caller can omit the section
// rather than render stale zeros. It is safe for concurrent use.
//
// This is a LIVE, worker-local view. It exists for in-process callers (an
// embedded CLI/provider, tests); the standalone `relay app inspect` process
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

// RemoveApp discards a removed app's warm container state: new
// acquires for the app fail immediately, idle containers are discarded now,
// busy ones are retired and discarded when their invocation releases, and the
// pool's state is deleted once it is empty. A later release of a busy container
// can never recreate the state (the app name is remembered as removed until
// Prepared reactivates it). It is non-blocking, so the reconciler's removal hook
// is never stalled by an in-flight invocation, and it is the runtime half of
// app removal; the runner separately retires the app's images.
//
// The app's runtime-pool metric series are deleted by the cache inside the
// SAME critical section that installs the removal tombstone, so no concurrent
// acquire/discard can recreate them (see containerCache.removeApp). A
// genuinely reactivated app gets a fresh pool (and fresh series) after that
// section. The worker's own metricsInstance.RemoveApp and the flush-time
// SweepAppMetrics still cover the runner's series; the runtime no longer
// performs a second, racy delete here.
func (m *Manager) RemoveApp(name string) {
	if name == "" {
		return
	}
	m.containers.removeApp(name)
	// Drop the app's external-service pull-check records so a later
	// re-added app starts with an immediate remote check and the in-memory
	// map does not grow without bound across removals.
	m.forgetServicePullChecks(name)
	// Drop the app's recorded live source mount so a later re-added app (possibly
	// with a different runtime) never inherits a stale mount.
	m.clearSourceMount(name)
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

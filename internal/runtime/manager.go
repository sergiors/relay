package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"

	"relay/internal/app"
	"relay/internal/buildtemp"
	"relay/internal/observability/metrics"
	"relay/internal/runtime/plan"
)

// arch and platform are the build-host architecture keys for the dependency
// fingerprint. They default to the running process's GOARCH/GOOS (the daemon
// this process builds on is the daemon it executes on, so the build host is the
// correct key). Making them variables lets tests inject synthetic architectures.
var (
	arch     = runtime.GOARCH
	platform = runtime.GOOS
)

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
	// hostname identifies this worker for container ownership. It is the worker's
	// stable identity (config.WorkerID), so the relay.hostname container label,
	// the orphan/service cleanup scope, and the stream consumer identity are one
	// and the same. The startup orphan sweep uses it to distinguish this worker's
	// stalled containers from those of every other worker sharing the daemon.
	hostname string
	// selfHostname is the OS hostname of the container THIS process runs in,
	// used ONLY to locate Relay's own container for HOT_RELOAD source-path
	// mapping (see daemonSourcePath). It is deliberately separate from hostname:
	// hostname is the worker's stable ownership identity (WORKER_ID) stamped on
	// containers, while self-inspection must resolve the actual OCI hostname (the
	// Docker-assigned container ID by default) to find this process's own
	// container. It is resolved once at construction from os.Hostname; empty when
	// unavailable, in which case self-inspection is skipped (it never falls back
	// to hostname, which would risk inspecting a wrong container).
	selfHostname string
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
	// ops is the Manager's single Docker operation lifecycle: it gates admission
	// of Manager-owned Docker work, tracks it, owns the cancellation every
	// detached path derives from, and drains it before CloseContext closes the
	// Docker client. It is created by NewManager; a Manager constructed directly
	// by tests lazily initializes it on first use (opsInit). It is deliberately
	// separate from leases: leases specialize in image ownership/retirement,
	// while ops owns every Docker operation uniformly. See operations.go.
	ops     *managerOps
	opsInit sync.Once
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
	// sourceMount is the worker-global HOT_RELOAD switch: when true, a runtime
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
	// used to translate a HOT_RELOAD app path into the path the Docker daemon
	// actually sees when Relay is itself a container (see daemonSourcePath). A
	// zero value resolves on first use.
	selfMounts containerMounts
	// containerDetect overrides the container self-identification precondition
	// used by daemonSourcePath. Nil uses the production default (the resolved
	// self-hostname is container-ID-like). Set only by tests.
	containerDetect func() bool

	// buildTempStore is the Manager's handle to Relay's private build-temp root
	// (internal/buildtemp). It is used to create the session below and to
	// scavenge abandoned sessions at worker startup. Nil when the root could not
	// be established, in which case the Manager keeps the historical os.TempDir
	// build roots (correct, just not crash-reclaimable). Set once at
	// construction and read-only after.
	buildTempStore *buildtemp.Store
	// buildTemp is the Manager's private build-temp session: the source and
	// dependency snapshot roots a preparation stages into live inside it, and
	// its OS advisory owner lock makes a crashed process's residue reclaimable by
	// the next startup. Nil when no session could be established (the same
	// os.TempDir fallback as buildTempStore). Set once at construction, closed
	// by CloseContext.
	buildTemp *buildtemp.Session

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
	// startExecutionContainerFn replaces the production startExecutionContainer
	// call inside Manager.startContainer. It is nil in production and set only by
	// in-package tests so the create-window operation admission, the
	// shutdown-race cleanup, and the release path can be exercised
	// deterministically without a Docker attach hijack (which the scripted
	// daemon cannot perform).
	startExecutionContainerFn func(ctx context.Context, ops containerOps, cli *client.Client, log *slog.Logger, fn, image string, env []string, networks []string, limits app.ResourceLimits, meta RunMeta, workDir string, mounts ...mount.Mount) (reusableContainer, error)
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
	// sourceMount is the worker-global HOT_RELOAD switch (WithSourceMount).
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

// WithSourceMount enables or disables HOT_RELOAD for this worker: when true,
// a runtime whose dependency layout permits it (plan.Spec.MountableSource, i.e.
// Python and Node) builds a source-independent app image and bind-mounts the
// app's live /apps source read-only into its execution and entrypoint-service
// containers instead of baking it. The worker wires config's resolved
// HOT_RELOAD value; a direct NewManager caller that omits it gets the
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
// stable container-ownership identity (config.WorkerID); it is stamped as the
// relay.hostname label on every execution container and gates the startup orphan
// sweep. Relay's own OCI hostname — used only to locate its container for
// HOT_RELOAD source-path mapping — is resolved separately from os.Hostname (see
// the Manager.selfHostname field).
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
		selfHostname:             osHostname(),
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
	// The Docker operation lifecycle shares the manager lifecycle: closing the
	// manager (or a parent cancellation, e.g. Relay shutdown) cancels every
	// admitted operation, and CloseContext drains them before closing the client.
	mgr.ops = newManagerOps(lifecycle)
	mgr.containers = newContainerCache()
	mgr.containers.idleTimeout = resolved.idleTimeout
	mgr.containers.now = resolved.now
	mgr.containers.metrics = registry
	mgr.containers.maxWarm = resolved.maxWarmContainers
	mgr.containers.publishWarmCapacity()
	// The container cache admits ordinary pool teardowns as normal Manager
	// operations through the same lifecycle, so shutdown owns every discard.
	mgr.containers.ops = mgr.ops
	mgr.leases = newImageCoordinator()
	mgr.maintInterval = maintenanceInterval(resolved.idleTimeout)
	if !resolved.deferredMaintenance {
		mgr.startMaintenance(mgr.maintInterval)
	}
	// Establish the Manager's private build-temp session: the source and
	// dependency snapshot roots a preparation stages live inside it, protected by
	// an OS advisory owner lock, so a crashed process's residue is reclaimable at
	// the next startup (see ScavengeBuildRoots) instead of leaking under
	// os.TempDir. It is best-effort: if the shared temp root or its lock cannot
	// be used, the Manager keeps the historical os.TempDir build roots — correct,
	// just not crash-reclaimable — and logs why. A build must never fail because
	// temp coordination is unavailable. The session is created under the
	// coordination lock, so a concurrent Relay (or a concurrent NewManager in a
	// test) can neither see nor sweep a half-created session.
	if store, err := buildtemp.Open(buildtemp.DefaultRoot()); err != nil {
		mgr.Logger().Warn("Runtime: build temp root unavailable; build roots use the system temp dir", "error", err)
	} else {
		mgr.buildTempStore = store
		session, serr := store.NewSession(lifecycle)
		if serr != nil {
			mgr.Logger().Warn("Runtime: build temp session unavailable; build roots use the system temp dir", "error", serr)
		} else {
			mgr.buildTemp = session
		}
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
// reflect whether the daemon is still reachable.
//
// It is admitted through the manager's Docker operation gate, so a readiness
// query in flight when Manager shutdown begins is cancelled and joined before
// the Docker client closes, and a query after admission has closed is refused
// (readiness correctly reports not-ready). The caller still owns the bound: the
// caller's ctx is merged with, never replaced by, the manager lifecycle, so a
// cancelled query aborts promptly and can never stall readiness.
//
// The STARTUP ping in NewManager is deliberately not this method: it runs on the
// local ping context before the manager is returned, so no caller can hold the
// manager — and therefore no Close can race it.
func (m *Manager) Ping(ctx context.Context) error {
	if m == nil || m.cli == nil {
		return fmt.Errorf("docker client unavailable")
	}
	opCtx, release, err := m.beginDockerOp(ctx)
	if err != nil {
		return err
	}
	defer release()
	_, err = m.cli.Ping(opCtx, client.PingOptions{})
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

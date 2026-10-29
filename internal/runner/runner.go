package runner

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"relay/internal/observability/metrics"
	"relay/internal/runtime"
	"relay/internal/secrets"
)

// DefaultMaxConcurrentInvocations is the runner's default global cap on concurrently
// executing invocations in a single worker (MAX_CONCURRENT_INVOCATIONS). A zero or
// negative value passed to SetMaxConcurrentInvocations falls back to this. The runner
// deliberately owns this constant (it cannot import config; worker wires the
// cfg value).
const DefaultMaxConcurrentInvocations = 8

// slotWaitTimeout bounds how long Handle waits for a free concurrency slot
// (global or per-app) before giving up. The two slots are acquired sequentially,
// each with its own wait (see reserveSlots), so a single delivery can wait up to
// roughly 2*slotWaitTimeout, and warm admission (warmAdmitWait) can add more —
// which may exceed the stream layer's default MinPendingIdle reclaim threshold
// (1m). That overlap is safe: every wait here happens BEFORE TryStart, so a
// timed-out delivery claims no handler attempt, records no retry, ACKs nothing,
// and leaves the message pending in the PEL. A reclaim that overlaps a
// still-waiting local delivery therefore cannot cause a concurrent execution,
// because TryStart is the only admission point and invocation state rejects a
// second concurrent run. On timeout Handle returns ErrInvocationNotEligible and
// the message stays pending, so reclaim replays it later. The wait absorbs brief
// contention without churn; it is not a latency guarantee.
const slotWaitTimeout = 30 * time.Second

// warmAdmitWait bounds how long Handle/InvokeHandler wait for a slot in the
// worker-global warm-container budget (MAX_WARM_CONTAINERS) before leaving the
// work pending. It is added AFTER the concurrency-slot wait, so the total wait
// for one delivery can approach 60s (two sequential slot waits) plus this warm
// wait — it is NOT a bound below the stream layer's MinPendingIdle reclaim
// threshold (1m). That is safe because every wait happens BEFORE the handler
// attempt is claimed (TryStart): a delivery that exhausts the wait claims no
// attempt, records no retry, and stays pending in the PEL, so a reclaim
// overlapping a still-waiting local delivery cannot execute the same invocation
// concurrently — invocation state admits exactly one claim. A saturated budget
// is relieved as containers free, and the wait avoids reclaim churn on a brief
// overlap without ever charging a handler attempt.
const warmAdmitWait = 5 * time.Second

// Manual-invocation sentinel errors. They let the worker socket map a manual
// invocation failure onto a stable wire code without inspecting error strings
// (see internal/worker/socket.go). They are returned (wrapped) by
// Runner.InvokeApp.
var (
	// ErrAppNotFound reports a manual invocation for an app that is
	// absent from the current registry (never loaded, or already removed).
	ErrAppNotFound = errors.New("app not found")
	// ErrAppUnavailable reports a manual invocation for an app that is
	// registered but not runnable (its image could not be built at
	// startup/reconcile, so it has no Prepared handle).
	ErrAppUnavailable = errors.New("app unavailable")
	// ErrHandlerNotFound reports a DLQ replay whose handler is no longer present
	// in the app's CURRENT template (an intentional configuration change).
	// The worker socket maps it onto a stable wire code and the CLI keeps the
	// DLQ entry.
	ErrHandlerNotFound = errors.New("handler not found")
)

// Executor is the subset of the runtime Manager that invocations need. It is a
// small interface so Handle and PreparedApp construction can be exercised
// in tests without a Docker daemon; the concrete *runtime.Manager satisfies it.
type Executor interface {
	Execute(
		ctx context.Context,
		prepared *runtime.Prepared,
		handler string,
		eventJSON []byte,
		extraEnv []string,
	) error
}

// Orchestrates the flow: for each decoded event it evaluates all loaded
// apps and, for every matching rule, executes the corresponding handler in
// a container. It contains no Redis, matcher, or docker details; execution is
// delegated to the runtime executor. The app set is an atomic snapshot so
// it can be reconciled (swapped) live without disrupting in-flight invocations.
type Runner struct {
	reg     *Registry
	log     *slog.Logger
	metrics *metrics.Registry
	// refs tracks which relay images are currently executing and which have been
	// retired but cannot be removed yet. It is what lets the runner retire
	// superseded app versions without interrupting an in-flight execution
	// (see ImageInUse / RetireImage).
	refs *imageRefCounter
	// cleanerMu guards cleaner, the optional image lifecycle capability
	// discovered from a registry executor. The cache is POSITIVE-ONLY: a lookup
	// that finds no ImageCleaner entry does not cache that absence, because the
	// registry is live-swappable and a capable app can be published later. Once
	// discovered, the cleaner is retained even after the last app is removed
	// from the registry, so that removal's image cleanup can still run (the
	// reconciler removes the registry entry before calling RemoveAppImages). A
	// lookup miss can be retried after a registry change.
	cleanerMu sync.Mutex
	cleaner   ImageCleaner
	// invalidator resolves once to the executor's ContainerInvalidator
	// capability. Unlike cleaner, a missing capability is cached.
	invalidatorOnce sync.Once
	invalidator     ContainerInvalidator
	// warmAdmitter resolves to the executor's warm-budget admitter capability
	// (runtime.WarmAdmitter) once. Unlike cleaner, a missing capability is
	// cached; a nil admitter means warm-budget admission is unconditional
	// (fake/in-process executors, an unbounded runtime).
	warmAdmitterOnce sync.Once
	warmAdmitter     runtime.WarmAdmitter
	// nodeID is this worker's container-ownership identity, stamped as the
	// relay.node_id label on every execution container via RunMeta. It is the
	// same value as the Redis consumer identity. It must be set (via
	// SetNodeID) before Consume begins; when unset, an empty node label is
	// emitted, which is diagnostic-only and harmless.
	nodeID string
	// maxHandlerTimeout caps every rule's handler timeout (0 = uncapped), stored
	// as nanoseconds in an atomic.Int64. It is defense in depth: template
	// validation enforces the cap at load, and this runtime cap guarantees a
	// misconfigured or hot-swapped template can never run a handler longer than
	// the stream layer's MaxRuleTimeout. The capped value is also what TryStart
	// persists as the invocation's running deadline, so the persisted deadline
	// matches the local timer by construction.
	maxHandlerTimeout atomic.Int64
	// secrets resolves secret references to values immediately before each
	// execution. It is nil when no provider is configured (a template that
	// references secrets then fails the invocation with a clear error). It is
	// set via SetSecretProvider; the worker wires the production local provider.
	secrets secrets.Provider
	// maxConcurrentInvocations is the worker-global cap on concurrently executing
	// invocations (0 = uncapped, which SetMaxConcurrentInvocations normalizes to
	// DefaultMaxConcurrentInvocations). It is stored as an atomic so a SetMaxConcurrentInvocations
	// call (worker wires it right after construction) and concurrent Handle
	// calls read a consistent value.
	maxConcurrentInvocations atomic.Int64
	// globalSem is the global concurrency semaphore, sized to the (normalized)
	// max concurrency. It is stored as an atomic pointer so a SetMaxConcurrentInvocations
	// call (worker wires it right after construction) is race-free against
	// concurrent Handle calls reading it: readers get either the old or the new
	// semaphore, both of which are internally consistent.
	globalSem atomic.Pointer[semaphore]
	// fnSems is a mutex-protected map of per-app semaphores, keyed by
	// app name and created on demand. An app's semaphore is RESIZED by
	// replacing its pointer when the app's resolved template concurrency
	// changes (see concurrencySems), so a hot-swapped concurrency takes effect
	// without a restart. In-flight acquisitions release to the semaphore pointer
	// they captured, so replacing the map entry never strands a held slot.
	fnSemsMu sync.Mutex
	fnSems   map[string]*semaphore
	// inFlight is the current number of invocations executing concurrently in
	// this worker. It is kept in sync with the global semaphore slots and feeds
	// the in_flight_invocations gauge (set on acquire/release).
	inFlight atomic.Int64
	// slotWait is how long an invocation waits for a free concurrency slot
	// before giving up (leaving the message pending and reclaiming it later). It
	// defaults to slotWaitTimeout and is overridable by tests (package-internal
	// tests set r.slotWait directly to keep the slot-timeout tests fast).
	slotWait time.Duration
	// warmWait is how long an invocation waits for a slot in the worker-global
	// warm-container budget (MAX_WARM_CONTAINERS) before being left pending. It
	// defaults to warmAdmitWait and is overridable by tests (package-internal
	// tests set r.warmWait directly to keep the warm-admission tests fast).
	warmWait time.Duration
	// imageCleanupRetryDelays is the bounded backoff between retries of an image
	// removal that was skipped because a relay-owned container still references
	// it. It defaults to the production ~60s horizon (2+4+8+16+30s across 5
	// attempts) before the image is deferred to a later natural cleanup pass.
	// It is a field (not a package var) so package-internal tests can shrink it
	// directly to milliseconds without mutating shared state. It is read-only
	// after New/NewWithMetrics: tests must set it before use and never mutate it
	// while the runner is running.
	imageCleanupRetryDelays []time.Duration
	// imageCleanupAttemptDone, when non-nil, is called at the end of every async
	// image-cleanup attempt (the goroutine retryImageCleanupAttempt starts),
	// whether the attempt removed the image or deferred it. It is the test-only
	// synchronization seam: production leaves it nil, and tests install it to
	// wait for an attempt's terminal classification deterministically instead of
	// sleeping. Like imageCleanupRetryDelays it is read-only after
	// New/NewWithMetrics: tests must set it before the retirement that starts the
	// attempt. It runs on the cleanup goroutine and must not block.
	imageCleanupAttemptDone func(image string)
}

// New creates a Runner over the given prepared apps. Each invocation is
// bounded by the matching rule's own timeout. Metrics are nil (disabled).
func New(prepared []*PreparedApp, logger *slog.Logger) *Runner {
	return NewWithMetrics(prepared, logger, nil)
}

// NewWithMetrics is like New but wires an optional metrics registry. A nil
// registry is safe: every metric call is a no-op.
func NewWithMetrics(prepared []*PreparedApp, logger *slog.Logger, registry *metrics.Registry) *Runner {
	r := &Runner{
		reg:                     &Registry{},
		log:                     logger,
		metrics:                 registry,
		refs:                    newImageRefCounter(),
		fnSems:                  map[string]*semaphore{},
		slotWait:                slotWaitTimeout,
		warmWait:                warmAdmitWait,
		imageCleanupRetryDelays: defaultImageCleanupRetryDelays,
	}
	// maxConcurrentInvocations defaults to DefaultMaxConcurrentInvocations so an uncalled
	// SetMaxConcurrentInvocations (a runner constructed directly, as in tests) still has a
	// bounded global concurrency. SetMaxConcurrentInvocations overwrites it.
	r.maxConcurrentInvocations.Store(DefaultMaxConcurrentInvocations)
	r.globalSem.Store(newSemaphore(DefaultMaxConcurrentInvocations))
	// When a retired image's last in-flight execution releases it, run the async
	// removal automatically. r is fully built before any goroutine can run, and
	// imageRemovedIdle is nil-safe on a nil cleaner.
	r.refs.setOnIdle(r.imageRemovedIdle)
	r.reg.Set(prepared)
	return r
}

// Registry exposes the runner's mutable snapshot set so the reconciler can swap
// apps live without round-tripping through New.
func (r *Runner) Registry() *Registry { return r.reg }

// SetNodeID sets this worker's container-ownership node identity, stamped
// as the relay.node_id label on every execution container. It must be called
// before Consume begins processing; it takes effect on the next Handle, so
// setting it
// right after construction (as internal/worker does) labels every invocation.
// The constructor guarantees a non-nil Runner.
func (r *Runner) SetNodeID(nodeID string) {
	r.nodeID = nodeID
}

// SetMaxHandlerTimeout caps every rule's handler timeout to at most timeout. A value
// of 0 (the default) leaves rule timeouts uncapped. It takes effect on the next
// Handle. It is defense in depth: template validation enforces the cap at load,
// and this runtime cap guarantees a misconfigured or hot-swapped template can
// never run a handler longer than the stream layer's MaxRuleTimeout. The capped
// value is also what TryStart persists as the invocation's running deadline, so
// the persisted deadline matches the local timer by construction.
func (r *Runner) SetMaxHandlerTimeout(timeout time.Duration) {
	r.maxHandlerTimeout.Store(int64(timeout))
}

// SetSecretProvider wires the provider that resolves secret references to
// values at execution time. It takes effect on the next Handle. A nil provider
// means no secrets are available: a template that references a secret then fails
// the invocation with a clear error. The worker wires the production local
// provider after construction.
func (r *Runner) SetSecretProvider(provider secrets.Provider) {
	r.secrets = provider
}

// SetMaxConcurrentInvocations sets the worker-global cap on concurrently executing
// invocations. A value of 0 or negative (the zero value) falls back to
// DefaultMaxConcurrentInvocations (8); a value of 0 must not mean "unbounded". It takes
// effect on the next Handle. It is wired by the worker right next to
// SetNodeID/SetSecretProvider/SetMaxHandlerTimeout. It immediately stores a
// new global semaphore, so a call after construction resizes it.
func (r *Runner) SetMaxConcurrentInvocations(n int) {
	if n < 1 {
		n = DefaultMaxConcurrentInvocations
	}
	r.maxConcurrentInvocations.Store(int64(n))
	r.globalSem.Store(newSemaphore(n))
}

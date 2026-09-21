package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"relay/internal/function"
	"relay/internal/metrics"
	"relay/internal/runtime"
	"relay/internal/secrets"
	"relay/internal/stream"
)

// DefaultMaxConcurrency is the runner's default global cap on concurrently
// executing invocations in a single worker (MAX_CONCURRENCY). A zero or
// negative value passed to SetMaxConcurrency falls back to this. The runner
// deliberately owns this constant (it cannot import config; worker wires the
// cfg value).
const DefaultMaxConcurrency = 8

// slotWaitTimeout bounds how long Handle waits for a free concurrency slot
// (global or per-function) before giving up. It is deliberately well below the
// stream layer's default MinPendingIdle reclaim threshold (1m): if slots never
// free within the wait, Handle returns ErrInvocationNotEligible and the message
// stays pending, so reclaim replays it later — and locally buffered events never
// sit long enough to defeat the reclaim pacing (see the README's
// "Concurrency and backpressure" note).
const slotWaitTimeout = 30 * time.Second

// invocationOutcome classifies what one delivery round did for a single matched
// invocation. Handle's rule loop returns one of these per invocation so it can
// aggregate the per-invocation outcomes AFTER the whole loop (see Handle's doc
// comment), rather than fail-fast on the first failure. outcomeRetryable and
// outcomeExhausted are produced by recordFailure; the skip outcomes are produced
// by the TryStart/slot paths.
type invocationOutcome int

const (
	// outcomeExecuted means the handler ran and returned success (the invocation
	// was marked complete).
	outcomeExecuted invocationOutcome = iota
	// outcomeTerminalSkip means the invocation was skipped because it is already
	// terminal: complete or exhausted (never eligible again). Exhausted skips are
	// distinguishable from complete skips by the TryStart attempt number (>0).
	outcomeTerminalSkip
	// outcomePendingSkip means the invocation was skipped because it is protected
	// (running or waiting out a retry backoff) or its concurrency slot timed out:
	// it is unresolved, so the message must stay pending (never ACKed).
	outcomePendingSkip
	// outcomeRetryable means the invocation failed a retryable attempt: a retry
	// backoff was scheduled and the message must stay pending for a later retry.
	outcomeRetryable
	// outcomeExhausted means the invocation failed its last attempt: it was
	// marked exhausted (terminal) and, if every other matched invocation is also
	// terminal, the message routes to the DLQ.
	outcomeExhausted
)

// Executor is the subset of the runtime Manager that invocations need. It is a
// small interface so Handle and PreparedFunction construction can be exercised
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

// Registry holds the current set of prepared functions behind a lock so swaps
// are atomic: Handle takes one snapshot per call and keeps it for the whole
// invocation, so an in-flight execution never sees a half-replaced set. It is
// exported so the reconciler can swap functions live from its own package.
type Registry struct {
	mu  sync.RWMutex
	fns []*PreparedFunction
}

func (r *Registry) snapshot() []*PreparedFunction {
	r.mu.RLock()
	defer r.mu.RUnlock()
	// Copy the slice header only; the underlying elements are immutable once
	// published, so in-flight Handles keep using the snapshot even if Swap runs.
	return append([]*PreparedFunction(nil), r.fns...)
}

// Set replaces the entire registry contents in one atomic step. Functions are
// kept sorted by name so iteration order (and Names) is deterministic.
func (r *Registry) Set(fns []*PreparedFunction) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fns = append([]*PreparedFunction(nil), fns...)
	sortFn(r.fns)
}

// Replace swaps the entry for name, adding it if absent. A nil pf removes the
// entry (used when a function directory disappears). The slice stays name-sorted.
func (r *Registry) Replace(name string, pf *PreparedFunction) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, cur := range r.fns {
		if cur.fn.Name == name {
			if pf == nil {
				r.fns = append(r.fns[:i], r.fns[i+1:]...)
			} else {
				r.fns[i] = pf
			}
			sortFn(r.fns)
			return
		}
	}
	if pf != nil {
		r.fns = append(r.fns, pf)
		sortFn(r.fns)
	}
}

// sortFn orders the registry by function name so Names() and iteration are
// deterministic regardless of the order functions were discovered or swapped in.
func sortFn(fns []*PreparedFunction) {
	sort.Slice(fns, func(i, j int) bool { return fns[i].fn.Name < fns[j].fn.Name })
}

// GetByName returns the prepared function for the given name, or nil if absent,
// without disturbing the running snapshot.
func (r *Registry) GetByName(name string) *PreparedFunction {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, pf := range r.fns {
		if pf.fn.Name == name {
			return pf
		}
	}
	return nil
}

// Names returns a snapshot of the currently registered function names, so the
// reconciler can enumerate what is active without pinning the whole slice.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.fns))
	for _, pf := range r.fns {
		out = append(out, pf.fn.Name)
	}
	return out
}

// Orchestrates the flow: for each decoded event it evaluates all loaded
// functions and, for every matching rule, executes the corresponding handler in
// a container. It contains no Redis, matcher, or docker details; execution is
// delegated to the runtime executor. The function set is an atomic snapshot so
// it can be reconciled (swapped) live without disrupting in-flight invocations.
type Runner struct {
	reg     *Registry
	log     *slog.Logger
	metrics *metrics.Registry
	// refs tracks which relay images are currently executing and which have been
	// retired but cannot be removed yet. It is what lets the runner retire
	// superseded function versions without interrupting an in-flight execution
	// (see ImageInUse / RetireImage).
	refs *imageRefCounter
	// cleaner resolves to the optional image lifecycle capability of the
	// executor, resolved once and reused. A nil cleaner (fake executors in tests)
	// makes every retirement a no-op.
	cleanerOnce sync.Once
	cleaner     ImageCleaner
	// invalidator resolves to the executor's ContainerInvalidator capability
	// the same way cleaner above is resolved (one pass over the registry).
	invalidatorOnce sync.Once
	invalidator     ContainerInvalidator
	// hostname is this worker's container-ownership identity, stamped as the
	// relay.hostname label on every execution container via RunMeta. It is the
	// same value as the Redis consumer identity. It must be set (via
	// SetHostname) before Consume begins; when unset, an empty hostname label is
	// emitted, which is diagnostic-only and harmless.
	hostname string
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
	// maxConcurrency is the worker-global cap on concurrently executing
	// invocations (0 = uncapped, which SetMaxConcurrency normalizes to
	// DefaultMaxConcurrency). It is stored as an atomic so a SetMaxConcurrency
	// call (worker wires it right after construction) and concurrent Handle
	// calls read a consistent value.
	maxConcurrency atomic.Int64
	// globalSem is the global concurrency semaphore, sized to the (normalized)
	// max concurrency. It is stored as an atomic pointer so a SetMaxConcurrency
	// call (worker wires it right after construction) is race-free against
	// concurrent Handle calls reading it: readers get either the old or the new
	// semaphore, both of which are internally consistent.
	globalSem atomic.Pointer[semaphore]
	// fnSems is a mutex-protected map of per-function semaphores, keyed by
	// function name and created on demand. A function's semaphore is RESIZED by
	// replacing its pointer when the function's resolved template concurrency
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
	// imageCleanupRetryDelays is the bounded backoff between retries of an image
	// removal that was skipped because a relay-owned container still references
	// it. It defaults to the production ~60s horizon (2+4+8+16+30s across 5
	// attempts) before the image is deferred to a later natural cleanup pass.
	// It is a field (not a package var) so package-internal tests can shrink it
	// directly to milliseconds without mutating shared state. It is read-only
	// after New/NewWithMetrics: tests must set it before use and never mutate it
	// while the runner is running.
	imageCleanupRetryDelays []time.Duration
}

// defaultImageCleanupRetryDelays is the production backoff schedule for image
// removal retries: a ~60s horizon across 5 attempts before deferring to a later
// natural cleanup pass.
var defaultImageCleanupRetryDelays = []time.Duration{
	2 * time.Second,
	4 * time.Second,
	8 * time.Second,
	16 * time.Second,
	30 * time.Second,
}

// ImageCleaner is the subset of the runtime Manager that image retirement
// needs. It is a small interface so the runner can retire superseded function
// images without depending on the runtime package concretely; test fakes that
// do not implement it simply yield a nil cleaner (no retirement).
// ContainerInvalidator is the optional capability of the runtime executor
// that image retirement needs on top of ImageCleaner: invalidate pooled
// execution containers for a retired image so the image's
// container-reference guard clears promptly. Implementations must never
// block on an in-flight invocation: Manager.InvalidateImage discards idle
// containers immediately and retires busy ones (discarded on release) without
// waiting for them.
type ContainerInvalidator interface {
	// InvalidateImage discards any cached execution container running the
	// given image.
	InvalidateImage(image string)
}

type ImageCleaner interface {
	// RemoveImage removes a single relay-owned image, treating an already-gone
	// image as success.
	RemoveImage(ctx context.Context, image string) error
	// ImageReferencedByManagedContainer reports whether any relay-owned container
	// (event, schedule, or service) still references the image via its
	// relay.image label. It lets the runner confirm no relay-owned container still
	// references an image before removing it, and to decide (conservatively) how
	// to interpret a RemoveImage failure.
	ImageReferencedByManagedContainer(ctx context.Context, image string) (bool, error)
	// FunctionImageTags lists every local image tag (full references) belonging
	// to a function's repository, so the runner can retire each version with
	// in-flight safety.
	FunctionImageTags(ctx context.Context, name string) ([]string, error)
	// CleanupUnusedDependencies removes managed dependency images no managed
	// function image references anymore. Lifecycle-driven: call it after a
	// managed function image was successfully removed, so a dependency layer
	// whose last referencing function version just disappeared is pruned. It is
	// best-effort and must not affect the outcome of the removal that preceded
	// it.
	CleanupUnusedDependencies(ctx context.Context) (int, error)
}

// Pairs a loaded function with its prepared image and the executor used to run
// invocations. A function whose image could not be built is marked unavailable
// and skipped during execution.
type PreparedFunction struct {
	fn        function.Function
	prepared  *runtime.Prepared
	executor  Executor
	available bool
}

func (p *PreparedFunction) Name() string {
	return p.fn.Name
}

func (p *PreparedFunction) Function() function.Function {
	return p.fn
}

// Prepared returns the built image handle, or nil for an unavailable function.
func (p *PreparedFunction) Prepared() *runtime.Prepared {
	return p.prepared
}

func NewPrepared(
	fn function.Function,
	prepared *runtime.Prepared,
	executor Executor,
) *PreparedFunction {
	return &PreparedFunction{
		fn:        fn,
		prepared:  prepared,
		executor:  executor,
		available: true,
	}
}

// NewUnavailable wraps a function whose image could not be built so the runner
// can skip it without losing the function's identity.
func NewUnavailable(fn function.Function) *PreparedFunction {
	return &PreparedFunction{fn: fn, available: false}
}

// New creates a Runner over the given prepared functions. Each invocation is
// bounded by the matching rule's own timeout. Metrics are nil (disabled).
func New(prepared []*PreparedFunction, logger *slog.Logger) *Runner {
	return NewWithMetrics(prepared, logger, nil)
}

// NewWithMetrics is like New but wires an optional metrics registry. A nil
// registry is safe: every metric call is a no-op.
func NewWithMetrics(prepared []*PreparedFunction, logger *slog.Logger, m *metrics.Registry) *Runner {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	r := &Runner{
		reg:                     &Registry{},
		log:                     logger,
		metrics:                 m,
		refs:                    newImageRefCounter(),
		fnSems:                  map[string]*semaphore{},
		slotWait:                slotWaitTimeout,
		imageCleanupRetryDelays: defaultImageCleanupRetryDelays,
	}
	// maxConcurrency defaults to DefaultMaxConcurrency so an uncalled
	// SetMaxConcurrency (a runner constructed directly, as in tests) still has a
	// bounded global concurrency. SetMaxConcurrency overwrites it.
	r.maxConcurrency.Store(DefaultMaxConcurrency)
	r.globalSem.Store(newSemaphore(DefaultMaxConcurrency))
	// When a retired image's last in-flight execution releases it, run the async
	// removal automatically. r is fully built before any goroutine can run, and
	// imageRemovedIdle is nil-safe on a nil cleaner.
	r.refs.setOnIdle(r.imageRemovedIdle)
	r.reg.Set(prepared)
	return r
}

// imageRemovedIdle is the onIdle hook: a retired image just became idle, so
// remove it off the event path.
func (r *Runner) imageRemovedIdle(image string) {
	r.removeImageAsync(image)
}

// Registry exposes the runner's mutable snapshot set so the reconciler can swap
// functions live without round-tripping through New.
func (r *Runner) Registry() *Registry { return r.reg }

// SetHostname sets this worker's container-ownership hostname, stamped as the
// relay.hostname label on every execution container. It is nil-safe (a nil
// Runner is a no-op) and must be called before Consume begins processing; it
// takes effect on the next Handle, so setting it right after construction (as
// internal/worker does) labels every invocation.
func (r *Runner) SetHostname(h string) {
	if r == nil {
		return
	}
	r.hostname = h
}

// SetMaxHandlerTimeout caps every rule's handler timeout to at most d. A value
// of 0 (the default) leaves rule timeouts uncapped. It is nil-safe (a nil
// Runner is a no-op) and takes effect on the next Handle. It is defense in
// depth: template validation enforces the cap at load, and this runtime cap
// guarantees a misconfigured or hot-swapped template can never run a handler
// longer than the stream layer's MaxRuleTimeout. The capped value is also what
// TryStart persists as the invocation's running deadline, so the persisted
// deadline matches the local timer by construction.
func (r *Runner) SetMaxHandlerTimeout(d time.Duration) {
	if r == nil {
		return
	}
	r.maxHandlerTimeout.Store(int64(d))
}

// SetSecretProvider wires the provider that resolves secret references to
// values at execution time. It is nil-safe (a nil Runner is a no-op) and takes
// effect on the next Handle. A nil provider means no secrets are available: a
// template that references a secret then fails the invocation with a clear
// error. The worker wires the production local provider after construction.
func (r *Runner) SetSecretProvider(p secrets.Provider) {
	if r == nil {
		return
	}
	r.secrets = p
}

// SetMaxConcurrency sets the worker-global cap on concurrently executing
// invocations. A value of 0 or negative (the zero value) falls back to
// DefaultMaxConcurrency (8); a value of 0 must not mean "unbounded". It is
// nil-safe (a nil Runner is a no-op) and takes effect on the next Handle. It
// is wired by the worker right next to SetHostname/SetSecretProvider/
// SetMaxHandlerTimeout. The global semaphore is (re)built on the next acquisition,
// so a call after construction resizes it.
func (r *Runner) SetMaxConcurrency(n int) {
	if r == nil {
		return
	}
	if n < 1 {
		n = DefaultMaxConcurrency
	}
	r.maxConcurrency.Store(int64(n))
	r.globalSem.Store(newSemaphore(n))
}

// resolver returns the runner's resolved image cleaner, or nil when the executor
// does not implement retirement (tests, unavailable-only runners). It is resolved
// once and cached; resolution scanning the registry is cheap and safe.
func (r *Runner) resolver() ImageCleaner {
	r.cleanerOnce.Do(func() {
		for _, pf := range r.reg.snapshot() {
			if c, ok := pf.executor.(ImageCleaner); ok {
				r.cleaner = c
				return
			}
		}
	})
	return r.cleaner
}

// invalidatorResolver returns the runner's resolved container invalidator, or
// nil when the executor does not implement retirement (tests, unavailable-only
// runners). Resolution scans the registry snapshot once and caches.
func (r *Runner) invalidatorResolver() ContainerInvalidator {
	r.invalidatorOnce.Do(func() {
		for _, pf := range r.reg.snapshot() {
			if c, ok := pf.executor.(ContainerInvalidator); ok {
				r.invalidator = c
				return
			}
		}
	})
	return r.invalidator
}

// ImageInUse reports whether any execution is currently holding a reference to
// the given image (in-flight Handle). It is the guard the reconciler consults
// before removing a superseded image, and Release uses it to know when a retired
// image becomes removable.
func (r *Runner) ImageInUse(image string) bool {
	return r.refs.inUse(image)
}

// RetireImage retires the given image reference so it can be removed once it is
// no longer in use. When nothing holds it now, it is removed immediately off the
// event path; otherwise it is marked pending and removed once the last in-flight
// execution releases it (see imageRefCounter.release → imageRemovedIdle). A nil
// cleaner (no ImageCleaner executor) makes this a no-op, which is correct for
// test fakes and unavailable-only runners.
func (r *Runner) RetireImage(image string) {
	if image == "" {
		return
	}
	if !r.refs.recordRetired(image) {
		// Already retired (first retirement owns removal); nothing to do.
		return
	}
	// Invalidate pooled execution containers running this image BEFORE any
	// removal attempt: the discard clears the image's relay-owned-container
	// reference promptly, so ErrImageInUse / the reference guard does not wait
	// for a retry pass. InvalidateImage is strictly non-blocking manager-side
	// (idle containers are discarded now, busy ones retired until release), so
	// retirement never stalls on an in-flight invocation.
	if inv := r.invalidatorResolver(); inv != nil {
		inv.InvalidateImage(image)
	}
	if !r.ImageInUse(image) {
		// Idle right now: remove immediately instead of waiting for a release
		// that will only ever fire if a new execution picks this image up.
		r.removeImageAsync(image)
	}
}

// RemoveFunctionImages retires every local version of a function's images so
// that, once idle, each is removed. It is the function-removal path: the
// reconciler calls it when a function directory vanishes, and all of its version
// images become garbage. A nil cleaner makes this a no-op.
func (r *Runner) RemoveFunctionImages(name string) {
	cleaner := r.resolver()
	if cleaner == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	tags, err := cleaner.FunctionImageTags(ctx, name)
	if err != nil {
		r.log.Warn("Image cleanup: list function versions failed", "function", name, "error", err)
		return
	}
	for _, tag := range tags {
		r.RetireImage(tag)
	}
}

// removeImageAsync removes a retired image off the event path so a docker round
// trip can never add latency (or failure) to Handle. Removal is gated by two
// independent guards: the in-flight refcount (an execution may have (re)claimed
// the image after it was retired) and the relay-owned container reference check
// (a persistent service container may still reference it). When either guard
// holds, removal is skipped and retried with a bounded backoff
// (r.imageCleanupRetryDelays); when the attempts exhaust, the image is deferred
// to a later natural cleanup pass rather than force-removed. A nil cleaner is a
// no-op.
func (r *Runner) removeImageAsync(image string) {
	cleaner := r.resolver()
	if cleaner == nil {
		return
	}
	// Snapshot the retry backoff once so a goroutine that outlives a test
	// (which may set r.imageCleanupRetryDelays) never races a later field write.
	// attempt is the retry counter shared across a single guard-skip chain; a
	// later retire of the same image that reaches removal does not cancel it —
	// the guards make duplicate attempts benign, so a bounded per-image cleanup
	// is safe and keeps goroutine count bounded.
	delays := append([]time.Duration(nil), r.imageCleanupRetryDelays...)
	attempt := 0
	r.retryImageCleanupAttempt(image, cleaner, delays, &attempt)
}

// retryImageCleanupAttempt performs one removal attempt off the event path. It
// guards the removal with the in-flight refcount and the relay-owned container
// reference check; a guard-skip schedules a bounded retry (sharing the attempt
// counter) using the snapshot backoff, and exhausting the retries defers the
// image to a later natural cleanup pass.
func (r *Runner) retryImageCleanupAttempt(image string, cleaner ImageCleaner, delays []time.Duration, attempt *int) {
	go func() {
		// A retirement that is superseded by a new execution must not remove an
		// image a container is about to start; skip removal if it became in-use.
		// The in-flight execution's release path re-owns the removal when it
		// goes idle, so we do not burn a retry attempt here.
		if r.ImageInUse(image) {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		referenced, err := cleaner.ImageReferencedByManagedContainer(ctx, image)
		if err != nil {
			// Never remove on unknown state: treat the reference check failure as
			// conservatively referenced and retry.
			r.skipAndRetryImageCleanup(image, delays, "reference check failed", attempt)
			return
		}
		if referenced {
			r.skipAndRetryImageCleanup(image, delays, "relay-owned container references it", attempt)
			return
		}
		if err := cleaner.RemoveImage(ctx, image); err != nil {
			// A removal that fails while a container references the image is a
			// guard-skip (the daemon refused, or the reference appeared mid-call),
			// not a genuine failure. Re-consult the reference to classify the
			// error: referenced -> debug skip + retry; otherwise the genuine Warn.
			if again, aerr := cleaner.ImageReferencedByManagedContainer(ctx, image); aerr == nil && again {
				r.skipAndRetryImageCleanup(image, delays, "relay-owned container references it", attempt)
				return
			}
			r.log.Warn("Image cleanup: remove retired failed", "image", image, "error", err)
			return
		}

		// The function image was successfully removed. This is the lifecycle
		// moment a dependency layer may become orphaned: the removed function
		// image was the only reference to its dependency, so run dependency GC
		// now to prune any layer no managed function image references anymore.
		// It is best-effort: a failure here (a genuine daemon error) is worth a
		// Warn — it retries on the next natural pass after the next function-image
		// removal or at the next startup — and must NOT affect the outcome of the
		// removal that already succeeded. The function image's own removal (and
		// this GC) both proceed off the event path in this same goroutine, and the
		// reconciler pump is serial with this retire hook, so this cannot race a
		// build that FROM the dependency.
		if _, err := cleaner.CleanupUnusedDependencies(ctx); err != nil {
			r.log.Warn("Dependency image cleanup failed", "error", err)
		}
	}()
}

// skipAndRetryImageCleanup logs a debug-level skip and schedules the next
// removal attempt with bounded backoff (delays is the per-chain snapshot), or —
// on the final attempt — logs a single Info deferring the image to a later natural
// cleanup pass (boot sweep, the next rebuild's retire, or function-removal
// retirement). It is context-free and off the event path.
func (r *Runner) skipAndRetryImageCleanup(image string, delays []time.Duration, reason string, attempt *int) {
	r.log.Debug("Image cleanup: image still in use; skipping", "image", image, "reason", reason)
	*attempt++
	if *attempt > len(delays) {
		r.log.Info("Image cleanup: image still in use after retries; deferring to a later cleanup pass", "image", image)
		return
	}
	delay := delays[*attempt-1]
	time.AfterFunc(delay, func() {
		r.retryImageCleanupAttempt(image, r.resolver(), delays, attempt)
	})
}

// toImage returns the image a prepared function executes, or "" when it is
// unavailable/nil so refcount and retirement stay nil-safe for fake and
// unavailable paths.
func toImage(pf *PreparedFunction) string {
	if pf == nil || pf.prepared == nil {
		return ""
	}
	return pf.prepared.Image
}

// executeWithRefs runs one rule's handler while holding a reference to the
// function's image for the duration of the invocation, so a concurrent
// RetireImage cannot remove the image an in-flight execution still needs
// (at-least-once safety). The release is deferred so it runs even if the
// executor panics; the helper is called per rule so the defer scope is
// per-invocation rather than accumulating across a long rule loop. extraEnv are
// the per-invocation env vars (template env values + resolved secrets).
func (r *Runner) executeWithRefs(
	pf *PreparedFunction,
	invokeCtx context.Context,
	handler string,
	eventJSON []byte,
	extraEnv []string,
) error {
	image := toImage(pf)
	r.refs.acquire(image)
	defer r.refs.release(image)
	return pf.executor.Execute(invokeCtx, pf.prepared, handler, eventJSON, extraEnv)
}

// semaphore is a channel-based counting semaphore that bounds how many
// invocations may execute concurrently (globally or per function). acquireReserve
// blocks up to slotWaitTimeout for a free slot, returning false on timeout (the
// invocation is left pending and reclaimed later). A per-function semaphore is
// REPLACED (not mutated) when the function's resolved concurrency changes, so an
// in-flight acquisition still releases to the semaphore pointer it captured
// while new acquisitions use the resized one; the global semaphore is rebuilt by
// SetMaxConcurrency.
//
// The per-function semaphore is the ONLY per-function invocation limiter: the
// runtime's warm container pool is sized from the same effective concurrency
// (template concurrency clipped to MAX_CONCURRENCY), so the semaphore always
// admits no more concurrent Execute calls than the pool has containers, and the
// pool never blocks in the runner path. The global semaphore is the broader cap
// shared across functions.
type semaphore struct {
	slots chan struct{}
	// capacity is the limit the channel was created with. It is immutable and
	// lets concurrencySems detect a reconciled concurrency change by comparing
	// the desired value against the installed semaphore, so the map entry is
	// replaced only on a real change (never resized per call).
	capacity int
}

func newSemaphore(n int) *semaphore {
	return &semaphore{slots: make(chan struct{}, n), capacity: n}
}

// acquire acquires one slot, blocking up to wait until a slot frees, ctx is
// done, or wait elapses. On success it returns (true, waited false/true) and the
// caller MUST release (via release) exactly once. On timeout/cancel it returns
// (false, ...) and no slot is held. waited reports whether the call had to
// block at all (a slot was not immediately available): it feeds the
// concurrency_waits_total counter.
func (s *semaphore) acquire(ctx context.Context, wait time.Duration) (got bool, waited bool) {
	select {
	case s.slots <- struct{}{}:
		return true, false
	default:
		// Not immediately free: fall through to the blocking wait.
	}
	waited = true
	select {
	case s.slots <- struct{}{}:
		return true, true
	case <-time.After(wait):
		return false, true
	case <-ctx.Done():
		return false, true
	}
}

func (s *semaphore) release() {
	<-s.slots
}

// concurrencySems returns the global and per-function semaphores for the given
// function. The function's semaphore is created on demand and RESIZED in place
// (by replacing the map entry with a freshly sized semaphore) whenever the
// function's resolved concurrency differs from the installed one. Replacement
// rather than channel mutation is deliberate: an in-flight acquisition holds
// the OLD pointer and releases to it, so shrinking can never block a release on
// a full new channel nor lose a slot, and no held slot is ever stranded. The
// global semaphore is non-nil on the runner (normalized on construction).
//
// The resized-to concurrency is resolved from the CURRENT registry entry, not
// only the caller's snapshot value (see currentConcurrency): an in-flight Handle
// holding an older snapshot cannot resize the semaphore backwards after a newer
// snapshot already applied a larger bound. The resolved value is additionally
// clipped to the worker-global MAX_CONCURRENCY (see effectiveConcurrency), so a
// function asking for more than the global cap never gets a per-function
// semaphore larger than the global one — and the runtime's warm-pool bound,
// also clipped to the cap, agrees with it.
func (r *Runner) concurrencySems(fnName string, fnConcurrency int) (global *semaphore, fn *semaphore) {
	// Global semaphore: normalized on construction / SetMaxConcurrency; it is
	// always non-nil in practice. Guard nil defensively (a zero-valued Runner
	// in tests would read nil).
	global = r.globalSem.Load()
	if global == nil {
		global = newSemaphore(DefaultMaxConcurrency)
	}
	fnConcurrency = r.effectiveConcurrency(fnName, fnConcurrency)
	r.fnSemsMu.Lock()
	defer r.fnSemsMu.Unlock()
	if s, ok := r.fnSems[fnName]; ok {
		if s.capacity == fnConcurrency {
			return global, s
		}
		// Resolved concurrency changed: install a freshly sized semaphore so new
		// acquisitions use the new bound. In-flight holds keep the old pointer
		// and release to it (see the semaphore doc).
		s = newSemaphore(fnConcurrency)
		r.fnSems[fnName] = s
		return global, s
	}
	s := newSemaphore(fnConcurrency)
	r.fnSems[fnName] = s
	return global, s
}

// currentConcurrency returns fnName's current resolved per-function concurrency
// from the registry, falling back to fallback when the function is absent or has
// no parsed template. It makes a semaphore resize authoritative to the latest
// published template rather than a caller's possibly-stale registry snapshot.
func (r *Runner) currentConcurrency(fnName string, fallback int) int {
	if r.reg == nil {
		return fallback
	}
	if pf := r.reg.GetByName(fnName); pf != nil && pf.fn.Template != nil {
		return pf.fn.Template.Concurrency
	}
	return fallback
}

// effectiveConcurrency returns the per-function semaphore capacity for fnName:
// its current resolved template concurrency clipped to the worker-global
// MAX_CONCURRENCY. Clipping matters when the template asks for more than the
// global cap (e.g. concurrency 15 with MAX_CONCURRENCY=8): the per-function
// semaphore is then sized to the cap, matching the runtime's effective warm-pool
// bound, so the pool and the semaphore never disagree. A zero/negative template
// value falls back to function.DefaultConcurrency; a zero maxConcurrency (a
// zero-valued Runner in tests) falls back to DefaultMaxConcurrency, never
// "uncapped". The global value is read fresh, so a SetMaxConcurrency call is
// reflected on the next acquisition (which resizes the function's semaphore).
func (r *Runner) effectiveConcurrency(fnName string, fallback int) int {
	n := r.currentConcurrency(fnName, fallback)
	if n < 1 {
		n = function.DefaultConcurrency
	}
	limit := int(r.maxConcurrency.Load())
	if limit < 1 {
		limit = DefaultMaxConcurrency
	}
	if n > limit {
		return limit
	}
	return n
}

// RemoveFunctionSemaphore drops a function's per-function semaphore. It is
// called when the function is removed from the registry, so a later recreation
// of the same name starts from a fresh semaphore rather than inheriting a stale
// bound (and so a removed function's map entry does not linger). In-flight
// acquisitions still release to the pointer they captured. It is nil-safe.
func (r *Runner) RemoveFunctionSemaphore(fnName string) {
	if r == nil {
		return
	}
	r.fnSemsMu.Lock()
	delete(r.fnSems, fnName)
	r.fnSemsMu.Unlock()
}

// reserveSlots acquires both the global and per-function slots for one
// invocation, counting a concurrency_waits_total whenever either slot was not
// immediately free (the acquisition had to block). It returns a release func on
// success (call it after the invocation, releasing both slots and the in-flight
// gauge) and whether the acquisition had to block at all (for a debug log); on
// timeout it returns (nil, ...) and does NOT hold any slot. It must be called
// BEFORE TryStart so a blocked invocation is never counted as an attempt and does
// not persist state.
func (r *Runner) reserveSlots(ctx context.Context, fnName string, fnConcurrency int) (release func(), waited bool) {
	global, fn := r.concurrencySems(fnName, fnConcurrency)

	// Acquire the global slot first (the broader bound). A blocked acquire
	// counts a wait, whether it eventually succeeds or not.
	got, w := global.acquire(ctx, r.slotWait)
	if w {
		r.metrics.Inc(metrics.MetricConcurrencyWaits)
	}
	waited = waited || w
	if !got {
		return nil, waited
	}

	// Then the per-function slot. A blocked acquire also counts a wait. If the
	// per-function slot never frees, release the global slot so it does not
	// leak to another function's wait.
	got, w = fn.acquire(ctx, r.slotWait)
	if w {
		r.metrics.Inc(metrics.MetricConcurrencyWaits)
	}
	waited = waited || w
	if !got {
		global.release()
		return nil, waited
	}

	// Both slots held and the invocation is about to execute: publish the
	// in-flight gauge for it.
	r.inFlight.Add(1)
	r.metrics.SetGauge(metrics.MetricInFlightInvocations, float64(r.inFlight.Load()))

	released := false
	release = func() {
		if released {
			return
		}
		released = true
		fn.release()
		global.release()
		// Publish the in-flight gauge AFTER dropping below the cap.
		r.inFlight.Add(-1)
		r.metrics.SetGauge(metrics.MetricInFlightInvocations, float64(r.inFlight.Load()))
	}
	return release, waited
}

// runInvocation runs one rule's handler while holding a reference to the
// function's image for the duration of the invocation, converting an
// executor/runtime panic into a failed-attempt error instead of letting it
// escape Handle and kill the worker. This is the single panic boundary for
// message processing: panics inside executor/runtime code are a misbehaving
// execution (isolated, retried via the normal pending/reclaim flow), while
// panics elsewhere (startup, reconciler, Redis client) stay fatal and visible.
//
// The invocation context's cancel is deferred here so it always runs, even when
// the executor panics: without this, a panic that unwinds past the call site
// would skip the explicit cancel() and leak the context's timer until it fired
// on its own. executeWithRefs's own defers (image refcount release) run during
// panic unwinding BEFORE the recover here, so the image reference is never
// leaked; the returned panic value is logged by the caller.
func (r *Runner) runInvocation(
	pf *PreparedFunction,
	invokeCtx context.Context,
	cancel context.CancelFunc,
	handler string,
	eventJSON []byte,
	extraEnv []string,
) (panicked bool, panicValue any, err error) {
	defer cancel()
	defer func() {
		if pv := recover(); pv != nil {
			panicked = true
			panicValue = pv
			err = fmt.Errorf("executor panic: %v", pv)
		}
	}()
	return false, nil, r.executeWithRefs(pf, invokeCtx, handler, eventJSON, extraEnv)
}

// Handle evaluates the event against all loaded functions and executes every
// matching rule's handler. It returns nil only when every invocation succeeded
// (or nothing matched); otherwise it returns an error so the stream layer does
// not acknowledge the message.
//
// At-least-once semantics: Handle returns an error after partial successes, so
// a retried message re-runs the handlers that already succeeded. events_received
// and events_processed count each delivery attempt that reaches the handler
// handoff, so retries increment them too — they are delivery-attempt counters,
// not unique-event counters. Handle does NOT claim exactly-once: redeliveries
// re-run whatever is not yet recorded as complete, and the handlers must stay
// idempotent.
//
// Aggregate, per-invocation semantics: a single Redis message can match multiple
// "<function>/<handler>" invocations, and each is tracked independently in the
// per-message invocation-state hash. Every eligible matching invocation gets its
// own attempt on each delivery regardless of the others' outcomes: a failure in one
// handler does NOT prevent later matching handlers from running. The rule loop is
// strictly sequential (never parallel), and after iterating every matching rule
// Handle aggregates the per-invocation outcomes into the message-level return
// contract below.
//
// Invocation state: when the stream layer injects an InvocationState into ctx
// (see stream.WithInvocationState), Handle skips any matching invocation whose
// "<function>/<handler>" ID is already recorded as completed on a previous
// delivery, is protected by an active attempt deadline or a retry backoff (a
// running or next_attempt_at marker whose persisted deadline has not yet passed
// — this or another replica may be executing it, or it is waiting out its
// backoff), or is exhausted (terminal). Skipped invocations are not executions:
// they do not touch the handler_* or function_handler_* metrics.
// function_events_total still counts the function as engaged (it matched), which
// is attribution, not execution counting. When no invocation state is present
// (direct Handle callers/tests, or invocation tracking disabled) Handle behaves
// exactly as before: it runs every matching handler and returns the first
// failure's plain error immediately, with no invocation wrapping or DLQ
// attribution.
//
// Return contract (with invocation state, aggregated after the full rule loop):
//   - a plain (retryable) error when any matched invocation had a retryable
//     failure this delivery — regardless of other invocations' outcomes — so
//     the message stays pending and is retried. The first such error is returned.
//   - a wrapped stream.ErrInvocationExhausted when every invocation in the
//     matched set is terminal (complete or exhausted), at least one of them is
//     exhausted, and no retryable failure occurred this delivery. The whole
//     message is terminal, so the stream layer routes it to the DLQ.
//   - a wrapped stream.ErrInvocationNotEligible when no retryable failure
//     occurred and the message is not all-terminal, but at least one matched
//     invocation was skipped because it is protected (running or waiting out a
//     retry backoff) or its concurrency slot timed out. This fires even when
//     other invocations executed successfully in this same call: a protected
//     or slot-timeout invocation is unresolved, so the message must stay pending
//     and NOT be ACKed (this is what fixes the cross-replica ACK hazard — a
//     replica that reclaims a message whose invocation is still in flight on
//     another replica must not ACK it).
//   - nil when every matched invocation is complete (or nothing matched), so the
//     stream layer ACKs the message.
//
// Panic boundary: each invocation's execution runs inside runInvocation, which
// recovers an executor/runtime panic and converts it into a normal failed
// attempt (see runInvocation). A panicking execution is therefore isolated and
// retried via the same failure/backoff/exhaustion machinery as any other
// failure — it never escapes Handle to kill the worker. The image refcount is
// still released (executeWithRefs's defer runs during unwinding) and the
// invocation's context cancel is deferred so no timer leaks. Panics elsewhere
// (startup, reconciler, Redis client) are not recovered here and stay fatal.
func (r *Runner) Handle(ctx context.Context, msgID string, event map[string]any) error {
	// A message received at the runner is one logical event handled across all
	// matching rules. This is the message-level counter.
	r.metrics.Inc(metrics.MetricEventsReceived)
	// Best-effort delivery attempt, defaulting to 1 when the stream did not set
	// it (e.g. when the runner is driven directly in tests). It is the delivery
	// attempt used for logging; without invocation state the handler attempt is
	// unknown, so the delivery attempt is logged instead.
	deliveryAttempt := stream.DeliveryAttemptFrom(ctx)
	// Best-effort invocation state, absent when the stream did not inject it
	// (direct Handle callers/tests, or invocation tracking disabled).
	invState, hasState := stream.InvocationStateFrom(ctx)

	// Trackers for the message-level return contract, aggregated only after the
	// whole rule loop has run (see the Handle doc comment). skippedPending records
	// whether any matched invocation was skipped because it is protected (running
	// or waiting out a retry backoff) or its concurrency slot timed out — an
	// unresolved invocation that must keep the message pending even when other
	// invocations succeeded this call. firstErr holds the first plain retryable
	// failure; exhaustedErr holds the plain error of an exhaustion this delivery so
	// the aggregate can wrap ErrInvocationExhausted; anyExhausted records whether
	// any matched invocation exhausted this delivery. These are only meaningful
	// when hasState is true.
	skippedPending := false
	var firstErr, exhaustedErr error
	anyExhausted := false
	// executed tracks whether any invocation actually executed, for the
	// no-state (direct caller/test) path, which preserves the old fail-fast
	// tail exactly. With invocation state it is unused (the aggregate uses the
	// per-invocation outcomes instead).
	executed := false

	// Take one consistent snapshot for the whole call so a concurrent registry
	// swap mid-execution cannot reorder or drop functions under us.
	snapshot := r.reg.snapshot()

	// Pre-pass: collect every matched invocation ID so an exhausted attempt can
	// decide whether the whole message is terminal (all matched invocations
	// complete or exhausted). This must be complete before any execution, because
	// a rule that exhausts early must still see the full set of matched
	// invocations (including ones that sort later).
	var matched []string
	for _, pf := range snapshot {
		if !pf.available {
			continue
		}
		for _, rule := range pf.fn.Template.MatchingEventRules(event) {
			matched = append(matched, pf.fn.Name+"/"+rule.Handler)
		}
	}

	for _, pf := range snapshot {
		if !pf.available {
			continue
		}
		rules := pf.fn.Template.MatchingEventRules(event)
		// A function is "involved" in an event when at least one of its rules
		// matches, regardless of whether the execution later fails. This is the
		// functions-engaged counter: an event matching two functions counts once
		// per function here, while the message-level events_processed_total
		// (stream) and events_received_total (above) count it once globally.
		if len(rules) > 0 {
			r.metrics.IncLabels(metrics.MetricFunctionEvents,
				[]metrics.Label{{Name: "function", Value: pf.fn.Name}})
		}
		for _, rule := range rules {
			// The invocation identity is stable across restarts and config
			// reloads as long as the rule still exists: the function name and the
			// rule handler string. Renaming either invalidates old invocation
			// state — old entries simply never match, and the msg-level set of
			// required invocations is recomputed each delivery from current
			// templates, so a rule removed from the template no longer gates the
			// ACK.
			invocation := pf.fn.Name + "/" + rule.Handler
			if hasState && invState.IsComplete(invocation) {
				r.log.Debug("Function handler: already succeeded for event; skipping",
					"function", pf.fn.Name,
					"handler", rule.Handler,
					"message_id", msgID,
					"delivery_attempt", deliveryAttempt,
				)
				continue
			}
			// Cap the rule timeout at the configured maximum (defense in depth;
			// template validation enforces the cap at load). This guarantees a
			// handler can never run longer than the stream layer's MaxRuleTimeout,
			// and the same capped value is what TryStart persists as the running
			// deadline, so the persisted deadline matches the local timer by
			// construction.
			timeout := rule.Timeout
			if cap := time.Duration(r.maxHandlerTimeout.Load()); cap > 0 && timeout > cap {
				timeout = cap
			}
			// executeRule runs this single rule's invocation while holding the
			// worker-global and per-function concurrency slots (whose defer scope
			// is THIS call, so a multi-rule message never accumulates slots across
			// rules). It classifies this invocation's outcome for this delivery and
			// returns it plus the plain error (for a failed attempt) so the outer
			// loop can aggregate AFTER iterating every matching rule — a failure in
			// one handler never prevents later handlers from running.
			executeRule := func() (invocationOutcome, error) {
				// The per-invocation handler attempt number. With invocation state
				// it comes from TryStart (Redis-backed, incremented per actual
				// execution); without it there is no persisted handler attempt, so
				// the delivery count is used as the handler_attempt fallback.
				handlerAttempt := int(deliveryAttempt)
				// Reserve the worker-global and per-function concurrency slots
				// BEFORE TryStart, so a blocked invocation is never counted as an
				// attempt and does not persist state. If no slot frees within
				// slotWait (well below MinPendingIdle, so a locally buffered event
				// never defeats reclaim), the invocation is treated as not
				// eligible: it stays pending and is replayed by a later reclaim,
				// with no retry accounting and no DLQ. The slots are released via
				// defer as soon as THIS invocation finishes.
				releaseSlots, waited := r.reserveSlots(ctx, pf.fn.Name, pf.fn.Template.Concurrency)
				if releaseSlots == nil {
					// No slot freed in time: skip this invocation without marking
					// it failed, leave the message pending (reclaim replays it
					// later).
					skippedPending = true
					r.log.Debug("Function handler: concurrency slot wait timed out; leaving pending",
						"function", pf.fn.Name,
						"handler", rule.Handler,
						"message_id", msgID,
						"delivery_attempt", int(deliveryAttempt),
					)
					return outcomePendingSkip, nil
				}
				if waited {
					r.log.Debug("Function handler: waiting for concurrency slot",
						"function", pf.fn.Name,
						"handler", rule.Handler,
						"message_id", msgID,
						"delivery_attempt", int(deliveryAttempt),
					)
				}
				defer releaseSlots()
				// Claim the invocation for this execution before running it.
				// TryStart persists an absolute running deadline (now + timeout)
				// and returns started=false when the invocation is already
				// complete (handled above), exhausted, or protected by an active
				// attempt deadline or a retry backoff — this or another replica
				// may be executing it, or it is waiting out its backoff, so we
				// must not run it concurrently. The IsComplete check above is the
				// fast path that avoids an HSET on completed invocations;
				// TryStart's own HGET also reads "ok" and covers the same case,
				// so the two are consistent.
				if hasState {
					started, n, wait := invState.TryStart(invocation, timeout)
					if !started {
						// The slot is released by the deferred releaseSlots before
						// the next rule acquires.
						if wait > 0 {
							// Protected by an active running deadline or a retry
							// backoff. The message must stay pending (the protected
							// invocation may still complete or fail on its own), so
							// this is a "not eligible" skip, not a completion.
							skippedPending = true
							r.log.Debug("Function handler: not eligible for event (running or waiting for retry); leaving pending",
								"function", pf.fn.Name,
								"handler", rule.Handler,
								"message_id", msgID,
								"handler_attempt", n,
								"next_attempt_in", wait,
							)
							return outcomePendingSkip, nil
						}
						// Terminal (complete or exhausted): skipped like complete,
						// never eligible again. An exhausted skip (n > 0) is
						// indistinguishable here from the already-complete case,
						// which is fine: exhaustion accounting is per-invocation and
						// the message-level DLQ decision happens in the aggregate.
						r.log.Debug("Function handler: terminal for event; skipping",
							"function", pf.fn.Name,
							"handler", rule.Handler,
							"message_id", msgID,
							"handler_attempt", n,
						)
						return outcomeTerminalSkip, nil
					}
					handlerAttempt = n
				}
				// The invocation attempt has actually begun: the TryStart above
				// (or the absence of invocation state, for direct callers)
				// claimed it and the slots are held. This is the
				// last_execution_at attribution point — every claimed attempt
				// counts, retries included (they are real executions), while
				// the skip branches returned above never reach it.
				r.metrics.SetFunctionTimestamp(pf.fn.Name, metrics.FunctionTimestampExecution, time.Now().Unix())
				r.log.Debug("Function rule: matched event",
					"function", pf.fn.Name,
					"handler", rule.Handler,
					"message_id", msgID,
					"handler_attempt", handlerAttempt,
				)
				eventJSON, err := json.Marshal(event)
				if err != nil {
					// The invocation was already claimed (deferred-released) but
					// will not execute: this is a failed attempt, so treat it as
					// such — schedule a retry (or exhaust), then CONTINUE to the
					// next matching rule so each independent invocation gets its own
					// failed attempt.
					if hasState {
						return r.recordFailure(invState, invocation, handlerAttempt, rule.Retries, pf.fn.Name, rule.Handler, msgID, err)
					}
					return outcomeRetryable, fmt.Errorf("function %q handler %q: marshal event: %w", pf.fn.Name, rule.Handler, err)
				}
				// Resolve the template's env values and secret references into the
				// per-invocation extra env, immediately before container creation.
				// Secret values are resolved per execution (never cached on
				// Prepared, never in the fingerprint), so rotating a secret value
				// never requires a rebuild. A resolution failure is a failed
				// attempt (the invocation was already claimed by TryStart), so it
				// flows through the same retry/exhaustion machinery as an
				// execution failure and, like marshal failures, does not
				// short-circuit the remaining rules. The error names the secret
				// REFERENCE only — never any value.
				extraEnv, err := r.resolveExtraEnv(ctx, pf.fn.Template)
				if err != nil {
					if hasState {
						return r.recordFailure(invState, invocation, handlerAttempt, rule.Retries, pf.fn.Name, rule.Handler, msgID, err)
					}
					return outcomeRetryable, fmt.Errorf("function %q handler %q: %w", pf.fn.Name, rule.Handler, err)
				}
				invokeCtx, cancel := context.WithTimeout(ctx, timeout)
				// Stamp the invocation's diagnostic metadata into the context so
				// the executor can attach it as container labels. This keeps the
				// Executor interface (and every test fake) unchanged.
				eventID, eventName := eventFields(event)
				invokeCtx = runtime.WithRunMeta(invokeCtx, runtime.RunMeta{
					Type:      runtime.ContainerTypeEvent,
					Function:  pf.fn.Name,
					Handler:   rule.Handler,
					MessageID: msgID,
					EventID:   eventID,
					EventName: eventName,
					Hostname:  r.hostname,
					Image:     toImage(pf),
				})
				start := time.Now()
				panicked, panicValue, err := r.runInvocation(pf, invokeCtx, cancel, rule.Handler, eventJSON, extraEnv)
				d := time.Since(start)
				if panicked {
					// A panicking execution is a misbehaving handler, not a
					// healthy failure: log the panic value and the full stack so
					// the bug is visible and attributable, then funnel it through
					// the SAME failure branch below (metrics + recordFailure) so
					// retry and exhaustion accounting stay per-invocation.
					r.log.Error("Function handler: PANICKED for event",
						"function", pf.fn.Name,
						"handler", rule.Handler,
						"message_id", msgID,
						"handler_attempt", handlerAttempt,
						"panic_value", fmt.Sprintf("%v", panicValue),
						"stack", string(debug.Stack()),
					)
				}
				if err != nil {
					r.metrics.IncLabels(metrics.MetricHandlerInvocations,
						[]metrics.Label{
							{Name: "outcome", Value: "failure"},
							{Name: "function", Value: pf.fn.Name},
							{Name: "handler", Value: rule.Handler},
						})
					// Unlabeled total for the SQLite snapshot; the labeled counter
					// above stays for Prometheus.
					r.metrics.Inc(metrics.MetricHandlerFailure)
					// Per-function failure attribution (per rule execution). A
					// failed attempt that will retry still counts as a failure
					// (and as an execution above); only a DLQ-routed exhaustion
					// additionally sets last_dlq_at (see recordFailure).
					r.metrics.IncLabels(metrics.MetricFunctionHandlerFailure,
						[]metrics.Label{{Name: "function", Value: pf.fn.Name}})
					r.metrics.SetFunctionTimestamp(pf.fn.Name, metrics.FunctionTimestampFailure, time.Now().Unix())
					r.metrics.ObserveDurationLabels(metrics.MetricHandlerDuration,
						[]metrics.Label{
							{Name: "function", Value: pf.fn.Name},
							{Name: "handler", Value: rule.Handler},
						}, d)
					r.log.Warn("Function handler: execution failed for event",
						"function", pf.fn.Name,
						"handler", rule.Handler,
						"message_id", msgID,
						"handler_attempt", handlerAttempt,
						"duration", d,
						"reason", err,
					)
					// Record the failure and decide retry vs exhaustion. This is
					// the per-invocation retry driver: a retryable failure
					// schedules a backoff and counts function_retries_total; an
					// exhausted attempt marks the invocation terminal and counts
					// function_dlq_total. The message-level DLQ decision (whether
					// EVERY matched invocation is terminal) is left to Handle's
					// end-of-loop aggregate, so an invocation may exhaust while
					// others still run.
					if hasState {
						return r.recordFailure(invState, invocation, handlerAttempt, rule.Retries, pf.fn.Name, rule.Handler, msgID, err)
					}
					// No invocation state (direct callers/tests): every failure
					// counts as a retry driver, but there is no Redis-backed
					// attempt count to decide exhaustion — so no DLQ attribution
					// here either. The DLQ metric is only meaningful with
					// invocation state, where exhaustion is actually persisted
					// and observable.
					r.metrics.IncLabels(metrics.MetricFunctionRetries,
						[]metrics.Label{{Name: "function", Value: pf.fn.Name}})
					return outcomeRetryable, err
				}
				// Record the invocation as completed so a redelivery skips it.
				// This happens BEFORE the success metrics so a crash between the
				// side effect and MarkComplete re-runs the handler (at-least-once;
				// the handler must remain idempotent). A mark failure is logged by
				// the handle and does not fail the invocation.
				if hasState {
					invState.MarkComplete(invocation)
				}
				r.metrics.IncLabels(metrics.MetricHandlerInvocations,
					[]metrics.Label{
						{Name: "outcome", Value: "success"},
						{Name: "function", Value: pf.fn.Name},
						{Name: "handler", Value: rule.Handler},
					})
				// Unlabeled total for the SQLite snapshot; the labeled counter
				// above stays for Prometheus.
				r.metrics.Inc(metrics.MetricHandlerSuccess)
				// Per-function success attribution (per rule execution).
				r.metrics.IncLabels(metrics.MetricFunctionHandlerSuccess,
					[]metrics.Label{{Name: "function", Value: pf.fn.Name}})
				r.metrics.SetFunctionTimestamp(pf.fn.Name, metrics.FunctionTimestampSuccess, time.Now().Unix())
				r.metrics.ObserveDurationLabels(metrics.MetricHandlerDuration,
					[]metrics.Label{
						{Name: "function", Value: pf.fn.Name},
						{Name: "handler", Value: rule.Handler},
					}, d)
				r.log.Info("Function handler: executed for event",
					"function", pf.fn.Name,
					"handler", rule.Handler,
					"message_id", msgID,
					"handler_attempt", handlerAttempt,
					"duration", d,
				)
				return outcomeExecuted, nil
			}
			outcome, err := executeRule()
			if !hasState {
				// No invocation state (direct callers/tests): preserve the old
				// fail-fast behavior exactly — run every matching handler and
				// return the first failure's plain error immediately; the tail
				// handles the nothing-executed/not-eligible case. No invocation
				// wrapping or DLQ attribution when matched.
				if outcome == outcomeRetryable {
					return err
				}
				if outcome == outcomeExecuted {
					executed = true
				}
				continue
			}
			// Aggregate the per-invocation outcome for the message-level contract,
			// decided only after the whole loop (see Handle's doc comment). Keep
			// iterating regardless: each matching invocation gets its own attempt.
			switch outcome {
			case outcomeRetryable:
				if firstErr == nil {
					firstErr = err
				}
			case outcomeExhausted:
				anyExhausted = true
				if exhaustedErr == nil {
					exhaustedErr = err
				}
			}
		}
	}
	// Aggregate: decide the single message-level error from the per-invocation
	// outcomes collected across the whole rule loop.
	if hasState {
		// 1. Any retryable failure this delivery → the message stays pending
		//    (retryable): return the first such failure, even if other
		//    invocations succeeded or are otherwise still running.
		if firstErr != nil {
			return firstErr
		}
		// 2. Every matched invocation is terminal (complete or exhausted) AND at
		//    least one exhausted → the message is terminal; route it to the DLQ.
		//    allMatchedTerminal fails open to false on a read error, keeping the
		//    message pending rather than DLQ'ing it.
		if anyExhausted && allMatchedTerminal(invState, matched) {
			return fmt.Errorf("%w: %w", stream.ErrInvocationExhausted, exhaustedErr)
		}
		// 3. Any matched invocation was protected- or slot-timeout-skipped
		//    (unresolved) → the message stays pending with NO retry accounting.
		//    This fires even when other invocations executed successfully this
		//    call: an unresolved invocation must not be ACKed away.
		if skippedPending {
			return stream.ErrInvocationNotEligible
		}
		// 4. Every matched invocation is complete (or nothing matched) → ACK.
		return nil
	}
	// No invocation state: preserve the old fail-fast tail — nil when something
	// executed successfully (or nothing matched); ErrInvocationNotEligible when
	// nothing executed and at least one invocation was protected- or
	// slot-timeout-skipped (so the stream leaves the message pending).
	if executed {
		return nil
	}
	if skippedPending {
		return stream.ErrInvocationNotEligible
	}
	return nil
}

// obsoleteOccurrence is the terminal error returned when a schedule occurrence
// names a function or handler that is no longer in the configuration. The stream
// layer recognizes ErrInvocationObsolete and ACKs the message: an obsolete
// occurrence is never retried or dead-lettered.
func obsoleteOccurrence(fnName, handler string) error {
	return fmt.Errorf(
		"%w: schedule function %q handler %q no longer in configuration",
		stream.ErrInvocationObsolete, fnName, handler,
	)
}

// InvokeHandler executes a single schedule-occurrence invocation routed through
// the stream. The handler's timeout is read from the function's current
// template (single source of truth), capped at the configured maximum exactly
// like Handle caps rule timeouts, and passed BOTH to TryStart and to
// context.WithTimeout so the persisted running deadline matches the local kill
// timer. It reuses the exact event execution path: registry snapshot lookup,
// the global + per-function concurrency slots, per-invocation secret
// resolution, the panic boundary, and the same handler metrics.
//
// InvokeHandler participates in the SAME per-invocation invocation-state
// lifecycle as Handle: when the stream injects an InvocationState into ctx
// (via stream.WithInvocationState), it reserves concurrency slots before
// TryStart (a slot timeout is never an attempt and persists no state), claims
// the "<function>/<handler>" invocation with TryStart using the capped timeout,
// skips already-complete redeliveries (stream ACKs), leaves pending under a
// protected running/backoff marker, marks the invocation complete on success
// (before the success metrics), and on failure drives recordFailure for the
// retry-backoff/exhaustion decision from the template's schedule Retries. The
// stream layer (via ConsumerConfig.ScheduleRunner) drives retry, backoff,
// invocation state, and DLQ around this single invocation.
func (r *Runner) InvokeHandler(ctx context.Context, msgID, fnName, handler string, payload []byte) error {
	// Invocation state (when present) distinguishes the production stream path
	// from direct callers/tests: obsolete-removal is only treated as terminal
	// on the production path. See the availability checks below.
	invState, hasState := stream.InvocationStateFrom(ctx)

	// Find the function in the current registry. GetByName returns nil only when
	// the function is ABSENT (removed) — a present but unavailable function
	// returns a non-nil entry whose Prepared() is nil. These two cases must be
	// handled differently:
	//   - absent (removed): an intentional configuration change, so an occurrence
	//     for it is OBSOLETE and terminal (ACKed, never retried/DLQ'd) — but only
	//     on the production state-carrying path; direct callers/tests keep the
	//     legacy plain error.
	//   - present but unavailable: temporary (build failed at startup/reconcile),
	//     retryable exactly as today.
	pf := r.reg.GetByName(fnName)
	if pf == nil {
		if hasState {
			r.log.Warn("Schedule: occurrence obsolete; function removed; acknowledging",
				"function", fnName,
				"handler", handler,
			)
			return obsoleteOccurrence(fnName, handler)
		}
		r.log.Warn("Schedule: function is not available", "function", fnName)
		return fmt.Errorf("schedule invocation: function %q is not available", fnName)
	}
	if pf.Prepared() == nil {
		// The function exists but its image could not be built yet: temporarily
		// unavailable, so the occurrence is retryable (not obsolete — the function
		// is still configured).
		r.log.Warn("Schedule: function is temporarily unavailable", "function", fnName)
		return fmt.Errorf("schedule invocation: function %q is not available", fnName)
	}

	// Resolve the handler's timeout AND retry count from the function's CURRENT
	// template — the single source of truth, so a hot-swapped template's new
	// values apply to future occurrences automatically. The template's FIRST
	// matching schedule entry provides both; multiple entries sharing a handler
	// behave identically (occurrence identity distinguishes them by scheduled_at).
	// Fall back to the event defaults when the handler has no schedule entry,
	// exactly as Handle falls back to the rule defaults. On the production
	// (state-carrying) path, a missing schedule entry means the handler was
	// removed from the template while the occurrence was pending → obsolete.
	timeout := function.DefaultTimeout
	retries := function.DefaultRetries
	found := false
	for _, sch := range pf.fn.Template.Schedules {
		if sch.Handler == handler {
			timeout = sch.Timeout
			retries = sch.Retries
			found = true
			break
		}
	}
	if hasState && !found {
		r.log.Warn("Schedule: occurrence obsolete; schedule handler no longer in template; acknowledging",
			"function", fnName,
			"handler", handler,
		)
		return obsoleteOccurrence(fnName, handler)
	}

	// Cap the schedule timeout at the configured maximum, exactly like Handle
	// caps each rule's timeout (defense in depth; template validation enforces
	// it at load).
	if cap := time.Duration(r.maxHandlerTimeout.Load()); cap > 0 && timeout > cap {
		timeout = cap
	}

	invocation := fnName + "/" + handler

	// Reserve the worker-global and per-function concurrency slots BEFORE
	// TryStart so a blocked invocation is never counted as an attempt and does
	// not persist state (same ordering as Handle's event path). A slot timeout
	// means the invocation is unresolved: with invocation state it returns a
	// "not eligible" skip (the stream leaves the message pending with no retry
	// accounting); without state it preserves the legacy plain error.
	releaseSlots, _ := r.reserveSlots(ctx, fnName, pf.fn.Template.Concurrency)
	if releaseSlots == nil {
		r.log.Warn("Schedule: concurrency slot wait timed out", "function", fnName, "handler", handler)
		if hasState {
			return stream.ErrInvocationNotEligible
		}
		return fmt.Errorf("schedule invocation: concurrency slot wait timed out")
	}
	defer releaseSlots()

	if hasState {
		// Fast path: an already-completed ("ok") invocation on redelivery means a
		// previous delivery succeeded but the ACK failed (or is racing). The
		// message should be ACKed, not re-run and not DLQ'd — return nil so the
		// stream ACKs and then clears state.
		if invState.IsComplete(invocation) {
			r.log.Debug("Schedule: invocation already succeeded; skipping (stream ACKs)",
				"function", fnName,
				"handler", handler,
			)
			return nil
		}
		// Claim the invocation for this execution before running it. TryStart
		// persists an absolute running deadline (now + the capped timeout) and
		// returns started=false when the invocation is already exhausted or
		// protected by an active attempt deadline or a retry backoff (this or
		// another replica may be executing it, or it is waiting out its backoff).
		started, handlerAttempt, wait := invState.TryStart(invocation, timeout)
		if !started {
			if wait > 0 {
				// Protected by an active running deadline or a retry backoff. The
				// message must stay pending: the protected invocation may still
				// complete or fail on its own, so this is a "not eligible" skip,
				// never an ACK.
				r.log.Debug("Schedule: invocation not eligible (running or waiting for retry); leaving pending",
					"function", fnName,
					"handler", handler,
					"handler_attempt", handlerAttempt,
					"next_attempt_in", wait,
				)
				return stream.ErrInvocationNotEligible
			}
			// Terminal skip: the invocation is already exhausted. A schedule has
			// exactly ONE invocation, so a terminal skip means the message is
			// terminal and must route to the DLQ.
			r.log.Debug("Schedule: invocation terminal (exhausted); routing to DLQ",
				"function", fnName,
				"handler", handler,
				"handler_attempt", handlerAttempt,
			)
			return fmt.Errorf("%w: schedule function %q handler %q is exhausted", stream.ErrInvocationExhausted, fnName, handler)
		}
		err := r.invokeOnce(ctx, pf, handler, payload, timeout, invState, invocation, msgID)
		if err != nil {
			// A failed attempt — resolve extra env, marshal, execution, or
			// timeout failures all land here. recordFailure decides retry vs
			// exhaustion using the template's schedule Retries: a retryable
			// failure schedules a backoff and returns a plain error (the stream
			// leaves the message pending, gated by next_attempt_at); an exhausted
			// attempt marks the invocation terminal and, because a schedule has
			// exactly ONE invocation (this one), the message is terminal — wrap
			// stream.ErrInvocationExhausted so the stream routes it to the DLQ.
			outcome, retErr := r.recordFailure(invState, invocation, handlerAttempt, retries, fnName, handler, msgID, err)
			if outcome == outcomeExhausted {
				return fmt.Errorf("%w: %w", stream.ErrInvocationExhausted, retErr)
			}
			return retErr
		}
		return nil
	}

	// No invocation state (direct callers/tests): preserve the legacy behavior
	// exactly — execute the single handler and return the plain error (or nil on
	// success). MarkComplete/success metrics still emit inside invokeOnce.
	return r.invokeOnce(ctx, pf, handler, payload, timeout, nil, "", msgID)
}

// invokeOnce is the smallest reusable single-handler execution core shared by
// InvokeHandler (with or without invocation state): it resolves the per-invocation
// template env/secrets, runs the handler inside the panic boundary and the
// caller-provided capped timeout, and emits the handler success/failure metrics.
// It assumes the caller has already reserved the concurrency slots; on success,
// when invState is non-nil it marks the invocation complete BEFORE the success
// metrics so a crash between the side effect and MarkComplete re-runs the
// handler (at-least-once; the handler must remain idempotent), mirroring
// Handle's per-rule ordering. It returns the plain execution error (nil on
// success) that the caller routes through the invocation-state lifecycle.
func (r *Runner) invokeOnce(
	ctx context.Context,
	pf *PreparedFunction,
	handler string,
	payload []byte,
	timeout time.Duration,
	invState stream.InvocationState,
	invocation string,
	msgID string,
) error {
	// The invocation attempt has actually begun: the caller claimed it via
	// TryStart (or is a direct no-state caller) and holds the concurrency
	// slots. This is the schedule path's last_execution_at attribution point —
	// every claimed attempt counts, retries included.
	r.metrics.SetFunctionTimestamp(pf.fn.Name, metrics.FunctionTimestampExecution, time.Now().Unix())

	// Resolve the template's env values and secret references immediately before
	// container creation, mirroring Handle's rule path.
	extraEnv, err := r.resolveExtraEnv(ctx, pf.fn.Template)
	if err != nil {
		// Counted as a handler failure with a zero duration (no execution
		// happened), mirroring how Handle attributes a resolution failure.
		r.recordHandlerFailure(pf.fn.Name, handler, 0)
		return fmt.Errorf("schedule invocation: function %q handler %q: %w", pf.fn.Name, handler, err)
	}

	r.log.Debug("Schedule: invoking handler",
		"function", pf.fn.Name,
		"handler", handler,
	)

	invokeCtx, cancel := context.WithTimeout(ctx, timeout)
	// Stamp the invocation's diagnostic metadata. MessageID is the schedule
	// occurrence's real Redis stream message ID, stamped on relay.message_id;
	// EventID/EventName are empty. Type: containerTypeSchedule is the strict
	// relay.type marker that classifies this one-shot invocation container.
	invokeCtx = runtime.WithRunMeta(invokeCtx, runtime.RunMeta{
		Type:      runtime.ContainerTypeSchedule,
		Function:  pf.fn.Name,
		Handler:   handler,
		MessageID: msgID,
		Hostname:  r.hostname,
		Image:     toImage(pf),
	})
	start := time.Now()
	panicked, panicValue, err := r.runInvocation(pf, invokeCtx, cancel, handler, payload, extraEnv)
	d := time.Since(start)
	if panicked {
		r.log.Error("Function handler: PANICKED for schedule",
			"function", pf.fn.Name,
			"handler", handler,
			"panic_value", fmt.Sprintf("%v", panicValue),
			"stack", string(debug.Stack()),
		)
		r.recordHandlerFailure(pf.fn.Name, handler, d)
		return err
	}
	if err != nil {
		r.recordHandlerFailure(pf.fn.Name, handler, d)
		r.log.Warn("Function handler: execution failed for schedule",
			"function", pf.fn.Name,
			"handler", handler,
			"duration", d,
			"reason", err,
		)
		return err
	}
	// Mark the invocation complete on success BEFORE the success metrics so a
	// crash between the side effect and MarkComplete re-runs the handler
	// (at-least-once; the handler must remain idempotent). A mark failure is
	// logged by the handle and does not fail the invocation.
	if invState != nil {
		invState.MarkComplete(invocation)
	}
	r.metrics.IncLabels(metrics.MetricHandlerInvocations,
		[]metrics.Label{
			{Name: "outcome", Value: "success"},
			{Name: "function", Value: pf.fn.Name},
			{Name: "handler", Value: handler},
		})
	// Unlabeled total for the SQLite snapshot; the labeled counter above stays
	// for Prometheus.
	r.metrics.Inc(metrics.MetricHandlerSuccess)
	// Per-function success attribution.
	r.metrics.IncLabels(metrics.MetricFunctionHandlerSuccess,
		[]metrics.Label{{Name: "function", Value: pf.fn.Name}})
	r.metrics.SetFunctionTimestamp(pf.fn.Name, metrics.FunctionTimestampSuccess, time.Now().Unix())
	r.metrics.ObserveDurationLabels(metrics.MetricHandlerDuration,
		[]metrics.Label{
			{Name: "function", Value: pf.fn.Name},
			{Name: "handler", Value: handler},
		}, d)
	r.log.Info("Function handler: executed for schedule",
		"function", pf.fn.Name,
		"handler", handler,
		"duration", d,
	)
	return nil
}

// recordHandlerFailure increments the failure metrics shared by Handle's
// failure branch and InvokeHandler: the labeled invocation outcome counter, the
// unlabeled total, per-function failure attribution, and the duration
// histogram. A failed attempt that will retry still counts as a failure here
// (it sets last_failure_at); only the DLQ-routed exhaustion additionally sets
// last_dlq_at (see recordFailure). It deliberately does NOT touch
// events_received/processed or function_events_total — the stream layer counts
// events_processed_total for a schedule delivery (see stream.processScheduleMessage),
// so those are not double-counted here.
func (r *Runner) recordHandlerFailure(fnName, handler string, d time.Duration) {
	r.metrics.IncLabels(metrics.MetricHandlerInvocations,
		[]metrics.Label{
			{Name: "outcome", Value: "failure"},
			{Name: "function", Value: fnName},
			{Name: "handler", Value: handler},
		})
	r.metrics.Inc(metrics.MetricHandlerFailure)
	r.metrics.IncLabels(metrics.MetricFunctionHandlerFailure,
		[]metrics.Label{{Name: "function", Value: fnName}})
	r.metrics.SetFunctionTimestamp(fnName, metrics.FunctionTimestampFailure, time.Now().Unix())
	r.metrics.ObserveDurationLabels(metrics.MetricHandlerDuration,
		[]metrics.Label{
			{Name: "function", Value: fnName},
			{Name: "handler", Value: handler},
		}, d)
}

// recordFailure handles a failed invocation attempt: it decides whether the
// handler attempt is retryable or exhausted, updates the invocation state and
// metrics accordingly, and returns the outcome plus the plain error Handle
// should aggregate. It is used for execution, marshal, secret-resolution, and
// timeout failures (all are failed attempts).
//
// A retryable handler attempt (handlerAttempt < 1+retries) schedules a retry
// backoff via RecordFailure, counts function_retries_total, and returns
// (outcomeRetryable, err). An exhausted handler attempt (handlerAttempt >=
// 1+retries) marks the invocation terminal via MarkExhausted, counts
// function_dlq_total, and returns (outcomeExhausted, err). The message-level
// DLQ decision (whether EVERY matched invocation is terminal) is NOT made
// here; it is deferred to Handle's end-of-loop aggregation, so an invocation
// can exhaust while others still run without short-circuiting them.
//
// The returned error is always the plain, unwrapped failure so Handle only has
// to wrap stream.ErrInvocationExhausted once, at the aggregate, if the message
// is terminal.
func (r *Runner) recordFailure(
	invState stream.InvocationState,
	invocation string,
	handlerAttempt int,
	retries int,
	fnName, handler, msgID string,
	origErr error,
) (invocationOutcome, error) {
	maxAttempts := 1 + retries
	if handlerAttempt >= maxAttempts {
		// Exhausted: mark the invocation terminal. This is the DLQ attribution
		// point: the invocation exhausted its retries and the message is being
		// routed to the DLQ, so last_dlq_at is stamped HERE — not on every
		// failure, and not again on the later terminal-skip redeliveries of the
		// same invocation.
		invState.MarkExhausted(invocation, handlerAttempt)
		r.metrics.IncLabels(metrics.MetricFunctionDLQ,
			[]metrics.Label{{Name: "function", Value: fnName}})
		r.metrics.SetFunctionTimestamp(fnName, metrics.FunctionTimestampDLQ, time.Now().Unix())
		r.log.Error("Function handler: exhausted; invocation terminal",
			"function", fnName,
			"handler", handler,
			"message_id", msgID,
			"handler_attempt", handlerAttempt,
			"handler_attempts_total", maxAttempts,
		)
		return outcomeExhausted, fmt.Errorf(
			"function %q handler %q exhausted after %d handler attempts: %w",
			fnName, handler, handlerAttempt, origErr,
		)
	}
	// Retryable: schedule a retry backoff and count the retry.
	backoff := retryBackoff(handlerAttempt)
	invState.RecordFailure(invocation, backoff)
	r.metrics.IncLabels(metrics.MetricFunctionRetries,
		[]metrics.Label{{Name: "function", Value: fnName}})
	r.log.Warn("Function handler: failed attempt; retrying later",
		"function", fnName,
		"handler", handler,
		"message_id", msgID,
		"handler_attempt", handlerAttempt,
		"handler_attempts_total", maxAttempts,
		"retry_backoff", backoff,
		"reason", origErr,
	)
	return outcomeRetryable, fmt.Errorf(
		"function %q handler %q: handler attempt %d failed: %w",
		fnName, handler, handlerAttempt, origErr,
	)
}

// allMatchedTerminal reports whether every matched invocation is terminal
// (complete or exhausted). It is used to decide whether a message whose last
// failing invocation just exhausted has any runnable invocation left: if none,
// the message is terminal and must be routed to the DLQ. A read error fails
// open to false (not terminal), so the message is conservatively left pending.
func allMatchedTerminal(invState stream.InvocationState, matched []string) bool {
	for _, inv := range matched {
		if !invState.IsTerminal(inv) {
			return false
		}
	}
	return true
}

// retryBackoff returns the retry delay to apply after a failed attempt with the
// given completed attempt number. The schedule is fixed and non-configurable:
// attempt 1 → 1m, 2 → 2m, 3 → 5m, 4+ → 10m (capped). The runner owns rule
// semantics, so the schedule lives here, not in the stream store.
func retryBackoff(completedAttempts int) time.Duration {
	switch {
	case completedAttempts <= 1:
		return time.Minute
	case completedAttempts == 2:
		return 2 * time.Minute
	case completedAttempts == 3:
		return 5 * time.Minute
	default:
		return 10 * time.Minute
	}
}

// eventFields extracts the low-cardinality, label-safe event_id and event_name
// from the event for the invocation's runtime RunMeta (container labels and the
// stream output prefix). These are never used as metric labels or structured
// log fields.
func eventFields(event map[string]any) (eventID, eventName string) {
	if v, ok := event["event_id"]; ok {
		eventID = stringify(v)
	}
	if v, ok := event["event_name"]; ok {
		eventName = stringify(v)
	}
	return eventID, eventName
}

func stringify(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// resolveExtraEnv builds the per-invocation extra env for a template: the
// literal env values followed by the resolved secret values, both name-ordered.
// Secret values are resolved here, immediately before execution, so they are
// never cached on Prepared or baked into the fingerprint. A template that
// references a secret but has no provider configured fails with a clear error
// naming the reference. The returned slice is the ONLY place a resolved secret
// value lives before it is handed to the executor (and from there to the
// container's Config.Env); it is never logged or persisted.
func (r *Runner) resolveExtraEnv(ctx context.Context, tmpl *function.Template) ([]string, error) {
	var extra []string
	for _, ev := range tmpl.EnvList() {
		extra = append(extra, ev.Name+"="+ev.Value)
	}
	for _, sb := range tmpl.SecretList() {
		if r.secrets == nil {
			return nil, fmt.Errorf("function references secret %q but no secret provider is configured", sb.Ref)
		}
		val, err := r.secrets.Resolve(ctx, sb.Ref.String())
		if err != nil {
			// The provider's error carries the reference name only, never a value.
			return nil, fmt.Errorf("resolve secret %q: %w", sb.Ref, err)
		}
		extra = append(extra, sb.Name+"="+val)
	}
	return extra, nil
}

// cleanupTimeout bounds every docker image-removal call made off the event path
// (see removeImageAsync), so a wedged daemon cannot hold a goroutine or block
// shutdown indefinitely. It is deliberately short: retirement is best-effort
// cleanup, never on the critical path.
const cleanupTimeout = 10 * time.Second

// imageRefCounter tracks how many in-flight executions hold each image and which
// images have been retired (superseded) but can only be removed once idle.
//
// acquire/release bracket a single invocation: the count for prepared.Image is
// bumped on entry and dropped on return, so ImageInUse reports live executions
// even while a swap concurrently replaces the registry entry. RetireImage marks
// an image retired; when its count reaches zero, release triggers its
// asynchronous removal via the runner (which owns the cleaner). A retired image
// that is never in flight (count already zero at retire time) is removed
// immediately by RetireImage instead.
type imageRefCounter struct {
	mu      sync.Mutex
	uses    map[string]int64
	retired map[string]bool
	onIdle  func(image string)
}

// newImageRefCounter builds an empty counter whose onIdle hook fires removal.
func newImageRefCounter() *imageRefCounter {
	return &imageRefCounter{
		uses:    map[string]int64{},
		retired: map[string]bool{},
	}
}

func (c *imageRefCounter) setOnIdle(fn func(string)) {
	c.mu.Lock()
	c.onIdle = fn
	c.mu.Unlock()
}

func (c *imageRefCounter) acquire(image string) {
	if image == "" {
		return
	}
	c.mu.Lock()
	c.uses[image]++
	// A retired image was idle when its removal fired but is being (re)claimed by
	// a new execution; drop the retirement mark so the pending removal aborts.
	// The image cannot vanish mid-invocation: retire is guarded by in-use, and
	// removing is guarded by in-use again under the same lock discipline.
	delete(c.retired, image)
	c.mu.Unlock()
}

// release drops a reference and, when a retired image's count just reached zero,
// fires onIdle so the runner can remove it. Retired-but-idle images are removed
// exactly once, and only after the last in-flight execution has finished.
func (c *imageRefCounter) release(image string) {
	if image == "" {
		return
	}
	c.mu.Lock()
	left := c.uses[image] - 1
	if left <= 0 {
		delete(c.uses, image)
		left = 0
	}
	// If the image was marked retired and is now idle, ownership of its removal
	// has transferred to this release: clear the mark (so no other release fires
	// a duplicate) and report the idle transition.
	becameIdle := left == 0 && c.retired[image]
	if becameIdle {
		delete(c.retired, image)
	}
	onIdle := c.onIdle
	c.mu.Unlock()
	if becameIdle && onIdle != nil {
		onIdle(image)
	}
}

// inUse reports whether an execution currently holds the image.
func (c *imageRefCounter) inUse(image string) bool {
	if image == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.uses[image] > 0
}

// recordRetired marks image as retired (superseded). It returns true when the
// image was not already retired, so the caller knows whether this retirement
// owns removal.
func (c *imageRefCounter) recordRetired(image string) bool {
	if image == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.retired[image] {
		return false
	}
	c.retired[image] = true
	return true
}

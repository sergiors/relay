package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"relay/internal/app"
	eventmatch "relay/internal/event"
	"relay/internal/observability/metrics"
	"relay/internal/runtime"
	"relay/internal/schedule"
	"relay/internal/secrets"
	"relay/internal/stream"
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
	// distinguishable from complete skips by the existing marker's attempt
	// count (>0).
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
	// cleaner resolves to the optional image lifecycle capability of the
	// executor, resolved once and reused. A nil cleaner (fake executors in tests)
	// makes every retirement a no-op.
	cleanerOnce sync.Once
	cleaner     ImageCleaner
	// invalidator resolves to the executor's ContainerInvalidator capability
	// the same way cleaner above is resolved (one pass over the registry).
	invalidatorOnce sync.Once
	invalidator     ContainerInvalidator
	// warmAdmitter resolves to the executor's warm-budget admitter capability
	// (runtime.WarmAdmitter) the same way cleaner above is resolved (one pass
	// over the registry); a nil admitter means warm-budget admission is
	// unconditional (fake/in-process executors, an unbounded runtime).
	warmAdmitterOnce sync.Once
	warmAdmitter     runtime.WarmAdmitter
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

// SetHostname sets this worker's container-ownership hostname, stamped as the
// relay.hostname label on every execution container. It must be called before
// Consume begins processing; it takes effect on the next Handle, so setting it
// right after construction (as internal/worker does) labels every invocation.
// The constructor guarantees a non-nil Runner.
func (r *Runner) SetHostname(hostname string) {
	r.hostname = hostname
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
// SetHostname/SetSecretProvider/SetMaxHandlerTimeout. It immediately stores a
// new global semaphore, so a call after construction resizes it.
func (r *Runner) SetMaxConcurrentInvocations(n int) {
	if n < 1 {
		n = DefaultMaxConcurrentInvocations
	}
	r.maxConcurrentInvocations.Store(int64(n))
	r.globalSem.Store(newSemaphore(n))
}

// warmAdmitterResolver returns the runner's resolved warm-budget admitter, or
// nil when the executor does not implement it (fake executors in tests, or an
// unbounded runtime). Resolution scans the registry snapshot once and caches,
// mirroring invalidatorResolver.
func (r *Runner) warmAdmitterResolver() runtime.WarmAdmitter {
	r.warmAdmitterOnce.Do(func() {
		for _, pf := range r.reg.snapshot() {
			if adm, ok := pf.executor.(runtime.WarmAdmitter); ok {
				r.warmAdmitter = adm
				return
			}
		}
	})
	return r.warmAdmitter
}

// warmSaturated reports whether err is the runtime's create-time warm-budget
// saturation sentinel (a narrow race between admission and container create). It
// is BACKPRESSURE, not a handler failure: the caller must leave the work pending
// without charging a retry or a DLQ entry.
func warmSaturated(err error) bool {
	return errors.Is(err, runtime.ErrWarmBudgetSaturated)
}

// lifecycleCancelled reports whether the DELIVERY context (the worker lifecycle
// context the stream layer hands the runner) is already canceled, i.e. the
// worker is shutting down. A cancellation must not change the PERSISTED
// retry/exhaustion decision: if it were treated as a failed attempt, a shutdown
// could schedule a retry backoff or write a terminal exhaustion marker (and thus
// a DLQ entry) for work the handler never had a chance to finish, while the
// stream layer leaves the message pending anyway (see processMessage). The
// runner therefore refuses to claim an attempt once the lifecycle is canceled
// (pre-claim check on each path) and refuses to issue a retry transition or an
// exhausted marker for a canceled delivery (recordFailure's guard). This is
// deliberately narrower than "a cancellation charges nothing": the execution
// failure telemetry (handler_failure_total, the labeled invocation-outcome and
// per-app failure counters, and last_failure_at) is incremented as soon as a
// post-claim executor reports an error, BEFORE recordFailure's guard is
// consulted, so a post-claim cancellation is still counted as a failure in
// telemetry while the persisted invocation state is left untouched. The check is
// on the PARENT context, never the invocation's own timeout context: a genuine
// handler timeout (context.DeadlineExceeded on a live delivery) still records
// normally.
func lifecycleCancelled(ctx context.Context) bool {
	return ctx.Err() != nil
}

// admitWarm pre-admits ctx against the worker-global warm-container budget
// (MAX_WARM_CONTAINERS) BEFORE a handler attempt is claimed, returning a context
// carrying the permit and whether admission succeeded. ok=false means every warm
// execution container is busy and none can be evicted within warmAdmitWait
// (backpressure): the caller MUST leave the work pending WITHOUT claiming a
// handler attempt, so no retry budget is charged. When the executor exposes no
// warm admitter (fake/in-process executors, an unbounded runtime) admission is
// unconditional and the context is returned unchanged.
//
// The permit is released by Manager.Execute when it is not consumed (an idle
// container was leased) or by the owning container on discard when it is; a
// caller that admits and then does not execute must release it, which the
// consume-aware WarmPermit.Release makes safe. The wait is bounded by
// warmAdmitWait (not r.slotWait); it is added AFTER the concurrency-slot wait, so
// the total wait may exceed the stream reclaim threshold, which is safe because
// it happens before the claim (see slotWaitTimeout). Tests may shorten it via
// r.warmWait.
func (r *Runner) admitWarm(ctx context.Context) (context.Context, bool) {
	adm := r.warmAdmitterResolver()
	if adm == nil {
		return ctx, true
	}
	wait := r.warmWait
	if wait <= 0 {
		wait = warmAdmitWait
	}
	waitCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	permit, err := adm.AcquireWarmPermit(waitCtx)
	if err != nil {
		return ctx, false
	}
	return runtime.WithWarmPermit(ctx, permit), true
}

// executeWithRefs runs one rule's handler while holding a reference to the
// app's image for the duration of the invocation, so a concurrent
// RetireImage cannot remove the image an in-flight execution still needs
// (at-least-once safety). The release is deferred so it runs even if the
// executor panics; the helper is called per rule so the defer scope is
// per-invocation rather than accumulating across a long rule loop. extraEnv are
// the per-invocation env vars (template env values + resolved secrets).
//
// lease is the snapshot's admitted publication lease pinning the image; it is
// attached to the execution context so Manager.Execute executes under that
// admitted authority rather than acquiring a fresh (possibly rejected) lease. A
// nil lease (fake executors, no-runtime apps, test values) leaves the
// context unchanged and lets Execute acquire its own.
func (r *Runner) executeWithRefs(
	pf *PreparedApp,
	invokeCtx context.Context,
	handler string,
	eventJSON []byte,
	extraEnv []string,
	lease *runtime.ImageLease,
) error {
	image := toImage(pf)
	r.refs.acquire(image)
	defer r.refs.release(image)
	if lease != nil {
		invokeCtx = runtime.WithSnapshotLease(invokeCtx, lease)
	}
	return pf.executor.Execute(invokeCtx, pf.prepared, handler, eventJSON, extraEnv)
}

// runInvocation runs one rule's handler while holding a reference to the
// app's image for the duration of the invocation, converting an
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
	pf *PreparedApp,
	invokeCtx context.Context,
	cancel context.CancelFunc,
	handler string,
	eventJSON []byte,
	extraEnv []string,
	lease *runtime.ImageLease,
	trace invocationTrace,
) (panicked bool, panicValue any, err error) {
	// The end-to-end invocation span, shared by the event-rule, schedule, and
	// manual paths (all funnel through here). It is a child of the delivery
	// (or replay/manual operation) context, and its span context is propagated
	// into the executor so the runtime's runtime.execute/acquire/invoke children
	// nest beneath it. RunMeta and the timeout already stamped on invokeCtx are
	// inherited.
	//
	// Retry lineage: a new attempt links to the PREVIOUS attempt's stored span
	// reference (when one was persisted) and then records its own span context
	// so a later retry, even after a worker restart, can link back to it. The
	// link is not a parent: every attempt is a distinct function.invoke span
	// (usually sharing the original upstream trace through the delivery context)
	// while the link records the causal retry chain across attempts. Recording
	// only ever persists the compact traceparent/tracestate lineage, never
	// baggage, and a state-free caller (manual/replay) records nothing.
	var runtimeName string
	if pf != nil && pf.fn.Template != nil {
		runtimeName = pf.fn.Template.Runtime
	}
	var fnName string
	if pf != nil {
		fnName = pf.fn.Name
	}
	spanCtx, span := startInvocationSpan(invokeCtx, fnName, handler, runtimeName, trace.spanOpts()...)
	trace.record(span.SpanContext())
	defer func() {
		if panicked {
			finishInvocationSpan(span, fmt.Errorf("executor panic: %v", panicValue))
			return
		}
		finishInvocationSpan(span, err)
	}()
	defer cancel()
	defer func() {
		if pv := recover(); pv != nil {
			panicked = true
			panicValue = pv
			err = fmt.Errorf("executor panic: %v", pv)
		}
	}()
	return false, nil, r.executeWithRefs(pf, spanCtx, handler, eventJSON, extraEnv, lease)
}

// Handle evaluates the event against all loaded apps and executes every
// matching rule's handler. It returns nil only when every invocation succeeded
// (or nothing matched); otherwise it returns an error so the stream layer does
// not acknowledge the message.
//
// At-least-once semantics: Handle returns an error after partial successes, so
// a retried message re-runs the handlers that already succeeded. The event
// classification counters (events_received/matched/unmatched_total) are NOT
// delivery-attempt counters: each logical event is classified exactly once
// across redeliveries/retries via an atomic claim in the message's
// invocation-state hash (see stream.InvocationState.ClaimClassification). A
// handler failure stays in the matched class. Handle does NOT claim
// exactly-once execution: redeliveries re-run whatever is not yet recorded as
// complete, and the handlers must stay idempotent.
//
// Aggregate, per-invocation semantics: a single Redis message can match multiple
// "<app>/<handler>" invocations, and each is tracked independently in the
// per-message invocation-state hash. Every eligible matching invocation gets its
// own attempt on each delivery regardless of the others' outcomes: a failure in one
// handler does NOT prevent later matching handlers from running. The rule loop is
// strictly sequential (never parallel), and after iterating every matching rule
// Handle aggregates the per-invocation outcomes into the message-level return
// contract below.
//
// Invocation state: when the stream layer injects an InvocationState into ctx
// (see stream.WithInvocationState), Handle skips any matching invocation whose
// "<app>/<handler>" ID is already recorded as completed on a previous
// delivery, is protected by an active attempt deadline or a retry backoff (a
// running or next_attempt_at marker whose persisted deadline has not yet passed
// — this or another replica may be executing it, or it is waiting out its
// backoff), or is exhausted (terminal). Skipped invocations are not executions:
// they do not touch the handler_* or function_* metrics.
// app_events_matched_total still counts the app as engaged (it
// matched), which is attribution, not execution counting. When no invocation
// state is present (direct Handle callers/tests, or invocation tracking
// disabled) Handle behaves exactly as before: it runs every matching handler
// and returns the first failure's plain error immediately, with no invocation
// wrapping or DLQ attribution.
//
// The per-invocation attempt is claimed (TryStart) only AFTER the concurrency
// slots and the warm-container budget are admitted, so a capacity rejection
// spends no attempt. The claim is the persisted attempt boundary: a crash after
// a confirmed claim but before the handler runs spends that attempt when its
// running deadline later elapses. The configured retry budget bounds normal
// failing executions but does not cap admitted claims across repeated crashes, so
// the persisted count can exceed 1+retries before any real failure (see
// internal/stream/doc.go). TryStart itself never exhausts, though: exhaustion is
// recorded here only when a later attempt actually runs and fails with the
// persisted count already at or above 1+retries, so a crash alone cannot DLQ a
// message.
//
// Pending desired generations: the registry can hold, beside the active set, a
// PENDING desired template for a valid generation that is being prepared but is
// not yet runnable (see Registry.SetPending). Pending rules are MATCHED — they
// engage their app and keep the message pending as unavailable — but they never
// execute, claim a TryStart, exhaust, ACK, or DLQ. This closes the event-loss
// window during preparation: a delivery that matches ONLY a pending rule (a
// brand-new app before its first image, or a new rule not yet in the active
// generation) is matched-but-unavailable rather than unmatched, so it is never
// ACKed away. A pending rule whose invocation is ALSO matched by the active
// generation is deduped by "<app>/<handler>": the active generation executes it
// and it is not separately held. When the new generation is installed (or the
// app is removed/invalidated) the pending entry is cleared atomically with the
// registry mutation.
//
// Return contract (with invocation state, aggregated after the full rule loop):
//   - a plain (retryable) error when any matched invocation had a retryable
//     failure this delivery — regardless of other invocations' outcomes — so
//     the message stays pending and is retried. The first such error is returned.
//   - a wrapped runner.ErrAppUnavailable when at least one MATCHED
//     invocation belongs to a configured app whose image is not currently
//     built and that invocation has not already completed. Such an event is
//     MATCHED, never unmatched, but the invocation cannot run this delivery: the
//     message stays pending (not ACKed, and not DLQ'd solely for
//     unavailability). No handler attempt is claimed and no handler counter is
//     touched, because no handler ran. A later delivery — once the app is
//     rebuilt and available again — completes the outstanding invocation.
//   - a wrapped stream.ErrInvocationExhausted when every invocation in the
//     matched set is terminal (complete or exhausted), at least one of them is
//     exhausted, and no retryable failure or unavailable match occurred this
//     delivery. The whole message is terminal, so the stream layer routes it to
//     the DLQ. This also fires on a redelivery where the exhausting invocation
//     was already marked exhausted by an earlier delivery: the message may still
//     be pending because its DLQ write (or the post-DLQ XACK) failed, so it must
//     be re-routed rather than returned nil and ACKed without a DLQ entry.
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
	// failure; exhaustedInvocations collects EVERY exhausted matched invocation —
	// the typed metadata of one that exhausted this delivery, or one synthesized
	// from a terminal-skip of an invocation already marked exhausted on a previous
	// delivery (so a redelivery after a failed DLQ write re-routes instead of
	// ACKing). The aggregate returns them all on a *stream.HandlerExhaustedError,
	// so each exhausted app/handler gets its own DLQ entry with its own
	// attempt count. anyExhausted records whether any matched invocation is
	// exhausted (this delivery or a previous one). These are only meaningful when
	// hasState is true.
	skippedPending := false
	var firstErr error
	// claimErr records the first ambiguous-claim failure (a TryStart store
	// error) seen this delivery. It is kept separate from firstErr because a
	// claim failure is NOT a failed attempt (no handler ran, no retry/exhaustion
	// accounting); it must stay pending, and it is surfaced (wrapping
	// ErrInvocationNotEligible) so the cause is distinguishable from an ordinary
	// protected skip.
	var claimErr error
	var exhaustedInvocations []stream.ExhaustedInvocation
	anyExhausted := false
	// executed tracks whether any invocation actually executed, for the
	// no-state (direct caller/test) path, which preserves the old fail-fast
	// tail exactly. With invocation state it is unused (the aggregate uses the
	// per-invocation outcomes instead).
	executed := false

	// Take one consistent snapshot for the whole call so a concurrent registry
	// swap mid-execution cannot reorder or drop apps under us, and PIN each
	// published app's image while the registry lock still protects its
	// entry. The pins are held until Handle returns, so an image published at
	// snapshot time cannot be removed out from under this delivery even if the
	// entry is superseded concurrently; each matching execution carries its
	// app's pin into Manager.Execute. The pins are shares of the published
	// lease, so a retirement in progress never blocks an already-admitted
	// delivery.
	snap := r.reg.snapshotPinned()
	defer snap.release()
	snapshot := snap.fns

	// Pre-pass: collect every matched invocation ID so an exhausted attempt can
	// decide whether the whole message is terminal (all matched invocations
	// complete or exhausted), and collect the engaged app names. The
	// registry snapshot holds one entry per app, so an app with several
	// matching rules is appended once here (deduped by construction). This must
	// be complete before any execution, because a rule that exhausts early must
	// still see the full set of matched invocations (including ones that sort
	// later). Matching is pure; a panic here is a programming error that escapes
	// and no counter has been touched yet.
	//
	// Matching includes configured apps whose image is not currently built
	// (available == false): an event matching ONLY such an app is MATCHED,
	// not unmatched, so it is classified matched and the app is counted as
	// engaged. Skipping unavailable apps here (the previous behavior) both
	// mis-classified those events as unmatched and let a mixed message ACK
	// without running the unavailable app's share. unavailableMatched
	// records the invocations that matched but cannot run this delivery; an
	// unresolved one keeps the message pending rather than ACKed or DLQ'd for
	// unavailability alone (see the aggregate below). A nil Template (only
	// reachable from a hand-built unavailable entry, never from the loader)
	// cannot match and is skipped.
	var matched []string
	var matchedFns []string
	var unavailableMatched []string
	for _, pf := range snapshot {
		if pf.fn.Template == nil {
			continue
		}
		rules := snap.matchingRules(pf, event)
		if len(rules) == 0 {
			continue
		}
		matchedFns = append(matchedFns, pf.fn.Name)
		for _, rule := range rules {
			invocation := pf.fn.Name + "/" + rule.Handler
			matched = append(matched, invocation)
			if !pf.available {
				unavailableMatched = append(unavailableMatched, invocation)
			}
		}
	}

	// Pending desired matches: a valid generation being PREPARED but not yet
	// runnable contributes its desired invocations as matched-but-unavailable,
	// so a delivery arriving during preparation stays pending instead of being
	// acknowledged as unmatched (the new-app / new-rule event-loss gap this
	// closes). Pending entries are non-runnable placeholders (no image, no
	// executor): they are matched here for CLASSIFICATION and holding only and
	// are deliberately absent from the execution loop below. Dedupe by
	// invocation identity against the active matches: a rule that is active in
	// v1 and also present in pending v2 is executed by the active path and must
	// not be separately held. A pending-only invocation still counts as matched
	// and engages its app, but its non-terminal (unresolved) state keeps the
	// message pending — it never executes, claims a TryStart, exhausts, ACKs, or
	// DLQs.
	matchedSet := make(map[string]struct{}, len(matched))
	for _, inv := range matched {
		matchedSet[inv] = struct{}{}
	}
	matchedFnSet := make(map[string]struct{}, len(matchedFns))
	for _, fnName := range matchedFns {
		matchedFnSet[fnName] = struct{}{}
	}
	for _, pf := range snap.pending {
		if pf.fn.Template == nil {
			continue
		}
		rules := snap.matchingPendingRules(pf, event)
		if len(rules) == 0 {
			continue
		}
		engaged := false
		for _, rule := range rules {
			invocation := pf.fn.Name + "/" + rule.Handler
			if _, dup := matchedSet[invocation]; dup {
				// Already matched (and executable) through the active
				// generation: do not separately hold the same invocation.
				continue
			}
			matchedSet[invocation] = struct{}{}
			matched = append(matched, invocation)
			unavailableMatched = append(unavailableMatched, invocation)
			engaged = true
		}
		if engaged {
			if _, dup := matchedFnSet[pf.fn.Name]; !dup {
				matchedFnSet[pf.fn.Name] = struct{}{}
				matchedFns = append(matchedFns, pf.fn.Name)
			}
		}
	}

	// An unavailable matched invocation is UNRESOLVED unless it already COMPLETED
	// for this message: a complete marker means the invocation's work here is
	// settled (it ran before this worker lost/never had the app), so it does
	// not hold the message pending. Anything else — never ran, or exhausted
	// awaiting a DLQ re-route whose attempt count only the available path can read
	// back — is unresolved, and the message must stay pending: it must neither be
	// ACKed (which could drop an outstanding execution or an unpersisted DLQ
	// entry) nor be dead-lettered solely for unavailability. The exhausted case is
	// deliberately included: this worker cannot attribute the exhausted attempt
	// count from an unavailable entry, and ACKing would race another replica's
	// DLQ write, so the safe choice is to hold pending until the app is
	// available again and the normal path surfaces the exhaustion with correct
	// metadata. With no invocation state there is no marker to consult, so any
	// unavailable match is treated as unresolved (the fail-safe direction). This
	// is read-only: no attempt is claimed and no counter is touched for an
	// unavailable app.
	var unresolvedUnavailable []string
	if len(unavailableMatched) > 0 {
		if hasState {
			for _, invocation := range unavailableMatched {
				if !invState.IsComplete(invocation) {
					unresolvedUnavailable = append(unresolvedUnavailable, invocation)
				}
			}
		} else {
			unresolvedUnavailable = unavailableMatched
		}
	}

	// Classify the logical event exactly once (received == matched +
	// unmatched), regardless of delivery attempt. With invocation state the
	// stream-injected handle claims the classification atomically in the
	// message's invocation-state hash: only the first delivery across
	// redeliveries, reclaims, and replicas wins, so retries never double-count.
	// A claim error counts nothing (classification is a partition, so a missed
	// count is preferable to a double count). Without invocation state (direct
	// Handle callers/tests) there is no dedup channel, so each call is treated
	// as a distinct logical event.
	//
	// A handler failure does NOT move the event out of the matched class: the
	// classification is decided from matching alone, before any execution.
	// Unmatched events are acknowledged and never retried.
	classify := true
	if hasState {
		claimed, err := invState.ClaimClassification()
		if err != nil {
			classify = false
		} else {
			classify = claimed
		}
	}
	if classify {
		r.metrics.Inc(metrics.MetricEventsReceived)
		if len(matchedFns) > 0 {
			r.metrics.Inc(metrics.MetricEventsMatched)
			// An app is "engaged" by an event when at least one of its
			// rules matches, regardless of whether the execution later fails:
			// an event matching two apps counts once per app here,
			// while MetricEventsMatched counts it once globally.
			for _, fnName := range matchedFns {
				r.metrics.IncLabels(metrics.MetricAppEventsMatched,
					[]metrics.Label{{Name: "app", Value: fnName}})
			}
		} else {
			r.metrics.Inc(metrics.MetricEventsUnmatched)
		}
	}

	for _, pf := range snapshot {
		if !pf.available {
			continue
		}
		rules := snap.matchingRules(pf, event)
		for _, rule := range rules {
			// The invocation identity is stable across restarts and config
			// reloads as long as the rule still exists: the app name and the
			// rule handler string. Renaming either invalidates old invocation
			// state — old entries simply never match, and the msg-level set of
			// required invocations is recomputed each delivery from current
			// templates, so a rule removed from the template no longer gates the
			// ACK.
			invocation := pf.fn.Name + "/" + rule.Handler
			if hasState && invState.IsComplete(invocation) {
				r.log.Debug("App handler: already succeeded for event; skipping",
					"app", pf.fn.Name,
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
			// worker-global and per-app concurrency slots (whose defer scope
			// is THIS call, so a multi-rule message never accumulates slots across
			// rules). It classifies this invocation's outcome for this delivery and
			// returns it plus the plain error (for a failed attempt) so the outer
			// loop can aggregate AFTER iterating every matching rule — a failure in
			// one handler never prevents later handlers from running.
			executeRule := func() (invocationOutcome, error) {
				// The per-invocation handler attempt/claim. With invocation
				// state it comes from TryStart (Redis-backed: the persisted
				// attempt advances at the confirmed claim, with a fresh opaque
				// token; execution and its metrics normally follow, but a
				// post-claim crash can spend a claim unexecuted). Without
				// invocation state there is no persisted handler attempt, so it
				// stays the zero claim (explicitly not attributed): the delivery
				// count is NOT reused as a handler attempt count, because
				// deliveries count redeliveries, not executions. The no-state path
				// never calls recordFailure, so it never fabricates an
				// exhaustion/DLQ attribution either.
				var claim stream.InvocationClaim
				// Reserve the worker-global and per-app concurrency slots
				// BEFORE TryStart, so a blocked invocation is never counted as an
				// attempt and does not persist state. reserveSlots acquires the
				// two semaphores sequentially, each with its own up-to-slotWait
				// wait, and warm admission can then add warmAdmitWait, so the
				// total wait may approach 60s + the warm wait — enough to overlap
				// MinPendingIdle and let a reclaim redeliver this still-waiting
				// message. That overlap is safe: TryStart is the only admission
				// point and runs AFTER capacity is reserved, and invocation state
				// prevents a concurrent second execution, so a reclaim cannot run
				// the invocation while this delivery still waits. On a timeout
				// this path claims nothing (pre-claim): it records no retry, ACKs
				// nothing, and leaves the message pending in the PEL for reclaim.
				// The slots are released via defer as soon as THIS invocation
				// finishes.
				releaseSlots, waited := r.reserveSlots(ctx, pf.fn.Name, pf.fn.Template.Concurrency)
				if releaseSlots == nil {
					// No slot freed in time: skip this invocation without marking
					// it failed, leave the message pending (reclaim replays it
					// later).
					skippedPending = true
					r.log.Debug("App handler: concurrency slot wait timed out; leaving pending",
						"app", pf.fn.Name,
						"handler", rule.Handler,
						"message_id", msgID,
						"delivery_attempt", int(deliveryAttempt),
					)
					return outcomePendingSkip, nil
				}
				if waited {
					r.log.Debug("App handler: waiting for concurrency slot",
						"app", pf.fn.Name,
						"handler", rule.Handler,
						"message_id", msgID,
						"delivery_attempt", int(deliveryAttempt),
					)
				}
				defer releaseSlots()
				// Pre-admit the worker-global warm-container budget
				// (MAX_WARM_CONTAINERS) BEFORE TryStart, so a saturated warm
				// budget leaves the invocation pending (backpressure) without
				// claiming a handler attempt or charging a retry. The permit is
				// consumed by the container Execute creates (released on its
				// discard) or released unused here when Execute reuses an idle
				// container; the deferred Release below is the safety net for the
				// paths that do not reach Execute.
				warmCtx, warmOK := r.admitWarm(ctx)
				if !warmOK {
					skippedPending = true
					r.log.Debug("App handler: warm-container budget saturated; leaving pending",
						"app", pf.fn.Name,
						"handler", rule.Handler,
						"message_id", msgID,
						"delivery_attempt", int(deliveryAttempt),
					)
					return outcomePendingSkip, nil
				}
				warmPermit := runtime.WarmPermitFrom(warmCtx)
				defer warmPermit.Release()
				// A lifecycle cancellation that landed while this invocation
				// waited for capacity must not claim an attempt: with no claim
				// there is no retry budget or exhaustion to charge, and the
				// message is left pending for a live consumer after restart
				// (satisfying "shutdown leaves messages pending, not counted as
				// attempts"). The concurrency slots and warm permit acquired
				// above are released by the deferred releases on this return
				// path. This check is deliberately BEFORE TryStart: a
				// cancellation that races an already-confirmed claim is
				// PRESERVED (the claim is spent, never rolled back) and handled
				// by recordFailure's cancellation gate below. Gated on hasState
				// because only the state-carrying path claims a persisted
				// attempt.
				if hasState && lifecycleCancelled(ctx) {
					skippedPending = true
					r.log.Debug("App handler: lifecycle canceled before claim; leaving pending without claiming an attempt",
						"app", pf.fn.Name,
						"handler", rule.Handler,
						"message_id", msgID,
						"delivery_attempt", int(deliveryAttempt),
					)
					return outcomePendingSkip, nil
				}
				// Claim the invocation for this execution before running it.
				// TryStart atomically persists an absolute running deadline
				// (now + timeout), the attempt, and a fresh claim token, and
				// returns started=false when the invocation is already complete
				// (handled above), exhausted, or protected by an active attempt
				// deadline or a retry backoff — this or another replica may be
				// executing it, or it is waiting out its backoff, so we must not
				// run it concurrently. The IsComplete check above is the fast path
				// that avoids a script call on completed invocations; TryStart's
				// own read also covers "ok" and the same case, so the two are
				// consistent.
				if hasState {
					started, startClaim, wait, startErr := invState.TryStart(invocation, timeout)
					if startErr != nil {
						// The claim outcome is unknown (Redis/transport or
						// token-generation error): do NOT run the handler (an
						// ambiguous claim could race a replica that won the same
						// claim) and leave the message pending so a later delivery
						// retries the claim. No handler attempt was confirmed, so
						// the claim stays zero and no retry/exhaustion accounting
						// happens here. The returned error wraps
						// ErrInvocationNotEligible (the stream's pending/no-ACK
						// contract) plus the distinct ErrInvocationClaimUnconfirmed
						// sentinel so the aggregate can surface a claim failure
						// separately from an ordinary protected skip.
						skippedPending = true
						r.log.Warn("App handler: claim failed (outcome unknown); leaving pending without executing",
							"app", pf.fn.Name,
							"handler", rule.Handler,
							"message_id", msgID,
							"error", startErr,
						)
						return outcomePendingSkip, fmt.Errorf("%w: %v: %w: %w",
							stream.ErrInvocationNotEligible, invocation,
							stream.ErrInvocationClaimUnconfirmed, startErr)
					}
					if !started {
						// The slot is released by the deferred releaseSlots before
						// the next rule acquires.
						if wait > 0 {
							// Protected by an active running deadline or a retry
							// backoff. The message must stay pending (the protected
							// invocation may still complete or fail on its own), so
							// this is a "not eligible" skip, not a completion.
							skippedPending = true
							r.log.Debug("App handler: not eligible for event (running or waiting for retry); leaving pending",
								"app", pf.fn.Name,
								"handler", rule.Handler,
								"message_id", msgID,
								"handler_attempt", startClaim.Attempt,
								"next_attempt_in", wait,
							)
							return outcomePendingSkip, nil
						}
						// Terminal (complete or exhausted): never execute again. An exhausted
						// invocation must still drive the message-level DLQ decision, so a redelivery
						// after a failed DLQ write or XACK routes to the DLQ again instead of being
						// acknowledged as complete.
						if startClaim.Attempt > 0 {
							anyExhausted = true
							exhaustedInvocations = append(exhaustedInvocations, stream.ExhaustedInvocation{
								App:      pf.fn.Name,
								Handler:  rule.Handler,
								Attempts: startClaim.Attempt,
							})
						}
						r.log.Debug("App handler: terminal for event; skipping",
							"app", pf.fn.Name,
							"handler", rule.Handler,
							"message_id", msgID,
							"handler_attempt", startClaim.Attempt,
						)
						return outcomeTerminalSkip, nil
					}
					claim = startClaim
				}
				handlerAttempt := claim.Attempt
				// The claimed attempt has begun: the TryStart above (or the
				// absence of invocation state, for direct callers) claimed it and
				// the slots are held. The persisted attempt advances at that
				// claim; execution normally follows immediately, but a crash (or
				// the create-time budget race) after the claim can leave it spent
				// unexecuted. This is where last_execution_at is stamped — once
				// per claimed attempt, retries included — and the skip branches
				// returned above never reach it.
				r.metrics.SetAppTimestamp(pf.fn.Name, metrics.AppTimestampExecution, time.Now().Unix())
				r.log.Debug("App rule: matched event",
					handlerLogFields(hasState, pf.fn.Name, rule.Handler, msgID, handlerAttempt, deliveryAttempt)...,
				)
				eventJSON, err := json.Marshal(event)
				if err != nil {
					// The invocation was already claimed (deferred-released) but
					// will not execute: this is a failed attempt, so treat it as
					// such — schedule a retry (or exhaust), then CONTINUE to the
					// next matching rule so each independent invocation gets its own
					// failed attempt.
					if hasState {
						return r.recordFailure(ctx, invState, invocation, claim, rule.Retries, pf.fn.Name, rule.Handler, msgID, err)
					}
					return outcomeRetryable, fmt.Errorf("app %q handler %q: marshal event: %w", pf.fn.Name, rule.Handler, err)
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
						return r.recordFailure(ctx, invState, invocation, claim, rule.Retries, pf.fn.Name, rule.Handler, msgID, err)
					}
					return outcomeRetryable, fmt.Errorf("app %q handler %q: %w", pf.fn.Name, rule.Handler, err)
				}
				invokeCtx, cancel := context.WithTimeout(ctx, timeout)
				// Carry the pre-admitted warm-budget permit into Execute so a
				// newly created container owns the counted slot. The permit is
				// released by Execute (unused, on a warm lease) or by the
				// container on discard; the deferred release above is the safety
				// net for paths that never reach Execute.
				if warmPermit != nil {
					invokeCtx = runtime.WithWarmPermit(invokeCtx, warmPermit)
				}
				// Stamp the invocation's diagnostic metadata into the context so
				// the executor can attach it as container labels. This keeps the
				// Executor interface (and every test fake) unchanged.
				eventID, eventName := eventFields(event)
				invokeCtx = runtime.WithRunMeta(invokeCtx, runtime.RunMeta{
					Type:      runtime.ContainerTypeEvent,
					App:       pf.fn.Name,
					Handler:   rule.Handler,
					MessageID: msgID,
					EventID:   eventID,
					EventName: eventName,
					Hostname:  r.hostname,
					Image:     toImage(pf),
				})
				start := time.Now()
				panicked, panicValue, err := r.runInvocation(pf, invokeCtx, cancel, rule.Handler, eventJSON, extraEnv, snap.pinFor(pf), invocationTrace{state: invState, invocation: invocation, attempt: handlerAttempt})
				elapsed := time.Since(start)
				if panicked {
					// A panicking execution is a misbehaving handler, not a
					// healthy failure: log the panic value and the full stack so
					// the bug is visible and attributable, then funnel it through
					// the SAME failure branch below (metrics + recordFailure) so
					// retry and exhaustion accounting stay per-invocation.
					r.log.Error("App handler: PANICKED for event",
						append(handlerLogFields(hasState, pf.fn.Name, rule.Handler, msgID, handlerAttempt, deliveryAttempt),
							"panic_value", fmt.Sprintf("%v", panicValue),
							"stack", string(debug.Stack()),
						)...,
					)
				}
				if warmSaturated(err) {
					// The container could not be created because the global warm
					// budget saturated at create time (a race between admission
					// and the actual create). This is BACKPRESSURE, not a handler
					// failure: no failure, retry, or DLQ accounting is charged and
					// the message stays pending. The attempt claimed by TryStart
					// above IS nonetheless spent — this is the accepted
					// claim-before-execute window (the same one a crash between the
					// claim and the handler start falls in): the claimed running
					// marker's deadline simply elapses, and a later delivery claims
					// the NEXT attempt. Pre-claim backpressure (a slot timeout or a
					// saturated warm-budget admission above) never reaches a claim
					// and so spends nothing. Nothing here exhausts: the eventual
					// DLQ still needs a later attempt to actually run and fail.
					skippedPending = true
					r.log.Debug("App handler: warm-container budget saturated at create; leaving pending",
						"app", pf.fn.Name,
						"handler", rule.Handler,
						"message_id", msgID,
					)
					return outcomePendingSkip, nil
				}
				if err != nil {
					r.metrics.IncLabels(metrics.MetricHandlerInvocations,
						[]metrics.Label{
							{Name: "outcome", Value: "failure"},
							{Name: "app", Value: pf.fn.Name},
							{Name: "handler", Value: rule.Handler},
						})
					// Unlabeled total for the PostgreSQL snapshot; the labeled counter
					// above stays for Prometheus.
					r.metrics.Inc(metrics.MetricHandlerFailure)
					// Per-app failure attribution (per rule execution). A
					// failed attempt that will retry still counts as a failure
					// (and as an execution above); only a DLQ-routed exhaustion
					// additionally sets last_dlq_at (see recordFailure).
					r.metrics.IncLabels(metrics.MetricFunctionHandlerFailure,
						[]metrics.Label{{Name: "app", Value: pf.fn.Name}})
					r.metrics.SetAppTimestamp(pf.fn.Name, metrics.AppTimestampFailure, time.Now().Unix())
					r.metrics.ObserveDurationLabels(metrics.MetricHandlerDuration,
						[]metrics.Label{
							{Name: "app", Value: pf.fn.Name},
							{Name: "handler", Value: rule.Handler},
						}, elapsed)
					r.log.Warn("App handler: execution failed for event",
						append(handlerLogFields(hasState, pf.fn.Name, rule.Handler, msgID, handlerAttempt, deliveryAttempt),
							"duration", elapsed,
							"reason", err,
						)...,
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
						return r.recordFailure(ctx, invState, invocation, claim, rule.Retries, pf.fn.Name, rule.Handler, msgID, err)
					}
					// No invocation state (direct callers/tests): every failure
					// counts as a retry driver, but there is no Redis-backed
					// attempt count to decide exhaustion — so no DLQ attribution
					// here either. The DLQ metric is only meaningful with
					// invocation state, where exhaustion is actually persisted
					// and observable.
					r.metrics.IncLabels(metrics.MetricFunctionRetries,
						[]metrics.Label{{Name: "app", Value: pf.fn.Name}})
					return outcomeRetryable, err
				}
				// Record the invocation as completed so a redelivery skips it.
				// This happens BEFORE the success metrics so a crash between the
				// side effect and MarkComplete re-runs the handler (at-least-once;
				// the handler must remain idempotent). The completion is CASed on
				// this attempt's claim; a refusal (the marker was re-claimed by a
				// newer token, or is already terminal exhausted) means this
				// delivery does not own the invocation's resolution, so treat it
				// as unresolved: leave the message pending (never ACK a
				// superseded claim's outcome) and let the newer claim or terminal
				// state drive the decision on a later delivery.
				if hasState {
					if !invState.MarkComplete(invocation, claim) {
						skippedPending = true
						r.log.Warn("App handler: completion superseded; leaving pending",
							"app", pf.fn.Name,
							"handler", rule.Handler,
							"message_id", msgID,
							"handler_attempt", handlerAttempt,
						)
						return outcomePendingSkip, nil
					}
				}
				r.recordHandlerSuccess(pf.fn.Name, rule.Handler, elapsed)
				r.log.Info("App handler: executed for event",
					append(handlerLogFields(hasState, pf.fn.Name, rule.Handler, msgID, handlerAttempt, deliveryAttempt),
						"duration", elapsed,
					)...,
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
			case outcomePendingSkip:
				// Every pending skip (protected invocation, slot timeout, claim
				// failure, or a superseded stale transition) leaves the message
				// unresolved: it must never be ACKed. A claim failure is
				// additionally remembered via its distinguishable error so the
				// aggregate can surface its cause.
				skippedPending = true
				if err != nil && errors.Is(err, stream.ErrInvocationClaimUnconfirmed) && claimErr == nil {
					claimErr = err
				}
			case outcomeRetryable:
				if firstErr == nil {
					firstErr = err
				}
			case outcomeExhausted:
				anyExhausted = true
				// recordFailure returns a *stream.HandlerExhaustedError carrying
				// exactly one exhausted invocation; collect it so the aggregate
				// reports every exhausted app/handler with its own attempt
				// count. A non-typed error is ignored here (it cannot happen in
				// production) and the terminal-skip metadata still drives the
				// aggregate.
				var typed *stream.HandlerExhaustedError
				if errors.As(err, &typed) {
					exhaustedInvocations = append(exhaustedInvocations, typed.Invocations...)
				}
			}
		}
	}
	// Aggregate: decide the single message-level error from the per-invocation
	// outcomes collected across the whole rule loop.
	if hasState {
		// 0. An ambiguous claim failure (a TryStart store error) is surfaced
		//    first: the claim outcome is unknown, so the message stays pending
		//    with no handler execution and no retry accounting, and the distinct
		//    sentinel keeps the cause visible even when another invocation also
		//    failed this delivery.
		if claimErr != nil {
			return claimErr
		}
		// 1. Any retryable failure this delivery → the message stays pending
		//    (retryable): return the first such failure, even if other
		//    invocations succeeded or are otherwise still running.
		if firstErr != nil {
			return firstErr
		}
		// 2. A matched-but-unavailable invocation is unresolved (it matched and
		//    has not already completed) → the message stays pending and is NOT
		//    ACKed, and it is NOT dead-lettered solely for unavailability. No
		//    handler attempt was claimed and no handler counter was touched for
		//    it (no handler ran). Returning the retryable ErrAppUnavailable
		//    makes the stream leave the message pending (an ordinary retryable
		//    error), exactly like the schedule path's unavailable handling, so a
		//    later delivery — after the app is rebuilt and available again —
		//    completes the outstanding work. This is checked BEFORE the DLQ
		//    decision so an exhausted sibling can never dead-letter a message
		//    whose unavailable invocation is still outstanding.
		if len(unresolvedUnavailable) > 0 {
			return unavailableMatchError(unresolvedUnavailable)
		}
		// 3. Every matched invocation is terminal (complete or exhausted) AND at
		//    least one exhausted → the message is terminal; route it to the DLQ.
		//    allMatchedTerminal fails open to false on a read error, keeping the
		//    message pending rather than DLQ'ing it. The aggregate error carries
		//    EVERY exhausted invocation's exact app/handler/attempt metadata
		//    and wraps stream.ErrInvocationExhausted, so the stream layer writes
		//    one correctly-attributed DLQ entry per exhausted invocation.
		if anyExhausted && allMatchedTerminal(invState, matched) {
			return &stream.HandlerExhaustedError{Invocations: dedupeExhausted(exhaustedInvocations)}
		}
		// 4. Any matched invocation was protected- or slot-timeout-skipped
		//    (unresolved) → the message stays pending with NO retry accounting.
		//    This fires even when other invocations executed successfully this
		//    call: an unresolved invocation must not be ACKed away. A claim
		//    failure was already surfaced at step 0.
		if skippedPending {
			return stream.ErrInvocationNotEligible
		}
		// 5. Every matched invocation is complete (or nothing matched) → ACK.
		return nil
	}
	// No invocation state: preserve the old fail-fast tail — nil when something
	// executed successfully (or nothing matched); ErrInvocationNotEligible when
	// nothing executed and at least one invocation was protected- or
	// slot-timeout-skipped (so the stream leaves the message pending). A
	// matched-but-unavailable invocation is likewise unresolved: it is reported
	// (never silently treated as unmatched/complete) so the caller leaves the
	// message pending. With no state there is no terminal marker to consult, so
	// any unavailable match counts as unresolved.
	if len(unresolvedUnavailable) > 0 {
		return unavailableMatchError(unresolvedUnavailable)
	}
	if executed {
		return nil
	}
	if skippedPending {
		return stream.ErrInvocationNotEligible
	}
	return nil
}

// unavailableMatchError reports the matched-but-unavailable invocations of a
// message as a retryable error wrapping ErrAppUnavailable. An unavailable
// app's event is MATCHED (not unmatched) and must keep the message pending
// without being dead-lettered for unavailability alone, so the stream layer
// treats this exactly like any other retryable failure. The invocation IDs are
// included so the log line names the outstanding app/handler.
func unavailableMatchError(invocations []string) error {
	return fmt.Errorf("%w: matched but unavailable: %s", ErrAppUnavailable, strings.Join(invocations, ", "))
}

// obsoleteOccurrence is the terminal error returned when a schedule occurrence
// names an app or schedule that is no longer in the configuration. The
// stream layer recognizes ErrInvocationObsolete and ACKs the message: an
// obsolete occurrence is never retried or dead-lettered.
func obsoleteOccurrence(fnName, scheduleName string) error {
	return fmt.Errorf(
		"%w: schedule app %q schedule %q no longer in configuration",
		stream.ErrInvocationObsolete, fnName, scheduleName,
	)
}

// rejectAppAbsent disposes of a schedule occurrence whose app is ABSENT from
// THIS replica's registry. A local absence is NOT proof that the occurrence is
// obsolete: another replica may already have ADMITTED it (pinning a descriptor
// and claiming the invocation), and an app-absent replica cannot execute an
// adopted descriptor — there is no image or executor. The terminal obsolete
// rejection is therefore RESERVED atomically (stream.InvocationState
// ReserveScheduleRejection), which refuses it when anything was already admitted:
//
//   - reserved / already reserved: the reserved kind is authoritative. An
//     obsolete reservation is ACKed (never retried or dead-lettered); an invalid
//     one — reserved earlier, when the app was still present here — is
//     dead-lettered.
//   - an ACTIVE marker (a pinned descriptor whose invocation is running/backoff,
//     or a legacy no-descriptor active marker): the message stays PENDING
//     (ErrInvocationNotEligible) so the attempt is not cancelled. It is never
//     ACKed and never DLQ'd.
//   - an exhausted marker (pinned descriptor OR legacy no-descriptor): the
//     admitted invocation(s) already exhausted and still owe a per-invocation DLQ
//     entry, so a typed HandlerExhaustedError is returned — the stream persists
//     the missing entries (idempotently; an already-":dlq" marker is skipped) and
//     then ACKs.
//   - a pinned descriptor with no terminal marker, terminal-retained, or a "ok"
//     marker: a pinned no-marker admission stays PENDING; a completed invocation
//     (or a message already off the PEL) returns nil so the stream ACKs and
//     terminal-retains idempotently.
//
// A reservation error means the outcome is unknown: the message is left pending,
// exactly like an unconfirmed claim, so no rejection is dispositioned on an
// ambiguous result.
func (r *Runner) rejectAppAbsent(invState stream.InvocationState, fnName, scheduleName string) error {
	admission, err := invState.ReserveScheduleRejection(stream.ScheduleRejectionObsolete)
	if err != nil {
		r.log.Warn("Schedule: app absent; rejection reservation failed; leaving pending",
			"app", fnName,
			"schedule", scheduleName,
			"error", err,
		)
		return fmt.Errorf("%w: %v: %w: %w",
			stream.ErrInvocationNotEligible, fnName,
			stream.ErrInvocationClaimUnconfirmed, err)
	}
	if admission.InFlight {
		// An ACTIVE admission already exists (a pinned descriptor admitted
		// elsewhere, or a legacy active marker with no readable descriptor). This
		// replica cannot resolve it, so keep the message pending: never reject,
		// never ACK/DLQ.
		r.log.Debug("Schedule: app absent here but the occurrence is admitted; leaving pending",
			"app", fnName,
			"schedule", scheduleName,
		)
		return stream.ErrInvocationNotEligible
	}
	if len(admission.ExhaustedInvocations) > 0 {
		// An exhausted marker owns the message (a pinned descriptor whose
		// invocation exhausted before its DLQ write, or a legacy no-descriptor
		// exhausted marker): the admitted invocation(s) already exhausted and still
		// owe a per-invocation DLQ entry. Return the typed exhaustion error so the
		// stream persists the missing entries (idempotently — an already-":dlq"
		// marker is skipped) and then ACKs. This is never a rejection and never
		// leaves the disposition stuck.
		r.log.Warn("Schedule: app absent here; exhausted invocation owed a DLQ entry; routing to DLQ",
			"app", fnName,
			"schedule", scheduleName,
			"invocations", len(admission.ExhaustedInvocations),
		)
		return &stream.HandlerExhaustedError{Invocations: admission.ExhaustedInvocations}
	}
	if admission.Rejected {
		if admission.Rejection == stream.ScheduleRejectionInvalid {
			// Reserved invalid by an earlier delivery (when the app was still
			// present): terminal and non-retryable, so dead-letter it.
			r.log.Warn("Schedule: occurrence previously rejected as invalid; routing to DLQ",
				"app", fnName,
				"schedule", scheduleName,
			)
			return fmt.Errorf("%w: schedule app %q schedule %q was rejected as invalid before admission",
				stream.ErrScheduleInvalid, fnName, scheduleName)
		}
		r.log.Warn("Schedule: occurrence obsolete; app removed; acknowledging",
			"app", fnName,
			"schedule", scheduleName,
		)
		return obsoleteOccurrence(fnName, scheduleName)
	}
	// A zero admission is either terminal-retained (the message already left the
	// PEL) or a legacy no-descriptor "ok" marker (the admitted invocation already
	// succeeded). Return nil so the stream ACKs and terminal-retains idempotently.
	r.log.Debug("Schedule: app removed and occurrence already dispositioned; acknowledging",
		"app", fnName,
		"schedule", scheduleName,
	)
	return nil
}

// InvokeHandler executes a single schedule-occurrence invocation routed through
// the stream. Its admission boundary is deliberate and documented here:
//
// BEFORE ADMISSION (no descriptor pinned for the message): the occurrence
// resolves the CURRENT template by its STABLE schedule NAME on every delivery, so
// a handler/timeout/retries change under the same name takes effect and the
// name's current handler is what runs. Because an occurrence may block for a long
// time waiting for a concurrency slot, this config snapshot is refreshed from the
// registry AFTER the slot is admitted and immediately before the atomic first
// claim, so a reload that completed during the wait is observed. A missing NAME
// means the schedule was removed while the occurrence was pending; if nothing was
// ever pinned the occurrence is OBSOLETE, and — with invocation state — the
// obsolete rejection is RESERVED atomically (never a descriptor or attempt) and
// the stream ACKs it (never retried, never dead-lettered). A locally ABSENT app
// takes the same reserve-only path: the reservation is refused when another
// replica already admitted the occurrence, and the message then either stays
// PENDING (an active no-descriptor attempt) or completes the admitted
// invocation's real terminal disposition — a legacy "ok" marker is ACKed, and a
// legacy exhausted marker is dead-lettered (idempotently) then ACKed.
//
// AT ADMISSION (the first successful claim): the resolved descriptor — schedule
// name, handler, the CAPPED timeout, and the retry budget — is pinned ATOMICALLY
// with the claim (stream.InvocationState.TryStartScheduled), so a concurrent
// replica with a different template, or a reload racing the post-slot refresh,
// can never diverge: exactly one descriptor wins and every other delivery adopts
// it. Redis is therefore the global first-writer: a reload concurrent with that
// atomic first claim is treated as a competing proposal, and whichever descriptor
// the script persists is authoritative.
//
// AFTER ADMISSION (a descriptor is pinned): the descriptor is the single source
// of truth. The pinned handler, timeout, and retries are used EVEN IF the current
// schedule was renamed, retimed, or REMOVED, so an admitted invocation completes
// its retry/DLQ lifecycle instead of being cancelled by a config change. The
// schedule name remains the occurrence/dedup identity, but a pinned message no
// longer consults the current template for execution.
//
// The handler timeout is capped at the configured maximum exactly like Handle
// caps rule timeouts, and the CAPPED value is passed BOTH to admission (persisted
// as the running deadline) and to context.WithTimeout so the persisted deadline
// matches the local kill timer. The capped value is what the descriptor pins, so
// it stays stable across attempts.
//
// It reuses the exact event execution path: registry snapshot lookup, the global
// + per-app concurrency slots, per-invocation secret resolution, the panic
// boundary, and the same handler metrics. Invocation/DLQ attribution stays
// handler-based ("<app>/<handler>"); the descriptor adds schedule provenance
// only.
//
// When scheduleName is empty (the state-free DLQ-replay path, which knows only a
// recorded app/handler), resolution falls back to the first schedule entry
// matching the handler, then to that exact event rule (never event matching), and
// the no-state path executes a single attempt exactly as before.
//
// InvokeHandler takes only the app/schedule/handler strings; it does not carry
// the occurrence's scheduled instant, so it performs no semantic validation.
// The production stream entry point is InvokeOccurrence, which additionally
// validates the occurrence's scheduled_at against the current schedule before
// admission.
func (r *Runner) InvokeHandler(
	ctx context.Context, msgID, fnName, scheduleName, handler string, payload []byte,
) error {
	return r.invokeHandler(ctx, msgID, fnName, scheduleName, handler, payload, nil)
}

// InvokeOccurrence executes one schedule occurrence routed through the stream.
// It is InvokeHandler's production entry point: the decoded Occurrence (app,
// stable schedule name, envelope handler, scheduled instant, and derived
// identity) is passed so that, BEFORE admission, the occurrence is semantically
// validated against the CURRENT template — the named app must be configured and
// available, the stable schedule name must exist, and scheduled_at must be a
// real firing of that schedule's current cron/timezone (see validateOccurrence).
//
// The envelope handler is NEVER the execution target: only the app/schedule
// name select the invocation, and the current template's handler is what runs.
// The envelope handler is carried for provenance/logging only, so a handler
// change under the same name remains a valid live change.
//
// Validation applies only to a not-yet-admitted occurrence. Once a descriptor
// is pinned (first successful admission), it is authoritative and the
// occurrence completes its lifecycle even if the schedule was since changed or
// removed (see InvokeHandler). The state-free path (no invocation state) keeps
// its legacy single-attempt behavior and does not validate.
func (r *Runner) InvokeOccurrence(
	ctx context.Context, msgID string, occ schedule.Occurrence, payload []byte,
) error {
	return r.invokeHandler(ctx, msgID, occ.App, occ.Schedule, occ.Handler, payload,
		func(tmpl *app.Template) error { return validateOccurrence(tmpl, occ) })
}

// invokeHandler is the shared implementation of InvokeHandler and
// InvokeOccurrence. validate, when non-nil, is the occurrence's pre-admission
// semantic validation, called with the CURRENT template once the app and
// schedule name have resolved and the app is available. It runs only for an
// unadmitted occurrence (no pinned descriptor) whose schedule name still exists;
// a missing name is left to the existing obsolete path, and an admitted
// occurrence is authoritative and never re-validated.
func (r *Runner) invokeHandler(
	ctx context.Context, msgID, fnName, scheduleName, handler string, payload []byte,
	validate func(tmpl *app.Template) error,
) error {
	// Invocation state (when present) distinguishes the production stream path
	// from direct callers/tests: obsolete-removal and descriptor pinning only
	// apply on the production path. See the availability checks below.
	invState, hasState := stream.InvocationStateFrom(ctx)

	// Find the app in the current registry. GetByName returns nil only when
	// the app is ABSENT (removed) — a present but unavailable app
	// returns a non-nil entry whose Prepared() is nil. These two cases must be
	// handled differently:
	//   - absent (removed): an occurrence for it is OBSOLETE and terminal (ACKed,
	//     never retried/DLQ'd) — but only on the production state-carrying path;
	//     direct callers/tests keep the legacy plain error. Because a local
	//     absence is NOT proof of removal (another replica may have ADMITTED the
	//     occurrence), the obsolete rejection is RESERVED atomically first
	//     (rejectAppAbsent), and an already-admitted invocation is completed
	//     instead of rejected: a pinned descriptor admitted elsewhere or a legacy
	//     ACTIVE no-descriptor marker leaves the message PENDING, a legacy "ok"
	//     marker is ACKed, and a legacy exhausted marker is dead-lettered
	//     (idempotently) then ACKed — this replica has no image or executor, so it
	//     never executes the adopted descriptor. (The schedule NAME, not the
	//     app, is what admission pins; a still-present app whose schedule was
	//     removed is NOT obsolete once admitted — see the descriptor path below.)
	//   - present but unavailable: temporary (build failed at startup/reconcile),
	//     retryable exactly as today.
	//
	// The lookup PINS the app's published image (a share of its publication
	// lease, acquired under the registry lock), held until this invocation
	// returns, so a concurrent retirement cannot remove the image between this
	// lookup and the handler's execution. An UNADMITTED schedule occurrence
	// re-reads the current entry after the slot wait below and swaps in that
	// entry's pin (see the refresh there); the closure releases whichever lease
	// `pin` holds at return, so the stale lookup's lease is not leaked.
	pf, pin := r.reg.getByNamePinned(fnName)
	defer func() { pin.Release() }()
	if pf == nil {
		if hasState {
			// The app is absent from THIS replica. A local absence is NOT proof
			// that the occurrence is obsolete: another replica may already have
			// ADMITTED it, so the terminal obsolete rejection must be RESERVED
			// atomically, and only when nothing was admitted. See rejectAppAbsent.
			return r.rejectAppAbsent(invState, fnName, scheduleName)
		}
		r.log.Warn("Schedule: app is not available", "app", fnName)
		return fmt.Errorf("schedule invocation: app %q is not available", fnName)
	}
	if pf.Prepared() == nil {
		// The app exists but its image could not be built yet: temporarily
		// unavailable, so the occurrence is retryable (not obsolete — the app
		// is still configured).
		r.log.Warn("Schedule: app is temporarily unavailable", "app", fnName)
		return fmt.Errorf("schedule invocation: app %q is not available", fnName)
	}

	// Reserve the worker-global and per-app concurrency slots BEFORE the
	// admission claim so a blocked invocation is never counted as an attempt and
	// does not persist state (same ordering as Handle's event path). A slot
	// timeout means the invocation is unresolved: with invocation state it
	// returns a "not eligible" skip (the stream leaves the message pending with no
	// retry accounting); without state it preserves the legacy plain error. The
	// reservation is made for both the state-free and state-carrying paths, so a
	// held app slot also bounds a direct/no-state caller.
	releaseSlots, _ := r.reserveSlots(ctx, fnName, pf.fn.Template.Concurrency)
	if releaseSlots == nil {
		r.log.Warn("Schedule: concurrency slot wait timed out", "app", fnName, "handler", handler)
		if hasState {
			return stream.ErrInvocationNotEligible
		}
		return fmt.Errorf("schedule invocation: concurrency slot wait timed out")
	}
	defer releaseSlots()

	// Pre-admit the worker-global warm-container budget (MAX_WARM_CONTAINERS)
	// before any handler attempt is claimed, so a saturated warm budget leaves
	// the occurrence pending (backpressure) instead of charging a retry. The
	// permit rides the invocation context into Execute.
	schedCtx, warmOK := r.admitWarm(ctx)
	if !warmOK {
		r.log.Debug("Schedule: warm-container budget saturated; leaving pending",
			"app", fnName, "handler", handler)
		if hasState {
			return stream.ErrInvocationNotEligible
		}
		return fmt.Errorf("schedule invocation: warm-container budget saturated")
	}
	defer runtime.WarmPermitFrom(schedCtx).Release()

	// The state-free path (direct callers/tests, DLQ replay) preserves the legacy
	// single-attempt behavior exactly: resolve the current schedule, cap the
	// timeout, and execute once. No descriptor, no broker lifecycle.
	if !hasState {
		desc := resolveScheduleDescriptor(pf.fn.Template, scheduleName, handler)
		if cap := time.Duration(r.maxHandlerTimeout.Load()); cap > 0 && desc.Timeout > cap {
			desc.Timeout = cap
		}
		return r.invokeOnce(schedCtx, pf, desc.Handler, payload, desc.Timeout, pin, nil, "", stream.InvocationClaim{}, msgID)
	}

	// Read the pinned schedule descriptor ONCE (a Redis read, and the
	// authoritative source for an already-admitted occurrence). A descriptor
	// pinned by an earlier first admission is immutable while the message is
	// recoverable, so this read is stable for the rest of the delivery.
	desc, known := invState.ScheduleDescriptor()
	// admitted records whether a descriptor was already pinned at the START of
	// this delivery. `known` is later reassigned when the current template can
	// propose a descriptor, so this is the stable "is this occurrence
	// already-admitted?" signal the pre-admission validation keys on.
	admitted := known

	// An UNADMITTED schedule occurrence (scheduleName != "" and no pinned
	// descriptor) resolves its template by the stable schedule NAME, and it may
	// have waited arbitrarily long for a concurrency slot while a reload
	// hot-swapped or removed the app. The lookup above is stale for that
	// occurrence: refresh it from the registry under the lock and pin the NEWEST
	// entry, dropping the stale pin, so the descriptor proposed for the atomic
	// first admission comes from the CURRENT config. A reload racing AFTER this
	// fresh snapshot is a concurrent proposal: the atomic descriptor write in
	// TryStartScheduled remains the global first-winner boundary. A pinned
	// descriptor is authoritative and is NEVER re-resolved (its admitted schedule
	// may already be gone), but the app's availability is still rechecked.
	if scheduleName != "" && !known {
		// getByNamePinned returns a FRESH share of the current entry's
		// publication lease (always a distinct lease object, or nil when the
		// entry has none), so release the stale lookup's pin and adopt the fresh
		// one unconditionally — even when fresh is nil, so an app removed
		// while the occurrence waited drops the stale pin with it.
		fresh, freshPin := r.reg.getByNamePinned(fnName)
		pin.Release()
		pf, pin = fresh, freshPin
	}
	if pf == nil {
		// The app was present at the stale lookup but is gone from the CURRENT
		// registry now, after the slot wait. Use the SAME absent-app disposition as
		// the initial lookup: reserve the obsolete rejection atomically, and only
		// when nothing was admitted (a replica with the app may have admitted it
		// while this one waited).
		return r.rejectAppAbsent(invState, fnName, scheduleName)
	}
	if pf.Prepared() == nil {
		// The app exists but its image could not be built yet: temporarily
		// unavailable, so the occurrence is retryable (not obsolete — the
		// app is still configured).
		r.log.Warn("Schedule: app is temporarily unavailable", "app", fnName)
		return fmt.Errorf("schedule invocation: app %q is not available", fnName)
	}

	// Resolve the descriptor that owns this message. A pinned descriptor (read
	// above) is authoritative and survives a later template change or the
	// schedule's removal; otherwise resolve the CURRENT template by schedule NAME
	// (the production path) or by handler (the state-carrying fallback).
	if !known {
		desc = resolveScheduleDescriptor(pf.fn.Template, scheduleName, handler)
		// The production path carries a schedule NAME. When the app is
		// present but that NAME is gone, the occurrence may have been admitted by
		// ANOTHER replica whose template still had it, so we must NOT pre-empt the
		// atomic admission: pass no descriptor (known=false) and let the script
		// hand back the pinned one to adopt, or report it obsolete.
		if scheduleName != "" && !scheduleNameExists(pf.fn.Template, scheduleName) {
			known = false
		} else {
			// A resolved name, or the state-carrying handler fallback: a descriptor
			// is known (a fallback with no matching entry keeps the defaults).
			known = true
		}
	}
	// Pre-admission semantic validation (schedule-occurrence entry point only):
	// the app is present and available, so validate the occurrence against the
	// CURRENT template — the stable name must resolve to a schedule and
	// scheduled_at must be a real firing of that schedule's current
	// cron/timezone. A MISSING name is deliberately a no-op here: the atomic
	// admission below already decides between adopting a descriptor pinned by
	// another replica and reporting the never-admitted occurrence obsolete. A
	// pinned (already-admitted) occurrence is authoritative and never
	// re-validated, so a schedule edited or removed after admission still
	// completes under its pinned contract.
	//
	// On failure the disposition is deliberately NOT decided here: the caller
	// proposes no descriptor (known=false) and the atomic admission below still
	// observes any descriptor a concurrent replica has pinned, so an admitted
	// occurrence is adopted rather than raced into an ACK (no ACK hazard). Only
	// an admission that finds nothing pinned AND no proposal is then reported as
	// invalid, exactly where a never-admitted occurrence is reported obsolete.
	var invalidErr error
	if validate != nil && !admitted {
		if invalidErr = validate(pf.fn.Template); invalidErr != nil {
			r.log.Warn("Schedule: invalid occurrence claim; deferring to admission",
				"app", fnName,
				"schedule", scheduleName,
				"reason", invalidErr,
			)
			known = false
		}
	}

	// The admission input is either a PROPOSAL (a resolved descriptor) or a
	// terminal REJECTION. When the occurrence is not admitted and the current
	// template lets us propose nothing, it is terminally rejected: invalid when
	// the schedule NAME exists but the occurrence can never be a firing of it,
	// obsolete when the NAME (or the app) is gone. The rejection is RESERVED
	// atomically at the admission boundary (stream.InvocationState.TryStartScheduled),
	// which also refuses it if a concurrent replica admitted the occurrence first —
	// so a rejection can never race a live admission into an ACK/DLQ (F-030). A
	// pinned descriptor (admitted==true) always proposes, so an admitted
	// occurrence is never rejected.
	rejection := stream.ScheduleRejectionNone
	if !known {
		if invalidErr != nil {
			rejection = stream.ScheduleRejectionInvalid
		} else {
			rejection = stream.ScheduleRejectionObsolete
		}
	}

	// Cap the timeout exactly like Handle caps each rule's timeout (defense in
	// depth; template validation enforces the cap at load). When the descriptor is
	// pinned, its timeout was already capped at admission, so re-capping is a
	// no-op that keeps a hand-built/oversized pinned value safe.
	if cap := time.Duration(r.maxHandlerTimeout.Load()); cap > 0 && desc.Timeout > cap {
		desc.Timeout = cap
	}

	// A lifecycle cancellation that landed while the occurrence waited for
	// capacity must not claim an attempt (same contract as Handle's event
	// path). The concurrency slots and warm permit acquired above are released
	// by the deferred releases on this return path. This is BEFORE the atomic
	// admission: a cancellation racing an already-confirmed claim is PRESERVED
	// (spent, never rolled back) and handled by recordFailure's cancellation
	// gate below. This branch is reached only on the state-carrying path, which
	// is the only path that claims a persisted attempt.
	if lifecycleCancelled(ctx) {
		r.log.Debug("Schedule: lifecycle canceled before claim; leaving pending without claiming an attempt",
			"app", fnName, "handler", desc.Handler)
		return stream.ErrInvocationNotEligible
	}

	// Atomically pin the descriptor (only when none is pinned) and claim the
	// invocation. A lost descriptor race is resolved inside the handle: the
	// pinned descriptor is adopted and the admission retried, so the winning
	// handler/timeout/retries is what executes and no second invocation field is
	// ever claimed.
	admission, err := invState.TryStartScheduled(desc, rejection, func(d stream.ScheduleDescriptor) string {
		return fnName + "/" + d.Handler
	})
	if err != nil {
		// The claim outcome is unknown (Redis/transport or token-generation
		// error): do NOT run the handler and leave the message pending so a later
		// delivery retries the claim. No handler attempt was confirmed. The error
		// wraps ErrInvocationNotEligible (the stream's pending/no-ACK contract)
		// plus ErrInvocationClaimUnconfirmed so the cause is distinguishable.
		r.log.Warn("Schedule: claim failed (outcome unknown); leaving pending without executing",
			"app", fnName,
			"handler", desc.Handler,
			"error", err,
		)
		return fmt.Errorf("%w: %v: %w: %w",
			stream.ErrInvocationNotEligible, fnName+"/"+desc.Handler,
			stream.ErrInvocationClaimUnconfirmed, err)
	}
	if admission.Rejected {
		// Nothing was ever pinned and none can be: the occurrence was terminally
		// rejected before admission. For the schedule-occurrence entry point that
		// can mean two distinct things, told apart by the RESERVED kind:
		//
		//   - invalid: the schedule NAME exists but the occurrence can never be a
		//     firing of it (a non-firing scheduled_at, or an unparseable current
		//     cron). The claim can never succeed, so it is dead-lettered.
		//   - obsolete: the NAME (or the app) is gone from the current template —
		//     an intentional removal, so the occurrence is ACKed and never retried
		//     or dead-lettered. Removal semantics are preserved exactly.
		//
		// The RESERVED kind is authoritative: a redelivery (or a delivery whose
		// current template has since changed) dispositions the occurrence exactly
		// as the delivery that reserved it decided. invalidErr, when present, is
		// the current delivery's detailed rejection message and is preferred for
		// the DLQ reason; on a redelivery it may be nil because the template
		// changed (or the occurrence was already reserved), so a generic invalid
		// error is returned instead.
		if admission.Rejection == stream.ScheduleRejectionInvalid {
			if invalidErr != nil {
				r.log.Warn("Schedule: occurrence invalid; routing to DLQ",
					"app", fnName,
					"schedule", scheduleName,
					"reason", invalidErr,
				)
				return invalidErr
			}
			r.log.Warn("Schedule: occurrence previously rejected as invalid; routing to DLQ",
				"app", fnName,
				"schedule", scheduleName,
			)
			return fmt.Errorf("%w: schedule app %q schedule %q was rejected as invalid before admission",
				stream.ErrScheduleInvalid, fnName, scheduleName)
		}
		r.log.Warn("Schedule: occurrence obsolete; schedule no longer in template; acknowledging",
			"app", fnName,
			"schedule", scheduleName,
		)
		return obsoleteOccurrence(fnName, scheduleName)
	}
	if admission.InFlight {
		// An ACTIVE admission already exists but this caller must not act on it (a
		// no-descriptor running/backoff marker, or a pinned descriptor with no
		// terminal marker, detected by the rejection boundary). Keep the message
		// pending so the attempt is not cancelled: never ACK, never DLQ.
		r.log.Debug("Schedule: admission in flight without an adoptable descriptor; leaving pending",
			"app", fnName,
			"schedule", scheduleName,
		)
		return stream.ErrInvocationNotEligible
	}
	if len(admission.ExhaustedInvocations) > 0 {
		// An exhausted marker owns the message (the schedule NAME was removed but
		// the occurrence was admitted — under a pinned descriptor or the legacy
		// pre-pinning path). The admitted invocation(s) still owe a per-invocation
		// DLQ entry, so return the typed exhaustion error: the stream persists the
		// missing entries (idempotently) and then ACKs.
		r.log.Warn("Schedule: exhausted invocation owed a DLQ entry; routing to DLQ",
			"app", fnName,
			"schedule", scheduleName,
			"invocations", len(admission.ExhaustedInvocations),
		)
		return &stream.HandlerExhaustedError{Invocations: admission.ExhaustedInvocations}
	}

	// The descriptor that owns the message (adopted on a lost race, else our own).
	ed := admission.Descriptor
	handler = ed.Handler
	timeout := ed.Timeout
	retries := ed.Retries
	invocation := fnName + "/" + handler

	if !admission.Started {
		if admission.Wait > 0 {
			// Protected by an active running deadline or a retry backoff. The
			// message must stay pending: the protected invocation may still
			// complete or fail on its own, so this is a "not eligible" skip, never
			// an ACK.
			r.log.Debug("Schedule: invocation not eligible (running or waiting for retry); leaving pending",
				"app", fnName,
				"handler", handler,
				"handler_attempt", admission.Claim.Attempt,
				"next_attempt_in", admission.Wait,
			)
			return stream.ErrInvocationNotEligible
		}
		// Terminal. Attempt 0 is a complete (or terminal-retained) invocation, so
		// the message is ACKed. A positive attempt is an exhausted invocation: a
		// schedule has exactly ONE invocation, so the message is terminal and must
		// route to the DLQ, with the persisted exhausted attempt carried on the
		// typed error for DLQ attribution.
		if admission.Claim.Attempt == 0 {
			r.log.Debug("Schedule: invocation already succeeded; skipping (stream ACKs)",
				"app", fnName,
				"handler", handler,
			)
			return nil
		}
		r.log.Debug("Schedule: invocation terminal (exhausted); routing to DLQ",
			"app", fnName,
			"handler", handler,
			"handler_attempt", admission.Claim.Attempt,
		)
		return &stream.HandlerExhaustedError{
			Invocations: []stream.ExhaustedInvocation{{
				App:      fnName,
				Handler:  handler,
				Attempts: admission.Claim.Attempt,
			}},
		}
	}

	err = r.invokeOnce(schedCtx, pf, handler, payload, timeout, pin, invState, invocation, admission.Claim, msgID)
	if err != nil {
		// A stale completion (the claim was superseded or the marker is already
		// terminal) is not a handler failure: do NOT ACK a superseded claim's
		// outcome — leave the message pending so a later delivery resolves it.
		if errors.Is(err, stream.ErrInvocationNotEligible) {
			return err
		}
		// A create-time warm-budget saturation is BACKPRESSURE, not a handler
		// failure: leave the occurrence pending (a later delivery retries the
		// claim) without charging a retry or a DLQ. As on the event path, this
		// race happens after the confirmed admission, so the attempt it claimed
		// is spent even though nothing is charged (see Handle's create-time
		// saturation branch).
		if warmSaturated(err) {
			r.log.Debug("Schedule: warm-container budget saturated at create; leaving pending",
				"app", fnName,
				"handler", handler,
			)
			return stream.ErrInvocationNotEligible
		}
		// A failed attempt — resolve extra env, marshal, execution, or timeout
		// failures all land here. recordFailure decides retry vs exhaustion using
		// the ADMITTED descriptor's retry budget: a retryable failure schedules a
		// backoff and returns a plain error (the stream leaves the message pending,
		// gated by next_attempt_at); an exhausted attempt marks the invocation
		// terminal and returns a typed *stream.HandlerExhaustedError (which already
		// wraps stream.ErrInvocationExhausted and carries the exhausted handler
		// attempt). A schedule has exactly ONE invocation (this one), so the message
		// is terminal and the stream routes it to the DLQ.
		outcome, retErr := r.recordFailure(ctx, invState, invocation, admission.Claim, retries, fnName, handler, msgID, err)
		if outcome == outcomePendingSkip {
			// A stale transition (the claim was superseded, or the marker is
			// already terminal): do NOT ACK a superseded claim's outcome — leave
			// the message pending so a later delivery resolves it.
			return stream.ErrInvocationNotEligible
		}
		return retErr
	}
	return nil
}

// resolveScheduleDescriptor resolves a schedule occurrence's descriptor from the
// app's CURRENT template. When scheduleName is non-empty the schedule is
// resolved by its STABLE NAME (the production identity); a handler change under
// the same name is picked up, and a missing NAME leaves the envelope handler with
// the defaults (the caller decides whether that is obsolete). When scheduleName
// is empty (the state-free DLQ-replay path) it resolves by exact handler: first
// among the current schedules, then among event rules. This is never event
// matching — it selects by handler string alone, so no other rule can run. The
// returned descriptor's Timeout is NOT capped here; the caller caps it (and the
// capped value is what gets pinned).
func resolveScheduleDescriptor(tmpl *app.Template, scheduleName, handler string) stream.ScheduleDescriptor {
	desc := stream.ScheduleDescriptor{
		Handler: handler,
		Timeout: app.DefaultTimeout,
		Retries: app.DefaultRetries,
	}
	found := false
	if scheduleName != "" {
		for _, sch := range tmpl.Schedules {
			if sch.Name == scheduleName {
				desc.Schedule = scheduleName
				desc.Handler = sch.Handler
				desc.Timeout = sch.Timeout
				desc.Retries = sch.Retries
				found = true
				break
			}
		}
	}
	if !found {
		for _, sch := range tmpl.Schedules {
			if sch.Handler == handler {
				desc.Handler = handler
				desc.Timeout = sch.Timeout
				desc.Retries = sch.Retries
				found = true
				break
			}
		}
	}
	if !found {
		for _, rule := range tmpl.Events {
			if rule.Handler == handler {
				if rule.Timeout > 0 {
					desc.Timeout = rule.Timeout
				}
				desc.Retries = rule.Retries
				break
			}
		}
	}
	return desc
}

// scheduleNameExists reports whether the template still declares a schedule with
// the given stable name. The production path uses it only to decide whether a
// first-time (unpinned) occurrence can be admitted locally, never to cancel an
// already-admitted one.
func scheduleNameExists(tmpl *app.Template, scheduleName string) bool {
	for _, sch := range tmpl.Schedules {
		if sch.Name == scheduleName {
			return true
		}
	}
	return false
}

// scheduleByName returns the template's schedule with the given stable NAME, and
// whether it exists.
func scheduleByName(tmpl *app.Template, scheduleName string) (app.Schedule, bool) {
	for _, sch := range tmpl.Schedules {
		if sch.Name == scheduleName {
			return sch, true
		}
	}
	return app.Schedule{}, false
}

// validateOccurrence is the schedule occurrence's pre-admission semantic
// validation against the CURRENT template. It is called only once the app is
// present and available and no descriptor has been pinned (an admitted
// occurrence is authoritative and never re-validated).
//
// It enforces two invariants, using the schedule's stable NAME and the shared
// firing primitive:
//
//   - The name must resolve to a currently-configured schedule. A name that is
//     gone is NOT rejected here: returning nil lets the existing admission path
//     decide between adopting a descriptor pinned by another replica and
//     reporting the never-admitted occurrence OBSOLETE (ACKed, never retried or
//     dead-lettered). This keeps removal semantics exactly as they were.
//   - scheduled_at must be a real firing of that schedule's current cron in its
//     effective timezone (see schedule.IsFiring), so timezone/DST handling is
//     the parser's, not a re-implementation. A schedule whose own cron cannot be
//     parsed is a corrupt configuration: it is surfaced as invalid (a bounded,
//     non-retryable DLQ) rather than silently accepted, because no instant can
//     be a firing of an unparseable schedule.
//
// The envelope handler plays no part: the schedule is selected by name and its
// current handler is the execution target, so a handler change under the same
// name stays valid.
func validateOccurrence(tmpl *app.Template, occ schedule.Occurrence) error {
	sch, ok := scheduleByName(tmpl, occ.Schedule)
	if !ok {
		// Missing name: left to the admission path's obsolete decision.
		return nil
	}
	if sch.Location == nil {
		return fmt.Errorf("%w: app %q schedule %q has no timezone",
			stream.ErrScheduleInvalid, occ.App, occ.Schedule)
	}
	firing, err := schedule.IsFiring(sch.Cron, sch.Location, occ.ScheduledAt)
	if err != nil {
		return fmt.Errorf("%w: app %q schedule %q has an unparseable cron %q: %v",
			stream.ErrScheduleInvalid, occ.App, occ.Schedule, sch.Cron, err)
	}
	if !firing {
		return fmt.Errorf("%w: app %q schedule %q scheduled_at %s is not a firing of cron %q in %s",
			stream.ErrScheduleInvalid, occ.App, occ.Schedule,
			occ.ScheduledAt.UTC().Format(time.RFC3339), sch.Cron, sch.Location)
	}
	return nil
}

// InvokeApp executes every event rule of the named app whose pattern
// matches event, in declaration order, and returns the number of handlers
// invoked together with a concise error describing any failures. It is the
// synchronous manual-invocation primitive behind `relay app invoke`,
// driven by the worker over its query socket against the LIVE runner/runtime
// pool.
//
// It deliberately reuses the event execution path: the current registry
// snapshot, the worker-global + per-app concurrency slots, the rule
// timeout (capped at the configured maximum exactly like Handle), per-invocation
// env/secret resolution, the image in-flight reference, the panic boundary, the
// normal runtime executor (Manager.Execute), and the handler success/failure
// execution metrics. It deliberately does NOT reuse the broker lifecycle: it
// never consults stream.InvocationState, never claims or counts the event
// classification counters (events_received/matched/unmatched, and not
// app_events_matched_total), never schedules a retry, and never
// dead-letters. A manual invocation is an operator action, not a stream
// delivery, so no broker state is written.
//
// Failure semantics mirror Handle's aggregate behavior where it is sensible: a
// failure in one matching handler does NOT prevent the later matching handlers
// from running, and the returned error is the first failure after every matching
// handler has been attempted. Matching zero rules is not an error: it returns
// (0, nil) so the caller renders "no matching handlers".
//
// An app absent from the current registry returns ErrAppNotFound; a
// registered but unrunnable app (its image could not be built) returns
// ErrAppUnavailable. Both are wrapped with the app name so the socket
// can map them onto stable wire codes.
//
// Tracing: the worker-side root operation span is opened BEFORE registry and
// match validation, so an invalid app, an unavailable app, or a
// no-match invocation is still traced (with its error recorded) without changing
// any return semantics.
func (r *Runner) InvokeApp(
	ctx context.Context, name string, event map[string]any,
) (count int, err error) {
	// A worker-side root operation span around the whole manual invocation, so
	// an operator `relay app invoke` is traced on the worker without the
	// CLI carrying any trace context. It is a NEW root regardless of the
	// caller's context; every matching rule's function.invoke runs as its child
	// (the root span's context is the base for each rule's invokeCtx). It is
	// opened before validation and finalized by the deferred finalizer on every
	// return path (success, validation error, or handler failure).
	opCtx, opSpan := startManualInvokeSpan(ctx, name)
	ctx = opCtx
	defer func() { finishOperationSpan(opSpan, err) }()

	pf, pin := r.reg.getByNamePinned(name)
	if pin != nil {
		defer pin.Release()
	}
	if pf == nil {
		return 0, fmt.Errorf("%w: %q", ErrAppNotFound, name)
	}
	if pf.Prepared() == nil {
		return 0, fmt.Errorf("%w: %q", ErrAppUnavailable, name)
	}

	// Matching is pure and in declaration order. An app with no matching
	// rule is a successful no-op.
	rules := eventmatch.MatchingEventRules(pf.fn.Template.Events, event)
	if len(rules) == 0 {
		return 0, nil
	}

	// Marshal the supplied event once for every matching rule. A marshal
	// failure is a programming error in the caller's decoded value; report it
	// rather than executing a handler with a corrupt payload.
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return 0, fmt.Errorf("app %q: marshal event: %w", name, err)
	}
	eventID, eventName := eventFields(event)

	var firstErr error
	for _, rule := range rules {
		// Cap the rule timeout at the configured maximum (defense in depth;
		// template validation enforces the cap at load), exactly like Handle.
		timeout := rule.Timeout
		if cap := time.Duration(r.maxHandlerTimeout.Load()); cap > 0 && timeout > cap {
			timeout = cap
		}

		// Reserve the worker-global and per-app concurrency slots, exactly
		// like the event path. A slot timeout is not an execution: record the
		// first such error and keep trying the later rules (which may themselves
		// be blocked, in which case they are reported the same way).
		releaseSlots, _ := r.reserveSlots(ctx, name, pf.fn.Template.Concurrency)
		if releaseSlots == nil {
			r.log.Warn("App invoke: concurrency slot wait timed out",
				"app", name,
				"handler", rule.Handler,
			)
			if firstErr == nil {
				firstErr = fmt.Errorf("app %q handler %q: concurrency slot wait timed out", name, rule.Handler)
			}
			continue
		}

		// Pre-admit the worker-global warm-container budget before claiming an
		// attempt. A manual invocation is an operator action, not a stream
		// delivery, so a saturated budget is reported as an error (no pending
		// semantics) and the later rules are still tried.
		warmCtx, warmOK := r.admitWarm(ctx)
		if !warmOK {
			releaseSlots()
			r.log.Warn("App invoke: warm-container budget saturated",
				"app", name,
				"handler", rule.Handler,
			)
			if firstErr == nil {
				firstErr = fmt.Errorf("app %q handler %q: warm-container budget saturated", name, rule.Handler)
			}
			continue
		}

		err := func() error {
			defer releaseSlots()
			defer runtime.WarmPermitFrom(warmCtx).Release()

			// The handler attempt is about to begin: this is the
			// last_execution_at attribution point, exactly as Handle's rule loop
			// and InvokeHandler stamp it. A slot timeout above never reaches here
			// (it is not an execution).
			r.metrics.SetAppTimestamp(name, metrics.AppTimestampExecution, time.Now().Unix())

			extraEnv, err := r.resolveExtraEnv(ctx, pf.fn.Template)
			if err != nil {
				r.recordHandlerFailure(name, rule.Handler, 0)
				return fmt.Errorf("app %q handler %q: %w", name, rule.Handler, err)
			}

			invokeCtx, cancel := context.WithTimeout(warmCtx, timeout)
			// Stamp the invocation's diagnostic metadata so the execution
			// container carries its owner and identity labels. The type is the
			// event one-shot type: a manual invocation runs an event rule against
			// an operator-supplied event, so it belongs to the same execution
			// population (and is swept by hostname if the worker dies mid-call).
			// MessageID is empty: there is no stream message.
			invokeCtx = runtime.WithRunMeta(invokeCtx, runtime.RunMeta{
				Type:      runtime.ContainerTypeEvent,
				App:       name,
				Handler:   rule.Handler,
				EventID:   eventID,
				EventName: eventName,
				Hostname:  r.hostname,
				Image:     toImage(pf),
			})

			start := time.Now()
			panicked, panicValue, err := r.runInvocation(
				pf, invokeCtx, cancel, rule.Handler, eventJSON, extraEnv,
				pin, invocationTrace{},
			)
			elapsed := time.Since(start)
			if panicked {
				r.log.Error("App invoke: handler PANICKED",
					"app", name,
					"handler", rule.Handler,
					"panic_value", fmt.Sprintf("%v", panicValue),
					"stack", string(debug.Stack()),
				)
			}
			if warmSaturated(err) {
				// Backpressure, not a handler failure: a manual invocation is
				// synchronous, so report it without counting a handler failure.
				r.log.Warn("App invoke: warm-container budget saturated",
					"app", name,
					"handler", rule.Handler,
				)
				return fmt.Errorf("app %q handler %q: warm-container budget saturated", name, rule.Handler)
			}
			if err != nil {
				r.recordHandlerFailure(name, rule.Handler, elapsed)
				r.log.Warn("App invoke: handler execution failed",
					"app", name,
					"handler", rule.Handler,
					"duration", elapsed,
					"reason", err,
				)
				return fmt.Errorf("app %q handler %q: %w", name, rule.Handler, err)
			}
			r.recordHandlerSuccess(name, rule.Handler, elapsed)
			r.log.Info("App invoke: handler executed",
				"app", name,
				"handler", rule.Handler,
				"duration", elapsed,
			)
			return nil
		}()
		// Count every handler whose execution was attempted, including a failed
		// attempt: the count answers "how many handlers ran", and the caller
		// only prints it when there is no error anyway.
		count++
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if firstErr != nil {
		return count, firstErr
	}
	return count, nil
}

// ReplayDLQ re-executes the exact app/handler recorded by one DLQ entry
// against the CURRENT registry, for the `relay dlq replay` command. It is the
// DLQ counterpart of InvokeApp: an operator action against the LIVE runner,
// never a stream delivery.
//
// It validates the current configuration and then delegates to InvokeHandler:
//
//   - an app absent from the current registry returns ErrAppNotFound;
//   - a registered but unrunnable app (image build failed) returns
//     ErrAppUnavailable;
//   - a handler no longer present in the app's current template (its event
//     rules or schedules) returns ErrHandlerNotFound — the entry is retained and
//     the operator sees that the configuration changed.
//
// The delegation runs with a context carrying NO stream.InvocationState (an
// explicit opt-out, so it holds even if the caller's context came from the stream
// delivery path), so InvokeHandler takes its state-free single-attempt path:
// exactly one synchronous execution of the named handler (never event matching,
// so no other rule can run), against the app's current runtime, env/secrets
// resolution, concurrency slots, and timeout resolution. It writes NO broker
// state — no invocation-state hash, no event classification counters, and no
// retry/DLQ counters (recordFailure is only reached on the state-carrying path).
// It DOES record the normal handler success/failure execution metrics, exactly as
// a manual invocation does, because those are execution outcomes rather than
// broker state.
//
// The handler timeout follows InvokeHandler's current resolution: the matching
// schedule entry's timeout when the handler has one, otherwise the CURRENT event
// rule's timeout for that exact handler, otherwise the app default — always
// capped by the configured maximum. Resolving the event rule is by exact handler
// string, never event matching, so the replay still runs exactly one handler.
// event is the entry's original payload, replayed verbatim.
//
// Tracing: a `dlq.replay` span wraps the whole replay as a NEW ROOT worker
// operation (independent of the caller's context), carrying a span link to the
// original failed invocation when lineage is supplied (the DLQ entry's optional
// compact trace reference, forwarded by the control path — never embedded in the
// event JSON). The replayed `function.invoke` runs as its child. An empty or
// malformed lineage simply omits the link; the replay still runs. The operation
// span is opened BEFORE registry/handler validation, so a removed app or
// handler is still traced (with its error recorded) without changing any return
// semantics.
func (r *Runner) ReplayDLQ(ctx context.Context, fnName, handler string, event []byte, lineage string) (err error) {
	// The replay operation is a new root worker span; its child function.invoke
	// (started via InvokeHandler below) inherits this context. It is opened
	// before validation and records the terminal outcome through the deferred
	// finalizer on every return path.
	opCtx, opSpan := startReplaySpan(ctx, fnName, handler, lineage)
	defer func() { finishOperationSpan(opSpan, err) }()

	pf := r.reg.GetByName(fnName)
	if pf == nil {
		return fmt.Errorf("%w: %q", ErrAppNotFound, fnName)
	}
	if pf.Prepared() == nil {
		return fmt.Errorf("%w: %q", ErrAppUnavailable, fnName)
	}
	if !templateHasHandler(pf.fn.Template, handler) {
		return fmt.Errorf("%w: app %q handler %q", ErrHandlerNotFound, fnName, handler)
	}
	// Strip any inherited invocation state and run the state-free path: no
	// TryStart/complete/retry/exhaustion, and no DLQ accounting. The opt-out
	// makes the guarantee explicit even when the caller's context came from the
	// stream delivery path.
	return r.InvokeHandler(stream.WithoutInvocationState(opCtx), "", fnName, "", handler, event)
}

// templateHasHandler reports whether handler is present in the template's
// current event rules or schedules. A DLQ entry attributes an exhausted
// invocation that came from one of those, so either counts as "still
// configured". A nil template has no handlers.
func templateHasHandler(tmpl *app.Template, handler string) bool {
	if tmpl == nil {
		return false
	}
	for _, rule := range tmpl.Events {
		if rule.Handler == handler {
			return true
		}
	}
	for _, sch := range tmpl.Schedules {
		if sch.Handler == handler {
			return true
		}
	}
	return false
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
	pf *PreparedApp,
	handler string,
	payload []byte,
	timeout time.Duration,
	lease *runtime.ImageLease,
	invState stream.InvocationState,
	invocation string,
	claim stream.InvocationClaim,
	msgID string,
) error {
	// The claimed attempt has begun: the caller claimed it via TryStart (or is a
	// direct no-state caller) and holds the concurrency slots. The persisted
	// attempt advances at that claim; execution normally follows, though a
	// post-claim crash can leave it spent unexecuted. This is the schedule path's
	// last_execution_at attribution point — once per claimed attempt, retries
	// included.
	r.metrics.SetAppTimestamp(pf.fn.Name, metrics.AppTimestampExecution, time.Now().Unix())

	// Resolve the template's env values and secret references immediately before
	// container creation, mirroring Handle's rule path.
	extraEnv, err := r.resolveExtraEnv(ctx, pf.fn.Template)
	if err != nil {
		// Counted as a handler failure with a zero duration (no execution
		// happened), mirroring how Handle attributes a resolution failure.
		r.recordHandlerFailure(pf.fn.Name, handler, 0)
		return fmt.Errorf("schedule invocation: app %q handler %q: %w", pf.fn.Name, handler, err)
	}

	r.log.Debug("Schedule: invoking handler",
		"app", pf.fn.Name,
		"handler", handler,
	)

	invokeCtx, cancel := context.WithTimeout(ctx, timeout)
	// Stamp the invocation's diagnostic metadata. MessageID is the schedule
	// occurrence's real Redis stream message ID, stamped on relay.message_id;
	// EventID/EventName are empty. Type: containerTypeSchedule is the strict
	// relay.type marker that classifies this one-shot invocation container.
	invokeCtx = runtime.WithRunMeta(invokeCtx, runtime.RunMeta{
		Type:      runtime.ContainerTypeSchedule,
		App:       pf.fn.Name,
		Handler:   handler,
		MessageID: msgID,
		Hostname:  r.hostname,
		Image:     toImage(pf),
	})
	start := time.Now()
	panicked, panicValue, err := r.runInvocation(pf, invokeCtx, cancel, handler, payload, extraEnv, lease,
		invocationTrace{state: invState, invocation: invocation, attempt: claim.Attempt})
	elapsed := time.Since(start)
	if panicked {
		r.log.Error("App handler: PANICKED for schedule",
			"app", pf.fn.Name,
			"handler", handler,
			"panic_value", fmt.Sprintf("%v", panicValue),
			"stack", string(debug.Stack()),
		)
		r.recordHandlerFailure(pf.fn.Name, handler, elapsed)
		return err
	}
	if err != nil {
		r.recordHandlerFailure(pf.fn.Name, handler, elapsed)
		r.log.Warn("App handler: execution failed for schedule",
			"app", pf.fn.Name,
			"handler", handler,
			"duration", elapsed,
			"reason", err,
		)
		return err
	}
	// Mark the invocation complete on success BEFORE the success metrics so a
	// crash between the side effect and MarkComplete re-runs the handler
	// (at-least-once; the handler must remain idempotent). The completion is
	// CASed on this attempt's claim; a refusal means the marker was re-claimed by
	// a newer claim or is already terminal exhausted, so this delivery does not
	// own the resolution: return the not-eligible sentinel so the stream leaves
	// the message pending rather than ACK a superseded claim's outcome.
	if invState != nil {
		if !invState.MarkComplete(invocation, claim) {
			return fmt.Errorf("%w: completion superseded for %v", stream.ErrInvocationNotEligible, invocation)
		}
	}
	r.recordHandlerSuccess(pf.fn.Name, handler, elapsed)
	r.log.Info("App handler: executed for schedule",
		"app", pf.fn.Name,
		"handler", handler,
		"duration", elapsed,
	)
	return nil
}

// handlerLogFields builds the common structured-log fields for a handler
// execution log line: app, handler, message_id, and the attempt
// attribution. With invocation state the attempt attribution is the
// Redis-backed handler attempt (the count that drives retry/exhaustion); without
// it there is no handler attempt to attribute, so the message delivery attempt is
// reported instead. It never presents a delivery count as a handler attempt.
func handlerLogFields(hasState bool, fnName, handler, msgID string, handlerAttempt int, deliveryAttempt int64) []any {
	fields := []any{
		"app", fnName,
		"handler", handler,
		"message_id", msgID,
	}
	if hasState {
		return append(fields, "handler_attempt", handlerAttempt)
	}
	return append(fields, "delivery_attempt", int(deliveryAttempt))
}

// recordHandlerSuccess increments the success metrics shared by Handle's
// success branch, InvokeHandler, and manual invocation: the labeled invocation
// outcome counter, the unlabeled total, per-app success attribution, the
// last_success_at timestamp, and the duration histogram. It deliberately does
// NOT touch the event classification counters or app_events_matched_total —
// Handle owns those and counts them once per logical event, so an execution
// must not be double-attributed here.
func (r *Runner) recordHandlerSuccess(fnName, handler string, duration time.Duration) {
	r.metrics.IncLabels(metrics.MetricHandlerInvocations,
		[]metrics.Label{
			{Name: "outcome", Value: "success"},
			{Name: "app", Value: fnName},
			{Name: "handler", Value: handler},
		})
	// Unlabeled total for the PostgreSQL snapshot; the labeled counter above stays
	// for Prometheus.
	r.metrics.Inc(metrics.MetricHandlerSuccess)
	// Per-app success attribution.
	r.metrics.IncLabels(metrics.MetricFunctionHandlerSuccess,
		[]metrics.Label{{Name: "app", Value: fnName}})
	r.metrics.SetAppTimestamp(fnName, metrics.AppTimestampSuccess, time.Now().Unix())
	r.metrics.ObserveDurationLabels(metrics.MetricHandlerDuration,
		[]metrics.Label{
			{Name: "app", Value: fnName},
			{Name: "handler", Value: handler},
		}, duration)
}

// recordHandlerFailure increments the failure metrics shared by Handle's
// failure branch, InvokeHandler, and manual invocation: the labeled invocation
// outcome counter, the unlabeled total, per-app failure attribution, and
// the duration histogram. A failed attempt that will retry still counts as a
// failure here (it sets last_failure_at); only the DLQ-routed exhaustion
// additionally sets last_dlq_at (see recordFailure). It deliberately does NOT
// touch the event classification counters or app_events_matched_total —
// Handle owns those and counts them once per logical event, so a failure must
// not be double-attributed here.
func (r *Runner) recordHandlerFailure(fnName, handler string, duration time.Duration) {
	r.metrics.IncLabels(metrics.MetricHandlerInvocations,
		[]metrics.Label{
			{Name: "outcome", Value: "failure"},
			{Name: "app", Value: fnName},
			{Name: "handler", Value: handler},
		})
	r.metrics.Inc(metrics.MetricHandlerFailure)
	r.metrics.IncLabels(metrics.MetricFunctionHandlerFailure,
		[]metrics.Label{{Name: "app", Value: fnName}})
	r.metrics.SetAppTimestamp(fnName, metrics.AppTimestampFailure, time.Now().Unix())
	r.metrics.ObserveDurationLabels(metrics.MetricHandlerDuration,
		[]metrics.Label{
			{Name: "app", Value: fnName},
			{Name: "handler", Value: handler},
		}, duration)
}

// recordFailure handles a failed invocation attempt: it decides whether the
// handler attempt is retryable or exhausted, updates the invocation state and
// metrics accordingly, and returns the outcome plus the plain error Handle
// should aggregate. It is used for execution, marshal, secret-resolution, and
// timeout failures (all are failed attempts).
//
// A retryable handler attempt (attempt < 1+retries) schedules a retry backoff
// via RecordFailure, counts function_retries_total, and returns
// (outcomeRetryable, err). An exhausted handler attempt (attempt >= 1+retries)
// marks the invocation terminal via MarkExhausted, counts function_dlq_total, and
// returns (outcomeExhausted, err). Both transitions are CASed on the claim
// (attempt + token): a refusal (the claim is stale, or the marker is already
// terminal) returns (outcomePendingSkip, nil) so the message stays pending and a
// superseded claim never ACKs or dead-letters a newer claim's message. The
// message-level DLQ decision (whether EVERY matched invocation is terminal) is
// NOT made here; it is deferred to Handle's end-of-loop aggregation, so an
// invocation can exhaust while others still run without short-circuiting them.
//
// The retryable error is returned plain. The exhausted error is a
// *stream.HandlerExhaustedError carrying the exhausted handler attempt, so the
// stream layer can attribute the DLQ entry's handler_attempts from the handler
// retry state rather than the message delivery count. Its message names the
// same attempt count, keeping the DLQ `reason` consistent with
// `handler_attempts`.
//
// Lifecycle cancellation: a failure observed after the delivery context was
// canceled (worker shutdown) must not change the PERSISTED retry/exhaustion
// decision, regardless of the persisted attempt count. recordFailure returns
// (outcomePendingSkip, nil) without issuing either transition, so a shutdown can
// never schedule a retry backoff or write an exhaustion marker (and therefore
// never a DLQ entry); the claim, if one was confirmed, is left in place and its
// running deadline elapses so a later delivery claims the next attempt. The
// stream layer leaves the message pending either way. This guard covers only the
// persisted invocation state: the caller has already incremented the failure
// telemetry (handler_failure_total etc.) for the post-claim execution error that
// brought it here, so a shutdown-canceled execution is still visible in those
// counters even though no retry/exhaustion transition is issued.
func (r *Runner) recordFailure(
	ctx context.Context,
	invState stream.InvocationState,
	invocation string,
	claim stream.InvocationClaim,
	retries int,
	fnName, handler, msgID string,
	origErr error,
) (invocationOutcome, error) {
	// A lifecycle cancellation must not produce a persisted failed-attempt
	// transition. The delivery is being abandoned by a shutdown and the stream
	// layer will leave the message pending regardless, so scheduling a retry
	// backoff or writing a terminal exhaustion marker here would let a shutdown
	// consume the retry budget or dead-letter work the handler never had a chance
	// to run. (The caller has already counted the failure telemetry for any
	// post-claim execution error; only this persisted transition is suppressed.)
	// Leave the claim (if any) in place: its running deadline elapses and a later
	// delivery claims the NEXT attempt. Reported as a pending skip (no retry/DLQ
	// transition) exactly like a protected skip. This is evaluated BEFORE either
	// transition, so even a script that would otherwise commit is not issued for
	// a canceled delivery.
	if lifecycleCancelled(ctx) {
		r.log.Debug("App handler: attempt canceled during shutdown; leaving pending without charging retry/exhaustion",
			"app", fnName,
			"handler", handler,
			"message_id", msgID,
			"handler_attempt", claim.Attempt,
			"reason", origErr,
		)
		return outcomePendingSkip, nil
	}
	handlerAttempt := claim.Attempt
	maxAttempts := 1 + retries
	if handlerAttempt >= maxAttempts {
		// Exhausted: mark the invocation terminal, CASed on this attempt's claim
		// (attempt + token). If the store refuses (the claim is stale — a newer
		// token now owns the marker — or the marker is already terminal), this
		// delivery does not own the invocation's resolution: do NOT report
		// exhaustion (which would dead-letter a message whose newer claim may
		// still resolve), leave the message pending, and let the newer claim or a
		// later terminal-skip delivery drive the DLQ decision. This is the
		// exhaustion-commit attribution point: the invocation exhausted its
		// retries and its terminal exhausted marker is now persisted, so
		// function_dlq_total and last_dlq_at are stamped HERE — not on every
		// failure, not again on the later terminal-skip redeliveries of the same
		// invocation, and not at the message-level DLQ write (which happens later
		// in the stream layer, only after all exhausted siblings resolve, and
		// only if the XADD succeeds).
		if !invState.MarkExhausted(invocation, claim) {
			r.log.Warn("App handler: exhaustion superseded; leaving pending",
				"app", fnName,
				"handler", handler,
				"message_id", msgID,
				"handler_attempt", handlerAttempt,
			)
			return outcomePendingSkip, nil
		}
		r.metrics.IncLabels(metrics.MetricFunctionDLQ,
			[]metrics.Label{{Name: "app", Value: fnName}})
		r.metrics.SetAppTimestamp(fnName, metrics.AppTimestampDLQ, time.Now().Unix())
		r.log.Error("App handler: exhausted; invocation terminal",
			"app", fnName,
			"handler", handler,
			"message_id", msgID,
			"handler_attempt", handlerAttempt,
			"handler_attempts_total", maxAttempts,
		)
		return outcomeExhausted, &stream.HandlerExhaustedError{
			Invocations: []stream.ExhaustedInvocation{{
				App:      fnName,
				Handler:  handler,
				Attempts: handlerAttempt,
				Err:      origErr,
			}},
		}
	}
	// Retryable: schedule a retry backoff and count the retry. The claim
	// (attempt + token) is passed as a compare-and-set guard so a stale owner
	// (whose running deadline elapsed and whose invocation was re-claimed by a
	// newer claim) cannot overwrite the newer claim's marker. If the store
	// refuses (stale claim or terminal marker), this delivery does not own the
	// invocation: leave the message pending rather than schedule a retry on a
	// superseded claim's behalf.
	backoff := retryBackoff(handlerAttempt)
	if !invState.RecordFailure(invocation, claim, backoff) {
		r.log.Warn("App handler: retry superseded; leaving pending",
			"app", fnName,
			"handler", handler,
			"message_id", msgID,
			"handler_attempt", handlerAttempt,
		)
		return outcomePendingSkip, nil
	}
	r.metrics.IncLabels(metrics.MetricFunctionRetries,
		[]metrics.Label{{Name: "app", Value: fnName}})
	r.log.Warn("App handler: failed attempt; retrying later",
		"app", fnName,
		"handler", handler,
		"message_id", msgID,
		"handler_attempt", handlerAttempt,
		"handler_attempts_total", maxAttempts,
		"retry_backoff", backoff,
		"reason", origErr,
	)
	return outcomeRetryable, fmt.Errorf(
		"app %q handler %q: handler attempt %d failed: %w",
		fnName, handler, handlerAttempt, origErr,
	)
}

// dedupeExhausted returns the exhausted invocations deduplicated by
// "<app>/<handler>", preserving first-seen order. This is a defensive
// backstop: a parsed template rejects duplicate event handlers, so the same
// invocation should not appear twice in the aggregate, but hand-built
// templates and legacy aggregates may still carry duplicate metadata; the DLQ
// contract is one entry per exhausted invocation, so it is collapsed here.
func dedupeExhausted(invocations []stream.ExhaustedInvocation) []stream.ExhaustedInvocation {
	if len(invocations) < 2 {
		return invocations
	}
	seen := make(map[string]bool, len(invocations))
	out := make([]stream.ExhaustedInvocation, 0, len(invocations))
	for _, iv := range invocations {
		inv := iv.Invocation()
		if seen[inv] {
			continue
		}
		seen[inv] = true
		out = append(out, iv)
	}
	return out
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

// resolveExtraEnv builds the per-invocation env for a template: the literal env
// values followed by the resolved secret values, both name-ordered. Secret
// values are resolved here, immediately before execution, so they are never
// cached on Prepared or baked into the fingerprint. A template that references a
// secret but has no provider configured fails with a clear error naming the
// reference. The returned slice is the ONLY place a resolved secret value lives
// before it is handed to the executor; the executor carries it in the request
// frame's env map to the reused bootstrap process. It is never logged,
// persisted, or written into an execution container's Docker Config.Env.
func (r *Runner) resolveExtraEnv(ctx context.Context, tmpl *app.Template) ([]string, error) {
	var extra []string
	for _, ev := range tmpl.EnvList() {
		extra = append(extra, ev.Name+"="+ev.Value)
	}
	for _, sb := range tmpl.SecretList() {
		if r.secrets == nil {
			return nil, fmt.Errorf("app references secret %q but no secret provider is configured", sb.Ref)
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

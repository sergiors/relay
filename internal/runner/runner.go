package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"relay/internal/function"
	"relay/internal/logging"
	"relay/internal/metrics"
	"relay/internal/runtime"
	"relay/internal/secrets"
	"relay/internal/stream"
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
	log     *log.Logger
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
}

// ImageCleaner is the subset of the runtime Manager that image retirement
// needs. It is a small interface so the runner can retire superseded function
// images without depending on the runtime package concretely; test fakes that
// do not implement it simply yield a nil cleaner (no retirement).
type ImageCleaner interface {
	// RemoveImage removes a single relay-owned image, treating an already-gone
	// image as success.
	RemoveImage(ctx context.Context, image string) error
	// FunctionImageTags lists every local image tag (full references) belonging
	// to a function's repository, so the runner can retire each version with
	// in-flight safety.
	FunctionImageTags(ctx context.Context, name string) ([]string, error)
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
func New(prepared []*PreparedFunction, logger *log.Logger) *Runner {
	return NewWithMetrics(prepared, logger, nil)
}

// NewWithMetrics is like New but wires an optional metrics registry. A nil
// registry is safe: every metric call is a no-op.
func NewWithMetrics(prepared []*PreparedFunction, logger *log.Logger, m *metrics.Registry) *Runner {
	if logger == nil {
		logger = log.Default()
	}
	r := &Runner{reg: &Registry{}, log: logger, metrics: m, refs: newImageRefCounter()}
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
// cmd/worker/main.go does) labels every invocation.
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
		r.log.Printf("image cleanup: list function %q versions: %v", name, err)
		return
	}
	for _, tag := range tags {
		r.RetireImage(tag)
	}
}

// removeImageAsync removes a retired image off the event path so a docker round
// trip can never add latency (or failure) to Handle. It re-checks in-use just
// before removing because an execution may have (re)claimed the image after it
// was retired; if so it is left in place for that execution and its own release
// path. A nil cleaner is a no-op.
func (r *Runner) removeImageAsync(image string) {
	cleaner := r.resolver()
	if cleaner == nil {
		return
	}
	go func() {
		// A retirement that is superseded by a new execution must not remove an
		// image a container is about to start; skip removal if it became in-use.
		if r.ImageInUse(image) {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		if err := cleaner.RemoveImage(ctx, image); err != nil {
			r.log.Printf("image cleanup: remove retired %s: %v", image, err)
		}
	}()
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
func (r *Runner) executeWithRefs(pf *PreparedFunction, invokeCtx context.Context, handler string, eventJSON []byte, extraEnv []string) error {
	image := toImage(pf)
	r.refs.acquire(image)
	defer r.refs.release(image)
	return pf.executor.Execute(invokeCtx, pf.prepared, handler, eventJSON, extraEnv)
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
) (err error, panicked bool, panicValue any) {
	defer cancel()
	defer func() {
		if pv := recover(); pv != nil {
			panicked = true
			panicValue = pv
			err = fmt.Errorf("executor panic: %v", pv)
		}
	}()
	err = r.executeWithRefs(pf, invokeCtx, handler, eventJSON, extraEnv)
	return err, false, nil
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
// not unique-event counters.
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
// exactly as before.
//
// Return contract (with invocation state):
//   - nil when every matched invocation is complete (or nothing matched).
//   - a wrapped stream.ErrInvocationNotEligible when at least one matched
//     invocation was skipped because it is protected (running or waiting out a
//     retry backoff) and nothing executed. The stream layer leaves the message
//     pending without counting a retry or routing to the DLQ — this is what
//     fixes the cross-replica ACK hazard: a replica that reclaims a message
//     whose invocation is still in flight on another replica must not ACK it.
//   - a wrapped stream.ErrInvocationExhausted when a failing invocation's
//     attempts are exhausted AND every other matched invocation is complete or
//     also exhausted, so the message is terminal and the stream layer routes it
//     to the DLQ.
//   - the plain execution error otherwise (a retryable failure, or an exhausted
//     invocation while other matched invocations may still run), so the message
//     stays pending.
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
	r.metrics.Inc("events_received_total")
	// Best-effort delivery attempt, defaulting to 1 when the stream did not set
	// it (e.g. when the runner is driven directly in tests). It is used for
	// logging and as the per-invocation attempt number when no invocation state
	// is present (each direct call is then treated as attempt 1).
	deliveryAttempt := stream.DeliveryAttemptFrom(ctx)
	// Best-effort invocation state, absent when the stream did not inject it
	// (direct Handle callers/tests, or invocation tracking disabled).
	invState, hasState := stream.InvocationStateFrom(ctx)

	// Track whether any invocation actually executed and whether any matched
	// invocation was skipped because it is protected (running or waiting out a
	// retry backoff). These drive the return contract above.
	executed := false
	skippedPending := false

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
		for _, rule := range pf.fn.Template.MatchingRules(event) {
			matched = append(matched, pf.fn.Name+"/"+rule.Handler)
		}
	}

	for _, pf := range snapshot {
		if !pf.available {
			continue
		}
		eventID, eventName := eventFields(event)
		rules := pf.fn.Template.MatchingRules(event)
		// A function is "involved" in an event when at least one of its rules
		// matches, regardless of whether the execution later fails. This is the
		// functions-engaged counter: an event matching two functions counts once
		// per function here, while the message-level events_processed_total
		// (stream) and events_received_total (above) count it once globally.
		if len(rules) > 0 {
			r.metrics.IncLabels("function_events_total",
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
				r.log.Printf("function %q handler %q already succeeded for event %q; skipping%s",
					pf.fn.Name, rule.Handler, msgID,
					logging.Fields(
						"function", pf.fn.Name,
						"handler", rule.Handler,
						"message_id", msgID,
						"event_id", eventID,
						"event_name", eventName,
						"attempt", deliveryAttempt,
					))
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
			// The per-invocation attempt number. With invocation state it comes
			// from TryStart (Redis-backed, incremented per actual execution);
			// without it, each direct call is simply attempt 1.
			attempt := int(deliveryAttempt)
			// Claim the invocation for this execution before running it. TryStart
			// persists an absolute running deadline (now + timeout) and returns
			// started=false when the invocation is already complete (handled
			// above), exhausted, or protected by an active attempt deadline or a
			// retry backoff — this or another replica may be executing it, or it
			// is waiting out its backoff, so we must not run it concurrently. The
			// IsComplete check above is the fast path that avoids an HSET on
			// completed invocations; TryStart's own HGET also reads "ok" and
			// covers the same case, so the two are consistent.
			if hasState {
				started, n, wait := invState.TryStart(invocation, timeout)
				if !started {
					if wait > 0 {
						// Protected by an active running deadline or a retry
						// backoff. The message must stay pending (the protected
						// invocation may still complete or fail on its own), so
						// this is a "not eligible" skip, not a completion.
						skippedPending = true
						r.log.Printf("function %q handler %q not eligible for event %q (running or waiting for retry; eligible in %s)%s",
							pf.fn.Name, rule.Handler, msgID, wait,
							logging.Fields(
								"function", pf.fn.Name,
								"handler", rule.Handler,
								"message_id", msgID,
								"event_id", eventID,
								"event_name", eventName,
								"attempt", n,
								"next_attempt_in", wait,
							))
					} else {
						// Terminal (exhausted): skipped like complete, never
						// eligible again.
						r.log.Printf("function %q handler %q exhausted for event %q; skipping%s",
							pf.fn.Name, rule.Handler, msgID,
							logging.Fields(
								"function", pf.fn.Name,
								"handler", rule.Handler,
								"message_id", msgID,
								"event_id", eventID,
								"event_name", eventName,
								"attempt", n,
							))
					}
					continue
				}
				attempt = n
			}
			executed = true
			r.log.Printf("function %q rule %q matched event %q%s", pf.fn.Name, rule.Handler, msgID,
				logging.Fields(
					"function", pf.fn.Name,
					"handler", rule.Handler,
					"message_id", msgID,
					"event_id", eventID,
					"event_name", eventName,
					"attempt", attempt,
				))
			eventJSON, err := json.Marshal(event)
			if err != nil {
				// The invocation was already claimed (TryStart above) but will not
				// execute: this is a failed attempt, so schedule a retry with the
				// same backoff rules as an execution failure (it IS a failed
				// attempt). If the attempt is exhausted, mark it terminal.
				if hasState {
					return r.recordFailure(
						invState,
						invocation,
						attempt,
						rule.Retries,
						matched,
						pf.fn.Name,
						rule.Handler,
						msgID,
						eventID,
						eventName,
						err,
					)
				}
				return fmt.Errorf("function %q handler %q: marshal event: %w", pf.fn.Name, rule.Handler, err)
			}
			// Resolve the template's env values and secret references into the
			// per-invocation extra env, immediately before container creation.
			// Secret values are resolved per execution (never cached on Prepared,
			// never in the fingerprint), so rotating a secret value never requires
			// a rebuild. A resolution failure is a failed attempt (the invocation
			// was already claimed by TryStart), so it flows through the same
			// retry/exhaustion machinery as an execution failure. The error names
			// the secret REFERENCE only — never any value.
			extraEnv, err := r.resolveExtraEnv(ctx, pf.fn.Template)
			if err != nil {
				if hasState {
					return r.recordFailure(
						invState,
						invocation,
						attempt,
						rule.Retries,
						matched,
						pf.fn.Name,
						rule.Handler,
						msgID,
						eventID,
						eventName,
						err,
					)
				}
				return fmt.Errorf("function %q handler %q: %w", pf.fn.Name, rule.Handler, err)
			}
			invokeCtx, cancel := context.WithTimeout(ctx, timeout)
			// Stamp the invocation's diagnostic metadata into the context so the
			// executor can attach it as container labels. This keeps the
			// Executor interface (and every test fake) unchanged.
			invokeCtx = runtime.WithRunMeta(invokeCtx, runtime.RunMeta{
				Function:  pf.fn.Name,
				Handler:   rule.Handler,
				MessageID: msgID,
				EventID:   eventID,
				EventName: eventName,
				Hostname:  r.hostname,
				Image:     toImage(pf),
			})
			start := time.Now()
			err, panicked, panicValue := r.runInvocation(pf, invokeCtx, cancel, rule.Handler, eventJSON, extraEnv)
			d := time.Since(start)
			if panicked {
				// A panicking execution is a misbehaving handler, not a healthy
				// failure: log the panic value and the full stack so the bug is
				// visible and attributable, then funnel it through the SAME
				// failure branch below (metrics + recordFailure) so retry and
				// exhaustion accounting stay per-invocation. The stack is kept in
				// the message body (multi-line) since logging.Fields values must
				// stay single-line.
				r.log.Printf("function %q handler %q PANICKED for event %q: %v\n%s%s",
					pf.fn.Name, rule.Handler, msgID, panicValue, debug.Stack(),
					logging.Fields(
						"function", pf.fn.Name,
						"handler", rule.Handler,
						"message_id", msgID,
						"event_id", eventID,
						"event_name", eventName,
						"attempt", attempt,
					))
			}
			if err != nil {
				r.metrics.IncLabels("handler_invocations_total",
					[]metrics.Label{
						{Name: "outcome", Value: "failure"},
						{Name: "function", Value: pf.fn.Name},
						{Name: "handler", Value: rule.Handler},
					})
				// Unlabeled total for the SQLite snapshot; the labeled counter
				// above stays for Prometheus.
				r.metrics.Inc("handler_failure_total")
				// Per-function failure attribution (per rule execution).
				r.metrics.IncLabels("function_handler_failure_total",
					[]metrics.Label{{Name: "function", Value: pf.fn.Name}})
				r.metrics.ObserveDurationLabels("handler_duration_seconds",
					[]metrics.Label{
						{Name: "function", Value: pf.fn.Name},
						{Name: "handler", Value: rule.Handler},
					}, d)
				r.log.Printf("function %q handler %q execution failed for event %q: %v%s", pf.fn.Name, rule.Handler, msgID, err,
					logging.Fields(
						"function", pf.fn.Name,
						"handler", rule.Handler,
						"message_id", msgID,
						"event_id", eventID,
						"event_name", eventName,
						"attempt", attempt,
						"duration", d,
					))
				// Record the failure and decide retry vs exhaustion. This is the
				// per-invocation retry driver: a retryable failure schedules a
				// backoff and counts function_retries_total; an exhausted attempt
				// marks the invocation terminal and, when the whole message is
				// terminal, routes it to the DLQ.
				if hasState {
					return r.recordFailure(invState, invocation, attempt, rule.Retries, matched, pf.fn.Name, rule.Handler, msgID, eventID, eventName, err)
				}
				// No invocation state (direct callers/tests): every failure counts
				// as a retry driver, but there is no Redis-backed attempt count to
				// decide exhaustion — so no DLQ attribution here either. The DLQ
				// metric is only meaningful with invocation state, where
				// exhaustion is actually persisted and observable.
				r.metrics.IncLabels("function_retries_total",
					[]metrics.Label{{Name: "function", Value: pf.fn.Name}})
				return err
			}
			// Record the invocation as completed so a redelivery skips it. This
			// happens BEFORE the success metrics so a crash between the side
			// effect and MarkComplete re-runs the handler (at-least-once; the
			// handler must remain idempotent). A mark failure is logged by the
			// handle and does not fail the invocation.
			if hasState {
				invState.MarkComplete(invocation)
			}
			r.metrics.IncLabels("handler_invocations_total",
				[]metrics.Label{
					{Name: "outcome", Value: "success"},
					{Name: "function", Value: pf.fn.Name},
					{Name: "handler", Value: rule.Handler},
				})
			// Unlabeled total for the SQLite snapshot; the labeled counter above
			// stays for Prometheus.
			r.metrics.Inc("handler_success_total")
			// Per-function success attribution (per rule execution).
			r.metrics.IncLabels("function_handler_success_total",
				[]metrics.Label{{Name: "function", Value: pf.fn.Name}})
			r.metrics.ObserveDurationLabels("handler_duration_seconds",
				[]metrics.Label{
					{Name: "function", Value: pf.fn.Name},
					{Name: "handler", Value: rule.Handler},
				}, d)
			r.log.Printf("function %q handler %q executed for event %q%s", pf.fn.Name, rule.Handler, msgID,
				logging.Fields(
					"function", pf.fn.Name,
					"handler", rule.Handler,
					"message_id", msgID,
					"event_id", eventID,
					"event_name", eventName,
					"attempt", attempt,
					"duration", d,
				),
			)
		}
	}
	// Return contract: nil when something executed successfully (or nothing
	// matched); ErrInvocationNotEligible when nothing executed and at least one
	// matched invocation is protected (running or waiting out a retry backoff).
	if executed {
		return nil
	}
	if skippedPending {
		return stream.ErrInvocationNotEligible
	}
	return nil
}

// recordFailure handles a failed invocation attempt: it decides whether the
// attempt is retryable or exhausted, updates the invocation state and metrics
// accordingly, and returns the error Handle should propagate. It is used for
// both execution failures and marshal failures (both are failed attempts).
//
// A retryable attempt (attempt < 1+retries) schedules a retry backoff via
// RecordFailure and counts function_retries_total. An exhausted attempt
// (attempt >= 1+retries) marks the invocation terminal via MarkExhausted; if
// every other matched invocation is also terminal, the whole message is
// terminal and the returned error wraps stream.ErrInvocationExhausted so the
// stream layer routes it to the DLQ. Otherwise the plain error is returned so
// the message stays pending and the other invocations continue.
func (r *Runner) recordFailure(
	invState stream.InvocationState,
	invocation string,
	attempt int,
	retries int,
	matched []string,
	fnName, handler, msgID, eventID, eventName string,
	origErr error,
) error {
	maxAttempts := 1 + retries
	if attempt >= maxAttempts {
		// Exhausted: mark the invocation terminal.
		invState.MarkExhausted(invocation, attempt)
		r.metrics.IncLabels("function_dlq_total",
			[]metrics.Label{{Name: "function", Value: fnName}})
		r.log.Printf("function %q handler %q exhausted after %d/%d attempts for event %q%s",
			fnName, handler, attempt, maxAttempts, msgID,
			logging.Fields(
				"function", fnName,
				"handler", handler,
				"message_id", msgID,
				"event_id", eventID,
				"event_name", eventName,
				"attempt", attempt,
				"attempts_total", maxAttempts,
			))
		// If every matched invocation is now terminal, the message is terminal
		// and must be routed to the DLQ. Otherwise leave it pending so the other
		// invocations continue.
		if allMatchedTerminal(invState, matched) {
			return fmt.Errorf("%w: function %q handler %q exhausted after %d attempts: %w", stream.ErrInvocationExhausted, fnName, handler, attempt, origErr)
		}
		return fmt.Errorf("function %q handler %q exhausted after %d attempts: %w", fnName, handler, attempt, origErr)
	}
	// Retryable: schedule a retry backoff and count the retry.
	backoff := retryBackoff(attempt)
	invState.RecordFailure(invocation, backoff)
	r.metrics.IncLabels("function_retries_total",
		[]metrics.Label{{Name: "function", Value: fnName}})
	r.log.Printf("function %q handler %q failed attempt %d/%d for event %q; retrying in %s%s",
		fnName, handler, attempt, maxAttempts, msgID, backoff,
		logging.Fields(
			"function", fnName,
			"handler", handler,
			"message_id", msgID,
			"event_id", eventID,
			"event_name", eventName,
			"attempt", attempt,
			"attempts_total", maxAttempts,
			"retry_delay", backoff,
		))
	return fmt.Errorf("function %q handler %q: attempt %d failed: %w", fnName, handler, attempt, origErr)
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
// for structured logging. These are never used as metric labels.
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

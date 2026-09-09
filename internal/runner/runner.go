package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"relay/internal/function"
	"relay/internal/logging"
	"relay/internal/metrics"
	"relay/internal/runtime"
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
	r := &Runner{reg: &Registry{}, log: logger, metrics: m}
	r.reg.Set(prepared)
	return r
}

// Registry exposes the runner's mutable snapshot set so the reconciler can swap
// functions live without round-tripping through New.
func (r *Runner) Registry() *Registry { return r.reg }

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
// delivery. Skipped invocations are not executions: they do not touch the
// handler_* or function_handler_* metrics. function_events_total still counts
// the function as engaged (it matched), which is attribution, not execution
// counting. When no invocation state is present (direct Handle callers/tests,
// or invocation tracking disabled) Handle behaves exactly as before.
func (r *Runner) Handle(ctx context.Context, msgID string, event map[string]any) error {
	// A message received at the runner is one logical event handled across all
	// matching rules. This is the message-level counter.
	r.metrics.Inc("events_received_total")
	// Best-effort delivery attempt, defaulting to 1 when the stream did not set
	// it (e.g. when the runner is driven directly in tests).
	attempt := stream.DeliveryAttemptFrom(ctx)
	// Best-effort invocation state, absent when the stream did not inject it
	// (direct Handle callers/tests, or invocation tracking disabled).
	invState, hasState := stream.InvocationStateFrom(ctx)

	// Take one consistent snapshot for the whole call so a concurrent registry
	// swap mid-execution cannot reorder or drop functions under us.
	for _, pf := range r.reg.snapshot() {
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
						"attempt", attempt,
					))
				continue
			}
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
				return fmt.Errorf("function %q handler %q: marshal event: %w", pf.fn.Name, rule.Handler, err)
			}
			invokeCtx, cancel := context.WithTimeout(ctx, rule.Timeout)
			start := time.Now()
			err = pf.executor.Execute(invokeCtx, pf.prepared, rule.Handler, eventJSON)
			d := time.Since(start)
			cancel()
			if err != nil {
				r.metrics.IncLabels("handler_invocations_total",
					[]metrics.Label{
						{Name: "outcome", Value: "failure"},
						{Name: "function", Value: pf.fn.Name},
						{Name: "handler", Value: rule.Handler},
					})
				// Unlabeled total feeding the SQLite snapshot; the labeled counter
				// above remains for Prometheus, this one is simpler to aggregate.
				r.metrics.Inc("handler_failure_total")
				// Per-function failure attribution (per rule execution).
				r.metrics.IncLabels("function_handler_failure_total",
					[]metrics.Label{{Name: "function", Value: pf.fn.Name}})
				// A failing rule execution is a retry driver: the message will be
				// retried or, once attempts are exhausted, routed to the DLQ. Both
				// are downstream of this failure, so every failure counts here.
				// This is the per-function retry driver, distinct from the
				// message-level retries_total (stream layer), which counts
				// redelivery/retry events once per message.
				r.metrics.IncLabels("function_retries_total",
					[]metrics.Label{{Name: "function", Value: pf.fn.Name}})
				// When this failure is the one that exhausts the delivery attempts
				// (attempt >= stream.DefaultMaxAttempts), the message is routed to
				// the DLQ. This mirrors the stream layer's DLQ threshold; the
				// authoritative global count remains dlq_entries_total.
				if attempt >= stream.DefaultMaxAttempts {
					r.metrics.IncLabels("function_dlq_total",
						[]metrics.Label{{Name: "function", Value: pf.fn.Name}})
				}
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
			// Unlabeled total feeding the SQLite snapshot; the labeled counter
			// above remains for Prometheus, this one is simpler to aggregate.
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
	return nil
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

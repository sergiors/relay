package runtime

import "context"

// HandlerContext is the per-invocation execution metadata Relay carries to the
// handler as the request frame's "context" object. It is deliberately separate
// from RunMeta: RunMeta is diagnostic container metadata (Docker labels and the
// output prefix), while HandlerContext describes the execution the handler is
// running and is visible across the container protocol boundary.
//
// The identity fields have distinct contracts:
//   - RequestID is stable for one logical invocation. A broker-backed execution
//     derives it from the source message identity and the invocation's
//     app/handler, so retries, redeliveries, and worker restarts compute the
//     same value. A state-free execution (manual invocation, DLQ replay, direct
//     caller) uses a fresh, unique value.
//   - Attempt is the 1-based handler attempt for a broker-backed claim, or 1 for
//     a state-free execution.
//   - MaxAttempts is the configured retry budget as a total attempt count
//     (initial attempt plus retries). It is never raised to Attempt: a config
//     change can leave the claimed attempt above the current budget.
//   - MessageID is the source stream message ID when known, including a
//     state-free delivery or DLQ replay; it is empty when no source identity is
//     available (for example, a manual invocation).
//
// The effective deadline is NOT stored here: it is the invocation context's
// deadline, read at frame-encoding time so it always matches the context that
// actually cancels the execution. The trace ID is likewise derived from the
// trace context carried by the invocation context.
type HandlerContext struct {
	RequestID   string
	Attempt     int
	MaxAttempts int
	MessageID   string
	App         string
	Handler     string
}

// handlerContextKey is the unexported context key carrying the per-invocation
// HandlerContext into the executor, so the runtime package owns the contract.
type handlerContextKey struct{}

// WithHandlerContext returns a child of ctx carrying the invocation's handler
// metadata. The runner injects it before calling the executor (alongside
// WithRunMeta), so the request frame can carry it without widening the Executor
// interface.
func WithHandlerContext(ctx context.Context, hc HandlerContext) context.Context {
	return context.WithValue(ctx, handlerContextKey{}, hc)
}

// HandlerContextFrom returns the HandlerContext carried in ctx, or the zero
// value when absent. It is best-effort context: an absent value is normalized
// into a complete state-free execution context by Manager.Execute and the frame
// encoder.
func HandlerContextFrom(ctx context.Context) HandlerContext {
	hc, _ := ctx.Value(handlerContextKey{}).(HandlerContext)
	return hc
}

// normalized returns hc with the complete execution identity. The concrete
// handler being invoked wins when non-empty, as does the owning app when known
// at the call site. A missing request ID becomes a fresh state-free identity,
// and a missing budget becomes the single initial attempt. A supplied attempt
// is never raised or lowered and the budget is never clamped up to it: a config
// change can legitimately leave a claimed attempt above the configured budget,
// and the handler must still see the budget that applies.
func (hc HandlerContext) normalized(appFallback, handler string) HandlerContext {
	if handler != "" {
		hc.Handler = handler
	}
	if appFallback != "" {
		hc.App = appFallback
	}
	if hc.RequestID == "" {
		hc.RequestID = newRequestID()
	}
	if hc.Attempt < 1 {
		hc.Attempt = 1
	}
	if hc.MaxAttempts < 1 {
		hc.MaxAttempts = 1
	}
	return hc
}

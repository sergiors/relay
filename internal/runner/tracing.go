package runner

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"relay/internal/observability/tracing"
	"relay/internal/stream"
)

// invocationTrace is the durable trace-identity seam for one invocation attempt:
// the invocation-state handle, the invocation's stable "<app>/<handler>"
// ID, and the 1-based handler attempt (0 when unknown, e.g. a state-free
// manual/replay caller). It lets runInvocation read the trace lineage recorded by
// the invocation's PREVIOUS attempt and link the new function.invoke span to it,
// then persist the current attempt's span context so a later retry (even on a
// restarted worker) can link back to it.
//
// A zero value (nil state and/or empty invocation) makes every read and write a
// no-op, so state-free callers (manual invocation, DLQ replay, direct callers)
// never touch broker state. Only the compact lineage (traceparent/tracestate) is
// ever persisted; baggage is deliberately excluded (see
// tracing.SpanContextToString).
type invocationTrace struct {
	state      stream.InvocationState
	invocation string
	// attempt is the 1-based handler attempt for this execution (0 when there is
	// no invocation state to attribute one). It is emitted as the
	// low-cardinality function.attempt span attribute; 0 omits it.
	attempt int
}

// reference returns the compact serialized trace lineage recorded by the
// invocation's previous attempt, or "" when there is none (or the seam is a
// no-op). It is best-effort: a store error is already logged by the handle and
// reported as no reference.
func (t invocationTrace) reference() string {
	if t.state == nil || t.invocation == "" {
		return ""
	}
	return t.state.TraceReference(t.invocation)
}

// record persists the current attempt's span context as the invocation's lineage
// so a later retry links back to it. It is a no-op for an invalid span context
// (tracing disabled) or a zero seam, and best-effort otherwise (a write error is
// logged by the handle and never fails the invocation).
func (t invocationTrace) record(sc trace.SpanContext) {
	if t.state == nil || t.invocation == "" {
		return
	}
	if lineage := tracing.SpanContextToString(sc); lineage != "" {
		t.state.RecordTrace(t.invocation, lineage)
	}
}

// spanOpts returns the per-attempt span-start options for the invocation span:
// the retry link to the previous attempt's stored lineage (when valid) and the
// low-cardinality function.attempt attribute (when a handler attempt is known).
// The retry link is not a parent: every attempt is its own function.invoke span.
func (t invocationTrace) spanOpts() []trace.SpanStartOption {
	opts := t.retryLink()
	if t.attempt >= 1 {
		opts = append(opts, trace.WithAttributes(attribute.Int("function.attempt", t.attempt)))
	}
	return opts
}

// retryLink returns the span-link option that links a new attempt's
// function.invoke span to the invocation's stored previous-attempt lineage, or
// nil when there is no valid reference (first attempt, tracing disabled, or a
// state-free caller). Reading an unknown/malformed stored value yields no link
// rather than an error, so a corrupt lineage never fails an invocation.
func (t invocationTrace) retryLink() []trace.SpanStartOption {
	link, ok := tracing.SpanContextFromString(t.reference())
	if !ok {
		return nil
	}
	return []trace.SpanStartOption{trace.WithLinks(trace.Link{SpanContext: link})}
}

// startInvocationSpan begins the end-to-end `function.invoke` span shared by
// every runner invocation path (event rule, schedule occurrence, manual
// invocation). The attributes are low-cardinality configuration identities: the
// app name, the handler, and the declared runtime. The event payload is
// never attached. Extra opts let a caller attach a retry span link (and any
// further per-attempt option) without changing the shared attribute set.
func startInvocationSpan(
	ctx context.Context,
	fnName, handler, runtimeName string,
	opts ...trace.SpanStartOption,
) (context.Context, trace.Span) {
	base := []trace.SpanStartOption{
		trace.WithAttributes(
			attribute.String("relay.app.name", fnName),
			attribute.String("relay.handler.name", handler),
			attribute.String("relay.app.runtime", runtimeName),
		),
	}
	return tracing.Start(ctx, "function.invoke", append(base, opts...)...)
}

// finishInvocationSpan records the invocation result and ends the span. A
// failure records the error and sets codes.Error; success sets codes.Ok. The
// result attribute is the low-cardinality "success"/"failure" outcome.
func finishInvocationSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(attribute.String("function.result", "failure"))
		span.End()
		return
	}
	span.SetStatus(codes.Ok, "")
	span.SetAttributes(attribute.String("function.result", "success"))
	span.End()
}

// startReplaySpan begins the worker-side `dlq.replay` root operation span around
// a DLQ replay. It is always a NEW ROOT (trace.WithNewRoot) so the replay is a
// distinct operation regardless of the caller's context, and it carries a link
// to the original failed invocation when the persisted DLQ lineage is valid.
// `function.invoke` runs as its child.
func startReplaySpan(ctx context.Context, fnName, handler, lineage string) (context.Context, trace.Span) {
	opts := []trace.SpanStartOption{
		trace.WithNewRoot(),
		trace.WithAttributes(
			attribute.String("relay.app.name", fnName),
			attribute.String("relay.handler.name", handler),
		),
	}
	if link, ok := tracing.SpanContextFromString(lineage); ok {
		opts = append(opts, trace.WithLinks(trace.Link{SpanContext: link}))
	}
	return tracing.Start(ctx, "dlq.replay", opts...)
}

// startManualInvokeSpan begins the worker-side `app.manual_invoke` root
// operation span around a manual `relay app invoke`. It is always a NEW
// ROOT so an operator invocation is traced on the worker without requiring the
// CLI to carry trace context. Every matching rule's `function.invoke` runs as
// its child.
func startManualInvokeSpan(ctx context.Context, fnName string) (context.Context, trace.Span) {
	return tracing.Start(ctx, "app.manual_invoke",
		trace.WithNewRoot(),
		trace.WithAttributes(attribute.String("relay.app.name", fnName)),
	)
}

// finishOperationSpan records the terminal outcome of a worker-side operation
// span (dlq.replay, app.manual_invoke): an error records the error and sets
// codes.Error, success sets codes.Ok. The result attribute is the
// low-cardinality "success"/"failure" outcome.
func finishOperationSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(attribute.String("function.result", "failure"))
		span.End()
		return
	}
	span.SetStatus(codes.Ok, "")
	span.SetAttributes(attribute.String("function.result", "success"))
	span.End()
}

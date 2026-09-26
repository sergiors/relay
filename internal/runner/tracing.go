package runner

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"relay/internal/observability/tracing"
)

// startInvocationSpan begins the end-to-end `function.invoke` span shared by
// every runner invocation path (event rule, schedule occurrence, manual
// invocation). The attributes are low-cardinality configuration identities:
// the function name, the handler, and the declared runtime. The event payload
// is never attached.
func startInvocationSpan(
	ctx context.Context,
	fnName, handler, runtimeName string,
) (context.Context, trace.Span) {
	return tracing.Start(ctx, "function.invoke",
		trace.WithAttributes(
			attribute.String("function.name", fnName),
			attribute.String("function.handler", handler),
			attribute.String("function.runtime", runtimeName),
		),
	)
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

package runtime

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"relay/internal/observability/tracing"
)

// startRuntimeSpan begins a runtime-layer span. The attributes are the function
// name and the image reference (both bounded, configuration-level identities),
// never the handler payload.
func startRuntimeSpan(ctx context.Context, name, fnName, image string) (context.Context, trace.Span) {
	return tracing.Start(ctx, name,
		trace.WithAttributes(
			attribute.String("relay.function", fnName),
			attribute.String("relay.image", image),
		),
	)
}

// finishRuntimeSpan records the terminal result and ends the span. A panic that
// unwinds through an execution is recovered by the runner's panic boundary and
// recorded on the enclosing function.invoke span, so these runtime spans record
// only the error return; keeping the finalizer single-purpose avoids a second
// recover layer here.
func finishRuntimeSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

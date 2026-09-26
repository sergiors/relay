package stream

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"

	"relay/internal/observability/tracing"
)

// messageSpan starts a processing span for one stream message. The attributes
// are deliberately low-cardinality: the stream and group (configuration), the
// delivery attempt, and the operation. The message id and the event payload are
// deliberately NOT attached (the id is unbounded per message; the payload may
// carry secrets).
func (c *Consumer) messageSpan(
	ctx context.Context,
	name string,
	deliveryAttempt int64,
) (context.Context, trace.Span) {
	return tracing.Start(ctx, name,
		trace.WithAttributes(
			semconv.MessagingSystemKey.String("redis"),
			semconv.MessagingDestinationName(c.stream),
			semconv.MessagingOperationTypeKey.String("process"),
			attribute.String("messaging.consumer.group.name", c.group),
			attribute.Int64("relay.delivery_attempt", deliveryAttempt),
		),
	)
}

// finishMessageSpan records the terminal outcome and ends a message span. A
// genuine failure records the error and sets codes.Error; the pending, ack, and
// dlq outcomes are recorded as a low-cardinality relay.outcome attribute so the
// delivery result is inspectable without any payload.
func finishMessageSpan(span trace.Span, outcome string, err error) {
	span.SetAttributes(attribute.String("relay.outcome", outcome))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

// redisSpan starts a child span around a meaningful Redis operation (an ACK or a
// DLQ write), not every helper. It inherits the current trace context so the
// Redis operation is a child of the message-processing span.
func redisSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	return tracing.Start(ctx, name)
}

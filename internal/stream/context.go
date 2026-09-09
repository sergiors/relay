package stream

import "context"

// deliveryAttemptKey is the context key carrying the current delivery attempt
// number into the Handler. It is unexported so the stream package owns the
// contract; the runner reads it best-effort via DeliveryAttemptFrom.
type deliveryAttemptKey struct{}

// WithDeliveryAttempt returns a child of ctx carrying the current delivery
// attempt number. The stream layer sets this before invoking the Handler so the
// runner can attribute retries without changing the Handler signature.
func WithDeliveryAttempt(ctx context.Context, attempt int64) context.Context {
	return context.WithValue(ctx, deliveryAttemptKey{}, attempt)
}

// DeliveryAttemptFrom returns the delivery attempt number carried in ctx, or 1
// if absent/unset. Callers must treat the value as best-effort context: it never
// panics and never returns a value below 1.
func DeliveryAttemptFrom(ctx context.Context) int64 {
	if n, ok := ctx.Value(deliveryAttemptKey{}).(int64); ok && n >= 1 {
		return n
	}
	return 1
}

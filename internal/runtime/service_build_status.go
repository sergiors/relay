package runtime

import "context"

type serviceBuildObserverKey struct{}

// WithServiceBuildObserver carries the focused lifecycle callback to the
// service source resolver without a global hook, so independent functions can
// build concurrently.
func WithServiceBuildObserver(ctx context.Context, fn func()) context.Context {
	return context.WithValue(ctx, serviceBuildObserverKey{}, fn)
}

func ServiceBuildObserverFromContext(ctx context.Context) func() {
	fn, _ := ctx.Value(serviceBuildObserverKey{}).(func())
	return fn
}

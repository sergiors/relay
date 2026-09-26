package runtime

import "context"

// This file holds the focused lifecycle-callback context seams the reconciler
// and worker use to publish the persisted build status at the ACTUAL image-build
// boundary. Carrying the callback on the context (rather than a global hook)
// keeps independent functions building concurrently without a shared registry.

type serviceBuildObserverKey struct{}

// WithServiceBuildObserver carries the focused service-Dockerfile build callback
// to the service source resolver without a global hook, so independent functions
// can build concurrently. It fires immediately before a `build` source's
// Dockerfile build is issued, and never for a resolve that reuses an existing
// image.
func WithServiceBuildObserver(ctx context.Context, fn func()) context.Context {
	return context.WithValue(ctx, serviceBuildObserverKey{}, fn)
}

// ServiceBuildObserverFromContext returns the service build callback carried by
// ctx, or nil.
func ServiceBuildObserverFromContext(ctx context.Context) func() {
	fn, _ := ctx.Value(serviceBuildObserverKey{}).(func())
	return fn
}

type functionBuildObserverKey struct{}

// WithFunctionBuildObserver carries the focused managed-runtime function-image
// build callback into Manager.Prepare, so the caller can publish the building
// status only when an image build is ACTUALLY issued — never when Prepare merely
// reuses an existing local image or is a no-op for a template that needs no
// runtime. It coexists with the service observer: the two are independent
// because a function image build and a service image build happen on separate
// paths.
func WithFunctionBuildObserver(ctx context.Context, fn func()) context.Context {
	return context.WithValue(ctx, functionBuildObserverKey{}, fn)
}

// FunctionBuildObserverFromContext returns the function build callback carried
// by ctx, or nil.
func FunctionBuildObserverFromContext(ctx context.Context) func() {
	fn, _ := ctx.Value(functionBuildObserverKey{}).(func())
	return fn
}

// notifyFunctionBuild invokes the function build observer carried by ctx, if
// any. It is called at the exact boundary of a function or dependency image
// build — after the reuse probes — so a reused/no-op preparation never publishes
// a spurious building status.
func notifyFunctionBuild(ctx context.Context) {
	if fn := FunctionBuildObserverFromContext(ctx); fn != nil {
		fn()
	}
}

package runtime

import "context"

// This file holds the focused lifecycle-callback context seam the reconciler
// and worker use to publish the persisted build status at the ACTUAL managed
// runtime image-build boundary (an app image or a dependency image).
// Carrying the callback on the context (rather than a global hook) keeps
// independent apps building concurrently without a shared registry.

type appBuildObserverKey struct{}

// WithAppBuildObserver carries the focused managed-runtime app-image
// build callback into Manager.Prepare, so the caller can publish the building
// status only when an image build is ACTUALLY issued — never when Prepare merely
// reuses an existing local image or is a no-op for a template that needs no
// runtime.
func WithAppBuildObserver(ctx context.Context, fn func()) context.Context {
	return context.WithValue(ctx, appBuildObserverKey{}, fn)
}

// AppBuildObserverFromContext returns the app build callback carried
// by ctx, or nil.
func AppBuildObserverFromContext(ctx context.Context) func() {
	fn, _ := ctx.Value(appBuildObserverKey{}).(func())
	return fn
}

// notifyAppBuild invokes the app build observer carried by ctx, if
// any. It is called at the exact boundary of an app or dependency image
// build — after the reuse probes — so a reused/no-op preparation never publishes
// a spurious building status.
func notifyAppBuild(ctx context.Context) {
	if fn := AppBuildObserverFromContext(ctx); fn != nil {
		fn()
	}
}

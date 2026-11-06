//go:build integration

package runtime

// Docker-backed pin coverage: a REAL build from a pinned multi-platform index
// reference. The pin digest comes from daemon-local image metadata (this
// daemon's containerd image store exposes the loaded index digest) with Docker's
// DistributionInspect as the registry-side fallback — the TEST resolves the
// digest; production never performs a registry lookup (Prepare renders the pin
// verbatim into FROM).

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/moby/moby/client"

	"relay/internal/testutil"
)

// pinnedIndexDigest resolves the multi-platform index digest the test pins for
// baseRef. It prefers the local image's descriptor (an offline, daemon-local
// resolution — the digest is exactly the image the build will consume) and
// falls back to Docker's DistributionInspect with the registry when the local
// metadata does not carry a descriptor (an older graphdriver daemon).
func pinnedIndexDigest(t *testing.T, ctx context.Context, cl *client.Client, baseRef string) string {
	t.Helper()
	if insp, err := cl.ImageInspect(ctx, baseRef); err == nil && insp.Descriptor != nil && insp.Descriptor.Digest != "" {
		if mediaType := string(insp.Descriptor.MediaType); mediaType != "" && !isIndexMediaType(mediaType) {
			t.Fatalf("local descriptor of %s is %q rather than an image index", baseRef, mediaType)
		}
		return string(insp.Descriptor.Digest)
	}
	dist, err := cl.DistributionInspect(ctx, baseRef, client.DistributionInspectOptions{})
	if err != nil {
		t.Fatalf("resolve the pinned index digest for %s: the base image must exist locally "+
			"or be resolvable via the daemon's distribution inspect: %v", baseRef, err)
	}
	return string(dist.Descriptor.Digest)
}

// isIndexMediaType reports whether mediaType is a multi-platform index/list
// media type (the shape the pins persist).
func isIndexMediaType(mediaType string) bool {
	switch mediaType {
	case "application/vnd.oci.image.index.v1+json",
		"application/vnd.docker.distribution.manifest.list.v2+json":
		return true
	default:
		return false
	}
}

// assertPinnedImageLabels asserts the provenance + ownership label set of a
// managed image built under a pin: the strict classification fields are
// untouched and the provenance records the runtime, the CONFIGURED source
// reference, the selected digest, and the explicit target platform.
func assertPinnedImageLabels(
	t *testing.T,
	labels map[string]string,
	runtimeName, sourceImage, digest, platform string,
) {
	t.Helper()
	if labels[labelProvenanceRuntime] != runtimeName {
		t.Errorf("relay.provenance.runtime = %q, want %q", labels[labelProvenanceRuntime], runtimeName)
	}
	if labels[labelProvenanceSourceImage] != sourceImage {
		t.Errorf("relay.provenance.source_image = %q, want the configured ref %q (never ref@digest)",
			labels[labelProvenanceSourceImage], sourceImage)
	}
	if labels[labelProvenanceDigest] != digest {
		t.Errorf("relay.provenance.digest = %q, want the selected index digest %q", labels[labelProvenanceDigest], digest)
	}
	if labels[labelProvenancePlatform] != platform {
		t.Errorf("relay.provenance.platform = %q, want the explicit target %q", labels[labelProvenancePlatform], platform)
	}
}

// assertBuiltPlatform pins the explicit build target on a REAL image: Linux at
// the architecture the worker resolves (on this suite's linux/arm64 daemon that
// is exactly linux/arm64).
func assertBuiltPlatform(t *testing.T, osName, architecture string) {
	t.Helper()
	if osName != "linux" || architecture != runtime.GOARCH {
		t.Fatalf("built image platform = %s/%s, want linux/%s (the explicit build target)",
			osName, architecture, runtime.GOARCH)
	}
}

// TestIntegrationPinnedAppBuildUsesPinnedIndexRefAndPlatform proves a pinned
// no-dependency app builds a REAL image FROM the pinned
// <configured-ref>@<sha256:index> multi-platform reference: the build succeeds
// only because the daemon resolved that exact index, the image inspects with
// the explicit target platform, and its labels carry the provenance plus the
// untouched ownership classification. Prepared.Fingerprint stays source-only.
func TestIntegrationPinnedAppBuildUsesPinnedIndexRefAndPlatform(t *testing.T) {
	cl := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Register the app-image cleanup FIRST so a mid-test failure never leaks
	// this test's image onto the shared daemon.
	t.Cleanup(cleanupImagePrefixes(cl, "relay-app-pin-no-deps-e2e:"))

	digest := pinnedIndexDigest(t, ctx, cl, "node:24-alpine")
	pin := RuntimeImageSelection{RuntimeID: "nodejs24.x", SourceImage: "node:24-alpine", Digest: digest}

	// Given: a dependency-less Node app prepared under the pin.
	dir, fn := nodeHandlerFixture(t, "pin-no-deps-e2e")
	mgr, err := newCoordinatedRealManager(t, testutil.DiscardLogger(), nil,
		WithRuntimeImageSelections([]RuntimeImageSelection{pin}))
	if err != nil {
		t.Fatalf("new pinned manager: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	prepared, err := mgr.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("pinned prepare: %v", err)
	}

	// Then: the pin-folded identity is a distinct tag whose fingerprint input is
	// still the source-only identity.
	sourceFP := fpOf(t, dir)
	if prepared.Fingerprint != sourceFP {
		t.Fatalf("Prepared.Fingerprint = %q, want the source-only %q", prepared.Fingerprint, sourceFP)
	}
	if want := ImageRef(fn.Name, imageIdentityWithSelection(
		sourceFP, imageSelection{sourceImage: "node:24-alpine", digest: digest}, "")); prepared.Image != want {
		t.Fatalf("Prepared.Image = %q, want the pin-folded reference %q", prepared.Image, want)
	}

	// Then: the image exists and inspects with the EXPLICIT target platform.
	insp, err := cl.ImageInspect(ctx, prepared.Image)
	if err != nil {
		t.Fatalf("inspect the pinned-built image: %v", err)
	}
	assertBuiltPlatform(t, insp.Os, insp.Architecture)
	if insp.Config == nil || insp.Config.Labels == nil {
		t.Fatalf("pinned-built image %s carries no config labels", prepared.Image)
	}
	labels := insp.Config.Labels
	if labels[labelType] != ImageTypeApp || labels[labelApp] != fn.Name {
		t.Fatalf("ownership labels changed under a pin: type=%q app=%q", labels[labelType], labels[labelApp])
	}
	if labels[labelFingerprint] != sourceFP {
		t.Errorf("relay.fingerprint = %q, want the source fingerprint %q", labels[labelFingerprint], sourceFP)
	}
	if labels[labelBootstrap] == "" {
		t.Errorf("relay.bootstrap = %q, want the bootstrap identity to survive the pin", labels[labelBootstrap])
	}
	assertPinnedImageLabels(t, labels, "nodejs24.x", "node:24-alpine", digest, targetPlatformLabel())
}

// TestIntegrationPinnedDependencyBuildUsesPinnedIndexRefAndSharesLayer proves a
// pinned dependency-bearing app builds its relay-dep-* layer FROM the pinned
// multi-platform index reference and the layer stays content-addressed and
// SHARED: a second preparation of identical manifests under the same pin
// derives the same reference and reuses (never rebuilds) the real image.
func TestIntegrationPinnedDependencyBuildUsesPinnedIndexRefAndSharesLayer(t *testing.T) {
	cl := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// App-image cleanup is registered FIRST; dependency layers are
	// content-addressed and shared daemon-wide and are intentionally retained.
	t.Cleanup(cleanupImagePrefixes(cl, "relay-app-pin-deps-e2e:", "relay-app-pin-deps-second:"))

	digest := pinnedIndexDigest(t, ctx, cl, "python:3.14-slim")
	pin := RuntimeImageSelection{RuntimeID: "python3.14", SourceImage: "python:3.14-slim", Digest: digest}
	sel := imageSelection{sourceImage: "python:3.14-slim", digest: digest}

	// Given: a Python app with a requirements layer, prepared under the pin.
	dir := t.TempDir()
	fn := pythonHandlerFixture(t, dir, "pin-deps-e2e", true)
	pinnedRef := expectedPinnedDependencyRef(t, fn, sel)
	unpinnedRef := expectedDependencyRef(t, fn)
	if pinnedRef == unpinnedRef {
		t.Fatal("fixture precondition: a pinned dependency reference must differ from the unpinned one")
	}

	mgr, err := newCoordinatedRealManager(t, testutil.DiscardLogger(), nil,
		WithRuntimeImageSelections([]RuntimeImageSelection{pin}))
	if err != nil {
		t.Fatalf("new pinned manager: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	prepared, err := mgr.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("pinned prepare: %v", err)
	}

	// Then: the layer is the folded, content-addressed reference and the
	// prepared app carries it (never the unpinned reference).
	if prepared.Dependency != pinnedRef {
		t.Fatalf("Prepared.Dependency = %q, want the pinned %q (unpinned would be %q)",
			prepared.Dependency, pinnedRef, unpinnedRef)
	}
	depInsp, err := cl.ImageInspect(ctx, pinnedRef)
	if err != nil {
		t.Fatalf("inspect the pinned dependency layer %s: %v", pinnedRef, err)
	}
	assertBuiltPlatform(t, depInsp.Os, depInsp.Architecture)
	if depInsp.Config == nil || depInsp.Config.Labels == nil {
		t.Fatalf("pinned dependency layer %s carries no config labels", pinnedRef)
	}
	depLabels := depInsp.Config.Labels
	if depLabels[labelType] != ImageTypeDependency || depLabels[labelRuntime] != "python3.14" {
		t.Fatalf("dependency ownership labels changed under a pin: type=%q runtime=%q",
			depLabels[labelType], depLabels[labelRuntime])
	}
	assertPinnedImageLabels(t, depLabels, "python3.14", "python:3.14-slim", digest, targetPlatformLabel())

	// Then: the app image builds FROM the pinned layer and carries the
	// provenance too.
	appInsp, err := cl.ImageInspect(ctx, prepared.Image)
	if err != nil {
		t.Fatalf("inspect the pinned-built app image: %v", err)
	}
	assertBuiltPlatform(t, appInsp.Os, appInsp.Architecture)
	if appInsp.Config == nil || appInsp.Config.Labels == nil {
		t.Fatalf("pinned-built app image %s carries no config labels", prepared.Image)
	}
	appLabels := appInsp.Config.Labels
	if appLabels[labelType] != ImageTypeApp || appLabels[labelApp] != fn.Name {
		t.Fatalf("app ownership labels changed under a pin: type=%q app=%q", appLabels[labelType], appLabels[labelApp])
	}
	assertPinnedImageLabels(t, appLabels, "python3.14", "python:3.14-slim", digest, targetPlatformLabel())

	// Then: a SECOND preparation with identical manifests under the same pin
	// derives the SAME reference and reuses the layer (no rebuild).
	firstDepID := depInsp.ID
	dir2 := t.TempDir()
	fn2 := pythonHandlerFixture(t, dir2, "pin-deps-second", true)
	if secondRef := expectedPinnedDependencyRef(t, fn2, sel); secondRef != pinnedRef {
		t.Fatalf("identical manifests under the same pin resolved %q, want %q", secondRef, pinnedRef)
	}
	mgr2, err := newCoordinatedRealManager(t, testutil.DiscardLogger(), nil,
		WithRuntimeImageSelections([]RuntimeImageSelection{pin}))
	if err != nil {
		t.Fatalf("new second pinned manager: %v", err)
	}
	t.Cleanup(func() { _ = mgr2.Close() })
	prepared2, err := mgr2.Prepare(ctx, fn2)
	if err != nil {
		t.Fatalf("second pinned prepare: %v", err)
	}
	if prepared2.Dependency != pinnedRef {
		t.Fatalf("second Prepared.Dependency = %q, want the shared %q", prepared2.Dependency, pinnedRef)
	}
	reinsp, err := cl.ImageInspect(ctx, pinnedRef)
	if err != nil {
		t.Fatalf("re-inspect the pinned dependency layer: %v", err)
	}
	if reinsp.ID != firstDepID {
		t.Errorf("the pinned layer was rebuilt instead of reused (before %s after %s)", firstDepID, reinsp.ID)
	}
}

package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/authconfig"
	"github.com/moby/moby/api/types/registry"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// TestValidateBaseImageResolutionAcceptsMultiPlatformIndexes pins the accept
// path of the update validation: an OCI image index or a Docker manifest list
// with a valid sha256 index digest is accepted with or without an
// Engine-provided platform list, because the index digest is what Relay
// persists.
func TestValidateBaseImageResolutionAcceptsMultiPlatformIndexes(t *testing.T) {
	for _, mediaType := range []string{
		"application/vnd.oci.image.index.v1+json",
		"application/vnd.docker.distribution.manifest.list.v2+json",
	} {
		resolution := BaseImageResolution{
			Ref:       "python:3.14-slim",
			Digest:    pinnedDigestA,
			MediaType: mediaType,
		}
		if err := ValidateBaseImageResolution("python:3.14-slim", resolution); err != nil {
			t.Fatalf("media type %s: validation failed: %v", mediaType, err)
		}
	}
}

// TestValidateBaseImageResolutionRejectsInvalidEngineMetadata covers every
// rejection reason the update path must fail closed on: metadata for another
// reference, digest shape problems, a missing or non-index media type, and an
// Engine-reported platform list that does not prove a multi-platform index.
func TestValidateBaseImageResolutionRejectsInvalidEngineMetadata(t *testing.T) {
	cases := []struct {
		name       string
		ref        string
		resolution BaseImageResolution
		wantInErr  string
	}{
		{
			name: "metadata for another reference",
			ref:  "node:24-alpine",
			resolution: BaseImageResolution{
				Ref: "node:26-alpine", Digest: pinnedDigestA,
				MediaType: mediaTypeOCIImageIndex,
			},
			wantInErr: `Engine metadata refers to "node:26-alpine"`,
		},
		{
			name: "digest without sha256 prefix",
			ref:  "node:24-alpine",
			resolution: BaseImageResolution{
				Ref: "node:24-alpine", Digest: "ed3b1a7f", MediaType: mediaTypeOCIImageIndex,
			},
			wantInErr: "must be sha256:<64 lowercase hex>",
		},
		{
			name: "digest too short",
			ref:  "node:24-alpine",
			resolution: BaseImageResolution{
				Ref: "node:24-alpine", Digest: "sha256:abcd", MediaType: mediaTypeOCIImageIndex,
			},
			wantInErr: "must be sha256:<64 lowercase hex>",
		},
		{
			name: "digest with uppercase hex",
			ref:  "node:24-alpine",
			resolution: BaseImageResolution{
				Ref: "node:24-alpine", Digest: "sha256:" + strings.ToUpper(repeatHex('a')),
				MediaType: mediaTypeOCIImageIndex,
			},
			wantInErr: "must be sha256:<64 lowercase hex>",
		},
		{
			name: "empty media type",
			ref:  "node:24-alpine",
			resolution: BaseImageResolution{
				Ref: "node:24-alpine", Digest: pinnedDigestA,
			},
			wantInErr: "no manifest media type",
		},
		{
			name: "platform-specific docker manifest",
			ref:  "node:24-alpine",
			resolution: BaseImageResolution{
				Ref: "node:24-alpine", Digest: pinnedDigestA,
				MediaType: "application/vnd.docker.distribution.manifest.v2+json",
			},
			wantInErr: "is not an OCI image index or Docker manifest list",
		},
		{
			name: "platform-specific OCI manifest",
			ref:  "node:24-alpine",
			resolution: BaseImageResolution{
				Ref: "node:24-alpine", Digest: pinnedDigestA,
				MediaType: "application/vnd.oci.image.manifest.v1+json",
			},
			wantInErr: "is not an OCI image index or Docker manifest list",
		},
		{
			name: "unknown media type",
			ref:  "node:24-alpine",
			resolution: BaseImageResolution{
				Ref: "node:24-alpine", Digest: pinnedDigestA, MediaType: "text/plain",
			},
			wantInErr: "is not an OCI image index or Docker manifest list",
		},
		{
			name: "single concrete platform",
			ref:  "node:24-alpine",
			resolution: BaseImageResolution{
				Ref: "node:24-alpine", Digest: pinnedDigestA,
				MediaType: mediaTypeOCIImageIndex,
				Platforms: []ocispec.Platform{{OS: "linux", Architecture: "amd64"}},
			},
			wantInErr: "1 distinct concrete platform(s)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateBaseImageResolution(tc.ref, tc.resolution)
			if err == nil {
				t.Fatalf("invalid resolution %+v was accepted", tc.resolution)
			}
			if !strings.Contains(err.Error(), tc.wantInErr) {
				t.Fatalf("error = %v, want it to mention %q", err, tc.wantInErr)
			}
		})
	}

	// An empty queried reference is rejected even with a well-formed resolution.
	resolution := BaseImageResolution{
		Ref: "", Digest: pinnedDigestA, MediaType: mediaTypeOCIImageIndex,
	}
	if err := ValidateBaseImageResolution("", resolution); err == nil ||
		!strings.Contains(err.Error(), "reference is empty") {
		t.Fatalf("empty ref validation err = %v, want the empty-reference error", err)
	}
}

// TestValidateBaseImageResolutionCountsDistinctConcretePlatforms pins the
// concrete-platform set semantics: OS+architecture+variant identity is
// lowercase and whitespace-trimmed, variants distinguish two platforms, and
// duplicate entries do not.
func TestValidateBaseImageResolutionCountsDistinctConcretePlatforms(t *testing.T) {
	platform := func(osName, arch, variant string) ocispec.Platform {
		return ocispec.Platform{OS: osName, Architecture: arch, Variant: variant}
	}
	cases := []struct {
		name      string
		platforms []ocispec.Platform
		wantValid bool
	}{
		{
			name:      "two distinct architectures are valid",
			platforms: []ocispec.Platform{platform("linux", "amd64", ""), platform("linux", "arm64", "v8")},
			wantValid: true,
		},
		{
			name:      "same architecture twice is a single platform",
			platforms: []ocispec.Platform{platform("linux", "amd64", ""), platform("linux", "amd64", "")},
			wantValid: false,
		},
		{
			name:      "variants distinguish two arm platforms",
			platforms: []ocispec.Platform{platform("linux", "arm", "v7"), platform("linux", "arm", "")},
			wantValid: true,
		},
		{
			name:      "case and whitespace do not create a second platform",
			platforms: []ocispec.Platform{platform("Linux", "AMD64", ""), platform("linux ", " amd64", "")},
			wantValid: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resolution := BaseImageResolution{
				Ref: "node:24-alpine", Digest: pinnedDigestA,
				MediaType: mediaTypeOCIImageIndex, Platforms: tc.platforms,
			}
			err := ValidateBaseImageResolution("node:24-alpine", resolution)
			if tc.wantValid && err != nil {
				t.Fatalf("platforms %+v rejected: %v", tc.platforms, err)
			}
			if !tc.wantValid && err == nil {
				t.Fatalf("platforms %+v accepted, want a platform-count rejection", tc.platforms)
			}
		})
	}
}

// TestValidateBaseImageResolutionIgnoresAttestationPlatformsForRequiredCount
// pins the platform-count invariant: Engine attestation descriptors
// ("unknown"/"unknown") are ignored for the required distinct-platform count,
// so an index listing one real platform alongside an attestation descriptor is
// REJECTED as a single-platform manifest, never accepted as "two platforms".
func TestValidateBaseImageResolutionIgnoresAttestationPlatformsForRequiredCount(t *testing.T) {
	resolution := BaseImageResolution{
		Ref: "node:24-alpine", Digest: pinnedDigestA,
		MediaType: mediaTypeOCIImageIndex,
		Platforms: []ocispec.Platform{
			{OS: "linux", Architecture: "amd64"},
			{OS: "unknown", Architecture: "unknown"}, // attestation descriptor
		},
	}
	err := ValidateBaseImageResolution("node:24-alpine", resolution)
	if err == nil {
		t.Fatal("one real platform plus an attestation descriptor was accepted; " +
			"documented contract: attestation entries are ignored for the count")
	}
	if !strings.Contains(err.Error(), "1 distinct concrete platform(s)") {
		t.Fatalf("err = %v, want the single-concrete-platform rejection", err)
	}
}

// distEngineResponse renders a /distribution/<ref>/json body for a scripted
// Engine: a descriptor plus an optional platform list.
func distEngineResponse(
	t *testing.T,
	mediaType string,
	digest digest.Digest,
	platforms []ocispec.Platform,
) string {
	t.Helper()
	body := struct {
		Descriptor ocispec.Descriptor `json:"Descriptor"`
		Platforms  []ocispec.Platform `json:"Platforms"`
	}{
		Descriptor: ocispec.Descriptor{MediaType: mediaType, Digest: digest, Size: 1024},
		Platforms:  platforms,
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal distribution inspect body: %v", err)
	}
	return string(raw)
}

// stubAuthProvider is a RegistryAuthProvider returning a fixed encoded header
// for every reference; it records what it was asked about. An empty encoded
// value means "resolved to anonymous access", like a registryauth.Store
// without configured credentials.
type stubAuthProvider struct {
	encoded  string
	err      error
	requests []string
}

func (s *stubAuthProvider) EncodedAuthFor(_ context.Context, imageRef string) (string, error) {
	s.requests = append(s.requests, imageRef)
	if s.err != nil {
		return "", s.err
	}
	return s.encoded, nil
}

// encodedBasicAuth renders the X-Registry-Auth header value Docker clients
// produce for a username/password pair.
func encodedBasicAuth(t *testing.T, username, password string) string {
	t.Helper()
	encoded, err := authconfig.Encode(registry.AuthConfig{
		Username:      username,
		Password:      password,
		ServerAddress: "https://index.docker.io/v1/",
	})
	if err != nil {
		t.Fatalf("encode registry auth: %v", err)
	}
	return encoded
}

// TestDistributionResolverResolvesValidatedIndexThroughEngine drives the real
// DistributionResolver against a scripted Engine API: the request goes to the
// Engine distribution endpoint for the exact configured reference, the returned
// metadata is echoed with the queried Ref so validation cannot be fooled, and a
// structurally valid multi-platform index resolution comes back.
func TestDistributionResolverResolvesValidatedIndexThroughEngine(t *testing.T) {
	const ref = "node:24-alpine"
	var requestedPath string
	cl := newScriptedDockerClient(t, dockerRoute{
		method: http.MethodGet,
		path:   "/distribution/" + ref + "/json",
		body: distEngineResponse(t, mediaTypeOCIImageIndex, digest.Digest(pinnedDigestA), []ocispec.Platform{
			{OS: "linux", Architecture: "amd64"},
			{OS: "linux", Architecture: "arm64", Variant: "v8"},
		}),
		onRequest: func(r *http.Request) { requestedPath = r.URL.Path },
	})
	resolver := NewDistributionResolver(cl, &stubAuthProvider{})

	got, err := resolver.ResolveBaseImage(context.Background(), ref)
	if err != nil {
		t.Fatalf("ResolveBaseImage: %v", err)
	}
	if !strings.HasSuffix(requestedPath, "/distribution/"+ref+"/json") {
		t.Fatalf("engine request path = %q, want the distribution endpoint for %q", requestedPath, ref)
	}
	if got.Ref != ref {
		t.Fatalf("resolution Ref = %q, want the queried reference %q", got.Ref, ref)
	}
	if got.Digest != pinnedDigestA {
		t.Fatalf("resolution Digest = %q, want the Engine-reported index digest", got.Digest)
	}
	if got.MediaType != mediaTypeOCIImageIndex {
		t.Fatalf("resolution MediaType = %q, want %q", got.MediaType, mediaTypeOCIImageIndex)
	}
	if len(got.Platforms) != 2 || got.Platforms[0].Architecture != "amd64" ||
		got.Platforms[1].Variant != "v8" {
		t.Fatalf("resolution Platforms = %+v, want the Engine-reported platform list", got.Platforms)
	}
}

// TestDistributionResolverSendsConfiguredRegistryAuthHeader pins the auth
// transport contract: the provider's encoded value is forwarded verbatim as
// the X-Registry-Auth request header, the provider is asked about exactly the
// reference Relay will query, and a nil provider means NO header at all
// (anonymous access) rather than an empty one.
func TestDistributionResolverSendsConfiguredRegistryAuthHeader(t *testing.T) {
	const ref = "python:3.14-slim"
	encoded := encodedBasicAuth(t, "relay-user", "relay-secret")
	var gotHeader []string
	provider := &stubAuthProvider{encoded: encoded}
	cl := newScriptedDockerClient(t, dockerRoute{
		method: http.MethodGet,
		path:   "/distribution/" + ref + "/json",
		body:   distEngineResponse(t, mediaTypeOCIImageIndex, digest.Digest(pinnedDigestB), nil),
		onRequest: func(r *http.Request) {
			gotHeader = r.Header.Values(registry.AuthHeader)
		},
	})
	resolver := NewDistributionResolver(cl, provider)

	if _, err := resolver.ResolveBaseImage(context.Background(), ref); err != nil {
		t.Fatalf("ResolveBaseImage with auth provider: %v", err)
	}
	if len(gotHeader) != 1 || gotHeader[0] != encoded {
		t.Fatalf("X-Registry-Auth header = %v, want exactly the provider's encoded value", gotHeader)
	}
	if len(provider.requests) != 1 || provider.requests[0] != ref {
		t.Fatalf("provider asked for %v, want only the queried reference %q",
			provider.requests, ref)
	}

	// Without any provider the request carries no auth header: anonymous access.
	anonymous := newScriptedDockerClient(t, dockerRoute{
		method: http.MethodGet,
		path:   "/distribution/" + ref + "/json",
		body:   distEngineResponse(t, mediaTypeOCIImageIndex, digest.Digest(pinnedDigestB), nil),
		onRequest: func(r *http.Request) {
			gotHeader = r.Header.Values(registry.AuthHeader)
		},
	})
	if _, err := NewDistributionResolver(anonymous, nil).
		ResolveBaseImage(context.Background(), ref); err != nil {
		t.Fatalf("anonymous ResolveBaseImage: %v", err)
	}
	if len(gotHeader) != 0 {
		t.Fatalf("anonymous request carried X-Registry-Auth %v, want no header", gotHeader)
	}
}

// TestDistributionResolverSurfacesEngineFailures pins the failure contracts of
// the resolver: an Engine error is wrapped with the queried reference, an auth
// provider error aborts before any Engine call, a canceled caller context
// cancels the Engine call, and an empty reference is rejected without touching
// the Engine.
func TestDistributionResolverSurfacesEngineFailures(t *testing.T) {
	const ref = "python:3.14-slim"

	// Engine rejects the reference: the error names the queried reference and
	// the Engine boundary, and stays wrapped for the caller.
	notFound := newScriptedDockerClient(t, dockerRoute{
		method: http.MethodGet,
		path:   "/distribution/",
		status: http.StatusNotFound,
		body:   `{"message":"no such image"}`,
	})
	_, err := NewDistributionResolver(notFound, nil).ResolveBaseImage(context.Background(), ref)
	if err == nil || !strings.Contains(err.Error(), "resolve base image ") ||
		!strings.Contains(err.Error(), "Docker Engine") {
		t.Fatalf("engine 404 error = %v, want the wrapped resolve error naming the Engine", err)
	}

	// The provider failing must abort BEFORE the resolver contacts the daemon:
	// the route list intentionally carries no distribution route, so any Engine
	// request would fail the test loudly instead of silently passing.
	providerErr := errors.New("credential store unavailable")
	failing := &stubAuthProvider{err: providerErr}
	noRoutes := newScriptedDockerClient(t) // only the ping routes
	_, err = NewDistributionResolver(noRoutes, failing).ResolveBaseImage(context.Background(), ref)
	if !errors.Is(err, providerErr) {
		t.Fatalf("provider error = %v, want the provider failure", err)
	}

	// A canceled caller context surfaces as a cancellation, not a generic error:
	// the scripted transport observes the caller's context on the real path.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	cancelledEngine := newScriptedDockerClient(t, dockerRoute{
		method: http.MethodGet,
		path:   "/distribution/",
		fail:   func(req *http.Request) error { return req.Context().Err() },
	})
	_, err = NewDistributionResolver(cancelledEngine, nil).ResolveBaseImage(cancelled, ref)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled-context error = %v, want context.Canceled", err)
	}

	// An empty reference never reaches the Engine.
	if _, err := NewDistributionResolver(noRoutes, nil).
		ResolveBaseImage(context.Background(), ""); err == nil ||
		!strings.Contains(err.Error(), "reference is empty") {
		t.Fatalf("empty ref error = %v, want the empty-reference error", err)
	}
}

// TestDistributionResolverRejectsPlatformSpecificEngineResponse pins that the
// resolver validates the Engine's answer itself: an Engine that reports a
// platform-specific manifest for a configured tag produces a validation error,
// never a resolution Relay could persist as a pin.
func TestDistributionResolverRejectsPlatformSpecificEngineResponse(t *testing.T) {
	const ref = "node:24-alpine"
	cl := newScriptedDockerClient(t, dockerRoute{
		method: http.MethodGet,
		path:   "/distribution/" + ref + "/json",
		body: distEngineResponse(t,
			"application/vnd.docker.distribution.manifest.v2+json", digest.Digest(pinnedDigestA), nil),
	})
	_, err := NewDistributionResolver(cl, nil).ResolveBaseImage(context.Background(), ref)
	if err == nil ||
		!strings.Contains(err.Error(), "is not an OCI image index or Docker manifest list") {
		t.Fatalf("platform-manifest error = %v, want the media-type rejection", err)
	}
}

// TestDistributionResolverCallIsBoundedByTheCallerDeadline pins that the
// caller's context bounds the Engine call: the scripted transport blocks until
// the deadline expires, so an Engine call that hangs is cancelled by context
// cancellation, not by Relay-side timing.
func TestDistributionResolverCallIsBoundedByTheCallerDeadline(t *testing.T) {
	const ref = "node:24-alpine"
	cl := newScriptedDockerClient(t, dockerRoute{
		method: http.MethodGet,
		path:   "/distribution/",
		fail: func(req *http.Request) error {
			<-req.Context().Done() // the caller's deadline cancels a hung Engine call
			return req.Context().Err()
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := NewDistributionResolver(cl, nil).ResolveBaseImage(ctx, ref)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline error = %v, want context.DeadlineExceeded", err)
	}
}

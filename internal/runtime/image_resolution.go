package runtime

import (
	"context"
	"fmt"
	"strings"

	"github.com/moby/moby/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// Multi-platform base-image media types accepted for a runtime image pin. Relay
// persists the digest of the INDEX, not a platform-specific child manifest: a
// worker builds every image for the local Linux architecture from
// "<configured-ref>@<index-digest>", so pinning a single-platform manifest would
// make the other supported architectures unavailable.
const (
	mediaTypeOCIImageIndex      = "application/vnd.oci.image.index.v1+json"
	mediaTypeDockerManifestList = "application/vnd.docker.distribution.manifest.list.v2+json"
)

// BaseImageResolution is the immutable registry metadata for one configured
// base-image reference, obtained through the Docker Engine Distribution API. Ref
// is the exact reference that was queried; Digest is the multi-platform index
// digest; MediaType is the descriptor's manifest media type; and Platforms is the
// Engine-reported platform list (possibly empty).
type BaseImageResolution struct {
	Ref       string
	Digest    string
	MediaType string
	Platforms []ocispec.Platform
}

// RegistryAuthProvider supplies the encoded X-Registry-Auth header for an image
// reference. An empty value means anonymous access. Implementations must not
// log, print, or include the returned credential in errors.
type RegistryAuthProvider interface {
	EncodedAuthFor(ctx context.Context, imageRef string) (string, error)
}

// DistributionResolver resolves a configured base-image reference through the
// Docker Engine's Distribution API. The Engine contacts the registry, so Relay
// keeps no registry HTTP client and no registry SDK; it never pulls the image.
// A nil auth provider means anonymous access.
type DistributionResolver struct {
	client *client.Client
	auth   RegistryAuthProvider
}

// NewDistributionResolver returns a resolver using the supplied Engine client.
// The caller owns the client lifecycle and must close it when done.
func NewDistributionResolver(cli *client.Client, auth RegistryAuthProvider) *DistributionResolver {
	return &DistributionResolver{client: cli, auth: auth}
}

// ResolveBaseImage queries the registry through the Docker Engine and returns the
// validated manifest metadata. The result always names imageRef as its Ref, so
// validation cannot accidentally accept metadata produced for another
// reference. The caller's context bounds the Engine call.
func (r *DistributionResolver) ResolveBaseImage(
	ctx context.Context,
	imageRef string,
) (BaseImageResolution, error) {
	if r == nil || r.client == nil {
		return BaseImageResolution{}, fmt.Errorf("resolve base image %q: Docker Engine client is unavailable", imageRef)
	}
	if imageRef == "" {
		return BaseImageResolution{}, fmt.Errorf("resolve base image: reference is empty")
	}
	var encodedAuth string
	if r.auth != nil {
		encoded, err := r.auth.EncodedAuthFor(ctx, imageRef)
		if err != nil {
			return BaseImageResolution{}, fmt.Errorf("resolve base image %q: registry auth: %w", imageRef, err)
		}
		encodedAuth = encoded
	}
	result, err := r.client.DistributionInspect(ctx, imageRef, client.DistributionInspectOptions{
		EncodedRegistryAuth: encodedAuth,
	})
	if err != nil {
		return BaseImageResolution{}, fmt.Errorf("resolve base image %q through Docker Engine: %w", imageRef, err)
	}
	resolution := BaseImageResolution{
		Ref:       imageRef,
		Digest:    result.Descriptor.Digest.String(),
		MediaType: result.Descriptor.MediaType,
		Platforms: append([]ocispec.Platform(nil), result.Platforms...),
	}
	if err := ValidateBaseImageResolution(imageRef, resolution); err != nil {
		return BaseImageResolution{}, err
	}
	return resolution, nil
}

// ValidateBaseImageResolution validates the Engine response used for an update:
// the metadata must be for the queried reference, carry a valid sha256 index
// digest, and describe an OCI image index or Docker manifest list rather than a
// missing, unknown, or platform-specific manifest. When the Engine reports a
// platform list, it must contain at least two distinct concrete OS/architecture
// combinations; unknown/attestation entries are ignored for that count.
//
// The digest is the multi-platform INDEX digest Relay persists. Relay never
// resolves or persists a child manifest digest.
func ValidateBaseImageResolution(ref string, resolution BaseImageResolution) error {
	if ref == "" {
		return fmt.Errorf("resolve base image: reference is empty")
	}
	if resolution.Ref != ref {
		return fmt.Errorf(
			"resolve base image %q: Engine metadata refers to %q",
			ref, resolution.Ref)
	}
	if !ValidImageDigest(resolution.Digest) {
		return fmt.Errorf(
			"resolve base image %q: digest %q must be sha256:<64 lowercase hex>",
			ref, resolution.Digest)
	}
	switch resolution.MediaType {
	case mediaTypeOCIImageIndex, mediaTypeDockerManifestList:
		// A multi-platform index, as required for a runtime pin.
	case "":
		return fmt.Errorf("resolve base image %q: Engine returned no manifest media type", ref)
	default:
		return fmt.Errorf(
			"resolve base image %q: media type %q is not an OCI image index or Docker manifest list",
			ref, resolution.MediaType)
	}

	if len(resolution.Platforms) == 0 {
		// The Engine API does not always include the platform list (for example
		// with older versions). The index digest is still a valid multi-platform
		// identity, so absence is not an error; when present, it must prove more
		// than one concrete platform.
		return nil
	}
	concrete := make(map[string]struct{}, len(resolution.Platforms))
	for _, platform := range resolution.Platforms {
		osName := strings.ToLower(strings.TrimSpace(platform.OS))
		arch := strings.ToLower(strings.TrimSpace(platform.Architecture))
		if osName == "" || arch == "" || osName == "unknown" || arch == "unknown" {
			// Docker Engine reports OCI attestation/provenance descriptors as
			// OS "unknown", architecture "unknown". They are not buildable
			// concrete platforms and must not satisfy the multi-platform count.
			continue
		}
		variant := strings.ToLower(strings.TrimSpace(platform.Variant))
		concrete[osName+"/"+arch+"/"+variant] = struct{}{}
	}
	if len(concrete) < 2 {
		return fmt.Errorf(
			"resolve base image %q: Engine reports %d distinct concrete platform(s); a runtime pin requires a multi-platform index",
			ref, len(concrete))
	}
	return nil
}

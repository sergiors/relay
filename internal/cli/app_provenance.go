package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/moby/moby/client"

	"relay/internal/runtime"
)

// appImageProvenanceTimeout bounds the single local image inspect `app inspect`
// performs. Docker may be slow or unresponsive; provenance is diagnostic, so a
// lookup that cannot finish promptly is reported as unavailable instead of
// stalling the command.
const appImageProvenanceTimeout = 10 * time.Second

// appProvenance is the base-image provenance presented by `app inspect`. It is
// derived only from the app's active image. An unavailable value is rendered
// explicitly and never replaced by the currently configured runtime.
type appProvenance struct {
	reference string
	available bool
	reason    string
}

// display renders the provenance value shown after "Base image:".
func (p appProvenance) display() string {
	if p.available {
		return p.reference
	}
	if p.reason == "" {
		return "unavailable"
	}
	return "unavailable (" + p.reason + ")"
}

// lookupAppProvenance reads the active image's provenance through the injected
// seam. It is best-effort by design: no active image (guarded by the caller)
// or no usable lookup result renders as unavailable provenance and never fails
// inspect.
func lookupAppProvenance(
	ctx context.Context, image string, deps Dependencies,
) appProvenance {
	if deps.ReadImageProvenance == nil {
		return appProvenance{reason: "image inspection not configured"}
	}

	inspectCtx, cancel := context.WithTimeout(ctx, appImageProvenanceTimeout)
	defer cancel()

	provenance, err := deps.ReadImageProvenance(inspectCtx, image)
	if err != nil {
		return appProvenance{reason: "active image could not be inspected"}
	}
	reference := provenance.Reference()
	if reference == "" {
		return appProvenance{
			reason: "active image has no recorded provenance",
		}
	}
	return appProvenance{reference: reference, available: true}
}

// readDockerImageProvenance is the production ImageProvenanceLookup. It reads
// the given local image exactly once and extracts Relay's diagnostic labels.
// It never pulls, configures, or consults a runtime selection.
func readDockerImageProvenance(
	ctx context.Context, image string,
) (runtime.ImageProvenance, error) {
	cli, err := client.New(client.FromEnv)
	if err != nil {
		return runtime.ImageProvenance{}, fmt.Errorf(
			"connect to Docker Engine: %w", err)
	}
	defer func() { _ = cli.Close() }()

	insp, err := cli.ImageInspect(ctx, image)
	if err != nil {
		return runtime.ImageProvenance{}, fmt.Errorf("inspect image: %w", err)
	}
	if insp.Config == nil {
		return runtime.ImageProvenance{}, nil
	}
	return runtime.ImageProvenanceFromLabels(insp.Config.Labels), nil
}

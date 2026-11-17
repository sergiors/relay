package plan

import (
	"encoding/json"
	"fmt"
	"io/fs"
)

// RuntimeMetadataPath is the in-image path of the generated runtime metadata
// document. It sits beside the bootstrap in /relay, so a bootstrap module reads
// it with a path relative to its own directory.
const RuntimeMetadataPath = "/relay/runtime-metadata.json"

// RuntimeMetadata is the exact document written to RuntimeMetadataPath. The
// field set is part of the bootstrap contract: only the managed runtime
// identity and the base reference the image was built FROM are published.
//
// BaseImage is the configured base image for an unpinned runtime or
// configured-ref@selected multi-platform index digest for a pinned one. It
// records the reference the build used verbatim; an unpinned tag is mutable and
// the document never implies otherwise.
type RuntimeMetadata struct {
	Runtime   string `json:"runtime"`
	BaseImage string `json:"base_image"`
}

// RuntimeMetadataFile returns the plan file that materializes metadata for a
// runtime image. The engine calls it with the same Spec it plans the image
// from, so the document names exactly the runtime and base reference used for
// that build. Being a plan file, its path and content are part of the bootstrap
// identity label (stale-content rebuilds) and its COPY line is part of the
// rendered Dockerfile (source-mounted image identity).
func RuntimeMetadataFile(spec Spec) (File, error) {
	if spec.Name == "" {
		return File{}, fmt.Errorf("runtime metadata: runtime name is empty")
	}

	if spec.BaseImage == "" {
		return File{}, fmt.Errorf("runtime %q: runtime metadata: base image is empty", spec.Name)
	}

	content, err := json.Marshal(RuntimeMetadata{Runtime: spec.Name, BaseImage: spec.BaseImage})
	if err != nil {
		return File{}, fmt.Errorf("runtime %q: runtime metadata: %w", spec.Name, err)
	}

	return File{
		Path:    RuntimeMetadataPath,
		Content: append(content, '\n'),
		Mode:    fs.FileMode(0o644),
	}, nil
}

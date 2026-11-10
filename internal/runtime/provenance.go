package runtime

// ImageProvenance is the diagnostic build provenance Relay stamps on a managed
// app or dependency image. It is read only from the image labels: it never
// reflects the currently configured runtime or a newer persisted selection.
// SourceImage is the configured base reference (without a digest); Digest is
// the selected multi-platform index digest, present only when the image was
// built from a pinned runtime. Platform is the explicit build target.
type ImageProvenance struct {
	RuntimeName string
	SourceImage string
	Digest      string
	Platform    string
}

// ImageProvenanceFromLabels extracts the provenance Relay records on a managed
// image. Missing labels yield zero-valued fields; callers decide how to present
// an image that predates the provenance labels. It reads nothing else from the
// label set.
func ImageProvenanceFromLabels(labels map[string]string) ImageProvenance {
	return ImageProvenance{
		RuntimeName: labels[labelProvenanceRuntime],
		SourceImage: labels[labelProvenanceSourceImage],
		Digest:      labels[labelProvenanceDigest],
		Platform:    labels[labelProvenancePlatform],
	}
}

// Reference renders the base reference recorded on the image: source@digest
// when the runtime was pinned, or the source reference alone when it was not.
// It is empty when the image records no source reference: a digest on its own
// is not a base reference, so the caller reports provenance as unavailable.
func (p ImageProvenance) Reference() string {
	if p.SourceImage == "" {
		return ""
	}
	if p.Digest == "" {
		return p.SourceImage
	}
	return p.SourceImage + "@" + p.Digest
}

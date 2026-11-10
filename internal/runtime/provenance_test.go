package runtime

import "testing"

// A pinned build records both the configured source reference and the selected
// index digest: the reference renders as source@digest so an operator can tell
// exactly what the active image was built from.
func TestImageProvenanceReferencePinnedSourceWithDigest(t *testing.T) {
	// given
	prov := ImageProvenance{
		RuntimeName: "python3.14",
		SourceImage: "python:3.14-slim",
		Digest:      "sha256:0123abcd",
		Platform:    "linux/amd64",
	}

	// when
	got := prov.Reference()

	// then
	want := "python:3.14-slim@sha256:0123abcd"
	if got != want {
		t.Fatalf("Reference() = %q, want %q", got, want)
	}
}

// An unpinned build records no digest: the reference is the source alone, with
// no dangling "@" and no digest-only rendering.
func TestImageProvenanceReferenceUnpinnedSource(t *testing.T) {
	// given
	prov := ImageProvenance{
		RuntimeName: "nodejs24.x",
		SourceImage: "node:24-alpine",
		Platform:    "linux/arm64",
	}

	// when
	got := prov.Reference()

	// then
	if got != "node:24-alpine" {
		t.Fatalf("Reference() = %q, want the source reference alone", got)
	}
}

// An image that predates the provenance labels carries neither source nor
// digest: its reference is empty so the CLI can report it as missing rather
// than invent one.
func TestImageProvenanceReferenceMissingLabels(t *testing.T) {
	// given
	prov := ImageProvenance{RuntimeName: "python3.14"}

	// when
	got := prov.Reference()

	// then
	if got != "" {
		t.Fatalf("Reference() = %q, want empty without provenance labels", got)
	}
}

// A digest recorded without a source reference is not a base reference: the
// reference stays empty instead of rendering digest-only, so callers report
// provenance as unavailable.
func TestImageProvenanceReferenceDigestWithoutSource(t *testing.T) {
	// given
	prov := ImageProvenance{Digest: "sha256:0123abcd"}

	// when
	got := prov.Reference()

	// then
	if got != "" {
		t.Fatalf("Reference() = %q, want empty without a source reference", got)
	}
}

// Runtime and platform alone identify how an image was built but not what it
// was built from: the reference remains unavailable without a source reference.
func TestImageProvenanceReferenceRuntimePlatformOnly(t *testing.T) {
	// given
	prov := ImageProvenance{
		RuntimeName: "python3.14",
		Platform:    "linux/amd64",
	}

	// when
	got := prov.Reference()

	// then
	if got != "" {
		t.Fatalf("Reference() = %q, want empty without a source reference", got)
	}
}

// FromLabels reads exactly the four provenance labels and maps each onto its
// field; unrelated image labels are ignored.
func TestImageProvenanceFromLabels(t *testing.T) {
	// given
	labels := map[string]string{
		labelProvenanceRuntime:     "python3.14",
		labelProvenanceSourceImage: "python:3.14-slim",
		labelProvenanceDigest:      "sha256:0123abcd",
		labelProvenancePlatform:    "linux/amd64",
		"relay.fingerprint":        "unrelated",
		"maintainer":               "relay",
	}

	// when
	got := ImageProvenanceFromLabels(labels)

	// then
	want := ImageProvenance{
		RuntimeName: "python3.14",
		SourceImage: "python:3.14-slim",
		Digest:      "sha256:0123abcd",
		Platform:    "linux/amd64",
	}
	if got != want {
		t.Fatalf("ImageProvenanceFromLabels() = %+v, want %+v", got, want)
	}
}

// An image without the provenance labels yields a zero provenance whose
// reference is empty, so the caller reports missing provenance honestly.
func TestImageProvenanceFromLabelsWithoutProvenance(t *testing.T) {
	// given
	labels := map[string]string{"maintainer": "relay"}

	// when
	got := ImageProvenanceFromLabels(labels)

	// then
	if got != (ImageProvenance{}) {
		t.Fatalf("ImageProvenanceFromLabels() = %+v, want the zero provenance", got)
	}
	if ref := got.Reference(); ref != "" {
		t.Fatalf("Reference() = %q, want empty", ref)
	}
}

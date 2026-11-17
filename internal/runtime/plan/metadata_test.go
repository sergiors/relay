package plan

import (
	"encoding/json"
	"io/fs"
	"os"
	"strings"
	"testing"
)

// TestRuntimeMetadataFileRecordsSpecVerbatim pins the exact document the
// builder materializes: path under /relay beside the bootstrap, one line of
// JSON naming the spec's runtime and the spec's base reference VERBATIM (the
// effective reference the caller captured — never re-derived), a trailing
// newline, and the historical 0644 file mode. It is deterministic per spec.
func TestRuntimeMetadataFileRecordsSpecVerbatim(t *testing.T) {
	specs := []Spec{
		{Name: "python3.14", Engine: EnginePython, BaseImage: "python:3.14-slim"},
		// A pinned runtime: the caller passes configured-ref@digest as the spec's
		// effective base image, so the document names exactly what the build
		// built FROM. It is recorded again verbatim, without digest parsing.
		{Name: "nodejs24.x", Engine: EngineNode, BaseImage: "node:24-alpine@sha256:" + strings.Repeat("a", 64)},
	}
	for _, spec := range specs {
		t.Run(spec.Name, func(t *testing.T) {
			a, err := RuntimeMetadataFile(spec)
			if err != nil {
				t.Fatalf("RuntimeMetadataFile: %v", err)
			}
			b, err := RuntimeMetadataFile(spec)
			if err != nil {
				t.Fatalf("RuntimeMetadataFile: %v", err)
			}
			if a.Path != b.Path || string(a.Content) != string(b.Content) || a.Mode != b.Mode {
				t.Fatal("RuntimeMetadataFile is not deterministic for one spec")
			}

			if a.Path != RuntimeMetadataPath {
				t.Errorf("path = %q, want %q", a.Path, RuntimeMetadataPath)
			}
			if RuntimeMetadataPath != "/relay/runtime-metadata.json" {
				t.Fatalf("RuntimeMetadataPath = %q, want /relay/runtime-metadata.json", RuntimeMetadataPath)
			}
			if a.Mode != fs.FileMode(0o644) {
				t.Errorf("mode = %v, want 0644", a.Mode)
			}
			if !strings.HasSuffix(string(a.Content), "\n") {
				t.Errorf("content %q must end with a newline", a.Content)
			}

			var doc RuntimeMetadata
			if err := json.Unmarshal(a.Content, &doc); err != nil {
				t.Fatalf("content is not JSON: %v (%q)", err, a.Content)
			}
			if doc.Runtime != spec.Name || doc.BaseImage != spec.BaseImage {
				t.Errorf("metadata = %+v, want runtime %q base %q", doc, spec.Name, spec.BaseImage)
			}
			// The published field set is exactly the two contract fields.
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(a.Content, &fields); err != nil {
				t.Fatalf("decode fields: %v", err)
			}
			if len(fields) != 2 {
				t.Errorf("document carries %d fields (%v), want exactly runtime and base_image", len(fields), fields)
			}
		})
	}
}

// TestRuntimeMetadataFileDistinguishesPins pins that a pin change is a content
// change: the document for a pinned spec differs from the unpinned document for
// the same runtime and from a different pin, so an image whose Dockerfile COPYs
// this file re-fingerprints (rebuilds) when the selected digest changes — even
// under HOT_RELOAD, where the app source is never baked.
func TestRuntimeMetadataFileDistinguishesPins(t *testing.T) {
	runtime := "nodejs24.x"
	unpinned, err := RuntimeMetadataFile(Spec{Name: runtime, BaseImage: "node:24-alpine"})
	if err != nil {
		t.Fatalf("unpinned: %v", err)
	}
	pinnedA, err := RuntimeMetadataFile(Spec{Name: runtime, BaseImage: "node:24-alpine@" + "sha256:" + strings.Repeat("a", 64)})
	if err != nil {
		t.Fatalf("pinnedA: %v", err)
	}
	pinnedB, err := RuntimeMetadataFile(Spec{Name: runtime, BaseImage: "node:24-alpine@" + "sha256:" + strings.Repeat("b", 64)})
	if err != nil {
		t.Fatalf("pinnedB: %v", err)
	}
	if string(pinnedA.Content) == string(unpinned.Content) {
		t.Error("a pinned base must change the runtime metadata content")
	}
	if string(pinnedA.Content) == string(pinnedB.Content) {
		t.Error("a different selected digest must change the runtime metadata content")
	}
}

// TestRuntimeMetadataFileRejectsIncompleteSpec pins the fail-closed contract:
// a spec without a runtime name or a base reference is an error (never a
// document with empty fields baked into an image identity), and the message
// names the runtime when one is known.
func TestRuntimeMetadataFileRejectsIncompleteSpec(t *testing.T) {
	if _, err := RuntimeMetadataFile(Spec{Name: "nodejs24.x", BaseImage: ""}); err == nil {
		t.Fatal("empty base image must be rejected")
	} else if !strings.Contains(err.Error(), "nodejs24.x") {
		t.Errorf("error = %q, want it to name the runtime", err)
	}
	if _, err := RuntimeMetadataFile(Spec{Name: "", BaseImage: "python:3.14-slim"}); err == nil {
		t.Fatal("empty runtime name must be rejected")
	} else if !strings.Contains(err.Error(), "runtime name is empty") {
		t.Errorf("error = %q, want it to name the missing runtime name", err)
	}
}

// TestRuntimeMetadataFileIsStagedBesideBootstrap verifies the generated file is
// a regular 0644 file when materialized at its /relay path beside the
// bootstrap, so the image carries a readable metadata file owned like every
// other plan file.
func TestRuntimeMetadataFileIsStagedBesideBootstrap(t *testing.T) {
	f, err := RuntimeMetadataFile(Spec{Name: "python3.14", BaseImage: "python:3.14-slim"})
	if err != nil {
		t.Fatalf("RuntimeMetadataFile: %v", err)
	}
	root := t.TempDir()
	target := root + "/ctx" + f.Path
	if err := os.MkdirAll(root+"/ctx/relay", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(target, f.Content, f.Mode); err != nil {
		t.Fatalf("write staged file: %v", err)
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatalf("stat staged file: %v", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != fs.FileMode(0o644) {
		t.Errorf("staged mode = %v, want a regular 0644 file", info.Mode().Perm())
	}
}

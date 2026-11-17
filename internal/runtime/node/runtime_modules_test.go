package node

import (
	"encoding/json"
	"io/fs"
	"path"
	"strings"
	"testing"

	"relay/internal/runtime/plan"
)

// TestPlanShipsEmbeddedRuntimeModules pins the embedded module packaging: the
// plan writes EVERY embedded .mjs runtime module (each at /relay/<name> with
// the exact embedded bytes), so the split bootstrap's relative imports resolve
// inside the image. The resolve hook is not among them: it is planned
// separately, only into a source-mounted image. The plan must also carry the
// runtime metadata document beside the modules.
func TestPlanShipsEmbeddedRuntimeModules(t *testing.T) {
	p, err := Engine{}.Plan(testSpecs[0], t.TempDir(), nil)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	embedded, err := fs.ReadDir(runtimeModules, ".")
	if err != nil {
		t.Fatalf("read embedded modules: %v", err)
	}

	written := map[string]plan.File{}
	for _, f := range p.Files {
		if strings.HasPrefix(f.Path, relayDir+"/") {
			written[strings.TrimPrefix(f.Path, relayDir+"/")] = f
		}
	}

	for _, entry := range embedded {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".mjs") || name == resolveHookFile {
			continue
		}
		want, err := runtimeModules.ReadFile(name)
		if err != nil {
			t.Fatalf("read embedded module %s: %v", name, err)
		}
		got, ok := written[name]
		if !ok {
			t.Errorf("plan must write the embedded module %s, wrote %v", name, moduleList(written))
			continue
		}
		if string(got.Content) != string(want) {
			t.Errorf("planned %s content differs from the embedded module", name)
		}
		if got.Mode != fs.FileMode(0o644) {
			t.Errorf("%s mode = %v, want 0644", name, got.Mode)
		}
		if got.Path != path.Join(relayDir, name) {
			t.Errorf("%s path = %q, want %q", name, got.Path, path.Join(relayDir, name))
		}
	}

	// A baked plan must never carry the resolve hook: the test above enumerates
	// only the shared set, so check its absence explicitly.
	if _, ok := written[resolveHookFile]; ok {
		t.Error("baked plan must not write the resolve hook (it belongs to source-mounted images only)")
	}
	// The bootstrap is part of the embedded set; it must be present under its
	// planned entrypoint name.
	if _, ok := written["bootstrap.mjs"]; !ok {
		t.Errorf("plan must write /relay/bootstrap.mjs, wrote %v", moduleList(written))
	}
	// The runtime metadata document ships beside the modules.
	if _, ok := written["runtime-metadata.json"]; !ok {
		t.Errorf("plan must write /relay/runtime-metadata.json, wrote %v", moduleList(written))
	}
}

// moduleList returns the plan's /relay file names for diagnostics.
func moduleList(written map[string]plan.File) []string {
	names := make([]string, 0, len(written))
	for name := range written {
		names = append(names, name)
	}
	for i := 1; i < len(names); i++ {
		for j := i; j > 0 && names[j] < names[j-1]; j-- {
			names[j], names[j-1] = names[j-1], names[j]
		}
	}
	return names
}

// TestPlanMetadataRecordsEffectiveBaseImage pins the metadata SOURCE accuracy:
// the engine plans the spec passed BY the caller, so the generated document
// records the effective base reference verbatim — the configured tag for an
// unpinned runtime, and the pinned configured-ref@digest exactly as selected
// for a pinned one. An unpinned document must never be reused for a pinned
// build: each base change produces a different file (a re-fingerprint).
func TestPlanMetadataRecordsEffectiveBaseImage(t *testing.T) {
	digest := "sha256:" + strings.Repeat("d", 64)
	cases := []struct {
		name      string
		baseImage string
	}{
		{"unpinned keeps the configured tag", "node:24-alpine"},
		{"pinned records the selected digest", "node:24-alpine@" + digest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := plan.Spec{
				Name:      "nodejs24.x",
				Engine:    plan.EngineNode,
				BaseImage: tc.baseImage,
			}
			p, err := Engine{}.Plan(spec, t.TempDir(), nil)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			var metadata *plan.File
			for i := range p.Files {
				if p.Files[i].Path == plan.RuntimeMetadataPath {
					metadata = &p.Files[i]
					break
				}
			}
			if metadata == nil {
				t.Fatalf("plan has no %s file: %v", plan.RuntimeMetadataPath, moduleList(map[string]plan.File{}))
			}
			want, err := plan.RuntimeMetadataFile(spec)
			if err != nil {
				t.Fatalf("RuntimeMetadataFile: %v", err)
			}
			if string(metadata.Content) != string(want.Content) {
				t.Errorf("metadata = %q, want %q", metadata.Content, want.Content)
			}

			var doc plan.RuntimeMetadata
			if err := json.Unmarshal(metadata.Content, &doc); err != nil {
				t.Fatalf("decode metadata: %v", err)
			}
			if doc.Runtime != "nodejs24.x" || doc.BaseImage != tc.baseImage {
				t.Errorf("metadata = %+v, want runtime nodejs24.x base %q", doc, tc.baseImage)
			}
			// The plan's FROM is the same effective reference, so the document
			// names exactly what the image is built FROM.
			if p.BaseImage != tc.baseImage {
				t.Errorf("plan base image = %q, want the effective %q", p.BaseImage, tc.baseImage)
			}
		})
	}
}

// TestPlanMetadataChangesWithBaseImage pins that a base reference change (a
// pin refresh) changes the plan's metadata file content — the input the
// bootstrap identity label hashes — so a same-source rebuild after a pin
// change is detected even when nothing else moves.
func TestPlanMetadataChangesWithBaseImage(t *testing.T) {
	dir := t.TempDir()
	unpinned, err := Engine{}.Plan(plan.Spec{Name: "nodejs24.x", BaseImage: "node:24-alpine"}, dir, nil)
	if err != nil {
		t.Fatalf("unpinned plan: %v", err)
	}
	pinned, err := Engine{}.Plan(plan.Spec{
		Name:      "nodejs24.x",
		BaseImage: "node:24-alpine@sha256:" + strings.Repeat("e", 64),
	}, dir, nil)
	if err != nil {
		t.Fatalf("pinned plan: %v", err)
	}
	a, b := metadataOf(t, unpinned), metadataOf(t, pinned)
	if string(a.Content) == string(b.Content) {
		t.Error("the plan's runtime metadata must change when the effective base image changes")
	}
	if !bytesEqualExceptMetadata(unpinned.Files, pinned.Files, a) {
		t.Error("a base change must alter no other planned file content")
	}
}

// metadataOf returns the plan's runtime metadata file, failing when absent.
func metadataOf(t *testing.T, p plan.BuildPlan) plan.File {
	t.Helper()
	for i := range p.Files {
		if p.Files[i].Path == plan.RuntimeMetadataPath {
			return p.Files[i]
		}
	}
	t.Fatalf("plan has no %s", plan.RuntimeMetadataPath)
	return plan.File{}
}

// bytesEqualExceptMetadata compares the two plans' non-metadata files as an
// unordered {path: content} set.
func bytesEqualExceptMetadata(a, b []plan.File, metadata plan.File) bool {
	index := func(files []plan.File) map[string]string {
		out := make(map[string]string, len(files))
		for _, f := range files {
			if f.Path == metadata.Path {
				continue
			}
			out[f.Path] = string(f.Content)
		}
		return out
	}
	ai, bi := index(a), index(b)
	if len(ai) != len(bi) {
		return false
	}
	for k, v := range ai {
		if bi[k] != v {
			return false
		}
	}
	return true
}

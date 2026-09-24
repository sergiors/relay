package function

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A template without a `networks` key renders a nil Networks slice, preserving
// the pre-networks behavior.
func TestParseNoNetworksNil(t *testing.T) {
	tmpl := mustParse(t, `
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
`)
	if tmpl.Networks != nil {
		t.Fatalf("expected nil Networks, got %#v", tmpl.Networks)
	}
}

// `networks` is normalized deterministically: entries are trimmed, duplicates
// removed, and the result sorted, so authoring order and cosmetic whitespace
// never change the parsed value.
func TestParseNetworksNormalized(t *testing.T) {
	tmpl := mustParse(t, `
runtime: node24
networks:
  - " backend "
  - frontend
  - backend
  - "frontend"
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
`)
	want := []string{"backend", "frontend"}
	if len(tmpl.Networks) != len(want) {
		t.Fatalf("networks = %#v, want %#v", tmpl.Networks, want)
	}
	for i := range want {
		if tmpl.Networks[i] != want[i] {
			t.Fatalf("networks = %#v, want %#v", tmpl.Networks, want)
		}
	}
}

// Two templates listing the same networks in different order parse to the same
// deterministic slice.
func TestParseNetworksOrderIndependent(t *testing.T) {
	a := mustParse(t, `
runtime: node24
networks: [zeta, alpha, mid]
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
`)
	b := mustParse(t, `
runtime: node24
networks: [mid, zeta, alpha]
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
`)
	if len(a.Networks) != 3 || len(b.Networks) != 3 {
		t.Fatalf("networks = %#v / %#v", a.Networks, b.Networks)
	}
	for i := range a.Networks {
		if a.Networks[i] != b.Networks[i] {
			t.Fatalf("normalization not order-independent: %#v vs %#v", a.Networks, b.Networks)
		}
	}
	if a.RuntimeGeneration() != b.RuntimeGeneration() {
		t.Fatal("same network set must yield the same runtime generation")
	}
}

// An empty network name is rejected, and a non-string entry is rejected rather
// than coerced.
func TestParseNetworksRejected(t *testing.T) {
	cases := []struct {
		name     string
		networks string
		wantErr  string
	}{
		{"empty string", "networks: [\"\"]\n", "must not be empty"},
		{"whitespace only", "networks: [\"   \"]\n", "must not be empty"},
		{"number", "networks: [123]\n", "must be a string"},
		{"bool", "networks: [true]\n", "must be a string"},
		{"map", "networks:\n  - {name: x}\n", "must be a string"},
		{"list", "networks:\n  - [a, b]\n", "must be a string"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTemplate([]byte(`
runtime: node24
` + tc.networks + `
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
`))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

// RuntimeGeneration is empty for no networks and changes when the network set
// changes.
func TestRuntimeGeneration(t *testing.T) {
	none := mustParse(t, `
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
`)
	if none.RuntimeGeneration() != "" {
		t.Fatalf("no networks: RuntimeGeneration = %q, want empty", none.RuntimeGeneration())
	}

	one := mustParse(t, `
runtime: node24
networks: [backend]
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
`)
	two := mustParse(t, `
runtime: node24
networks: [backend, frontend]
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
`)
	if one.RuntimeGeneration() == "" || two.RuntimeGeneration() == "" {
		t.Fatal("configured networks must have a non-empty runtime generation")
	}
	if one.RuntimeGeneration() == two.RuntimeGeneration() {
		t.Fatal("a changed network set must change the runtime generation")
	}
	// Same set, authored differently: same generation.
	dup := mustParse(t, `
runtime: node24
networks: [backend, backend]
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
`)
	if dup.RuntimeGeneration() != one.RuntimeGeneration() {
		t.Fatal("duplicate entries must not change the runtime generation")
	}
}

// ImageFingerprint ignores a runtime-only `networks` edit (no rebuild), while
// the full Fingerprint still detects it (so the reconciler reconciles).
func TestImageFingerprintIgnoresNetworksEdit(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "handler.js"), "export function main(e){}\n")
	writeFile(t, filepath.Join(dir, "template.yaml"), `runtime: node24
networks: [backend]
services:
  - entrypoint: service.js
`)
	contentBefore := fp(t, dir)
	imageBefore, err := ImageFingerprint(dir)
	if err != nil {
		t.Fatalf("image fingerprint: %v", err)
	}

	writeFile(t, filepath.Join(dir, "template.yaml"), `runtime: node24
networks: [backend, frontend]
services:
  - entrypoint: service.js
`)
	contentAfter := fp(t, dir)
	imageAfter, err := ImageFingerprint(dir)
	if err != nil {
		t.Fatalf("image fingerprint: %v", err)
	}

	if contentAfter == contentBefore {
		t.Fatal("a networks edit must change the full fingerprint (so the reconciler reacts)")
	}
	if imageAfter != imageBefore {
		t.Fatal("a networks-only edit must NOT change the image fingerprint (no rebuild)")
	}
}

// TestImageFingerprintNoNetworksMatchesContentFingerprint is the upgrade
// regression guard: a template WITHOUT `networks` is hashed VERBATIM by the
// image fingerprint, so it fingerprints exactly as the pre-split content
// fingerprint did. Without this, canonicalizing every template.yaml (dropping
// comments, rewriting indentation) would give every existing no-`networks`
// function a new image tag and force a spurious rebuild on upgrade.
func TestImageFingerprintNoNetworksMatchesContentFingerprint(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "handler.js"), "export function main(e){}\n")
	// Comments, a flow sequence, and 2-space list indentation: all things a
	// whole-document re-marshal would have rewritten.
	writeFile(t, filepath.Join(dir, "template.yaml"), `# top comment
runtime: node24   # inline
services:
  - entrypoint: service.js
`)
	content := fp(t, dir)
	image, err := ImageFingerprint(dir)
	if err != nil {
		t.Fatalf("image fingerprint: %v", err)
	}
	if image != content {
		t.Fatalf("no-networks image fingerprint = %q, want the verbatim content fingerprint %q", image, content)
	}
}

// TestImageFingerprintAddRemoveNetworksDoesNotRebuild covers the ADD and REMOVE
// directions of a network-only edit (not just changing the values): the image
// fingerprint is invariant to the `networks` key's presence, in both block and
// flow form, and to whether the key is the first or last top-level key. This is
// what the byte-faithful splice buys over a whole-document canonicalization,
// which would reformat the non-network remainder and so register a spurious
// rebuild the moment `networks` appeared.
func TestImageFingerprintAddRemoveNetworksDoesNotRebuild(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "handler.js"), "export function main(e){}\n")
	base := "# top comment\nruntime: node24\nservices:\n  - entrypoint: service.js\n"
	writeFile(t, filepath.Join(dir, "template.yaml"), base)
	contentBase := fp(t, dir)
	imageBase, err := ImageFingerprint(dir)
	if err != nil {
		t.Fatalf("image fingerprint: %v", err)
	}

	cases := map[string]string{
		"flow after runtime":       "# top comment\nruntime: node24\nnetworks: [backend]\nservices:\n  - entrypoint: service.js\n",
		"block after runtime":      "# top comment\nruntime: node24\nnetworks:\n  - backend\n  - frontend\nservices:\n  - entrypoint: service.js\n",
		"flow as last key":         "# top comment\nruntime: node24\nservices:\n  - entrypoint: service.js\nnetworks: [backend]\n",
		"block as last key":        "# top comment\nruntime: node24\nservices:\n  - entrypoint: service.js\nnetworks:\n  - backend\n  - frontend\n",
		"block multi-line flow":    "# top comment\nruntime: node24\nnetworks: [\n  backend,\n  frontend,\n]\nservices:\n  - entrypoint: service.js\n",
		"inline comment preserved": "# top comment\nruntime: node24\nnetworks: [backend] # pick networks\nservices:\n  - entrypoint: service.js\n",
		"null value":               "# top comment\nruntime: node24\nnetworks:\nservices:\n  - entrypoint: service.js\n",
		"empty flow sequence":      "# top comment\nruntime: node24\nnetworks: []\nservices:\n  - entrypoint: service.js\n",
	}
	for name, tmpl := range cases {
		t.Run(name, func(t *testing.T) {
			writeFile(t, filepath.Join(dir, "template.yaml"), tmpl)
			got, err := ImageFingerprint(dir)
			if err != nil {
				t.Fatalf("image fingerprint: %v", err)
			}
			if got != imageBase {
				t.Fatalf("adding networks (%s) changed the image fingerprint; want no rebuild", name)
			}
			// The full fingerprint must still differ, so the reconciler reacts.
			if fp(t, dir) == contentBase {
				t.Fatal("adding networks must change the full fingerprint")
			}
		})
	}

	// Removing the key restores both fingerprints exactly.
	writeFile(t, filepath.Join(dir, "template.yaml"), base)
	got, err := ImageFingerprint(dir)
	if err != nil {
		t.Fatalf("image fingerprint: %v", err)
	}
	if got != imageBase {
		t.Fatal("removing networks must restore the image fingerprint")
	}
	if fp(t, dir) != contentBase {
		t.Fatal("removing networks must restore the content fingerprint")
	}
}

// TestImageFingerprintFlowRootIgnoresNetworks covers the flow-style root mapping
// edge case: a single-line root mapping (`{networks: [a], runtime: node24}`) puts
// several top-level keys on one line, so the whole line cannot be removed without
// dropping the siblings. The image fingerprint must STILL ignore a `networks`
// edit there — removing only the pair's own bytes and one adjacent separator —
// so adding, moving, changing, or removing `networks` in a flow root does not
// force a rebuild. Each keyed form is compared against the SAME document authored
// without the key (its own base), so the assertion is on a byte-exact remainder,
// not just self-consistency.
func TestImageFingerprintFlowRootIgnoresNetworks(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "handler.js"), "export function main(e){}\n")
	spacedBase := "{runtime: node24, services: [{entrypoint: service.js}]}\n"
	tightBase := "{runtime: node24,services: [{entrypoint: service.js}]}\n"

	cases := []struct {
		name string
		base string // the no-`networks` document this case must fingerprint as
		tmpl string // the same document with `networks` added/moved/changed
	}{
		{"networks first", spacedBase, "{networks: [backend], runtime: node24, services: [{entrypoint: service.js}]}\n"},
		{"networks middle", spacedBase, "{runtime: node24, networks: [backend], services: [{entrypoint: service.js}]}\n"},
		{"networks last", spacedBase, "{runtime: node24, services: [{entrypoint: service.js}], networks: [backend]}\n"},
		{"networks empty", spacedBase, "{runtime: node24, networks: [], services: [{entrypoint: service.js}]}\n"},
		{"networks null", spacedBase, "{runtime: node24, networks: null, services: [{entrypoint: service.js}]}\n"},
		{"networks value with comma", spacedBase, "{runtime: node24, networks: [\"b,c\"], services: [{entrypoint: service.js}]}\n"},
		{"networks nested value", spacedBase, "{runtime: node24, networks: [a, [b, c]], services: [{entrypoint: service.js}]}\n"},
		{"networks tight spacing", tightBase, "{runtime: node24,networks: [backend],services: [{entrypoint: service.js}]}\n"},
		{"networks tight last", tightBase, "{runtime: node24,services: [{entrypoint: service.js}],networks: [backend]}\n"},
		{"multi-line flow root", "{\n  runtime: node24,\n  services: [{entrypoint: service.js}],\n}\n",
			"{\n  runtime: node24,\n  services: [{entrypoint: service.js}],\n  networks: [backend],\n}\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writeFile(t, filepath.Join(dir, "template.yaml"), tc.base)
			baseContent := fp(t, dir)
			imageBase, err := ImageFingerprint(dir)
			if err != nil {
				t.Fatalf("image fingerprint: %v", err)
			}

			writeFile(t, filepath.Join(dir, "template.yaml"), tc.tmpl)
			got, err := ImageFingerprint(dir)
			if err != nil {
				t.Fatalf("image fingerprint: %v", err)
			}
			if got != imageBase {
				t.Fatalf("networks in a flow root (%s) changed the image fingerprint; want no rebuild\n%s", tc.name, tc.tmpl)
			}
			// The full fingerprint must still differ, so the reconciler reacts.
			if fp(t, dir) == baseContent {
				t.Fatal("networks in a flow root must change the full fingerprint")
			}
		})
	}
}

// TestImageFingerprintFlowRootMalformedUnchanged is the malformed-input safety
// guard for the flow-root path: a value shape the pair splice cannot measure (a
// plain scalar `networks`, illegal for the list) must fall back to hashing the
// raw bytes, so the image fingerprint equals the full content fingerprint and
// no sibling bytes are ever dropped. This keeps the optimization unable to
// weaken malformed-input safety.
func TestImageFingerprintFlowRootMalformedUnchanged(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "handler.js"), "export function main(e){}\n")
	writeFile(t, filepath.Join(dir, "template.yaml"),
		"{runtime: node24, networks: bogus, services: [{entrypoint: service.js}]}\n")
	content := fp(t, dir)
	image, err := ImageFingerprint(dir)
	if err != nil {
		t.Fatalf("image fingerprint: %v", err)
	}
	if image != content {
		t.Fatalf("unmeasurable flow-root value must hash raw: image = %q, content = %q", image, content)
	}
}

// TestTemplateImageContentNeverDropsSibling is a byte-level safety guard for the
// whole-line splice: a `networks` key alone on its line whose flow value closes
// on a line shared with a sibling key (`networks: [\n  a,\n], runtime: node24`)
// must not take the sibling with it. The guard must fall back to removing only
// the pair, leaving every sibling key intact.
func TestTemplateImageContentNeverDropsSibling(t *testing.T) {
	cases := []string{
		"{\n  networks: [\n    a,\n  ], runtime: node24\n}\n",
		"{\n  runtime: node24,\n  networks: [\n    a,\n  ], services: [x]\n}\n",
	}
	for _, in := range cases {
		out := templateImageContent("template.yaml", []byte(in))
		if out == nil {
			t.Fatalf("expected a splice for %q, got nil (raw fallback)", in)
		}
		if got := string(out); strings.Contains(got, "networks") {
			t.Fatalf("networks not removed: %q", got)
		}
		if !strings.Contains(string(out), "runtime") && !strings.Contains(string(out), "services") {
			t.Fatalf("sibling key dropped from %q -> %q", in, out)
		}
	}
}

// TestImageFingerprintTracksContentAndNormalizesNetworks: ImageFingerprint still
// detects a real content edit, and reordering the same network set does not
// change it.
func TestImageFingerprintTracksContentAndNormalizesNetworks(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "handler.js"), "export function main(e){ return 1 }\n")
	writeFile(t, filepath.Join(dir, "template.yaml"), `runtime: node24
networks: [a, b]
services:
  - entrypoint: service.js
`)
	base, err := ImageFingerprint(dir)
	if err != nil {
		t.Fatalf("image fingerprint: %v", err)
	}

	// Reordering the same network set is not a content change.
	writeFile(t, filepath.Join(dir, "template.yaml"), `runtime: node24
networks: [b, a]
services:
  - entrypoint: service.js
`)
	if got, err := ImageFingerprint(dir); err != nil {
		t.Fatalf("image fingerprint: %v", err)
	} else if got != base {
		t.Fatal("reordering the same network set must not change the image fingerprint")
	}

	// A real handler edit does change it.
	writeFile(t, filepath.Join(dir, "handler.js"), "export function main(e){ return 2 }\n")
	if got, err := ImageFingerprint(dir); err != nil {
		t.Fatalf("image fingerprint: %v", err)
	} else if got == base {
		t.Fatal("a handler edit must change the image fingerprint")
	}
}

// The shipped examples parse with no networks (nil), pinning the default.
func TestExampleTemplatesNoNetworks(t *testing.T) {
	dirs := []string{
		"../../examples/functions/users-api-node/template.yaml",
		"../../examples/functions/fastapi-service/template.yaml",
	}
	for _, p := range dirs {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Skipf("example not present: %v", err)
		}
		tmpl, err := ParseTemplate(data)
		if err != nil {
			t.Fatalf("parse %s: %v", p, err)
		}
		if tmpl.Networks != nil {
			t.Fatalf("%s: Networks = %#v, want nil", p, tmpl.Networks)
		}
	}
}

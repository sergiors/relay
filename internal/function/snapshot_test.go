package function

import (
	"os"
	"path/filepath"
	"testing"

	"relay/internal/source"
)

// TestSnapshotFingerprintMatchesFingerprintSelection pins that the digest derived
// from the immutable snapshot is EXACTLY the digest FingerprintSelection has
// always produced for the same tree: the capture refactor must not change
// identity bytes.
func TestSnapshotFingerprintMatchesFingerprintSelection(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, ".gitignore"), "*.log\n")
	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: python3.14\nresources:\n  memory: 1GiB\n")
	writeFile(t, filepath.Join(dir, "handler.py"), "def handler(e): return 1\n")
	writeFile(t, filepath.Join(dir, "debug.log"), "noise\n")

	selection := mustSnapshotSelection(t, dir)
	want, err := FingerprintSelection(selection)
	if err != nil {
		t.Fatalf("FingerprintSelection: %v", err)
	}

	snapshot, err := CaptureSourceSnapshot(selection)
	if err != nil {
		t.Fatalf("CaptureSourceSnapshot: %v", err)
	}
	defer snapshot.Discard()
	if got := snapshot.Fingerprint(); got != want {
		t.Fatalf("snapshot fingerprint = %q, want %q", got, want)
	}
}

// TestSnapshotIsImmutableAcrossMutation is the core TOCTOU proof at the function
// layer: once captured, mutating (or deleting) the on-disk files does not change
// the snapshot's digest, because the bytes are held in memory. This is what lets
// the runtime derive the tag and stage the context from one read.
func TestSnapshotIsImmutableAcrossMutation(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: node24\n")
	writeFile(t, filepath.Join(dir, "index.js"), "export function h(){ return 1; }\n")

	snapshot, err := CaptureSourceSnapshot(mustSnapshotSelection(t, dir))
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	defer snapshot.Discard()
	before := snapshot.Fingerprint()

	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function h(){ return 2; }\n"), 0o644); err != nil {
		t.Fatalf("mutate: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, "index.js")); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if got := snapshot.Fingerprint(); got != before {
		t.Fatalf("snapshot fingerprint changed after on-disk mutation: %q != %q", got, before)
	}
}

// TestSnapshotEntriesCarryStageRelAndContent pins the capture contract the
// stager relies on: every entry below the selected directory carries a non-empty
// selected-directory-relative StageRel and its exact bytes, a directory entry is
// flagged, and an ancestor policy file is fingerprinted with an EMPTY StageRel
// (never staged).
func TestSnapshotEntriesCarryStageRelAndContent(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "fn")
	writeFile(t, filepath.Join(root, ".gitignore"), "*.generated\n")
	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: node24\n")
	writeFile(t, filepath.Join(dir, "sub", "helper.js"), "export const x = 1\n")

	selection, err := source.New(root, dir)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	snapshot, err := CaptureSourceSnapshot(selection)
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	defer snapshot.Discard()

	byRel := map[string]SnapshotEntry{}
	for _, e := range snapshot.Entries() {
		byRel[e.Rel] = e
	}
	helper, ok := byRel["fn/sub/helper.js"]
	if !ok {
		t.Fatalf("missing helper entry; entries = %v", byRel)
	}
	if helper.StageRel != "sub/helper.js" || string(helper.Content) != "export const x = 1\n" || helper.IsDir || !helper.Regular {
		t.Fatalf("helper entry = %+v", helper)
	}
	if !byRel["fn/sub"].IsDir {
		t.Fatalf("sub must be captured as a directory: %+v", byRel["fn/sub"])
	}
	ancestor, ok := byRel[".gitignore"]
	if !ok {
		t.Fatalf("ancestor .gitignore must be fingerprinted; entries = %v", byRel)
	}
	if ancestor.StageRel != "" {
		t.Fatalf("ancestor policy file StageRel = %q, want empty (never staged)", ancestor.StageRel)
	}
	if string(ancestor.Content) != "*.generated\n" {
		t.Fatalf("ancestor content = %q", ancestor.Content)
	}
}

// TestSnapshotDiscardReleasesBytes pins cleanup: Discard clears the captured
// entries and is nil-safe and idempotent, so a preparation can defer it on every
// path without retaining the tree.
func TestSnapshotDiscardReleasesBytes(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "index.js"), "export function h(){}\n")

	snapshot, err := CaptureSourceSnapshot(mustSnapshotSelection(t, dir))
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	if len(snapshot.Entries()) == 0 {
		t.Fatal("expected captured entries")
	}
	snapshot.Discard()
	if len(snapshot.Entries()) != 0 {
		t.Fatalf("entries after Discard = %d, want 0", len(snapshot.Entries()))
	}
	snapshot.Discard() // idempotent
	var nilSnap *SourceSnapshot
	nilSnap.Discard() // nil-safe
}

// TestSnapshotTemplateResourcesStripped pins that the snapshot's template-only
// resource stripping is preserved: a resource-only edit yields the same digest
// while a non-resource template edit changes it.
func TestSnapshotTemplateResourcesStripped(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "index.js"), "export function run(e){}\n")
	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: node24\nresources:\n  memory: 128MiB\nevents:\n  - handler: index.run\n    pattern:\n      event_name: [INSERT]\n")

	base := snapshotDigest(t, dir)
	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: node24\nresources:\n  memory: 512MiB\nevents:\n  - handler: index.run\n    pattern:\n      event_name: [INSERT]\n")
	if got := snapshotDigest(t, dir); got != base {
		t.Fatalf("resource-only edit changed the snapshot digest: %q != %q", got, base)
	}
	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: node24\nresources:\n  memory: 512MiB\nevents:\n  - handler: index.run\n    pattern:\n      event_name: [MODIFY]\n")
	if got := snapshotDigest(t, dir); got == base {
		t.Fatal("a non-resource template edit must change the snapshot digest")
	}
}

func mustSnapshotSelection(t *testing.T, dir string) *source.Selection {
	t.Helper()
	selection, err := source.ForDir(dir)
	if err != nil {
		t.Fatalf("select %q: %v", dir, err)
	}
	return selection
}

func snapshotDigest(t *testing.T, dir string) string {
	t.Helper()
	snapshot, err := CaptureSourceSnapshot(mustSnapshotSelection(t, dir))
	if err != nil {
		t.Fatalf("capture %q: %v", dir, err)
	}
	defer snapshot.Discard()
	return snapshot.Fingerprint()
}

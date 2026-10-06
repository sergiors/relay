package app

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

// TestSnapshotIsImmutableAcrossMutation is the core TOCTOU proof at the app
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

// TestSnapshotEntriesCarryStageRelAndStageContent pins the capture contract the
// stager relies on: every entry below the selected directory carries a non-empty
// selected-directory-relative StageRel, a regular file is staged at its StageRel
// under the snapshot's private root, a directory entry is flagged, and an
// ancestor policy file is fingerprinted with an EMPTY StageRel (never staged).
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
	if helper.StageRel != "sub/helper.js" || helper.IsDir || !helper.Regular {
		t.Fatalf("helper entry = %+v", helper)
	}
	staged, err := os.ReadFile(filepath.Join(snapshot.Root(), "sub", "helper.js"))
	if err != nil {
		t.Fatalf("read staged helper: %v", err)
	}
	if string(staged) != "export const x = 1\n" {
		t.Fatalf("staged helper content = %q", staged)
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
	if _, err := os.Stat(filepath.Join(snapshot.Root(), ".gitignore")); !os.IsNotExist(err) {
		t.Fatalf("ancestor policy file must never be staged, stat err = %v", err)
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

// TestSnapshotDiscardRemovesRoot pins the disk-backed cleanup: Discard removes
// the snapshot's private root (the app build context) so a preparation leaks no
// temporary directory, and the fingerprint remains readable after Discard.
func TestSnapshotDiscardRemovesRoot(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "index.js"), "export function h(){}\n")

	snapshot, err := CaptureSourceSnapshot(mustSnapshotSelection(t, dir))
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	root := snapshot.Root()
	if root == "" {
		t.Fatal("a captured snapshot must own a private root")
	}
	if _, err := os.Stat(filepath.Join(root, "index.js")); err != nil {
		t.Fatalf("staged source missing under root: %v", err)
	}
	before := snapshot.Fingerprint()

	snapshot.Discard()
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("Discard must remove the snapshot root, stat err = %v", err)
	}
	if snapshot.Root() != "" {
		t.Fatalf("Root after Discard = %q, want empty", snapshot.Root())
	}
	if got := snapshot.Fingerprint(); got != before {
		t.Fatalf("fingerprint changed across Discard: %q != %q", got, before)
	}
}

// TestSnapshotStagesDiskBackedContentAndFingerprintMatchesSelection is the
// disk-backed contract proof: the fingerprint computed by the capture equals
// FingerprintSelection over the same tree, the staged root carries exactly the
// selected regular files (minus template.yaml), and a mutation of the source
// after capture changes neither the digest nor the already-staged bytes.
func TestSnapshotStagesDiskBackedContentAndFingerprintMatchesSelection(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, ".gitignore"), "*.log\n")
	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: node24\n")
	writeFile(t, filepath.Join(dir, "index.js"), "export function h(){}\n")
	writeFile(t, filepath.Join(dir, "debug.log"), "noise\n")

	selection := mustSnapshotSelection(t, dir)
	want, err := FingerprintSelection(selection)
	if err != nil {
		t.Fatalf("FingerprintSelection: %v", err)
	}
	snapshot, err := CaptureSourceSnapshot(selection)
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	defer snapshot.Discard()
	if got := snapshot.Fingerprint(); got != want {
		t.Fatalf("snapshot fingerprint = %q, want %q", got, want)
	}
	if got := mustRead(t, filepath.Join(snapshot.Root(), "index.js")); got != "export function h(){}\n" {
		t.Fatalf("staged index.js = %q", got)
	}
	if _, err := os.Stat(filepath.Join(snapshot.Root(), "template.yaml")); !os.IsNotExist(err) {
		t.Fatalf("template.yaml must not be staged, stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(snapshot.Root(), "debug.log")); !os.IsNotExist(err) {
		t.Fatalf("ignored file must not be staged, stat err = %v", err)
	}

	// Mutating the source after capture must not touch the digest or the staged
	// bytes: both come from the single capture.
	writeFile(t, filepath.Join(dir, "index.js"), "export function h(){ return 'mutated'; }\n")
	if got := snapshot.Fingerprint(); got != want {
		t.Fatalf("fingerprint changed after mutation: %q != %q", got, want)
	}
	if got := mustRead(t, filepath.Join(snapshot.Root(), "index.js")); got != "export function h(){}\n" {
		t.Fatalf("staged index.js changed after mutation: %q", got)
	}
}

func mustRead(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(raw)
}

// TestSnapshotExcludesTemplateDirSubtreeAndNonRegular pins two staging-policy
// corners preserved from the historical stager: a directory literally named
// template.yaml is excluded WITH its subtree (its contents still fingerprint),
// and a non-regular entry (a symlink) is fingerprinted but never staged, while a
// regular file is staged.
func TestSnapshotExcludesTemplateDirSubtreeAndNonRegular(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "index.js"), "export function h(){}\n")
	if err := os.MkdirAll(filepath.Join(dir, "template.yaml"), 0o755); err != nil {
		t.Fatalf("mkdir template.yaml dir: %v", err)
	}
	writeFile(t, filepath.Join(dir, "template.yaml", "secret.env"), "TOKEN=xyz\n")
	if err := os.Symlink(filepath.Join(dir, "index.js"), filepath.Join(dir, "link.js")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	snapshot, err := CaptureSourceSnapshot(mustSnapshotSelection(t, dir))
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	defer snapshot.Discard()

	// The directory named template.yaml and its whole subtree are fingerprinted
	// (they are selected source) but never staged.
	for _, rel := range []string{"template.yaml", "template.yaml/secret.env"} {
		found := false
		for _, e := range snapshot.Entries() {
			if e.Rel == rel {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("entry %q must be fingerprinted; entries = %+v", rel, snapshot.Entries())
		}
	}
	if _, err := os.Stat(filepath.Join(snapshot.Root(), "template.yaml")); !os.IsNotExist(err) {
		t.Fatalf("a directory named template.yaml must be excluded from the context, stat err = %v", err)
	}
	// The symlink participates in the fingerprint (the historical reader followed
	// it) but is never staged; only the regular target is.
	if _, err := os.Stat(filepath.Join(snapshot.Root(), "index.js")); err != nil {
		t.Fatalf("regular file must be staged: %v", err)
	}
	if _, err := os.Stat(filepath.Join(snapshot.Root(), "link.js")); !os.IsNotExist(err) {
		t.Fatalf("a symlink must not be staged, stat err = %v", err)
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

package function

import (
	"os"
	"path/filepath"
	"testing"

	"relay/internal/source"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func fp(t *testing.T, dir string) string {
	t.Helper()
	f, err := Fingerprint(dir)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	return f
}

func TestFingerprintUnchangedStable(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: python3.14\n")
	writeFile(t, filepath.Join(dir, "handler.py"), "def handler(e):\n")

	got := fp(t, dir)
	for i := 0; i < 3; i++ {
		if again := fp(t, dir); again != got {
			t.Fatalf("expected stable fingerprint %q, got %q", got, again)
		}
	}
}

// Deterministic across directory iteration order because paths are sorted.
func TestFingerprintDeterministicAcrossOrdering(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: node24\n")
	writeFile(t, filepath.Join(dir, "index.js"), "export function hi(e){}\n")

	first := fp(t, dir)
	second := fp(t, dir)
	if first != second {
		t.Fatalf("deterministic fingerprint expected, %q != %q", first, second)
	}
}

func TestFingerprintContentChangeDetected(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: python3.14\n")
	writeFile(t, filepath.Join(dir, "handler.py"), "def handler(e): return 1\n")
	before := fp(t, dir)

	writeFile(t, filepath.Join(dir, "handler.py"), "def handler(e): return 2\n")
	after := fp(t, dir)
	if after == before {
		t.Fatal("content change should change fingerprint")
	}
}

func TestFingerprintDependencyFileChangeDetected(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: python3.14\n")
	writeFile(t, filepath.Join(dir, "requirements.txt"), "requests==2.0\n")
	before := fp(t, dir)

	writeFile(t, filepath.Join(dir, "requirements.txt"), "requests==2.1\n")
	after := fp(t, dir)
	if after == before {
		t.Fatal("dependency change should change fingerprint")
	}
}

// TestFingerprintNativeUvPairDetected pins that the native uv project files are
// selected source: editing either pyproject.toml or uv.lock changes the function
// fingerprint (so the reconciler forces a rebuild), and each is individually
// observable.
func TestFingerprintNativeUvPairDetected(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: python3.14\n")
	writeFile(t, filepath.Join(dir, "pyproject.toml"), "[project]\nname = \"x\"\ndependencies = [\"six==1.16.0\"]\n")
	writeFile(t, filepath.Join(dir, "uv.lock"), "version = 1\n")
	base := fp(t, dir)

	writeFile(t, filepath.Join(dir, "pyproject.toml"), "[project]\nname = \"x\"\ndependencies = [\"six==1.17.0\"]\n")
	if got := fp(t, dir); got == base {
		t.Fatal("editing pyproject.toml must change the fingerprint")
	}
	pyprojectChanged := fp(t, dir)

	writeFile(t, filepath.Join(dir, "uv.lock"), "version = 1\n# changed\n")
	if got := fp(t, dir); got == pyprojectChanged {
		t.Fatal("editing uv.lock must change the fingerprint")
	}
}

func TestFingerprintTemplateChangeDetected(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: python3.14\n")
	writeFile(t, filepath.Join(dir, "handler.py"), "def handler(e): pass\n")
	before := fp(t, dir)

	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: node24\n")
	after := fp(t, dir)
	if after == before {
		t.Fatal("template change should change fingerprint")
	}
}

// A service path change alone (same runtime, same handler) changes the
// fingerprint, because the fingerprint covers template.yaml contents verbatim.
// This is the regression guard for "path participates in fingerprinting": the
// reconciler decides staleness from the fingerprint, so a path-only edit must
// be visible without a second fingerprint mechanism.
func TestFingerprintServicePathChangeDetected(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "template.yaml"), `runtime: node24
services:
  - entrypoint: service.js
    host: api.example.com
    path: /v1
`)
	before := fp(t, dir)

	writeFile(t, filepath.Join(dir, "template.yaml"), `runtime: node24
services:
  - entrypoint: service.js
    host: api.example.com
    path: /v2
`)
	after := fp(t, dir)
	if after == before {
		t.Fatal("service path change should change fingerprint")
	}
}

// Covers file add, remove, and rename paths.
func TestFingerprintAddRemoveDetected(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: python3.14\n")
	writeFile(t, filepath.Join(dir, "a.txt"), "a\n")
	base := fp(t, dir)

	writeFile(t, filepath.Join(dir, "b.txt"), "b\n")
	if got := fp(t, dir); got == base {
		t.Fatal("adding a file should change fingerprint")
	}

	if err := os.Remove(filepath.Join(dir, "b.txt")); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if got := fp(t, dir); got != base {
		t.Fatal("removing an added file should restore the original fingerprint")
	}

	if err := os.Rename(filepath.Join(dir, "a.txt"), filepath.Join(dir, "c.txt")); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if got := fp(t, dir); got == base {
		t.Fatal("rename (path change) should change fingerprint")
	}
}

// Nested directory contents are included and their relative paths distinguish
// identical file names.
func TestFingerprintNestedDirs(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: python3.14\n")
	writeFile(t, filepath.Join(dir, "events", "a", "x.txt"), "hi\n")
	base := fp(t, dir)

	writeFile(t, filepath.Join(dir, "events", "b", "x.txt"), "hi\n")
	if got := fp(t, dir); got == base {
		t.Fatal("adding a same-named file in a different subdir should change fingerprint")
	}
}

func TestFingerprintUnreadableFileErrors(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: python3.14\n")
	writeFile(t, filepath.Join(dir, "secret.txt"), "s\n")
	if err := os.Chmod(filepath.Join(dir, "secret.txt"), 0); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if _, err := Fingerprint(dir); err == nil {
		t.Fatal("expected error for unreadable file")
	}
}

// TestFingerprintIgnoresIgnoredSource pins the core selection guarantee: bytes
// of a file excluded by the function's .gitignore are NOT hashed, so editing or
// removing it cannot force a rebuild.
func TestFingerprintIgnoresIgnoredSource(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, ".gitignore"), "*.log\n")
	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: python3.14\n")
	writeFile(t, filepath.Join(dir, "handler.py"), "def handler(e): return 1\n")
	writeFile(t, filepath.Join(dir, "debug.log"), "noise v1\n")
	base := fp(t, dir)

	// Editing an ignored file must not change the digest.
	writeFile(t, filepath.Join(dir, "debug.log"), "noise v2\n")
	if got := fp(t, dir); got != base {
		t.Fatal("editing an ignored file must not change the fingerprint")
	}

	// Adding and removing an ignored file must not change the digest.
	writeFile(t, filepath.Join(dir, "sub", "trace.log"), "more noise\n")
	if got := fp(t, dir); got != base {
		t.Fatal("adding an ignored file must not change the fingerprint")
	}
	if err := os.Remove(filepath.Join(dir, "sub", "trace.log")); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if got := fp(t, dir); got != base {
		t.Fatal("removing an ignored file must not change the fingerprint")
	}
}

// TestFingerprintTracksIgnoreRuleContent pins the change-detection half of the
// contract: the applicable .gitignore files ARE hashed, so editing a rule changes
// the digest even when no included file changes. Without this, a rule-only edit
// (which can change which files are source) would be invisible to the
// reconciler.
func TestFingerprintTracksIgnoreRuleContent(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, ".gitignore"), "*.log\n")
	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: python3.14\n")
	writeFile(t, filepath.Join(dir, "handler.py"), "def handler(e): return 1\n")
	base := fp(t, dir)

	writeFile(t, filepath.Join(dir, ".gitignore"), "*.log\n*.tmp\n")
	if got := fp(t, dir); got == base {
		t.Fatal("editing an applicable .gitignore must change the fingerprint")
	}
}

func TestFingerprintTracksAncestorIgnoreAfterSubselection(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "services", "fn")
	writeFile(t, filepath.Join(root, ".gitignore"), "*.generated\n")
	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: node24\n")
	writeFile(t, filepath.Join(dir, "handler.js"), "export const x = 1\n")

	sel, err := source.New(root, dir)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	before, err := FingerprintSelection(sel)
	if err != nil {
		t.Fatalf("before: %v", err)
	}
	writeFile(t, filepath.Join(root, ".gitignore"), "*.generated\nchanged-policy\n")
	after, err := FingerprintSelection(sel)
	if err != nil {
		t.Fatalf("after: %v", err)
	}
	if before == after {
		t.Fatal("ancestor ignore content must affect a subtree fingerprint")
	}
}

// TestFingerprintNestedIgnoreRuleChangeDetected proves a nested .gitignore's
// content is also policy: it changes the digest, and it correctly controls which
// files under its directory are hashed.
func TestFingerprintNestedIgnoreRuleChangeDetected(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: python3.14\n")
	writeFile(t, filepath.Join(dir, "pkg", ".gitignore"), "*.tmp\n")
	writeFile(t, filepath.Join(dir, "pkg", "handler.py"), "x=1\n")
	writeFile(t, filepath.Join(dir, "pkg", "scratch.tmp"), "noise\n")
	base := fp(t, dir)

	writeFile(t, filepath.Join(dir, "pkg", ".gitignore"), "*.tmp\n*.bak\n")
	if got := fp(t, dir); got == base {
		t.Fatal("editing a nested .gitignore must change the fingerprint")
	}

	// The nested rule excludes the file under it: editing that file does not
	// change the digest (compare against a fresh baseline with the new rule).
	withNewRule := fp(t, dir)
	writeFile(t, filepath.Join(dir, "pkg", "scratch.tmp"), "different noise\n")
	if got := fp(t, dir); got != withNewRule {
		t.Fatal("editing a file ignored by a nested rule must not change the fingerprint")
	}
}

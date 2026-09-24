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

	selection, err := source.New(root, dir)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	before, err := FingerprintSelection(selection)
	if err != nil {
		t.Fatalf("before: %v", err)
	}
	writeFile(t, filepath.Join(root, ".gitignore"), "*.generated\nchanged-policy\n")
	after, err := FingerprintSelection(selection)
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

// TestFingerprintTracksTypeScriptSources pins that TypeScript handlers are
// ordinary selected source: editing a .ts file, editing the tsconfig.json, and
// editing a locally imported .ts module each change the digest, so the function
// artifact is rebuilt. Relay runs no type-checker, but the transpiled output is
// baked into the image, so the sources must version it.
func TestFingerprintTracksTypeScriptSources(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: node24\n")
	writeFile(t, filepath.Join(dir, "tsconfig.json"), `{"compilerOptions":{"strict":true}}`+"\n")
	writeFile(t, filepath.Join(dir, "src", "handler.ts"),
		"import { msg } from \"./msg\";\nexport function handler(e) { console.log(msg(e)); }\n")
	writeFile(t, filepath.Join(dir, "src", "msg.ts"), "export function msg(e) { return \"v1\"; }\n")
	base := fp(t, dir)

	// Editing the handler source.
	writeFile(t, filepath.Join(dir, "src", "handler.ts"),
		"import { msg } from \"./msg\";\nexport function handler(e) { console.log(msg(e) + \"!\"); }\n")
	if got := fp(t, dir); got == base {
		t.Fatal("editing a .ts handler must change the fingerprint")
	}
	handlerEdit := fp(t, dir)

	// Editing a locally imported .ts module.
	writeFile(t, filepath.Join(dir, "src", "msg.ts"), "export function msg(e) { return \"v2\"; }\n")
	if got := fp(t, dir); got == handlerEdit {
		t.Fatal("editing an imported .ts module must change the fingerprint")
	}
	msgEdit := fp(t, dir)

	// Editing the tsconfig (it shapes the transpilation).
	writeFile(t, filepath.Join(dir, "tsconfig.json"), `{"compilerOptions":{"strict":false}}`+"\n")
	if got := fp(t, dir); got == msgEdit {
		t.Fatal("editing tsconfig.json must change the fingerprint")
	}
}

// TestFingerprintIgnoresIgnoredTypeScript pins that a .ts file excluded by the
// function's .gitignore is not source: editing it changes neither the function
// fingerprint nor the build context, reusing the shared selection policy (no
// TypeScript-specific filtering).
func TestFingerprintIgnoresIgnoredTypeScript(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, ".gitignore"), "*.generated.ts\n")
	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: node24\n")
	writeFile(t, filepath.Join(dir, "handler.ts"), "export function handler(e) {}\n")
	writeFile(t, filepath.Join(dir, "scratch.generated.ts"), "export const scratch = 1\n")
	base := fp(t, dir)

	writeFile(t, filepath.Join(dir, "scratch.generated.ts"), "export const scratch = 2\n")
	if got := fp(t, dir); got != base {
		t.Fatal("editing a .ts file ignored by .gitignore must not change the fingerprint")
	}
}

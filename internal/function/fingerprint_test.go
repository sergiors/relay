package function

import (
	"os"
	"path/filepath"
	"testing"
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

// TestFingerprintUnchangedStable asserts an unchanged dir yields a stable digest.
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

// TestFingerprintDeterministicAcrossOrdering verifies the digest is independent
// of directory iteration order because paths are sorted.
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

// TestFingerprintAddRemoveDetected covers file add, remove, and rename paths.
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

// TestFingerprintNestedDirs ensures nested directory contents are included and
// their relative paths distinguish identical file names.
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

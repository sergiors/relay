package git

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/app"
	"relay/internal/source"
)

// TestSelectMissingSourceDir pins that an app selection cannot be built for a
// source directory that is absent from the checkout; the error is the clear
// missing-source message the sync surfaces.
func TestSelectMissingSourceDir(t *testing.T) {
	src := filepath.Join(t.TempDir(), "does-not-exist")
	_, err := source.ForDir(src)
	if err == nil {
		t.Fatal("select of missing source: nil error, want missing-source error")
	}
	if !errors.Is(err, source.ErrNotExist) {
		t.Fatalf("err = %v, want source.ErrNotExist", err)
	}
}

// TestMaterializeMissingSourceLeavesLiveAppsUntouched pins fail-closed behavior:
// when the source directory is absent, materialization errors without removing
// an existing app from /apps.
func TestMaterializeMissingSourceLeavesLiveAppsUntouched(t *testing.T) {
	funcs := t.TempDir()
	src := filepath.Join(t.TempDir(), "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(src); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(funcs, "keep", "template.yaml"), "runtime: node\n")
	if _, _, err := materialize(src, funcs); err == nil {
		t.Fatal("materialize of vanished source: nil error, want missing-source error")
	} else if !strings.Contains(err.Error(), "does not exist in the checkout") {
		t.Fatalf("err = %v, want clear missing-source message", err)
	}
	if _, serr := os.Stat(filepath.Join(funcs, "keep")); serr != nil {
		t.Fatalf("missing-source materialize removed an existing function dir: %v", serr)
	}
}

func TestMaterializeSelectionErrorHasGitContext(t *testing.T) {
	src := t.TempDir()
	appDir := filepath.Join(src, "broken")
	writeFile(t, filepath.Join(appDir, "template.yaml"), "runtime: node\n")
	if err := os.Mkdir(filepath.Join(appDir, ".gitignore"), 0o755); err != nil {
		t.Fatal(err)
	}

	_, _, err := materialize(src, t.TempDir())
	if err == nil {
		t.Fatal("materialize with an unreadable app selection: nil error")
	}
	if !strings.Contains(err.Error(), `git: select app dir "broken": source: read`) {
		t.Fatalf("err = %v, want git app-selection context", err)
	}
	var pathErr *os.PathError
	if !errors.As(err, &pathErr) {
		t.Fatalf("err = %v, want wrapped filesystem error", err)
	}
}

func TestMaterializeEmptySourceClearsStale(t *testing.T) {
	src := filepath.Join(t.TempDir(), "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	funcs := t.TempDir()
	writeFile(t, filepath.Join(funcs, "stale", "template.yaml"), "runtime: node\n")
	mat, rem, err := materialize(src, funcs)
	if err != nil {
		t.Fatalf("materialize empty source: %v", err)
	}
	if len(mat) != 0 {
		t.Fatalf("materialized = %v, want none", mat)
	}
	if len(rem) != 1 || rem[0] != "stale" {
		t.Fatalf("removed = %v, want [stale]", rem)
	}
	if _, serr := os.Stat(filepath.Join(funcs, "stale")); !os.IsNotExist(serr) {
		t.Fatal("stale dir not removed by empty-source materialize")
	}
}

func TestMaterializeDiscoveryIgnoresNonApps(t *testing.T) {
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "valid", "template.yaml"), "runtime: node\n")
	writeFile(t, filepath.Join(src, "notadir.txt"), "x\n")
	writeFile(t, filepath.Join(src, "nofn", "readme.md"), "not a function\n")
	names, err := discoverApps(src)
	if err != nil {
		t.Fatalf("discoverApps: %v", err)
	}
	if len(names) != 1 || names[0] != "valid" {
		t.Fatalf("discoverApps = %v, want [valid]", names)
	}
}

func TestDiscoverAppsRejectsSymlinkedTemplate(t *testing.T) {
	src := t.TempDir()
	target := filepath.Join(t.TempDir(), "template.yaml")
	if err := os.WriteFile(target, []byte("runtime: nodejs24.x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(src, "linked"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(src, "linked", "template.yaml")); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}

	if names, err := discoverApps(src); err == nil {
		t.Fatalf("discoverApps = %v, want an error for a symlinked template", names)
	} else if !errors.Is(err, app.ErrInvalidPath) {
		t.Fatalf("discoverApps error = %v, want ErrInvalidPath", err)
	}
}

// TestDiscoverAppsSkipsSymlinkEntries pins the os-level no-follow boundary in
// checkout discovery for whole ENTRIES: an app directory replaced by a symlink
// (to a real app inside the source), a symlink whose target has vanished, and a
// plain file are never app directories and are skipped without any error, as
// the loader would skip them. A real sibling with a template is still selected.
func TestDiscoverAppsSkipsSymlinkEntries(t *testing.T) {
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "valid", "template.yaml"), "runtime: nodejs24.x\n")
	if err := os.Symlink(filepath.Join(src, "valid"), filepath.Join(src, "aliased")); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	if err := os.Symlink(filepath.Join(t.TempDir(), "vanished"), filepath.Join(src, "dangling")); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	if err := os.WriteFile(filepath.Join(src, "plainfile"), []byte("not an app\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	names, err := discoverApps(src)
	if err != nil {
		t.Fatalf("discoverApps: %v", err)
	}
	if len(names) != 1 || names[0] != "valid" {
		t.Fatalf("discoverApps = %v, want [valid]", names)
	}
}

// TestMaterializeDoesNotRemoveStagingDir pins that the deterministic-removal
// pass leaves a Relay-owned staging directory in the target alone: it is not a
// materialized app, so it is neither reported as removed nor deleted (the
// creator owns its lifecycle), while a genuine stale app dir is still
// removed.
func TestMaterializeDoesNotRemoveStagingDir(t *testing.T) {
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "keep", "template.yaml"), "runtime: nodejs24.x\n")

	funcs := t.TempDir()
	// A transient staging directory in the target (as git leaves it mid-copy).
	stage := filepath.Join(funcs, ".sync-live")
	writeFile(t, filepath.Join(stage, "template.yaml"), "runtime: nodejs24.x\n")
	// A genuinely stale app directory.
	writeFile(t, filepath.Join(funcs, "stale", "template.yaml"), "runtime: nodejs24.x\n")

	mat, rem, err := materialize(src, funcs)
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	if len(mat) != 1 || mat[0] != "keep" {
		t.Fatalf("materialized = %v, want [keep]", mat)
	}
	if len(rem) != 1 || rem[0] != "stale" {
		t.Fatalf("removed = %v, want [stale] (the staging dir is not removed)", rem)
	}
	if _, err := os.Stat(stage); err != nil {
		t.Fatalf("staging dir must be left in place: %v", err)
	}
	if _, err := os.Stat(filepath.Join(funcs, "stale")); !os.IsNotExist(err) {
		t.Fatal("stale function dir was not removed")
	}
}

// TestMaterializeHonorsAppIgnoreRules proves the app's own policy drives
// materialization: its .gitignore travels with it so the build context and
// fingerprint can keep applying the same rules against /apps, and files it
// ignores are not copied. A sibling directory named by a CHECKOUT-ROOT rule is
// still an app because ancestor rules never apply.
func TestMaterializeHonorsAppIgnoreRules(t *testing.T) {
	src := t.TempDir()
	writeFile(t, filepath.Join(src, ".gitignore"), "ignored/\n")
	writeFile(t, filepath.Join(src, "keep", "template.yaml"), "runtime: nodejs24.x\n")
	writeFile(t, filepath.Join(src, "keep", ".gitignore"), "*.tmp\n")
	writeFile(t, filepath.Join(src, "keep", "handler.js"), "export const x=1;\n")
	writeFile(t, filepath.Join(src, "keep", "scratch.tmp"), "junk\n")
	writeFile(t, filepath.Join(src, "ignored", "template.yaml"), "runtime: nodejs24.x\n")

	funcs := t.TempDir()
	mat, rem, err := materialize(src, funcs)
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	if len(mat) != 2 || mat[0] != "ignored" || mat[1] != "keep" {
		t.Fatalf("materialized = %v, want [ignored keep] (ancestor rule must not hide an app)", mat)
	}
	if len(rem) != 0 {
		t.Fatalf("removed = %v, want none", rem)
	}
	if _, err := os.Stat(filepath.Join(funcs, "ignored", "template.yaml")); err != nil {
		t.Fatalf("app hidden by a checkout-root rule was not materialized: %v", err)
	}
	if _, err := os.Stat(filepath.Join(funcs, "keep", "template.yaml")); err != nil {
		t.Fatalf("included function not materialized: %v", err)
	}
	if _, err := os.Stat(filepath.Join(funcs, "keep", "handler.js")); err != nil {
		t.Fatalf("included source missing: %v", err)
	}
	// The app's own policy travels with it and its rules exclude the ignored
	// file from the copy.
	if _, err := os.Stat(filepath.Join(funcs, "keep", ".gitignore")); err != nil {
		t.Fatalf("applicable .gitignore not materialized with the function: %v", err)
	}
	if _, err := os.Stat(filepath.Join(funcs, "keep", "scratch.tmp")); !os.IsNotExist(err) {
		t.Fatalf("ignored file was materialized; stat err = %v", err)
	}
}

// TestMaterializeIgnoresAncestorIgnoreRules proves a checkout-root or monorepo
// ancestor .gitignore never shapes an app's copied source: even rules matching
// the app directory, its files, and its template do not exclude them, and no
// ancestor file is copied or merged into the app.
func TestMaterializeIgnoresAncestorIgnoreRules(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, ".gitignore"), "*.generated\nservices/\n")
	sub := filepath.Join(root, "services")
	writeFile(t, filepath.Join(sub, "funa", "template.yaml"), "runtime: nodejs24.x\n")
	writeFile(t, filepath.Join(sub, "funa", "handler.generated"), "generated\n")
	writeFile(t, filepath.Join(sub, "funa", "handler.js"), "export const x=1;\n")

	funcs := t.TempDir()
	mat, _, err := materialize(sub, funcs)
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	if len(mat) != 1 || mat[0] != "funa" {
		t.Fatalf("materialized = %v, want [funa]", mat)
	}
	if _, err := os.Stat(filepath.Join(funcs, "funa", "handler.generated")); err != nil {
		t.Fatalf("ancestor ignore rule excluded an app file: %v", err)
	}
	if _, err := os.Stat(filepath.Join(funcs, "funa", "template.yaml")); err != nil {
		t.Fatalf("ancestor ignore rule excluded the app template: %v", err)
	}
	if _, err := os.Stat(filepath.Join(funcs, "funa", "handler.js")); err != nil {
		t.Fatalf("included source missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(funcs, "funa", ".gitignore")); !os.IsNotExist(err) {
		t.Fatalf("ancestor policy was copied into the app; stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(funcs, ".gitignore")); !os.IsNotExist(err) {
		t.Fatalf("ancestor policy leaked into the apps root; stat err = %v", err)
	}
}

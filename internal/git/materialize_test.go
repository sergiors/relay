package git

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/source"
)

// mustSelect resolves a selection rooted at root for dir, failing the test on
// error. Tests use it to drive materialize with the shared policy explicitly.
func mustSelect(t *testing.T, root, dir string) *source.Selection {
	t.Helper()
	sel, err := source.New(root, dir)
	if err != nil {
		t.Fatalf("select %q under %q: %v", dir, root, err)
	}
	return sel
}

// TestSelectMissingSourceDir pins that a Selection cannot be built for a source
// directory that is absent from the checkout; the error is the clear
// missing-source message the sync surfaces (sync.go maps source.ErrNotExist to the
// same wording).
func TestSelectMissingSourceDir(t *testing.T) {
	src := filepath.Join(t.TempDir(), "does-not-exist")
	_, err := source.New(t.TempDir(), src)
	if err == nil {
		t.Fatal("select of missing source: nil error, want missing-source error")
	}
	if !errors.Is(err, source.ErrNotExist) {
		t.Fatalf("err = %v, want source.ErrNotExist", err)
	}
}

// TestMaterializeMissingSourceRace pins the TOCTOU guard: if the source dir
// vanishes between Selection construction and the materialize read, materialize
// returns the clear missing-source error and leaves /functions untouched (even a
// pre-existing dir is NOT removed, so a failed sync cannot erase valid functions).
func TestMaterializeMissingSourceRace(t *testing.T) {
	funcs := t.TempDir()
	src := filepath.Join(t.TempDir(), "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	sel := mustSelect(t, src, src)
	if err := os.RemoveAll(src); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(funcs, "keep", "template.yaml"), "runtime: node\n")
	if _, _, err := materialize(sel, funcs); err == nil {
		t.Fatal("materialize of vanished source: nil error, want missing-source error")
	} else if !strings.Contains(err.Error(), "does not exist in the checkout") {
		t.Fatalf("err = %v, want clear missing-source message", err)
	}
	if _, serr := os.Stat(filepath.Join(funcs, "keep")); serr != nil {
		t.Fatalf("missing-source materialize removed an existing function dir: %v", serr)
	}
}

func TestMaterializeEmptySourceClearsStale(t *testing.T) {
	src := filepath.Join(t.TempDir(), "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	funcs := t.TempDir()
	writeFile(t, filepath.Join(funcs, "stale", "template.yaml"), "runtime: node\n")
	mat, rem, err := materialize(mustSelect(t, src, src), funcs)
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

func TestMaterializeDiscoveryIgnoresNonFunctions(t *testing.T) {
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "valid", "template.yaml"), "runtime: node\n")
	writeFile(t, filepath.Join(src, "notadir.txt"), "x\n")
	writeFile(t, filepath.Join(src, "nofn", "readme.md"), "not a function\n")
	names, err := discoverFunctions(mustSelect(t, src, src))
	if err != nil {
		t.Fatalf("discoverFunctions: %v", err)
	}
	if len(names) != 1 || names[0] != "valid" {
		t.Fatalf("discoverFunctions = %v, want [valid]", names)
	}
}

// TestMaterializeHonorsIgnoreRules proves the shared policy drives
// materialization: a function directory excluded by the source root's
// .gitignore is not copied, while an included sibling is; and a .gitignore
// inside the function travels with it so the build context and fingerprint can
// keep applying the same rules against /functions. Ignored files inside the
// function are not copied either.
func TestMaterializeHonorsIgnoreRules(t *testing.T) {
	src := t.TempDir()
	writeFile(t, filepath.Join(src, ".gitignore"), "ignored/\n")
	writeFile(t, filepath.Join(src, "keep", "template.yaml"), "runtime: node24\n")
	writeFile(t, filepath.Join(src, "keep", ".gitignore"), "*.tmp\n")
	writeFile(t, filepath.Join(src, "keep", "handler.js"), "export const x=1;\n")
	writeFile(t, filepath.Join(src, "keep", "scratch.tmp"), "junk\n")
	writeFile(t, filepath.Join(src, "ignored", "template.yaml"), "runtime: node24\n")

	funcs := t.TempDir()
	mat, rem, err := materialize(mustSelect(t, src, src), funcs)
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	if len(mat) != 1 || mat[0] != "keep" {
		t.Fatalf("materialized = %v, want [keep] (ignored/ is excluded)", mat)
	}
	if len(rem) != 0 {
		t.Fatalf("removed = %v, want none", rem)
	}
	if _, err := os.Stat(filepath.Join(funcs, "ignored")); !os.IsNotExist(err) {
		t.Fatalf("ignored function dir was materialized; stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(funcs, "keep", "template.yaml")); err != nil {
		t.Fatalf("included function not materialized: %v", err)
	}
	if _, err := os.Stat(filepath.Join(funcs, "keep", "handler.js")); err != nil {
		t.Fatalf("included source missing: %v", err)
	}
	// The function's own policy travels with it and its rules exclude the
	// ignored file from the copy.
	if _, err := os.Stat(filepath.Join(funcs, "keep", ".gitignore")); err != nil {
		t.Fatalf("applicable .gitignore not materialized with the function: %v", err)
	}
	if _, err := os.Stat(filepath.Join(funcs, "keep", "scratch.tmp")); !os.IsNotExist(err) {
		t.Fatalf("ignored file was materialized; stat err = %v", err)
	}
}

// TestMaterializeAnchorsRootIgnoreRules proves a root-level .gitignore governs a
// monorepo subtree selection: a rule anchored at the checkout root excludes a
// path beneath the selected subtree, exactly as git would apply it.
func TestMaterializeAnchorsRootIgnoreRules(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, ".gitignore"), "*.generated\n")
	sub := filepath.Join(root, "services")
	writeFile(t, filepath.Join(sub, "funa", "template.yaml"), "runtime: node24\n")
	writeFile(t, filepath.Join(sub, "funa", "handler.generated"), "generated\n")
	writeFile(t, filepath.Join(sub, "funa", "handler.js"), "export const x=1;\n")

	funcs := t.TempDir()
	mat, _, err := materialize(mustSelect(t, root, sub), funcs)
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	if len(mat) != 1 || mat[0] != "funa" {
		t.Fatalf("materialized = %v, want [funa]", mat)
	}
	if _, err := os.Stat(filepath.Join(funcs, "funa", "handler.generated")); !os.IsNotExist(err) {
		t.Fatalf("root-anchored ignore rule did not exclude the file; stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(funcs, "funa", "handler.js")); err != nil {
		t.Fatalf("included source missing: %v", err)
	}
	// The root policy is carried into the function so a later fingerprint and
	// build context retain the same applicable policy after sync.
	if _, err := os.Stat(filepath.Join(funcs, "funa", ".gitignore")); err != nil {
		t.Fatalf("ancestor .gitignore was not preserved: %v", err)
	}
}

package git

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMaterializeMissingSourceDir(t *testing.T) {
	funcs := t.TempDir()
	src := filepath.Join(t.TempDir(), "does-not-exist")
	_, _, err := materialize(src, funcs)
	if err == nil {
		t.Fatal("materialize of missing source: nil error, want missing-source error")
	}
	if !strings.Contains(err.Error(), "does not exist in the checkout") {
		t.Fatalf("err = %v, want clear missing-source message", err)
	}
	// /functions must remain untouched: even a pre-existing dir is NOT removed.
	writeFile(t, filepath.Join(funcs, "keep", "template.yaml"), "runtime: node\n")
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

func TestMaterializeDiscoveryIgnoresNonFunctions(t *testing.T) {
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "valid", "template.yaml"), "runtime: node\n")
	writeFile(t, filepath.Join(src, "notadir.txt"), "x\n")
	writeFile(t, filepath.Join(src, "nofn", "readme.md"), "not a function\n")
	names, err := discoverFunctions(src)
	if err != nil {
		t.Fatalf("discoverFunctions: %v", err)
	}
	if len(names) != 1 || names[0] != "valid" {
		t.Fatalf("discoverFunctions = %v, want [valid]", names)
	}
}

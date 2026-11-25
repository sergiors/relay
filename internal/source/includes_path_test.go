package source

import (
	"path/filepath"
	"testing"
)

// TestIncludesPathRefusesPathsOutsideAppRoot pins the documented contract of
// IncludesPath: "A path outside the selected directory is not included." The
// selection root is the app directory, so nothing above it may ever be
// selected as source: not the parent checkout's files, not the parent
// directory itself, and not a sibling whose name merely shares the app's name
// as a prefix (app vs app-sibling must not confuse a boundary check).
//
// Regression guard: Selection.Rel must reject filepath.Rel results of ".." or
// "../..." before Includes can treat them as non-matches (which mean
// "included"). The in-root controls below ensure app-local policy is unchanged.
func TestIncludesPathRefusesPathsOutsideAppRoot(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, ".gitignore"), "*.log\n")
	dir := filepath.Join(root, "app")
	writeFile(t, filepath.Join(dir, ".gitignore"), "*.tmp\n")
	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: nodejs24.x\n")
	writeFile(t, filepath.Join(dir, "handler.js"), "export function h() {}\n")
	writeFile(t, filepath.Join(dir, "scratch.tmp"), "noise\n")
	sibling := filepath.Join(root, "app-sibling")
	writeFile(t, filepath.Join(sibling, "leaked.js"), "export function l() {}\n")

	selection, err := ForDir(dir)
	if err != nil {
		t.Fatalf("ForDir: %v", err)
	}

	outside := []struct {
		name  string
		path  string
		isDir bool
	}{
		{"sibling sharing the app name prefix", filepath.Join(sibling, "leaked.js"), false},
		{"sibling directory sharing the app name prefix", sibling, true},
		{"ancestor ignore file", filepath.Join(root, ".gitignore"), false},
		{"ancestor directory", root, true},
	}
	for _, tc := range outside {
		if selection.IncludesPath(tc.path, tc.isDir) {
			t.Errorf("%s: IncludesPath(%q) = true, want false: the path is outside the app root",
				tc.name, tc.path)
		}
	}

	if !selection.IncludesPath(dir, true) {
		t.Error("the selected root itself must be included")
	}
	if !selection.IncludesPath(filepath.Join(dir, "handler.js"), false) {
		t.Error("an included file inside the app root must stay included")
	}
	if selection.IncludesPath(filepath.Join(dir, "scratch.tmp"), false) {
		t.Error("an app-local ignored file must stay excluded")
	}
}

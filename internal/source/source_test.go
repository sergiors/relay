package source

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
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

// walkNames returns the root-relative slash paths WalkDir visits, sorted. The
// selected root itself is omitted: every walk necessarily visits it, and the
// tests are about which descendants are traversed.
func walkNames(t *testing.T, selection *Selection) []string {
	t.Helper()
	var names []string
	err := selection.WalkDir(func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == selection.Dir() {
			return nil
		}
		rel, rerr := filepath.Rel(selection.Root(), path)
		if rerr != nil {
			return rerr
		}
		names = append(names, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	sort.Strings(names)
	return names
}

func TestForDirExcludesIgnoredFiles(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, ".gitignore"), "*.log\n")
	writeFile(t, filepath.Join(dir, "handler.py"), "x=1\n")
	writeFile(t, filepath.Join(dir, "debug.log"), "noise\n")
	writeFile(t, filepath.Join(dir, "sub", "trace.log"), "noise\n")
	writeFile(t, filepath.Join(dir, "sub", "lib.py"), "y=2\n")

	selection, err := ForDir(dir)
	if err != nil {
		t.Fatalf("ForDir: %v", err)
	}

	got := walkNames(t, selection)
	want := []string{".gitignore", "handler.py", "sub", "sub/lib.py"}
	if len(got) != len(want) {
		t.Fatalf("walk = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("walk = %v, want %v", got, want)
		}
	}
	if selection.Includes("debug.log", false) {
		t.Error("debug.log should be excluded")
	}
	if !selection.Includes(".gitignore", false) {
		t.Error("the applicable .gitignore must always be included")
	}
	if !selection.Includes(".", true) {
		t.Error("the selected root must always be included")
	}
}

func TestIncludesNeverMatchesGitDir(t *testing.T) {
	dir := t.TempDir()
	selection, err := ForDir(dir)
	if err != nil {
		t.Fatalf("ForDir: %v", err)
	}
	if selection.Includes(".git/config", false) {
		t.Error(".git contents must never be source")
	}
}

func TestNewAnchorsAncestorRulesToRoot(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, ".gitignore"), "*.generated\n")
	sub := filepath.Join(root, "services")
	writeFile(t, filepath.Join(sub, "handler.generated"), "gen\n")
	writeFile(t, filepath.Join(sub, "handler.js"), "js\n")

	selection, err := New(root, sub)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if selection.Root() != root || selection.Dir() != sub {
		t.Fatalf("root/dir = %q/%q, want %q/%q", selection.Root(), selection.Dir(), root, sub)
	}
	if selection.Includes("services/handler.generated", false) {
		t.Error("root-anchored rule must exclude a path beneath the selected subtree")
	}
	if !selection.Includes("services/handler.js", false) {
		t.Error("unmatched path beneath the subtree must be included")
	}
}

// TestNewSelectedDirExplicitDespiteAncestorIgnore pins that the operator-chosen
// selected directory is itself always source even when a root rule would ignore
// it: the walk visits the selected dir rather than skipping it like any other
// ignored directory. (Its CONTENTS remain subject to the rules: a `services/`
// dirOnly rule still excludes everything beneath, exactly as git treats an
// ignored directory.)
func TestNewSelectedDirExplicitDespiteAncestorIgnore(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, ".gitignore"), "services/\n")
	sub := filepath.Join(root, "services")
	writeFile(t, filepath.Join(sub, "handler.js"), "js\n")

	selection, err := New(root, sub)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	visitedRoot := false
	err = selection.WalkDir(func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == selection.Dir() {
			visitedRoot = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if !visitedRoot {
		t.Fatal("the explicitly selected directory must be visited even if an ancestor rule ignores it")
	}
	// A rule matching the selected dir still governs its contents.
	if selection.Includes("services/handler.js", false) {
		t.Error("dirOnly ancestor rule must still exclude descendants of the selected dir")
	}
}

func TestNestedGitignoreDoesNotReincludeIgnoredParent(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, ".gitignore"), "build/\n")
	// A nested file that would re-include; git never reads it because build/ is
	// ignored, so it must have no effect.
	writeFile(t, filepath.Join(dir, "build", ".gitignore"), "!keep.txt\n")
	writeFile(t, filepath.Join(dir, "build", "keep.txt"), "x\n")

	selection, err := ForDir(dir)
	if err != nil {
		t.Fatalf("ForDir: %v", err)
	}
	got := walkNames(t, selection)
	if len(got) != 1 || got[0] != ".gitignore" {
		t.Fatalf("walk = %v, want [.gitignore] (build/ fully ignored)", got)
	}
}

func TestDeeperRulesOverrideShallower(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, ".gitignore"), "*.tmp\n")
	writeFile(t, filepath.Join(dir, "sub", ".gitignore"), "!keep.tmp\n")
	writeFile(t, filepath.Join(dir, "sub", "keep.tmp"), "x\n")
	writeFile(t, filepath.Join(dir, "sub", "drop.tmp"), "y\n")

	selection, err := ForDir(dir)
	if err != nil {
		t.Fatalf("ForDir: %v", err)
	}
	if !selection.Includes("sub/keep.tmp", false) {
		t.Error("deeper negation must re-include sub/keep.tmp")
	}
	if selection.Includes("sub/drop.tmp", false) {
		t.Error("inherited rule must still exclude sub/drop.tmp")
	}
}

func TestSubSharesRootAndRules(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, ".gitignore"), "*.log\n")
	writeFile(t, filepath.Join(dir, "fn", "handler.py"), "x\n")
	writeFile(t, filepath.Join(dir, "fn", "debug.log"), "noise\n")

	selection, err := ForDir(dir)
	if err != nil {
		t.Fatalf("ForDir: %v", err)
	}
	sub, err := selection.Sub(filepath.Join(dir, "fn"))
	if err != nil {
		t.Fatalf("Sub: %v", err)
	}
	if sub.Root() != dir {
		t.Fatalf("Sub root = %q, want %q (rules stay anchored)", sub.Root(), dir)
	}
	got := walkNames(t, sub)
	if len(got) != 1 || got[0] != "fn/handler.py" {
		t.Fatalf("sub walk = %v, want [fn/handler.py]", got)
	}
}

// TestIgnoreFileAlwaysIncludedEvenWhenMatched pins that a rule which would
// otherwise exclude the policy file itself cannot do so: the .gitignore is part
// of the selection policy, so dropping it from a copy or a fingerprint would make
// the policy invisible.
func TestIgnoreFileAlwaysIncludedEvenWhenMatched(t *testing.T) {
	dir := t.TempDir()
	// A pattern that matches the ignore file's own name.
	writeFile(t, filepath.Join(dir, ".gitignore"), ".gitignore\n*.log\n")
	writeFile(t, filepath.Join(dir, "handler.py"), "x\n")

	selection, err := ForDir(dir)
	if err != nil {
		t.Fatalf("ForDir: %v", err)
	}
	if !selection.Includes(".gitignore", false) {
		t.Error(".gitignore must always be source even when a rule matches it")
	}
	got := walkNames(t, selection)
	if len(got) != 2 || got[0] != ".gitignore" || got[1] != "handler.py" {
		t.Fatalf("walk = %v, want [.gitignore handler.py]", got)
	}
}

func TestIgnoreFilesListsOnlyExistingFiles(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, ".gitignore"), "*.log\n")
	writeFile(t, filepath.Join(dir, "sub", ".gitignore"), "*.tmp\n")
	writeFile(t, filepath.Join(dir, "sub", "x.py"), "x\n")
	writeFile(t, filepath.Join(dir, "other", "y.py"), "y\n")

	selection, err := ForDir(dir)
	if err != nil {
		t.Fatalf("ForDir: %v", err)
	}
	got := selection.IgnoreFiles()
	want := []string{filepath.Join(dir, ".gitignore"), filepath.Join(dir, "sub", ".gitignore")}
	if len(got) != len(want) {
		t.Fatalf("IgnoreFiles = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("IgnoreFiles = %v, want %v", got, want)
		}
	}
}

func TestNewMissingDirWrapsErrNotExist(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope")
	_, err := ForDir(missing)
	if !errors.Is(err, ErrNotExist) {
		t.Fatalf("err = %v, want ErrNotExist", err)
	}
}

func TestNewRejectsDirOutsideRoot(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if _, err := New(root, outside); err == nil {
		t.Fatal("New with dir outside root: nil error, want error")
	}
}

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

// walkNames returns the directory-relative slash paths WalkDir visits, sorted.
// The selected root itself is omitted: every walk necessarily visits it, and
// the tests are about which descendants are traversed.
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
		rel, rerr := selection.Rel(path)
		if rerr != nil {
			return rerr
		}
		names = append(names, rel)
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

// TestForDirIgnoresAncestorIgnoreRules pins that an app selection is
// self-contained: a checkout-root or monorepo-parent .gitignore never applies to
// files under the app directory, even when its pattern would match them and even
// when it ignores the app directory itself or its template.
func TestForDirIgnoresAncestorIgnoreRules(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, ".gitignore"), "*.generated\nservices/\n")
	dir := filepath.Join(root, "services", "fn")
	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: nodejs24.x\n")
	writeFile(t, filepath.Join(dir, "handler.generated"), "gen\n")
	writeFile(t, filepath.Join(dir, "handler.js"), "js\n")

	selection, err := ForDir(dir)
	if err != nil {
		t.Fatalf("ForDir: %v", err)
	}
	if selection.Dir() != dir {
		t.Fatalf("Dir = %q, want %q", selection.Dir(), dir)
	}
	if !selection.Includes("handler.generated", false) {
		t.Error("an ancestor rule must not exclude an app file")
	}
	if !selection.Includes("template.yaml", false) {
		t.Error("the app template must always be source")
	}
	got := walkNames(t, selection)
	want := []string{"handler.generated", "handler.js", "template.yaml"}
	if len(got) != len(want) {
		t.Fatalf("walk = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("walk = %v, want %v", got, want)
		}
	}
}

// TestTemplateAlwaysIncludedEvenWhenMatched pins that the mandatory app
// template is source regardless of a local ignore rule. Discovery and the loader
// require it, so a rule that matches template.yaml cannot stage or fingerprint an
// app without its definition.
func TestTemplateAlwaysIncludedEvenWhenMatched(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, ".gitignore"), "template.yaml\n")
	writeFile(t, filepath.Join(dir, "template.yaml"), "runtime: nodejs24.x\n")
	writeFile(t, filepath.Join(dir, "handler.js"), "js\n")

	selection, err := ForDir(dir)
	if err != nil {
		t.Fatalf("ForDir: %v", err)
	}
	if !selection.Includes("template.yaml", false) {
		t.Error("template.yaml must always be source")
	}
	got := walkNames(t, selection)
	want := []string{".gitignore", "handler.js", "template.yaml"}
	if len(got) != len(want) {
		t.Fatalf("walk = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("walk = %v, want %v", got, want)
		}
	}
}

// TestForDirPreservesAnchoredAndRelativePatterns pins that making the app
// directory the selection root does not weaken standard .gitignore semantics:
// a leading slash anchors a pattern to the app root, a slash-containing
// pattern is relative to that root, and a negation still re-includes a file in
// an otherwise matched directory.
func TestForDirPreservesAnchoredAndRelativePatterns(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, ".gitignore"),
		"/root-only.txt\nsub/*.log\n!sub/keep.log\n")
	writeFile(t, filepath.Join(dir, "root-only.txt"), "root\n")
	writeFile(t, filepath.Join(dir, "nested", "root-only.txt"), "nested\n")
	writeFile(t, filepath.Join(dir, "sub", "drop.log"), "drop\n")
	writeFile(t, filepath.Join(dir, "sub", "keep.log"), "keep\n")
	writeFile(t, filepath.Join(dir, "sub", "nested", "other.log"), "other\n")

	selection, err := ForDir(dir)
	if err != nil {
		t.Fatalf("ForDir: %v", err)
	}
	got := walkNames(t, selection)
	want := []string{
		".gitignore", "nested", "nested/root-only.txt",
		"sub", "sub/keep.log", "sub/nested", "sub/nested/other.log",
	}
	if len(got) != len(want) {
		t.Fatalf("walk = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("walk = %v, want %v", got, want)
		}
	}
	if selection.Includes("root-only.txt", false) {
		t.Error("an anchored /root-only.txt must exclude only the app-root file")
	}
	if selection.Includes("sub/drop.log", false) {
		t.Error("sub/*.log must exclude a log directly under sub")
	}
	if !selection.Includes("sub/keep.log", false) {
		t.Error("!sub/keep.log must re-include the matched file")
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

// TestSiblingAppsApplyOwnRulesIndependently pins that rooting the selection at
// each app directory isolates policies across sibling apps: a negation, an
// anchored rule, and a directory exclusion in one app's .gitignore never reach
// a sibling, a checkout-root rule reaches neither, and each app's own and
// nested rules stay fully effective on their own.
func TestSiblingAppsApplyOwnRulesIndependently(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, ".gitignore"), "*.log\n*.generated\nfunb/\n")

	funa := filepath.Join(root, "funa")
	writeFile(t, filepath.Join(funa, ".gitignore"), "*.log\n!keep.log\n/root-only.txt\nbuild/\n")
	writeFile(t, filepath.Join(funa, "template.yaml"), "runtime: nodejs24.x\n")
	writeFile(t, filepath.Join(funa, "handler.js"), "a\n")
	writeFile(t, filepath.Join(funa, "handler.generated"), "gen\n")
	writeFile(t, filepath.Join(funa, "debug.log"), "noise\n")
	writeFile(t, filepath.Join(funa, "keep.log"), "keep\n")
	writeFile(t, filepath.Join(funa, "root-only.txt"), "root\n")
	writeFile(t, filepath.Join(funa, "nested", "root-only.txt"), "nested\n")
	writeFile(t, filepath.Join(funa, "build", "out.js"), "z\n")

	funb := filepath.Join(root, "funb")
	writeFile(t, filepath.Join(funb, ".gitignore"), "*.tmp\nsecret/\n")
	writeFile(t, filepath.Join(funb, "template.yaml"), "runtime: nodejs24.x\n")
	writeFile(t, filepath.Join(funb, "handler.js"), "b\n")
	writeFile(t, filepath.Join(funb, "debug.log"), "noise\n")
	writeFile(t, filepath.Join(funb, "keep.log"), "keep\n")
	writeFile(t, filepath.Join(funb, "root-only.txt"), "root\n")
	writeFile(t, filepath.Join(funb, "build", "out.js"), "z\n")
	writeFile(t, filepath.Join(funb, "scratch.tmp"), "noise\n")
	writeFile(t, filepath.Join(funb, "secret", "key.js"), "secret\n")

	selectionA, err := ForDir(funa)
	if err != nil {
		t.Fatalf("ForDir(funa): %v", err)
	}
	selectionB, err := ForDir(funb)
	if err != nil {
		t.Fatalf("ForDir(funb): %v", err)
	}

	// funa: its own anchored/relative/nested rules apply; the ancestor rules and
	// nothing from funb change the outcome.
	wantA := []string{
		".gitignore", "handler.generated", "handler.js", "keep.log",
		"nested", "nested/root-only.txt", "template.yaml",
	}
	gotA := walkNames(t, selectionA)
	if len(gotA) != len(wantA) {
		t.Fatalf("funa walk = %v, want %v", gotA, wantA)
	}
	for i := range wantA {
		if gotA[i] != wantA[i] {
			t.Fatalf("funa walk = %v, want %v", gotA, wantA)
		}
	}

	// funb: unaffected by funa's rules (negation target, anchored rule, and
	// directory exclusion all stay effective only in funa), unaffected by the
	// ancestor rules that name *.log, *.generated, and funb/ itself, and its own
	// rules still exclude its .tmp file and secret directory.
	wantB := []string{
		".gitignore", "build", "build/out.js", "debug.log",
		"handler.js", "keep.log", "root-only.txt", "template.yaml",
	}
	gotB := walkNames(t, selectionB)
	if len(gotB) != len(wantB) {
		t.Fatalf("funb walk = %v, want %v", gotB, wantB)
	}
	for i := range wantB {
		if gotB[i] != wantB[i] {
			t.Fatalf("funb walk = %v, want %v", gotB, wantB)
		}
	}

	// Each selection reads only its own policy file: no checkout-root or
	// sibling policy enters either app's applicable set.
	for _, tc := range []struct {
		name      string
		selection *Selection
		dir       string
	}{{"funa", selectionA, funa}, {"funb", selectionB, funb}} {
		got := tc.selection.IgnoreFiles()
		want := []string{filepath.Join(tc.dir, IgnoreFile)}
		if len(got) != 1 || got[0] != want[0] {
			t.Errorf("%s IgnoreFiles = %v, want %v", tc.name, got, want)
		}
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

func TestForDirMissingDirWrapsErrNotExist(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope")
	_, err := ForDir(missing)
	if !errors.Is(err, ErrNotExist) {
		t.Fatalf("err = %v, want ErrNotExist", err)
	}
}

// TestExampleFastapiServiceExcludesGeneratedVenv pins the example-hygiene
// decision: the fastapi-service example ships an app-root .gitignore that
// excludes its generated Python environment (`.venv/`) as a whole, so source
// selection never descends into it. Without that root rule the only protection
// is the generated `.venv/.gitignore` (content `*`), which would itself enter
// the copy and the fingerprint; a root rule excludes the whole directory before
// any inner file is read. The test asserts the policy from the real example
// directory and does not require the (gitignored, developer-local) `.venv`
// tree to be present, since .gitignore matching is independent of the target's
// existence.
func TestExampleFastapiServiceExcludesGeneratedVenv(t *testing.T) {
	dir := filepath.Join("..", "..", "examples", "apps", "fastapi-service")
	selection, err := ForDir(dir)
	if err != nil {
		t.Fatalf("ForDir(%s): %v", dir, err)
	}
	if selection.Includes(".venv", true) {
		t.Error("the generated .venv directory must be excluded from source by the example's root .gitignore")
	}
	if selection.Includes(".venv/.gitignore", false) {
		t.Error("no file inside .venv may be source, including its generated inner .gitignore")
	}
	if !selection.Includes("app/main.py", false) {
		t.Error("the example's real application source must remain included")
	}
}

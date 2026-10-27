package runtime

import (
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/runtime/node"
	"relay/internal/runtime/plan"
)

// planFilePathCase is one plan.File.Path under test. want is the required
// outcome: for accepted paths the context-relative staging path (identically
// the rendered COPY source token), for rejected paths a required error reason.
type planFilePathCase struct {
	name string
	path string
	want string
}

// validPlanFilePathCases are the generated destinations both engines emit. The
// absolute image path is mirrored INTO the context by trimming exactly the
// leading separator: the same string is the staged location and the COPY
// source, so engine contract paths keep their meaning across the render/stage
// boundary.
func validPlanFilePathCases() []planFilePathCase {
	return []planFilePathCase{
		{"python bootstrap", "/relay/bootstrap.py", "relay/bootstrap.py"},
		{"node bootstrap", "/relay/bootstrap.mjs", "relay/bootstrap.mjs"},
		{"node resolve hook", node.ResolveHookPath, "relay/resolve-hook.mjs"},
		{"node injected package.json", "/app/package.json", "app/package.json"},
		{"nested directory", "/relay/sub/gen.json", "relay/sub/gen.json"},
		{"deep nested", "/a/b/c/d.txt", "a/b/c/d.txt"},
		{"single segment", "/x", "x"},
		{"single leading hyphen", "/-file", "-file"},
		{"allowed punctuation", "/relay/pkg.1+build~x=y:z,a@b%c#d.json", "relay/pkg.1+build~x=y:z,a@b%c#d.json"},
		// A "--"-prefixed segment is only a Dockerfile flag in the FIRST COPY
		// token; once the source starts with the trimmed directory, the token
		// is no longer in flag position and is accepted as a literal name.
		{"mid-path double dash is not a flag position", "/relay/--chmod=777", "relay/--chmod=777"},
	}
}

// invalidPlanFilePathCases is the rejection battery for plan.File.Path. Every
// case must be rejected (never repaired) so the renderer and the stager always
// derive the same relative path from the same accepted input; want is the
// distinct validation branch the rejection must come from.
func invalidPlanFilePathCases() []planFilePathCase {
	return []planFilePathCase{
		{"empty", "", "path is empty"},
		{"relative file", "relay/bootstrap.py", "must be absolute"},
		{"relative nested", "relay/sub/x.txt", "must be absolute"},
		{"dot relative", "./x.txt", "must be absolute"},
		{"image root", "/", "must not be the image root"},
		{"repeated separator", "//x.txt", "is not canonical"},
		{"trailing separator", "/relay/", "is not canonical"},
		{"double trailing separator", "/relay//", "is not canonical"},
		{"dot segment", "/relay/./x.txt", "is not canonical"},
		{"trailing dot segment", "/relay/.", "is not canonical"},
		{"leading parent traversal", "/../relay-escape.txt", "is not canonical"},
		{"inner parent traversal", "/relay/../escape.txt", "is not canonical"},
		{"nested parent traversal", "/relay/sub/../../escape.txt", "is not canonical"},
		{"trailing parent traversal", "/relay/..", "is not canonical"},
		{"backslash separator", "/relay\\bootstrap.py", "must not contain a backslash"},
		{"trailing backslash", "/relay\\", "must not contain a backslash"},
		{"space", "/relay/boot strap.py", "must not contain whitespace"},
		{"tab", "/relay/boot\tstrap.py", "must not contain whitespace"},
		{"newline", "/relay/boot\nstrap.py", "must not contain whitespace"},
		{"carriage return", "/relay/boot\rstrap.py", "must not contain whitespace"},
		{"form feed", "/relay/boot\x0cstrap.py", "must not contain whitespace"},
		{"vertical tab", "/relay/boot\x0bstrap.py", "must not contain whitespace"},
		{"NUL byte", "/relay/boot\x00strap.py", "must not contain control characters"},
		{"control byte", "/relay/boot\x01strap.py", "must not contain control characters"},
		{"DEL byte", "/relay/boot\x7fstrap.py", "must not contain control characters"},
		{"dollar substitution", "/relay/$boot.py", "is unsafe in an unquoted COPY source"},
		{"brace dollar substitution", "/relay/${BOOT}", "is unsafe in an unquoted COPY source"},
		{"glob star", "/relay/*.py", "is unsafe in an unquoted COPY source"},
		{"glob question", "/relay/boot?.py", "is unsafe in an unquoted COPY source"},
		{"glob bracket", "/relay/boot[.py", "is unsafe in an unquoted COPY source"},
		{"double quote", "/relay/\"boot.py", "is unsafe in an unquoted COPY source"},
		{"single quote", "/relay/'boot.py", "is unsafe in an unquoted COPY source"},
		{"backtick", "/relay/`boot.py", "is unsafe in an unquoted COPY source"},
		{"semicolon", "/relay/a;b.py", "is unsafe in an unquoted COPY source"},
		{"pipe", "/relay/a|b.py", "is unsafe in an unquoted COPY source"},
		{"ampersand", "/relay/a&b.py", "is unsafe in an unquoted COPY source"},
		{"parenthesis", "/relay/a(b.py", "is unsafe in an unquoted COPY source"},
		{"brace", "/relay/a{b.py", "is unsafe in an unquoted COPY source"},
		{"exclamation", "/relay/a!b.py", "is unsafe in an unquoted COPY source"},
		{"caret", "/relay/a^b.py", "is unsafe in an unquoted COPY source"},
		{"less than", "/relay/a<b.py", "is unsafe in an unquoted COPY source"},
		{"nested instruction flag", "/--chmod=777", "would render as a Dockerfile flag"},
		{"invalid UTF-8", "/relay/boot\xffstrap.py", "must be valid UTF-8"},
		{"non-breaking space", "/relay/boot\u00a0strap.py", "must contain only printable, non-space characters"},
		{"zero-width joiner", "/relay/boot\u200dstrap.py", "must contain only printable, non-space characters"},
		{"generated Dockerfile collision", "/Dockerfile", "collides with the generated Dockerfile"},
		{"dockerignore collision", "/.dockerignore", "reserved build-context control file"},
		{"Dockerfile-specific dockerignore collision", "/Dockerfile.dockerignore", "reserved build-context control file"},
	}
}

// TestPlanFileContextPathAcceptsContractDestinations pins the accepted shape of
// plan.File.Path: every engine-generated destination (Python and Node) maps
// onto its context-relative path by trimming exactly one leading separator,
// which is also the rendered COPY source for that file.
func TestPlanFileContextPathAcceptsContractDestinations(t *testing.T) {
	for _, tc := range validPlanFilePathCases() {
		t.Run(tc.name, func(t *testing.T) {
			rel, err := planFileContextPath(tc.path)
			if err != nil {
				t.Fatalf("planFileContextPath(%q) = %v, want nil", tc.path, err)
			}
			if rel != tc.want {
				t.Fatalf("planFileContextPath(%q) = %q, want %q", tc.path, rel, tc.want)
			}

			// The renderer must name exactly the accepted relative path: one
			// COPY whose source is rel and whose destination is the file's
			// image directory, so COPY writes the staged file where Path says.
			p := plan.BuildPlan{
				BaseImage: "example:1",
				Files:     []plan.File{{Path: tc.path, Content: []byte("x")}},
			}
			df := mustRenderDockerfile(t, p)
			line := "COPY " + tc.want + " " + path.Dir(tc.path) + "/\n"
			if !strings.Contains(df, line) {
				t.Fatalf("renderer for %q must emit %q, got:\n%s", tc.path, line, df)
			}
		})
	}
}

// TestPlanFileContextPathRejects pins the whole rejection battery directly on
// the validator: empty, relative, image root, traversal, non-canonical, repeated
// or trailing separators, backslashes, whitespace, control bytes, Dockerfile
// tokenizer syntax, invalid UTF-8, and the reserved generated Dockerfile name —
// each rejected on a distinct rule, never repaired into a different path.
func TestPlanFileContextPathRejects(t *testing.T) {
	for _, tc := range invalidPlanFilePathCases() {
		t.Run(tc.name, func(t *testing.T) {
			rel, err := planFileContextPath(tc.path)
			if err == nil {
				t.Fatalf("planFileContextPath(%q) = %q, want an error", tc.path, rel)
			}
			if rel != "" {
				t.Fatalf("planFileContextPath(%q) returned rel %q along with an error; rejected paths must not yield a usable path", tc.path, rel)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("planFileContextPath(%q) error %q must mention %q", tc.path, err, tc.want)
			}
		})
	}
}

// TestRenderDockerfileRejectsInvalidPlanFilePaths proves the renderer rejects
// the same battery the stager rejects: renderDockerfile validates the plan's
// file list up front and fails closed (no Dockerfile text) on any malformed
// path, so a bad plan can never be built, not even partially.
func TestRenderDockerfileRejectsInvalidPlanFilePaths(t *testing.T) {
	for _, tc := range invalidPlanFilePathCases() {
		t.Run(tc.name, func(t *testing.T) {
			p := plan.BuildPlan{
				BaseImage: "example:1",
				Files:     []plan.File{{Path: tc.path, Content: []byte("x")}},
			}
			df, err := renderDockerfile(p)
			if err == nil {
				t.Fatalf("renderDockerfile accepted plan file path %q:\n%s", tc.path, df)
			}
			if df != "" {
				t.Fatalf("renderDockerfile returned partial output on rejection:\n%s", df)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("renderer error %q must mention %q", err, tc.want)
			}
		})
	}
}

// TestPlanFileDockerfileCollisionStagedAndRenderedRejected covers the reserved
// "/Dockerfile" plan path through the STAGER as well: the generated Dockerfile
// is written to that exact context path after staging, so a plan file there
// would be silently replaced. writePlanFiles must refuse before touching the
// context, leaving no Dockerfile behind.
func TestPlanFileReservedContextFilesRejectedByStagerAndRenderer(t *testing.T) {
	for _, tc := range []struct {
		path string
		want string
	}{
		{"/Dockerfile", "collides with the generated Dockerfile"},
		{"/.dockerignore", "reserved build-context control file"},
		{"/Dockerfile.dockerignore", "reserved build-context control file"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			ctx := t.TempDir()
			files := []plan.File{{Path: tc.path, Content: []byte("reserved")}}

			if err := writePlanFiles(ctx, "reserved", files); err == nil {
				t.Fatalf("writePlanFiles accepted reserved context file %q", tc.path)
			} else if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("writePlanFiles error %q must mention %q", err, tc.want)
			}
			if _, err := renderDockerfile(plan.BuildPlan{BaseImage: "example:1", Files: files}); err == nil {
				t.Fatalf("renderDockerfile accepted reserved context file %q", tc.path)
			}

			entries, err := os.ReadDir(ctx)
			if err != nil {
				t.Fatalf("ReadDir(ctx): %v", err)
			}
			if len(entries) != 0 {
				t.Fatalf("rejected plan file must stage nothing, ctx has: %v", entries)
			}
		})
	}
}

// TestValidatePlanFilesRejectsDuplicates proves a plan that names the same
// destination twice is rejected by the validator, the renderer, and the stager:
// a silently kept winner would make the staged bytes and the COPY lines
// disagree about which content a path owns.
func TestValidatePlanFilesRejectsDuplicates(t *testing.T) {
	files := []plan.File{
		{Path: "/relay/a.txt", Content: []byte("one")},
		{Path: "/relay/a.txt", Content: []byte("two")},
	}

	if _, err := validatePlanFiles(files); err == nil {
		t.Fatal("validatePlanFiles accepted duplicate plan file paths")
	} else if !strings.Contains(err.Error(), "duplicate path") {
		t.Fatalf("validatePlanFiles error %q must report the duplicate", err)
	}

	if _, err := renderDockerfile(plan.BuildPlan{BaseImage: "example:1", Files: files}); err == nil {
		t.Fatal("renderDockerfile accepted duplicate plan file paths")
	}

	ctx := t.TempDir()
	if err := writePlanFiles(ctx, "dupe", files); err == nil {
		t.Fatal("writePlanFiles accepted duplicate plan file paths")
	}
	entries, err := os.ReadDir(ctx)
	if err != nil {
		t.Fatalf("ReadDir(ctx): %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("duplicate rejection must stage nothing, ctx has: %v", entries)
	}
}

// TestValidatePlanFilesRejectsFileDirectoryConflicts validates the full list
// before staging so one generated file cannot also be another file's parent.
func TestValidatePlanFilesRejectsFileDirectoryConflicts(t *testing.T) {
	for _, files := range [][]plan.File{
		{
			{Path: "/x", Content: []byte("file")},
			{Path: "/x/y", Content: []byte("child")},
		},
		{
			{Path: "/x/y", Content: []byte("child")},
			{Path: "/x", Content: []byte("file")},
		},
	} {
		if _, err := validatePlanFiles(files); err == nil || !strings.Contains(err.Error(), "conflicts with parent file") {
			t.Fatalf("validatePlanFiles(%v) = %v, want file/directory conflict", files, err)
		}
		if _, err := renderDockerfile(plan.BuildPlan{BaseImage: "example:1", Files: files}); err == nil {
			t.Fatalf("renderer accepted file/directory conflict: %v", files)
		}

		ctx := t.TempDir()
		if err := writePlanFiles(ctx, "conflict", files); err == nil {
			t.Fatalf("stager accepted file/directory conflict: %v", files)
		}
		entries, err := os.ReadDir(ctx)
		if err != nil {
			t.Fatalf("ReadDir(ctx): %v", err)
		}
		if len(entries) != 0 {
			t.Fatalf("conflicting paths must fail before staging: %v", entries)
		}
	}

	if _, err := validatePlanFiles([]plan.File{
		{Path: "/x", Content: []byte("file")},
		{Path: "/x.y", Content: []byte("sibling")},
	}); err != nil {
		t.Fatalf("distinct sibling paths must be accepted: %v", err)
	}
}

// TestWritePlanFilesRejectsRelativeContext pins the stager's precondition that
// the build context root is absolute: a relative ctxDir cannot be confined, so
// the whole write is refused before any file is staged.
func TestWritePlanFilesRejectsRelativeContext(t *testing.T) {
	err := writePlanFiles("relative-ctx", "app", []plan.File{
		{Path: "/relay/bootstrap.py", Content: []byte("x")},
	})
	if err == nil {
		t.Fatal("writePlanFiles accepted a relative build context")
	}
	if !strings.Contains(err.Error(), "must be absolute") {
		t.Fatalf("writePlanFiles error %q must report a non-absolute context", err)
	}
}

func TestWritePlanFilesRejectsUnsafeContextRoot(t *testing.T) {
	files := []plan.File{{Path: "/relay/generated.txt", Content: []byte("generated")}}

	t.Run("symlink root", func(t *testing.T) {
		outside := t.TempDir()
		link := filepath.Join(t.TempDir(), "context-link")
		if err := os.Symlink(outside, link); err != nil {
			t.Fatalf("create symlinked context root: %v", err)
		}
		if err := writePlanFiles(link, "app", files); err == nil || !strings.Contains(err.Error(), "is a symlink") {
			t.Fatalf("writePlanFiles(%q) = %v, want symlink-root error", link, err)
		}
		if _, err := os.Stat(filepath.Join(outside, "relay")); !os.IsNotExist(err) {
			t.Fatalf("write through symlinked context root created outside path: %v", err)
		}
	})

	t.Run("non-directory root", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "context-file")
		if err := os.WriteFile(root, []byte("keep"), 0o644); err != nil {
			t.Fatalf("create context file: %v", err)
		}
		if err := writePlanFiles(root, "app", files); err == nil || !strings.Contains(err.Error(), "is not a directory") {
			t.Fatalf("writePlanFiles(%q) = %v, want non-directory error", root, err)
		}
		got, err := os.ReadFile(root)
		if err != nil || string(got) != "keep" {
			t.Fatalf("context file changed: got %q, err %v", got, err)
		}
	})
}

// TestWritePlanFilesPlacesGeneratedFilesSafelyAndAppliesModes proves staged
// generated files land under the context root at their mirrored relative paths
// (never at the absolute path's host meaning, never outside), and that the
// default mode is 0644 while an explicit mode is applied verbatim.
func TestWritePlanFilesPlacesGeneratedFilesSafelyAndAppliesModes(t *testing.T) {
	ctx := t.TempDir()
	parent := filepath.Dir(ctx)
	before, err := os.ReadDir(parent)
	if err != nil {
		t.Fatalf("ReadDir(parent) before: %v", err)
	}

	files := []plan.File{
		{Path: "/relay/bootstrap.py", Content: []byte("PY"), Mode: 0},
		{Path: "/relay/bootstrap.mjs", Content: []byte("JS"), Mode: fs.FileMode(0o644)},
		{Path: node.ResolveHookPath, Content: []byte("HOOK"), Mode: 0},
		{Path: "/app/package.json", Content: []byte("{}"), Mode: fs.FileMode(0o600)},
		{Path: "/relay/sub/gen.json", Content: []byte("{}"), Mode: fs.FileMode(0o600)},
	}
	if err := writePlanFiles(ctx, "modes", files); err != nil {
		t.Fatalf("writePlanFiles: %v", err)
	}

	for _, f := range files {
		rel := strings.TrimPrefix(f.Path, "/")
		staged := filepath.Join(ctx, filepath.FromSlash(rel))
		got, err := os.ReadFile(staged)
		if err != nil {
			t.Fatalf("plan file %q not staged at %q: %v", f.Path, staged, err)
		}
		if string(got) != string(f.Content) {
			t.Fatalf("staged %q = %q, want %q", staged, got, f.Content)
		}
		// Mode 0 means the default 0644; any other value is used as-is. Owner
		// bits survive every umask, and 0644/0600 are not reduced by the CI
		// umask 022, so the perm bits must match exactly.
		wantPerm := fs.FileMode(0o644)
		if f.Mode != 0 {
			wantPerm = f.Mode.Perm()
		}
		info, err := os.Stat(staged)
		if err != nil {
			t.Fatalf("Stat(%q): %v", staged, err)
		}
		if info.Mode().Perm() != wantPerm {
			t.Fatalf("staged %q mode = %s, want perm %o", staged, info.Mode(), wantPerm)
		}
	}

	after, err := os.ReadDir(parent)
	if err != nil {
		t.Fatalf("ReadDir(parent) after: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("staging must create nothing next to the context: before %v after %v", before, after)
	}
}

// TestWritePlanFilesValidatesEntireListBeforeFirstWrite pins the all-or-nothing
// validation order: one malformed entry (early or late in the list) rejects the
// WHOLE list before any earlier valid entry is staged, so a bad plan can never
// leave a partially written context.
func TestWritePlanFilesValidatesEntireListBeforeFirstWrite(t *testing.T) {
	ctx := t.TempDir()

	// Invalid last: the earlier valid entry must not have been written.
	late := []plan.File{
		{Path: "/relay/sub/ok.txt", Content: []byte("ok")},
		{Path: "missing-slash.txt", Content: []byte("late")},
	}
	err := writePlanFiles(ctx, "app", late)
	if err == nil {
		t.Fatal("writePlanFiles accepted a list whose last path is relative")
	}
	if !strings.Contains(err.Error(), "plan file 1") {
		t.Fatalf("error %q must identify the malformed entry by index", err)
	}
	entries, err := os.ReadDir(ctx)
	if err != nil {
		t.Fatalf("ReadDir(ctx): %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("later invalid entry must reject the whole list before earlier writes, ctx has: %v", entries)
	}

	// Invalid first: rejection is positional, at index 0.
	early := []plan.File{
		{Path: "early.txt", Content: []byte("first")},
		{Path: "/relay/ok.txt", Content: []byte("second")},
	}
	err = writePlanFiles(ctx, "app", early)
	if err == nil {
		t.Fatal("writePlanFiles accepted a list whose first path is relative")
	}
	if !strings.Contains(err.Error(), "plan file 0") {
		t.Fatalf("error %q must identify the malformed entry by index", err)
	}
}

// TestWritePlanFilesRejectsTraversalWithoutOutsideWrites pins the end-to-end
// traversal defense: plan paths that would climb out of the context are
// rejected by validation before any staging, so nothing is written inside OR
// outside the context, and the context's parent is left untouched.
func TestWritePlanFilesRejectsTraversalWithoutOutsideWrites(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
	}{
		{"leading parent traversal", "/../relay-escape.txt"},
		{"inner parent traversal", "/relay/../escape.txt"},
		{"nested parent traversal", "/relay/sub/../../escape.txt"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.TempDir()
			parent := filepath.Dir(ctx)
			before, err := os.ReadDir(parent)
			if err != nil {
				t.Fatalf("ReadDir(parent) before: %v", err)
			}

			if err := writePlanFiles(ctx, "app", []plan.File{
				{Path: tc.path, Content: []byte("escape")},
			}); err == nil {
				t.Fatalf("writePlanFiles accepted traversal path %q", tc.path)
			} else if !strings.Contains(err.Error(), "is not canonical") {
				t.Fatalf("traversal path %q must be rejected as non-canonical, got %q", tc.path, err)
			}

			// The lexical escape target (one level above the context) must not
			// exist: refusing the path is not enough if a write had escaped.
			candidate := filepath.Join(parent, "relay-escape.txt")
			if _, err := os.Stat(candidate); !os.IsNotExist(err) {
				t.Fatalf("a write escaped the context to %s: %v", candidate, err)
			}
			after, err := os.ReadDir(parent)
			if err != nil {
				t.Fatalf("ReadDir(parent) after: %v", err)
			}
			if len(after) != len(before) {
				t.Fatalf("staging must not write outside the context: before %v after %v", before, after)
			}
		})
	}
}

// TestPlanFileTargetRejectsTraversalBypassingValidation pins the
// defense-in-depth containment for a caller that bypasses
// planFileContextPath: a relative path whose traversal rejoins root is caught
// by the final lexical containment check and resolves to nothing.
func TestPlanFileTargetRejectsTraversalBypassingValidation(t *testing.T) {
	root := t.TempDir()

	// A validated nested path resolves to exactly root/<rel>.
	ok, err := planFileTarget(root, "relay/sub/gen.txt")
	if err != nil {
		t.Fatalf("planFileTarget(valid): %v", err)
	}
	if want := filepath.Join(root, "relay", "sub", "gen.txt"); ok != want {
		t.Fatalf("planFileTarget(valid) = %q, want %q", ok, want)
	}

	target, err := planFileTarget(root, "a/../../escape.txt")
	if err == nil {
		t.Fatalf("planFileTarget accepted traversal path, target = %q", target)
	}
	if !strings.Contains(err.Error(), "escapes the build context") {
		t.Fatalf("planFileTarget traversal error %q must report the escape", err)
	}
	if target != "" {
		t.Fatalf("planFileTarget returned target %q along with an error", target)
	}
	// Neither the rejoined-inside target nor the parent of the context was
	// created by the rejected resolution.
	for _, escaped := range []string{
		filepath.Join(root, "escape.txt"),
		filepath.Join(filepath.Dir(root), "escape.txt"),
	} {
		if _, err := os.Stat(escaped); !os.IsNotExist(err) {
			t.Fatalf("rejected traversal target %s must not exist, Stat err = %v", escaped, err)
		}
	}
}

// TestWritePlanFilesRefusesSymlinkTargetAndParent proves the stager refuses to
// write through links rather than following them: a symlink at the FINAL target
// and a symlink at any PARENT directory are both rejected, and the linked
// outside destination keeps its exact previous content — containment holds even
// when a link inside the context points outside it.
func TestWritePlanFilesRefusesSymlinkTargetAndParent(t *testing.T) {
	outside := t.TempDir()
	payload := filepath.Join(outside, "payload.txt")
	sentinel := []byte("outside-payload")
	if err := os.WriteFile(payload, sentinel, 0o644); err != nil {
		t.Fatalf("write outside payload: %v", err)
	}
	outsideCount := func(t *testing.T) int {
		t.Helper()
		entries, err := os.ReadDir(outside)
		if err != nil {
			t.Fatalf("ReadDir(outside): %v", err)
		}
		return len(entries)
	}
	unreadPayload := func(t *testing.T) {
		t.Helper()
		got, err := os.ReadFile(payload)
		if err != nil {
			t.Fatalf("read outside payload: %v", err)
		}
		if string(got) != string(sentinel) {
			t.Fatalf("outside payload changed through a symlink: %q", got)
		}
	}
	symlink := func(t *testing.T, oldname, newname string) {
		t.Helper()
		if err := os.Symlink(oldname, newname); err != nil {
			t.Fatalf("create symlink %q -> %q: %v", newname, oldname, err)
		}
	}

	t.Run("symlinked final target", func(t *testing.T) {
		ctx := t.TempDir()
		link := filepath.Join(ctx, "gen.txt")
		symlink(t, payload, link)
		before := outsideCount(t)

		err := writePlanFiles(ctx, "app", []plan.File{
			{Path: "/gen.txt", Content: []byte("generated")},
		})
		if err == nil {
			t.Fatal("writePlanFile followed a symlink at the final target")
		}
		if !strings.Contains(err.Error(), "refusing to replace symlink") {
			t.Fatalf("error %q must report the symlink refusal", err)
		}
		unreadPayload(t)
		if got := outsideCount(t); got != before {
			t.Fatalf("a write leaked outside the context: outside has %d entries, want %d", got, before)
		}
		// The refusal must leave the link itself intact (no replaced artifact).
		info, err := os.Lstat(link)
		if err != nil {
			t.Fatalf("Lstat(link): %v", err)
		}
		if info.Mode()&os.ModeSymlink == 0 {
			t.Fatalf("rejected symlink was replaced: mode %s", info.Mode())
		}
	})

	t.Run("symlinked parent directory", func(t *testing.T) {
		ctx := t.TempDir()
		if err := os.Mkdir(filepath.Join(ctx, "relay"), 0o755); err != nil {
			t.Fatalf("mkdir relay: %v", err)
		}
		// One level deeper than the first component, so the rejection is by the
		// component walk and not by an accidental full-path shortcut.
		link := filepath.Join(ctx, "relay", "sub")
		symlink(t, outside, link)
		before := outsideCount(t)

		err := writePlanFiles(ctx, "app", []plan.File{
			{Path: "/relay/sub/gen.txt", Content: []byte("generated")},
		})
		if err == nil {
			t.Fatal("writePlanFile wrote through a symlinked parent directory")
		}
		if !strings.Contains(err.Error(), "is a symlink; refusing to write through it") {
			t.Fatalf("error %q must report the symlinked parent refusal", err)
		}
		unreadPayload(t)
		if got := outsideCount(t); got != before {
			t.Fatalf("a write leaked outside the context through a parent link: outside has %d entries, want %d", got, before)
		}
	})
}

// TestWritePlanFileReplacesRegularStagedSourceFileIntentionally pins the one
// deliberate overwrite: when a plan file's context path collides with a regular
// staged source file, the GENERATED file intentionally replaces it (this is how
// an injected file wins over a user-provided one). Replacement must not affect
// any other staged source file.
func TestWritePlanFileReplacesRegularStagedSourceFileIntentionally(t *testing.T) {
	ctx := t.TempDir()
	if err := os.WriteFile(filepath.Join(ctx, "handler.py"), []byte("staged-source-bytes"), 0o644); err != nil {
		t.Fatalf("write staged source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(ctx, "other.py"), []byte("keep me"), 0o644); err != nil {
		t.Fatalf("write untouched source: %v", err)
	}

	if err := writePlanFiles(ctx, "app", []plan.File{
		{Path: "/handler.py", Content: []byte("generated"), Mode: fs.FileMode(0o644)},
	}); err != nil {
		t.Fatalf("writePlanFiles: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(ctx, "handler.py"))
	if err != nil {
		t.Fatalf("read replaced file: %v", err)
	}
	if string(got) != "generated" {
		t.Fatalf("plan file must intentionally replace the staged source file, got %q", got)
	}
	kept, err := os.ReadFile(filepath.Join(ctx, "other.py"))
	if err != nil {
		t.Fatalf("read untouched file: %v", err)
	}
	if string(kept) != "keep me" {
		t.Fatalf("an unrelated staged source must be untouched, got %q", kept)
	}
}

// TestRenderedCopyPathsMatchStagedRelativePaths pins the render/stage contract
// end to end: with the plan's files staged into a context, the rendered
// Dockerfile carries EXACTLY one COPY per generated file (and no other COPY
// beyond the whole-tree source copy), and each COPY source is the relative path
// at which the stager independently placed that file — discovered by walking
// the context, not by re-deriving it from plan data. The final lexical check
// also proves the staged tree contains nothing but the files the plan names.
func TestRenderedCopyPathsMatchStagedRelativePaths(t *testing.T) {
	p := plan.BuildPlan{
		BaseImage: "example:1",
		WorkDir:   "/app",
		Files: []plan.File{
			{Path: "/relay/bootstrap.py", Content: []byte("PY"), Mode: fs.FileMode(0o644)},
			{Path: "/relay/bootstrap.mjs", Content: []byte("JS"), Mode: fs.FileMode(0o644)},
			{Path: node.ResolveHookPath, Content: []byte("HOOK"), Mode: fs.FileMode(0o644)},
			{Path: "/app/package.json", Content: []byte("{}"), Mode: fs.FileMode(0o600)},
			{Path: "/relay/sub/gen.json", Content: []byte("{}"), Mode: fs.FileMode(0o600)},
		},
	}
	ctx := t.TempDir()
	if err := writePlanFiles(ctx, "agreement", p.Files); err != nil {
		t.Fatalf("writePlanFiles: %v", err)
	}

	// Every file the stager put under the context, by its context-relative path.
	staged := map[string]string{}
	err := filepath.Walk(ctx, func(walked string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(ctx, walked)
		if err != nil {
			return err
		}
		staged[filepath.ToSlash(rel)] = walked
		return nil
	})
	if err != nil {
		t.Fatalf("Walk(ctx): %v", err)
	}
	if len(staged) != len(p.Files) {
		t.Fatalf("context must hold exactly the plan's files, staged: %v", staged)
	}

	df := mustRenderDockerfileWithSource(t, p, false)
	copies := 0
	for _, line := range strings.Split(df, "\n") {
		if !strings.HasPrefix(line, "COPY ") {
			continue
		}
		copies++
		fields := strings.Fields(line)
		if len(fields) != 3 {
			t.Fatalf("COPY line %q must be source and destination only, got %d fields", line, len(fields))
		}
		source, dest := fields[1], fields[2]
		stagedPath, ok := staged[source]
		if !ok {
			t.Fatalf("COPY names path %q, which the stager never staged: %v", source, staged)
		}
		got, err := os.ReadFile(stagedPath)
		if err != nil {
			t.Fatalf("read staged %q: %v", stagedPath, err)
		}
		for _, f := range p.Files {
			if string(f.Content) == string(got) && strings.TrimPrefix(f.Path, "/") == source {
				want := path.Dir(f.Path) + "/"
				if dest != want {
					t.Fatalf("COPY destination for %q = %q, want %q", source, dest, want)
				}
				break
			}
		}
	}
	if copies != len(p.Files) {
		t.Fatalf("renderer must emit exactly one COPY per generated plan file (got %d in %d files):\n%s", copies, len(p.Files), df)
	}
}

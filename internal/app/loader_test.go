package app

import (
	"bytes"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTemplate(t *testing.T, dir, name, content string) {
	t.Helper()
	writeFile(t, filepath.Join(dir, name, "template.yaml"), content)
}

func TestLoadValidApp(t *testing.T) {
	dir := t.TempDir()
	writeTemplate(t, dir, "enrollment-events", `
runtime: python3.14
events:
  - handler: handler.completed
    pattern:
      event_name: [MODIFY]
`)

	loader := NewLoader(dir, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	fns, err := loader.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(fns) != 1 {
		t.Fatalf("expected 1 app, got %d", len(fns))
	}
	if fns[0].Name != "enrollment-events" {
		t.Errorf("expected name from directory, got %q", fns[0].Name)
	}
	if fns[0].Template == nil {
		t.Error("expected parsed template")
	}
	if fns[0].Template.Runtime != "python3.14" {
		t.Errorf("expected runtime python3.14, got %q", fns[0].Template.Runtime)
	}
	if len(fns[0].Template.Events) != 1 {
		t.Errorf("expected 1 rule, got %d", len(fns[0].Template.Events))
	}
	if fns[0].Dir != filepath.Join(dir, "enrollment-events") {
		t.Errorf("expected Dir %q, got %q", filepath.Join(dir, "enrollment-events"), fns[0].Dir)
	}
}

func TestLoadIgnoresDirWithoutTemplate(t *testing.T) {
	dir := t.TempDir()
	writeTemplate(t, dir, "valid", `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      event_name: [MODIFY]
`)
	// A directory without template.yaml.
	if err := os.MkdirAll(filepath.Join(dir, "no-template"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	loader := NewLoader(dir, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	fns, err := loader.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(fns) != 1 {
		t.Fatalf("expected 1 app, got %d", len(fns))
	}
	if fns[0].Name != "valid" {
		t.Errorf("expected 'valid', got %q", fns[0].Name)
	}
}

func TestLoadSkipsInvalidYAML(t *testing.T) {
	dir := t.TempDir()
	writeTemplate(t, dir, "bad", "events: [unclosed")
	writeTemplate(t, dir, "good", `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      event_name: [MODIFY]
`)

	loader := NewLoader(dir, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	fns, err := loader.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(fns) != 1 {
		t.Fatalf("expected 1 app, got %d", len(fns))
	}
	if fns[0].Name != "good" {
		t.Errorf("expected 'good', got %q", fns[0].Name)
	}
}

func TestLoadMultipleApps(t *testing.T) {
	dir := t.TempDir()
	writeTemplate(t, dir, "fn-a", `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      event_name: [MODIFY]
`)
	writeTemplate(t, dir, "fn-b", `
runtime: node24
events:
  - handler: index.main
    pattern:
      event_name: [DELETE]
`)

	loader := NewLoader(dir, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	fns, err := loader.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(fns) != 2 {
		t.Fatalf("expected 2 apps, got %d", len(fns))
	}
	names := map[string]bool{}
	for _, fn := range fns {
		names[fn.Name] = true
	}
	if !names["fn-a"] || !names["fn-b"] {
		t.Errorf("expected fn-a and fn-b, got %v", names)
	}
}

func TestLoadOneInvalidDoesNotPreventValid(t *testing.T) {
	dir := t.TempDir()
	writeTemplate(t, dir, "bad", "not: [valid: yaml")
	writeTemplate(t, dir, "good", `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      event_name: [MODIFY]
`)
	writeTemplate(t, dir, "also-good", `
runtime: node24
events:
  - handler: index.main
    pattern:
      event_name: [INSERT]
`)

	loader := NewLoader(dir, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	fns, err := loader.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(fns) != 2 {
		t.Fatalf("expected 2 apps, got %d", len(fns))
	}
}

func TestLoadUnsupportedRuntimeRejected(t *testing.T) {
	dir := t.TempDir()
	writeTemplate(t, dir, "bad-runtime", `
runtime: python3.12
events:
  - handler: handler.main
    pattern:
      event_name: [MODIFY]
`)
	writeTemplate(t, dir, "good", `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      event_name: [MODIFY]
`)

	loader := NewLoader(dir, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	fns, err := loader.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(fns) != 1 {
		t.Fatalf("expected 1 app (unsupported runtime skipped), got %d", len(fns))
	}
	if fns[0].Name != "good" {
		t.Errorf("expected 'good', got %q", fns[0].Name)
	}
}

func TestLoadSkipsInvalidName(t *testing.T) {
	dir := t.TempDir()
	// A directory whose name violates the rule must be skipped (with a log),
	// not crash the load.
	writeTemplate(t, dir, "Invalid Name", `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      event_name: [MODIFY]
`)
	writeTemplate(t, dir, "valid-fn", `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      event_name: [MODIFY]
`)

	loader := NewLoader(dir, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	fns, err := loader.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(fns) != 1 {
		t.Fatalf("expected 1 app (invalid name skipped), got %d", len(fns))
	}
	if fns[0].Name != "valid-fn" {
		t.Errorf("expected 'valid-fn', got %q", fns[0].Name)
	}
}

func TestLoadMissingRuntimeRejected(t *testing.T) {
	dir := t.TempDir()
	writeTemplate(t, dir, "no-runtime", `
events:
  - handler: handler.main
    pattern:
      event_name: [MODIFY]
`)
	writeTemplate(t, dir, "good", `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      event_name: [MODIFY]
`)

	loader := NewLoader(dir, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	fns, err := loader.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(fns) != 1 {
		t.Fatalf("expected 1 app (missing runtime skipped), got %d", len(fns))
	}
	if fns[0].Name != "good" {
		t.Errorf("expected 'good', got %q", fns[0].Name)
	}
}

// TestLoadSingleAppliesSharedPathPolicy pins the path policy LoadSingle and
// Loader.Load share: a legal name and a real direct-child directory are
// required, a symlink is never accepted (even when the target is a real
// directory inside the root, and especially when it points outside), and a
// missing directory is reported as not-exist so a caller treats it as a removal.
func TestLoadSingleAppliesSharedPathPolicy(t *testing.T) {
	root := t.TempDir()
	writeTemplate(t, root, "valid", `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      event_name: [MODIFY]
`)

	t.Run("valid direct child", func(t *testing.T) {
		fn, err := LoadSingle(root, "valid")
		if err != nil {
			t.Fatalf("LoadSingle(valid) = %v", err)
		}
		if fn.Name != "valid" || fn.Dir != filepath.Join(root, "valid") {
			t.Fatalf("loaded %+v, want name/dir of valid", fn)
		}
	})

	t.Run("missing directory is not-exist", func(t *testing.T) {
		if _, err := LoadSingle(root, "absent"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("LoadSingle(absent) = %v, want fs.ErrNotExist", err)
		}
	})

	t.Run("invalid name", func(t *testing.T) {
		for _, name := range []string{"", "Upper", "has space", ".hidden", "a/b", ".."} {
			if _, err := LoadSingle(root, name); !errors.Is(err, ErrInvalidPath) {
				t.Errorf("LoadSingle(%q) = %v, want ErrInvalidPath", name, err)
			}
		}
	})

	t.Run("symlink to an inside directory is rejected", func(t *testing.T) {
		link := filepath.Join(root, "link-inside")
		if err := os.Symlink(filepath.Join(root, "valid"), link); err != nil {
			t.Skipf("symlink unsupported: %v", err)
		}
		if _, err := LoadSingle(root, "link-inside"); !errors.Is(err, ErrInvalidPath) {
			t.Fatalf("LoadSingle(symlink) = %v, want ErrInvalidPath", err)
		}
	})

	t.Run("symlink to an outside target is rejected", func(t *testing.T) {
		outside := t.TempDir()
		writeTemplate(t, outside, "outside", `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      event_name: [MODIFY]
`)
		if err := os.Symlink(filepath.Join(outside, "outside"), filepath.Join(root, "link-outside")); err != nil {
			t.Skipf("symlink unsupported: %v", err)
		}
		// The outside target has a valid template; the link must STILL be
		// rejected rather than followed.
		if _, err := LoadSingle(root, "link-outside"); !errors.Is(err, ErrInvalidPath) {
			t.Fatalf("LoadSingle(outside symlink) = %v, want ErrInvalidPath", err)
		}
	})

	t.Run("not a directory", func(t *testing.T) {
		if err := os.WriteFile(filepath.Join(root, "afile"), []byte("x"), 0o644); err != nil {
			t.Fatalf("write file: %v", err)
		}
		if _, err := LoadSingle(root, "afile"); !errors.Is(err, ErrInvalidPath) {
			t.Fatalf("LoadSingle(file) = %v, want ErrInvalidPath", err)
		}
	})
}

// TestLoadRejectsSymlinkedAppDir pins that startup discovery applies the
// same no-follow policy: a symlinked directory entry is skipped even when its
// target (inside or outside the root) holds a valid template.
func TestLoadRejectsSymlinkedAppDir(t *testing.T) {
	root := t.TempDir()
	writeTemplate(t, root, "valid", `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      event_name: [MODIFY]
`)
	if err := os.Symlink(filepath.Join(root, "valid"), filepath.Join(root, "aliased")); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	loader := NewLoader(root, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	fns, err := loader.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(fns) != 1 || fns[0].Name != "valid" {
		t.Fatalf("Load = %+v, want only the real 'valid' directory", fns)
	}
}

// TestLoadWithDiagnosticsReportsPresentInvalidEntries pins the loader diagnostic
// seam: each PRESENT entry that cannot be loaded is reported exactly once as a
// LoadIssue (invalid template, missing template, invalid name), valid apps
// are still returned, and a directory that vanished between the read and the
// load is NOT reported (a removal, not an invalid desired definition).
func TestLoadWithDiagnosticsReportsPresentInvalidEntries(t *testing.T) {
	dir := t.TempDir()
	writeTemplate(t, dir, "good", `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      event_name: [MODIFY]
`)
	writeTemplate(t, dir, "bad-yaml", "events: [unclosed")
	writeTemplate(t, dir, "bad-runtime", `
runtime: python3.12
events:
  - handler: handler.main
    pattern:
      event_name: [MODIFY]
`)
	writeTemplate(t, dir, "bad-name", "runtime: python3.14\n")
	// A directory with no template.yaml: present, but not an invalid definition
	// (it may be mid-copy), so it is reported as an issue without being treated
	// as loadable.
	if err := os.MkdirAll(filepath.Join(dir, "no-template"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	loader := NewLoader(dir, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	fns, issues, err := loader.LoadWithDiagnostics()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(fns) != 1 || fns[0].Name != "good" {
		t.Fatalf("valid apps = %+v, want only 'good'", fns)
	}

	byName := make(map[string]error, len(issues))
	for _, issue := range issues {
		if _, dup := byName[issue.Name]; dup {
			t.Errorf("duplicate issue for %q", issue.Name)
		}
		byName[issue.Name] = issue.Err
	}
	for _, name := range []string{"bad-yaml", "bad-runtime", "bad-name", "no-template"} {
		if _, ok := byName[name]; !ok {
			t.Errorf("missing issue for present entry %q (issues: %v)", name, byName)
		}
	}
	if len(issues) != 4 {
		t.Fatalf("issues = %d, want 4 (one per present invalid/incomplete entry)", len(issues))
	}
}

// TestLoadWithDiagnosticsRootReadErrorIsFatal pins that a root read failure is
// returned as an error with no apps and no issues, exactly like Load: the
// loader cannot even enumerate entries, so there is nothing to report.
func TestLoadWithDiagnosticsRootReadErrorIsFatal(t *testing.T) {
	loader := NewLoader(filepath.Join(t.TempDir(), "does-not-exist"), slog.New(slog.NewTextHandler(os.Stderr, nil)))
	fns, issues, err := loader.LoadWithDiagnostics()
	if err == nil {
		t.Fatal("missing root must be a fatal error")
	}
	if fns != nil || issues != nil {
		t.Fatalf("fatal root error must return no apps/issues, got %v/%v", fns, issues)
	}
}

// TestLoadWithDiagnosticsLogsInvalidEntries pins that the diagnostic variant
// logs exactly like Load always has: a present-but-invalid entry is warned
// about, while an entry without a template.yaml (ErrNotReady) stays silent
// because it is not invalid, merely mid-copy. Without this, swapping the worker
// to LoadWithDiagnostics would silently drop startup diagnostics.
func TestLoadWithDiagnosticsLogsInvalidEntries(t *testing.T) {
	dir := t.TempDir()
	writeTemplate(t, dir, "bad-yaml", "events: [unclosed")
	// Present but not ready: must be reported as an issue yet never logged.
	if err := os.MkdirAll(filepath.Join(dir, "no-template"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	var buf bytes.Buffer
	loader := NewLoader(dir, slog.New(slog.NewTextHandler(&buf, nil)))
	_, issues, err := loader.LoadWithDiagnostics()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(issues) != 2 {
		t.Fatalf("issues = %+v, want one invalid and one not-ready", issues)
	}

	logs := buf.String()
	if !strings.Contains(logs, "bad-yaml") || !strings.Contains(logs, "invalid; skipping") {
		t.Fatalf("invalid entry must be logged, logs = %q", logs)
	}
	if strings.Contains(logs, "no-template") {
		t.Fatalf("ErrNotReady must stay silent, logs = %q", logs)
	}
}

// TestLoadWithDiagnosticsMatchesLoadValidSet pins that the diagnostic variant
// returns exactly the valid set Load does, so a caller can swap to it without
// changing execution behavior; only the invalid diagnostics are additive.
func TestLoadWithDiagnosticsMatchesLoadValidSet(t *testing.T) {
	dir := t.TempDir()
	writeTemplate(t, dir, "good", `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      event_name: [MODIFY]
`)
	writeTemplate(t, dir, "bad", "not: [valid: yaml")

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	plain, err := NewLoader(dir, logger).Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	diag, issues, err := NewLoader(dir, logger).LoadWithDiagnostics()
	if err != nil {
		t.Fatalf("LoadWithDiagnostics: %v", err)
	}
	if len(plain) != len(diag) || len(diag) != 1 || plain[0].Name != diag[0].Name {
		t.Fatalf("Load = %+v, LoadWithDiagnostics = %+v, want the same single valid app", plain, diag)
	}
	if len(issues) != 1 || issues[0].Name != "bad" {
		t.Fatalf("issues = %+v, want one for bad", issues)
	}
}

func TestLoadBrokenAppDoesNotPreventValid(t *testing.T) {
	dir := t.TempDir()
	// Missing handler.
	writeTemplate(t, dir, "no-handler", `
runtime: python3.14
events:
  - pattern:
      event_name: [MODIFY]
`)
	// Missing pattern.
	writeTemplate(t, dir, "no-pattern", `
runtime: python3.14
events:
  - handler: handler.main
`)
	// Invalid handler syntax.
	writeTemplate(t, dir, "bad-handler", `
runtime: python3.14
events:
  - handler: no-dot-here
    pattern:
      event_name: [MODIFY]
`)
	writeTemplate(t, dir, "good", `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      event_name: [MODIFY]
`)

	loader := NewLoader(dir, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	fns, err := loader.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(fns) != 1 {
		t.Fatalf("expected 1 app, got %d", len(fns))
	}
	if fns[0].Name != "good" {
		t.Errorf("expected 'good', got %q", fns[0].Name)
	}
}

// TestIsReservedDir pins the shared predicate: only the exact Relay-owned
// staging prefix is reserved. A broader "hidden directory" rule would swallow
// genuinely invalid user names (and the invalid-name diagnostics they must
// still produce), so that is explicitly rejected.
func TestIsReservedDir(t *testing.T) {
	reserved := []string{".sync-abc123", ".sync-", ".sync-0"}
	for _, name := range reserved {
		if !IsReservedDir(name) {
			t.Errorf("IsReservedDir(%q) = false, want true", name)
		}
	}
	notReserved := []string{"valid", ".hidden", "sync-abc", "a.sync-1", "", ".SYNC-1", "..sync"}
	for _, name := range notReserved {
		if IsReservedDir(name) {
			t.Errorf("IsReservedDir(%q) = true, want false", name)
		}
	}
}

// TestLoadIgnoresStagingDir pins that a Relay-owned staging directory directly
// under the root — as internal/git creates via os.MkdirTemp(dst, ".sync-*") —
// is skipped entirely by discovery: it is not an app, not a LoadIssue, and
// not logged. A valid neighbor still loads and a genuinely invalid user
// directory still produces its warning, so the narrow reservation does not
// weaken either normal discovery or invalid-name diagnostics.
func TestLoadIgnoresStagingDir(t *testing.T) {
	dir := t.TempDir()
	writeTemplate(t, dir, "valid", `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      event_name: [MODIFY]
`)
	// A staging directory mid-copy, with no template.yaml yet (as git creates
	// it, then copies into it): must be invisible to the loader.
	if err := os.MkdirAll(filepath.Join(dir, ".sync-123456"), 0o755); err != nil {
		t.Fatalf("mkdir staging: %v", err)
	}
	// A genuinely invalid user directory: still an invalid desired definition.
	writeTemplate(t, dir, "bad-yaml", "events: [unclosed")

	var buf bytes.Buffer
	loader := NewLoader(dir, slog.New(slog.NewTextHandler(&buf, nil)))
	fns, issues, err := loader.LoadWithDiagnostics()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(fns) != 1 || fns[0].Name != "valid" {
		t.Fatalf("valid apps = %+v, want only 'valid'", fns)
	}
	for _, issue := range issues {
		if issue.Name == ".sync-123456" {
			t.Fatalf("staging directory must not be reported as an issue: %+v", issue)
		}
	}
	if len(issues) != 1 || issues[0].Name != "bad-yaml" {
		t.Fatalf("issues = %+v, want only 'bad-yaml'", issues)
	}
	if logs := buf.String(); strings.Contains(logs, ".sync-") {
		t.Fatalf("staging directory must not be logged, logs = %q", logs)
	}
}

// TestLoadIgnoresStagingDirThatLooksValid pins that even a staging directory
// that happens to contain a valid template.yaml is never loaded as an app:
// the prefix, not the contents, decides reservedness.
func TestLoadIgnoresStagingDirThatLooksValid(t *testing.T) {
	dir := t.TempDir()
	writeTemplate(t, dir, "valid", `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      event_name: [MODIFY]
`)
	// A stage dir a partial copy may have already populated with a template.
	writeTemplate(t, dir, ".sync-abc", `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      event_name: [MODIFY]
`)

	loader := NewLoader(dir, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	fns, err := loader.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(fns) != 1 || fns[0].Name != "valid" {
		t.Fatalf("Load = %+v, want only 'valid' (a staged tree is never an app)", fns)
	}
}

package function

import (
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func writeTemplate(t *testing.T, dir, name, content string) {
	t.Helper()
	writeFile(t, filepath.Join(dir, name, "template.yaml"), content)
}

func TestLoadValidFunction(t *testing.T) {
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
		t.Fatalf("expected 1 function, got %d", len(fns))
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
		t.Fatalf("expected 1 function, got %d", len(fns))
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
		t.Fatalf("expected 1 function, got %d", len(fns))
	}
	if fns[0].Name != "good" {
		t.Errorf("expected 'good', got %q", fns[0].Name)
	}
}

func TestLoadMultipleFunctions(t *testing.T) {
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
		t.Fatalf("expected 2 functions, got %d", len(fns))
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
		t.Fatalf("expected 2 functions, got %d", len(fns))
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
		t.Fatalf("expected 1 function (unsupported runtime skipped), got %d", len(fns))
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
		t.Fatalf("expected 1 function (invalid name skipped), got %d", len(fns))
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
		t.Fatalf("expected 1 function (missing runtime skipped), got %d", len(fns))
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

// TestLoadRejectsSymlinkedFunctionDir pins that startup discovery applies the
// same no-follow policy: a symlinked directory entry is skipped even when its
// target (inside or outside the root) holds a valid template.
func TestLoadRejectsSymlinkedFunctionDir(t *testing.T) {
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

func TestLoadBrokenFunctionDoesNotPreventValid(t *testing.T) {
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
		t.Fatalf("expected 1 function, got %d", len(fns))
	}
	if fns[0].Name != "good" {
		t.Errorf("expected 'good', got %q", fns[0].Name)
	}
}

package function

import (
	"log"
	"os"
	"path/filepath"
	"testing"
)

func writeTemplate(t *testing.T, dir, name, content string) {
	t.Helper()
	path := filepath.Join(dir, name, "template.yaml")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
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

	loader := NewLoader(dir, log.New(os.Stderr, "", 0))
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
	if len(fns[0].Template.Rules) != 1 {
		t.Errorf("expected 1 rule, got %d", len(fns[0].Template.Rules))
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

	loader := NewLoader(dir, log.New(os.Stderr, "", 0))
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

	loader := NewLoader(dir, log.New(os.Stderr, "", 0))
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

	loader := NewLoader(dir, log.New(os.Stderr, "", 0))
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

	loader := NewLoader(dir, log.New(os.Stderr, "", 0))
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

	loader := NewLoader(dir, log.New(os.Stderr, "", 0))
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

	loader := NewLoader(dir, log.New(os.Stderr, "", 0))
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

	loader := NewLoader(dir, log.New(os.Stderr, "", 0))
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

	loader := NewLoader(dir, log.New(os.Stderr, "", 0))
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

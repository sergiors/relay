package app

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestDirIsAppsRoot pins the fixed application-convention root: the loader reads
// apps from /apps, the one and only root. The constant is a public contract
// (the CLI and the git webhook default AppsDir to it), so a silent change here
// would move every discovered app.
func TestDirIsAppsRoot(t *testing.T) {
	if Dir != "/apps" {
		t.Fatalf("Dir = %q, want %q", Dir, "/apps")
	}
}

// TestInvalidEntryLogUsesAppField pins the structured log identity for a
// present-but-invalid app: the warning names the app under the "app" attribute,
// matching the rest of the app-identity telemetry (metrics label, tracing
// attribute). A regression to a "function" field key would silently desync the
// logs from the metrics/traces.
func TestInvalidEntryLogUsesAppField(t *testing.T) {
	dir := t.TempDir()
	// A present directory with an invalid template is logged and skipped.
	writeTemplate(t, dir, "bad-app", "runtime: python3.14\n")

	var buf bytes.Buffer
	loader := NewLoader(dir, slog.New(slog.NewTextHandler(&buf, nil)))
	apps, err := loader.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(apps) != 0 {
		t.Fatalf("invalid app must not load, got %d", len(apps))
	}
	logs := buf.String()
	if !strings.Contains(logs, "app=bad-app") {
		t.Fatalf("invalid-entry warning must carry app=bad-app, got:\n%s", logs)
	}
	if strings.Contains(logs, "function=bad-app") {
		t.Fatalf("invalid-entry warning must not carry a function field, got:\n%s", logs)
	}
}

// TestLoaderReadsConfiguredRoot pins that discovery is rooted at the directory
// the loader was constructed with, and that a direct child directory is loaded
// by its directory name (the app name).
func TestLoaderReadsConfiguredRoot(t *testing.T) {
	dir := t.TempDir()
	writeTemplate(t, dir, "demo", `
runtime: python3.14
events:
  - handler: handler.run
    pattern:
      event_name: [INSERT]
`)
	loader := NewLoader(dir, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	apps, err := loader.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(apps) != 1 || apps[0].Name != "demo" {
		t.Fatalf("apps = %+v, want one app named demo", apps)
	}
	if apps[0].Dir != filepath.Join(dir, "demo") {
		t.Fatalf("app dir = %q, want %q", apps[0].Dir, filepath.Join(dir, "demo"))
	}
}

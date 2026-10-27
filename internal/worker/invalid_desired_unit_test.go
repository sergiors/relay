package worker

import (
	"os"
	"path/filepath"
	"testing"

	"relay/internal/app"
)

// TestLoaderIgnoresStagingDirAtStartup pins the worker's startup loader path: a
// staging directory alongside a valid app yields exactly the valid app
// with no issue for the staging name.
func TestLoaderIgnoresStagingDirAtStartup(t *testing.T) {
	root := t.TempDir()
	writeWorkerApp(t, root, "good", "def handler(e): return 1\n")
	if err := os.MkdirAll(filepath.Join(root, ".sync-xyz"), 0o755); err != nil {
		t.Fatalf("mkdir stage: %v", err)
	}

	loader := app.NewLoader(root, discardLogger())
	fns, issues, err := loader.LoadWithDiagnostics()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(fns) != 1 || fns[0].Name != "good" {
		t.Fatalf("valid apps = %+v, want only good", fns)
	}
	if len(issues) != 0 {
		t.Fatalf("issues = %+v, want none (staging is not an invalid desired definition)", issues)
	}
}

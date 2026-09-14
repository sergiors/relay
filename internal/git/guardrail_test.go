package git

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
) // readSource returns the source file at relPath relative to the module root
// (../../ from this package). These are ARCHITECTURAL GUARDRAIL tests (not style
// checks): they pin that the automatic runtime never references git sync and
// that the git package keeps no background timers. They read source text, not
// runtime state, so they are cheap and deterministic.
func readSource(t *testing.T, relPath string) string {
	t.Helper()
	abs := filepath.Join("..", "..", relPath)
	data, err := os.ReadFile(abs)
	if err != nil {
		t.Fatalf("read %s: %v", relPath, err)
	}
	return string(data)
}

// TestWorkerNeverReferencesGit pins that the long-running runtime has NO git
// hooks: the worker/reconciler only watch /functions and must not know about
// git sync, which is a separate, manual operator command. A change that wires
// automatic git into `relay start` would show up here as a reference to
// "internal/git" or "git sync" in worker.go. This is intentionally a source-scan
// (cheap, deterministic, no runtime) — it guards the architectural boundary that
// git sync must never run on its own.
func TestWorkerNeverReferencesGit(t *testing.T) {
	worker := readSource(t, "internal/worker/worker.go")
	for _, forbid := range []string{"internal/git", "git sync", "git\\.Sync", "internal/git.GitDir"} {
		if strings.Contains(worker, forbid) {
			t.Fatalf("worker.go references git sync (%q); Relay's runtime must never sync on its own", forbid)
		}
	}
}

// TestGitPackageHasNoTimers pins that the git package performs NO background
// scheduling: sync is a single bounded one-shot operation (a caller-provided
// timeout), never a recurring poll or ticker. A future change that adds polling
// would introduce a time.Ticker/NewTicker here; this source-scan rejects it. The
// only time usage allowed is the one-shot timeout context and RFC3339
// timestamps, both of which use time.WithTimeout/Now and are deliberately not
// flagged.
func TestGitPackageHasNoTimers(t *testing.T) {
	// The git package source lives in the current directory (internal/git). Scan
	// every non-test .go file here.
	pkgDir := "."
	entries, err := os.ReadDir(pkgDir)
	if err != nil {
		t.Fatalf("read git dir: %v", err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(pkgDir, e.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		if strings.Contains(string(data), "time.NewTicker") ||
			strings.Contains(string(data), "time.Ticker") ||
			strings.Contains(string(data), "NewTimer") ||
			strings.Contains(string(data), "time.After(") {
			t.Fatalf("git package file %s must not schedule background timers/polls", e.Name())
		}
	}
}

// TestGitPackageNeverConstructsLoggers pins the DI convention: the git package
// receives loggers from callers and never constructs its own. The fallback-
// construction helper (which built a discard slog.Logger when nil) was
// deliberately removed in the logger-DI refactor, so any future
// `slog.New(` in the package would be a regression back to self-constructed
// loggers. Source-scan, same style as TestGitPackageHasNoTimers — cheap,
// deterministic, no runtime.
func TestGitPackageNeverConstructsLoggers(t *testing.T) {
	pkgDir := "."
	entries, err := os.ReadDir(pkgDir)
	if err != nil {
		t.Fatalf("read git dir: %v", err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(pkgDir, e.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		if strings.Contains(string(data), "slog.New(") {
			t.Fatalf("git package file %s must not construct its own logger (slog.New) — loggers are caller-injected (DI)", e.Name())
		}
	}
}

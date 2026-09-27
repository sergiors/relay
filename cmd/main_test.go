package main

import (
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	urfavecli "github.com/urfave/cli/v3"
)

// TestRunCommand_Success pins the success path of the process boundary: a nil
// error maps to exit code 0 and writes nothing to stderr, so a successful
// command exits 0 without emitting output.
func TestRunCommand_Success(t *testing.T) {
	var stderr bytes.Buffer

	if code := runCommand(func() error { return nil }, &stderr); code != 0 {
		t.Fatalf("runCommand returned %d, want 0", code)
	}
	if got := stderr.String(); got != "" {
		t.Fatalf("runCommand wrote %q to stderr, want no output", got)
	}
}

// TestRunCommand_GenericFailure pins the generic runtime-failure path: a plain
// error maps to exit code 1 and is printed exactly once to stderr.
func TestRunCommand_GenericFailure(t *testing.T) {
	var stderr bytes.Buffer
	err := errors.New("boom")

	if code := runCommand(func() error { return err }, &stderr); code != 1 {
		t.Fatalf("runCommand returned %d, want 1", code)
	}
	if got, want := stderr.String(), "boom\n"; got != want {
		t.Fatalf("stderr = %q, want %q", got, want)
	}
}

// TestRunCommand_ExitCoder pins that a urfave/cli ExitCoder (the cli.Exit(msg, 2)
// usage errors the command tree returns) propagates its explicit exit code and
// is still printed exactly once — not duplicated, not swallowed.
func TestRunCommand_ExitCoder(t *testing.T) {
	var stderr bytes.Buffer
	err := urfavecli.Exit("usage error", 2)

	if code := runCommand(func() error { return err }, &stderr); code != 2 {
		t.Fatalf("runCommand returned %d, want 2", code)
	}
	if got, want := stderr.String(), "usage error\n"; got != want {
		t.Fatalf("stderr = %q, want %q", got, want)
	}
}

// TestRunCommand_ExitCoderWrapped pins that the ExitCoder lookup uses errors.As,
// so a wrapped ExitCoder still yields its explicit code rather than the generic
// 1. Commands may wrap usage errors before returning them.
func TestRunCommand_ExitCoderWrapped(t *testing.T) {
	var stderr bytes.Buffer
	err := fmt.Errorf("context: %w", urfavecli.Exit("wrapped usage", 3))

	if code := runCommand(func() error { return err }, &stderr); code != 3 {
		t.Fatalf("runCommand returned %d, want 3", code)
	}
	if got, want := stderr.String(), "context: wrapped usage\n"; got != want {
		t.Fatalf("stderr = %q, want %q", got, want)
	}
}

// moduleRoot locates the module root by walking up from this test file's own
// directory until it finds go.mod. Resolving from runtime.Caller keeps the
// guardrail scan below independent of the test working directory.
func moduleRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller could not locate cmd test")
	}
	dir := filepath.Dir(thisFile)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("walked to filesystem root from %s without finding go.mod", filepath.Dir(thisFile))
		}
		dir = parent
	}
}

// TestInternalPackagesDoNotCallOsExit pins the process-boundary contract: only
// cmd/main.go owns os.Exit. If internal command, worker, or config code called
// os.Exit it would tear down the process without unwinding the command
// boundary, breaking error propagation and testability. The scan walks EVERY
// production Go file under internal recursively (not just cli/worker), so a
// new os.Exit anywhere in the internal tree — config included — is caught.
// Test files are skipped (subprocess helpers legitimately use os.Exit). The
// scan parses the AST (not raw source) so comments discussing os.Exit, like
// worker.go's, are not false positives.
func TestInternalPackagesDoNotCallOsExit(t *testing.T) {
	root := moduleRoot(t)
	internalDir := filepath.Join(root, "internal")
	fset := token.NewFileSet()
	scanned := 0

	err := filepath.WalkDir(internalDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}
		scanned++
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			ident, ok := sel.X.(*ast.Ident)
			if ok && ident.Name == "os" && sel.Sel.Name == "Exit" {
				t.Errorf("%s references os.Exit; only cmd/main.go owns the process exit", path)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("scan internal packages: %v", err)
	}
	if scanned == 0 {
		t.Fatalf("scanned no production files under %s; the guard is not effective", internalDir)
	}
}

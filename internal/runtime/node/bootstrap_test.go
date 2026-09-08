package node

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestBootstrapContent is a static check that the embedded bootstrap implements
// the contract the engine relies on (handler resolution, stdin event reading,
// module resolution). It is distinct from the run-a-real-interpreter tests below.
func TestBootstrapContent(t *testing.T) {
	bs := string(Bootstrap)
	if !strings.Contains(bs, "RELAY_HANDLER") {
		t.Error("node bootstrap must read RELAY_HANDLER")
	}
	if !strings.Contains(bs, "lastIndexOf") {
		t.Error("node bootstrap must split handler at the last dot")
	}
	if !strings.Contains(bs, "statSync") {
		t.Error("node bootstrap must resolve modules via the filesystem, not import attempts")
	}
	if !strings.Contains(bs, "module not found") {
		t.Error("node bootstrap must report a clear module-not-found error")
	}
	if !strings.Contains(bs, "import(modPath)") || !strings.Contains(bs, "await import") {
		t.Error("node bootstrap must import the resolved module exactly once")
	}
	if !strings.Contains(bs, "stdin") {
		t.Error("node bootstrap must read event from stdin")
	}
}

// appRoot returns a temp dir root for the current test. It creates the in-image
// app and relay subdirectories and returns the root.
func appRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "app"), 0o755); err != nil {
		t.Fatalf("mkdir app: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "relay"), 0o755); err != nil {
		t.Fatalf("mkdir relay: %v", err)
	}
	return root
}

// writeModule writes a module at root/app/<modPath> (e.g. modPath "src/email.mjs").
func writeModule(t *testing.T, root, modPath, src string) {
	t.Helper()
	path := filepath.Join(root, "app", filepath.FromSlash(modPath))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		t.Fatalf("write module: %v", err)
	}
}

// runNode runs the real embedded bootstrap under node. The bootstrap writes
// the handler into root/app (mirroring the in-image /app layout) and runs it.
// It returns the combined output and whether the process exited non-zero.
func runNode(t *testing.T, root, handler, stdin string) (string, bool) {
	t.Helper()
	// Point the bootstrap at root/app instead of /app, mirroring the image.
	bs := string(Bootstrap)
	bs = strings.Replace(
		bs,
		`const base = "/app/"`,
		"const base = "+quote(filepath.Join(root, "app")+"/"),
		1,
	)
	bsPath := filepath.Join(root, "relay", "bootstrap.mjs")
	if err := os.WriteFile(bsPath, []byte(bs), 0o644); err != nil {
		t.Fatalf("write bootstrap: %v", err)
	}

	cmd := exec.Command("node", bsPath)
	cmd.Env = append(os.Environ(), "RELAY_HANDLER="+handler)
	cmd.Stdin = strings.NewReader(stdin)
	out, err := cmd.CombinedOutput()
	return string(out), err != nil
}

func quote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func skipIfNoNode(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not available")
	}
}

func TestNodeRuntimeSync(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
export function sync_handler(event) {
  console.log("sync " + event.msg);
}
`)
	out, failed := runNode(t, root, "index.sync_handler", `{"msg":"hi"}`)
	if failed {
		t.Fatalf("expected success, output: %s", out)
	}
	if !strings.Contains(out, "sync hi") {
		t.Errorf("output = %q, want to contain 'sync hi'", out)
	}
}

func TestNodeRuntimeAsync(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
export async function async_handler(event) {
  console.log("async " + event.msg);
}
`)
	out, failed := runNode(t, root, "index.async_handler", `{"msg":"hi"}`)
	if failed {
		t.Fatalf("expected success, output: %s", out)
	}
	if !strings.Contains(out, "async hi") {
		t.Errorf("output = %q, want to contain 'async hi'", out)
	}
}

func TestNodeRuntimeNestedModule(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeModule(t, root, "src/email.mjs", `
export function send(event) {
  console.log("send " + event.msg);
}
`)
	out, failed := runNode(t, root, "src.email.send", `{"msg":"hi"}`)
	if failed {
		t.Fatalf("expected success, output: %s", out)
	}
	if !strings.Contains(out, "send hi") {
		t.Errorf("output = %q, want to contain 'send hi'", out)
	}
}

func TestNodeRuntimeMissingModule(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	out, failed := runNode(t, root, "absent.exists", `{}`)
	if !failed {
		t.Fatalf("expected failure for missing module, output: %s", out)
	}
	if !strings.Contains(out, "module not found") {
		t.Errorf("output = %q, want to contain 'module not found'", out)
	}
}

func TestNodeRuntimeMissingExport(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
export function present(event) {}
`)
	out, failed := runNode(t, root, "index.absent", `{}`)
	if !failed {
		t.Fatalf("expected failure for missing export, output: %s", out)
	}
	if !strings.Contains(out, "has no function") {
		t.Errorf("output = %q, want to contain 'has no function'", out)
	}
}

func TestNodeRuntimeMalformedJSON(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
export function f(event) {}
`)
	out, failed := runNode(t, root, "index.f", `{not json`)
	if !failed {
		t.Fatalf("expected failure for malformed JSON, output: %s", out)
	}
	if !strings.Contains(out, "failed to read event from stdin") {
		t.Errorf("output = %q, want to contain 'failed to read event from stdin'", out)
	}
}

func TestNodeRuntimeHandlerException(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
export function boom(event) {
  throw new Error("kaboom");
}
`)
	out, failed := runNode(t, root, "index.boom", `{}`)
	if !failed {
		t.Fatalf("expected failure for handler exception, output: %s", out)
	}
	if !strings.Contains(out, "failed:") || !strings.Contains(out, "kaboom") {
		t.Errorf("output = %q, want to contain handler failure with 'kaboom'", out)
	}
}

// TestNodeRuntimeBrokenImport is the regression test for the module resolution
// bug: a module that exists but import a missing dependency must surface the
// REAL import error, not a "module not found" message.
func TestNodeRuntimeBrokenImport(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
import "missing-package";
export function f(event) {}
`)
	out, failed := runNode(t, root, "index.f", `{}`)
	if !failed {
		t.Fatalf("expected failure for broken import, output: %s", out)
	}
	if strings.Contains(out, "module not found") {
		t.Errorf("broken import must NOT be reported as 'module not found', output: %s", out)
	}
	if !strings.Contains(out, "missing-package") {
		t.Errorf("output = %q, expected the real import error to mention 'missing-package'", out)
	}
}

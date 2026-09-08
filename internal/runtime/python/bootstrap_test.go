package python

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestBootstrapContent is a static check that the embedded bootstrap implements
// the contract the engine relies on (handler resolution, stdin event reading,
// async support). It is distinct from the run-a-real-interpreter tests below.
func TestBootstrapContent(t *testing.T) {
	bs := string(Bootstrap)
	if !strings.Contains(bs, "RELAY_HANDLER") {
		t.Error("python bootstrap must read RELAY_HANDLER")
	}
	if !strings.Contains(bs, "rpartition") {
		t.Error("python bootstrap must split handler at the last dot")
	}
	if !strings.Contains(bs, "stdin") {
		t.Error("python bootstrap must read event from stdin")
	}
	if !strings.Contains(bs, "inspect.isawaitable") {
		t.Error("python bootstrap must support async handlers via inspect.isawaitable")
	}
	if !strings.Contains(bs, "asyncio.run") {
		t.Error("python bootstrap must run async handlers via asyncio.run")
	}
}

// runPython runs the real embedded bootstrap under python3 with PYTHONPATH
// pointing at dir (which holds the handler module). It returns the combined
// output and whether the process exited non-zero.
func runPython(t *testing.T, dir, handler, stdin string) (string, bool) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "bootstrap.py"), Bootstrap, 0o644); err != nil {
		t.Fatalf("write bootstrap: %v", err)
	}
	cmd := exec.Command("python3", filepath.Join(dir, "bootstrap.py"))
	cmd.Env = append(os.Environ(),
		"PYTHONPATH="+dir,
		"RELAY_HANDLER="+handler,
	)
	cmd.Stdin = strings.NewReader(stdin)
	out, err := cmd.CombinedOutput()
	return string(out), err != nil
}

// writeHandler writes <module>.py into dir for the given module name (e.g.
// "handler" -> handler.py, "src.email" -> src/email.py).
func writeHandler(t *testing.T, dir, module, src string) {
	t.Helper()
	path := filepath.Join(dir, filepath.FromSlash(strings.ReplaceAll(module, ".", "/"))+".py")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		t.Fatalf("write handler: %v", err)
	}
}

func skipIfNoPython(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available")
	}
}

func TestPythonRuntimeSync(t *testing.T) {
	skipIfNoPython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
def sync_handler(event):
    print("sync %s" % event["msg"])
`)
	out, failed := runPython(t, dir, "handler.sync_handler", `{"msg":"hi"}`)
	if failed {
		t.Fatalf("expected success, output: %s", out)
	}
	if !strings.Contains(out, "sync hi") {
		t.Errorf("output = %q, want to contain 'sync hi'", out)
	}
}

func TestPythonRuntimeAsync(t *testing.T) {
	skipIfNoPython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
async def async_handler(event):
    print("async %s" % event["msg"])
`)
	out, failed := runPython(t, dir, "handler.async_handler", `{"msg":"hi"}`)
	if failed {
		t.Fatalf("expected success for async handler, output: %s", out)
	}
	if !strings.Contains(out, "async hi") {
		t.Errorf("output = %q, want to contain 'async hi'", out)
	}
}

func TestPythonRuntimeMissingModule(t *testing.T) {
	skipIfNoPython(t)
	dir := t.TempDir()
	out, failed := runPython(t, dir, "doesnotexist.main", `{}`)
	if !failed {
		t.Fatalf("expected failure for missing module, output: %s", out)
	}
	if !strings.Contains(out, "failed to import module") {
		t.Errorf("output = %q, want to contain 'failed to import module'", out)
	}
}

func TestPythonRuntimeMissingFunction(t *testing.T) {
	skipIfNoPython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
def present(event):
    pass
`)
	out, failed := runPython(t, dir, "handler.absent", `{}`)
	if !failed {
		t.Fatalf("expected failure for missing function, output: %s", out)
	}
	if !strings.Contains(out, "has no function") {
		t.Errorf("output = %q, want to contain 'has no function'", out)
	}
}

func TestPythonRuntimeMalformedJSON(t *testing.T) {
	skipIfNoPython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
def f(event):
    pass
`)
	out, failed := runPython(t, dir, "handler.f", `{not json`)
	if !failed {
		t.Fatalf("expected failure for malformed JSON, output: %s", out)
	}
	if !strings.Contains(out, "failed to read event from stdin") {
		t.Errorf("output = %q, want to contain 'failed to read event from stdin'", out)
	}
}

func TestPythonRuntimeHandlerException(t *testing.T) {
	skipIfNoPython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
def boom(event):
    raise ValueError("kaboom")
`)
	out, failed := runPython(t, dir, "handler.boom", `{}`)
	if !failed {
		t.Fatalf("expected failure for handler exception, output: %s", out)
	}
	if !strings.Contains(out, "handler ") || !strings.Contains(out, "kaboom") {
		t.Errorf("output = %q, want to contain handler failure with 'kaboom'", out)
	}
}

package python

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// relaySentinel is the protocol response prefix. It is hardcoded here (rather
// than imported) to avoid an import cycle with the Go runtime package.
const relaySentinel = "@@RELAY@@"

// TestBootstrapIsEmbeddedAndAddsAppToSysPath asserts the two things static
// source inspection is actually needed for. The process-behavior tests below
// cover the rest of the invocation protocol (sentinel framing, module import,
// async handlers, env rotation, error handling), so they are no longer
// duplicated as brittle substring checks. The sys.path assertion is kept because
// it pins the specific import contract: function sources are importable by
// module name (the package-relative service entrypoint depends on it).
func TestBootstrapIsEmbeddedAndAddsAppToSysPath(t *testing.T) {
	if len(Bootstrap) == 0 {
		t.Fatal("embedded python bootstrap is empty")
	}
	if !strings.Contains(string(Bootstrap), `sys.path.insert(0, "/app")`) {
		t.Error(`python bootstrap must put function sources on sys.path via sys.path.insert(0, "/app")`)
	}
}

// pyProc is a live bootstrap process driven by the tests: request lines are
// written to its stdin, protocol responses are read from its stdout, and any
// other stdout output (user prints) plus stderr are collected.
type pyProc struct {
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	stdout   *bufio.Reader
	seq      int
	stderrMu sync.Mutex
	stderr   strings.Builder
}

// invokeResult is one protocol response plus any user output that arrived
// while waiting for it.
type invokeResult struct {
	OK    bool
	Err   string
	Out   string
	Frame string
}

// startPython runs the real embedded bootstrap under python3 with PYTHONPATH
// pointing at dir (which holds the handler modules).
func startPython(t *testing.T, dir string) *pyProc {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "bootstrap.py"), Bootstrap, 0o644); err != nil {
		t.Fatalf("write bootstrap: %v", err)
	}
	cmd := exec.Command("python3", filepath.Join(dir, "bootstrap.py"))
	cmd.Env = append(os.Environ(), "PYTHONPATH="+dir)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin pipe: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	p := &pyProc{cmd: cmd, stdin: stdin, stdout: bufio.NewReader(stdout)}
	go func() {
		_, _ = io.Copy(p.stderrWriter(), stderr)
	}()
	if err := cmd.Start(); err != nil {
		t.Fatalf("start bootstrap: %v", err)
	}
	t.Cleanup(func() {
		_ = stdin.Close()
		_ = cmd.Wait()
	})
	return p
}

func (p *pyProc) stderrWriter() io.Writer { return p }
func (p *pyProc) Write(b []byte) (int, error) {
	p.stderrMu.Lock()
	defer p.stderrMu.Unlock()
	_, _ = p.stderr.Write(b)
	return len(b), nil
}

func (p *pyProc) stderrString() string {
	p.stderrMu.Lock()
	defer p.stderrMu.Unlock()
	return p.stderr.String()
}

// invoke writes one request frame and reads stdout until a sentinel protocol
// response line arrives, collecting any interleaved user output. It fails the
// test on protocol EOF (the process died mid-invocation).
func (p *pyProc) invoke(t *testing.T, handler string, event string, env map[string]string) invokeResult {
	t.Helper()
	id := fmt.Sprintf("t%d", p.seq)
	p.seq++
	frame, err := json.Marshal(reqFrame{ID: id, Handler: handler, Event: json.RawMessage(event), Env: env})
	if err != nil {
		t.Fatalf("build request frame: %v", err)
	}
	if _, err := p.stdin.Write(append(frame, '\n')); err != nil {
		t.Fatalf("write request frame: %v", err)
	}
	var res invokeResult
	for {
		line, err := p.stdout.ReadBytes('\n')
		if err != nil {
			t.Fatalf("stdout EOF before a protocol response (process died): %v; stderr:\n%s", err, p.stderrString())
		}
		s := strings.TrimRight(string(line), "\n")
		if strings.HasPrefix(s, relaySentinel) {
			res.Frame = s
			var resp respFrame
			if err := json.Unmarshal([]byte(strings.TrimPrefix(s, relaySentinel)), &resp); err != nil {
				t.Fatalf("response frame is not parseable JSON: %q", s)
			}
			if resp.ID != id {
				t.Fatalf("response id = %q, want %q", resp.ID, id)
			}
			res.OK = resp.OK
			res.Err = resp.Error
			return res
		}
		res.Out += s + "\n"
	}
}

type reqFrame struct {
	ID      string            `json:"id"`
	Handler string            `json:"handler"`
	Event   json.RawMessage   `json:"event"`
	Env     map[string]string `json:"env"`
}

type respFrame struct {
	ID    string `json:"id"`
	OK    bool   `json:"ok"`
	Error string `json:"error"`
}

// writeHandler writes a handler module into the test PYTHONPATH dir.
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

func TestPythonRuntimeTwoInvocationsAndModuleState(t *testing.T) {
	skipIfNoPython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
_counter = {"n": 0}

def count(event):
    _counter["n"] += 1
    print("count=%d" % _counter["n"])

def completed(event):
    print("completed %s" % event["msg"])
`)
	p := startPython(t, dir)

	r1 := p.invoke(t, "handler.count", `{}`, nil)
	r2 := p.invoke(t, "handler.count", `{}`, nil)
	r3 := p.invoke(t, "handler.completed", `{"msg":"hi"}`, nil)
	for i, r := range []invokeResult{r1, r2, r3} {
		if !r.OK {
			t.Fatalf("invoke %d failed: %s", i+1, r.Err)
		}
	}
	if got := strings.TrimSpace(r1.Out); got != "count=1" {
		t.Errorf("first count out = %q, want count=1", got)
	}
	if got := strings.TrimSpace(r2.Out); got != "count=2" {
		t.Errorf("second count out = %q, want count=2 (module state persisted, interpreter stayed alive)", got)
	}
	if !strings.Contains(r3.Out, "completed hi") {
		t.Errorf("third invoke (different handler) out = %q, want it to contain 'completed hi'", r3.Out)
	}
}

func TestPythonRuntimeAsyncHandler(t *testing.T) {
	skipIfNoPython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
import asyncio

async def async_handler(event):
    await asyncio.sleep(0)
    print("async %s" % event["msg"])
`)
	p := startPython(t, dir)
	r := p.invoke(t, "handler.async_handler", `{"msg":"hi"}`, nil)
	if !r.OK {
		t.Fatalf("expected success for async handler, error: %s", r.Err)
	}
	if !strings.Contains(r.Out, "async hi") {
		t.Errorf("out = %q, want to contain 'async hi'", r.Out)
	}
}

func TestPythonRuntimeHandlerErrorKeepsProcessAlive(t *testing.T) {
	skipIfNoPython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
_counter = {"n": 0}

def count(event):
    _counter["n"] += 1
    print("count=%d" % _counter["n"])

def boom(event):
    raise ValueError("kaboom")
`)
	p := startPython(t, dir)

	r := p.invoke(t, "handler.boom", `{}`, nil)
	if r.OK {
		t.Fatal("expected the raising handler to fail the invocation")
	}
	if !strings.Contains(r.Err, "kaboom") {
		t.Errorf("error = %q, want it to contain 'kaboom'", r.Err)
	}
	// The stderr mirror is asynchronous in the test harness; poll briefly.
	stderrOK := false
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(p.stderrString(), "failed") {
			stderrOK = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !stderrOK {
		t.Errorf("stderr = %q, want the operator-visible failure log", p.stderrString())
	}
	// The process must stay healthy: a THIRD invocation succeeds and the
	// module state survived.
	r2 := p.invoke(t, "handler.count", `{}`, nil)
	if !r2.OK || !strings.Contains(r2.Out, "count=1") {
		t.Fatalf("post-error invoke = %+v, want count=1 on a healthy container", r2)
	}
}

func TestPythonRuntimeEnvRotation(t *testing.T) {
	skipIfNoPython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
import os

def showenv(event):
    print("VAR=" + os.environ.get("VAR", ""))
`)
	p := startPython(t, dir)

	r1 := p.invoke(t, "handler.showenv", `{}`, map[string]string{"VAR": "one", "SECRET": "aaa"})
	if !r1.OK || !strings.Contains(r1.Out, "VAR=one") {
		t.Fatalf("first env invoke: %+v, want VAR=one", r1)
	}
	r2 := p.invoke(t, "handler.showenv", `{}`, map[string]string{"VAR": "two"})
	if !r2.OK || !strings.Contains(r2.Out, "VAR=two") {
		t.Fatalf("second env invoke: %+v, want VAR=two (rotated)", r2)
	}
}

func TestPythonRuntimeEOFExitsZero(t *testing.T) {
	skipIfNoPython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", "def f(event):\n    pass\n")
	p := startPython(t, dir)
	r := p.invoke(t, "handler.f", `{}`, nil)
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	if err := p.stdin.Close(); err != nil {
		t.Fatalf("close stdin: %v", err)
	}
	err := p.cmd.Wait()
	if err != nil {
		t.Fatalf("bootstrap did not exit 0 on stdin EOF: %v; stderr:\n%s", err, p.stderrString())
	}
}

func TestPythonRuntimeMalformedFrameIsFatal(t *testing.T) {
	skipIfNoPython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", "def f(event):\n    pass\n")
	if err := os.MkdirAll(filepath.Join(dir, "relay"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "relay", "bootstrap.py"), Bootstrap, 0o644); err != nil {
		t.Fatalf("write bootstrap: %v", err)
	}
	cmd := exec.Command("python3", filepath.Join(dir, "bootstrap.py"))
	cmd.Env = append(os.Environ(), "PYTHONPATH="+dir)
	cmd.Stdin = strings.NewReader("this is not json\n")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected nonzero exit for a malformed request frame, out: %s", out)
	}
}

func TestPythonRuntimeFrameShape(t *testing.T) {
	skipIfNoPython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", "def f(event):\n    pass\n")
	p := startPython(t, dir)
	r := p.invoke(t, "handler.f", `{}`, nil)
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	if got, want := strings.Count(r.Frame, "\n"), 0; got != want {
		t.Errorf("response frame %q must be EXACTLY one line", r.Frame)
	}
	if !strings.HasPrefix(r.Frame, relaySentinel) {
		t.Errorf("response frame %q must start with the sentinel %q", r.Frame, relaySentinel)
	}
	if !json.Valid([]byte(strings.TrimPrefix(r.Frame, relaySentinel))) {
		t.Errorf("response frame payload %q must be valid JSON", r.Frame)
	}
}

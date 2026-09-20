package node

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
)

// relaySentinel is the protocol response prefix. It is hardcoded here (rather
// than imported) to avoid an import cycle with the Go runtime package.
const relaySentinel = "@@RELAY@@"

// TestBootstrapContent is a static check that the embedded bootstrap implements
// the persistent invocation protocol the engine and the Go side rely on.
func TestBootstrapContent(t *testing.T) {
	bs := string(Bootstrap)
	for _, want := range []struct {
		frag string
		why  string
	}{
		{"@@RELAY@@", "must write the @@RELAY@@ sentinel protocol prefix"},
		{"lastIndexOf", "must split the handler at the last dot"},
		{"statSync", "must resolve modules via the filesystem, not import attempts"},
		{"module not found", "must report a clear module-not-found error"},
		{`import(modPath)`, "must import the resolved module"},
		{"moduleCache", "must cache import promises so module state persists"},
		{"process.stdin", "must read request lines from stdin"},
		{"process.exit(0)", "must exit cleanly on stdin EOF"},
		{"process.env", "must apply per-request env to process.env"},
	} {
		if !strings.Contains(bs, want.frag) {
			t.Errorf("node bootstrap must contain %q (%s)", want.frag, want.why)
		}
	}
}

// nodeProc is a live bootstrap process driven by the tests.
type nodeProc struct {
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

func (r invokeResult) hasErrorText(frag string) bool { return strings.Contains(r.Err, frag) }

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

// bootstrapFor returns the embedded bootstrap with the /app base rewritten to
// root/app, mirroring the in-image layout.
func bootstrapFor(root string) string {
	bs := string(Bootstrap)
	return strings.Replace(
		bs,
		`const base = "/app/" + parts.join("/")`,
		fmt.Sprintf(`const base = %s + parts.join("/")`, quote(filepath.Join(root, "app")+"/")),
		1,
	)
}

// startNode runs the real embedded bootstrap under node. The bootstrap is
// written into root/relay with the /app base rewritten to root/app, mirroring
// the in-image layout.
func startNode(t *testing.T, root string) *nodeProc {
	t.Helper()
	bs := bootstrapFor(root)
	bsPath := filepath.Join(root, "relay", "bootstrap.mjs")
	if err := os.WriteFile(bsPath, []byte(bs), 0o644); err != nil {
		t.Fatalf("write bootstrap: %v", err)
	}
	cmd := exec.Command("node", bsPath)
	cmd.Env = os.Environ()
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
	p := &nodeProc{cmd: cmd, stdin: stdin, stdout: bufio.NewReader(stdout)}
	go func() {
		p.writeErr(stderr)
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

func (p *nodeProc) writeErr(r io.Reader) {
	_, _ = io.Copy(p, r)
}

func (p *nodeProc) Write(b []byte) (int, error) {
	p.stderrMu.Lock()
	defer p.stderrMu.Unlock()
	_, _ = p.stderr.Write(b)
	return len(b), nil
}

func (p *nodeProc) stderrString() string {
	p.stderrMu.Lock()
	defer p.stderrMu.Unlock()
	return p.stderr.String()
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

func quote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// invoke writes one request frame and reads stdout until a sentinel protocol
// response line arrives.
func (p *nodeProc) invoke(t *testing.T, handler string, event string, env map[string]string) invokeResult {
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

func skipIfNoNode(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not available")
	}
}

func TestNodeRuntimeTwoInvocationsAndModuleState(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
const counter = { n: 0 };

export function count() {
  counter.n += 1;
  console.log("count=" + counter.n);
}

export function send(event) {
  console.log("send " + event.msg);
}
`)
	p := startNode(t, root)

	r1 := p.invoke(t, "index.count", `{}`, nil)
	r2 := p.invoke(t, "index.count", `{}`, nil)
	r3 := p.invoke(t, "index.send", `{"msg":"hi"}`, nil)
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
	if !strings.Contains(r3.Out, "send hi") {
		t.Errorf("third invoke (different handler) out = %q, want it to contain 'send hi'", r3.Out)
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
	p := startNode(t, root)
	r := p.invoke(t, "src.email.send", `{"msg":"hi"}`, nil)
	if !r.OK {
		t.Fatalf("expected success, error: %s", r.Err)
	}
	if !strings.Contains(r.Out, "send hi") {
		t.Errorf("out = %q, want to contain 'send hi'", r.Out)
	}
}

func TestNodeRuntimeAsyncHandler(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
export async function async_handler(event) {
  await new Promise((r) => setTimeout(r, 0));
  console.log("async " + event.msg);
}
`)
	p := startNode(t, root)
	r := p.invoke(t, "index.async_handler", `{"msg":"hi"}`, nil)
	if !r.OK {
		t.Fatalf("expected success for async handler, error: %s", r.Err)
	}
	if !strings.Contains(r.Out, "async hi") {
		t.Errorf("out = %q, want to contain 'async hi'", r.Out)
	}
}

func TestNodeRuntimeHandlerErrorKeepsProcessAlive(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
const counter = { n: 0 };

export function count() {
  counter.n += 1;
  console.log("count=" + counter.n);
}

export function boom() {
  throw new Error("kaboom");
}
`)
	p := startNode(t, root)

	r := p.invoke(t, "index.boom", `{}`, nil)
	if r.OK {
		t.Fatal("expected the raising handler to fail the invocation")
	}
	if !strings.Contains(r.Err, "kaboom") {
		t.Errorf("error = %q, want it to contain 'kaboom'", r.Err)
	}
	// A missing handler/module is an invocation failure too, not a crash.
	rMissing := p.invoke(t, "index.absent", `{}`, nil)
	if rMissing.OK || !rMissing.hasErrorText("has no function") {
		t.Errorf("missing export = %+v, want an ok:false with 'has no function'", rMissing)
	}
	// The process must stay healthy: a THIRD invocation succeeds and module
	// state survived.
	r2 := p.invoke(t, "index.count", `{}`, nil)
	if !r2.OK || !strings.Contains(r2.Out, "count=1") {
		t.Fatalf("post-error invoke = %+v, want count=1 on a healthy container", r2)
	}
}

func TestNodeRuntimeMissingModule(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	p := startNode(t, root)
	r := p.invoke(t, "absent.exists", `{}`, nil)
	if r.OK {
		t.Fatal("expected failure for missing module")
	}
	if !r.hasErrorText("module not found") {
		t.Errorf("error = %q, want 'module not found'", r.Err)
	}
}

// TestNodeRuntimeBrokenImport keeps the regression test for the module
// resolution bug: a module that exists but imports a missing dependency must
// surface the REAL import error, not a "module not found" message, and the
// container (bootstrap process) stays healthy.
func TestNodeRuntimeBrokenImport(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
import "missing-package";
export function f(event) {}
`)
	p := startNode(t, root)
	r := p.invoke(t, "index.f", `{}`, nil)
	if r.OK {
		t.Fatal("expected failure for broken import")
	}
	if strings.Contains(r.Err, "module not found") {
		t.Errorf("broken import must NOT be reported as 'module not found', error: %s", r.Err)
	}
	if !strings.Contains(r.Err, "missing-package") {
		t.Errorf("error = %q, expected the real import error to mention 'missing-package'", r.Err)
	}
	r2 := p.invoke(t, "index.f", `{}`, nil)
	if r2.OK || !r2.hasErrorText("missing-package") {
		t.Errorf("second invoke = %+v, want the same real failure (cached import), not a crash", r2)
	}
}

func TestNodeRuntimeEnvRotation(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
export function env() {
  console.log("VAR=" + (process.env.VAR ?? ""));
}
`)
	p := startNode(t, root)

	r1 := p.invoke(t, "index.env", `{}`, map[string]string{"VAR": "one", "SECRET": "aaa"})
	if !r1.OK || !strings.Contains(r1.Out, "VAR=one") {
		t.Fatalf("first env invoke: %+v, want VAR=one", r1)
	}
	r2 := p.invoke(t, "index.env", `{}`, map[string]string{"VAR": "two"})
	if !r2.OK || !strings.Contains(r2.Out, "VAR=two") {
		t.Fatalf("second env invoke: %+v, want VAR=two (rotated)", r2)
	}
}

func TestNodeRuntimeEOFExitsZero(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `export function f(event) {}`)
	p := startNode(t, root)
	r := p.invoke(t, "index.f", `{}`, nil)
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

func TestNodeRuntimeMalformedFrameIsFatal(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `export function f(event) {}`)
	if err := os.WriteFile(filepath.Join(root, "relay", "bootstrap.mjs"), []byte(bootstrapFor(root)), 0o644); err != nil {
		t.Fatalf("write bootstrap: %v", err)
	}
	cmd := exec.Command("node", filepath.Join(root, "relay", "bootstrap.mjs"))
	cmd.Env = os.Environ()
	cmd.Stdin = strings.NewReader("this is not json\n")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected nonzero exit for a malformed request frame, out: %s", out)
	}
}

// TestNodeRuntimeFrameShape asserts one line, sentinel-prefix, valid JSON.
func TestNodeRuntimeFrameShape(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `export function f(event) {}`)
	p := startNode(t, root)
	r := p.invoke(t, "index.f", `{}`, nil)
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	if strings.Contains(r.Frame, "\n") {
		t.Errorf("response frame %q must be EXACTLY one line", r.Frame)
	}
	if !strings.HasPrefix(r.Frame, relaySentinel) {
		t.Errorf("response frame %q must start with the sentinel %q", r.Frame, relaySentinel)
	}
	if !json.Valid([]byte(strings.TrimPrefix(r.Frame, relaySentinel))) {
		t.Errorf("response frame payload %q must be valid JSON", r.Frame)
	}
}

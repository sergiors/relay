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
	"unicode/utf8"
)

// relaySentinel is the protocol response prefix. It is hardcoded here (rather
// than imported) to avoid an import cycle with the Go runtime package.
const relaySentinel = "@@RELAY@@"

// TestBootstrapIsEmbeddedAndResolvesModulesByStat asserts the two things static
// source inspection is actually needed for. The process-behavior tests below
// cover the rest of the invocation protocol (sentinel framing, module caching,
// env rotation, error handling), so they are no longer duplicated as brittle
// substring checks. The stat-sync assertion is kept because it pins the specific
// module-resolution contract: modules are resolved via the filesystem (statSync),
// never by attempting an import of a path that may not exist.
func TestBootstrapIsEmbeddedAndResolvesModulesByStat(t *testing.T) {
	if len(Bootstrap) == 0 {
		t.Fatal("embedded node bootstrap is empty")
	}
	if !strings.Contains(string(Bootstrap), "statSync") {
		t.Error("node bootstrap must resolve modules via filesystem stat (statSync), not import attempts")
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
	if err := os.WriteFile(filepath.Join(root, "app", "package.json"), []byte(`{"type":"module"}`), 0o644); err != nil {
		t.Fatalf("write app package: %v", err)
	}
	writeOTelAPIStub(t, root)
	return root
}

func writeOTelAPIStub(t *testing.T, root string) {
	t.Helper()
	dir := filepath.Join(root, "app", "node_modules", "@opentelemetry", "api")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir OTel API stub: %v", err)
	}
	write := func(name, contents string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o644); err != nil {
			t.Fatalf("write OTel API stub: %v", err)
		}
	}
	write("package.json", `{"main":"index.cjs"}`)
	write("index.cjs", `
const { AsyncLocalStorage } = require("node:async_hooks");
const root = {};
const storage = new AsyncLocalStorage();
exports.context = { active: () => storage.getStore() ?? root, with: (value, fn) => storage.run(value, fn) };
exports.propagation = { extract: (base, carrier) => Object.keys(carrier).length ? { parent: carrier.traceparent ?? "", state: carrier.tracestate ?? "", baggage: carrier.baggage ?? "" } : base };
`)
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
	bs = strings.Replace(bs, `createRequire("/app/package.json")`, fmt.Sprintf(`createRequire(%s)`, quote(filepath.Join(root, "app", "package.json"))), 1)
	return strings.Replace(
		bs,
		`const base = "/app/" + parts.join("/")`,
		fmt.Sprintf(`const base = %s + parts.join("/")`, quote(filepath.Join(root, "app")+"/")),
		1,
	)
}

// startNode runs the real embedded bootstrap under node. The bootstrap is
// written into root/relay with the /app base rewritten to root/app, mirroring
// the in-image layout. Two controlled variables are seeded into the child
// environment so the exact-per-invocation env tests can prove baseline
// restoration and system-var preservation:
//
//	RELAY_TEST_BASELINE=process-baseline  (a key Relay may override then restore)
//	RELAY_TEST_SYSTEM=system-value        (a key Relay never touches)
func startNode(t *testing.T, root string) *nodeProc {
	t.Helper()
	bs := bootstrapFor(root)
	bsPath := filepath.Join(root, "relay", "bootstrap.mjs")
	if err := os.WriteFile(bsPath, []byte(bs), 0o644); err != nil {
		t.Fatalf("write bootstrap: %v", err)
	}
	cmd := exec.Command("node", bsPath)
	cmd.Env = append(os.Environ(),
		"RELAY_TEST_BASELINE=process-baseline",
		"RELAY_TEST_SYSTEM=system-value",
	)
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
	Trace   map[string]string `json:"trace,omitempty"`
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
	return p.invokeTrace(t, handler, event, env, nil)
}

func (p *nodeProc) invokeTrace(t *testing.T, handler string, event string, env, trace map[string]string) invokeResult {
	t.Helper()
	id := fmt.Sprintf("t%d", p.seq)
	p.seq++
	frame, err := json.Marshal(reqFrame{ID: id, Handler: handler, Event: json.RawMessage(event), Env: env, Trace: trace})
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

func TestNodeRuntimeTraceContextIsolatedAcrossInvocations(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
import { context } from "@opentelemetry/api";
export async function inspect(event) {
  await new Promise((resolve) => setTimeout(resolve, 0));
  console.log("trace=" + (context.active().parent ?? "root"));
  if (event.fail) throw new Error("trace failure");
}
`)
	p := startNode(t, root)

	r1 := p.invokeTrace(t, "index.inspect", `{"fail":false}`, nil, map[string]string{
		"traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
	})
	if !r1.OK || !strings.Contains(r1.Out, "trace=00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01") {
		t.Fatalf("traced invoke = %+v", r1)
	}
	r2 := p.invokeTrace(t, "index.inspect", `{"fail":true}`, nil, map[string]string{
		"traceparent": "00-cccccccccccccccccccccccccccccccc-dddddddddddddddd-01",
	})
	if r2.OK || !strings.Contains(r2.Out, "trace=00-cccccccccccccccccccccccccccccccc-dddddddddddddddd-01") {
		t.Fatalf("failed traced invoke = %+v", r2)
	}
	r3 := p.invoke(t, "index.inspect", `{}`, nil)
	if !r3.OK || !strings.Contains(r3.Out, "trace=root") {
		t.Fatalf("post-failure context = %+v, want restored root context", r3)
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

// TestNodeRuntimeEnvAppliedExactlyAcrossInvocations proves the bootstrap applies
// each request frame's env EXACTLY on the reused process: a key present in
// invocation 1 and absent in invocation 2 is removed (or restored to its
// pre-Relay baseline), unrelated OS/system/container variables are preserved,
// and RELAY_HANDLER is overwritten every invocation.
func TestNodeRuntimeEnvAppliedExactlyAcrossInvocations(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
export function show() {
  const e = process.env;
  console.log("FOO=" + (e.FOO ?? "<unset>"));
  console.log("BAR=" + (e.BAR ?? "<unset>"));
  console.log("BASELINE=" + (e.RELAY_TEST_BASELINE ?? "<unset>"));
  console.log("SYSTEM=" + (e.RELAY_TEST_SYSTEM ?? "<unset>"));
  console.log("HANDLER=" + (e.RELAY_HANDLER ?? "<unset>"));
}
`)
	p := startNode(t, root)

	// Invocation 1 carries FOO and BAR plus an override of an existing baseline
	// variable.
	r1 := p.invoke(t, "index.show", `{}`, map[string]string{
		"FOO": "one", "BAR": "bar1", "RELAY_TEST_BASELINE": "overridden",
	})
	if !r1.OK {
		t.Fatalf("invoke 1 failed: %s", r1.Err)
	}
	for _, want := range []string{"FOO=one", "BAR=bar1", "BASELINE=overridden", "SYSTEM=system-value", "HANDLER=index.show"} {
		if !strings.Contains(r1.Out, want) {
			t.Fatalf("invoke 1 out = %q, want %q", r1.Out, want)
		}
	}

	// Invocation 2 carries ONLY FOO: BAR must be gone, and the baseline override
	// must be restored to the process's original value. System env is intact.
	r2 := p.invoke(t, "index.show", `{}`, map[string]string{"FOO": "two"})
	if !r2.OK {
		t.Fatalf("invoke 2 failed: %s", r2.Err)
	}
	for _, want := range []string{"FOO=two", "BAR=<unset>", "BASELINE=process-baseline", "SYSTEM=system-value"} {
		if !strings.Contains(r2.Out, want) {
			t.Fatalf("invoke 2 out = %q, want %q (removed key must not leak forward)", r2.Out, want)
		}
	}

	// Invocation 3 carries NO env: FOO must be removed too.
	r3 := p.invoke(t, "index.show", `{}`, nil)
	if !r3.OK {
		t.Fatalf("invoke 3 failed: %s", r3.Err)
	}
	for _, want := range []string{"FOO=<unset>", "BAR=<unset>", "BASELINE=process-baseline", "SYSTEM=system-value"} {
		if !strings.Contains(r3.Out, want) {
			t.Fatalf("invoke 3 out = %q, want %q", r3.Out, want)
		}
	}
}

// TestNodeRuntimeEnvCleanupAfterHandlerFailure proves the top-of-request
// cleanup is robust even when the previous handler failed: a key applied for a
// failing invocation is still removed on the next request.
func TestNodeRuntimeEnvCleanupAfterHandlerFailure(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
export function boom() {
  throw new Error("kaboom");
}
export function show() {
  console.log("TOKEN=" + (process.env.TOKEN ?? "<unset>"));
}
`)
	p := startNode(t, root)

	r1 := p.invoke(t, "index.boom", `{}`, map[string]string{"TOKEN": "secret-v1"})
	if r1.OK {
		t.Fatal("expected the raising handler to fail")
	}
	r2 := p.invoke(t, "index.show", `{}`, nil)
	if !r2.OK {
		t.Fatalf("post-failure invoke failed: %s", r2.Err)
	}
	if !strings.Contains(r2.Out, "TOKEN=<unset>") {
		t.Fatalf("out = %q, want TOKEN=<unset> (cleanup must run even after a failed handler)", r2.Out)
	}
}

// TestNodeRuntimeNoEnvValueLeaks proves the bootstrap never writes an env value
// to a response error or operator-visible stderr: a resolved secret value is
// visible only to the handler through process.env.
func TestNodeRuntimeNoEnvValueLeaks(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
export function leak() {
  console.log("value=" + (process.env.SUPER_SECRET ?? ""));
  throw new Error("handler failed");
}
`)
	p := startNode(t, root)
	const secret = "CANARY-SECRET-VALUE-abc123"
	r := p.invoke(t, "index.leak", `{}`, map[string]string{"SUPER_SECRET": secret})
	if r.OK {
		t.Fatal("expected the raising handler to fail")
	}
	if strings.Contains(r.Err, secret) {
		t.Fatalf("response error leaked the secret value: %q", r.Err)
	}
	if !strings.Contains(r.Out, "value="+secret) {
		t.Fatalf("handler must observe its own env value; out = %q", r.Out)
	}
	// stderr carries the handler failure text only, never the env value.
	if strings.Contains(p.stderrString(), secret) {
		t.Fatalf("stderr leaked the secret value:\n%s", p.stderrString())
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

// goFrameCap is the Go bootstrap-side frame cap (internal/runtime
// maxResponseFrame = 3 KiB), hardcoded here to avoid an import cycle. A
// bootstrap response frame must always fit within it in UTF-8 bytes.
const goFrameCap = 3 << 10

// TestNodeRuntimeErrorFrameBound pins the error-frame byte cap across ASCII,
// multibyte, emoji, accented, and control/escaped text: the emitted frame is at
// most the Go cap in UTF-8 bytes, is valid UTF-8, is still parseable JSON with
// the expected id, and remains a handler error (ok:false) rather than a
// malformed response.
func TestNodeRuntimeErrorFrameBound(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
export function short() { throw new Error("short ascii"); }
export function long() { throw new Error("x".repeat(20000)); }
export function multi() { throw new Error("é".repeat(20000)); }
export function emoji() { throw new Error("😀".repeat(8000)); }
export function accented() { throw new Error("àéîõü".repeat(5000)); }
export function control() { throw new Error("\t\n\u0000\u0007".repeat(5000)); }
`)
	p := startNode(t, root)

	for _, tc := range []struct {
		name    string
		handler string
	}{
		{"short ascii", "index.short"},
		{"long ascii", "index.long"},
		{"multibyte", "index.multi"},
		{"emoji", "index.emoji"},
		{"accented", "index.accented"},
		{"control", "index.control"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := p.invoke(t, tc.handler, `{}`, nil)
			if r.OK {
				t.Fatal("expected the raising handler to fail the invocation")
			}
			if len(r.Frame) > goFrameCap {
				t.Fatalf("frame length %d exceeds the Go cap %d", len(r.Frame), goFrameCap)
			}
			if !utf8.ValidString(r.Frame) {
				t.Fatal("frame must be valid UTF-8")
			}
			payload := strings.TrimPrefix(r.Frame, relaySentinel)
			if !json.Valid([]byte(payload)) {
				t.Fatalf("frame payload must be valid JSON: %q", payload)
			}
			var resp respFrame
			if err := json.Unmarshal([]byte(payload), &resp); err != nil {
				t.Fatalf("frame payload must unmarshal: %v", err)
			}
			if resp.ID == "" || resp.OK {
				t.Fatalf("resp = %+v, want a non-empty id and ok:false for the handler error", resp)
			}
			if resp.Error == "" {
				t.Fatal("expected a non-empty bounded error string")
			}
			// The process must stay healthy after an oversized error.
			ok := p.invoke(t, "index.short", `{}`, nil)
			if ok.OK {
				t.Fatal("expected the short handler to also fail")
			}
		})
	}
}

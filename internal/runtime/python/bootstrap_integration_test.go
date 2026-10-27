//go:build integration

package python

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

// relaySentinel is the protocol response prefix. It is hardcoded here (rather
// than imported) to avoid an import cycle with the Go runtime package.
const relaySentinel = "@@RELAY@@"

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

// writeOtelStub writes a minimal stand-in for the OpenTelemetry API the runtime
// image installs, into dir (the test PYTHONPATH). The real embedded bootstrap
// imports `opentelemetry.context` and `opentelemetry.propagate`; this stub
// provides just those two with the same attach/detach/extract contract backed by
// contextvars, so the process-behavior tests can observe the managed invocation
// context without a network OTel install. Mirrors the Node bootstrap test's stub.
func writeOtelStub(t *testing.T, dir string) {
	t.Helper()
	pkg := filepath.Join(dir, "opentelemetry")
	if err := os.MkdirAll(pkg, 0o755); err != nil {
		t.Fatalf("mkdir opentelemetry stub: %v", err)
	}
	write := func(name, contents string) {
		if err := os.WriteFile(filepath.Join(pkg, name), []byte(contents), 0o644); err != nil {
			t.Fatalf("write opentelemetry stub: %v", err)
		}
	}
	write("__init__.py", "")
	write("context.py", `
import contextvars

_ROOT = "root"
_current = contextvars.ContextVar("relay_otel_context", default=_ROOT)


def attach(ctx):
    return _current.set(ctx)


def detach(token):
    _current.reset(token)


def get_current():
    return _current.get()
`)
	write("propagate.py", `
def extract(carrier, context="root", getter=None):
    if not carrier:
        return context
    return carrier.get("traceparent") or context
`)
}

// startPython runs the real embedded bootstrap under python3 with PYTHONPATH
// pointing at dir (which holds the handler modules and the OTel API stub).
func startPython(t *testing.T, dir string) *pyProc {
	t.Helper()
	writeOtelStub(t, dir)
	return startBootstrap(t, dir)
}

// startPythonWithoutOtel runs the bootstrap in a dir whose `opentelemetry`
// package deliberately raises on import, so the bootstrap's guarded import is
// exercised deterministically regardless of any ambient OTel install on the
// test host.
func startPythonWithoutOtel(t *testing.T, dir string) *pyProc {
	t.Helper()
	pkg := filepath.Join(dir, "opentelemetry")
	if err := os.MkdirAll(pkg, 0o755); err != nil {
		t.Fatalf("mkdir opentelemetry: %v", err)
	}
	if err := os.WriteFile(filepath.Join(pkg, "__init__.py"), []byte(`raise ImportError("otel unavailable")`), 0o644); err != nil {
		t.Fatalf("write broken opentelemetry: %v", err)
	}
	return startBootstrap(t, dir)
}

// startBootstrap runs the embedded bootstrap in dir as a live process. Two
// controlled variables are seeded into the child environment so the
// exact-per-invocation env tests can prove baseline restoration and system-var
// preservation:
//
//	RELAY_TEST_BASELINE=process-baseline  (a key Relay may override then restore)
//	RELAY_TEST_SYSTEM=system-value        (a key Relay never touches)
func startBootstrap(t *testing.T, dir string) *pyProc {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "bootstrap.py"), Bootstrap, 0o644); err != nil {
		t.Fatalf("write bootstrap: %v", err)
	}
	cmd := exec.Command("python3", filepath.Join(dir, "bootstrap.py"))
	cmd.Env = append(os.Environ(),
		"PYTHONPATH="+dir,
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
	return p.invokeTrace(t, handler, event, env, nil)
}

// invokeTrace is invoke with the optional W3C "trace" carrier the frame carries.
func (p *pyProc) invokeTrace(t *testing.T, handler string, event string, env, trace map[string]string) invokeResult {
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

func requirePython(t *testing.T) {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatalf("Python 3.9 or newer is required for these integration tests: %v", err)
	}
	out, err := exec.Command(python, "-c", "import sys; print('%d.%d' % sys.version_info[:2])").Output()
	if err != nil {
		t.Fatalf("check python3 version: %v", err)
	}
	version := strings.TrimSpace(string(out))
	majorText, minorText, ok := strings.Cut(version, ".")
	major, majorErr := strconv.Atoi(majorText)
	minor, minorErr := strconv.Atoi(minorText)
	if !ok || majorErr != nil || minorErr != nil || major < 3 || (major == 3 && minor < 9) {
		t.Fatalf("Python 3.9 or newer is required for these integration tests; found %q", version)
	}
}

func TestPythonRuntimeTwoInvocationsAndModuleState(t *testing.T) {
	requirePython(t)
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

// TestPythonRuntimeAsyncHandlerKeepsOneProcessLifetimeLoop proves the warm
// container's async semantics: one event loop lives for the whole process, a
// sync handler and an async handler run on that SAME loop, and a module-level
// asyncio resource (a Queue) created on first use stays usable across
// invocations instead of being orphaned by a per-invocation loop. It also
// proves an async failure fails only that invocation (a later await on the same
// loop succeeds) and that the sync/async transitions and the import/lookup
// distinctions are unchanged.
func TestPythonRuntimeAsyncHandlerKeepsOneProcessLifetimeLoop(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
import asyncio

_loop = None
_queue = None

def _remember():
    # The loop this handler runs on must be the SAME live loop every
    # invocation; the module-level Queue binds to it on first use.
    global _loop, _queue
    current = asyncio.get_running_loop()
    if _loop is None:
        _loop = current
        _queue = asyncio.Queue()
    assert current is _loop, "event loop changed between invocations"
    assert not _loop.is_closed(), "event loop was closed between invocations"
    return current

def sync_count(event):
    _remember()
    print("sync=on-persistent-loop")

async def async_touch(event):
    _remember()
    await _queue.put(event["msg"])
    print("async=%s" % (await _queue.get()))

async def async_boom(event):
    _remember()
    await asyncio.sleep(0)
    raise RuntimeError("async-kaboom")
`)
	p := startPython(t, dir)

	// sync -> async -> async -> async failure -> async recovery -> sync: every
	// step must see the same process-lifetime loop, and the module Queue must
	// survive between invocations.
	r := p.invoke(t, "handler.sync_count", `{}`, nil)
	if !r.OK || !strings.Contains(r.Out, "sync=on-persistent-loop") {
		t.Fatalf("sync invoke = %+v, want success on the persistent loop", r)
	}
	for _, msg := range []string{"a", "b"} {
		r := p.invoke(t, "handler.async_touch", fmt.Sprintf(`{"msg":%q}`, msg), nil)
		if !r.OK || !strings.Contains(r.Out, "async="+msg) {
			t.Fatalf("async invoke %q = %+v, want success on the same reused loop", msg, r)
		}
	}
	fail := p.invoke(t, "handler.async_boom", `{}`, nil)
	if fail.OK || !strings.Contains(fail.Err, "async-kaboom") {
		t.Fatalf("async failure = %+v, want ok:false with the handler error", fail)
	}
	recover := p.invoke(t, "handler.async_touch", `{"msg":"c"}`, nil)
	if !recover.OK || !strings.Contains(recover.Out, "async=c") {
		t.Fatalf("post-failure async invoke = %+v, want recovery on the same live loop", recover)
	}
	again := p.invoke(t, "handler.sync_count", `{}`, nil)
	if !again.OK || !strings.Contains(again.Out, "sync=on-persistent-loop") {
		t.Fatalf("post-async sync invoke = %+v, want success", again)
	}

	// Import/lookup distinctions are unchanged: a missing function and an
	// unimportable module both fail only the invocation.
	missing := p.invoke(t, "handler.nope", `{}`, nil)
	if missing.OK || !strings.Contains(missing.Err, "has no function") {
		t.Fatalf("missing function = %+v, want a lookup failure", missing)
	}
	badimport := p.invoke(t, "no_such_module.nope", `{}`, nil)
	if badimport.OK || !strings.Contains(badimport.Err, "failed to import module") {
		t.Fatalf("bad import = %+v, want an import failure", badimport)
	}
}

// TestPythonRuntimeAwaitableReturningHandler proves a sync handler that returns
// an awaitable is awaited on the persistent loop: both a plain sync def
// returning a coroutine and the common decorator shape (a sync wrapper around
// an async body) complete, and the awaited body's output is still emitted before
// the protocol response.
func TestPythonRuntimeAwaitableReturningHandler(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
import asyncio

def returns_coroutine(event):
    async def inner():
        await asyncio.sleep(0)
        print("awaited %s" % event["msg"])
    return inner()

class Awaitable:
    def __init__(self, msg):
        self._msg = msg
    def __await__(self):
        async def run():
            await asyncio.sleep(0)
            print("object %s" % self._msg)
        return run().__await__()

def returns_awaitable_object(event):
    return Awaitable(event["msg"])

def decorate(fn):
    def wrapper(event):
        async def run():
            await asyncio.sleep(0)
            fn(event)
        return run()
    return wrapper

@decorate
def decorated(event):
    print("decorated %s" % event["msg"])
`)
	p := startPython(t, dir)

	for _, tc := range []struct {
		handler string
		want    string
	}{
		{"handler.returns_coroutine", "awaited hi"},
		{"handler.returns_awaitable_object", "object hi"},
		{"handler.decorated", "decorated hi"},
	} {
		r := p.invoke(t, tc.handler, `{"msg":"hi"}`, nil)
		if !r.OK {
			t.Fatalf("%s failed: %s", tc.handler, r.Err)
		}
		if !strings.Contains(r.Out, tc.want) {
			t.Fatalf("%s out = %q, want %q (the returned awaitable must be awaited)", tc.handler, r.Out, tc.want)
		}
	}
}

func TestPythonRuntimeAsyncHandler(t *testing.T) {
	requirePython(t)
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

// TestPythonRuntimeTraceContextIsolatedAcrossInvocations proves the managed
// invocation context: the frame's "trace" carrier is attached for the handler
// call only, an async handler observes it across an await, a raising handler
// still restores the previous context, and an untraced invocation sees no
// leaked context. The stub OTel API mirrors the real attach/detach contract.
func TestPythonRuntimeTraceContextIsolatedAcrossInvocations(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
import asyncio
from opentelemetry import context

async def inspect(event):
    await asyncio.sleep(0)
    print("trace=" + str(context.get_current()))
    if event.get("fail"):
        raise ValueError("trace failure")
`)
	p := startPython(t, dir)

	r1 := p.invokeTrace(t, "handler.inspect", `{"fail":false}`, nil, map[string]string{
		"traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
	})
	if !r1.OK || !strings.Contains(r1.Out, "trace=00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01") {
		t.Fatalf("traced invoke = %+v, want the attached traceparent", r1)
	}

	r2 := p.invokeTrace(t, "handler.inspect", `{"fail":true}`, nil, map[string]string{
		"traceparent": "00-cccccccccccccccccccccccccccccccc-dddddddddddddddd-01",
	})
	if r2.OK || !strings.Contains(r2.Out, "trace=00-cccccccccccccccccccccccccccccccc-dddddddddddddddd-01") {
		t.Fatalf("failed traced invoke = %+v, want the attached traceparent before the raise", r2)
	}

	r3 := p.invoke(t, "handler.inspect", `{}`, nil)
	if !r3.OK || !strings.Contains(r3.Out, "trace=root") {
		t.Fatalf("post-failure untraced invoke = %+v, want the restored root context", r3)
	}
}

// TestPythonRuntimeTraceWithoutOtelIsBestEffort proves the guarded import: when
// the OpenTelemetry API is unavailable the bootstrap still runs the handler with
// the trace field present, degrading to no propagation instead of crashing.
func TestPythonRuntimeTraceWithoutOtelIsBestEffort(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", "def f(event):\n    print('ran %s' % event['msg'])\n")
	p := startPythonWithoutOtel(t, dir)

	r := p.invokeTrace(t, "handler.f", `{"msg":"hi"}`, nil, map[string]string{
		"traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
	})
	if !r.OK || !strings.Contains(r.Out, "ran hi") {
		t.Fatalf("invoke without the OTel API = %+v, want it to run best-effort", r)
	}
}

func TestPythonRuntimeHandlerErrorKeepsProcessAlive(t *testing.T) {
	requirePython(t)
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
	requirePython(t)
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

// TestPythonRuntimeEnvAppliedExactlyAcrossInvocations proves the bootstrap
// applies each request frame's env EXACTLY on the reused process: a key present
// in invocation 1 and absent in invocation 2 is removed (or restored to its
// pre-Relay baseline), unrelated OS/system/container variables are preserved,
// and RELAY_HANDLER is overwritten every invocation.
func TestPythonRuntimeEnvAppliedExactlyAcrossInvocations(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
import os

def show(event):
    print("FOO=" + os.environ.get("FOO", "<unset>"))
    print("BAR=" + os.environ.get("BAR", "<unset>"))
    print("BASELINE=" + os.environ.get("RELAY_TEST_BASELINE", "<unset>"))
    print("SYSTEM=" + os.environ.get("RELAY_TEST_SYSTEM", "<unset>"))
    print("HANDLER=" + os.environ.get("RELAY_HANDLER", "<unset>"))
`)
	p := startPython(t, dir)

	// Invocation 1 carries FOO and BAR plus an override of an existing baseline
	// variable.
	r1 := p.invoke(t, "handler.show", `{}`, map[string]string{
		"FOO": "one", "BAR": "bar1", "RELAY_TEST_BASELINE": "overridden",
	})
	if !r1.OK {
		t.Fatalf("invoke 1 failed: %s", r1.Err)
	}
	for _, want := range []string{"FOO=one", "BAR=bar1", "BASELINE=overridden", "SYSTEM=system-value", "HANDLER=handler.show"} {
		if !strings.Contains(r1.Out, want) {
			t.Fatalf("invoke 1 out = %q, want %q", r1.Out, want)
		}
	}

	// Invocation 2 carries ONLY FOO: BAR must be gone, and the baseline override
	// must be restored to the process's original value. System env is intact.
	r2 := p.invoke(t, "handler.show", `{}`, map[string]string{"FOO": "two"})
	if !r2.OK {
		t.Fatalf("invoke 2 failed: %s", r2.Err)
	}
	for _, want := range []string{"FOO=two", "BAR=<unset>", "BASELINE=process-baseline", "SYSTEM=system-value"} {
		if !strings.Contains(r2.Out, want) {
			t.Fatalf("invoke 2 out = %q, want %q (removed key must not leak forward)", r2.Out, want)
		}
	}

	// Invocation 3 carries NO env: FOO must be removed too.
	r3 := p.invoke(t, "handler.show", `{}`, nil)
	if !r3.OK {
		t.Fatalf("invoke 3 failed: %s", r3.Err)
	}
	for _, want := range []string{"FOO=<unset>", "BAR=<unset>", "BASELINE=process-baseline", "SYSTEM=system-value"} {
		if !strings.Contains(r3.Out, want) {
			t.Fatalf("invoke 3 out = %q, want %q", r3.Out, want)
		}
	}
}

// TestPythonRuntimeEnvCleanupAfterHandlerFailure proves the top-of-request
// cleanup is robust even when the previous handler failed: a key applied for a
// failing invocation is still removed on the next request.
func TestPythonRuntimeEnvCleanupAfterHandlerFailure(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
import os

def boom(event):
    raise ValueError("kaboom")

def show(event):
    print("TOKEN=" + os.environ.get("TOKEN", "<unset>"))
`)
	p := startPython(t, dir)

	r1 := p.invoke(t, "handler.boom", `{}`, map[string]string{"TOKEN": "secret-v1"})
	if r1.OK {
		t.Fatal("expected the raising handler to fail")
	}
	r2 := p.invoke(t, "handler.show", `{}`, nil)
	if !r2.OK {
		t.Fatalf("post-failure invoke failed: %s", r2.Err)
	}
	if !strings.Contains(r2.Out, "TOKEN=<unset>") {
		t.Fatalf("out = %q, want TOKEN=<unset> (cleanup must run even after a failed handler)", r2.Out)
	}
}

// TestPythonRuntimeNoEnvValueLeaks proves the bootstrap never writes an env
// value to a response error or operator-visible stderr: a resolved secret value
// is visible only to the handler through os.environ.
func TestPythonRuntimeNoEnvValueLeaks(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
import os

def leak(event):
    print("value=" + os.environ.get("SUPER_SECRET", ""))
    raise ValueError("handler failed")
`)
	p := startPython(t, dir)
	const secret = "CANARY-SECRET-VALUE-abc123"
	r := p.invoke(t, "handler.leak", `{}`, map[string]string{"SUPER_SECRET": secret})
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

func TestPythonRuntimeEOFExitsZero(t *testing.T) {
	requirePython(t)
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
	requirePython(t)
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
	requirePython(t)
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

// goFrameCap is the Go bootstrap-side frame cap (internal/runtime
// maxResponseFrame = 3 KiB), hardcoded here to avoid an import cycle. A
// bootstrap response frame must always fit within it in UTF-8 bytes.
const goFrameCap = 3 << 10

// TestPythonRuntimeErrorFrameBound pins the error-frame byte cap across ASCII,
// multibyte, emoji, accented, and control/escaped text: the emitted frame is at
// most the Go cap in UTF-8 bytes, is valid UTF-8, is still parseable JSON with
// the expected id, and remains a handler error (ok:false) rather than a
// malformed response.
func TestPythonRuntimeErrorFrameBound(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
def short(event):
    raise ValueError("short ascii")

def long(event):
    raise ValueError("x" * 20000)

def multi(event):
    raise ValueError("é" * 20000)

def emoji(event):
    raise ValueError("😀" * 8000)

def accented(event):
    raise ValueError("àéîõü" * 5000)

def control(event):
    raise ValueError("\t\n\x00\x07" * 5000)
`)
	p := startPython(t, dir)

	for _, tc := range []struct {
		name    string
		handler string
	}{
		{"short ascii", "handler.short"},
		{"long ascii", "handler.long"},
		{"multibyte", "handler.multi"},
		{"emoji", "handler.emoji"},
		{"accented", "handler.accented"},
		{"control", "handler.control"},
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
			ok := p.invoke(t, "handler.short", `{}`, nil)
			if ok.OK {
				t.Fatal("expected the short handler to also fail")
			}
		})
	}
}

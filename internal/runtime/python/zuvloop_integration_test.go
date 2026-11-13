//go:build integration

package python

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The zuvloop bootstrap-variant tests run the REAL embedded bootstrap as a live
// process (see bootstrap_integration_test.go for the harness conventions). The
// pinned zuvloop package itself is a Linux-wheel-only, Docker-image concern
// (covered by the runtime engine E2E suite against python:3.14); here the loop
// factory is a TEST DOUBLE installed under the module name `zuvloop` on the
// child's PYTHONPATH. The double records what the bootstrap actually asks it
// for, so the tests observe the bootstrap's own behavior: that it delegates
// loop creation to zuvloop.new_event_loop exactly once, that handlers run on
// the returned loop, and that an unavailable or failing factory is FATAL at
// startup with no fallback to the standard asyncio loop.

// zuvloopStubMarker is the attribute only the stub factory sets on the loop it
// returns; a handler observing it on asyncio.get_running_loop() proves the
// running loop came from the factory, not from asyncio's default.
const zuvloopStubMarker = "relay-zuvloop-stub"

// zuvloopStubSource is the test double module written as dir/zuvloop/__init__.py.
// new_event_loop returns a real asyncio loop tagged with the marker, and
// created_loop_count lets a handler pin how many loops the factory created.
const zuvloopStubSource = `
import asyncio

_created = []


def new_event_loop():
    loop = asyncio.new_event_loop()
    loop.relay_test_factory_marker = "` + zuvloopStubMarker + `"
    _created.append(loop)
    return loop


def created_loop_count():
    return len(_created)
`

// zuvloopStubFailingSource is the double whose factory fails initialization: an
// enabled container must die at startup instead of silently running the
// standard asyncio loop.
const zuvloopStubFailingSource = `
def new_event_loop():
    raise RuntimeError("relay-factory-kaboom")
`

// writeZuvloopStub installs the given double source under dir/zuvloop/ so the
// child's import of `zuvloop` resolves to the test double.
func writeZuvloopStub(t *testing.T, dir, source string) {
	t.Helper()
	pkg := filepath.Join(dir, "zuvloop")
	if err := os.MkdirAll(pkg, 0o755); err != nil {
		t.Fatalf("mkdir zuvloop stub: %v", err)
	}
	if err := os.WriteFile(filepath.Join(pkg, "__init__.py"), []byte(source), 0o644); err != nil {
		t.Fatalf("write zuvloop stub: %v", err)
	}
}

// startBootstrapContent runs the given bootstrap content in dir as a live
// process with the same child environment seeding as startBootstrap.
func startBootstrapContent(t *testing.T, dir string, content []byte) *pyProc {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "bootstrap.py"), content, 0o644); err != nil {
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
	go func() { _, _ = io.Copy(p.stderrWriter(), stderr) }()
	if err := cmd.Start(); err != nil {
		t.Fatalf("start bootstrap: %v", err)
	}
	t.Cleanup(func() {
		_ = stdin.Close()
		_ = cmd.Wait()
	})
	return p
}

// startZuvloopBootstrap writes the engine's ENABLED bootstrap variant (what the
// Python engine bakes into an app image when PYTHON_ZUVLOOP applies) into dir
// and runs it with the stub double available under PYTHONPATH.
func startZuvloopBootstrap(t *testing.T, dir string) *pyProc {
	t.Helper()
	content, err := bootstrapFor(true)
	if err != nil {
		t.Fatalf("enabled bootstrap variant: %v", err)
	}
	return startBootstrapContent(t, dir, content)
}

// requireLoopFactoryPython fails the test when the host python3 predates
// asyncio.run's loop_factory parameter (the enabled bootstrap's own contract;
// the managed runtime image is python:3.14).
func requireLoopFactoryPython(t *testing.T) {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatalf("python3 is required for these tests: %v", err)
	}
	out, err := exec.Command(python, "-c", "import sys; print('%d.%d' % sys.version_info[:2])").Output()
	if err != nil {
		t.Fatalf("check python3 version: %v", err)
	}
	version := strings.TrimSpace(string(out))
	majorText, minorText, ok := strings.Cut(version, ".")
	major, majorErr := strconv.Atoi(majorText)
	minor, minorErr := strconv.Atoi(minorText)
	if !ok || majorErr != nil || minorErr != nil || major < 3 || (major == 3 && minor < 12) {
		t.Fatalf("the enabled bootstrap needs Python 3.12+ (asyncio.run loop_factory); found %q", version)
	}
}

// TestPythonZuvloopEnabledSelectsFactoryLoop proves the enabled image's core
// behavior: the process's single loop is the object zuvloop.new_event_loop
// returned, a sync handler and an async handler both run on it, the factory
// creates EXACTLY ONE loop for the whole process lifetime, and a module-level
// asyncio resource (the Queue) stays bound to that same loop across warm
// invocations.
func TestPythonZuvloopEnabledSelectsFactoryLoop(t *testing.T) {
	requireLoopFactoryPython(t)
	dir := t.TempDir()
	writeOtelStub(t, dir)
	writeZuvloopStub(t, dir, zuvloopStubSource)
	writeHandler(t, dir, "handler", `
import asyncio
import zuvloop

_loop = None
_queue = None


def _observe(tag):
    global _loop, _queue
    current = asyncio.get_running_loop()
    marker = getattr(current, "relay_test_factory_marker", None)
    assert marker == "`+zuvloopStubMarker+`", (
        "running loop was not created by the zuvloop factory: %r" % (marker,))
    if _loop is None:
        _loop = current
        _queue = asyncio.Queue()
    assert current is _loop, "event loop changed between invocations"
    assert not current.is_closed(), "event loop closed between invocations"
    assert zuvloop.created_loop_count() == 1, (
        "factory created %d loops, want exactly one per process"
        % zuvloop.created_loop_count())
    print(tag)


def sync_probe(event, context):
    _observe("sync-marker=`+zuvloopStubMarker+`")


async def async_probe(event, context):
    _observe("async-marker=on-same-stub-loop")
    await _queue.put(event["msg"])
    print("queue=%s" % (await _queue.get()))
`)
	p := startZuvloopBootstrap(t, dir)

	sync := p.invoke(t, "handler.sync_probe", `{}`, nil)
	if !sync.OK || !strings.Contains(sync.Out, "sync-marker="+zuvloopStubMarker) {
		t.Fatalf("sync invoke = %+v, want success running on the factory loop", sync)
	}

	// Two async invocations exercise the module-level Queue on the same loop,
	// interleaved with a second sync invocation: warm-loop reuse must hold.
	first := p.invoke(t, "handler.async_probe", `{"msg":"a"}`, nil)
	if !first.OK || !strings.Contains(first.Out, "async-marker=on-same-stub-loop") || !strings.Contains(first.Out, "queue=a") {
		t.Fatalf("first async invoke = %+v, want success on the factory loop", first)
	}
	second := p.invoke(t, "handler.async_probe", `{"msg":"b"}`, nil)
	if !second.OK || !strings.Contains(second.Out, "queue=b") {
		t.Fatalf("second async invoke = %+v, want the same loop and a reused Queue", second)
	}
	again := p.invoke(t, "handler.sync_probe", `{}`, nil)
	if !again.OK || !strings.Contains(again.Out, "sync-marker="+zuvloopStubMarker) {
		t.Fatalf("post-async sync invoke = %+v, want the process still on the one factory loop", again)
	}
}

// TestPythonZuvloopEnabledMissingPackageFailsStartup pins the no-fallback
// startup contract for the import path: an enabled image WITHOUT the installed
// package dies at process startup (nonzero exit) with the missing-module error,
// never silently running the standard asyncio loop.
func TestPythonZuvloopEnabledMissingPackageFailsStartup(t *testing.T) {
	requireLoopFactoryPython(t)
	dir := t.TempDir()
	writeOtelStub(t, dir)
	writeHandler(t, dir, "handler", "def f(event, context):\n    pass\n")

	content, err := bootstrapFor(true)
	if err != nil {
		t.Fatalf("enabled bootstrap variant: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bootstrap.py"), content, 0o644); err != nil {
		t.Fatalf("write bootstrap: %v", err)
	}
	cmd := exec.Command("python3", filepath.Join(dir, "bootstrap.py"))
	cmd.Env = append(os.Environ(), "PYTHONPATH="+dir)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("an enabled image without the zuvloop package must fail startup, output:\n%s", out)
	}
	if !strings.Contains(string(out), "No module named 'zuvloop'") {
		t.Errorf("startup failure output must name the missing package, got:\n%s", out)
	}
}

// TestPythonZuvloopEnabledFactoryFailureFailsStartup pins the no-fallback
// startup contract for loop initialization: an enabled image whose loop factory
// fails must die at startup (nonzero exit) with the factory's error, and never
// start a container on the standard asyncio loop.
func TestPythonZuvloopEnabledFactoryFailureFailsStartup(t *testing.T) {
	requireLoopFactoryPython(t)
	dir := t.TempDir()
	writeOtelStub(t, dir)
	writeZuvloopStub(t, dir, zuvloopStubFailingSource)
	writeHandler(t, dir, "handler", "def f(event, context):\n    pass\n")

	content, err := bootstrapFor(true)
	if err != nil {
		t.Fatalf("enabled bootstrap variant: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bootstrap.py"), content, 0o644); err != nil {
		t.Fatalf("write bootstrap: %v", err)
	}
	cmd := exec.Command("python3", filepath.Join(dir, "bootstrap.py"))
	cmd.Env = append(os.Environ(), "PYTHONPATH="+dir)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("an enabled image whose loop factory fails must fail startup, output:\n%s", out)
	}
	if !strings.Contains(string(out), "relay-factory-kaboom") {
		t.Errorf("startup failure output must carry the factory error, got:\n%s", out)
	}
}

// TestPythonZuvloopDisabledNeverTouchesZuvloop pins the disabled image's
// isolation: even with a zuvloop package importable, the standard bootstrap
// never imports it and the running loop carries no factory marker — the
// historical asyncio behavior byte-for-byte.
func TestPythonZuvloopDisabledNeverTouchesZuvloop(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeOtelStub(t, dir)
	writeZuvloopStub(t, dir, zuvloopStubSource)
	writeHandler(t, dir, "handler", `
import asyncio
import sys


def probe(event, context):
    print("zuvloop-imported=%s" % ("zuvloop" in sys.modules))
    loop = asyncio.get_running_loop()
    print("factory-probed=%s" % (hasattr(loop, "relay_test_factory_marker")))


async def async_probe(event, context):
    await asyncio.sleep(0)
    print("ran-async ok")
`)
	// The standard-mode process: the embedded bootstrap as-is.
	p := startBootstrapContent(t, dir, Bootstrap)

	probe := p.invoke(t, "handler.probe", `{}`, nil)
	if !probe.OK {
		t.Fatalf("probe invoke = %+v, want success", probe)
	}
	if !strings.Contains(probe.Out, "zuvloop-imported=False") {
		t.Errorf("disabled image must not import zuvloop, out = %q", probe.Out)
	}
	if !strings.Contains(probe.Out, "factory-probed=False") {
		t.Errorf("disabled image's running loop must not come from the zuvloop factory, out = %q", probe.Out)
	}
	async := p.invoke(t, "handler.async_probe", `{}`, nil)
	if !async.OK || !strings.Contains(async.Out, "ran-async ok") {
		t.Fatalf("async invoke = %+v, want the standard asyncio behavior", async)
	}
}

// TestPythonZuvloopEnabledEOFExitsZero pins the protocol lifecycle of the
// enabled image: after Relay is done (stdin EOF) the container exits 0 cleanly,
// exactly like the standard image.
func TestPythonZuvloopEnabledEOFExitsZero(t *testing.T) {
	requireLoopFactoryPython(t)
	dir := t.TempDir()
	writeOtelStub(t, dir)
	writeZuvloopStub(t, dir, zuvloopStubSource)
	writeHandler(t, dir, "handler", "def f(event, context):\n    pass\n")
	p := startZuvloopBootstrap(t, dir)
	r := p.invoke(t, "handler.f", `{}`, nil)
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	if err := p.stdin.Close(); err != nil {
		t.Fatalf("close stdin: %v", err)
	}
	err := p.cmd.Wait()
	if err != nil {
		t.Fatalf("enabled bootstrap did not exit 0 on stdin EOF: %v; stderr:\n%s", err, p.stderrString())
	}
}

//go:build integration

package python

// End-to-end coverage for the Python SDK's dependency injection through the
// real embedded bootstrap: handler modules import relay_runtime from the
// packages/python source tree, declare dependencies with Depends, and are
// driven over the authenticated execution channel exactly as Relay drives a
// warm container. The tests pin per-invocation resource lifecycles, nested
// providers observing the event and context, provider failures keeping the
// container healthy, container-wide caching across warm invocations, and
// container-scoped cleanup during the graceful shutdown that follows Relay
// closing the channel.

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// relayRuntimeSrc resolves the packages/python source tree so the child
// process imports relay_runtime from its live checkout, mirroring an app that
// declared relay-runtime as a dependency.
func relayRuntimeSrc(t *testing.T) string {
	t.Helper()
	src, err := filepath.Abs(
		filepath.Join("..", "..", "..", "packages", "python", "src"))
	if err != nil {
		t.Fatalf("resolve relay_runtime source path: %v", err)
	}
	if _, err := os.Stat(filepath.Join(src, "relay_runtime", "__init__.py")); err != nil {
		t.Fatalf("relay_runtime sources not found at %s: %v", src, err)
	}
	return src
}

// requireRelayRuntimeImport proves python3 can import the public package from
// the source tree before any bootstrap spawns, so a version or syntax problem
// fails with a direct diagnosis instead of a confusing invocation error.
func requireRelayRuntimeImport(t *testing.T, src string) string {
	t.Helper()
	cmd := exec.Command("python3", "-c", "from relay_runtime import Depends, bind")
	cmd.Env = append(os.Environ(), "PYTHONPATH="+src)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("python3 cannot import relay_runtime from %s: %v\n%s", src, err, out)
	}
	return "PYTHONPATH=" + src
}

// dependencyInjectionEnv puts dir ahead of the SDK source on the child's
// PYTHONPATH; the entry is appended last so it replaces the harness's
// dir-only PYTHONPATH (exec keeps the last duplicate key).
func dependencyInjectionEnv(t *testing.T, dir, src string) []string {
	t.Helper()
	_ = requireRelayRuntimeImport(t, src)
	return []string{"PYTHONPATH=" + dir + string(os.PathListSeparator) + src}
}

// TestPythonRuntimeDependencyInjectionInvocationScope drives dependency
// injection end to end through the real embedded bootstrap: a @bind handler
// receives nested providers (one through Annotated, one reading the invocation
// context), an invocation-scoped generator resource opens and closes per
// invocation, and a failing provider fails only that invocation on a
// still-healthy container.
func TestPythonRuntimeDependencyInjectionInvocationScope(t *testing.T) {
	requirePython(t)
	src := relayRuntimeSrc(t)
	dir := t.TempDir()
	writeOtelStub(t, dir)
	writeHandler(t, dir, "handler", `
from typing import Annotated

from relay_runtime import Depends, bind


def provide_identity(context):
    return context.request_id


def provide_label(ident=Depends(provide_identity)):
    return "label:" + ident


def provide_conn():
    print("conn=setup")
    yield "conn"
    print("conn=teardown")


def bad_provider():
    raise ValueError("dep-kaboom")


@bind
def serve(event, context, label: Annotated[str, Depends(provide_label)],
          conn=Depends(provide_conn)):
    print("serve %s %s %s" % (conn, label, event["msg"]))


@bind
def explode(event, context, value=Depends(bad_provider)):
    print("unreachable")
`)
	p := startEmbeddedBootstrap(t, dir, Bootstrap,
		dependencyInjectionEnv(t, dir, src))

	r1 := p.invoke(t, "handler.serve", `{"msg":"one"}`, nil)
	if !r1.OK {
		t.Fatalf("first DI invoke failed: %s", r1.Err)
	}
	for _, want := range []string{
		"conn=setup",
		"serve conn label:test-request one",
		"conn=teardown",
	} {
		if !strings.Contains(r1.Out, want) {
			t.Errorf("first invoke out = %q, want %q", r1.Out, want)
		}
	}

	// The second warm invocation opens a FRESH invocation-scoped resource
	// while the process, module state, and interpreter stay alive.
	r2 := p.invoke(t, "handler.serve", `{"msg":"two"}`, nil)
	if !r2.OK {
		t.Fatalf("second DI invoke failed: %s", r2.Err)
	}
	for _, want := range []string{
		"conn=setup",
		"serve conn label:test-request two",
		"conn=teardown",
	} {
		if !strings.Contains(r2.Out, want) {
			t.Errorf("second invoke out = %q, want %q", r2.Out, want)
		}
	}

	// A provider failure fails that invocation with the provider's error and
	// never reaches the handler body.
	bad := p.invoke(t, "handler.explode", `{"msg":"three"}`, nil)
	if bad.OK {
		t.Fatal("a raising provider must fail its invocation")
	}
	if !strings.Contains(bad.Err, "dep-kaboom") {
		t.Errorf("provider failure error = %q, want the provider's message", bad.Err)
	}
	if strings.Contains(bad.Out, "unreachable") {
		t.Errorf("handler ran despite provider failure: %q", bad.Out)
	}

	// The container stays healthy: the next invocation succeeds.
	r4 := p.invoke(t, "handler.serve", `{"msg":"four"}`, nil)
	if !r4.OK {
		t.Fatalf("invoke after provider failure failed: %s", r4.Err)
	}
	if !strings.Contains(r4.Out, "serve conn label:test-request four") {
		t.Errorf("post-failure invoke out = %q, want the served line", r4.Out)
	}
}

// TestPythonRuntimeContainerScopeCleansUpOnGracefulShutdown pins the
// container scope across the warm process: the resource opens once for the
// whole runtime, warm invocations share it without reopening, and its cleanup
// runs during the graceful shutdown that follows Relay closing the
// authenticated channel (the process then exits 0).
func TestPythonRuntimeContainerScopeCleansUpOnGracefulShutdown(t *testing.T) {
	requirePython(t)
	src := relayRuntimeSrc(t)
	dir := t.TempDir()
	writeOtelStub(t, dir)
	writeHandler(t, dir, "handler", `
from relay_runtime import Depends, bind


def provide_conn():
    print("conn=setup")
    yield "conn"
    print("conn=teardown")


@bind
def serve(event, context, conn=Depends(provide_conn, scope="container")):
    print("serve %s %s" % (conn, event["msg"]))
`)
	p := startEmbeddedBootstrap(t, dir, Bootstrap,
		dependencyInjectionEnv(t, dir, src))

	r1 := p.invoke(t, "handler.serve", `{"msg":"one"}`, nil)
	if !r1.OK {
		t.Fatalf("first invoke failed: %s", r1.Err)
	}
	if !strings.Contains(r1.Out, "conn=setup") || !strings.Contains(r1.Out, "serve conn one") {
		t.Fatalf("first invoke out = %q, want the setup and served lines", r1.Out)
	}

	// The second warm invocation reuses the container-scoped value: no
	// second setup, no teardown between invocations.
	r2 := p.invoke(t, "handler.serve", `{"msg":"two"}`, nil)
	if !r2.OK {
		t.Fatalf("second invoke failed: %s", r2.Err)
	}
	if !strings.Contains(r2.Out, "serve conn two") {
		t.Errorf("second invoke out = %q, want the served line", r2.Out)
	}
	if strings.Contains(r2.Out, "conn=setup") {
		t.Errorf("container-scoped resource reopened on a warm invocation: %q", r2.Out)
	}
	if strings.Contains(r2.Out, "conn=teardown") {
		t.Errorf("container-scoped resource closed between invocations: %q", r2.Out)
	}

	// Relay is done: closing the channel must trigger the graceful runtime
	// shutdown that closes container-scoped resources, then exit 0.
	if err := p.closeChannel(); err != nil {
		t.Fatalf("close channel: %v", err)
	}
	if err := p.waitExit(t, 10*time.Second); err != nil {
		t.Fatalf("bootstrap did not exit 0 on channel close: %v; stderr:\n%s",
			err, p.stderrString())
	}

	out := p.stdoutString()
	if n := strings.Count(out, "conn=setup"); n != 1 {
		t.Errorf("container-scoped setups = %d, want exactly 1:\n%s", n, out)
	}
	if n := strings.Count(out, "conn=teardown"); n != 1 {
		t.Errorf("container-scoped cleanups = %d, want exactly 1 at shutdown:\n%s", n, out)
	}
	if strings.LastIndex(out, "conn=teardown") < strings.LastIndex(out, "serve conn two") {
		t.Errorf("cleanup must run during graceful shutdown, after the last invocation:\n%s", out)
	}
}

// TestPythonRuntimeContainerScopedAsyncResourceOnProcessLoop pins the
// container-scoped ASYNC resource contract across the warm process: an async
// generator provider is created once on the bootstrap's process-lifetime event
// loop, every warm invocation observes that same loop and the same resource,
// and graceful shutdown closes the resource while the loop that created it is
// still running. The loop identity is printed by the provider itself.
func TestPythonRuntimeContainerScopedAsyncResourceOnProcessLoop(t *testing.T) {
	requirePython(t)
	src := relayRuntimeSrc(t)
	dir := t.TempDir()
	writeOtelStub(t, dir)
	writeHandler(t, dir, "handler", `
import asyncio

from relay_runtime import Depends, bind

SETUP_LOOP = None
SERVED = 0


async def provide_conn():
    global SETUP_LOOP
    SETUP_LOOP = asyncio.get_running_loop()
    print("conn=setup")
    yield "conn"
    print("conn=teardown same-loop=%s" % (asyncio.get_running_loop() is SETUP_LOOP))


@bind
async def serve(event, context, conn=Depends(provide_conn, scope="container")):
    global SERVED
    SERVED += 1
    loop = asyncio.get_running_loop()
    print("serve conn=%s same-loop=%s nth=%d" % (conn, loop is SETUP_LOOP, SERVED))
`)
	p := startEmbeddedBootstrap(t, dir, Bootstrap,
		dependencyInjectionEnv(t, dir, src))

	r1 := p.invoke(t, "handler.serve", `{"msg":"one"}`, nil)
	if !r1.OK {
		t.Fatalf("first invoke failed: %s", r1.Err)
	}
	if !strings.Contains(r1.Out, "conn=setup") {
		t.Errorf("first invoke out = %q, want the resource setup", r1.Out)
	}
	if !strings.Contains(r1.Out, "serve conn=conn same-loop=True nth=1") {
		t.Errorf("first invoke out = %q, want same-loop serving on invocation 1", r1.Out)
	}

	// The second warm invocation reuses the same resource ON THE SAME
	// process-lifetime loop: no second setup, no teardown between
	// invocations, and the handler still observes the creating loop.
	r2 := p.invoke(t, "handler.serve", `{"msg":"two"}`, nil)
	if !r2.OK {
		t.Fatalf("second invoke failed: %s", r2.Err)
	}
	if !strings.Contains(r2.Out, "serve conn=conn same-loop=True nth=2") {
		t.Errorf("second invoke out = %q, want same-loop serving on invocation 2", r2.Out)
	}
	if strings.Contains(r2.Out, "conn=setup") {
		t.Errorf("async resource reopened on a warm invocation: %q", r2.Out)
	}
	if strings.Contains(r2.Out, "conn=teardown") {
		t.Errorf("async resource closed between invocations: %q", r2.Out)
	}

	// Relay is done: the graceful shutdown must close the async resource on
	// the loop that created it, then exit 0.
	if err := p.closeChannel(); err != nil {
		t.Fatalf("close channel: %v", err)
	}
	if err := p.waitExit(t, 10*time.Second); err != nil {
		t.Fatalf("bootstrap did not exit 0 on channel close: %v; stderr:\n%s",
			err, p.stderrString())
	}

	out := p.stdoutString()
	if n := strings.Count(out, "conn=setup"); n != 1 {
		t.Errorf("async resource setups = %d, want exactly 1:\n%s", n, out)
	}
	if !strings.Contains(out, "conn=teardown same-loop=True") {
		t.Errorf("shutdown must close the async resource on its creating loop:\n%s", out)
	}
	if strings.LastIndex(out, "conn=teardown") <
		strings.LastIndex(out, "serve conn=conn same-loop=True nth=2") {
		t.Errorf("async cleanup must run during graceful shutdown, after the last invocation:\n%s", out)
	}
}

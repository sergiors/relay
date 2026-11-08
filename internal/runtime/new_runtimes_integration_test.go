//go:build integration

// New-runtime Docker integration coverage for the two added runtime versions:
// python3.15 and nodejs26.x. The established suite exercises the same engines
// on python3.14 / nodejs24.x (see engine_e2e, uv, pnpm and images integration
// tests — those remain the old-version regression coverage); these tests prove
// the shared engines build and run the pinned NEW base images correctly:
//
//   - python3.15 image preparation from the official python:3.15-slim base,
//     the pinned uv binary, a uv-installed dependency that imports at runtime,
//     plus sync and async handler execution through one warm container.
//   - nodejs26.x image preparation from the official node:26-alpine base whose
//     interpreter is verified as Node 26, the pinned standalone pnpm binary, a
//     pnpm-installed dependency, plus JS and TS execution through one warm
//     container.
//   - runtime switching on an otherwise identical app: 3.14<->3.15 and
//     24<->26 produce distinct app fingerprints, image references and
//     dependency identities, and each execution demonstrably runs its OWN
//     interpreter version, so no version's cache can be served for another.
//   - persistent service containers on the new runtimes: the engine-dispatched
//     service entrypoint starts, runs the entrypoint source, and the same
//     prepared image still serves invocations (one image serves both).
package runtime

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/client"

	"relay/internal/app"
	"relay/internal/testutil"
)

// writeFileIn writes rel into dir, creating nested directories first (a plain
// os.WriteFile fails on a missing parent, so service fixtures under app/ use
// this instead of writeFile).
func writeFileIn(t *testing.T, dir, rel, content string) {
	t.Helper()
	abs := filepath.Join(dir, rel)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(abs), err)
	}
	if err := os.WriteFile(abs, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", rel, err)
	}
}

// newAppFromTemplate writes templateYAML into dir, parses it, and returns the
// app handle carrying the PARSED template. Prepare derives the build's handler
// modules from fn.Template.Events (template.yaml is Relay configuration, never
// baked), so a hand-stamped runtime-only template would declare NO handlers:
// the Node engine would transpile no TypeScript sources and the handler would
// fail at invocation with module-not-found. The parsed template is the exact
// production shape the loader produces.
func newAppFromTemplate(t *testing.T, name, dir, templateYAML string) app.App {
	t.Helper()
	writeFile(t, dir, "template.yaml", templateYAML)
	tmpl, err := app.ParseTemplate([]byte(templateYAML))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	return app.App{Name: name, Dir: dir, Template: tmpl}
}

// TestIntegrationPython315PrepareUvDepsSyncAsyncAndWarmReuse proves the full
// python3.15 lifecycle: Prepare builds an app image FROM a dependency layer on
// the python:3.15-slim base carrying the pinned uv binary; the built image runs
// Python 3.15; a uv-installed dependency (six) imports inside both a sync and
// an async handler; and two async invocations run through the SAME warm
// container process, which sees one stable event loop across both calls.
func TestIntegrationPython315PrepareUvDepsSyncAsyncAndWarmReuse(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// App-image cleanup first so a mid-test failure never leaks onto the shared
	// daemon; dependency layers are content-addressed and shared, so they are
	// intentionally retained.
	t.Cleanup(cleanupImagePrefixes(cli, "relay-app-py315-e2e:"))

	dir := t.TempDir()
	fn := newAppFromTemplate(t, "py315-e2e", dir, `
runtime: python3.15
events:
  - handler: handler.run
    pattern:
      status: [COMPLETED]
  - handler: handler.ache
    pattern:
      status: [COMPLETED]
`)
	writeFile(t, dir, "requirements.txt", "six==1.16.0\n")
	writeFile(t, dir, "handler.py", `
import asyncio
import sys

import six

_loop = None
_calls = 0


def ver():
    return ".".join(str(v) for v in sys.version_info[:2])


def run(event):
    print("sync run ver=" + ver() + " six=" + six.__version__)


async def ache(event):
    global _loop, _calls
    _calls += 1
    current = asyncio.get_running_loop()
    if _loop is None:
        _loop = current
    reused = current is _loop and not _loop.is_closed()
    print("async ache call=%d reused=%s ver=%s six=%s"
          % (_calls, reused, ver(), six.__version__))
`)

	m, _ := newManager(t)
	out := newAppOutputSink(t)
	depRef := expectedDependencyRef(t, fn)
	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if prepared.Dependency != depRef {
		t.Fatalf("prepare dependency = %q, want the content-addressed %q", prepared.Dependency, depRef)
	}
	if !imageExistsInDaemon(cli, ctx, depRef) {
		t.Fatalf("dependency image %s must exist after the build", depRef)
	}

	// The built image must run the pinned toolchain: Python 3.15 as the base
	// interpreter and uv 0.12.17 as the copied-in tool, probed as one-off
	// containers overriding the invocation entrypoint.
	if probeOut, err := runImageCommand(ctx, t, prepared.Image, []string{"python", "--version"}); err != nil {
		t.Fatalf("probe python --version in image: %v", err)
	} else if !strings.Contains(probeOut, "Python 3.15") {
		t.Errorf("image interpreter = %q, want Python 3.15", probeOut)
	}
	if probeOut, err := runImageCommand(ctx, t, prepared.Image, []string{"uv", "--version"}); err != nil {
		t.Fatalf("probe uv --version in image: %v", err)
	} else if !strings.Contains(probeOut, "uv 0.12.17") {
		t.Errorf("image uv binary = %q, want the pinned uv 0.12.17", probeOut)
	}

	// Sync execution imports the uv-installed dependency and reports the
	// container's own interpreter version (3.15 — independent of the host).
	if err := m.Execute(ctx, prepared, "handler.run", []byte(`{"status":"COMPLETED","event_id":"p1"}`), nil); err != nil {
		t.Fatalf("execute sync: %v", err)
	}
	// Async execution twice through the SAME warm container process: the second
	// call must observe the first call's live event loop (call=2 reused=True),
	// so a regression to a per-invocation loop or container fails Execute.
	acheEvent := []byte(`{"status":"COMPLETED","event_id":"a1"}`)
	if err := m.Execute(ctx, prepared, "handler.ache", acheEvent, nil); err != nil {
		t.Fatalf("execute async 1: %v", err)
	}
	if err := m.Execute(ctx, prepared, "handler.ache", []byte(`{"status":"COMPLETED","event_id":"a2"}`), nil); err != nil {
		t.Fatalf("execute async 2: %v", err)
	}
	awaitAppOutput(t, ctx, out,
		"sync run ver=3.15 six=1.16.0",
		"async ache call=1 reused=True ver=3.15 six=1.16.0",
		"async ache call=2 reused=True ver=3.15 six=1.16.0",
	)
}

// TestIntegrationNode26PreparePnpmJSTSAndWarmReuse proves the full nodejs26.x
// lifecycle: Prepare builds an app image FROM a dependency layer on the
// node:26-alpine base carrying the pinned standalone pnpm binary; the image's
// Node interpreter reports v26; the pnpm-installed dependency imports inside
// both a JavaScript and a transpiled TypeScript handler; and two JS
// invocations reuse the SAME warm execution container (and process) across
// both calls.
func TestIntegrationNode26PreparePnpmJSTSAndWarmReuse(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Both test-specific app images are cleaned up. Architecture-qualified names
	// prevent another architecture's image from being reused on the shared daemon.
	jsAppName := "node26-e2e-" + runtime.GOARCH
	tsAppName := "node26-ts-e2e-" + runtime.GOARCH
	t.Cleanup(cleanupImagePrefixes(cli, "relay-app-"+jsAppName+":", "relay-app-"+tsAppName+":"))

	// JS app: dependency import + per-invocation pid/version proof.
	jsDir := t.TempDir()
	jsFn := newAppFromTemplate(t, jsAppName, jsDir, `
runtime: nodejs26.x
events:
  - handler: handler.on
    pattern:
      status: [COMPLETED]
`)
	writeFile(t, jsDir, "package.json", `{"type":"module","dependencies":{"picocolors":"^1.0.0"}}`)
	writeFile(t, jsDir, "pnpm-lock.yaml", nodePnpmLockPicocolors)
	writeFile(t, jsDir, "handler.js", `import colors from "picocolors";
export function on(event) {
  console.log(colors.red("js on id=" + event.event_id + " ver=v" + process.versions.node + " pid=" + process.pid));
}
`)

	// TS app on the same runtime: a transpiled handler importing the same
	// committed dependency, so the import resolves from the dependency image's
	// node_modules rather than the bundled sources.
	tsDir := t.TempDir()
	tsFn := newAppFromTemplate(t, tsAppName, tsDir, `
runtime: nodejs26.x
events:
  - handler: index.tick
    pattern:
      status: [COMPLETED]
`)
	writeFile(t, tsDir, "package.json", `{"type":"module","dependencies":{"picocolors":"^1.0.0"}}`)
	writeFile(t, tsDir, "pnpm-lock.yaml", nodePnpmLockPicocolors)
	writeFile(t, tsDir, "index.ts", `import colors from "picocolors";
export function tick(event: { event_id: string }): void {
  console.log(colors.red("ts tick n26 " + event.event_id));
}
`)

	m, _ := newManager(t)
	out := newAppOutputSink(t)

	jsDep := expectedDependencyRef(t, jsFn)
	prepared, err := m.Prepare(ctx, jsFn)
	if err != nil {
		t.Fatalf("prepare js: %v", err)
	}
	jsImage, err := cli.ImageInspect(ctx, prepared.Image)
	if err != nil {
		t.Fatalf("inspect prepared js image: %v", err)
	}
	assertBuiltPlatform(t, jsImage.Os, jsImage.Architecture)
	if prepared.Dependency != jsDep {
		t.Fatalf("js prepare dependency = %q, want %q", prepared.Dependency, jsDep)
	}

	// The built image must run Node 26 (a v26 interpreter, never v24) and the
	// pinned standalone pnpm at its installed path.
	nodeOut, err := runImageCommand(ctx, t, prepared.Image, []string{"node", "--version"})
	if err != nil {
		t.Fatalf("probe node --version in image: %v", err)
	}
	if !strings.HasPrefix(strings.TrimSpace(nodeOut), "v26.") {
		t.Errorf("image node --version = %q, want a v26 interpreter", nodeOut)
	}
	pnpmOut, err := runImageCommand(ctx, t, prepared.Image, []string{"sh", "-c", "test -x /usr/local/bin/pnpm && pnpm --version"})
	if err != nil {
		t.Fatalf("probe pnpm --version in image: %v", err)
	}
	if !strings.Contains(pnpmOut, PnpmVersion) {
		t.Errorf("image pnpm --version = %q, want the pinned pnpm %s", pnpmOut, PnpmVersion)
	}

	// JS execution twice: the second invocation must REUSE the warm container
	// (id equality) and run in the SAME process (pid equality), with the
	// dependency imported on each call.
	if err := m.Execute(ctx, prepared, "handler.on", []byte(`{"event_id":"n26-0","status":"COMPLETED"}`), nil); err != nil {
		t.Fatalf("execute js 1: %v", err)
	}
	id1 := waitForContainerByLabel(ctx, m.cli, labelApp, jsFn.Name)
	if id1 == "" {
		t.Fatal("warm execution container not found after the first invocation")
	}
	if err := m.Execute(ctx, prepared, "handler.on", []byte(`{"event_id":"n26-1","status":"COMPLETED"}`), nil); err != nil {
		t.Fatalf("execute js 2: %v", err)
	}
	id2 := waitForContainerByLabel(ctx, m.cli, labelApp, jsFn.Name)
	if id2 == "" {
		t.Fatal("warm execution container not found after the second invocation")
	}
	if id2 != id1 {
		t.Errorf("second invocation must reuse the warm nodejs26.x container: id1=%s id2=%s", id1, id2)
	}
	// Await each invocation's DISTINCT line (the event id is contiguous with
	// the interpreter-version proof), so both lines are guaranteed present
	// before the pid assertion: two identical prefixes would let one line
	// satisfy both awaits and race the two-pid regex below.
	logs := awaitAppOutput(t, ctx, out,
		"id=n26-0 ver=v26.",
		"id=n26-1 ver=v26.",
	)
	// pid evidence: both invocations ran in the same node process, exactly as a
	// warm (process-lived, not per-invocation) execution container defines it.
	pids := regexp.MustCompile(`pid=(\d+)`).FindAllStringSubmatch(logs, -1)
	if len(pids) != 2 {
		t.Fatalf("expected exactly two pid proofs in the handler output, got %d:\n%s", len(pids), logs)
	}
	first, second := pids[0][1], pids[1][1]
	if first != second {
		t.Errorf("invocations must share one warm node process: pid1=%s pid2=%s", first, second)
	}
	if _, err := strconv.Atoi(first); err != nil {
		t.Fatalf("pid = %q is not numeric: %v", first, err)
	}

	// TS execution from its own prepared image: the transpiled handler runs on
	// Node 26 and its external import resolves from the dependency layer.
	// (The js app image does not carry index.ts, so driving index.tick on it
	// must fail: handler resolution is build-derived.)
	tsPrepared, err := m.Prepare(ctx, tsFn)
	if err != nil {
		t.Fatalf("prepare ts: %v", err)
	}
	tsImage, err := cli.ImageInspect(ctx, tsPrepared.Image)
	if err != nil {
		t.Fatalf("inspect prepared ts image: %v", err)
	}
	assertBuiltPlatform(t, tsImage.Os, tsImage.Architecture)
	if tsPrepared.Dependency != jsDep {
		t.Fatalf("ts prepare dependency = %q, want the same content-addressed %q (identical manifest)", tsPrepared.Dependency, jsDep)
	}
	if err := m.Execute(ctx, tsPrepared, "index.tick", []byte(`{"event_id":"n26-ts0","status":"COMPLETED"}`), nil); err != nil {
		t.Fatalf("execute ts: %v", err)
	}
	awaitAppOutput(t, ctx, out, "ts tick n26 n26-ts0")
}

// TestIntegrationPythonRuntimeSwitchIsolatesIdentities proves 3.14<->3.15
// switching on an otherwise IDENTICAL app never shares identity: the runtime
// change (and only the runtime change) yields a distinct app fingerprint, a
// distinct image reference, and a distinct content-addressed dependency layer
// with a different image id — and each execution provably ran its own
// interpreter (the handler prints the container's own sys.version). Re-preparing
// the 3.14 app after the 3.15 build resolves the ORIGINAL identities again,
// so the new version's build never clobbered the old version's cache.
func TestIntegrationPythonRuntimeSwitchIsolatesIdentities(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	t.Cleanup(cleanupImagePrefixes(cli, "relay-app-switch-py:"))

	fnName := "switch-py"
	mkApp := func(runtimeName string) app.App {
		dir := t.TempDir()
		fn := newAppFromTemplate(t, fnName, dir, "runtime: "+runtimeName+`
events:
  - handler: handler.run
    pattern:
      status: [COMPLETED]
`)
		writeFile(t, dir, "requirements.txt", "six==1.16.0\n")
		writeFile(t, dir, "handler.py", `import six, sys

def run(event):
    print("switch run ver=" + ".".join(str(v) for v in sys.version_info[:2]) + " six=" + six.__version__)
`)
		return fn
	}

	fn14 := mkApp("python3.14")
	fn15 := mkApp("python3.15")
	m, _ := newManager(t)
	out := newAppOutputSink(t)

	dep14 := expectedDependencyRef(t, fn14)
	dep15 := expectedDependencyRef(t, fn15)
	if dep14 == dep15 {
		t.Fatalf("dependency references must differ across the runtime switch, both %s", dep14)
	}

	prepared14, err := m.Prepare(ctx, fn14)
	if err != nil {
		t.Fatalf("prepare python3.14: %v", err)
	}
	if prepared14.Dependency != dep14 {
		t.Fatalf("python3.14 dependency = %q, want %q", prepared14.Dependency, dep14)
	}
	prepared15, err := m.Prepare(ctx, fn15)
	if err != nil {
		t.Fatalf("prepare python3.15: %v", err)
	}
	if prepared15.Dependency != dep15 {
		t.Fatalf("python3.15 dependency = %q, want %q", prepared15.Dependency, dep15)
	}

	if prepared14.Image == prepared15.Image {
		t.Errorf("app image references must differ across the runtime switch, both %s", prepared14.Image)
	}
	if prepared14.Fingerprint == prepared15.Fingerprint {
		t.Errorf("app fingerprints must differ across the runtime switch, both %s", prepared14.Fingerprint)
	}
	insp14, err := cli.ImageInspect(ctx, dep14)
	if err != nil {
		t.Fatalf("inspect python3.14 dependency image: %v", err)
	}
	insp15, err := cli.ImageInspect(ctx, dep15)
	if err != nil {
		t.Fatalf("inspect python3.15 dependency image: %v", err)
	}
	if insp14.ID == insp15.ID {
		t.Errorf("dependency layers must be distinct CONTENT across the runtime switch (both %s)", insp14.ID)
	}

	// Each execution prints the container's OWN interpreter version, so a
	// wrong-version cache would be caught in the output.
	if err := m.Execute(ctx, prepared14, "handler.run", []byte(`{"status":"COMPLETED","event_id":"sw-14"}`), nil); err != nil {
		t.Fatalf("execute python3.14: %v", err)
	}
	if err := m.Execute(ctx, prepared15, "handler.run", []byte(`{"status":"COMPLETED","event_id":"sw-15"}`), nil); err != nil {
		t.Fatalf("execute python3.15: %v", err)
	}
	awaitAppOutput(t, ctx, out,
		"switch run ver=3.14 six=1.16.0",
		"switch run ver=3.15 six=1.16.0",
	)

	// Isolation is stable: re-preparing the 3.14 app resolves the SAME image
	// and dependency identities again (the 3.15 build changed nothing about
	// 3.14's content-addressed cache entries).
	again, err := m.Prepare(ctx, fn14)
	if err != nil {
		t.Fatalf("re-prepare python3.14: %v", err)
	}
	if again.Image != prepared14.Image {
		t.Errorf("re-prepare python3.14 image = %q, want the original %q", again.Image, prepared14.Image)
	}
	if again.Dependency != dep14 {
		t.Errorf("re-prepare python3.14 dependency = %q, want the original %q", again.Dependency, dep14)
	}
}

// TestIntegrationNodeRuntimeSwitchIsolatesIdentities proves the nodejs24.x
// <-> nodejs26.x switching contract, mirroring the Python switch test: distinct
// fingerprints, image references and dependency images per runtime version,
// in-container interpreter versions proving which runtime served each
// execution, and identity stability for the 24 app after 26 was built.
func TestIntegrationNodeRuntimeSwitchIsolatesIdentities(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	t.Cleanup(cleanupImagePrefixes(cli, "relay-app-switch-node:"))

	fnName := "switch-node"
	mkApp := func(runtimeName string) app.App {
		dir := t.TempDir()
		fn := newAppFromTemplate(t, fnName, dir, "runtime: "+runtimeName+`
events:
  - handler: handler.run
    pattern:
      status: [COMPLETED]
`)
		writeFile(t, dir, "package.json", `{"type":"module","dependencies":{"picocolors":"^1.0.0"}}`)
		writeFile(t, dir, "pnpm-lock.yaml", nodePnpmLockPicocolors)
		writeFile(t, dir, "handler.js", `import colors from "picocolors";
export function run(event) {
  console.log(colors.red("switch run ver=v" + process.versions.node + " dep=" + !!event.event_id));
}
`)
		return fn
	}

	fn24 := mkApp("nodejs24.x")
	fn26 := mkApp("nodejs26.x")
	m, _ := newManager(t)
	out := newAppOutputSink(t)

	dep24 := expectedDependencyRef(t, fn24)
	dep26 := expectedDependencyRef(t, fn26)
	if dep24 == dep26 {
		t.Fatalf("dependency references must differ across the runtime switch, both %s", dep24)
	}

	prepared24, err := m.Prepare(ctx, fn24)
	if err != nil {
		t.Fatalf("prepare nodejs24.x: %v", err)
	}
	if prepared24.Dependency != dep24 {
		t.Fatalf("nodejs24.x dependency = %q, want %q", prepared24.Dependency, dep24)
	}
	prepared26, err := m.Prepare(ctx, fn26)
	if err != nil {
		t.Fatalf("prepare nodejs26.x: %v", err)
	}
	if prepared26.Dependency != dep26 {
		t.Fatalf("nodejs26.x dependency = %q, want %q", prepared26.Dependency, dep26)
	}

	if prepared24.Image == prepared26.Image {
		t.Errorf("app image references must differ across the runtime switch, both %s", prepared24.Image)
	}
	if prepared24.Fingerprint == prepared26.Fingerprint {
		t.Errorf("app fingerprints must differ across the runtime switch, both %s", prepared24.Fingerprint)
	}
	insp24, err := cli.ImageInspect(ctx, dep24)
	if err != nil {
		t.Fatalf("inspect nodejs24.x dependency image: %v", err)
	}
	insp26, err := cli.ImageInspect(ctx, dep26)
	if err != nil {
		t.Fatalf("inspect nodejs26.x dependency image: %v", err)
	}
	if insp24.ID == insp26.ID {
		t.Errorf("dependency layers must be distinct CONTENT across the runtime switch (both %s)", insp24.ID)
	}

	if err := m.Execute(ctx, prepared24, "handler.run", []byte(`{"status":"COMPLETED","event_id":"sw-24"}`), nil); err != nil {
		t.Fatalf("execute nodejs24.x: %v", err)
	}
	if err := m.Execute(ctx, prepared26, "handler.run", []byte(`{"status":"COMPLETED","event_id":"sw-26"}`), nil); err != nil {
		t.Fatalf("execute nodejs26.x: %v", err)
	}
	awaitAppOutput(t, ctx, out,
		"switch run ver=v24.",
		"switch run ver=v26.",
	)

	again, err := m.Prepare(ctx, fn24)
	if err != nil {
		t.Fatalf("re-prepare nodejs24.x: %v", err)
	}
	if again.Image != prepared24.Image {
		t.Errorf("re-prepare nodejs24.x image = %q, want the original %q", again.Image, prepared24.Image)
	}
	if again.Dependency != dep24 {
		t.Errorf("re-prepare nodejs24.x dependency = %q, want the original %q", again.Dependency, dep24)
	}
}

// TestIntegrationNewRuntimePersistentServices proves persistent service
// containers work on the two new runtimes: the engine-dispatched service
// entrypoint (python -m for Python app/main.py, node for Node app/service.js)
// starts a REAL long-lived container, reports its startup proof (for Python, a
// package-relative import plus the container's own 3.15 interpreter proof; for
// Node, the container's own v26 version), the SAME prepared image still serves
// invocations, and the reconcile stop path removes exactly that service
// container.
func TestIntegrationNewRuntimePersistentServices(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	t.Run("python3.15", func(t *testing.T) {
		t.Cleanup(cleanupImagePrefixes(cli, "relay-app-svc-py15:"))

		// The app name is fixed, so scoped isolation FIRST makes every run start
		// clean: any service container a prior (possibly failed) run leaked under
		// the same identity is listed and removed before the scenario, keeping the
		// exactly-one-replica and removed==1 assertions meaningful. Stopping every
		// daemon-wide service container is deliberately avoided (other tests and a
		// live worker own their own).
		isolateAppServiceContainers(t, ctx, cli, "svc-py15")

		dir := t.TempDir()
		fn := newAppFromTemplate(t, "svc-py15", dir, `
runtime: python3.15
events:
  - handler: index.hi
    pattern:
      status: [COMPLETED]
`)
		writeFile(t, dir, "index.py", "def hi(e):\n    print('hi-15')\n")
		writeFileIn(t, dir, "app/__init__.py", "")
		writeFileIn(t, dir, "app/deps.py", "NAME = 'from-relative-import-ok'\n")
		// The service entrypoint imports a package-relative NAME (the module
		// execution proof), prints the interpreter's own version, and handles
		// SIGTERM so reconciliation stops it promptly instead of paying the
		// daemon's 10s SIGKILL grace.
		writeFileIn(t, dir, "app/main.py", `import signal, sys
from .deps import NAME

def _on_sigterm(_signum, _frame):
    raise SystemExit(0)

signal.signal(signal.SIGTERM, _on_sigterm)

ver = ".".join(str(v) for v in sys.version_info[:2])
print("svc15 module-entrypoint " + NAME + " pyver=" + ver, flush=True)

def keep_alive():
    import time

    while True:
        time.sleep(86400)

keep_alive()
`)
		m, _ := newManager(t)
		out := newAppOutputSink(t)

		prepared, err := m.Prepare(ctx, fn)
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}

		entry, err := ServiceEntry("python3.15", "app/main.py")
		if err != nil {
			t.Fatalf("service entry: %v", err)
		}
		id, err := m.StartService(ctx, ServiceSpec{
			App:       fn.Name,
			Name:      "svc",
			SourceRef: "app/main.py",
			Port:      8080,
			Image:     prepared.Image,
			Entry:     entry,
			Env:       []string{"PORT=8080"},
		}, 0)
		if err != nil {
			t.Fatalf("start service: %v", err)
		}
		waitForContainerRunning(t, ctx, cli, id)
		insp, err := cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
		if err != nil {
			t.Fatalf("inspect service container: %v", err)
		}
		if insp.Container.Config == nil {
			t.Fatal("service container has no config")
		}
		if entrypoint := insp.Container.Config.Entrypoint; strings.Join(entrypoint, " ") != "python -m app.main" {
			t.Errorf("service entrypoint = %v, want [python -m app.main]", entrypoint)
		}

		// Poll the logs for the startup proof (relative import + 3.15 proof).
		// State.Running flips before the interpreter has booted and flushed.
		const proof = "svc15 module-entrypoint from-relative-import-ok pyver=3.15"
		var logs string
		if !pollUntil(ctx, 30*time.Second, func() bool {
			logs = readServiceLogs(ctx, cli, id)
			return strings.Contains(logs, proof)
		}) {
			t.Fatalf("service logs missing proof %q within 30s, got:\n%s", proof, logs)
		}

		// The same image serves an invocation alongside the service.
		if err := m.Execute(ctx, prepared, "index.hi", []byte(`{"status":"COMPLETED"}`), nil); err != nil {
			t.Fatalf("execute on service image: %v", err)
		}
		awaitAppOutput(t, ctx, out, "hi-15")

		// The reconcile stop path removes exactly this service's containers.
		removed, err := m.RemoveAppServiceContainers(ctx, fn.Name)
		if err != nil {
			t.Fatalf("remove service containers: %v", err)
		}
		if removed != 1 {
			t.Errorf("removed %d service containers, want 1", removed)
		}
		if !waitForAppServiceContainersCount(ctx, cli, fn.Name, "svc", 0) {
			t.Error("service container should be gone after removal")
		}
	})

	t.Run("nodejs26.x", func(t *testing.T) {
		t.Cleanup(cleanupImagePrefixes(cli, "relay-app-svc-node26:"))

		// Same fixed-name isolation as the python3.15 scenario: scoped, per-app-
		// identity removal of any remnants a prior run leaked, before the scenario.
		isolateAppServiceContainers(t, ctx, cli, "svc-node26")

		dir := t.TempDir()
		fn := newAppFromTemplate(t, "svc-node26", dir, `
runtime: nodejs26.x
events:
  - handler: index.hi
    pattern:
      status: [COMPLETED]
`)
		writeFile(t, dir, "index.js", "export function hi(e){ console.log('hi-26'); }\n")
		// Long-lived service entrypoint with a prompt SIGTERM handler, mirroring
		// the established nodejs24.x service fixtures. The startup proof carries
		// the CONTAINER's own node version, so a v24 image reaching a
		// nodejs26.x service is caught.
		writeFileIn(t, dir, "app/service.js", `process.on("SIGTERM", () => process.exit(0));
console.log("svc26 started node=" + process.version);
setInterval(() => {}, 1 << 30);
`)
		m, _ := newManager(t)
		out := newAppOutputSink(t)

		prepared, err := m.Prepare(ctx, fn)
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}

		entry, err := ServiceEntry("nodejs26.x", "app/service.js")
		if err != nil {
			t.Fatalf("service entry: %v", err)
		}
		id, err := m.StartService(ctx, ServiceSpec{
			App:       fn.Name,
			Name:      "svc",
			SourceRef: "app/service.js",
			Port:      8080,
			Image:     prepared.Image,
			Entry:     entry,
			Env:       []string{"PORT=8080"},
		}, 0)
		if err != nil {
			t.Fatalf("start service: %v", err)
		}
		waitForContainerRunning(t, ctx, cli, id)
		insp, err := cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
		if err != nil {
			t.Fatalf("inspect service container: %v", err)
		}
		if insp.Container.Config == nil {
			t.Fatal("service container has no config")
		}
		if entrypoint := insp.Container.Config.Entrypoint; strings.Join(entrypoint, " ") != "node /app/app/service.js" {
			t.Errorf("service entrypoint = %v, want [node /app/app/service.js]", entrypoint)
		}

		const proof = "svc26 started node=v26."
		var logs string
		if !pollUntil(ctx, 30*time.Second, func() bool {
			logs = readServiceLogs(ctx, cli, id)
			return strings.Contains(logs, proof)
		}) {
			t.Fatalf("service logs missing proof %q within 30s, got:\n%s", proof, logs)
		}

		if err := m.Execute(ctx, prepared, "index.hi", []byte(`{"status":"COMPLETED"}`), nil); err != nil {
			t.Fatalf("execute on service image: %v", err)
		}
		awaitAppOutput(t, ctx, out, "hi-26")

		removed, err := m.RemoveAppServiceContainers(ctx, fn.Name)
		if err != nil {
			t.Fatalf("remove service containers: %v", err)
		}
		if removed != 1 {
			t.Errorf("removed %d service containers, want 1", removed)
		}
		if !waitForAppServiceContainersCount(ctx, cli, fn.Name, "svc", 0) {
			t.Error("service container should be gone after removal")
		}
	})
}

// readServiceLogs drains a service container's multiplexed stdout/stderr into
// one plain string (the same demultiplexing shape the probe runner uses).
func readServiceLogs(ctx context.Context, cli *client.Client, id string) string {
	res, err := cli.ContainerLogs(ctx, id, client.ContainerLogsOptions{ShowStdout: true, ShowStderr: true})
	if err != nil {
		return ""
	}
	defer res.Close()
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, res)
	return buf.String()
}

// cleanupFailedBuildRetains removes the classic-builder intermediate containers
// THIS failed build retained. The IDs are parsed from the build's own
// propagated stream (failedBuildIntermediateIDs, started-minus-subsequently-
// removed) and are the daemon's own debug leftovers, so the cleanup touches
// only this test's leftovers. Removal failures are REPORTED (t.Errorf), NotFound
// is benign, because a build tooling step may legitimately remove its own
// container before the step fails. It must be called with the error text BEFORE
// any test failure so a mid-test Fatalf still leaves the cleanup registered.
func cleanupFailedBuildRetains(t *testing.T, cli *client.Client, errText string) {
	t.Helper()
	intermediates := failedBuildIntermediateIDs(errText)
	if len(intermediates) == 0 {
		return
	}
	t.Cleanup(func() {
		for _, id := range intermediates {
			// removeContainer treats NotFound as benign (the failed build may have
			// removed its own intermediate); every other removal failure is logged.
			if err := removeContainer(cli, id); err != nil {
				t.Errorf("cleanup: remove failed-build intermediate %s: %v", id, err)
			}
		}
	})
}

// TestIntegrationPython315MalformedRequirementsFailsPrepare pins the uv
// dependency-install failure path on the NEW Python runtime: a malformed line
// in a committed requirements.txt fails `uv pip install --system -r
// requirements.txt` INSIDE the dependency-image build, and the failure must be
// PROPAGATED to Prepare (the build stream's error text, not a silent success),
// with no app image tagged for the app.
func TestIntegrationPython315MalformedRequirementsFailsPrepare(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// A failed dependency build tags no image; the prefix cleanup is a harmless
	// no-op when nothing was tagged, and still guarantees no leak path.
	t.Cleanup(cleanupImagePrefixes(cli, "relay-app-py315-malformed:"))

	dir := t.TempDir()
	fn := newAppFromTemplate(t, "py315-malformed", dir, `
runtime: python3.15
events:
  - handler: index.hi
    pattern:
      status: [COMPLETED]
`)
	writeFile(t, dir, "index.py", "def hi(e):\n    return 'hi'\n")
	// six is a valid requirement; the second line is not a valid requirement
	// spec, so uv's pip frontend rejects it deterministically BEFORE resolving
	// anything (a parse failure, not a network failure).
	writeFile(t, dir, "requirements.txt", "six==1.16.0\n!!! not-a-valid-requirement\n")

	// mPrepare returns Prepare's error (it only Fatalfs on NewManager failure).
	prepared, err := mPrepare(ctx, t, fn)
	if err == nil {
		t.Fatalf("expected Prepare to fail on malformed requirements.txt, got success (image %s)", prepared.Image)
	}
	errText := err.Error()
	t.Logf("prepare error: %s", errText)

	// The failure must be the PROPAGATED uv/build-stream error, not an opaque
	// wrapper: any of the stable tokens the daemon's failure output carries for
	// an invalid requirement (uv's parse error context, the manifest name, or
	// the invalid token itself). The same propagation shape the established
	// failed-build test asserts for Node.
	if !strings.Contains(errText, "not-a-valid-requirement") &&
		!strings.Contains(errText, "requirements.txt") &&
		!strings.Contains(errText, "uv") {
		t.Errorf("expected the propagated prepare error to mention the uv requirement failure "+
			"(error text, manifest name, or uv tool), got: %s", errText)
	}

	// Failed-build debugging leftovers of THIS build are cleaned up.
	cleanupFailedBuildRetains(t, cli, errText)
}

// TestIntegrationNode26MalformedPackageJSONFailsPrepare pins the pnpm
// package-manifest failure path on the NEW Node runtime: a malformed
// package.json alongside a committed pnpm-lock.yaml reaches
// `pnpm install --frozen-lockfile` (the lock satisfies the plan-time manifest
// policy) and fails INSIDE the dependency-image build, with the pnpm/JSON
// failure propagated to Prepare and no app image tagged for the app. The same
// failure shape the established build test asserts on nodejs24.x
// (TestIntegrationFailedBuildPropagatesError), narrowed to the new-runtime
// identifier.
func TestIntegrationNode26MalformedPackageJSONFailsPrepare(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	t.Cleanup(cleanupImagePrefixes(cli, "relay-app-node26-malformed:"))

	dir := t.TempDir()
	fn := newAppFromTemplate(t, "node26-malformed", dir, `
runtime: nodejs26.x
events:
  - handler: index.hi
    pattern:
      status: [COMPLETED]
`)
	writeFile(t, dir, "index.js", "export function hi(e){ console.log('hi'); }\n")
	// The committed lock passes the plan-time pnpm-only manifest policy, so the
	// malformed manifest is exercised at the INSTALL step (deterministically and
	// before any resolution), not plan time.
	writeFile(t, dir, "pnpm-lock.yaml", nodePnpmLockEmpty)
	writeFile(t, dir, "package.json", "{ not json")

	prepared, err := mPrepare(ctx, t, fn)
	if err == nil {
		t.Fatalf("expected Prepare to fail on malformed package.json, got success (image %s)", prepared.Image)
	}
	errText := err.Error()
	t.Logf("prepare error: %s", errText)

	// The error must propagate the pnpm failure surface (stable tokens observed
	// on the daemon for a JSON parse failure inside `pnpm install`).
	if !strings.Contains(errText, "pnpm") &&
		!strings.Contains(errText, "JSON") &&
		!strings.Contains(errText, "package.json") {
		t.Errorf("expected the propagated prepare error to mention the pnpm/JSON failure, got: %s", errText)
	}

	cleanupFailedBuildRetains(t, cli, errText)
}

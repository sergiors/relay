//go:build integration

package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/testutil"
)

// PYTHON_ZUVLOOP end-to-end coverage against a real Docker daemon and the real
// pinned zuvloop package: the manager wires the worker-global setting into the
// Python preparation, the built app image runs invocation handlers on the
// zuvloop loop, a mode flip rebuilds the image (new relay.bootstrap label), a
// service-only app keeps the identical standard-mode image, and the disabled
// mode keeps the standard asyncio loop.

// zuvloopProbesHandler prints, per invocation, which event loop actually ran
// the handler and whether the module-level resource bound to the process's
// loop is still alive on the same loop. The handlers fail the invocation when
// the loop identity changes, so a regression fails Execute instead of passing.
//
// The loop's module is normalized to its TOP-LEVEL package before printing:
// zuvloop 0.0.17 implements its loop class inside the package (zuvloop._loop),
// while the standard asyncio loop lives in a submodule too (asyncio.unix_events
// on Linux). Asserting the normalized top-level package keeps the Docker
// assertions pinned to the observable "which loop family ran" question —
// zuvloop vs asyncio — without coupling to either library's private module
// layout.
const zuvloopProbesHandler = `
import asyncio

_loop = None
_queue = None
_calls = 0


def _loopmod(loop):
    return type(loop).__module__.split(".")[0]


def _observe():
    # One loop per process: the loop of every invocation (sync, event, AND
    # schedule shapes) must be the SAME live loop, and the module-level Queue
    # binds to it on first use and stays usable.
    global _loop, _queue, _calls
    _calls += 1
    current = asyncio.get_running_loop()
    if _loop is None:
        _loop = current
        _queue = asyncio.Queue()
    assert current is _loop, "event loop changed between invocations"
    assert not current.is_closed(), "event loop closed between invocations"
    return current, _queue, _calls


def sync_probe(event):
    # A synchronous handler is invoked INLINE on the running loop: this plain
    # function has no coroutine of its own, yet asyncio.get_running_loop()
    # succeeds and must already be the REAL pinned zuvloop loop. That is the
    # loop-family proof for sync handlers. The Queue created during this
    # invocation is NOT yet loop-bound: in CPython, put_nowait/get_nowait never
    # call _get_loop() (verified: the Queue's _loop stays None across the
    # roundtrip), so this roundtrip only proves the module resource is created
    # and usable during a sync handler. The async handlers that follow do the
    # binding/checking — their await paths and the explicit queue._get_loop()
    # is loop probe report reusable=True only when the Queue created during
    # this sync invocation binds to that same zuvloop loop.
    loop, queue, calls = _observe()
    queue.put_nowait("created-during-sync-handler")
    assert queue.get_nowait() == "created-during-sync-handler", (
        "the module Queue must round-trip during the sync handler")
    print("sync loopmod=%s call=%d" % (_loopmod(loop), calls))


async def touch(event):
    loop, queue, calls = _observe()
    await queue.put(event["tag"])
    await queue.get()
    print("event loopmod=%s call=%d reused=%s" % (
        _loopmod(loop), calls, queue._get_loop() is loop))


async def tick(event):
    # The schedule path: the runner invokes a cron occurrence's handler through
    # the same invocation protocol, so this name must run on the same
    # process-lifetime loop as the event handler above.
    loop, queue, calls = _observe()
    await queue.put("schedule")
    await queue.get()
    print("schedule loopmod=%s call=%d reused=%s" % (
        _loopmod(loop), calls, queue._get_loop() is loop))
`

// writeZuvloopApp writes an invoking Python app dir: an event rule and a cron
// schedule, both wired to the probe handlers. The hand-built template mirrors
// what app.ParseTemplate resolves for these two shapes (schedules carry their
// stable name and a minute-granularity cron).
func writeZuvloopApp(t *testing.T) app.App {
	t.Helper()
	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: python3.14
events:
  - handler: handler.touch
    pattern:
      status: [COMPLETED]
schedules:
  - name: hourly-tick
    handler: handler.tick
    cron: "0 * * * *"
`)
	writeFile(t, dir, "handler.py", zuvloopProbesHandler)
	return app.App{
		Name: "py-zuvloop-e2e",
		Dir:  dir,
		Template: &app.Template{
			Runtime: "python3.14",
			Events: []app.EventRule{{
				Handler: "handler.touch",
			}},
			Schedules: []app.Schedule{{
				Name:    "hourly-tick",
				Handler: "handler.tick",
				Cron:    "0 * * * *",
			}},
		},
	}
}

// imageBootstrapLabel inspects the named image's relay.bootstrap label and
// fails the test when the daemon cannot report it.
func imageBootstrapLabel(t *testing.T, ctx context.Context, m *Manager, image string) string {
	t.Helper()
	insp, err := m.cli.ImageInspect(ctx, image)
	if err != nil {
		t.Fatalf("inspect image %s: %v", image, err)
	}
	if insp.Config == nil {
		t.Fatalf("image %s has no config (no labels)", image)
	}
	return insp.Config.Labels[labelBootstrap]
}

// TestIntegrationPythonZuvloopEndToEndWarmLoop prepares an invoking Python app
// with the worker-global setting enabled and drives — through the SAME warm
// container — a SYNCHRONOUS handler first, then the async event handler, then
// the scheduled handler. The sync handler proves asyncio.get_running_loop()
// already returns the REAL pinned zuvloop loop for a plain function called
// inline on the loop, and the module-level Queue binds to it from sync context;
// the async event and scheduled handlers must then continue on that SAME loop
// and reuse the Queue (reused=True). Every probe reports the zuvloop loop, and
// the call counters pin that no invocation ran on a fresh loop (the handlers
// themselves fail Execute when the loop identity changes).
func TestIntegrationPythonZuvloopEndToEndWarmLoop(t *testing.T) {
	testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	fn := writeZuvloopApp(t)
	out := newAppOutputSink(t)
	m, err := newCoordinatedRealManager(t, testutil.DiscardLogger(), nil, WithPythonZuvloop(true))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	// First invocation is SYNCHRONOUS: the plain function must already observe
	// the real zuvloop loop via asyncio.get_running_loop().
	if err := m.Execute(ctx, prepared, "handler.sync_probe", []byte(`{"tag":"sync"}`), nil); err != nil {
		t.Fatalf("execute sync handler: %v", err)
	}
	// The async event handler continues on the same loop, reusing the Queue
	// that first bound under the sync handler.
	if err := m.Execute(ctx, prepared, "handler.touch", []byte(`{"event_id":"1757-0","status":"COMPLETED","tag":"event"}`), nil); err != nil {
		t.Fatalf("execute event handler: %v", err)
	}
	// The scheduled handler must also stay on the same process-lifetime loop
	// and resource.
	if err := m.Execute(ctx, prepared, "handler.tick", []byte(`{"tag":"schedule"}`), nil); err != nil {
		t.Fatalf("execute schedule handler: %v", err)
	}

	logs := awaitAppOutput(t, ctx, out,
		"sync loopmod=zuvloop",
		"event loopmod=zuvloop",
		"schedule loopmod=zuvloop",
		"reused=True",
	)
	if !strings.Contains(logs, "sync loopmod=zuvloop call=1") {
		t.Errorf("the FIRST (sync) invocation must already report the zuvloop loop; logs:\n%s", logs)
	}
	if !strings.Contains(logs, "event loopmod=zuvloop call=2") {
		t.Errorf("the async event handler must continue on the same loop (call=2); logs:\n%s", logs)
	}
	if !strings.Contains(logs, "schedule loopmod=zuvloop call=3") {
		t.Errorf("the scheduled handler must still be the same loop (call=3); logs:\n%s", logs)
	}
}

// TestIntegrationPythonZuvloopDisabledKeepsAsyncio prepares the same app under
// the default worker settings: the standard asyncio loop runs the handlers and
// no zuvloop loop ever appears.
func TestIntegrationPythonZuvloopDisabledKeepsAsyncio(t *testing.T) {
	testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	fn := writeZuvloopApp(t)
	out := newAppOutputSink(t)
	m, _ := newManager(t)

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if err := m.Execute(ctx, prepared, "handler.touch", []byte(`{"event_id":"1757-0","status":"COMPLETED","tag":"event"}`), nil); err != nil {
		t.Fatalf("execute event handler: %v", err)
	}
	if err := m.Execute(ctx, prepared, "handler.tick", []byte(`{"tag":"schedule"}`), nil); err != nil {
		t.Fatalf("execute schedule handler: %v", err)
	}

	logs := awaitAppOutput(t, ctx, out, "event loopmod=asyncio", "schedule loopmod=asyncio")
	// The negative assertion must target the loop-family MARKER, not arbitrary
	// sink text: the app name and forwarding prefixes can legitimately contain
	// the substring "zuvloop" while every invocation reports loopmod=asyncio.
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, "loopmod=zuvloop") {
			t.Errorf("the disabled mode must never select a zuvloop loop, got %q in:\n%s", strings.TrimSpace(line), logs)
		}
	}
}

// TestIntegrationPythonZuvloopModeFlipRebuildsImage pins the cross-worker
// tag-collision rule end to end against a real daemon: a Python invocation
// image built with PYTHON_ZUVLOOP carries a MODE-SCOPED baked tag (distinct from
// the raw source tag the disabled worker uses), so two workers with different
// settings resolve two IMMUTABLE refs for the same app instead of alternately
// retagging one mutable name (Execute resolves a ref's current content, so a
// shared tag could silently flip a warm handler between loops). Each ref
// labels for its own mode — the relay.bootstrap check still governs stale
// bootstrap/tool updates within a mode — and every manager reuses its own ref.
// Prepared.Fingerprint stays the raw app-source identity in every mode, and a
// service-only app is never mode-scoped: one raw ref and label for both modes,
// so flipping the setting must not rebuild its image.
func TestIntegrationPythonZuvloopModeFlipRebuildsImage(t *testing.T) {
	testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	fn := writeZuvloopApp(t)
	fp, err := app.FingerprintApp(fn.Dir, fn.Template)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	rawRef := ImageRef(fn.Name, fp)
	modeRef := ImageRef(fn.Name, zuvloopImageFingerprint(fp))
	if modeRef == rawRef {
		t.Fatalf("mode-scoped tag %s must differ from the raw source tag %s", modeRef, rawRef)
	}

	mOff, err := newCoordinatedRealManager(t, testutil.DiscardLogger(), nil)
	if err != nil {
		t.Fatalf("new disabled manager: %v", err)
	}
	defer func() { _ = mOff.Close() }()
	preparedOff, err := mOff.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare disabled: %v", err)
	}
	if preparedOff.Image != rawRef {
		t.Fatalf("disabled baked image ref = %s, want the unchanged raw source tag %s", preparedOff.Image, rawRef)
	}
	labelOff := imageBootstrapLabel(t, ctx, mOff, preparedOff.Image)

	mOn, err := newCoordinatedRealManager(t, testutil.DiscardLogger(), nil, WithPythonZuvloop(true))
	if err != nil {
		t.Fatalf("new enabled manager: %v", err)
	}
	defer func() { _ = mOn.Close() }()
	preparedOn, err := mOn.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare enabled: %v", err)
	}
	if preparedOn.Image != modeRef {
		t.Fatalf("enabled baked image ref = %s, want the mode-scoped tag %s", preparedOn.Image, modeRef)
	}
	if preparedOn.Image == preparedOff.Image {
		t.Fatalf("an enabled invoking app must use a mode-specific tag, got %s for both modes", preparedOn.Image)
	}
	labelOn := imageBootstrapLabel(t, ctx, mOn, preparedOn.Image)
	if labelOn == labelOff {
		t.Fatalf("enabling PYTHON_ZUVLOOP must produce a different relay.bootstrap label (both %s)", labelOn)
	}

	// The mode tag labels for its own mode: a second enabled prepare reuses its
	// ref (a label match, no redundant build).
	preparedOn2, err := mOn.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare enabled again: %v", err)
	}
	if preparedOn2.Image != modeRef {
		t.Errorf("second enabled prepare ref = %s, want the mode-scoped tag %s", preparedOn2.Image, modeRef)
	}
	if got := imageBootstrapLabel(t, ctx, mOn, preparedOn2.Image); got != labelOn {
		t.Errorf("second enabled prepare label = %s, want the enabled label %s", got, labelOn)
	}

	// Flip back: the disabled worker probes its RAW ref and reuses the ORIGINAL
	// disabled image (same ref, same label), never the enabled mode's image.
	mOff2, err := newCoordinatedRealManager(t, testutil.DiscardLogger(), nil)
	if err != nil {
		t.Fatalf("new second disabled manager: %v", err)
	}
	defer func() { _ = mOff2.Close() }()
	preparedOff2, err := mOff2.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare flipped back: %v", err)
	}
	if preparedOff2.Image != rawRef || preparedOff2.Image != preparedOff.Image {
		t.Errorf("flipped-back image ref = %s, want the original disabled ref %s (raw %s)", preparedOff2.Image, preparedOff.Image, rawRef)
	}
	if got := imageBootstrapLabel(t, ctx, mOff2, preparedOff2.Image); got != labelOff {
		t.Errorf("flipped-back label = %s, want the original disabled label %s", got, labelOff)
	}

	// Tag separations never leak into the persisted identity: every handle
	// carries the raw source fingerprint, never a mode-scoped digest.
	fpMismatch := preparedOff.Fingerprint != fp || preparedOn.Fingerprint != fp ||
		preparedOn2.Fingerprint != fp || preparedOff2.Fingerprint != fp
	if fpMismatch {
		t.Errorf("Prepared.Fingerprint must stay the raw source fingerprint %s across all modes; got off=%s on=%s on2=%s off2=%s",
			fp, preparedOff.Fingerprint, preparedOn.Fingerprint, preparedOn2.Fingerprint, preparedOff2.Fingerprint)
	}

	// Service-only app: never mode-scoped — identical raw ref + label under
	// both modes, so flipping the setting must NOT rebuild its image.
	svcTemplate := &app.Template{
		Runtime: "python3.14",
		Services: []app.Service{{
			Name: "web", Entrypoint: "service.py", Port: 3000, Replicas: 1,
		}},
	}
	svcDir := fn.Dir
	writeFile(t, svcDir, "svc-handlers.py", "def noop(event):\n    pass\n")
	writeFile(t, svcDir, "service.py", "def main():\n    pass\n")
	svcFn := app.App{Name: "py-zuvloop-svc", Dir: svcDir, Template: svcTemplate}
	svcFp, err := app.FingerprintApp(svcFn.Dir, svcFn.Template)
	if err != nil {
		t.Fatalf("service-only fingerprint: %v", err)
	}
	svcRawRef := ImageRef(svcFn.Name, svcFp)

	svcOff, err := mOff.Prepare(ctx, svcFn)
	if err != nil {
		t.Fatalf("prepare service-only (disabled): %v", err)
	}
	if svcOff.Image != svcRawRef {
		t.Errorf("service-only disabled image ref = %s, want the raw source tag %s", svcOff.Image, svcRawRef)
	}
	labelSvcOff := imageBootstrapLabel(t, ctx, mOff, svcOff.Image)

	svcOn, err := mOn.Prepare(ctx, svcFn)
	if err != nil {
		t.Fatalf("prepare service-only (enabled): %v", err)
	}
	if svcOn.Image != svcRawRef || svcOn.Image != svcOff.Image {
		t.Errorf("service-only app image ref changed with the mode: %s vs %s (want the raw source tag %s)", svcOff.Image, svcOn.Image, svcRawRef)
	}
	labelSvcOn := imageBootstrapLabel(t, ctx, mOn, svcOn.Image)
	if labelSvcOn != labelSvcOff {
		t.Errorf("service-only app image label changed with the mode: %s vs %s (want identical)", labelSvcOff, labelSvcOn)
	}
	if svcOn.Fingerprint != svcFp || svcOff.Fingerprint != svcFp {
		t.Errorf("service-only Prepared.Fingerprint must stay %s; got off=%s on=%s", svcFp, svcOff.Fingerprint, svcOn.Fingerprint)
	}
}

// TestIntegrationPythonZuvloopDependencyImageUnchanged proves at the Docker
// level that the mode never churns the shared dependency layer: the same
// dep-bearing app prepared under both modes reports the SAME relay-dep-*
// dependency reference (the app image's FROM), so flipping the setting reuses
// the content-addressed dependency image instead of rebuilding it.
func TestIntegrationPythonZuvloopDependencyImageUnchanged(t *testing.T) {
	testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	writeFile(t, dir, "requirements.txt", "six==1.16.0\n")
	writeFile(t, dir, "handler.py", "def completed(event):\n    print('ok')\n")
	fn := app.App{
		Name: "py-zuvloop-deps",
		Dir:  dir,
		Template: &app.Template{
			Runtime: "python3.14",
			Events:  []app.EventRule{{Handler: "handler.completed"}},
		},
	}

	mOff, err := newCoordinatedRealManager(t, testutil.DiscardLogger(), nil)
	if err != nil {
		t.Fatalf("new disabled manager: %v", err)
	}
	defer func() { _ = mOff.Close() }()
	off, err := mOff.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare disabled: %v", err)
	}
	if off.Dependency == "" {
		t.Fatalf("dep-bearing app must report a dependency reference, got empty (image %s)", off.Image)
	}

	mOn, err := newCoordinatedRealManager(t, testutil.DiscardLogger(), nil, WithPythonZuvloop(true))
	if err != nil {
		t.Fatalf("new enabled manager: %v", err)
	}
	defer func() { _ = mOn.Close() }()
	on, err := mOn.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare enabled: %v", err)
	}
	if on.Dependency != off.Dependency {
		t.Errorf("dependency reference changed with the mode: %s vs %s (want identical)", off.Dependency, on.Dependency)
	}
}

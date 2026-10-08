//go:build integration

package runtime

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/client"

	"relay/internal/app"
	"relay/internal/observability/metrics"
	"relay/internal/runtime/node"
	"relay/internal/testutil"
)

// TestIntegrationSourceMountPythonReflectsSourceChange is the end-to-end proof
// of SOURCE_MOUNT for a Python app: the image is built WITHOUT the source, the
// live /apps tree is bind-mounted read-only, a source-only edit reuses the exact
// same image (no rebuild), and the next invocation runs the edited code because
// the advanced source fingerprint rotates the warm generation onto a fresh
// container.
//
// It runs natively (the app dir is a real host path the daemon can bind-mount),
// which is the deployment shape SOURCE_MOUNT requires.
func TestIntegrationSourceMountPythonReflectsSourceChange(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	writeFile(t, dir, "handler.py", "def run(event):\n    raise ValueError('V1')\n")

	fn := app.App{Name: "src-mount-it", Dir: dir, Template: &app.Template{Runtime: "python3.14"}}
	m, err := NewManager(testutil.DiscardLogger(), nil, "test-host", WithSourceMount(true))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()

	first, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer cleanupImage(cli, ctx, first.Image)

	event := []byte(`{"kind":"ping"}`)
	if err := m.Execute(ctx, first, "handler.run", event, nil); err == nil || !strings.Contains(err.Error(), "V1") {
		t.Fatalf("first execute error = %v, want it to mention V1", err)
	}

	// Edit the live source. The image must be reused (source is not baked) and
	// the advanced fingerprint must rotate the warm generation so the next
	// invocation runs the new code from the mount.
	writeFile(t, dir, "handler.py", "def run(event):\n    raise ValueError('V2')\n")

	second, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("re-prepare: %v", err)
	}
	if second.Image != first.Image {
		t.Fatalf("source-only change moved the image tag: %q -> %q", first.Image, second.Image)
	}
	if second.Fingerprint == first.Fingerprint {
		t.Fatal("source-only change must advance the content fingerprint")
	}
	if err := m.Execute(ctx, second, "handler.run", event, nil); err == nil || !strings.Contains(err.Error(), "V2") {
		t.Fatalf("second execute error = %v, want it to mention V2 (live source mount)", err)
	}
}

// TestIntegrationSourceMountNodeJSReflectsSourceChange is the end-to-end proof of
// SOURCE_MOUNT for Node JavaScript: the image is built WITHOUT the source, the
// live tree is bind-mounted read-only at the DISTINCT target /app/src (so
// /app/node_modules and the persisted esbuild stay visible), a source-only edit
// reuses the exact same image, and the next invocation runs the edited code while
// a real dependency resolves from the dependency image.
func TestIntegrationSourceMountNodeJSReflectsSourceChange(t *testing.T) {
	cli := testutil.RequireDocker(t)
	depBefore := depTagSet(context.Background(), cli)
	t.Cleanup(cleanupNewDepImagesSince(cli, depBefore))

	dir := t.TempDir()
	writeFile(t, dir, "package.json", `{
  "type": "module",
  "dependencies": {"picocolors": "^1.0.0"}
}`)
	writeFile(t, dir, "pnpm-lock.yaml", nodePnpmLockPicocolors)
	writeFile(t, dir, "index.js", `
import colors from "picocolors";
export function handler(event) {
  console.log(colors.red("js V1 " + event.event_id));
}
`)

	fn := app.App{Name: "src-mount-node-js-it", Dir: dir, Template: &app.Template{Runtime: "node24"}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	m, err := NewManager(testutil.DiscardLogger(), nil, "test-host", WithSourceMount(true))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()
	out := newAppOutputSink(t)

	first, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer cleanupImage(cli, ctx, first.Image)
	if first.Dependency == "" {
		t.Fatal("expected the Node app to build FROM a dependency image")
	}
	if err := m.Execute(ctx, first, "index.handler", []byte(`{"event_id":"1"}`), nil); err != nil {
		t.Fatalf("first execute: %v", err)
	}
	// picocolors.red wraps the text in ANSI escapes; match the inner text, which
	// also proves the bare import resolved from the dependency layer's
	// node_modules (the mount does not hide it).
	awaitAppOutput(t, ctx, out, "js V1 1")
	// The source-mounted image must explicitly CLEAR the relay.fingerprint label:
	// it is built FROM a dependency image that carries its own, and Docker
	// inherits labels through FROM. An inherited constant label would freeze the
	// warm generation and a source edit would never rotate containers.
	if insp, ierr := cli.ImageInspect(ctx, first.Image); ierr != nil {
		t.Fatalf("inspect built image: %v", ierr)
	} else if insp.Config == nil {
		t.Fatal("inspected image has no Config")
	} else if v := insp.Config.Labels[labelFingerprint]; v != "" {
		t.Fatalf("relay.fingerprint = %q, want it explicitly cleared on a source-mounted image", v)
	}

	// Source-only edit: the image must be reused (source is not baked) and the
	// advanced fingerprint must rotate the warm generation onto a fresh container
	// that imports the edited mounted code.
	writeFile(t, dir, "index.js", `
import colors from "picocolors";
export function handler(event) {
  console.log(colors.red("js V2 " + event.event_id));
}
`)
	second, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("re-prepare: %v", err)
	}
	if second.Image != first.Image {
		t.Fatalf("source-only change moved the image tag: %q -> %q", first.Image, second.Image)
	}
	if second.Fingerprint == first.Fingerprint {
		t.Fatal("source-only change must advance the content fingerprint")
	}
	if err := m.Execute(ctx, second, "index.handler", []byte(`{"event_id":"2"}`), nil); err != nil {
		t.Fatalf("second execute: %v", err)
	}
	awaitAppOutput(t, ctx, out, "js V2 2")
}

// TestIntegrationSourceMountNodeCJSReflectsSourceChange is the CommonJS
// counterpart of the JS test: the mounted app is a CommonJS package (no
// "type":"module") whose handler uses require(), and the shared resolve hook
// re-anchors that require to the dependency image's /app/node_modules, so a
// pnpm-installed CJS dependency resolves even though the mount carries the
// source. A source-only edit reuses the image and runs the edited code.
func TestIntegrationSourceMountNodeCJSReflectsSourceChange(t *testing.T) {
	cli := testutil.RequireDocker(t)
	depBefore := depTagSet(context.Background(), cli)
	t.Cleanup(cleanupNewDepImagesSince(cli, depBefore))

	dir := t.TempDir()
	writeFile(t, dir, "package.json", `{
  "dependencies": {"picocolors": "^1.0.0"}
}`)
	writeFile(t, dir, "pnpm-lock.yaml", nodePnpmLockPicocolors)
	writeFile(t, dir, "index.js", `
const colors = require("picocolors");
exports.handler = function (event) {
  console.log(colors.red("cjs V1 " + event.event_id));
};
`)

	fn := app.App{Name: "src-mount-node-cjs-it", Dir: dir, Template: &app.Template{Runtime: "node24"}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	m, err := NewManager(testutil.DiscardLogger(), nil, "test-host", WithSourceMount(true))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()
	out := newAppOutputSink(t)

	first, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer cleanupImage(cli, ctx, first.Image)
	if first.Dependency == "" {
		t.Fatal("expected the Node app to build FROM a dependency image")
	}
	if err := m.Execute(ctx, first, "index.handler", []byte(`{"event_id":"1"}`), nil); err != nil {
		t.Fatalf("first execute: %v", err)
	}
	awaitAppOutput(t, ctx, out, "cjs V1 1")

	// Source-only edit: the image is reused (the source is mounted) and the
	// advanced fingerprint rotates the warm generation onto the edited code.
	writeFile(t, dir, "index.js", `
const colors = require("picocolors");
exports.handler = function (event) {
  console.log(colors.red("cjs V2 " + event.event_id));
};
`)
	second, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("re-prepare: %v", err)
	}
	if second.Image != first.Image {
		t.Fatalf("source-only change moved the image tag: %q -> %q", first.Image, second.Image)
	}
	if err := m.Execute(ctx, second, "index.handler", []byte(`{"event_id":"2"}`), nil); err != nil {
		t.Fatalf("second execute: %v", err)
	}
	awaitAppOutput(t, ctx, out, "cjs V2 2")
}

// TestIntegrationSourceMountNodeTSReflectsSourceChange is the end-to-end proof of
// SOURCE_MOUNT for Node TypeScript: the image is built WITHOUT the source and
// carries a PERSISTENT pinned esbuild, the live .ts graph is bind-mounted at
// /app/src, and the bootstrap bundles it at container startup. A source-only edit
// reuses the same image and the next invocation runs the re-bundled code, with
// bare packages resolving from the dependency image.
func TestIntegrationSourceMountNodeTSReflectsSourceChange(t *testing.T) {
	cli := testutil.RequireDocker(t)
	depBefore := depTagSet(context.Background(), cli)
	t.Cleanup(cleanupNewDepImagesSince(cli, depBefore))

	dir := t.TempDir()
	writeFile(t, dir, "package.json", `{
  "type": "module",
  "dependencies": {"picocolors": "^1.0.0"}
}`)
	writeFile(t, dir, "pnpm-lock.yaml", nodePnpmLockPicocolors)
	writeFile(t, dir, "message.ts", `
export function message(event: { event_id: string }): string {
  return "ts V1 " + event.event_id;
}
`)
	writeFile(t, dir, "index.ts", `
import colors from "picocolors";
import { context } from "@opentelemetry/api";
import { message } from "./message";
export function handler(event: { event_id: string }): void {
  console.log(colors.red(message(event)) + " otel=" + (typeof context.active === "function"));
}
`)

	tmpl := &app.Template{Runtime: "node24"}
	tmpl.Events = append(tmpl.Events, app.EventRule{Handler: "index.handler"})
	fn := app.App{Name: "src-mount-node-ts-it", Dir: dir, Template: tmpl}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	m, err := NewManager(testutil.DiscardLogger(), nil, "test-host", WithSourceMount(true))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()
	out := newAppOutputSink(t)

	first, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer cleanupImage(cli, ctx, first.Image)
	if first.Dependency == "" {
		t.Fatal("expected the Node app to build FROM a dependency image")
	}
	if err := m.Execute(ctx, first, "index.handler", []byte(`{"event_id":"1"}`), nil); err != nil {
		t.Fatalf("first execute: %v", err)
	}
	// "otel=true" proves the bundle's external @opentelemetry/api import resolved
	// from the managed /app/node_modules through the generated overlay's symlink.
	awaitAppOutput(t, ctx, out, "ts V1 1 otel=true")

	// Edit the local .ts module graph. The image is reused and the fresh container
	// re-bundles from the mount.
	writeFile(t, dir, "message.ts", `
export function message(event: { event_id: string }): string {
  return "ts V2 " + event.event_id;
}
`)
	second, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("re-prepare: %v", err)
	}
	if second.Image != first.Image {
		t.Fatalf("source-only change moved the image tag: %q -> %q", first.Image, second.Image)
	}
	if second.Fingerprint == first.Fingerprint {
		t.Fatal("source-only change must advance the content fingerprint")
	}
	if err := m.Execute(ctx, second, "index.handler", []byte(`{"event_id":"2"}`), nil); err != nil {
		t.Fatalf("second execute: %v", err)
	}
	awaitAppOutput(t, ctx, out, "ts V2 2")
}

// TestIntegrationSourceMountNodeTSSelfReference proves a mounted TypeScript
// handler can import its OWN package by name end to end. --packages=external
// emits the self-reference as an external import, and the generated bundle under
// /tmp has no package scope to resolve it (no package.json in the generated
// tree); the shared resolve hook intercepts it and resolves it through the
// mounted app manifest's own "exports" with native Node semantics. The exports
// map deliberately DIVERGES from the filesystem layout ("./lib/util" points at a
// nested ./lib/impl/util.ts while a decoy ./lib/util.ts exists), so a
// filesystem-layout alias would bundle the decoy. A source-only edit of the
// referenced module is re-bundled from the mount and reuses the same
// source-independent image.
func TestIntegrationSourceMountNodeTSSelfReference(t *testing.T) {
	cli := testutil.RequireDocker(t)
	depBefore := depTagSet(context.Background(), cli)
	t.Cleanup(cleanupNewDepImagesSince(cli, depBefore))

	dir := t.TempDir()
	writeFile(t, dir, "package.json", `{
  "name": "selfref-app",
  "type": "module",
  "dependencies": {"picocolors": "^1.0.0"},
  "exports": {"./lib/util": "./lib/impl/util.ts"}
}`)
	writeFile(t, dir, "pnpm-lock.yaml", nodePnpmLockPicocolors)
	if err := os.MkdirAll(filepath.Join(dir, "lib", "impl"), 0o755); err != nil {
		t.Fatalf("mkdir lib/impl: %v", err)
	}
	// A decoy at the layout path the old alias approximation would have chosen.
	writeFile(t, dir, "lib/util.ts", `
export function where(): string {
  return "DECOY LAYOUT";
}
`)
	writeFile(t, dir, "lib/impl/util.ts", `
export function where(): string {
  return "SELF V1";
}
`)
	writeFile(t, dir, "index.ts", `
import colors from "picocolors";
import { where } from "selfref-app/lib/util";
export function handler(event: { event_id: string }): void {
  console.log(colors.red(where() + " " + event.event_id));
}
`)

	tmpl := &app.Template{Runtime: "node24"}
	tmpl.Events = append(tmpl.Events, app.EventRule{Handler: "index.handler"})
	fn := app.App{Name: "src-mount-node-ts-selfref-it", Dir: dir, Template: tmpl}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	m, err := NewManager(testutil.DiscardLogger(), nil, "test-host", WithSourceMount(true))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()
	out := newAppOutputSink(t)

	first, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer cleanupImage(cli, ctx, first.Image)
	if first.Dependency == "" {
		t.Fatal("expected the Node app to build FROM a dependency image")
	}
	if err := m.Execute(ctx, first, "index.handler", []byte(`{"event_id":"1"}`), nil); err != nil {
		t.Fatalf("first execute: %v", err)
	}
	logs := awaitAppOutput(t, ctx, out, "SELF V1 1")
	if strings.Contains(logs, "DECOY LAYOUT") {
		t.Fatalf("self-reference resolved by filesystem layout instead of the manifest exports:\n%s", logs)
	}

	// Source-only edit of the self-referenced module: the image is reused and the
	// fresh container re-bundles from the mount.
	writeFile(t, dir, "lib/impl/util.ts", `
export function where(): string {
  return "SELF V2";
}
`)
	second, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("re-prepare: %v", err)
	}
	if second.Image != first.Image {
		t.Fatalf("source-only change moved the image tag: %q -> %q", first.Image, second.Image)
	}
	if second.Fingerprint == first.Fingerprint {
		t.Fatal("source-only change must advance the content fingerprint")
	}
	if err := m.Execute(ctx, second, "index.handler", []byte(`{"event_id":"2"}`), nil); err != nil {
		t.Fatalf("second execute: %v", err)
	}
	awaitAppOutput(t, ctx, out, "SELF V2 2")
}

// TestIntegrationSourceMountNodeHostNodeModulesDoesNotShadow is the end-to-end
// proof that a host app directory containing node_modules cannot shadow the
// dependency image under SOURCE_MOUNT. The host ships a deliberately conflicting
// `picocolors` stub that returns a sentinel string; the dependency image installs
// the real `picocolors`. The mounted handler imports it and prints the result, so
// the output proves Node resolved the bare import from /app/node_modules (the
// dependency image) rather than /app/src/node_modules (the masked host stub).
func TestIntegrationSourceMountNodeHostNodeModulesDoesNotShadow(t *testing.T) {
	cli := testutil.RequireDocker(t)
	depBefore := depTagSet(context.Background(), cli)
	t.Cleanup(cleanupNewDepImagesSince(cli, depBefore))

	dir := t.TempDir()
	writeFile(t, dir, "package.json", `{
  "type": "module",
  "dependencies": {"picocolors": "^1.0.0"}
}`)
	writeFile(t, dir, "pnpm-lock.yaml", nodePnpmLockPicocolors)
	writeFile(t, dir, "index.js", `
import colors from "picocolors";
export function handler(event) {
  console.log("shadow=" + colors.red("V1") + " id=" + event.event_id);
}
`)
	// The conflicting host package: same name as the dependency, different
	// behavior. If Node's /app/src/node_modules were visible, this would win.
	if err := os.MkdirAll(filepath.Join(dir, "node_modules", "picocolors"), 0o755); err != nil {
		t.Fatalf("mkdir host node_modules: %v", err)
	}
	writeFile(t, dir, "node_modules/picocolors/package.json", `{"name":"picocolors","version":"9.9.9","main":"index.cjs"}`)
	writeFile(t, dir, "node_modules/picocolors/index.cjs", `module.exports = { red: function () { return "HOST_MODULE_SHADOW"; } };`)

	fn := app.App{Name: "src-mount-shadow-it", Dir: dir, Template: &app.Template{Runtime: "node24"}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	m, err := NewManager(testutil.DiscardLogger(), nil, "test-host", WithSourceMount(true))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()
	out := newAppOutputSink(t)

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer cleanupImage(cli, ctx, prepared.Image)
	if prepared.Dependency == "" {
		t.Fatal("expected the Node app to build FROM a dependency image")
	}
	if err := m.Execute(ctx, prepared, "index.handler", []byte(`{"event_id":"1"}`), nil); err != nil {
		t.Fatalf("execute: %v", err)
	}
	logs := awaitAppOutput(t, ctx, out, "shadow=", "V1")
	if strings.Contains(logs, "HOST_MODULE_SHADOW") {
		t.Fatalf("host node_modules shadowed the dependency image:\n%s", logs)
	}
}

// TestIntegrationSourceMountNodeLateHostNodeModulesDoesNotShadow is the
// end-to-end proof that the bootstrap's ESM resolve hook keeps the dependency
// image authoritative even when a host node_modules appears AFTER preparation.
// Preparation sees no host node_modules, so the plan emits no /app/src/node_modules
// mask (Docker cannot create one inside the read-only source bind). The test then
// creates a conflicting top-level node_modules package that .gitignore excludes
// from the selected source, so the source fingerprint is unchanged and no
// reconcile occurs. Without the hook Node would find the host stub at
// /app/src/node_modules BEFORE the dependency image; the hook re-anchors the bare
// import to /app/node_modules, so the real dependency package wins.
func TestIntegrationSourceMountNodeLateHostNodeModulesDoesNotShadow(t *testing.T) {
	cli := testutil.RequireDocker(t)
	depBefore := depTagSet(context.Background(), cli)
	t.Cleanup(cleanupNewDepImagesSince(cli, depBefore))

	dir := t.TempDir()
	// node_modules is excluded from the selected source, so creating it after
	// preparation must not move the fingerprint.
	writeFile(t, dir, ".gitignore", "node_modules/\n")
	writeFile(t, dir, "package.json", `{
  "type": "module",
  "dependencies": {"picocolors": "^1.0.0"}
}`)
	writeFile(t, dir, "pnpm-lock.yaml", nodePnpmLockPicocolors)
	writeFile(t, dir, "index.js", `
import colors from "picocolors";
export function handler(event) {
  console.log("late=" + colors.red("V1") + " id=" + event.event_id);
}
`)

	fn := app.App{Name: "src-mount-late-it", Dir: dir, Template: &app.Template{Runtime: "node24"}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	fpBefore, err := app.FingerprintApp(dir, fn.Template)
	if err != nil {
		t.Fatalf("fingerprint before prepare: %v", err)
	}

	m, err := NewManager(testutil.DiscardLogger(), nil, "test-host", WithSourceMount(true))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()
	out := newAppOutputSink(t)

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer cleanupImage(cli, ctx, prepared.Image)
	if prepared.Dependency == "" {
		t.Fatal("expected the Node app to build FROM a dependency image")
	}
	if prepared.Fingerprint != fpBefore {
		t.Fatalf("prepared fingerprint = %q, want the selected-source fingerprint %q", prepared.Fingerprint, fpBefore)
	}
	// Preparation saw no host node_modules, so no mask was emitted: the scenario
	// under test is exactly the unmasked seed.
	if sm, ok := m.sourceMountFor(fn.Name); !ok || len(sm.Masks) != 0 {
		t.Fatalf("recorded mount = %+v (present=%v), want no masks before node_modules exists", sm, ok)
	}

	// Create the conflicting host package AFTER preparation. .gitignore excludes
	// it, so the selected-source fingerprint is unchanged and no reconcile runs;
	// the container is created later against the unmasked mount.
	if err := os.MkdirAll(filepath.Join(dir, "node_modules", "picocolors"), 0o755); err != nil {
		t.Fatalf("mkdir late host node_modules: %v", err)
	}
	writeFile(t, dir, "node_modules/picocolors/package.json", `{"name":"picocolors","version":"9.9.9","main":"index.cjs"}`)
	writeFile(t, dir, "node_modules/picocolors/index.cjs", `module.exports = { red: function () { return "LATE_HOST_MODULE_SHADOW"; } };`)

	fpAfter, err := app.FingerprintApp(dir, fn.Template)
	if err != nil {
		t.Fatalf("fingerprint after late node_modules: %v", err)
	}
	if fpAfter != fpBefore {
		t.Fatalf("creating an ignored node_modules moved the source fingerprint: %q -> %q", fpBefore, fpAfter)
	}

	if err := m.Execute(ctx, prepared, "index.handler", []byte(`{"event_id":"1"}`), nil); err != nil {
		t.Fatalf("execute: %v", err)
	}
	logs := awaitAppOutput(t, ctx, out, "late=", "V1")
	if strings.Contains(logs, "LATE_HOST_MODULE_SHADOW") {
		t.Fatalf("late host node_modules shadowed the dependency image:\n%s", logs)
	}
}

// TestIntegrationSourceMountNodeWorkingDirIsAppRoot is the end-to-end proof that
// a SOURCE_MOUNT Node execution container runs with the app's source root as its
// working directory: the mounted handler observes process.cwd() == /app/src and
// resolves a relative file path against the mounted app root.
func TestIntegrationSourceMountNodeWorkingDirIsAppRoot(t *testing.T) {
	cli := testutil.RequireDocker(t)

	dir := t.TempDir()
	writeFile(t, dir, "package.json", `{"type":"module"}`)
	writeFile(t, dir, "pnpm-lock.yaml", nodePnpmLockEmpty)
	writeFile(t, dir, "data.txt", "relative-ok\n")
	writeFile(t, dir, "index.js", `
import { readFileSync } from "node:fs";
export function handler(event) {
  console.log("cwd=" + process.cwd());
  console.log("data=" + readFileSync("./data.txt", "utf8").trim());
}
`)

	fn := app.App{Name: "src-mount-cwd-it", Dir: dir, Template: &app.Template{Runtime: "node24"}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	m, err := NewManager(testutil.DiscardLogger(), nil, "test-host", WithSourceMount(true))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()
	out := newAppOutputSink(t)

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer cleanupImage(cli, ctx, prepared.Image)
	if err := m.Execute(ctx, prepared, "index.handler", []byte(`{}`), nil); err != nil {
		t.Fatalf("execute: %v", err)
	}
	awaitAppOutput(t, ctx, out, "cwd=/app/src", "data=relative-ok")
}

// TestIntegrationSourceMountNodeServiceLateHostNodeModulesDoesNotShadow is the
// end-to-end proof that a mounted Node entrypoint service keeps the dependency
// image authoritative even when a conflicting host node_modules appears AFTER
// preparation. Preparation sees no host node_modules, so the plan emits no
// /app/src/node_modules mask (Docker cannot create one inside the read-only
// source bind). The test then creates a conflicting host package excluded by
// .gitignore, so the source fingerprint is unchanged and no reconcile occurs.
// The service process runs via `node --import /relay/resolve-hook.mjs
// /app/src/app/service.js` (it never runs the invocation bootstrap), and the
// preloaded shared hook re-anchors its bare import to /app/node_modules, so the
// real dependency package wins over the host stub.
func TestIntegrationSourceMountNodeServiceLateHostNodeModulesDoesNotShadow(t *testing.T) {
	cli := testutil.RequireDocker(t)
	depBefore := depTagSet(context.Background(), cli)
	t.Cleanup(cleanupNewDepImagesSince(cli, depBefore))

	dir := t.TempDir()
	// node_modules is excluded from the selected source, so creating it after
	// preparation must not move the fingerprint.
	writeFile(t, dir, ".gitignore", "node_modules/\n")
	writeFile(t, dir, "package.json", `{
  "type": "module",
  "dependencies": {"picocolors": "^1.0.0"}
}`)
	writeFile(t, dir, "pnpm-lock.yaml", nodePnpmLockPicocolors)
	if err := os.MkdirAll(filepath.Join(dir, "app"), 0o755); err != nil {
		t.Fatalf("mkdir app: %v", err)
	}
	writeFile(t, dir, "app/service.js", `
import colors from "picocolors";
process.on("SIGTERM", () => process.exit(0));
console.log("svc-shadow=" + colors.red("V1"));
setInterval(() => {}, 1 << 30);
`)

	appName := "src-mount-svc-shadow-it"
	fn := app.App{Name: appName, Dir: dir, Template: &app.Template{Runtime: "node24"}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	fpBefore, err := app.FingerprintApp(dir, fn.Template)
	if err != nil {
		t.Fatalf("fingerprint before prepare: %v", err)
	}

	m, err := NewManager(testutil.DiscardLogger(), nil, "test-host", WithSourceMount(true))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()
	t.Cleanup(func() {
		cc, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_, _ = m.RemoveAppServiceContainers(cc, appName)
		cleanupImagePrefixes(cli, "relay-app-"+appName+":")()
	})

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer cleanupImage(cli, ctx, prepared.Image)
	if prepared.Dependency == "" {
		t.Fatal("expected the Node app to build FROM a dependency image")
	}
	// Preparation saw no host node_modules, so no mask was emitted: the scenario
	// under test is exactly the unmasked seed, which only the preloaded hook can
	// defend.
	if sm, ok := m.sourceMountFor(fn.Name); !ok || len(sm.Masks) != 0 {
		t.Fatalf("recorded mount = %+v (present=%v), want no masks before node_modules exists", sm, ok)
	}

	// Create the conflicting host package AFTER preparation. .gitignore excludes
	// it, so the selected-source fingerprint is unchanged and no reconcile runs.
	if err := os.MkdirAll(filepath.Join(dir, "node_modules", "picocolors"), 0o755); err != nil {
		t.Fatalf("mkdir late host node_modules: %v", err)
	}
	writeFile(t, dir, "node_modules/picocolors/package.json", `{"name":"picocolors","version":"9.9.9","main":"index.cjs"}`)
	writeFile(t, dir, "node_modules/picocolors/index.cjs", `module.exports = { red: function () { return "SVC_HOST_MODULE_SHADOW"; } };`)

	fpAfter, err := app.FingerprintApp(dir, fn.Template)
	if err != nil {
		t.Fatalf("fingerprint after late node_modules: %v", err)
	}
	if fpAfter != fpBefore {
		t.Fatalf("creating an ignored node_modules moved the source fingerprint: %q -> %q", fpBefore, fpAfter)
	}

	tmpl := &app.Template{Runtime: "node24"}
	svc := app.Service{Name: "svc", Entrypoint: "app/service.js", Port: 3000}
	resolved, err := m.ResolveServiceImage(ctx, fn.Name, tmpl, svc, prepared.Image)
	if err != nil {
		t.Fatalf("resolve service image: %v", err)
	}
	if resolved.Mount == nil || resolved.Mount.WorkDir != "/app/src" {
		t.Fatalf("resolved mount = %+v, want a /app/src mount with work dir /app/src", resolved.Mount)
	}
	if len(resolved.Entry) != 4 || resolved.Entry[0] != "node" ||
		resolved.Entry[1] != "--import" || resolved.Entry[2] != node.ResolveHookPath ||
		resolved.Entry[3] != "/app/src/app/service.js" {
		t.Fatalf("service entry = %v, want [node --import %s /app/src/app/service.js]", resolved.Entry, node.ResolveHookPath)
	}

	id, err := m.StartService(ctx, ServiceSpec{
		App: appName, Name: svc.Name, SourceRef: svc.Entrypoint,
		Port: svc.Port, Image: resolved.Ref, ImageID: resolved.ID,
		Entry: resolved.Entry, Env: []string{"PORT=3000"},
		SourceMount: resolved.Mount,
	}, 0)
	if err != nil {
		t.Fatalf("start service: %v", err)
	}
	waitForContainerRunning(t, ctx, cli, id)

	const proof = "svc-shadow="
	var logs string
	if !pollUntil(ctx, 30*time.Second, func() bool {
		rc, lerr := cli.ContainerLogs(ctx, id, client.ContainerLogsOptions{ShowStdout: true, ShowStderr: true})
		if lerr != nil {
			t.Fatalf("logs: %v", lerr)
		}
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, rc)
		rc.Close()
		logs = buf.String()
		return strings.Contains(logs, proof)
	}) {
		t.Fatalf("service logs do not show the dependency-resolved output within 30s, got:\n%s", logs)
	}
	if strings.Contains(logs, "SVC_HOST_MODULE_SHADOW") {
		t.Fatalf("late host node_modules shadowed the dependency image in the service:\n%s", logs)
	}
}

// TestIntegrationSourceMountNodeServiceWorkingDir proves a managed entrypoint
// service under SOURCE_MOUNT runs with the same app-root working directory as the
// pooled runtime: its container WorkingDir is /app/src and the service process
// observes process.cwd() == /app/src with relative paths resolving against the
// mounted app root.
func TestIntegrationSourceMountNodeServiceWorkingDir(t *testing.T) {
	cli := testutil.RequireDocker(t)

	dir := t.TempDir()
	writeFile(t, dir, "package.json", `{"type":"module"}`)
	writeFile(t, dir, "pnpm-lock.yaml", nodePnpmLockEmpty)
	writeFile(t, dir, "data.txt", "svc-relative-ok\n")
	if err := os.MkdirAll(filepath.Join(dir, "app"), 0o755); err != nil {
		t.Fatalf("mkdir app: %v", err)
	}
	writeFile(t, dir, "app/service.js", `
import { readFileSync } from "node:fs";
process.on("SIGTERM", () => process.exit(0));
console.log("svc-cwd=" + process.cwd());
console.log("svc-data=" + readFileSync("./data.txt", "utf8").trim());
setInterval(() => {}, 1 << 30);
`)

	appName := "src-mount-svc-it"
	fn := app.App{Name: appName, Dir: dir, Template: &app.Template{Runtime: "node24"}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	m, err := NewManager(testutil.DiscardLogger(), nil, "test-host", WithSourceMount(true))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()
	t.Cleanup(func() {
		cc, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_, _ = m.RemoveAppServiceContainers(cc, appName)
		cleanupImagePrefixes(cli, "relay-app-"+appName+":")()
	})

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer cleanupImage(cli, ctx, prepared.Image)

	tmpl := &app.Template{Runtime: "node24"}
	svc := app.Service{Name: "svc", Entrypoint: "app/service.js", Port: 3000}
	resolved, err := m.ResolveServiceImage(ctx, fn.Name, tmpl, svc, prepared.Image)
	if err != nil {
		t.Fatalf("resolve service image: %v", err)
	}
	if resolved.Mount == nil || resolved.Mount.WorkDir != "/app/src" {
		t.Fatalf("resolved mount = %+v, want a /app/src mount with work dir /app/src", resolved.Mount)
	}
	if len(resolved.Entry) != 4 || resolved.Entry[0] != "node" ||
		resolved.Entry[1] != "--import" || resolved.Entry[2] != node.ResolveHookPath ||
		resolved.Entry[3] != "/app/src/app/service.js" {
		t.Fatalf("service entry = %v, want [node --import %s /app/src/app/service.js]", resolved.Entry, node.ResolveHookPath)
	}

	id, err := m.StartService(ctx, ServiceSpec{
		App: appName, Name: svc.Name, SourceRef: svc.Entrypoint,
		Port: svc.Port, Image: resolved.Ref, ImageID: resolved.ID,
		Entry: resolved.Entry, Env: []string{"PORT=3000"},
		SourceMount: resolved.Mount,
	}, 0)
	if err != nil {
		t.Fatalf("start service: %v", err)
	}
	waitForContainerRunning(t, ctx, cli, id)
	insp, err := cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if insp.Container.Config == nil || insp.Container.Config.WorkingDir != "/app/src" {
		t.Fatalf("service WorkingDir = %+v, want /app/src", insp.Container.Config)
	}
	const proof = "svc-cwd=/app/src"
	var logs string
	if !pollUntil(ctx, 30*time.Second, func() bool {
		rc, lerr := cli.ContainerLogs(ctx, id, client.ContainerLogsOptions{ShowStdout: true, ShowStderr: true})
		if lerr != nil {
			t.Fatalf("logs: %v", lerr)
		}
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, rc)
		rc.Close()
		logs = buf.String()
		return strings.Contains(logs, proof) && strings.Contains(logs, "svc-data=svc-relative-ok")
	}) {
		t.Fatalf("service logs do not show the cwd + relative-path proof within 30s, got:\n%s", logs)
	}
}

// TestIntegrationSourceMountNodeDependencyChangeBuildsAndExecutes verifies the
// SOURCE_MOUNT dependency lifecycle end to end: mutating the package manifest
// changes the dependency fingerprint/reference, produces a NEW dependency image
// generation on the daemon, and a replacement invocation resolves a package that
// exists ONLY in the new dependency layer while running the current mounted
// source.
func TestIntegrationSourceMountNodeDependencyChangeBuildsAndExecutes(t *testing.T) {
	cli := testutil.RequireDocker(t)
	depBefore := depTagSet(context.Background(), cli)
	t.Cleanup(cleanupNewDepImagesSince(cli, depBefore))

	dir := t.TempDir()
	writeFile(t, dir, "package.json", `{
  "type": "module",
  "dependencies": {"picocolors": "^1.0.0"}
}`)
	writeFile(t, dir, "pnpm-lock.yaml", nodePnpmLockPicocolors)
	writeFile(t, dir, "index.js", `
import colors from "picocolors";
export function handler(event) {
  console.log(colors.green("dep-v1 " + event.event_id));
}
`)

	fn := app.App{Name: "src-mount-dep-it", Dir: dir, Template: &app.Template{Runtime: "node24"}}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	m, err := NewManager(testutil.DiscardLogger(), nil, "test-host", WithSourceMount(true))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()
	out := newAppOutputSink(t)

	first, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	defer cleanupImage(cli, ctx, first.Image)
	if first.Dependency == "" {
		t.Fatal("expected the Node app to build FROM a dependency image")
	}
	if err := m.Execute(ctx, first, "index.handler", []byte(`{"event_id":"1"}`), nil); err != nil {
		t.Fatalf("first execute: %v", err)
	}
	awaitAppOutput(t, ctx, out, "dep-v1 1")

	// Change the manifest (add a new dependency) AND the source (import it).
	writeFile(t, dir, "package.json", `{
  "type": "module",
  "dependencies": {"picocolors": "^1.0.0", "ms": "2.1.3"}
}`)
	writeFile(t, dir, "pnpm-lock.yaml", nodePnpmLockPicocolorsMs)
	writeFile(t, dir, "index.js", `
import colors from "picocolors";
import ms from "ms";
export function handler(event) {
  console.log(colors.green("dep-v2 " + event.event_id + " ms=" + ms(1500)));
}
`)

	second, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("second prepare: %v", err)
	}
	defer cleanupImage(cli, ctx, second.Image)
	if second.Dependency == first.Dependency {
		t.Fatalf("manifest change kept the dependency reference %q; want a new generation", first.Dependency)
	}
	if want := expectedDependencyRef(t, fn); second.Dependency != want {
		t.Fatalf("dependency ref = %q, want %q (the current manifest's content address)", second.Dependency, want)
	}
	if !imageExistsInDaemon(cli, ctx, second.Dependency) {
		t.Fatalf("new dependency image %q was not built on the daemon", second.Dependency)
	}
	if err := m.Execute(ctx, second, "index.handler", []byte(`{"event_id":"2"}`), nil); err != nil {
		t.Fatalf("second execute: %v", err)
	}
	// ms(1500) => "2s"; its resolution proves the new dependency image is in
	// use, and "dep-v2 2" proves the replacement ran the current mounted source.
	awaitAppOutput(t, ctx, out, "dep-v2 2 ms=2s")
}

// sourceMountGateClosed and sourceMountGateOpen are the gate-file contents a
// blocked handler waits on. The gate is listed in .gitignore, so it is excluded
// from the selected source and flipping it releases a handler WITHOUT advancing
// the app fingerprint, keeping the release independent of the generation under
// test. It is still visible in the container because SOURCE_MOUNT binds the
// whole app directory read-only (fn.Dir), not merely the selected source.
const (
	sourceMountGateClosed = "closed"
	sourceMountGateOpen   = "release"
)

// writeSourceMountGateApp writes a SOURCE_MOUNT Node app whose v1 handler prints
// "START v1" and then blocks until the git-ignored gate file is released. A test
// flips <dir>/gate.txt to sourceMountGateOpen to complete the invocation
// deterministically (no sleep), and swaps index.js for the ungated v2 handler
// (writeSourceMountGateAppV2) to advance the source generation while the v1
// invocation is still in flight. It returns the app directory.
func writeSourceMountGateApp(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	writeFile(t, dir, ".gitignore", "gate.txt\n")
	writeFile(t, dir, "package.json", `{"type":"module"}`)
	writeFile(t, dir, "pnpm-lock.yaml", nodePnpmLockEmpty)
	writeFile(t, dir, "gate.txt", sourceMountGateClosed)
	writeFile(t, dir, "index.js", `
import { readFileSync } from "node:fs";
export async function run(event) {
  console.log("START v1");
  while (readFileSync("gate.txt", "utf8").trim() !== "`+sourceMountGateOpen+`") {
    await new Promise((r) => setTimeout(r, 5));
  }
  console.log("END v1");
}
`)
	return dir
}

// writeSourceMountGateAppV2 swaps the fixture's handler for an ungated v2 that
// returns at once, so a source-only edit advances the content fingerprint while
// the source-independent image tag stays put.
func writeSourceMountGateAppV2(t *testing.T, dir string) {
	t.Helper()
	writeFile(t, dir, "index.js", `
export function run(event) {
  console.log("v2 " + event.event_id);
}
`)
}

// releaseSourceMountGate flips the git-ignored gate file to the release value,
// deterministically completing a handler blocked in writeSourceMountGateApp.
func releaseSourceMountGate(t *testing.T, dir string) {
	t.Helper()
	writeFile(t, dir, "gate.txt", sourceMountGateOpen)
}

// releaseSourceMountGateOnCleanup registers a best-effort gate release so a
// failed assertion can never leave a handler blocked past the test (and so
// Manager.Close, registered earlier, runs only after the gate is open — t.Cleanup
// is LIFO). It writes directly rather than through writeFile, which must not
// t.Fatalf during cleanup.
func releaseSourceMountGateOnCleanup(t *testing.T, dir string) {
	t.Helper()
	t.Cleanup(func() {
		_ = os.WriteFile(filepath.Join(dir, "gate.txt"), []byte(sourceMountGateOpen), 0o644)
	})
}

// waitForContainerIDGone polls until the container with the exact id is gone (a
// not-found inspect), or the deadline passes. Unlike waitForContainerGone it
// targets ONE container, so a drained generation's container can be proven
// discarded even while another container for the same app remains.
func waitForContainerIDGone(ctx context.Context, cli *client.Client, id string) bool {
	return pollUntil(ctx, 15*time.Second, func() bool {
		_, err := cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
		return err != nil && cerrdefs.IsNotFound(err)
	})
}

// newSourceMountWarmManager builds a SOURCE_MOUNT Manager with a fresh metrics
// registry and an explicit worker-global warm-container bound
// (MAX_WARM_CONTAINERS), so a test can assert the global warm count and the
// warm-budget backpressure counter end to end. Close is registered as cleanup.
func newSourceMountWarmManager(t *testing.T, maxWarm int) (*Manager, *metrics.Registry) {
	t.Helper()
	reg := metrics.New()
	m, err := NewManager(
		testutil.DiscardLogger(),
		reg,
		"test-host",
		WithSourceMount(true),
		WithMaxWarmContainers(maxWarm),
	)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m, reg
}

// TestIntegrationSourceMountBusyGenerationAdvanceDrains is the end-to-end proof
// that a busy source-mounted invocation is NOT abruptly terminated when a
// source-only edit advances the generation: the old container completes under
// its admitted generation, retires on release, and a later invocation runs the
// updated mounted source. The busy handler blocks on a git-ignored gate file the
// test releases deterministically, so the scenario is driven by an output gate
// (START v1) and a controlled release rather than a sleep.
func TestIntegrationSourceMountBusyGenerationAdvanceDrains(t *testing.T) {
	cli := testutil.RequireDocker(t)
	dir := writeSourceMountGateApp(t)

	fn := app.App{Name: "src-mount-busy-it", Dir: dir,
		Template: &app.Template{Runtime: "node24", Concurrency: 2}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	m, _ := newSourceMountWarmManager(t, DefaultMaxWarmContainers)
	releaseSourceMountGateOnCleanup(t, dir)
	sink := &pollingSink{}
	prev := SetAppOutput(sink)
	t.Cleanup(func() { SetAppOutput(prev) })

	first, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare v1: %v", err)
	}
	defer cleanupImage(cli, ctx, first.Image)

	// Start the v1 invocation and hold it busy on the gate.
	v1Ctx := context.WithValue(context.Background(), runMetaKey{},
		RunMeta{Hostname: "test-host", App: fn.Name, Handler: "index.run", Image: first.Image})
	v1Done := make(chan error, 1)
	go func() { v1Done <- m.Execute(v1Ctx, first, "index.run", []byte(`{"event_id":"1"}`), nil) }()

	// Output gate: START v1 means the handler is mid-flight and its container is
	// leased (busy).
	if !waitForSinkContains(ctx, sink, "START v1") {
		t.Fatalf("v1 did not start; sink:\n%s", sink.String())
	}
	id1 := reusedContainerID(t, ctx, m, fn.Name)
	if id1 == "" {
		t.Fatal("expected the busy v1 container to be running")
	}

	// Advance the source generation WHILE v1 is busy: a source-only edit reuses
	// the same image but moves the content fingerprint, which rotates the warm
	// generation at the next acquire.
	writeSourceMountGateAppV2(t, dir)
	second, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("re-prepare: %v", err)
	}
	if second.Image != first.Image {
		t.Fatalf("source-only change moved the image tag: %q -> %q", first.Image, second.Image)
	}
	if second.Fingerprint == first.Fingerprint {
		t.Fatal("source-only change must advance the content fingerprint")
	}

	// The advanced generation's invocation runs on its own container while the
	// busy v1 drains; the old container is never discarded mid-invocation.
	v2Ctx := context.WithValue(context.Background(), runMetaKey{},
		RunMeta{Hostname: "test-host", App: fn.Name, Handler: "index.run", Image: second.Image})
	if err := m.Execute(v2Ctx, second, "index.run", []byte(`{"event_id":"2"}`), nil); err != nil {
		t.Fatalf("execute v2 while v1 busy: %v", err)
	}
	if !waitForSinkContains(ctx, sink, "v2 2") {
		t.Fatalf("v2 did not run on the advanced generation; sink:\n%s", sink.String())
	}
	if !waitForContainersCount(ctx, m.cli, labelApp, fn.Name, 2) {
		t.Fatalf("expected the busy v1 and the v2 container to coexist, got %d:\n%s",
			countContainersByLabel(ctx, m.cli, labelApp, fn.Name), sink.String())
	}
	if sink.contains("END v1") {
		t.Fatalf("v1 was terminated mid-invocation:\n%s", sink.String())
	}

	// Release the busy v1: it completes under its admitted generation, then its
	// superseded container is discarded on release.
	releaseSourceMountGate(t, dir)
	if err := <-v1Done; err != nil {
		t.Fatalf("v1 execute: %v", err)
	}
	if !waitForSinkContains(ctx, sink, "END v1") {
		t.Fatalf("v1 did not complete under its generation; sink:\n%s", sink.String())
	}
	if !waitForContainerIDGone(ctx, m.cli, id1) {
		t.Errorf("the drained v1 container %s should have been discarded on release", id1)
	}
	if !waitForContainersCount(ctx, m.cli, labelApp, fn.Name, 1) {
		t.Errorf("expected exactly one pooled container after drain, got %d",
			countContainersByLabel(ctx, m.cli, labelApp, fn.Name))
	}

	// A later invocation uses the updated mounted source.
	if err := m.Execute(v2Ctx, second, "index.run", []byte(`{"event_id":"3"}`), nil); err != nil {
		t.Fatalf("execute after drain: %v", err)
	}
	if !waitForSinkContains(ctx, sink, "v2 3") {
		t.Fatalf("a post-drain invocation must run the updated mounted source; sink:\n%s", sink.String())
	}
}

// TestIntegrationSourceMountWarmBoundBackpressuresBusyGeneration proves the
// worker-global MAX_WARM_CONTAINERS bound holds across a SOURCE_MOUNT
// generation advance: with the bound at 1, while the old generation's only
// container is busy an invocation for the advanced generation is
// backpressured — it neither starts a second container nor exceeds the warm
// count — and once the busy invocation is released the old container retires
// and the replacement runs the current mounted source. Admission goes through
// the same AcquireWarmPermit/WithWarmPermit seam the runner uses, so the
// backpressure is proven by the warm-wait counter (a positive signal) instead
// of a time-based negative window.
func TestIntegrationSourceMountWarmBoundBackpressuresBusyGeneration(t *testing.T) {
	cli := testutil.RequireDocker(t)
	dir := writeSourceMountGateApp(t)

	fn := app.App{Name: "src-mount-warm-it", Dir: dir,
		Template: &app.Template{Runtime: "node24", Concurrency: 1}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	m, reg := newSourceMountWarmManager(t, 1)
	releaseSourceMountGateOnCleanup(t, dir)
	if got := m.WarmBudgetCapacity(); got != 1 {
		t.Fatalf("WarmBudgetCapacity = %d, want 1", got)
	}
	sink := &pollingSink{}
	prev := SetAppOutput(sink)
	t.Cleanup(func() { SetAppOutput(prev) })

	first, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare v1: %v", err)
	}
	defer cleanupImage(cli, ctx, first.Image)

	// Start the v1 invocation and hold it busy on the gate.
	v1Ctx := context.WithValue(context.Background(), runMetaKey{},
		RunMeta{Hostname: "test-host", App: fn.Name, Handler: "index.run", Image: first.Image})
	v1Done := make(chan error, 1)
	go func() { v1Done <- m.Execute(v1Ctx, first, "index.run", []byte(`{"event_id":"1"}`), nil) }()

	if !waitForSinkContains(ctx, sink, "START v1") {
		t.Fatalf("v1 did not start; sink:\n%s", sink.String())
	}
	id1 := reusedContainerID(t, ctx, m, fn.Name)
	if id1 == "" {
		t.Fatal("expected the busy v1 container to be running")
	}
	// The busy v1 holds the sole global warm slot.
	if got := reg.Gauge(metrics.MetricRuntimeWarmContainers); got != 1 {
		t.Fatalf("warm count while v1 busy = %v, want 1", got)
	}

	// Advance the source generation while v1 is busy.
	writeSourceMountGateAppV2(t, dir)
	second, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("re-prepare: %v", err)
	}
	if second.Image != first.Image {
		t.Fatalf("source-only change moved the image tag: %q -> %q", first.Image, second.Image)
	}
	if second.Fingerprint == first.Fingerprint {
		t.Fatal("source-only change must advance the content fingerprint")
	}

	// Launch the advanced-generation invocation exactly as the runner does:
	// pre-admit against the worker-global warm budget BEFORE Execute.
	v2Done := make(chan error, 1)
	go func() {
		permit, perr := m.AcquireWarmPermit(ctx)
		if perr != nil {
			v2Done <- perr
			return
		}
		defer permit.Release()
		execCtx := context.WithValue(context.Background(), runMetaKey{},
			RunMeta{Hostname: "test-host", App: fn.Name, Handler: "index.run", Image: second.Image})
		v2Done <- m.Execute(WithWarmPermit(execCtx, permit), second, "index.run", []byte(`{"event_id":"2"}`), nil)
	}()

	// Warm-budget backpressure: with the only warm container busy, pre-admission
	// blocks. The warm-wait counter is the positive proof, so no time-based
	// negative window is needed.
	if !pollUntil(ctx, 30*time.Second, func() bool {
		return reg.Counter(metrics.MetricRuntimeWarmWaits) >= 1
	}) {
		t.Fatalf("advanced-generation invocation did not block on the warm budget; waits=%d",
			reg.Counter(metrics.MetricRuntimeWarmWaits))
	}
	// The bound held: no second container, the warm count stayed at 1, and the
	// advanced generation had not run.
	if got := countContainersByLabel(ctx, m.cli, labelApp, fn.Name); got != 1 {
		t.Fatalf("warm containers exceeded MAX_WARM_CONTAINERS while v2 waited: %d", got)
	}
	if got := reg.Gauge(metrics.MetricRuntimeWarmContainers); got != 1 {
		t.Fatalf("global warm count while v2 waited = %v, want 1", got)
	}
	if sink.contains("v2 2") {
		t.Fatalf("v2 bypassed backpressure and ran while the sole warm slot was held:\n%s", sink.String())
	}

	// Release the busy v1: it completes, the old generation retires, and the
	// backpressured invocation proceeds on the current mounted source.
	releaseSourceMountGate(t, dir)
	if err := <-v1Done; err != nil {
		t.Fatalf("v1 execute: %v", err)
	}
	if !waitForSinkContains(ctx, sink, "END v1") {
		t.Fatalf("v1 did not complete; sink:\n%s", sink.String())
	}
	if err := <-v2Done; err != nil {
		t.Fatalf("backpressured v2 execute: %v", err)
	}
	if !waitForSinkContains(ctx, sink, "v2 2") {
		t.Fatalf("replacement did not run the current mounted source; sink:\n%s", sink.String())
	}
	if !waitForContainerIDGone(ctx, m.cli, id1) {
		t.Errorf("the retired v1 container %s should be gone after the replacement", id1)
	}
	if !waitForContainersCount(ctx, m.cli, labelApp, fn.Name, 1) {
		t.Errorf("expected exactly one warm container after replacement, got %d",
			countContainersByLabel(ctx, m.cli, labelApp, fn.Name))
	}
	if got := reg.Gauge(metrics.MetricRuntimeWarmContainers); got != 1 {
		t.Errorf("global warm count after replacement = %v, want 1", got)
	}
}

// TestIntegrationSourceMountNodeServiceSourceReplacementReusesDependency is the
// end-to-end proof of a managed Node entrypoint service's source replacement
// under SOURCE_MOUNT: a source-only edit advances the source fingerprint and the
// service container is replaced start-before-stop (the new generation runs the
// edited mounted source while the old one is still up), while the dependency
// image is REUSED (no new relay-dep-* generation and the app image tag is
// unchanged). The service never runs the invocation bootstrap: it is launched as
// `node --import /relay/resolve-hook.mjs /app/src/app/service.js` and resolves its
// bare imports from the dependency image through the preloaded hook.
func TestIntegrationSourceMountNodeServiceSourceReplacementReusesDependency(t *testing.T) {
	cli := testutil.RequireDocker(t)
	depBefore := depTagSet(context.Background(), cli)
	t.Cleanup(cleanupNewDepImagesSince(cli, depBefore))

	dir := t.TempDir()
	writeFile(t, dir, "package.json", `{
  "type": "module",
  "dependencies": {"picocolors": "^1.0.0"}
}`)
	writeFile(t, dir, "pnpm-lock.yaml", nodePnpmLockPicocolors)
	if err := os.MkdirAll(filepath.Join(dir, "app"), 0o755); err != nil {
		t.Fatalf("mkdir app: %v", err)
	}
	// The service imports a real dependency and prints a version marker; SIGTERM
	// keeps Docker's stop prompt (node as PID 1 ignores the default action).
	writeFile(t, dir, "app/service.js", `
import colors from "picocolors";
process.on("SIGTERM", () => process.exit(0));
console.log("svc " + colors.red("V1"));
setInterval(() => {}, 1 << 30);
`)

	appName := "src-mount-svc-replace-it"
	fn := app.App{Name: appName, Dir: dir, Template: &app.Template{Runtime: "node24"}}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	m, err := NewManager(testutil.DiscardLogger(), nil, "test-host", WithSourceMount(true))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()
	t.Cleanup(func() {
		cc, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_, _ = m.RemoveAppServiceContainers(cc, appName)
		cleanupImagePrefixes(cli, "relay-app-"+appName+":")()
	})

	first, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare v1: %v", err)
	}
	defer cleanupImage(cli, ctx, first.Image)
	if first.Dependency == "" {
		t.Fatal("expected the Node app to build FROM a dependency image")
	}
	depAfterFirst := depTagSet(ctx, cli)

	tmpl := &app.Template{Runtime: "node24"}
	svc := app.Service{Name: "svc", Entrypoint: "app/service.js", Port: 3000}
	resolved1, err := m.ResolveServiceImage(ctx, appName, tmpl, svc, first.Image)
	if err != nil {
		t.Fatalf("resolve v1 service: %v", err)
	}
	if resolved1.Mount == nil || resolved1.Mount.Identity != first.Fingerprint {
		t.Fatalf("v1 mount = %+v, want identity %q", resolved1.Mount, first.Fingerprint)
	}
	if len(resolved1.Entry) != 4 || resolved1.Entry[0] != "node" ||
		resolved1.Entry[1] != "--import" || resolved1.Entry[2] != node.ResolveHookPath ||
		resolved1.Entry[3] != "/app/src/app/service.js" {
		t.Fatalf("v1 service entry = %v, want [node --import %s /app/src/app/service.js]", resolved1.Entry, node.ResolveHookPath)
	}

	id1, err := m.StartService(ctx, ServiceSpec{
		App: appName, Name: svc.Name, SourceRef: svc.Entrypoint,
		Port: svc.Port, Image: resolved1.Ref, ImageID: resolved1.ID,
		Entry: resolved1.Entry, Env: []string{"PORT=3000"},
		SourceMount: resolved1.Mount,
	}, 0)
	if err != nil {
		t.Fatalf("start v1 service: %v", err)
	}
	waitForContainerRunning(t, ctx, cli, id1)
	awaitServiceLog(t, ctx, cli, id1, "svc ", "V1")

	// Source-only edit: the app image and dependency layer must be REUSED, and
	// only the source fingerprint advances.
	writeFile(t, dir, "app/service.js", `
import colors from "picocolors";
process.on("SIGTERM", () => process.exit(0));
console.log("svc " + colors.red("V2"));
setInterval(() => {}, 1 << 30);
`)
	second, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare v2: %v", err)
	}
	if second.Image != first.Image {
		t.Fatalf("source-only change moved the app image: %q -> %q", first.Image, second.Image)
	}
	if second.Dependency != first.Dependency {
		t.Fatalf("source-only change moved the dependency reference: %q -> %q", first.Dependency, second.Dependency)
	}
	if second.Fingerprint == first.Fingerprint {
		t.Fatal("source-only change must advance the source fingerprint")
	}
	if got := newDepTagsSince(ctx, cli, depAfterFirst); len(got) != 0 {
		t.Fatalf("source-only change built a new dependency image: %v", got)
	}

	resolved2, err := m.ResolveServiceImage(ctx, appName, tmpl, svc, second.Image)
	if err != nil {
		t.Fatalf("resolve v2 service: %v", err)
	}
	if resolved2.Mount == nil || resolved2.Mount.Identity != second.Fingerprint {
		t.Fatalf("v2 mount = %+v, want identity %q", resolved2.Mount, second.Fingerprint)
	}
	if resolved2.Ref != resolved1.Ref {
		t.Fatalf("source-only change moved the service image ref: %q -> %q", resolved1.Ref, resolved2.Ref)
	}

	// Start-before-stop: the v2 replacement is started (and proven running the
	// edited source) while v1 is still up.
	id2, err := m.StartService(ctx, ServiceSpec{
		App: appName, Name: svc.Name, SourceRef: svc.Entrypoint,
		Port: svc.Port, Image: resolved2.Ref, ImageID: resolved2.ID,
		Entry: resolved2.Entry, Env: []string{"PORT=3000"},
		SourceMount: resolved2.Mount,
	}, 0)
	if err != nil {
		t.Fatalf("start v2 service: %v", err)
	}
	waitForContainerRunning(t, ctx, cli, id2)
	awaitServiceLog(t, ctx, cli, id2, "svc ", "V2")
	if id1 == id2 {
		t.Fatal("the replacement returned the same container id as v1")
	}
	// Both generations coexist at the start-before-stop boundary.
	if !waitForContainersCount(ctx, cli, labelApp, appName, 2) {
		t.Fatalf("expected v1 and v2 service containers to coexist during replacement, got %d",
			countContainersByLabel(ctx, cli, labelApp, appName))
	}

	// Stop the old generation; the replacement stays up and keeps running V2.
	list, err := m.ServiceContainerList(ctx)
	if err != nil {
		t.Fatalf("list services: %v", err)
	}
	var old []ServiceContainer
	for _, c := range list {
		if c.ID == id1 {
			old = append(old, c)
		}
	}
	if len(old) != 1 {
		t.Fatalf("found %d v1 service containers, want 1", len(old))
	}
	if err := m.StopServiceContainers(ctx, old); err != nil {
		t.Fatalf("stop v1 service: %v", err)
	}
	if !waitForContainerIDGone(ctx, cli, id1) {
		t.Errorf("the replaced v1 service container %s should be gone", id1)
	}
	if !waitForContainersCount(ctx, cli, labelApp, appName, 1) {
		t.Errorf("expected exactly the v2 service container after replacement, got %d",
			countContainersByLabel(ctx, cli, labelApp, appName))
	}
	awaitServiceLog(t, ctx, cli, id2, "svc ", "V2")
}

// awaitServiceLog polls a service container's logs until every want fragment is
// present, or fails the test after a bounded wait.
func awaitServiceLog(t *testing.T, ctx context.Context, cli *client.Client, id string, wants ...string) string {
	t.Helper()
	var logs string
	if !pollUntil(ctx, 30*time.Second, func() bool {
		rc, err := cli.ContainerLogs(ctx, id, client.ContainerLogsOptions{ShowStdout: true, ShowStderr: true})
		if err != nil {
			return false
		}
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, rc)
		rc.Close()
		logs = buf.String()
		for _, w := range wants {
			if !strings.Contains(logs, w) {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("service %s logs did not show %v within 30s, got:\n%s", id, wants, logs)
	}
	return logs
}

// TestIntegrationSourceMountNodeTSTsconfigAliasReflectsSourceChange is the
// end-to-end proof of the mounted TypeScript tsconfig path: a tsconfig.json at
// the app root (/app/src/tsconfig.json) supplies `paths` aliases that esbuild
// resolves while bundling at container startup, the generated .mjs lands under
// /tmp (the host mount is read-only and stays untouched), and a source-only edit
// reuses the same source-independent image while running the re-bundled code.
func TestIntegrationSourceMountNodeTSTsconfigAliasReflectsSourceChange(t *testing.T) {
	cli := testutil.RequireDocker(t)
	depBefore := depTagSet(context.Background(), cli)
	t.Cleanup(cleanupNewDepImagesSince(cli, depBefore))

	dir := t.TempDir()
	writeFile(t, dir, "package.json", `{
  "type": "module",
  "dependencies": {"picocolors": "^1.0.0"}
}`)
	writeFile(t, dir, "pnpm-lock.yaml", nodePnpmLockPicocolors)
	// paths aliases are relative to baseUrl (the app root mounted at /app/src).
	writeFile(t, dir, "tsconfig.json", `{
  "compilerOptions": {
    "baseUrl": ".",
    "paths": {"@app/*": ["./*"]}
  }
}`)
	if err := os.MkdirAll(filepath.Join(dir, "lib"), 0o755); err != nil {
		t.Fatalf("mkdir lib: %v", err)
	}
	writeFile(t, dir, "lib/msg.ts", `
export function message(event: { event_id: string }): string {
  return "alias V1 " + event.event_id;
}
`)
	writeFile(t, dir, "index.ts", `
import colors from "picocolors";
import { message } from "@app/lib/msg";
export function handler(event: { event_id: string }): void {
  console.log(colors.green(message(event)));
}
`)

	tmpl := &app.Template{Runtime: "node24"}
	tmpl.Events = append(tmpl.Events, app.EventRule{Handler: "index.handler"})
	fn := app.App{Name: "src-mount-node-ts-alias-it", Dir: dir, Template: tmpl}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	m, err := NewManager(testutil.DiscardLogger(), nil, "test-host", WithSourceMount(true))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() { _ = m.Close() }()
	out := newAppOutputSink(t)

	first, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer cleanupImage(cli, ctx, first.Image)
	if err := m.Execute(ctx, first, "index.handler", []byte(`{"event_id":"1"}`), nil); err != nil {
		t.Fatalf("first execute: %v", err)
	}
	awaitAppOutput(t, ctx, out, "alias V1 1")

	// The generated module must not appear beside the source: the host mount is
	// read-only and the bootstrap bundles into /tmp.
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, werr error) error {
		if werr != nil {
			return werr
		}
		if !info.IsDir() && strings.HasSuffix(path, ".mjs") {
			t.Errorf("generated module %s leaked into the read-only mount", path)
		}
		return nil
	}); err != nil {
		t.Fatalf("walk app dir: %v", err)
	}

	// Source-only edit of the aliased module: same image, re-bundled from the
	// mount at the next invocation.
	writeFile(t, dir, "lib/msg.ts", `
export function message(event: { event_id: string }): string {
  return "alias V2 " + event.event_id;
}
`)
	second, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("re-prepare: %v", err)
	}
	if second.Image != first.Image {
		t.Fatalf("source-only change moved the image tag: %q -> %q", first.Image, second.Image)
	}
	if second.Fingerprint == first.Fingerprint {
		t.Fatal("source-only change must advance the content fingerprint")
	}
	if err := m.Execute(ctx, second, "index.handler", []byte(`{"event_id":"2"}`), nil); err != nil {
		t.Fatalf("second execute: %v", err)
	}
	awaitAppOutput(t, ctx, out, "alias V2 2")
}

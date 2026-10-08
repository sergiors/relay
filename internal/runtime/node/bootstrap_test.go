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

// TestBootstrapSourceMountConstantsMatchEngine pins the bootstrap/engine
// agreement on the SOURCE_MOUNT layout: the argv flag, the mount target, the
// persisted esbuild path, and the shared resolve-hook preload are embedded
// verbatim in the bootstrap (or the hook module it imports) and must equal the
// engine's constants, since the plan writes the matching ENTRYPOINT, install
// command, and resolve-hook file.
func TestBootstrapSourceMountConstantsMatchEngine(t *testing.T) {
	src := string(Bootstrap)
	for _, want := range []string{
		`process.argv.includes("` + sourceMountFlag + `")`,
		`const MOUNT_ROOT = "` + SourceMountTarget + `";`,
		`const ESBUILD_BIN = "` + mountedEsbuildBin + `";`,
		// The bootstrap delegates the ESM resolve hook to the shared module; it
		// must import it (relative to /relay/bootstrap.mjs) rather than inline it.
		`await import("./resolve-hook.mjs")`,
	} {
		if !strings.Contains(src, want) {
			t.Errorf("bootstrap must contain %q", want)
		}
	}
	// The hook implementation lives in the shared module, not the bootstrap.
	if strings.Contains(src, "registerHooks(") {
		t.Error("bootstrap must not inline registerHooks; the shared resolve hook owns it")
	}
	hook := string(ResolveHook)
	for _, want := range []string{
		`const MOUNT_ROOT = "` + SourceMountTarget + `";`,
		`const BAKED_ROOT = "/app";`,
		// The generated TypeScript overlay is matched separately so a bundled
		// handler's external self-reference is intercepted there too.
		`const GENERATED_ROOT = "/tmp/relay-gen";`,
		`registerHooks({`,
		"installResolveHook();",
		// The CommonJS branch resolves through a dependency-root require and
		// defers builtins; without it Node's default require resolver ignores a
		// re-anchored parentURL and a host node_modules would shadow the image.
		"createRequire(",
		"isBuiltin(",
		"isRequireContext(",
		"shortCircuit: true",
		// The app self-reference exception resolves through the mounted manifest
		// with native ESM/require semantics.
		"isAppSelfReference(",
		"resolveAppSelfReference(",
		"appSourceAnchor",
		"appSourceRequire",
	} {
		if !strings.Contains(hook, want) {
			t.Errorf("resolve hook must contain %q", want)
		}
	}
	// The bootstrap must NOT reintroduce the filesystem-layout alias: it bypasses
	// the mounted manifest's "exports" and resolves the wrong file when a target
	// remaps. Resolution lives entirely in the shared hook.
	if strings.Contains(src, "selfPackageAlias") || strings.Contains(src, "--alias:") {
		t.Error("bootstrap must not carry the esbuild self-reference alias; the shared hook owns self-reference resolution")
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

// writeMountedModule writes a handler under the SOURCE_MOUNT root (root/app/src),
// mirroring the in-image /app/src bind target.
func writeMountedModule(t *testing.T, root, modPath, src string) {
	t.Helper()
	path := filepath.Join(root, "app", "src", filepath.FromSlash(modPath))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		t.Fatalf("write mounted module: %v", err)
	}
}

// bootstrapFor returns the embedded bootstrap with the baked source root
// rewritten to root/app, mirroring the in-image layout.
func bootstrapFor(root string) string {
	bs := rewriteBootstrapRoots(string(Bootstrap), root)
	return strings.Replace(
		bs,
		`const SOURCE_ROOT = MOUNTED ? MOUNT_ROOT : BAKED_ROOT;`,
		fmt.Sprintf(`const SOURCE_ROOT = %s;`, quote(filepath.Join(root, "app"))),
		1,
	)
}

// rewriteBootstrapRoots rewrites the baked (dependency) root to root/app, which
// is independent of source-mount mode: the managed API always lives in
// BAKED_ROOT/node_modules and the ESM resolve hook re-anchors bare imports there.
func rewriteBootstrapRoots(bs, root string) string {
	return strings.Replace(
		bs,
		`const BAKED_ROOT = "/app";`,
		fmt.Sprintf(`const BAKED_ROOT = %s;`, quote(filepath.Join(root, "app"))),
		1,
	)
}

// bootstrapForMounted returns the embedded bootstrap rewritten for a
// source-mounted layout: the mount root becomes root/app/src (a subdirectory of
// the fake /app, mirroring the real /app/src so /app/node_modules stays an
// ancestor for bare-import resolution), the baked fallback stays root/app, and
// the generated overlay root and esbuild path are redirected under root so a test
// can exercise mount-mode resolution without touching the real filesystem.
func bootstrapForMounted(root string) string {
	bs := rewriteBootstrapRoots(string(Bootstrap), root)
	bs = strings.Replace(
		bs,
		`const SOURCE_ROOT = MOUNTED ? MOUNT_ROOT : BAKED_ROOT;`,
		fmt.Sprintf(`const SOURCE_ROOT = MOUNTED ? MOUNT_ROOT : %s;`, quote(filepath.Join(root, "app"))),
		1,
	)
	bs = strings.Replace(
		bs,
		`const MOUNT_ROOT = "/app/src";`,
		fmt.Sprintf(`const MOUNT_ROOT = %s;`, quote(filepath.Join(root, "app", "src"))),
		1,
	)
	bs = strings.Replace(
		bs,
		`const GENERATED_ROOT = "/tmp/relay-gen";`,
		fmt.Sprintf(`const GENERATED_ROOT = %s;`, quote(filepath.Join(root, "gen"))),
		1,
	)
	return strings.Replace(
		bs,
		`const ESBUILD_BIN = "/relay/esbuild/node_modules/.bin/esbuild";`,
		fmt.Sprintf(`const ESBUILD_BIN = %s;`, quote(filepath.Join(root, "esbuild"))),
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
	return startNodeScript(t, root, bootstrapFor(root))
}

// resolveHookForMounted returns the embedded shared resolve hook rewritten for
// the test's fake roots (root/app/src, root/app, and the generated overlay
// root/gen), mirroring the in-image layout so the mounted bootstrap's dynamic
// import of ./resolve-hook.mjs finds a hook that re-anchors to the fake
// dependency root and resolves app self-references from the fake mounted
// manifest.
func resolveHookForMounted(root string) string {
	hook := strings.Replace(
		string(ResolveHook),
		`const MOUNT_ROOT = "/app/src";`,
		fmt.Sprintf(`const MOUNT_ROOT = %s;`, quote(filepath.Join(root, "app", "src"))),
		1,
	)
	hook = strings.Replace(
		hook,
		`const BAKED_ROOT = "/app";`,
		fmt.Sprintf(`const BAKED_ROOT = %s;`, quote(filepath.Join(root, "app"))),
		1,
	)
	return strings.Replace(
		hook,
		`const GENERATED_ROOT = "/tmp/relay-gen";`,
		fmt.Sprintf(`const GENERATED_ROOT = %s;`, quote(filepath.Join(root, "gen"))),
		1,
	)
}

// startNodeMounted starts the real embedded bootstrap in SOURCE_MOUNT mode (the
// --source-mount ENTRYPOINT flag) with the mount root and generated overlay
// rewritten under root. It writes the shared resolve hook beside the bootstrap
// (root/relay/resolve-hook.mjs) exactly as a source-mounted image would, so the
// bootstrap's dynamic import installs it.
func startNodeMounted(t *testing.T, root string) *nodeProc {
	t.Helper()
	hookPath := filepath.Join(root, "relay", "resolve-hook.mjs")
	if err := os.WriteFile(hookPath, []byte(resolveHookForMounted(root)), 0o644); err != nil {
		t.Fatalf("write resolve hook: %v", err)
	}
	return startNodeScriptArgs(t, root, bootstrapForMounted(root), "--source-mount")
}

func startNodeScript(t *testing.T, root, bs string) *nodeProc {
	return startNodeScriptArgs(t, root, bs)
}

func startNodeScriptArgs(t *testing.T, root, bs string, args ...string) *nodeProc {
	t.Helper()
	bsPath := filepath.Join(root, "relay", "bootstrap.mjs")
	if err := os.WriteFile(bsPath, []byte(bs), 0o644); err != nil {
		t.Fatalf("write bootstrap: %v", err)
	}
	cmd := exec.Command("node", append([]string{bsPath}, args...)...)
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

// TestNodeRuntimeSourceMountResolvesFromMountedRootAndDeps proves the
// SOURCE_MOUNT bootstrap resolves handlers from the mounted root (/app/src) and
// still resolves bare packages from /app/node_modules (an ancestor of the mount
// target), i.e. the distinct mount target does not hide the dependency tree.
// TypeScript transpilation at container startup needs the persisted esbuild and
// is covered by the Docker integration suite.
func TestNodeRuntimeSourceMountResolvesFromMountedRootAndDeps(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeMountedModule(t, root, "index.mjs", `
import { context } from "@opentelemetry/api";
export function show(event) {
  console.log("mounted " + event.msg + " " + (context.active().parent ?? "root"));
}
`)
	p := startNodeMounted(t, root)
	r := p.invoke(t, "index.show", `{"msg":"live"}`, nil)
	if !r.OK {
		t.Fatalf("mounted invoke failed: %s", r.Err)
	}
	if !strings.Contains(r.Out, "mounted live root") {
		t.Fatalf("out = %q, want mounted-root resolution with the shared OTel API from /app/node_modules", r.Out)
	}
}

// writePackage writes a minimal ESM package named name that exports a `marker`
// string, under nodeModules (an in-image dependency tree or a conflicting host
// tree).
func writePackage(t *testing.T, nodeModules, name, marker string) {
	t.Helper()
	dir := filepath.Join(nodeModules, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir package %s: %v", name, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "package.json"), []byte(`{"name":"`+name+`","type":"module","main":"index.js"}`), 0o644); err != nil {
		t.Fatalf("write package.json for %s: %v", name, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export const marker = "+quote(marker)+";\n"), 0o644); err != nil {
		t.Fatalf("write index.js for %s: %v", name, err)
	}
}

// TestNodeRuntimeSourceMountResolveHookPrefersDependencyImage proves the mounted
// bootstrap re-anchors bare package imports to the dependency tree even when a
// conflicting node_modules is visible at the mount root: the dependency package
// (BAKED_ROOT/node_modules) and a conflicting host package
// (MOUNT_ROOT/node_modules) both exist, and the hook must pick the dependency
// one. A relative local import must still resolve from the mounted source and the
// shared OTel API singleton must be preserved.
func TestNodeRuntimeSourceMountResolveHookPrefersDependencyImage(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writePackage(t, filepath.Join(root, "app", "node_modules"), "shadowpkg", "DEP")
	writePackage(t, filepath.Join(root, "app", "src", "node_modules"), "shadowpkg", "HOST")
	writeMountedModule(t, root, "local.mjs", `export const where = "LOCAL";`)
	writeMountedModule(t, root, "index.mjs", `
import { marker } from "shadowpkg";
import { where } from "./local.mjs";
import { context } from "@opentelemetry/api";
export function show(event) {
  console.log("pkg=" + marker + " local=" + where + " otel=" + (typeof context.active === "function"));
}
`)
	p := startNodeMounted(t, root)
	r := p.invoke(t, "index.show", `{}`, nil)
	if !r.OK {
		t.Fatalf("mounted invoke failed: %s", r.Err)
	}
	if !strings.Contains(r.Out, "pkg=DEP") {
		t.Fatalf("out = %q, want pkg=DEP (dependency image must win over a host node_modules)", r.Out)
	}
	if strings.Contains(r.Out, "pkg=HOST") {
		t.Fatalf("host node_modules shadowed the dependency image: %q", r.Out)
	}
	if !strings.Contains(r.Out, "local=LOCAL") {
		t.Fatalf("out = %q, want the relative import to resolve from the mounted source", r.Out)
	}
	if !strings.Contains(r.Out, "otel=true") {
		t.Fatalf("out = %q, want the shared OTel API from BAKED_ROOT/node_modules", r.Out)
	}
}

// TestNodeRuntimeSourceMountResolveHookHonorsImportConditions proves the hook
// delegates to Node's own resolver: a dependency package using conditional
// "exports" still selects the "import" condition (not "require") when it is
// re-anchored to BAKED_ROOT.
func TestNodeRuntimeSourceMountResolveHookHonorsImportConditions(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	dir := filepath.Join(root, "app", "node_modules", "condpkg")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir condpkg: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "package.json"), []byte(`{"name":"condpkg","type":"module","exports":{".":{"import":"./esm.js","require":"./cjs.cjs"}}}`), 0o644); err != nil {
		t.Fatalf("write condpkg package.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "esm.js"), []byte("export const kind = \"ESM\";\n"), 0o644); err != nil {
		t.Fatalf("write condpkg esm.js: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cjs.cjs"), []byte("module.exports = { kind: \"CJS\" };\n"), 0o644); err != nil {
		t.Fatalf("write condpkg cjs.cjs: %v", err)
	}
	writeMountedModule(t, root, "index.mjs", `
import { kind } from "condpkg";
export function show(event) { console.log("kind=" + kind); }
`)
	p := startNodeMounted(t, root)
	r := p.invoke(t, "index.show", `{}`, nil)
	if !r.OK {
		t.Fatalf("mounted invoke failed: %s", r.Err)
	}
	if !strings.Contains(r.Out, "kind=ESM") {
		t.Fatalf("out = %q, want the import condition to select the ESM entry", r.Out)
	}
}

// TestNodeRuntimeSourceMountDoesNotSeeBakedRoot proves the mount root is the
// resolution root: a handler present only under the baked workdir is NOT found
// when the process runs in source-mount mode.
func TestNodeRuntimeSourceMountDoesNotSeeBakedRoot(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeModule(t, root, "only-baked.mjs", `export function f(event) {}`)
	p := startNodeMounted(t, root)
	r := p.invoke(t, "only-baked.f", `{}`, nil)
	if r.OK {
		t.Fatal("a source-mounted bootstrap must not resolve handlers from the baked workdir")
	}
	if !r.hasErrorText("module not found") {
		t.Errorf("error = %q, want 'module not found'", r.Err)
	}
}

// TestNodeRuntimeSourceMountSharesOtelSingleton proves the mounted handler
// resolves the SAME @opentelemetry/api instance as the bootstrap: the handler's
// context.active() observes the trace context the bootstrap installed with
// context.with. A second, distinct API instance would have its own
// AsyncLocalStorage and report the root context instead.
func TestNodeRuntimeSourceMountSharesOtelSingleton(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeMountedModule(t, root, "index.mjs", `
import { context } from "@opentelemetry/api";
export function inspect(event) {
  console.log("trace=" + (context.active().parent ?? "root"));
}
`)
	p := startNodeMounted(t, root)
	r := p.invokeTrace(t, "index.inspect", `{}`, nil, map[string]string{
		"traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
	})
	if !r.OK || !strings.Contains(r.Out, "trace=00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01") {
		t.Fatalf("mounted handler must share the bootstrap's OTel singleton: %+v", r)
	}
}

// TestResolveHookPreloadPrefersDependencyImage proves the shared resolve hook
// self-installs when PRELOADED with --import, which is exactly how a mounted
// Node entrypoint service consumes it (a service process never runs the
// bootstrap). A bare import from a module under the mount root must resolve from
// the dependency root even when a conflicting host node_modules sits under the
// mount. This exercises the service path without Docker.
func TestResolveHookPreloadPrefersDependencyImage(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writePackage(t, filepath.Join(root, "app", "node_modules"), "shadowpkg", "DEP")
	writePackage(t, filepath.Join(root, "app", "src", "node_modules"), "shadowpkg", "HOST")
	writeMountedModule(t, root, "app/service.mjs", `
import { marker } from "shadowpkg";
console.log("marker=" + marker);
`)
	hookPath := filepath.Join(root, "relay", "resolve-hook.mjs")
	if err := os.WriteFile(hookPath, []byte(resolveHookForMounted(root)), 0o644); err != nil {
		t.Fatalf("write resolve hook: %v", err)
	}
	cmd := exec.Command("node", "--import", hookPath,
		filepath.Join(root, "app", "src", "app", "service.mjs"))
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("preloaded hook run failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "marker=DEP") {
		t.Fatalf("out = %q, want marker=DEP (dependency root must win when the hook is preloaded)", out)
	}
	if strings.Contains(string(out), "marker=HOST") {
		t.Fatalf("host node_modules shadowed the dependency root: %q", out)
	}
}

// writeCJSPackage writes a minimal CommonJS package named name that exports a
// `marker` string, under nodeModules (an in-image dependency tree or a
// conflicting host tree). It is the CommonJS counterpart of writePackage: a
// require() of a CJS module has no ambiguity about which export shape applies.
func writeCJSPackage(t *testing.T, nodeModules, name, marker string) {
	t.Helper()
	dir := filepath.Join(nodeModules, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir package %s: %v", name, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "package.json"), []byte(`{"name":"`+name+`","main":"index.cjs"}`), 0o644); err != nil {
		t.Fatalf("write package.json for %s: %v", name, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.cjs"), []byte("module.exports = { marker: "+quote(marker)+" };\n"), 0o644); err != nil {
		t.Fatalf("write index.cjs for %s: %v", name, err)
	}
}

// writeMountedCJSPackage writes the app's own CommonJS package.json under the
// mount root (root/app/src/package.json), so a mounted .js handler is
// interpreted as CommonJS and can use require().
func writeMountedCJSPackage(t *testing.T, root string) {
	t.Helper()
	writeMountedModule(t, root, "package.json", `{"type":"commonjs"}`)
}

// TestNodeRuntimeSourceMountCommonJSRequirePrefersDependencyImage proves the
// mounted bootstrap's resolve hook re-anchors a CommonJS require() to the
// dependency tree even when a conflicting node_modules is visible at the mount
// root. Node's default require resolver ignores a re-anchored parentURL, so this
// is the regression the CommonJS branch exists for: the dependency package
// (BAKED_ROOT/node_modules) must win over the host package
// (MOUNT_ROOT/node_modules), while a relative require keeps resolving from the
// mounted source.
func TestNodeRuntimeSourceMountCommonJSRequirePrefersDependencyImage(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeCJSPackage(t, filepath.Join(root, "app", "node_modules"), "shadowpkg", "DEP")
	writeCJSPackage(t, filepath.Join(root, "app", "src", "node_modules"), "shadowpkg", "HOST")
	writeMountedCJSPackage(t, root)
	writeMountedModule(t, root, "local.cjs", `module.exports = { where: "LOCAL" };`)
	writeMountedModule(t, root, "index.js", `
const { marker } = require("shadowpkg");
const local = require("./local.cjs");
exports.handler = function (event) {
  console.log("cjs=" + marker + " local=" + local.where);
};
`)
	p := startNodeMounted(t, root)
	r := p.invoke(t, "index.handler", `{}`, nil)
	if !r.OK {
		t.Fatalf("mounted CommonJS invoke failed: %s", r.Err)
	}
	if !strings.Contains(r.Out, "cjs=DEP") {
		t.Fatalf("out = %q, want cjs=DEP (dependency image must win a CommonJS require over a host node_modules)", r.Out)
	}
	if strings.Contains(r.Out, "cjs=HOST") {
		t.Fatalf("host node_modules shadowed the dependency image for require(): %q", r.Out)
	}
	if !strings.Contains(r.Out, "local=LOCAL") {
		t.Fatalf("out = %q, want the relative require to resolve from the mounted source", r.Out)
	}
}

// TestNodeRuntimeSourceMountCommonJSRequireNestedPrefersDependencyImage proves a
// NESTED host node_modules (MOUNT_ROOT/sub/node_modules) cannot shadow the
// dependency image either: a mounted handler in a subdirectory requires a bare
// package, and the hook re-anchors it to BAKED_ROOT/node_modules.
func TestNodeRuntimeSourceMountCommonJSRequireNestedPrefersDependencyImage(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeCJSPackage(t, filepath.Join(root, "app", "node_modules"), "shadowpkg", "DEP")
	writeCJSPackage(t, filepath.Join(root, "app", "src", "sub", "node_modules"), "shadowpkg", "NESTED_HOST")
	writeMountedCJSPackage(t, root)
	writeMountedModule(t, root, "sub/index.js", `
const { marker } = require("shadowpkg");
exports.handler = function (event) { console.log("nested=" + marker); };
`)
	p := startNodeMounted(t, root)
	r := p.invoke(t, "sub.index.handler", `{}`, nil)
	if !r.OK {
		t.Fatalf("mounted nested CommonJS invoke failed: %s", r.Err)
	}
	if !strings.Contains(r.Out, "nested=DEP") {
		t.Fatalf("out = %q, want nested=DEP (a nested host node_modules must not shadow the dependency image)", r.Out)
	}
	if strings.Contains(r.Out, "nested=NESTED_HOST") {
		t.Fatalf("nested host node_modules shadowed the dependency image: %q", r.Out)
	}
}

// TestNodeRuntimeSourceMountCommonJSRequireLateHostNodeModulesDoesNotShadow
// proves the hook defends a host node_modules created AFTER the process started
// (so it carries no prepare-time mask): the dependency image still wins a
// CommonJS require, at both the top level and a nested directory.
func TestNodeRuntimeSourceMountCommonJSRequireLateHostNodeModulesDoesNotShadow(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeCJSPackage(t, filepath.Join(root, "app", "node_modules"), "shadowpkg", "DEP")
	writeMountedCJSPackage(t, root)
	writeMountedModule(t, root, "index.js", `
const { marker } = require("shadowpkg");
exports.handler = function (event) { console.log("late=" + marker); };
`)
	writeMountedModule(t, root, "sub/index.js", `
const { marker } = require("shadowpkg");
exports.handler = function (event) { console.log("late-nested=" + marker); };
`)
	p := startNodeMounted(t, root)

	// Create the conflicting host trees only AFTER the hook is installed and the
	// process is running: no prepare-time mask could have covered them.
	writeCJSPackage(t, filepath.Join(root, "app", "src", "node_modules"), "shadowpkg", "LATE_HOST")
	writeCJSPackage(t, filepath.Join(root, "app", "src", "sub", "node_modules"), "shadowpkg", "LATE_NESTED_HOST")

	r1 := p.invoke(t, "index.handler", `{}`, nil)
	if !r1.OK || !strings.Contains(r1.Out, "late=DEP") {
		t.Fatalf("late top-level invoke = %+v, want late=DEP", r1)
	}
	if strings.Contains(r1.Out, "late=LATE_HOST") {
		t.Fatalf("late top-level host node_modules shadowed the dependency image: %q", r1.Out)
	}
	r2 := p.invoke(t, "sub.index.handler", `{}`, nil)
	if !r2.OK || !strings.Contains(r2.Out, "late-nested=DEP") {
		t.Fatalf("late nested invoke = %+v, want late-nested=DEP", r2)
	}
	if strings.Contains(r2.Out, "late-nested=LATE_NESTED_HOST") {
		t.Fatalf("late nested host node_modules shadowed the dependency image: %q", r2.Out)
	}
}

// TestNodeRuntimeSourceMountCommonJSRequireKeepsRelativeAbsoluteAndBuiltinNative
// proves the hook only re-anchors BARE specifiers: a relative require resolves
// from the mounted source, an absolute require resolves the exact path, and a
// builtin ("fs") keeps its builtin identity (it has no file to re-anchor).
func TestNodeRuntimeSourceMountCommonJSRequireKeepsRelativeAbsoluteAndBuiltinNative(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeMountedCJSPackage(t, root)
	writeMountedModule(t, root, "local.cjs", `module.exports = { where: "LOCAL" };`)
	writeMountedModule(t, root, "index.js", `
const fs = require("fs");
const local = require("./local.cjs");
const abs = require(process.argv[process.argv.length - 1]);
exports.handler = function (event) {
  console.log("fs=" + (typeof fs.readFileSync) + " local=" + local.where + " abs=" + abs.where);
};
`)
	// The bootstrap is invoked as `node bootstrap.mjs --source-mount <abs>`, so
	// process.argv[2] is the absolute path passed after the flag. The hook must
	// exist BEFORE the process starts (the bootstrap imports it at startup).
	hookPath := filepath.Join(root, "relay", "resolve-hook.mjs")
	if err := os.WriteFile(hookPath, []byte(resolveHookForMounted(root)), 0o644); err != nil {
		t.Fatalf("write resolve hook: %v", err)
	}
	absPath := filepath.Join(root, "app", "src", "local.cjs")
	p := startNodeScriptArgs(t, root, bootstrapForMounted(root), "--source-mount", absPath)
	r := p.invoke(t, "index.handler", `{}`, nil)
	if !r.OK {
		t.Fatalf("mounted invoke failed: %s", r.Err)
	}
	for _, want := range []string{"fs=function", "local=LOCAL", "abs=LOCAL"} {
		if !strings.Contains(r.Out, want) {
			t.Fatalf("out = %q, want %q (relative/absolute/builtin requires must stay native)", r.Out, want)
		}
	}
}

// TestNodeRuntimeSourceMountPackageImportsStayNative proves a package-imports
// specifier ("#...") keeps MOUNT_ROOT as its resolution root for BOTH module
// systems: it is not bare, so the hook leaves it to Node, which resolves it
// against the mounted app's own package.json. This is the "#imports unchanged"
// guarantee.
func TestNodeRuntimeSourceMountPackageImportsStayNative(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeMountedModule(t, root, "package.json",
		`{"type":"commonjs","imports":{"#util":{"require":"./util.cjs","import":"./util.mjs"}}}`)
	writeMountedModule(t, root, "util.cjs", `module.exports = { where: "MOUNT_UTIL" };`)
	writeMountedModule(t, root, "util.mjs", `export const where = "MOUNT_UTIL";`)
	writeMountedModule(t, root, "cjs.js", `
const util = require("#util");
exports.handler = function (event) { console.log("cjs-imports=" + util.where); };
`)
	writeMountedModule(t, root, "esm.mjs", `
import { where } from "#util";
export function handler(event) { console.log("esm-imports=" + where); }
`)
	p := startNodeMounted(t, root)
	r1 := p.invoke(t, "cjs.handler", `{}`, nil)
	if !r1.OK || !strings.Contains(r1.Out, "cjs-imports=MOUNT_UTIL") {
		t.Fatalf("CommonJS package-imports invoke = %+v, want cjs-imports=MOUNT_UTIL (#imports must keep the mounted source as its root)", r1)
	}
	r2 := p.invoke(t, "esm.handler", `{}`, nil)
	if !r2.OK || !strings.Contains(r2.Out, "esm-imports=MOUNT_UTIL") {
		t.Fatalf("ESM package-imports invoke = %+v, want esm-imports=MOUNT_UTIL (#imports must keep the mounted source as its root)", r2)
	}
}

// TestNodeRuntimeSourceMountConditionsDifferForRequireAndImport proves the hook
// preserves each module system's own export conditions for a dual package: a
// CommonJS require selects the "require" entry and an ESM import selects the
// "import" entry, both resolved from the dependency image.
func TestNodeRuntimeSourceMountConditionsDifferForRequireAndImport(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	dir := filepath.Join(root, "app", "node_modules", "condpkg")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir condpkg: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "package.json"), []byte(`{"name":"condpkg","type":"module","exports":{".":{"import":"./esm.js","require":"./cjs.cjs"}}}`), 0o644); err != nil {
		t.Fatalf("write condpkg package.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "esm.js"), []byte("export const kind = \"ESM\";\n"), 0o644); err != nil {
		t.Fatalf("write condpkg esm.js: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cjs.cjs"), []byte("module.exports = { kind: \"CJS\" };\n"), 0o644); err != nil {
		t.Fatalf("write condpkg cjs.cjs: %v", err)
	}
	writeMountedCJSPackage(t, root)
	writeMountedModule(t, root, "cjs.js", `
const { kind } = require("condpkg");
exports.handler = function (event) { console.log("require-kind=" + kind); };
`)
	writeMountedModule(t, root, "esm.mjs", `
import { kind } from "condpkg";
export function handler(event) { console.log("import-kind=" + kind); }
`)
	p := startNodeMounted(t, root)
	r1 := p.invoke(t, "cjs.handler", `{}`, nil)
	if !r1.OK || !strings.Contains(r1.Out, "require-kind=CJS") {
		t.Fatalf("require invoke = %+v, want require-kind=CJS (the require condition)", r1)
	}
	r2 := p.invoke(t, "esm.handler", `{}`, nil)
	if !r2.OK || !strings.Contains(r2.Out, "import-kind=ESM") {
		t.Fatalf("import invoke = %+v, want import-kind=ESM (the import condition)", r2)
	}
}

// TestResolveHookPreloadCommonJSRequirePrefersDependencyImage proves the shared
// hook's CommonJS branch when PRELOADED with --import, which is exactly how a
// mounted Node entrypoint service consumes it (a service process never runs the
// bootstrap). A CJS service under the mount root requires a bare package and the
// dependency root must win over a conflicting host node_modules.
func TestResolveHookPreloadCommonJSRequirePrefersDependencyImage(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeCJSPackage(t, filepath.Join(root, "app", "node_modules"), "shadowpkg", "DEP")
	writeCJSPackage(t, filepath.Join(root, "app", "src", "node_modules"), "shadowpkg", "HOST")
	writeMountedModule(t, root, "app/service.cjs", `
const { marker } = require("shadowpkg");
console.log("marker=" + marker);
`)
	hookPath := filepath.Join(root, "relay", "resolve-hook.mjs")
	if err := os.WriteFile(hookPath, []byte(resolveHookForMounted(root)), 0o644); err != nil {
		t.Fatalf("write resolve hook: %v", err)
	}
	cmd := exec.Command("node", "--import", hookPath,
		filepath.Join(root, "app", "src", "app", "service.cjs"))
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("preloaded hook run failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "marker=DEP") {
		t.Fatalf("out = %q, want marker=DEP (dependency root must win a CommonJS require when the hook is preloaded)", out)
	}
	if strings.Contains(string(out), "marker=HOST") {
		t.Fatalf("host node_modules shadowed the dependency root for a preloaded CommonJS require: %q", out)
	}
}

// TestNodeRuntimeSourceMountTranspilesTSWithMountedTsconfigToTmp proves the
// mounted TypeScript path: the bootstrap loads tsconfig.json from the MOUNT root
// (/app/src/tsconfig.json), bundles the handler with the pinned esbuild into the
// writable GENERATED_ROOT under /tmp (never back into the read-only mount), and
// imports the generated .mjs. A fake esbuild records its argv and writes the
// output, so the test needs neither the real tool nor Docker.
func TestNodeRuntimeSourceMountTranspilesTSWithMountedTsconfigToTmp(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeMountedModule(t, root, "tsconfig.json",
		`{"compilerOptions":{"strict":true,"paths":{"@app/*":["./*"]}}}`)
	writeMountedModule(t, root, "index.ts", `
export function handler(event: { msg: string }): void {
  console.log("source-ts " + event.msg);
}
`)

	// The fake esbuild records every argv entry and writes the --outfile target
	// with a valid ESM handler, so the bootstrap's dynamic import succeeds.
	logPath := filepath.Join(root, "esbuild-args.txt")
	esbuild := "#!/bin/sh\n" +
		"LOG=" + quote(logPath) + "\n" +
		": > \"$LOG\"\n" +
		"OUT=\"\"\n" +
		"for a in \"$@\"; do\n" +
		"  echo \"$a\" >> \"$LOG\"\n" +
		"  case \"$a\" in --outfile=*) OUT=\"${a#--outfile=}\" ;; esac\n" +
		"done\n" +
		"mkdir -p \"$(dirname \"$OUT\")\"\n" +
		"printf '%s\\n' 'export function handler(event) { console.log(\"fake-ts-ok \" + event.msg); }' > \"$OUT\"\n" +
		"exit 0\n"
	if err := os.WriteFile(filepath.Join(root, "esbuild"), []byte(esbuild), 0o755); err != nil {
		t.Fatalf("write fake esbuild: %v", err)
	}

	p := startNodeMounted(t, root)
	r := p.invoke(t, "index.handler", `{"msg":"live"}`, nil)
	if !r.OK {
		t.Fatalf("mounted TS invoke failed: %s", r.Err)
	}
	if !strings.Contains(r.Out, "fake-ts-ok live") {
		t.Fatalf("out = %q, want the transpiled handler output", r.Out)
	}

	argsRaw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read esbuild args: %v", err)
	}
	args := string(argsRaw)
	wantTsconfig := "--tsconfig=" + filepath.Join(root, "app", "src", "tsconfig.json")
	wantOutfile := "--outfile=" + filepath.Join(root, "gen", "index.mjs")
	for _, want := range []string{
		"--bundle",
		"--format=esm",
		"--platform=node",
		"--packages=external",
		"--target=node",
		wantTsconfig,
		wantOutfile,
	} {
		if !strings.Contains(args, want) {
			t.Errorf("esbuild args missing %q; got:\n%s", want, args)
		}
	}
	// The generated file must land in GENERATED_ROOT (/tmp), never beside the
	// source inside the read-only mount.
	if _, err := os.Stat(filepath.Join(root, "app", "src", "index.mjs")); err == nil {
		t.Fatal("the bootstrap wrote the generated .mjs into the mount; output must stay under /tmp")
	}
	walkErr := filepath.Walk(filepath.Join(root, "app", "src"), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".mjs") {
			t.Errorf("generated module %s leaked into the mount", path)
		}
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walk mount: %v", walkErr)
	}
}

// writeBodyEsbuild writes a fake esbuild at root/esbuild that records every argv
// entry to logPath and writes body (a complete ESM module) to the --outfile
// target. The body comes from a file so the shell never has to interpret it.
// This lets the mounted TypeScript transpile path be exercised end to end
// without the real tool, including the EXTERNAL imports --packages=external
// leaves in the generated bundle.
func writeBodyEsbuild(t *testing.T, root, logPath, body string) {
	t.Helper()
	bodyPath := filepath.Join(root, "generated-body.mjs")
	if err := os.WriteFile(bodyPath, []byte(body), 0o644); err != nil {
		t.Fatalf("write generated body: %v", err)
	}
	esbuild := "#!/bin/sh\n" +
		"LOG=" + quote(logPath) + "\n" +
		"BODY=" + quote(bodyPath) + "\n" +
		": > \"$LOG\"\n" +
		"OUT=\"\"\n" +
		"for a in \"$@\"; do\n" +
		"  echo \"$a\" >> \"$LOG\"\n" +
		"  case \"$a\" in --outfile=*) OUT=\"${a#--outfile=}\" ;; esac\n" +
		"done\n" +
		"mkdir -p \"$(dirname \"$OUT\")\"\n" +
		"cp \"$BODY\" \"$OUT\"\n" +
		"exit 0\n"
	if err := os.WriteFile(filepath.Join(root, "esbuild"), []byte(esbuild), 0o755); err != nil {
		t.Fatalf("write fake esbuild: %v", err)
	}
}

// TestNodeRuntimeSourceMountESMSelfReferenceUsesMountedExports proves a mounted
// JavaScript ESM handler can import its OWN package by name, resolved natively
// through the MOUNTED manifest's "exports". The exports map deliberately DIVERGES
// from the filesystem layout ("./lib/util" points at a nested
// ./lib/impl/real-util.js while a decoy ./lib/util.js exists), so a
// filesystem-layout alias would resolve the decoy or fail; the hook must honor
// the manifest's real target. A normal bare dependency in the same module still
// anchors to the dependency image over a conflicting host node_modules.
func TestNodeRuntimeSourceMountESMSelfReferenceUsesMountedExports(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writePackage(t, filepath.Join(root, "app", "node_modules"), "shadowpkg", "DEP")
	writePackage(t, filepath.Join(root, "app", "src", "node_modules"), "shadowpkg", "HOST")
	writeMountedModule(t, root, "package.json",
		`{"name":"@scope/app","type":"module","exports":{"./lib/util":"./lib/impl/real-util.js","./lib/*":"./lib/*.js"}}`)
	writeMountedModule(t, root, "lib/util.js", `export const where = "DECOY-LAYOUT";`)
	writeMountedModule(t, root, "lib/impl/real-util.js", `export const where = "REAL-EXPORT";`)
	writeMountedModule(t, root, "index.mjs", `
import { where } from "@scope/app/lib/util";
import { marker } from "shadowpkg";
export function show(event) {
  console.log("self=" + where + " pkg=" + marker);
}
`)
	p := startNodeMounted(t, root)
	r := p.invoke(t, "index.show", `{}`, nil)
	if !r.OK {
		t.Fatalf("mounted ESM self-reference invoke failed: %s", r.Err)
	}
	if !strings.Contains(r.Out, "self=REAL-EXPORT") {
		t.Fatalf("out = %q, want self=REAL-EXPORT (the mounted manifest's real exports target)", r.Out)
	}
	if strings.Contains(r.Out, "DECOY-LAYOUT") {
		t.Fatalf("self-reference resolved by filesystem layout instead of the manifest exports: %q", r.Out)
	}
	if !strings.Contains(r.Out, "pkg=DEP") {
		t.Fatalf("out = %q, want pkg=DEP (a normal bare dependency must still anchor to the dependency image)", r.Out)
	}
	if strings.Contains(r.Out, "pkg=HOST") {
		t.Fatalf("host node_modules shadowed the dependency image for a normal bare import: %q", r.Out)
	}
}

// TestNodeRuntimeSourceMountSelfReferenceWithoutExportsAnchorsToDependency proves
// the self-reference exception is gated on name+exports: a manifest with a name
// but NO exports is not a self-reference source, so importing that name resolves
// like any other bare dependency through the dependency image (never the host
// node_modules).
func TestNodeRuntimeSourceMountSelfReferenceWithoutExportsAnchorsToDependency(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writePackage(t, filepath.Join(root, "app", "node_modules"), "@scope/noexp", "DEP")
	writePackage(t, filepath.Join(root, "app", "src", "node_modules"), "@scope/noexp", "HOST")
	writeMountedModule(t, root, "package.json", `{"name":"@scope/noexp","type":"module"}`)
	writeMountedModule(t, root, "index.mjs", `
import { marker } from "@scope/noexp";
export function show(event) { console.log("noexp=" + marker); }
`)
	p := startNodeMounted(t, root)
	r := p.invoke(t, "index.show", `{}`, nil)
	if !r.OK {
		t.Fatalf("mounted no-exports invoke failed: %s", r.Err)
	}
	if !strings.Contains(r.Out, "noexp=DEP") {
		t.Fatalf("out = %q, want noexp=DEP (without exports the app name resolves as a dependency from the dependency image)", r.Out)
	}
	if strings.Contains(r.Out, "noexp=HOST") {
		t.Fatalf("host node_modules shadowed the dependency image: %q", r.Out)
	}
}

// TestNodeRuntimeSourceMountCommonJSSelfReferenceUsesMountedExports proves a
// mounted CommonJS handler can require its OWN package by name, resolved natively
// through the MOUNTED manifest's "require" export condition. The exports map
// diverges from the layout (the decoy ./lib/util.cjs exists, the real target is a
// nested ./lib/impl/real.cjs), so a layout alias would pick the wrong file.
func TestNodeRuntimeSourceMountCommonJSSelfReferenceUsesMountedExports(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeMountedModule(t, root, "package.json",
		`{"name":"@scope/appc","type":"commonjs","exports":{"./lib/util":{"require":"./lib/impl/real.cjs","import":"./lib/impl/real.mjs"}}}`)
	writeMountedModule(t, root, "lib/util.cjs", `module.exports = { where: "CJS-DECOY" };`)
	writeMountedModule(t, root, "lib/impl/real.cjs", `module.exports = { where: "CJS-REAL" };`)
	writeMountedModule(t, root, "index.js", `
const { where } = require("@scope/appc/lib/util");
exports.handler = function (event) { console.log("cjs=" + where); };
`)
	p := startNodeMounted(t, root)
	r := p.invoke(t, "index.handler", `{}`, nil)
	if !r.OK {
		t.Fatalf("mounted CommonJS self-reference invoke failed: %s", r.Err)
	}
	if !strings.Contains(r.Out, "cjs=CJS-REAL") {
		t.Fatalf("out = %q, want cjs=CJS-REAL (the manifest's require export condition)", r.Out)
	}
	if strings.Contains(r.Out, "CJS-DECOY") {
		t.Fatalf("self-reference resolved by filesystem layout instead of the manifest exports: %q", r.Out)
	}
}

// TestNodeRuntimeSourceMountGeneratedTSSelfReferenceUsesMountedExports proves a
// mounted TypeScript handler's self-reference survives transpilation WITHOUT an
// esbuild alias: the fake esbuild emits the self-reference as an EXTERNAL import
// (as the real pinned esbuild does with --packages=external) into the generated
// overlay under GENERATED_ROOT, and the shared hook resolves it through the
// mounted manifest's divergent exports map. It also pins that no --alias is ever
// passed to esbuild and that the rest of the transpile contract is unchanged.
func TestNodeRuntimeSourceMountGeneratedTSSelfReferenceUsesMountedExports(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeMountedModule(t, root, "package.json",
		`{"name":"@scope/appts","type":"module","exports":{"./lib/util":"./lib/impl/real-util.js","./lib/*":"./lib/*.js"}}`)
	writeMountedModule(t, root, "lib/util.js", `export const where = "TS-DECOY";`)
	writeMountedModule(t, root, "lib/impl/real-util.js", `export const where = "TS-REAL";`)
	writeMountedModule(t, root, "index.ts", `
import { where } from "@scope/appts/lib/util";
export function handler(event: { msg: string }): void { console.log("gen=" + where); }
`)
	logPath := filepath.Join(root, "esbuild-args.txt")
	writeBodyEsbuild(t, root, logPath,
		"import { where } from \"@scope/appts/lib/util\";\n"+
			"export function handler(event) { console.log(\"gen=\" + where); }\n")

	p := startNodeMounted(t, root)
	r := p.invoke(t, "index.handler", `{"msg":"live"}`, nil)
	if !r.OK {
		t.Fatalf("mounted generated TS self-reference invoke failed: %s", r.Err)
	}
	if !strings.Contains(r.Out, "gen=TS-REAL") {
		t.Fatalf("out = %q, want gen=TS-REAL (the generated bundle's external self-reference must resolve through the mounted manifest)", r.Out)
	}
	if strings.Contains(r.Out, "TS-DECOY") {
		t.Fatalf("generated self-reference resolved by filesystem layout instead of the manifest exports: %q", r.Out)
	}
	// The generated module must have landed in the generated overlay, not the mount.
	if _, err := os.Stat(filepath.Join(root, "gen", "index.mjs")); err != nil {
		t.Fatalf("expected the generated module under the overlay: %v", err)
	}
	argsRaw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read esbuild args: %v", err)
	}
	args := string(argsRaw)
	if strings.Contains(args, "--alias") {
		t.Errorf("esbuild must not receive a filesystem-layout self-reference alias; got:\n%s", args)
	}
	for _, want := range []string{"--bundle", "--packages=external", "--format=esm", "--outfile="} {
		if !strings.Contains(args, want) {
			t.Errorf("esbuild args missing %q; got:\n%s", want, args)
		}
	}
}

// TestNodeRuntimeSourceMountGeneratedBareDependencyAnchorsToDependency proves a
// generated TypeScript bundle's NORMAL bare dependency still anchors to the
// dependency image, over a conflicting host node_modules, even though the
// generated file lives outside the mount. (The generated overlay's node_modules
// symlink points at the dependency root; this pins the hook's re-anchor, which
// holds even when that symlink is unavailable.)
func TestNodeRuntimeSourceMountGeneratedBareDependencyAnchorsToDependency(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writePackage(t, filepath.Join(root, "app", "node_modules"), "shadowpkg", "DEP")
	writePackage(t, filepath.Join(root, "app", "src", "node_modules"), "shadowpkg", "HOST")
	writeMountedModule(t, root, "package.json", `{"type":"module"}`)
	writeMountedModule(t, root, "index.ts", `
import { marker } from "shadowpkg";
export function handler(event: { msg: string }): void { console.log("gendep=" + marker); }
`)
	logPath := filepath.Join(root, "esbuild-args.txt")
	writeBodyEsbuild(t, root, logPath,
		"import { marker } from \"shadowpkg\";\n"+
			"export function handler(event) { console.log(\"gendep=\" + marker); }\n")

	p := startNodeMounted(t, root)
	r := p.invoke(t, "index.handler", `{"msg":"live"}`, nil)
	if !r.OK {
		t.Fatalf("mounted generated dependency invoke failed: %s", r.Err)
	}
	if !strings.Contains(r.Out, "gendep=DEP") {
		t.Fatalf("out = %q, want gendep=DEP (a generated bundle's bare dependency must anchor to the dependency image)", r.Out)
	}
	if strings.Contains(r.Out, "gendep=HOST") {
		t.Fatalf("host node_modules shadowed the dependency image for a generated bare import: %q", r.Out)
	}
}

// TestResolveHookPreloadSelfReferenceUsesMountedExports proves the shared hook
// resolves an app self-reference when PRELOADED with --import, which is exactly
// how a mounted Node entrypoint service consumes it (a service process never runs
// the bootstrap). The service module lives under the mount root and imports its
// own package by name through a divergent exports map.
func TestResolveHookPreloadSelfReferenceUsesMountedExports(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeMountedModule(t, root, "package.json",
		`{"name":"@scope/appsvc","type":"module","exports":{"./lib/util":"./lib/impl/real-util.js","./lib/*":"./lib/*.js"}}`)
	writeMountedModule(t, root, "lib/util.js", `export const where = "SVC-DECOY";`)
	writeMountedModule(t, root, "lib/impl/real-util.js", `export const where = "SVC-REAL";`)
	writeMountedModule(t, root, "app/service.mjs", `
import { where } from "@scope/appsvc/lib/util";
console.log("svc-self=" + where);
`)
	hookPath := filepath.Join(root, "relay", "resolve-hook.mjs")
	if err := os.WriteFile(hookPath, []byte(resolveHookForMounted(root)), 0o644); err != nil {
		t.Fatalf("write resolve hook: %v", err)
	}
	cmd := exec.Command("node", "--import", hookPath,
		filepath.Join(root, "app", "src", "app", "service.mjs"))
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("preloaded hook run failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "svc-self=SVC-REAL") {
		t.Fatalf("out = %q, want svc-self=SVC-REAL (the preloaded hook must resolve the self-reference through the mounted manifest)", out)
	}
	if strings.Contains(string(out), "SVC-DECOY") {
		t.Fatalf("self-reference resolved by filesystem layout instead of the manifest exports: %q", out)
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

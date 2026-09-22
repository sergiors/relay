//go:build integration

// Engine end-to-end smoke tests: real Python/Node function builds executed
// against a real Docker daemon, including the bundled example functions.
package runtime

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/testutil"
)

// repoRoot is the repository root, derived from THIS source file's location
// (internal/runtime/x.go -> repo root) rather than the test working directory,
// so the real example function directories under examples/functions/ resolve
// regardless of where `go test` is invoked from.
var repoRoot = func() string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		panic("cannot locate engine_e2e_integration_test.go via runtime.Caller")
	}
	// thisFile = <repo>/internal/runtime/engine_e2e_integration_test.go
	return filepath.Join(filepath.Dir(thisFile), "..", "..")
}()

// readRealTemplate loads the template.yaml from one of the real example
// function directories and returns the parsed template alongside its dir.
func readRealTemplate(t *testing.T, relDir string) (string, *function.Template) {
	t.Helper()
	dir := filepath.Join(repoRoot, "examples", "functions", relDir)
	data, err := os.ReadFile(filepath.Join(dir, "template.yaml"))
	if err != nil {
		t.Fatalf("read real template %s: %v", relDir, err)
	}
	tmpl, err := function.ParseTemplate(data)
	if err != nil {
		t.Fatalf("parse real template %s: %v", relDir, err)
	}
	return dir, tmpl
}

const devEventJSON = `{
  "event_id": "evt_123",
  "event_name": "INSERT",
  "table_name": "users",
  "new_image": {"id": "user_123", "name": "John Doe", "email": "john@example.com"}
}`

func TestIntegrationPythonEndToEnd(t *testing.T) {
	testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// No dependency manifest: this smoke test proves python end-to-end prepare +
	// execute, not the dependency-install path. (The dependency layer is covered
	// by TestIntegrationDependencyImageLabels / DependencyLayerReuse /
	// RequirementsInstalledWithUvAndExecutes; an empty requirements.txt here
	// would only force a redundant 217MB relay-dep-* base export.)

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: python3.14
events:
  - handler: handler.completed
    pattern:
      status: [COMPLETED]
`)
	writeFile(t, dir, "handler.py", `
def completed(event):
    print("completed %s" % event.get("event_id"))
`)

	fn := function.Function{Name: "py-e2e", Dir: dir, Template: &function.Template{Runtime: "python3.14"}}
	m, _ := newManager(t)

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	eventJSON := []byte(`{"event_id":"1757-0","status":"COMPLETED"}`)
	if err := m.Execute(ctx, prepared, "handler.completed", eventJSON, nil); err != nil {
		t.Fatalf("execute: %v", err)
	}
}

func TestIntegrationPythonAsyncEndToEnd(t *testing.T) {
	testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// No dependency manifest: this smoke test proves async python handler
	// execution end to end, not the dependency-install path (see
	// TestIntegrationPythonEndToEnd).

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: python3.14
events:
  - handler: handler.completed
    pattern:
      status: [COMPLETED]
`)
	writeFile(t, dir, "handler.py", `
import asyncio

async def completed(event):
    print("async completed %s" % event.get("event_id"))
`)

	fn := function.Function{Name: "py-async-e2e", Dir: dir, Template: &function.Template{Runtime: "python3.14"}}
	m, _ := newManager(t)

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	eventJSON := []byte(`{"event_id":"1757-0","status":"COMPLETED"}`)
	if err := m.Execute(ctx, prepared, "handler.completed", eventJSON, nil); err != nil {
		t.Fatalf("execute async: %v", err)
	}
}

func TestIntegrationNodeEndToEnd(t *testing.T) {
	testutil.RequireDocker(t)

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: index.created
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", `
export async function created(event) {
  console.log("created " + event.event_id);
}
`)
	// No package.json: the image must inject an ESM package.json.

	fn := function.Function{
		Name: "node-e2e",
		Dir:  dir,
		Template: &function.Template{
			Runtime: "node24",
		},
	}
	m, _ := newManager(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	eventJSON := []byte(`{"event_id":"1757-0","event_name":"INSERT"}`)
	if err := m.Execute(ctx, prepared, "index.created", eventJSON, nil); err != nil {
		t.Fatalf("execute: %v", err)
	}
}

// TestRealUserEventsPythonEndToEnd drives the real
// examples/functions/user-events-python example: three rules whose handlers
// live in the events/ namespace package
// (events.created / events.updated / events.deleted).
func TestIntegrationRealUserEventsPythonEndToEnd(t *testing.T) {
	testutil.RequireDocker(t)

	dir, tmpl := readRealTemplate(t, "user-events-python")
	fn := function.Function{Name: "user-events-python", Dir: dir, Template: tmpl}
	m, _ := newManager(t)
	out := newFunctionOutputSink(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	// INSERT -> events.created.handler uses new_image.id.
	if err := m.Execute(ctx, prepared, "events.created.handler", []byte(devEventJSON), nil); err != nil {
		t.Fatalf("execute events.created.handler: %v", err)
	}

	// MODIFY -> events.updated.handler uses new_image.id.
	modifiedJSON := []byte(`{
	  "event_id": "evt_124",
	  "event_name": "MODIFY",
	  "table_name": "users",
	  "new_image": {"id": "user_123", "name": "John Doe", "email": "john@example.com"}
	}`)
	if err := m.Execute(ctx, prepared, "events.updated.handler", modifiedJSON, nil); err != nil {
		t.Fatalf("execute events.updated.handler: %v", err)
	}

	// REMOVE -> events.deleted.handler uses old_image.id; craft a payload with
	// old_image containing the id.
	deletedJSON := []byte(`{
	  "event_id": "evt_125",
	  "event_name": "REMOVE",
	  "table_name": "users",
	  "old_image": {"id": "user_123", "name": "John Doe", "email": "john@example.com"}
	}`)
	if err := m.Execute(ctx, prepared, "events.deleted.handler", deletedJSON, nil); err != nil {
		t.Fatalf("execute events.deleted.handler: %v", err)
	}

	logs := awaitFunctionOutput(t, ctx, out,
		"User created: user_123",
		"User updated: user_123",
		"User deleted: user_123",
	)
	t.Logf("captured handler output:\n%s", logs)
}

// TestRealWelcomeEmailNodeEndToEnd drives the real
// examples/functions/welcome-email-node example: a single rule whose handler
// resolves as handler.handler -> module
// "handler" -> /app/handler.js. No package.json is present, exercising the
// injected ESM package.json path.
func TestIntegrationRealWelcomeEmailNodeEndToEnd(t *testing.T) {
	testutil.RequireDocker(t)

	dir, tmpl := readRealTemplate(t, "welcome-email-node")
	fn := function.Function{Name: "welcome-email-node", Dir: dir, Template: tmpl}
	m, _ := newManager(t)
	out := newFunctionOutputSink(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	if err := m.Execute(ctx, prepared, "handler.handler", []byte(devEventJSON), nil); err != nil {
		t.Fatalf("execute handler.handler: %v", err)
	}

	logs := awaitFunctionOutput(t, ctx, out, "Sending welcome email to john@example.com")
	t.Logf("captured handler output:\n%s", logs)
}

// TestNodeBrokenDependencyEndToEnd verifies that a module that exists but
// imports a missing dependency surfaces the REAL import error through Execute,
// not a "module not found" message.
func TestIntegrationNodeBrokenDependencyEndToEnd(t *testing.T) {
	testutil.RequireDocker(t)

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: index.run
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", `
import "missing-package";
export function run(event) {
  console.log("should not run");
}
`)

	fn := function.Function{Name: "node-broken-e2e", Dir: dir, Template: &function.Template{Runtime: "node24"}}
	m, _ := newManager(t)
	out := newFunctionOutputSink(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	eventJSON := []byte(`{"event_id":"1","event_name":"INSERT"}`)
	err = m.Execute(ctx, prepared, "index.run", eventJSON, nil)
	if err == nil {
		t.Fatalf("expected execute to fail for broken dependency")
	}
	if strings.Contains(err.Error(), "not found") {
		t.Errorf("broken dependency must NOT be reported as module not found, got: %v", err)
	}
	logs := awaitFunctionOutput(t, ctx, out, "missing-package")
	t.Logf("execute error: %v", err)
	t.Logf("container logs: %s", logs)
}

// tsTemplate builds a hand-constructed node24 template carrying the given event
// handlers, since Prepare derives the handler modules from the template (an
// empty template would mean no TypeScript compilation).
func tsTemplate(handlers ...string) *function.Template {
	tmpl := &function.Template{Runtime: "node24"}
	for _, h := range handlers {
		tmpl.Events = append(tmpl.Events, function.EventRule{Handler: h})
	}
	return tmpl
}

// TestIntegrationNodeTSEndToEnd builds and runs a TypeScript handler with a local
// .ts import graph and no package.json: Relay transpiles the sources to generated
// .mjs at build time and the unchanged Node bootstrap executes them.
func TestIntegrationNodeTSEndToEnd(t *testing.T) {
	testutil.RequireDocker(t)

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: index.handler
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.ts", `
import { message } from "./message";
export function handler(event: { event_id: string }): void {
  console.log(message(event));
}
`)
	writeFile(t, dir, "message.ts", `
export function message(event: { event_id: string }): string {
  return "ts created " + event.event_id;
}
`)
	// No package.json and no tsconfig.json: the ESM package.json is injected.

	fn := function.Function{Name: "node-ts-e2e", Dir: dir, Template: tsTemplate("index.handler")}
	m, _ := newManager(t)
	out := newFunctionOutputSink(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if err := m.Execute(ctx, prepared, "index.handler", []byte(`{"event_id":"1757-0","event_name":"INSERT"}`), nil); err != nil {
		t.Fatalf("execute: %v", err)
	}
	// The transpiled handler's stdout reaches the function-output sink.
	awaitFunctionOutput(t, ctx, out, "ts created 1757-0")
}

// TestIntegrationNodeTSDepsEndToEnd proves packages left external by the bundler
// resolve at runtime from the dependency layer: the .ts handler imports a real
// CJS dependency installed into /app/node_modules by the dependency image.
func TestIntegrationNodeTSDepsEndToEnd(t *testing.T) {
	cli := testutil.RequireDocker(t)
	depBefore := depTagSet(context.Background(), cli)
	t.Cleanup(cleanupNewDepImagesSince(cli, depBefore))

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: index.handler
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "package.json", `{
  "type": "module",
  "dependencies": {"picocolors": "^1.0.0"}
}`)
	writeFile(t, dir, "index.ts", `
import colors from "picocolors";
export function handler(event: { event_id: string }): void {
  console.log(colors.red("dep " + event.event_id));
}
`)

	fn := function.Function{Name: "node-ts-deps-e2e", Dir: dir, Template: tsTemplate("index.handler")}
	m, _ := newManager(t)
	out := newFunctionOutputSink(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if prepared.Dependency == "" {
		t.Fatal("expected the function to be built FROM a dependency image")
	}
	if err := m.Execute(ctx, prepared, "index.handler", []byte(`{"event_id":"1757-0","event_name":"INSERT"}`), nil); err != nil {
		t.Fatalf("execute: %v", err)
	}
	// picocolors.red wraps the text in ANSI escapes; match the inner text. The
	// import resolves at runtime from the dependency layer's node_modules.
	awaitFunctionOutput(t, ctx, out, "dep 1757-0")
}

// TestIntegrationNodeTSExampleEndToEnd drives the real
// examples/functions/order-confirmation-typescript example: a TypeScript handler
// at src/handler.ts importing two local .ts modules (a type module and a message
// module), with a committed tsconfig.json and package-lock.json.
func TestIntegrationNodeTSExampleEndToEnd(t *testing.T) {
	testutil.RequireDocker(t)

	dir, tmpl := readRealTemplate(t, "order-confirmation-typescript")
	fn := function.Function{Name: "order-confirmation-typescript", Dir: dir, Template: tmpl}
	m, _ := newManager(t)
	out := newFunctionOutputSink(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	eventJSON := []byte(`{
	  "type": "order.created",
	  "order_id": "ord_42",
	  "customer_email": "jane@example.com",
	  "total": 99.5
	}`)
	if err := m.Execute(ctx, prepared, "src.handler.handler", eventJSON, nil); err != nil {
		t.Fatalf("execute src.handler.handler: %v", err)
	}

	logs := awaitFunctionOutput(t, ctx, out,
		"confirmation sent to jane@example.com",
		"ord_42",
		"$99.50",
	)
	t.Logf("captured handler output:\n%s", logs)
}

// TestIntegrationNodeTSMultipleHandlersEndToEnd proves ONE image transpiles and
// executes several TypeScript handlers: both modules are bundled by the single
// combined build RUN and both run through the reused image.
func TestIntegrationNodeTSMultipleHandlersEndToEnd(t *testing.T) {
	testutil.RequireDocker(t)

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
  - handler: events.deleted.handler
    pattern:
      event_name: [REMOVE]
`)
	if err := os.MkdirAll(filepath.Join(dir, "events"), 0o755); err != nil {
		t.Fatalf("mkdir events: %v", err)
	}
	writeFile(t, dir, "events/created.ts", `
export function handler(event: { event_id: string }): void {
  console.log("created ts " + event.event_id);
}
`)
	writeFile(t, dir, "events/deleted.ts", `
export function handler(event: { event_id: string }): void {
  console.log("deleted ts " + event.event_id);
}
`)

	fn := function.Function{
		Name:     "node-ts-multi-e2e",
		Dir:      dir,
		Template: tsTemplate("events.created.handler", "events.deleted.handler"),
	}
	m, _ := newManager(t)
	out := newFunctionOutputSink(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if err := m.Execute(ctx, prepared, "events.created.handler", []byte(`{"event_id":"1"}`), nil); err != nil {
		t.Fatalf("execute events.created.handler: %v", err)
	}
	if err := m.Execute(ctx, prepared, "events.deleted.handler", []byte(`{"event_id":"2"}`), nil); err != nil {
		t.Fatalf("execute events.deleted.handler: %v", err)
	}

	awaitFunctionOutput(t, ctx, out, "created ts 1", "deleted ts 2")
}

// TestIntegrationNodeTSAmbiguousFailsPrepare pins the build-time ambiguity error:
// a module with both a .js and a .ts source fails Prepare instead of silently
// shadowing one with the other's transpiled output.
func TestIntegrationNodeTSAmbiguousFailsPrepare(t *testing.T) {
	testutil.RequireDocker(t)

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: x.handler
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "x.js", "export function handler(e) {}\n")
	writeFile(t, dir, "x.ts", "export function handler(e: unknown): void {}\n")

	fn := function.Function{Name: "node-ts-ambiguous-e2e", Dir: dir, Template: tsTemplate("x.handler")}
	m, _ := newManager(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	_, err := m.Prepare(ctx, fn)
	if err == nil {
		t.Fatal("expected Prepare to fail for an ambiguous handler module")
	}
	if !strings.Contains(err.Error(), "ambiguous") {
		t.Errorf("error = %v, want it to mention ambiguity", err)
	}
}

// TestIntegrationNodeTSMissingFailsPrepare pins the build-time missing-module
// error: a declared handler with no source file fails Prepare clearly instead of
// every invocation failing later with module-not-found.
func TestIntegrationNodeTSMissingFailsPrepare(t *testing.T) {
	testutil.RequireDocker(t)

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
`)

	fn := function.Function{Name: "node-ts-missing-e2e", Dir: dir, Template: tsTemplate("events.created.handler")}
	m, _ := newManager(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	_, err := m.Prepare(ctx, fn)
	if err == nil {
		t.Fatal("expected Prepare to fail for a missing handler module")
	}
	if !strings.Contains(err.Error(), "events.created") {
		t.Errorf("error = %v, want it to name the missing module", err)
	}
}

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
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// This python build creates a relay-dep-* layer from its requirements.txt.
	// Clean only the dep images this test adds (delta vs snapshot, so layers
	// built concurrently by other tests/workers are untouched), and register it
	// BEFORE Prepare so it also runs on failure and never leaks into the sibling
	// dep-layer tests that follow on the shared daemon.
	depBefore := depTagSet(ctx, cli)
	t.Cleanup(cleanupNewDepImagesSince(cli, depBefore))

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
	writeFile(t, dir, "requirements.txt", "# no deps\n")

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
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// This python build creates a relay-dep-* layer from its requirements.txt.
	// Clean only the dep images this test adds (delta vs snapshot, so layers
	// built concurrently by other tests/workers are untouched), and register it
	// BEFORE Prepare so it also runs on failure and never leaks into the sibling
	// dep-layer tests that follow on the shared daemon.
	depBefore := depTagSet(ctx, cli)
	t.Cleanup(cleanupNewDepImagesSince(cli, depBefore))

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
	writeFile(t, dir, "requirements.txt", "# no deps\n")

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

	logs := out.String()
	for _, want := range []string{
		"User created: user_123",
		"User updated: user_123",
		"User deleted: user_123",
	} {
		if !strings.Contains(logs, want) {
			t.Errorf("expected stdout to contain %q, got: %s", want, logs)
		}
	}
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

	logs := out.String()
	if want := "Sending welcome email to john@example.com"; !strings.Contains(logs, want) {
		t.Errorf("expected stdout to contain %q, got: %s", want, logs)
	}
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
	logs := out.String()
	if !strings.Contains(logs, "missing-package") {
		t.Errorf("expected the real import error mentioning 'missing-package' in logs, got: %s", logs)
	}
	t.Logf("execute error: %v", err)
	t.Logf("container logs: %s", logs)
}

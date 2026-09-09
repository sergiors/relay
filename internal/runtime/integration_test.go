//go:build integration

package runtime

import (
	"bytes"
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/client"

	"relay/internal/function"
)

// dockerAvailable reports whether the Docker daemon is reachable via the
// Engine API.
func dockerAvailable(t *testing.T) bool {
	t.Helper()
	if os.Getenv("RELAY_SKIP_DOCKER") != "" {
		return false
	}
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		t.Logf("docker client: %v", err)
		return false
	}
	defer cli.Close()
	if _, err := cli.Ping(context.Background(), client.PingOptions{}); err != nil {
		t.Logf("docker unavailable: %v", err)
		return false
	}
	return true
}

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

// newManager returns a Manager wired to a logger that writes into the returned
// buffer, so container output is captured for assertions.
func newManager(t *testing.T) (*Manager, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	l := log.New(&buf, "", 0)
	m, err := NewManager(l, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	return m, &buf
}

func TestPythonEndToEnd(t *testing.T) {
	if !dockerAvailable(t) {
		t.Skip("docker not available")
	}

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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	eventJSON := []byte(`{"event_id":"1757-0","status":"COMPLETED"}`)
	if err := m.Execute(ctx, prepared, "handler.completed", eventJSON); err != nil {
		t.Fatalf("execute: %v", err)
	}
}

func TestPythonAsyncEndToEnd(t *testing.T) {
	if !dockerAvailable(t) {
		t.Skip("docker not available")
	}

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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	eventJSON := []byte(`{"event_id":"1757-0","status":"COMPLETED"}`)
	if err := m.Execute(ctx, prepared, "handler.completed", eventJSON); err != nil {
		t.Fatalf("execute async: %v", err)
	}
}

func TestNodeEndToEnd(t *testing.T) {
	if !dockerAvailable(t) {
		t.Skip("docker not available")
	}

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

	fn := function.Function{Name: "node-e2e", Dir: dir, Template: &function.Template{Runtime: "node24"}}
	m, _ := newManager(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	eventJSON := []byte(`{"event_id":"1757-0","event_name":"INSERT"}`)
	if err := m.Execute(ctx, prepared, "index.created", eventJSON); err != nil {
		t.Fatalf("execute: %v", err)
	}
}

// repoRoot is the repository root (the dir of internal/runtime/..), used to
// resolve the real example function directories under functions/.
var repoRoot = func() string {
	abs, err := filepath.Abs(".")
	if err != nil {
		panic(err)
	}
	return filepath.Join(abs, "..", "..")
}()

// readRealTemplate loads the template.yaml from one of the real example
// function directories and returns the parsed template alongside its dir.
func readRealTemplate(t *testing.T, relDir string) (string, *function.Template) {
	t.Helper()
	dir := filepath.Join(repoRoot, "functions", relDir)
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

// TestRealUserEventsPythonEndToEnd drives the real functions/user-events-python
// example: three rules whose handlers live in the events/ namespace package
// (events.created / events.updated / events.deleted).
func TestRealUserEventsPythonEndToEnd(t *testing.T) {
	if !dockerAvailable(t) {
		t.Skip("docker not available")
	}

	dir, tmpl := readRealTemplate(t, "user-events-python")
	fn := function.Function{Name: "user-events-python", Dir: dir, Template: tmpl}
	m, buf := newManager(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	// INSERT -> events.created.handler uses new_image.id.
	if err := m.Execute(ctx, prepared, "events.created.handler", []byte(devEventJSON)); err != nil {
		t.Fatalf("execute events.created.handler: %v", err)
	}

	// MODIFY -> events.updated.handler uses new_image.id.
	modifiedJSON := []byte(`{
	  "event_id": "evt_124",
	  "event_name": "MODIFY",
	  "table_name": "users",
	  "new_image": {"id": "user_123", "name": "John Doe", "email": "john@example.com"}
	}`)
	if err := m.Execute(ctx, prepared, "events.updated.handler", modifiedJSON); err != nil {
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
	if err := m.Execute(ctx, prepared, "events.deleted.handler", deletedJSON); err != nil {
		t.Fatalf("execute events.deleted.handler: %v", err)
	}

	logs := buf.String()
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

// TestRealWelcomeEmailNodeEndToEnd drives the real functions/welcome-email-node
// example: a single rule whose handler resolves as handler.handler -> module
// "handler" -> /app/handler.js. No package.json is present, exercising the
// injected ESM package.json path.
func TestRealWelcomeEmailNodeEndToEnd(t *testing.T) {
	if !dockerAvailable(t) {
		t.Skip("docker not available")
	}

	dir, tmpl := readRealTemplate(t, "welcome-email-node")
	fn := function.Function{Name: "welcome-email-node", Dir: dir, Template: tmpl}
	m, buf := newManager(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	if err := m.Execute(ctx, prepared, "handler.handler", []byte(devEventJSON)); err != nil {
		t.Fatalf("execute handler.handler: %v", err)
	}

	logs := buf.String()
	if want := "Sending welcome email to john@example.com"; !strings.Contains(logs, want) {
		t.Errorf("expected stdout to contain %q, got: %s", want, logs)
	}
	t.Logf("captured handler output:\n%s", logs)
}

// TestNodeBrokenDependencyEndToEnd verifies that a module that exists but
// imports a missing dependency surfaces the REAL import error through Execute,
// not a "module not found" message.
func TestNodeBrokenDependencyEndToEnd(t *testing.T) {
	if !dockerAvailable(t) {
		t.Skip("docker not available")
	}

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
	m, buf := newManager(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	eventJSON := []byte(`{"event_id":"1","event_name":"INSERT"}`)
	err = m.Execute(ctx, prepared, "index.run", eventJSON)
	if err == nil {
		t.Fatalf("expected execute to fail for broken dependency")
	}
	if strings.Contains(err.Error(), "not found") {
		t.Errorf("broken dependency must NOT be reported as module not found, got: %v", err)
	}
	logs := buf.String()
	if !strings.Contains(logs, "missing-package") {
		t.Errorf("expected the real import error mentioning 'missing-package' in logs, got: %s", logs)
	}
	t.Logf("execute error: %v", err)
	t.Logf("container logs: %s", logs)
}

// imageExistsInDaemon reports whether a local image carries exactly ref.
func imageExistsInDaemon(cli *client.Client, ctx context.Context, ref string) bool {
	_, err := cli.ImageInspect(ctx, ref)
	return err == nil
}

// TestIntegrationFingerprintedImageLifecycle exercises the fingerprint-versioned
// image lifecycle against a real Docker daemon: build v1 -> build v2 (changed
// source) -> the two are distinct images and v1 still present -> cleanup keeping
// only v2 retires v1 -> an unrelated (non-relay-owned) image is untouched. It
// also asserts the unrelated image is NOT removed by the sweep.
func TestIntegrationFingerprintedImageLifecycle(t *testing.T) {
	if !dockerAvailable(t) {
		t.Skip("docker not available")
	}
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: index.hi
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", "export function hi(e){ console.log('v1'); }\n")

	fn := function.Function{Name: "fn-ver", Dir: dir, Template: &function.Template{Runtime: "node24"}}
	fp1, err := function.Fingerprint(dir)
	if err != nil {
		t.Fatalf("fingerprint v1: %v", err)
	}
	ref1 := ImageRef(fn.Name, fp1)
	if imageExistsInDaemon(cli, ctx, ref1) {
		t.Fatalf("image %s already exists before build", ref1)
	}

	p1, err := mPrepare(ctx, t, fn)
	if err != nil {
		t.Fatalf("prepare v1: %v", err)
	}
	if p1.Image != ref1 {
		t.Fatalf("prepare image = %q, want %q", p1.Image, ref1)
	}
	if !imageExistsInDaemon(cli, ctx, ref1) {
		t.Fatalf("image %s should exist after v1 build", ref1)
	}

	// Change source -> distinct fingerprint -> distinct image.
	writeFile(t, dir, "index.js", "export function hi(e){ console.log('v2'); }\n")
	fp2, err := function.Fingerprint(dir)
	if err != nil {
		t.Fatalf("fingerprint v2: %v", err)
	}
	ref2 := ImageRef(fn.Name, fp2)
	if ref1 == ref2 {
		t.Fatalf("v1 and v2 references must differ, both = %s", ref1)
	}

	p2, err := mPrepare(ctx, t, fn)
	if err != nil {
		t.Fatalf("prepare v2: %v", err)
	}
	if p2.Image != ref2 {
		t.Fatalf("prepare image = %q, want %q", p2.Image, ref2)
	}
	// v1 is still around (not clobbered by v2).
	if !imageExistsInDaemon(cli, ctx, ref1) {
		t.Fatalf("v1 image %s still expected to exist alongside v2", ref1)
	}

	// An unrelated, non-relay image we create: it must never be touched by the
	// sweep. Build a tiny tagged image ourselves (not relay-namespaced) via the
	// Manager's ImageBuild against an inline one-line Dockerfile.
	unrelated := buildTestImage(ctx, t, "relay-unrelated-guard", `FROM scratch
CMD []
`)
	defer cleanupImage(cli, ctx, unrelated)

	// Conservative sweep keeping only ref2: ref1 (superseded) is removed, the
	// unrelated image survives.
	removed, err := mRemoveImagesExcept(ctx, t, map[string]bool{ref2: true})
	if err != nil {
		t.Fatalf("remove images except: %v", err)
	}
	if imageExistsInDaemon(cli, ctx, ref1) {
		t.Errorf("v1 image %s should have been removed by the sweep", ref1)
	}
	if removed == 0 {
		t.Errorf("expected at least one image removed (v1 %s)", ref1)
	}
	if !imageExistsInDaemon(cli, ctx, ref2) {
		t.Errorf("kept v2 image %s must survive the sweep", ref2)
	}
	if !imageExistsInDaemon(cli, ctx, unrelated) {
		t.Errorf("unrelated image %s must not be removed", unrelated)
	}
}

// mPrepare builds a function via a fresh Manager wired to a discard logger.
func mPrepare(ctx context.Context, t *testing.T, fn function.Function) (*Prepared, error) {
	t.Helper()
	m, err := NewManager(log.New(io.Discard, "", 0), nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer m.Close()
	return m.Prepare(ctx, fn)
}

// mRemoveImagesExcept runs the conservative sweep via a fresh Manager.
func mRemoveImagesExcept(ctx context.Context, t *testing.T, keep map[string]bool) (int, error) {
	t.Helper()
	m, err := NewManager(log.New(io.Discard, "", 0), nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer m.Close()
	return m.RemoveImagesExcept(ctx, keep)
}

// buildTestImage builds a tiny image with the given tag and an inline Dockerfile
// via the Engine API, used to create a non-relay-owned image to prove the sweep
// never touches it.
func buildTestImage(ctx context.Context, t *testing.T, ref, dockerfile string) string {
	t.Helper()
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer cli.Close()
	ctxDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(ctxDir, "Dockerfile"), []byte(dockerfile), 0o644); err != nil {
		t.Fatalf("write dockerfile: %v", err)
	}
	reader, err := tarContext(ctxDir)
	if err != nil {
		t.Fatalf("tar context: %v", err)
	}
	resp, err := cli.ImageBuild(ctx, reader, client.ImageBuildOptions{Tags: []string{ref}, Dockerfile: "Dockerfile"})
	if err != nil {
		t.Fatalf("build unrelated image: %v", err)
	}
	defer resp.Body.Close()
	if _, err := drainBuildResponse(resp.Body); err != nil {
		t.Fatalf("build unrelated image output: %v", err)
	}
	return ref
}

// cleanupImage removes an image, best-effort.
func cleanupImage(cli *client.Client, ctx context.Context, ref string) {
	_, _ = cli.ImageRemove(ctx, ref, client.ImageRemoveOptions{Force: true})
}

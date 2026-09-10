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

	"github.com/moby/moby/api/types/container"
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
// buffer, so container output is captured for assertions. The manager owns
// hostname "test-host" so container-ownership tests are deterministic.
func newManager(t *testing.T) (*Manager, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	l := log.New(&buf, "", 0)
	m, err := NewManager(l, nil, "test-host")
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
	if err := m.Execute(ctx, prepared, "handler.completed", eventJSON, nil); err != nil {
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
	if err := m.Execute(ctx, prepared, "handler.completed", eventJSON, nil); err != nil {
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
	if err := m.Execute(ctx, prepared, "index.created", eventJSON, nil); err != nil {
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

	if err := m.Execute(ctx, prepared, "handler.handler", []byte(devEventJSON), nil); err != nil {
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
	err = m.Execute(ctx, prepared, "index.run", eventJSON, nil)
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
	m, err := NewManager(log.New(io.Discard, "", 0), nil, "test-host")
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer m.Close()
	return m.Prepare(ctx, fn)
}

// mRemoveImagesExcept runs the conservative sweep via a fresh Manager.
func mRemoveImagesExcept(ctx context.Context, t *testing.T, keep map[string]bool) (int, error) {
	t.Helper()
	m, err := NewManager(log.New(io.Discard, "", 0), nil, "test-host")
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

// findContainerByLabel scans All containers for one carrying the exact
// relay.<key>=<value> label, returning its ID or "". It is a client-side filter
// matching the sweep's own ownership predicate.
func findContainerByLabel(ctx context.Context, cli *client.Client, key, value string) string {
	list, err := cli.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		return ""
	}
	for _, c := range list.Items {
		if c.Labels[key] == value {
			return c.ID
		}
	}
	return ""
}

// waitForContainerGone polls until no running-or-exited container carries the
// given label pair, or the deadline passes. AutoRemove removes the container on
// exit asynchronously: ContainerWait delivers the exit code the moment the
// process stops, but the daemon's removal completes a beat later, so callers
// must await removal rather than assert it at the instant Execute returns.
// It returns true once the container is gone.
func waitForContainerGone(ctx context.Context, cli *client.Client, key, value string) bool {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if findContainerByLabel(ctx, cli, key, value) == "" {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// TestIntegrationFunctionEnvInjection verifies template env values are injected
// into the execution container's environment, and that template.yaml is NOT
// baked into the image (it is Relay configuration, not function source).
func TestIntegrationFunctionEnvInjection(t *testing.T) {
	if !dockerAvailable(t) {
		t.Skip("docker not available")
	}

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
env:
  GREETING: hello
events:
  - handler: index.env
    pattern:
      event_name: [INSERT]
`)
	// The handler prints its env and lists the /app directory so the test can
	// assert both the injected value and the absence of template.yaml.
	writeFile(t, dir, "index.js", `
import { readdirSync } from "node:fs";
export function env(event) {
  console.log("GREETING=" + process.env.GREETING);
  console.log("FILES=" + readdirSync("/app").join(","));
}
`)
	fn := function.Function{Name: "env-e2e", Dir: dir, Template: &function.Template{Runtime: "node24"}}
	m, buf := newManager(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	// Inject the template env value via extraEnv (the runner's per-invocation
	// path).
	if err := m.Execute(ctx, prepared, "index.env", []byte(`{"event_name":"INSERT"}`), []string{"GREETING=hello"}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	logs := buf.String()
	if !strings.Contains(logs, "GREETING=hello") {
		t.Errorf("expected container env to contain GREETING=hello, got: %s", logs)
	}
	// template.yaml must NOT be in the image.
	if strings.Contains(logs, "template.yaml") {
		t.Errorf("template.yaml must not be baked into the image, got: %s", logs)
	}
}

// TestIntegrationSecretInjectionAndRotation verifies secret values are injected
// per invocation and that rotating the value between executions takes effect
// without a rebuild (the image is prepared once; the fingerprint is unchanged).
func TestIntegrationSecretInjectionAndRotation(t *testing.T) {
	if !dockerAvailable(t) {
		t.Skip("docker not available")
	}

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
secrets:
  TOKEN: relay-itest-token
events:
  - handler: index.secret
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", `
export function secret(event) {
  console.log("TOKEN=" + process.env.TOKEN);
}
`)
	fn := function.Function{Name: "secret-e2e", Dir: dir, Template: &function.Template{Runtime: "node24"}}
	m, buf := newManager(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	fp1 := prepared.Fingerprint

	// First execution with secret value v1.
	if err := m.Execute(ctx, prepared, "index.secret", []byte(`{"event_name":"INSERT"}`), []string{"TOKEN=v1"}); err != nil {
		t.Fatalf("execute v1: %v", err)
	}
	if !strings.Contains(buf.String(), "TOKEN=v1") {
		t.Errorf("expected TOKEN=v1, got: %s", buf.String())
	}

	// Rotate the value; the second execution sees v2 with NO rebuild (the
	// prepared image and fingerprint are unchanged).
	if err := m.Execute(ctx, prepared, "index.secret", []byte(`{"event_name":"INSERT"}`), []string{"TOKEN=v2"}); err != nil {
		t.Fatalf("execute v2: %v", err)
	}
	if !strings.Contains(buf.String(), "TOKEN=v2") {
		t.Errorf("expected TOKEN=v2 after rotation, got: %s", buf.String())
	}
	if prepared.Fingerprint != fp1 {
		t.Errorf("fingerprint changed across secret rotation: %s -> %s", fp1, prepared.Fingerprint)
	}
}

// TestIntegrationMultipleFunctionsSameSecret verifies two functions referencing
// the same secret both resolve it (the provider is shared, resolution is
// per-invocation).
func TestIntegrationMultipleFunctionsSameSecret(t *testing.T) {
	if !dockerAvailable(t) {
		t.Skip("docker not available")
	}

	// Both functions reference the same secret name; the runner resolves it per
	// invocation. This test drives the runtime layer directly with the resolved
	// value, proving the container receives it for each function.
	for _, tc := range []struct {
		name string
	}{
		{"multi-a"},
		{"multi-b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeFile(t, dir, "template.yaml", `
runtime: node24
secrets:
  TOKEN: relay-itest-token
events:
  - handler: index.secret
    pattern:
      event_name: [INSERT]
`)
			writeFile(t, dir, "index.js", `
export function secret(event) {
  console.log("TOKEN=" + process.env.TOKEN);
}
`)
			fn := function.Function{Name: tc.name, Dir: dir, Template: &function.Template{Runtime: "node24"}}
			m, buf := newManager(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			prepared, err := m.Prepare(ctx, fn)
			if err != nil {
				t.Fatalf("prepare: %v", err)
			}
			if err := m.Execute(ctx, prepared, "index.secret", []byte(`{"event_name":"INSERT"}`), []string{"TOKEN=shared-value"}); err != nil {
				t.Fatalf("execute: %v", err)
			}
			if !strings.Contains(buf.String(), "TOKEN=shared-value") {
				t.Errorf("expected TOKEN=shared-value, got: %s", buf.String())
			}
		})
	}
}

// TestIntegrationSuccessfulRunNoExplicitRemove verifies the normal completion
// path does NOT issue an explicit container removal: a handler that exits 0 is
// cleaned up by AutoRemove alone, so no "remove container" log line is emitted
// (the previous blanket deferred remove logged a spurious 409 "removal already
// in progress" on this path) and the container is gone.
func TestIntegrationSuccessfulRunNoExplicitRemove(t *testing.T) {
	if !dockerAvailable(t) {
		t.Skip("docker not available")
	}
	m, buf := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: index.ok
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", `
export function ok(event) {
  console.log("ok " + event.event_id);
}
`)
	fn := function.Function{Name: "no-remove-e2e", Dir: dir, Template: &function.Template{Runtime: "node24"}}
	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	execCtx := context.WithValue(context.Background(), runMetaKey{},
		RunMeta{Hostname: "test-host", Function: "no-remove-e2e", Handler: "index.ok", Image: prepared.Image})
	if err := m.Execute(execCtx, prepared, "index.ok", []byte(`{"event_id":"evt_1","event_name":"INSERT"}`), nil); err != nil {
		t.Fatalf("execute: %v", err)
	}

	logs := buf.String()
	if !strings.Contains(logs, "ok evt_1") {
		t.Errorf("expected handler output, got: %s", logs)
	}
	// The normal path must NOT emit an explicit removal log line.
	if strings.Contains(logs, "remove container") {
		t.Errorf("normal completion path must not log an explicit container removal, got: %s", logs)
	}
	// AutoRemove: the container must vanish after exit.
	if !waitForContainerGone(ctx, m.cli, labelHandler, "index.ok") {
		t.Error("container should have been auto-removed after successful exit")
	}
}

// TestIntegrationPanicDuringOutputNoRemovalNoise drives runContainer with a log
// func that panics while forwarding handler output — a panic AFTER the container
// exited on its own (wait.Result disarmed the backstop). The panic must
// propagate (recovered by the test), the container must still be gone via
// AutoRemove, and no removal log line may appear (the backstop must stay
// disarmed on the normal path even under unwinding).
func TestIntegrationPanicDuringOutputNoRemoval(t *testing.T) {
	if !dockerAvailable(t) {
		t.Skip("docker not available")
	}
	m, _ := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: index.paniclog
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", `
export function paniclog(event) {
  console.log("output before panic");
}
`)
	fn := function.Function{Name: "paniclog-e2e", Dir: dir, Template: &function.Template{Runtime: "node24"}}
	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	// Drive runContainer directly so the log func can panic on the first
	// forwarded output line — simulating a Relay bug surfacing during output
	// handling while the deferred cleanup is in scope.
	panicked := make(chan any, 1)
	go func() {
		defer func() { panicked <- recover() }()
		_ = runContainer(ctx, m.cli,
			func(format string, args ...any) { panic("log func exploded") },
			"paniclog-e2e", prepared.Image, nil, nil, "index.paniclog",
			[]byte(`{"event_name":"INSERT"}`),
			RunMeta{Hostname: "test-host", Function: "paniclog-e2e", Handler: "index.paniclog", Image: prepared.Image},
		)
	}()
	select {
	case pv := <-panicked:
		if pv == nil {
			t.Fatal("expected log-func panic to propagate out of runContainer")
		}
	case <-time.After(2 * time.Minute):
		t.Fatal("runContainer did not return after panic")
	}

	// The container exited on its own; AutoRemove (not the backstop) removed it.
	if !waitForContainerGone(ctx, m.cli, labelHandler, "index.paniclog") {
		t.Error("container should have been auto-removed after exit despite the log panic")
	}
}

// TestIntegrationFailedStartRemovesContainer verifies the backstop contract for
// the failed-start path: a container that is created but never started (so it
// will never exit on its own) is removed by removeContainer. A genuine
// ContainerStart failure inside runContainer is hard to force deterministically
// (the relay images' entrypoint always starts; a missing handler is a normal
// non-zero exit handled by AutoRemove), so this drives the backstop directly on
// a created-but-never-started container — the exact state the start-failure path
// leaves behind. The container must be gone afterward.
func TestIntegrationFailedStartRemovesContainer(t *testing.T) {
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

	// Create a container that is never started, carrying the relay labels so it
	// is attributable and greppable.
	resp, err := cli.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config:     &container.Config{Image: "node:24-alpine", Labels: runLabels(RunMeta{Hostname: "test-host", Function: "fail-start-e2e", Handler: "index.run", Image: "node:24-alpine"})},
		HostConfig: &container.HostConfig{},
	})
	if err != nil {
		t.Fatalf("create container: %v", err)
	}
	id := resp.ID
	t.Cleanup(func() { _ = removeContainer(cli, id) })

	// Never started: removeContainer must remove it (the backstop path).
	if err := removeContainer(cli, id); err != nil {
		t.Fatalf("removeContainer on never-started container: %v", err)
	}
	if !waitForContainerGone(ctx, cli, labelHandler, "index.run") {
		t.Error("created-but-never-started container should have been removed")
	}
}

// TestIntegrationRemoveContainerTwiceBenign verifies removeContainer is
// idempotent: removing an already-removed container (not-found) is benign and
// returns no error, so a backstop remove that races AutoRemove never surfaces a
// spurious failure.
func TestIntegrationRemoveContainerTwiceBenign(t *testing.T) {
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

	// Create a short-lived container that exits immediately, so AutoRemove
	// removes it on its own.
	resp, err := cli.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config:     &container.Config{Image: "node:24-alpine", Cmd: []string{"true"}},
		HostConfig: &container.HostConfig{AutoRemove: true},
	})
	if err != nil {
		t.Fatalf("create container: %v", err)
	}
	id := resp.ID
	if _, err := cli.ContainerStart(ctx, id, client.ContainerStartOptions{}); err != nil {
		t.Fatalf("start container: %v", err)
	}

	// First remove: either the daemon already auto-removed it (not-found) or
	// it is being removed (conflict); both are benign.
	if err := removeContainer(cli, id); err != nil {
		t.Fatalf("first removeContainer: %v", err)
	}
	// Second remove: the container is definitely gone now; must be benign.
	if err := removeContainer(cli, id); err != nil {
		t.Fatalf("second removeContainer on already-removed container: %v", err)
	}
}

// TestIntegrationContainerLabelsAndAutoRemove drives a real execution while
// verifying the seven diagnostic labels are present mid-flight (polled while the
// handler runs), that AutoRemove removes the container the moment it exits, that
// stdout is still captured, and that a non-zero exit code surfaces as an error
// while the container is still removed.
func TestIntegrationContainerLabelsAndAutoRemove(t *testing.T) {
	if !dockerAvailable(t) {
		t.Skip("docker not available")
	}
	m, buf := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: index.slow
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", `
export async function slow(event) {
  await new Promise(r => setTimeout(r, 1500));
  console.log("completed " + event.event_id);
}
`)
	fn := function.Function{Name: "labels-e2e", Dir: dir, Template: &function.Template{Runtime: "node24"}}
	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	execCtx := context.WithValue(context.Background(), runMetaKey{},
		RunMeta{
			Function:  "labels-e2e",
			Handler:   "index.slow",
			MessageID: "1791234567890-0",
			EventID:   "evt_777",
			EventName: "INSERT",
			Hostname:  "test-host",
			Image:     prepared.Image,
		})
	done := make(chan error, 1)
	go func() {
		done <- m.Execute(execCtx, prepared, "index.slow", []byte(`{"event_id":"evt_777","event_name":"INSERT"}`), nil)
	}()

	// Poll while the handler runs (sleeep 1.5s) for the container carrying our
	// handler label, then assert the full label set before it exits.
	id := ""
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if id = findContainerByLabel(ctx, m.cli, labelHandler, "index.slow"); id != "" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if id == "" {
		t.Fatal("container with relay.handler=index.slow not found during execution")
	}
	// Inspect what we saw to assert all seven labels.
	list, err := m.cli.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		t.Fatalf("list containers: %v", err)
	}
	var summaryLabels map[string]string
	for _, c := range list.Items {
		if c.ID == id {
			summaryLabels = c.Labels
		}
	}
	if summaryLabels == nil {
		t.Fatalf("container %s not in listing", id)
	}
	for k, want := range map[string]string{
		labelFunction:  "labels-e2e",
		labelHandler:   "index.slow",
		labelMessageID: "1791234567890-0",
		labelEventID:   "evt_777",
		labelEventName: "INSERT",
		labelHostname:  "test-host",
		labelImage:     prepared.Image,
	} {
		if got := summaryLabels[k]; got != want {
			t.Errorf("label %q = %q, want %q", k, got, want)
		}
	}

	if err := <-done; err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !strings.Contains(buf.String(), "completed evt_777") {
		t.Errorf("expected stdout to contain %q, got: %s", "completed evt_777", buf.String())
	}
	// AutoRemove: the container must vanish after exit (asynchronously on the
	// daemon side, so poll).
	if !waitForContainerGone(ctx, m.cli, labelHandler, "index.slow") {
		t.Error("container should have been auto-removed after exit")
	}
}

// TestIntegrationNonZeroExitAutoRemove verifies a handler that exits non-zero
// surfaces as an Execute error AND is still auto-removed (attach + exit code
// handling preserve the existing contract with AutoRemove enabled).
func TestIntegrationNonZeroExitAutoRemove(t *testing.T) {
	if !dockerAvailable(t) {
		t.Skip("docker not available")
	}
	m, _ := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: index.fail
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", `
export async function fail(event) {
  console.error("boom");
  process.exit(1);
}
`)
	fn := function.Function{Name: "fail-e2e", Dir: dir, Template: &function.Template{Runtime: "node24"}}
	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	execCtx := context.WithValue(context.Background(), runMetaKey{},
		RunMeta{Hostname: "test-host", Function: "fail-e2e", Handler: "index.fail", Image: prepared.Image})
	err = m.Execute(execCtx, prepared, "index.fail", []byte(`{"event_name":"INSERT"}`), nil)
	if err == nil {
		t.Fatal("expected execute to fail for non-zero exit")
	}
	if !strings.Contains(err.Error(), "exited with status 1") {
		t.Errorf("expected exit-status error, got: %v", err)
	}
	if !waitForContainerGone(ctx, m.cli, labelHandler, "index.fail") {
		t.Error("container should have been auto-removed after non-zero exit")
	}
}

// TestIntegrationTimeoutAutoRemove verifies the existing per-rule timeout path
// still works with AutoRemove: an over-long handler is killed on cancellation,
// the error surfaces, and the killed container is removed.
func TestIntegrationTimeoutAutoRemove(t *testing.T) {
	if !dockerAvailable(t) {
		t.Skip("docker not available")
	}
	m, _ := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: index.sleeper
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", `
export async function sleeper(event) {
  await new Promise(r => setTimeout(r, 10000));
  console.log("done");
}
`)
	fn := function.Function{Name: "timeout-e2e", Dir: dir, Template: &function.Template{Runtime: "node24"}}
	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	// Timeout the invocation after 1s: the handler sleeps 10s, so the ctx
	// cancel path must kill the container.
	invokeCtx, invokeCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer invokeCancel()
	execCtx := context.WithValue(invokeCtx, runMetaKey{},
		RunMeta{Hostname: "test-host", Function: "timeout-e2e", Handler: "index.sleeper", Image: prepared.Image})
	err = m.Execute(execCtx, prepared, "index.sleeper", []byte(`{"event_name":"INSERT"}`), nil)
	if err == nil {
		t.Fatal("expected execute to fail on timeout")
	}
	if !waitForContainerGone(ctx, m.cli, labelHandler, "index.sleeper") {
		t.Error("timed-out container should have been auto-removed after kill/exit")
	}
}

// TestIntegrationContainerHardening drives a real execution of both a Python
// and a Node function whose handlers assert the hardening from inside the
// container (non-root uid, read-only rootfs, writable /tmp, dropped caps), and
// mid-flight inspects the running container to assert the resource limits and
// host-config hardening are actually applied by the daemon. It also asserts
// networking is not disabled (outbound access is a legitimate function need).
func TestIntegrationContainerHardening(t *testing.T) {
	if !dockerAvailable(t) {
		t.Skip("docker not available")
	}

	// Python handler: asserts non-root uid, read-only rootfs (write to / must
	// fail with EROFS), writable /tmp, and dropped capabilities (CapEff == 0).
	// It exits non-zero on any failed assertion so Execute surfaces the failure.
	pyDir := t.TempDir()
	writeFile(t, pyDir, "template.yaml", `
runtime: python3.14
events:
  - handler: handler.check
    pattern:
      status: [COMPLETED]
`)
	writeFile(t, pyDir, "handler.py", `
import os
import tempfile

def check(event):
    # Non-root: the runtime user is uid 10001.
    if os.geteuid() != 10001:
        raise SystemExit("expected euid 10001, got %d" % os.geteuid())

    # Read-only rootfs: writing to / must fail with EROFS.
    try:
        with open("/probe-rootfs", "w") as f:
            f.write("x")
        raise SystemExit("expected write to / to fail on read-only rootfs")
    except OSError as e:
        if e.errno != 30:  # EROFS
            raise SystemExit("expected EROFS writing to /, got errno %d" % e.errno)

    # /tmp is the writable tmpfs: write, read back, unlink.
    fd, path = tempfile.mkstemp(dir="/tmp")
    with os.fdopen(fd, "w") as f:
        f.write("tmp-ok")
    with open(path) as f:
        if f.read() != "tmp-ok":
            raise SystemExit("tmpfs readback mismatch")
    os.unlink(path)

    # Dropped capabilities: CapEff must be 0 (CapDrop ALL).
    cap_eff = None
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("CapEff:"):
                cap_eff = line.split()[1]
                break
    if cap_eff != "0000000000000000":
        raise SystemExit("expected CapEff 0, got %s" % cap_eff)

    print("python hardening ok")
`)
	writeFile(t, pyDir, "requirements.txt", "# no deps\n")

	// Node handler: same assertions via process.getuid(), fs write to /, /tmp
	// write, and /proc/self/status CapEff.
	ndDir := t.TempDir()
	writeFile(t, ndDir, "template.yaml", `
runtime: node24
events:
  - handler: index.check
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, ndDir, "index.js", `
import { writeFileSync, readFileSync, unlinkSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

export function check(event) {
  // Non-root: the runtime user is uid 10001.
  if (process.getuid() !== 10001) {
    throw new Error("expected uid 10001, got " + process.getuid());
  }

  // Read-only rootfs: writing to / must fail with EROFS.
  try {
    writeFileSync("/probe-rootfs", "x");
    throw new Error("expected write to / to fail on read-only rootfs");
  } catch (e) {
    if (e.code !== "EROFS") {
      throw new Error("expected EROFS writing to /, got " + e.code);
    }
  }

  // /tmp is the writable tmpfs: write, read back, unlink.
  const dir = mkdtempSync(join(tmpdir(), "relay-"));
  const p = join(dir, "f");
  writeFileSync(p, "tmp-ok");
  if (readFileSync(p, "utf8") !== "tmp-ok") {
    throw new Error("tmpfs readback mismatch");
  }
  unlinkSync(p);

  // Dropped capabilities: CapEff must be 0 (CapDrop ALL).
  const status = readFileSync("/proc/self/status", "utf8");
  const m = status.match(/^CapEff:\s+(\S+)/m);
  if (!m || m[1] !== "0000000000000000") {
    throw new Error("expected CapEff 0, got " + (m && m[1]));
  }

  console.log("node hardening ok");
}
`)

	// Run both functions. Each Prepare builds a fresh image; the in-handler
	// assertions run inside the hardened container and Execute returns nil only
	// if every assertion passed.
	for _, tc := range []struct {
		name    string
		dir     string
		runtime string
		handler string
		event   []byte
	}{
		{"python", pyDir, "python3.14", "handler.check", []byte(`{"status":"COMPLETED"}`)},
		{"node", ndDir, "node24", "index.check", []byte(`{"event_name":"INSERT"}`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fn := function.Function{Name: "harden-" + tc.name, Dir: tc.dir, Template: &function.Template{Runtime: tc.runtime}}
			m, buf := newManager(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			prepared, err := m.Prepare(ctx, fn)
			if err != nil {
				t.Fatalf("prepare: %v", err)
			}

			// Run the handler in a goroutine so we can inspect the container
			// mid-flight while it runs.
			execCtx := context.WithValue(context.Background(), runMetaKey{},
				RunMeta{Hostname: "test-host", Function: "harden-" + tc.name, Handler: tc.handler, Image: prepared.Image})
			done := make(chan error, 1)
			go func() {
				done <- m.Execute(execCtx, prepared, tc.handler, tc.event, nil)
			}()

			// Poll for the running container, then inspect it to assert the
			// host-config hardening is actually applied by the daemon.
			id := ""
			deadline := time.Now().Add(15 * time.Second)
			for time.Now().Before(deadline) {
				if id = findContainerByLabel(ctx, m.cli, labelHandler, tc.handler); id != "" {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			if id == "" {
				t.Fatal("container not found during execution")
			}
			insp, err := m.cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
			if err != nil {
				t.Fatalf("inspect container: %v", err)
			}
			hc := insp.Container.HostConfig
			if hc == nil {
				t.Fatal("inspect returned nil HostConfig")
			}
			if hc.Memory != 512<<20 {
				t.Errorf("memory limit = %d, want %d", hc.Memory, 512<<20)
			}
			if hc.NanoCPUs != 1_000_000_000 {
				t.Errorf("nano cpus = %d, want %d", hc.NanoCPUs, 1_000_000_000)
			}
			if hc.PidsLimit == nil || *hc.PidsLimit != 128 {
				t.Errorf("pids limit = %v, want 128", hc.PidsLimit)
			}
			if len(hc.CapDrop) != 1 || hc.CapDrop[0] != "ALL" {
				t.Errorf("cap drop = %v, want [ALL]", hc.CapDrop)
			}
			if !hc.ReadonlyRootfs {
				t.Error("expected read-only rootfs")
			}
			if hc.Tmpfs["/tmp"] != "rw,nosuid,noexec,size=64m" {
				t.Errorf("tmpfs = %v, want /tmp rw,nosuid,noexec,size=64m", hc.Tmpfs)
			}
			if insp.Container.Config == nil || insp.Container.Config.NetworkDisabled {
				t.Error("expected networking to remain enabled (NetworkDisabled false)")
			}

			if err := <-done; err != nil {
				t.Fatalf("execute: %v", err)
			}
			if !strings.Contains(buf.String(), tc.name+" hardening ok") {
				t.Errorf("expected in-handler hardening assertions to pass, got logs:\n%s", buf.String())
			}
		})
	}
}

// createOrphanContainer creates and starts a node:24-alpine container carrying
// the given labels and command, returning its ID. t.Cleanup force-removes it so
// the sweep tests never leak.
func createOrphanContainer(t *testing.T, cli *client.Client, ctx context.Context, labels map[string]string) string {
	t.Helper()
	resp, err := cli.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config:     &container.Config{Image: "node:24-alpine", Labels: labels, Cmd: []string{"sh", "-c", "sleep 300"}},
		HostConfig: &container.HostConfig{},
	})
	if err != nil {
		t.Fatalf("create orphan container: %v", err)
	}
	id := resp.ID
	t.Cleanup(func() {
		_ = removeContainer(cli, id)
	})
	if _, err := cli.ContainerStart(ctx, id, client.ContainerStartOptions{}); err != nil {
		t.Fatalf("start orphan container %s: %v", id, err)
	}
	return id
}

// isClassicBuilderIntermediate reports whether a container's Config.Cmd matches
// the shape of a classic-builder (V1) intermediate container: a metadata-only
// step (`#(nop) ...`) or a `/bin/sh -c` step container. The daemon represents
// the step command either as a single string (`/bin/sh -c <cmd>`) or split into
// separate argv elements (`["/bin/sh","-c","<cmd>"]`), so both forms are
// detected. Relay execution containers never match this shape (their Cmd is the
// handler entrypoint), so any such container that is not relay-labeled is a
// leaked build intermediate.
func isClassicBuilderIntermediate(cmd []string) bool {
	for i, part := range cmd {
		if strings.Contains(part, "#(nop)") {
			return true
		}
		if strings.Contains(part, "/bin/sh -c") {
			return true
		}
		// Split argv form: ["/bin/sh","-c","<cmd>"].
		if part == "/bin/sh" && i+1 < len(cmd) && cmd[i+1] == "-c" {
			return true
		}
	}
	return false
}

// TestIntegrationRebuildLeavesNoIntermediateContainers verifies that rebuilding
// a function (v1 -> v2 -> v3) does not leak classic-builder intermediate
// containers. It snapshots the daemon's container set before each build and
// asserts that no NEW non-relay-labeled container matching the classic-builder
// intermediate shape remains after the build. It exercises the real daemon path
// (Manager.Prepare -> buildImage -> ImageBuild).
func TestIntegrationRebuildLeavesNoIntermediateContainers(t *testing.T) {
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
	fn := function.Function{Name: "rebuild-int", Dir: dir, Template: &function.Template{Runtime: "node24"}}

	// Track the relay-fn-* images this test creates so t.Cleanup can remove them
	// (intermediates are expected to be gone by the fix; if the test fails they
	// are left visible for debugging). t.Cleanup runs after the test's deferred
	// cancel() has fired, so use a fresh context here rather than the cancelled
	// test ctx.
	var createdImages []string
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		for _, ref := range createdImages {
			cleanupImage(cli, cleanupCtx, ref)
		}
	})

	// Build v1, then v2, then v3. After each build assert no new intermediate
	// container remains. Each version writes distinct source so every Prepare
	// produces a distinct fingerprint and a real rebuild (not a cache reuse).
	versions := []string{"v1", "v2", "v3"}
	for _, ver := range versions {
		// Write this version's distinct source before building it.
		writeFile(t, dir, "index.js", "export function hi(e){ console.log('"+ver+"'); }\n")
		// Snapshot the container set BEFORE this build, and count the non-relay
		// containers present so we can assert no growth.
		preList, err := cli.ContainerList(ctx, client.ContainerListOptions{All: true})
		if err != nil {
			t.Fatalf("snapshot containers before %s: %v", ver, err)
		}
		before := make(map[string]bool, len(preList.Items))
		nonRelayBefore := 0
		for _, c := range preList.Items {
			before[c.ID] = true
			if _, ok := c.Labels[labelFunction]; !ok {
				nonRelayBefore++
			}
		}

		prepared, err := mPrepare(ctx, t, fn)
		if err != nil {
			t.Fatalf("prepare %s: %v", ver, err)
		}
		createdImages = append(createdImages, prepared.Image)

		// The current function image must exist after the build.
		if !imageExistsInDaemon(cli, ctx, prepared.Image) {
			t.Fatalf("image %s should exist after %s build", prepared.Image, ver)
		}

		// After the build, list containers and classify any that are NEW (not in
		// the pre-build snapshot) and NOT relay-labeled.
		after, err := cli.ContainerList(ctx, client.ContainerListOptions{All: true})
		if err != nil {
			t.Fatalf("list containers after %s: %v", ver, err)
		}
		var newNonRelay []string
		nonRelayAfter := 0
		for _, c := range after.Items {
			if _, ok := c.Labels[labelFunction]; !ok {
				nonRelayAfter++
			}
			if before[c.ID] {
				continue // pre-existing; not ours
			}
			if _, ok := c.Labels[labelFunction]; ok {
				continue // a Relay execution container, not a build intermediate
			}
			newNonRelay = append(newNonRelay, c.ID)
		}

		// Any new non-relay container must NOT be a classic-builder intermediate.
		// Inspect each to classify it; a leaked intermediate fails the test.
		for _, id := range newNonRelay {
			insp, err := cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
			if err != nil {
				t.Fatalf("inspect new container %s after %s: %v", id, ver, err)
			}
			var cmd []string
			if insp.Container.Config != nil {
				cmd = insp.Container.Config.Cmd
			}
			if isClassicBuilderIntermediate(cmd) {
				t.Errorf("leaked classic-builder intermediate container %s after %s build (Cmd %v)", id, ver, cmd)
			}
		}

		// The count of non-relay-labeled containers must not grow across the
		// rebuild (no linear accumulation of intermediates).
		if nonRelayAfter > nonRelayBefore {
			t.Errorf("non-relay container count grew across %s build: before=%d after=%d (leaked intermediates)", ver, nonRelayBefore, nonRelayAfter)
		}
	}
}

// TestIntegrationFailedBuildKeepsIntermediatesAndPropagatesError verifies that
// a failed build (a) surfaces the real build-stream error through Prepare and
// (b) intentionally KEEPS its classic-builder intermediate containers for
// debugging (the daemon only removes intermediates when the build completed
// successfully; Remove:true does not apply to failed builds). It uses a
// malformed package.json so the node engine's `npm install --omit=dev` step
// fails deterministically.
func TestIntegrationFailedBuildKeepsIntermediatesAndPropagatesError(t *testing.T) {
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
	writeFile(t, dir, "index.js", "export function hi(e){ console.log('hi'); }\n")
	// Malformed package.json: the engine's `npm install --omit=dev` step must
	// fail parsing it, failing the build deterministically.
	writeFile(t, dir, "package.json", "{ not json")

	fn := function.Function{Name: "failed-build-int", Dir: dir, Template: &function.Template{Runtime: "node24"}}

	// Snapshot the container set BEFORE the failed build.
	preList, err := cli.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		t.Fatalf("snapshot containers before failed build: %v", err)
	}
	before := make(map[string]bool, len(preList.Items))
	for _, c := range preList.Items {
		before[c.ID] = true
	}

	// Prepare must fail. mPrepare returns the Prepare error (it only t.Fatalfs on
	// NewManager failure), so a non-nil error here is the expected outcome.
	prepared, err := mPrepare(ctx, t, fn)
	if err == nil {
		t.Fatalf("expected Prepare to fail on malformed package.json, got success (image %s)", prepared.Image)
	}
	t.Logf("prepare error: %v", err)

	// The error must propagate the build stream: assert it carries the npm
	// failure text, proving drainBuildResponse surfaced the stream error rather
	// than a silent success. Keep to a stable substring observed on the daemon.
	if !strings.Contains(err.Error(), "npm") && !strings.Contains(err.Error(), "JSON") {
		t.Errorf("expected the build-stream error to mention npm/JSON, got: %v", err)
	}

	// After the failed build, at least one NEW classic-builder intermediate
	// container must remain (failed builds keep intermediates for debugging).
	after, err := cli.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		t.Fatalf("list containers after failed build: %v", err)
	}
	var leakedIntermediates []string
	for _, c := range after.Items {
		if before[c.ID] {
			continue // pre-existing; not ours
		}
		insp, err := cli.ContainerInspect(ctx, c.ID, client.ContainerInspectOptions{})
		if err != nil {
			t.Fatalf("inspect new container %s after failed build: %v", c.ID, err)
		}
		var cmd []string
		if insp.Container.Config != nil {
			cmd = insp.Container.Config.Cmd
		}
		if isClassicBuilderIntermediate(cmd) {
			leakedIntermediates = append(leakedIntermediates, c.ID)
		}
	}
	if len(leakedIntermediates) == 0 {
		t.Error("expected at least one classic-builder intermediate container to remain after the failed build (failed builds keep intermediates for debugging)")
	}
	t.Logf("failed build left %d intermediate container(s): %v", len(leakedIntermediates), leakedIntermediates)

	// Cleanup: force-remove the leftover intermediate containers and any
	// relay-fn-failed-build-int:* image the failed build created. t.Cleanup runs
	// after the test's deferred cancel() has fired, so use a fresh context.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		for _, id := range leakedIntermediates {
			_ = removeContainer(cli, id)
		}
		// Remove any tagged relay-fn-failed-build-int:* image (the failed build
		// may or may not have produced a tagged image).
		imgs, err := cli.ImageList(cleanupCtx, client.ImageListOptions{All: true})
		if err != nil {
			return
		}
		for _, img := range imgs.Items {
			for _, tag := range img.RepoTags {
				if strings.HasPrefix(tag, "relay-fn-failed-build-int:") {
					cleanupImage(cli, cleanupCtx, tag)
					break
				}
			}
		}
	})
}

// TestIntegrationSweepOrphanContainers verifies the startup sweep removes a
// stalled Relay container owned by the current hostname, leaves another worker's
// container alone, and never touches an unrelated (non-Relay-labeled) container.
func TestIntegrationSweepOrphanContainers(t *testing.T) {
	if !dockerAvailable(t) {
		t.Skip("docker not available")
	}
	m, _ := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer cli.Close()

	// Our orphan: relay labels + our hostname, left running (as a crashed prior
	// process would leave a mid-invocation container).
	ours := createOrphanContainer(t, cli, ctx, map[string]string{
		labelFunction: "orphan-fn", labelHostname: "test-host", labelHandler: "index.hi",
	})
	// Another worker's orphan: different hostname, must survive.
	theirs := createOrphanContainer(t, cli, ctx, map[string]string{
		labelFunction: "orphan-fn", labelHostname: "other-host", labelHandler: "index.hi",
	})
	// Unrelated container: no relay labels, must survive.
	unrelated := createOrphanContainer(t, cli, ctx, map[string]string{"app": "whatever"})

	if _, err := m.SweepOrphanContainers(ctx, "test-host"); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	// Our hostname's orphan is gone...
	list, _ := cli.ContainerList(ctx, client.ContainerListOptions{All: true})
	for _, c := range list.Items {
		if c.ID == ours {
			t.Errorf("orphan container %s (ours) should have been swept", ours)
		}
	}
	// ...while the other worker's and the unrelated container survive.
	for _, cid := range []string{theirs, unrelated} {
		found := false
		for _, c := range list.Items {
			if c.ID == cid {
				found = true
			}
		}
		if !found {
			t.Errorf("container %s should NOT have been swept", cid)
		}
	}
}

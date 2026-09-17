//go:build integration

// This file exercises the Docker boundary end to end against a real Docker
// daemon: image build, rebuild, image lifecycle, container execution, orphan
// cleanup, timeout/cancel, exit codes, and stdout/stderr capture.
//
// This file is excluded from the default suite by the integration build tag.
// Running it (`go test -tags=integration ./...`) REQUIRES a reachable Docker
// daemon; a missing dependency fails the affected tests rather than skipping
// them. Start the documented dev dependencies with
// `docker compose -f compose.dev.yaml up -d`. The daemon is located via
// client.FromEnv, so DOCKER_HOST, the local socket, and a socket proxy are all
// respected.
package runtime

import (
	"bytes"
	"context"
	"io"
	"log/slog"

	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"

	"relay/internal/function"
	"relay/internal/runtime/plan"
)

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

// requireDocker fails the test immediately when the Docker Engine API daemon
// cannot be reached via client.FromEnv (DOCKER_HOST, socket, socket proxy are
// all respected). Integration tests fundamentally require Docker; missing
// infrastructure fails rather than skips.
func requireDocker(t *testing.T) *client.Client {
	t.Helper()
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		t.Fatalf("docker integration test requires a Docker daemon (client: %v)", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cli.Ping(ctx, client.PingOptions{}); err != nil {
		t.Fatalf("docker integration test requires a reachable Docker daemon (ping: %v); start one or run `docker compose -f compose.dev.yaml up -d`", err)
	}
	t.Cleanup(func() { cli.Close() })
	return cli
}

// newManager returns a Manager wired to a logger that writes into the returned
// buffer, capturing Relay operational logs for assertions. The manager owns
// hostname "test-host" so container-ownership tests are deterministic.
//
// Handler stdout/stderr is NO LONGER routed through the logger (it is forwarded
// as a raw transport to the function-output sink; see output.go and
// newFunctionOutputSink), so handler-output assertions must read from that sink,
// not from this operational log buffer. The logger level is nevertheless kept at
// DEBUG here so the operational-line assertions these tests make are unaffected.
func newManager(t *testing.T) (*Manager, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	l := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	m, err := NewManager(l, nil, "test-host")
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	return m, &buf
}

// newFunctionOutputSink installs a bytes.Buffer as the function-output sink and
// returns it, registering restoration of the previous sink. Container
// stdout/stderr is transport-forwarded here (not to the logger), so every
// handler-output assertion reads from this buffer.
func newFunctionOutputSink(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := SetFunctionOutput(buf)
	t.Cleanup(func() { SetFunctionOutput(prev) })
	return buf
}

func TestPythonEndToEnd(t *testing.T) {
	cli := requireDocker(t)
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

func TestPythonAsyncEndToEnd(t *testing.T) {
	cli := requireDocker(t)
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

func TestNodeEndToEnd(t *testing.T) {
	requireDocker(t)

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

// repoRoot is the repository root (the dir of internal/runtime/..), used to
// resolve the real example function directories under examples/functions/.
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

// TestRealUserEventsPythonEndToEnd drives the real
// examples/functions/user-events-python example: three rules whose handlers
// live in the events/ namespace package
// (events.created / events.updated / events.deleted).
func TestRealUserEventsPythonEndToEnd(t *testing.T) {
	requireDocker(t)

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
func TestRealWelcomeEmailNodeEndToEnd(t *testing.T) {
	requireDocker(t)

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
func TestNodeBrokenDependencyEndToEnd(t *testing.T) {
	requireDocker(t)

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

// imageExistsInDaemon reports whether a local image carries exactly ref.
func imageExistsInDaemon(cli *client.Client, ctx context.Context, ref string) bool {
	_, err := cli.ImageInspect(ctx, ref)
	return err == nil
}

// TestIntegrationFingerprintedImageLifecycle exercises the fingerprint-versioned
// image lifecycle against a real Docker daemon: build v1 -> build v2 (changed
// source) -> the two are distinct images and v1 still present -> removing v1
// (the superseded version) via the manager's per-image removal path retires it
// -> an unrelated (non-relay-owned) image is untouched.
func TestIntegrationFingerprintedImageLifecycle(t *testing.T) {
	cli := requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// The test's own fn-ver images should be cleaned up even on failure so the
	// test never leaks images into another package's cleanup on the shared
	// daemon. Best-effort force-removal, like cleanupImage.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		imgs, err := cli.ImageList(cleanupCtx, client.ImageListOptions{All: true})
		if err != nil {
			return
		}
		for _, img := range imgs.Items {
			for _, tag := range img.RepoTags {
				if strings.HasPrefix(tag, "relay-fn-fn-ver:") {
					cleanupImage(cli, cleanupCtx, tag)
					break
				}
			}
		}
	})

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

	// An unrelated, non-relay image we create: it must never be touched by any
	// of the test's removal operations. Build a tiny tagged image ourselves (not
	// relay-namespaced) via the Manager's ImageBuild against an inline one-line
	// Dockerfile.
	unrelated := buildTestImage(ctx, t, "relay-unrelated-guard", `FROM scratch
CMD []
`)
	defer cleanupImage(cli, ctx, unrelated)

	// Retire v1 (the superseded version) through the manager's per-image
	// removal path: it is non-forced and treats not-found as benign, matching
	// production removal semantics without issuing a whole-daemon sweep.
	//
	// Note: we deliberately do NOT call (*Manager).RemoveImagesExcept here. That
	// sweep is a worker-startup operation over ALL Relay-owned images on the
	// daemon; a unit-scoped lifecycle test must not assert on whole-daemon state
	// it does not own, since it races anything else creating relay-fn-* images on
	// a shared daemon (e.g. another Go test package running concurrently).
	m1, err := NewManager(slog.New(slog.NewTextHandler(io.Discard, nil)), nil, "test-host")
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer m1.Close()
	if err := m1.RemoveImage(ctx, ref1); err != nil {
		t.Fatalf("remove image %s: %v", ref1, err)
	}
	if imageExistsInDaemon(cli, ctx, ref1) {
		t.Errorf("v1 image %s should have been removed", ref1)
	}
	if !imageExistsInDaemon(cli, ctx, ref2) {
		t.Errorf("kept v2 image %s must survive", ref2)
	}
	if !imageExistsInDaemon(cli, ctx, unrelated) {
		t.Errorf("unrelated image %s must not be removed", unrelated)
	}
}

// mPrepare builds a function via a fresh Manager wired to a discard logger.
func mPrepare(ctx context.Context, t *testing.T, fn function.Function) (*Prepared, error) {
	t.Helper()
	m, err := NewManager(slog.New(slog.NewTextHandler(io.Discard, nil)), nil, "test-host")
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer m.Close()
	return m.Prepare(ctx, fn)
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
	requireDocker(t)

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
	m, _ := newManager(t)
	out := newFunctionOutputSink(t)

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
	logs := out.String()
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
	requireDocker(t)

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
	m, _ := newManager(t)
	out := newFunctionOutputSink(t)

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
	if !strings.Contains(out.String(), "TOKEN=v1") {
		t.Errorf("expected TOKEN=v1, got: %s", out.String())
	}

	// Rotate the value; the second execution sees v2 with NO rebuild (the
	// prepared image and fingerprint are unchanged).
	if err := m.Execute(ctx, prepared, "index.secret", []byte(`{"event_name":"INSERT"}`), []string{"TOKEN=v2"}); err != nil {
		t.Fatalf("execute v2: %v", err)
	}
	if !strings.Contains(out.String(), "TOKEN=v2") {
		t.Errorf("expected TOKEN=v2 after rotation, got: %s", out.String())
	}
	if prepared.Fingerprint != fp1 {
		t.Errorf("fingerprint changed across secret rotation: %s -> %s", fp1, prepared.Fingerprint)
	}
}

// pemValue is a canonical PEM-shaped private-key fixture (header/footer,
// multiple internal newlines, a blank line, and an indented value with double
// spaces). It is deliberately inert — not a real RSA key — so no real key
// material is required and printing it in a test failure is harmless. It has no
// trailing newline. Every multiline test across the repo uses exactly this
// fixture so a single corruption at any hop is caught.
const pemValue = "-----BEGIN PRIVATE KEY-----\nMIIB\nline2\n\nindented:  value\n-----END PRIVATE KEY-----"

// TestIntegrationMultilineSecretInjection verifies a PEM-shaped secret value is
// injected into an execution container's environment byte-for-byte and that the
// handler process observes the EXACT original value (including every internal
// newline, the blank line, and the indented line). It uses the same
// Manager.Execute extraEnv injection path the runner uses, so it proves the
// full chain: runner resolution -> runContainer -> container.Config.Env ->
// handler process -> handler stdout. The handler logs the value between clear
// delimiters so exact matching is robust against surrounding output. The
// manager's operational log buffer must never contain any fragment of the
// value (handler stdout goes to the function-output sink, not the op log).
func TestIntegrationMultilineSecretInjection(t *testing.T) {
	requireDocker(t)

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
secrets:
  PRIVATE_KEY: rsa-private-key
events:
  - handler: index.secret
    pattern:
      event_name: [INSERT]
`)
	// The handler logs the value between clear delimiters so the container-side
	// line is matched exactly.
	writeFile(t, dir, "index.js", `
export function secret(event) {
  const v = process.env.PRIVATE_KEY ?? "";
  console.log("PK<begin>" + v + "<end>");
}
`)
	fn := function.Function{Name: "multiline-secret-e2e", Dir: dir, Template: &function.Template{Runtime: "node24"}}
	m, logBuf := newManager(t)
	out := newFunctionOutputSink(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	// Same injection path the runner uses: the extraEnv entry carries the exact
	// fixture (no trailing newline stripped here — raw injection).
	if err := m.Execute(ctx, prepared, "index.secret", []byte(`{"event_name":"INSERT"}`), []string{"PRIVATE_KEY=" + pemValue}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	// The handler writes one logical line, "PK<begin>" + value + "<end>", which
	// the streamForwarder splits on internal '\n' and prefixes per line with
	// "[<fn>/<handler>] stdout: ". Reconstruct that representation so the sink
	// match is both robust against surrounding output and byte-for-byte exact
	// (every segment of the fixture arrives, in order, including the blank line).
	expected := "PK<begin>" + pemValue + "<end>"
	var want strings.Builder
	lineNo := 0
	for _, seg := range strings.Split(expected, "\n") {
		if lineNo > 0 {
			want.WriteByte('\n')
		}
		want.WriteString("[multiline-secret-e2e/index.secret] stdout: ")
		want.WriteString(seg)
		lineNo++
	}
	if !strings.Contains(out.String(), want.String()) {
		t.Errorf("handler output missing exact multiline value; expected:\n%s\n\ngot:\n%s", want.String(), out.String())
	}
	// Secret values must never appear in Relay operational logs; handler stdout
	// is transport-forwarded to the function-output sink, so it must be absent
	// from the manager's op log buffer.
	ops := logBuf.String()
	if strings.Contains(ops, "BEGIN PRIVATE KEY") || strings.Contains(ops, "MIIB") {
		t.Errorf("operational log leaked a secret fragment:\n%s", ops)
	}
}

// TestIntegrationMultipleFunctionsSameSecret verifies two functions referencing
// the same secret both resolve it (the provider is shared, resolution is
// per-invocation).
func TestIntegrationMultipleFunctionsSameSecret(t *testing.T) {
	requireDocker(t)

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
			m, _ := newManager(t)
			out := newFunctionOutputSink(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			prepared, err := m.Prepare(ctx, fn)
			if err != nil {
				t.Fatalf("prepare: %v", err)
			}
			if err := m.Execute(ctx, prepared, "index.secret", []byte(`{"event_name":"INSERT"}`), []string{"TOKEN=shared-value"}); err != nil {
				t.Fatalf("execute: %v", err)
			}
			if !strings.Contains(out.String(), "TOKEN=shared-value") {
				t.Errorf("expected TOKEN=shared-value, got: %s", out.String())
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
	requireDocker(t)
	m, buf := newManager(t)
	out := newFunctionOutputSink(t)
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

	if !strings.Contains(out.String(), "ok evt_1") {
		t.Errorf("expected handler output, got: %s", out.String())
	}
	// The normal path must NOT emit an explicit removal log line.
	if strings.Contains(buf.String(), "remove container") {
		t.Errorf("normal completion path must not log an explicit container removal, got: %s", buf.String())
	}
	// AutoRemove: the container must vanish after exit.
	if !waitForContainerGone(ctx, m.cli, labelHandler, "index.ok") {
		t.Error("container should have been auto-removed after successful exit")
	}
}

// panicWriter is an io.Writer whose Write always panics. It backs the
// function-output sink in TestIntegrationPanickingSinkDoesNotBreakInvocation so
// a forwarding emission of container output blows up. With transport forwarding
// the panic is swallowed, so this must never break the invocation.
type panicWriter struct{}

func (panicWriter) Write(p []byte) (int, error) { panic("function output sink exploded") }

// TestIntegrationPanickingSinkDoesNotBreakInvocation drives runContainer with a
// function-output sink whose Writer panics while forwarding handler output. Since
// forwarding is a best-effort transport, the panic must be swallowed: runContainer
// must return nil (invocation succeeds), the panic must not leak out of the
// process, and the container must still be auto-removed. This preserves the old
// test's cleanup/no-removal-noise intent while asserting the new transport-based
// behavior (a broken sink can never fail an invocation).
func TestIntegrationPanickingSinkDoesNotBreakInvocation(t *testing.T) {
	requireDocker(t)
	m, _ := newManager(t)
	prev := SetFunctionOutput(panicWriter{})
	defer SetFunctionOutput(prev)
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

	// Drive runContainer directly so output forwarding hits the panicking sink
	// on the first forwarded line. The panic must be swallowed by the transport;
	// runContainer must return nil and MUST NOT propagate the panic.
	panicked := make(chan any, 1)
	done := make(chan error, 1)
	go func() {
		defer func() { panicked <- recover() }()
		done <- runContainer(ctx, m.cli,
			slog.New(slog.NewTextHandler(io.Discard, nil)),
			"paniclog-e2e", prepared.Image, nil, nil, "index.paniclog",
			[]byte(`{"event_name":"INSERT"}`),
			RunMeta{Hostname: "test-host", Function: "paniclog-e2e", Handler: "index.paniclog", Image: prepared.Image},
		)
	}()
	select {
	case pv := <-panicked:
		if pv != nil {
			t.Fatalf("sink panic leaked out of runContainer: %v", pv)
		}
	case <-time.After(2 * time.Minute):
		t.Fatal("runContainer did not return")
	}
	if err := <-done; err != nil {
		t.Fatalf("runContainer returned error despite swallowed sink panic: %v", err)
	}

	// The container exited on its own; AutoRemove (not the backstop) removed it.
	if !waitForContainerGone(ctx, m.cli, labelHandler, "index.paniclog") {
		t.Error("container should have been auto-removed after exit despite the sink panic")
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
	cli := requireDocker(t)
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
	cli := requireDocker(t)
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
	requireDocker(t)
	m, _ := newManager(t)
	out := newFunctionOutputSink(t)
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
			Type:      ContainerTypeEvent,
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
		labelType:      ContainerTypeEvent,
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
	if !strings.Contains(out.String(), "completed evt_777") {
		t.Errorf("expected stdout to contain %q, got: %s", "completed evt_777", out.String())
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
	requireDocker(t)
	m, _ := newManager(t)
	out := newFunctionOutputSink(t)
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
	// stderr (console.error) is forwarded to the function-output sink and must
	// carry the function/handler prefix.
	if !strings.Contains(out.String(), "[fail-e2e/index.fail] stderr: boom") {
		t.Errorf("expected stderr 'boom' forwarded with prefix, got: %q", out.String())
	}
	if !waitForContainerGone(ctx, m.cli, labelHandler, "index.fail") {
		t.Error("container should have been auto-removed after non-zero exit")
	}
}

// TestIntegrationFunctionOutputIgnoresLogLevel drives a Node handler that prints
// to both stdout and stderr (multi-line) while Relay's own logger is wired to
// DISCARD at ERROR level. Since function output is forwarded as a raw transport
// — NOT routed through slog — the stdout lines, the stderr line, and the
// function/handler prefix must all still appear in the function-output sink even
// though every Relay log line is discarded at ERROR.
func TestIntegrationFunctionOutputIgnoresLogLevel(t *testing.T) {
	requireDocker(t)

	// Relay-operational logger discards everything below ERROR, so no handler
	// output can possibly flow through it (forwarding must be independent).
	opLogger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	m, err := NewManager(opLogger, nil, "test-host")
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer m.Close()
	out := newFunctionOutputSink(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: index.emit
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", `
export function emit(event) {
  console.log("out-first");
  console.log("out-second");
  console.error("err-line");
}
`)
	fn := function.Function{Name: "loglevel-e2e", Dir: dir, Template: &function.Template{Runtime: "node24"}}
	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	if err := m.Execute(ctx, prepared, "index.emit", []byte(`{"event_name":"INSERT"}`), nil); err != nil {
		t.Fatalf("execute: %v", err)
	}

	logs := out.String()
	for _, want := range []string{
		"[loglevel-e2e/index.emit] stdout: out-first",
		"[loglevel-e2e/index.emit] stdout: out-second",
		"[loglevel-e2e/index.emit] stderr: err-line",
	} {
		if !strings.Contains(logs, want) {
			t.Errorf("expected %q in function output despite ERROR-level Relay logs, got:\n%s", want, logs)
		}
	}
}

// TestIntegrationTimeoutAutoRemove verifies the existing per-rule timeout path
// still works with AutoRemove: an over-long handler is killed on cancellation,
// the error surfaces, and the killed container is removed.
func TestIntegrationTimeoutAutoRemove(t *testing.T) {
	requireDocker(t)
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
	requireDocker(t)

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
			m, _ := newManager(t)
			out := newFunctionOutputSink(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			// The python subtest's Prepare builds a relay-dep-* layer from its
			// requirements.txt; the node subtest has no package.json so it builds
			// no dep layer (cleanup is a harmless no-op for it). Clean only dep
			// images this subtest adds (delta vs snapshot, leaving other
			// tests'/workers' layers untouched) and register it BEFORE Prepare so
			// it also runs on failure and never leaks into the sibling dep-layer
			// tests on the shared daemon.
			depBefore := depTagSet(ctx, m.cli)
			t.Cleanup(cleanupNewDepImagesSince(m.cli, depBefore))

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
			if hc.Memory != 128<<20 {
				t.Errorf("memory limit = %d, want %d", hc.Memory, 128<<20)
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
			if !strings.Contains(out.String(), tc.name+" hardening ok") {
				t.Errorf("expected in-handler hardening assertions to pass, got logs:\n%s", out.String())
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

// isClassicBuilderIntermediateCmd is isClassicBuilderIntermediate for the
// flat command string a ContainerList Summary carries (Summary has no Config;
// the daemon joins the argv with spaces). Same detection, one argument.
func isClassicBuilderIntermediateCmd(command string) bool {
	return strings.Contains(command, "#(nop)") || strings.Contains(command, "/bin/sh -c")
}

// TestIntegrationRebuildLeavesNoIntermediateContainers verifies that rebuilding
// a function (v1 -> v2 -> v3) does not leak classic-builder intermediate
// containers. It snapshots the daemon's container set before each build and
// asserts that no NEW non-relay-labeled container matching the classic-builder
// intermediate shape remains after the build. It exercises the real daemon path
// (Manager.Prepare -> buildImage -> ImageBuild).
func TestIntegrationRebuildLeavesNoIntermediateContainers(t *testing.T) {
	cli := requireDocker(t)
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
		// Snapshot the container set BEFORE this build, and count the
		// classic-intermediate-shaped non-relay containers so we can assert no
		// growth. The count is scoped to the intermediate SHAPE (not all
		// non-relay containers) because the daemon is global: unrelated
		// transient containers (another package's concurrent tests under
		// `go test ./...`, the daemon's own async AutoRemove of an earlier
		// test's container, buildkit helpers) can appear in the build window
		// and must never fail this assertion — only a genuine intermediate
		// leak should. The per-container shape check below is the primary
		// assertion; this count is the redundant secondary signal.
		preList, err := cli.ContainerList(ctx, client.ContainerListOptions{All: true})
		if err != nil {
			t.Fatalf("snapshot containers before %s: %v", ver, err)
		}
		before := make(map[string]bool, len(preList.Items))
		interBefore := 0
		for _, c := range preList.Items {
			before[c.ID] = true
			if _, ok := c.Labels[labelFunction]; !ok && isClassicBuilderIntermediateCmd(c.Command) {
				interBefore++
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
		interAfter := 0
		for _, c := range after.Items {
			if _, ok := c.Labels[labelFunction]; !ok {
				if isClassicBuilderIntermediateCmd(c.Command) {
					interAfter++
				}
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

		// The count of classic-intermediate-shaped non-relay containers must not
		// grow across the rebuild (no linear accumulation of intermediates).
		if interAfter > interBefore {
			t.Errorf("intermediate-shaped container count grew across %s build: before=%d after=%d (leaked intermediates)", ver, interBefore, interAfter)
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
	cli := requireDocker(t)
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
	m, _ := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cli := requireDocker(t)

	// Our orphan: relay labels + our hostname, left running (as a crashed prior
	// process would leave a mid-invocation container).
	ours := createOrphanContainer(t, cli, ctx, map[string]string{
		labelType: ContainerTypeEvent, labelFunction: "orphan-fn", labelHostname: "test-host", labelHandler: "index.hi",
	})
	// Another worker's orphan: different hostname, must survive.
	theirs := createOrphanContainer(t, cli, ctx, map[string]string{
		labelType: ContainerTypeEvent, labelFunction: "orphan-fn", labelHostname: "other-host", labelHandler: "index.hi",
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

// cleanupImagePrefixes force-removes every local image whose repo tag starts with
// any of the given prefixes. It is t.Cleanup glue so dependency-layer tests never
// leak relay-dep-* / relay-fn-* images onto a shared daemon.
func cleanupImagePrefixes(cli *client.Client, prefixes ...string) func() {
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		imgs, err := cli.ImageList(ctx, client.ImageListOptions{All: true})
		if err != nil {
			return
		}
		for _, img := range imgs.Items {
			for _, tag := range img.RepoTags {
				for _, p := range prefixes {
					if strings.HasPrefix(tag, p) {
						cleanupImage(cli, ctx, tag)
						break
					}
				}
			}
		}
	}
}

// depTags lists every local relay-dep-* image tag currently present on the
// daemon.
func depTags(ctx context.Context, cli *client.Client) []string {
	list, err := cli.ImageList(ctx, client.ImageListOptions{All: true})
	if err != nil {
		return nil
	}
	var out []string
	for _, img := range list.Items {
		for _, tag := range img.RepoTags {
			if strings.HasPrefix(tag, depRepoPrefix) {
				out = append(out, tag)
			}
		}
	}
	return out
}

// depTagSet snapshots the CURRENT set of relay-dep-* tags on the daemon into a
// map so tests can diff their OWN additions against a baseline. The relay-dep-*
// namespace is content-addressed and shared across every test and worker
// process on a daemon (any Python build with a requirements manifest creates a
// relay-dep-* image), so asserting on whole-daemon relay-dep-* counts is
// inherently racy. Tests must snapshot at test start and assert only on the
// tags that appear SINCE that snapshot.
func depTagSet(ctx context.Context, cli *client.Client) map[string]bool {
	before := make(map[string]bool)
	for _, d := range depTags(ctx, cli) {
		before[d] = true
	}
	return before
}

// newDepTagsSince returns the relay-dep-* tags present NOW but NOT present in
// the given baseline snapshot. On a shared daemon this scopes a dep-layer
// assertion to exactly the images this test's builds created, ignoring relay-dep-*
// layers other tests or workers legitimately created.
func newDepTagsSince(ctx context.Context, cli *client.Client, before map[string]bool) []string {
	var out []string
	for _, d := range depTags(ctx, cli) {
		if !before[d] {
			out = append(out, d)
		}
	}
	return out
}

// cleanupNewDepImagesSince force-removes only the relay-dep-* images that
// appeared since the given baseline snapshot, leaving dep layers built
// concurrently by other tests/workers on the shared daemon untouched. It is
// t.Cleanup glue so dep-image-creating tests clean up after themselves and never
// leak relay-dep-* images into later sibling tests.
func cleanupNewDepImagesSince(cli *client.Client, before map[string]bool) func() {
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, d := range newDepTagsSince(ctx, cli, before) {
			cleanupImage(cli, ctx, d)
		}
	}
}

// TestIntegrationDependencyLayerReuse verifies the shared dependency layer is
// reused across source changes: build v1 (with requirements.txt), then change
// ONLY the handler source and build v2. The dependency image must exist
// unchanged BEFORE and AFTER (same tag, no new dep image), while the function
// image gets a NEW tag for the changed source.
//
// NOTE: the relay-dep-* namespace is content-addressed and shared daemon-wide,
// so the assertions count only the dep layers THIS test creates (the delta vs a
// snapshot taken at test start), never the whole-daemon relay-dep-* population —
// other tests or workers on a shared daemon legitimately create their own.
func TestIntegrationDependencyLayerReuse(t *testing.T) {
	cli := requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Register cleanup FIRST (before any Fatalf) so a mid-test failure can never
	// leak the dep-layer and function images this test creates into the sibling
	// tests that follow on the shared daemon. We remove only the dep images this
	// test built (delta vs the snapshot below), not every relay-dep-* layer on
	// the daemon, and force-remove this test's own function image.
	depBefore := depTagSet(ctx, cli)
	t.Cleanup(cleanupNewDepImagesSince(cli, depBefore))
	t.Cleanup(cleanupImagePrefixes(cli, "relay-fn-dep-reuse:"))

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: python3.14
events:
  - handler: handler.run
    pattern:
      status: [COMPLETED]
`)
	writeFile(t, dir, "requirements.txt", "six==1.16.0\n")

	fn := function.Function{Name: "dep-reuse", Dir: dir, Template: &function.Template{Runtime: "python3.14"}}

	// v1 source.
	writeFile(t, dir, "handler.py", "def run(event):\n    print('v1')\n")
	fp1, err := function.Fingerprint(dir)
	if err != nil {
		t.Fatalf("fingerprint v1: %v", err)
	}
	ref1 := ImageRef(fn.Name, fp1)

	p1, err := mPrepare(ctx, t, fn)
	if err != nil {
		t.Fatalf("prepare v1: %v", err)
	}
	if p1.Image != ref1 {
		t.Fatalf("prepare v1 image = %q, want %q", p1.Image, ref1)
	}
	depsAfterV1 := newDepTagsSince(ctx, cli, depBefore)
	if len(depsAfterV1) != 1 {
		t.Fatalf("expected exactly one NEW dependency image after v1 build, got %v", depsAfterV1)
	}
	depRef := depsAfterV1[0]
	depIDBefore, err := cli.ImageInspect(ctx, depRef)
	if err != nil {
		t.Fatalf("inspect dep image: %v", err)
	}

	// v2 source: ONLY the handler changes.
	writeFile(t, dir, "handler.py", "def run(event):\n    print('v2')\n")
	fp2, err := function.Fingerprint(dir)
	if err != nil {
		t.Fatalf("fingerprint v2: %v", err)
	}
	ref2 := ImageRef(fn.Name, fp2)
	if ref1 == ref2 {
		t.Fatalf("v1 and v2 refs must differ, both %s", ref1)
	}

	p2, err := mPrepare(ctx, t, fn)
	if err != nil {
		t.Fatalf("prepare v2: %v", err)
	}
	if p2.Image != ref2 {
		t.Fatalf("prepare v2 image = %q, want %q", p2.Image, ref2)
	}

	// The same dependency tag exists after v2, and it inspects to the SAME image
	// ID (identical content — not rebuilt). Count only THIS test's delta vs the
	// snapshot: still exactly one NEW layer and it is the same tag we captured
	// after v1.
	depsAfterV2 := newDepTagsSince(ctx, cli, depBefore)
	if len(depsAfterV2) != 1 {
		t.Fatalf("expected still exactly one NEW dependency image after v2 built from source change, got %v", depsAfterV2)
	}
	if depsAfterV2[0] != depRef {
		t.Fatalf("dep image tag changed across pure source change: %s -> %s", depRef, depsAfterV2[0])
	}
	depIDAfter, err := cli.ImageInspect(ctx, depRef)
	if err != nil {
		t.Fatalf("inspect dep image after v2: %v", err)
	}
	if depIDAfter.ID != depIDBefore.ID {
		t.Errorf("dependency layer was rebuilt across a pure source change (before %s after %s) — it should be reused", depIDBefore.ID, depIDAfter.ID)
	}
}

// TestIntegrationDependencyChangeProducesNewDepLayer verifies that changing the
// dependency manifest (adding a package to requirements.txt) yields a NEW
// dependency fingerprint and a NEW relay-dep-* image, with both the old and new
// dependency layers coexisting (old layers are never mutated).
//
// NOTE: the relay-dep-* namespace is content-addressed and shared daemon-wide,
// so the assertions count only the dep layers THIS test creates (the delta vs a
// snapshot taken at test start), never the whole-daemon relay-dep-* population.
func TestIntegrationDependencyChangeProducesNewDepLayer(t *testing.T) {
	cli := requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Register cleanup FIRST (before any Fatalf) so a mid-test failure can never
	// leak the dep-layer and function images this test creates into the sibling
	// tests that follow on the shared daemon.
	depBefore := depTagSet(ctx, cli)
	t.Cleanup(cleanupNewDepImagesSince(cli, depBefore))
	t.Cleanup(cleanupImagePrefixes(cli, "relay-fn-dep-change:"))

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: python3.14
events:
  - handler: handler.run
    pattern:
      status: [COMPLETED]
`)
	writeFile(t, dir, "handler.py", "def run(event):\n    print('ok')\n")
	fn := function.Function{Name: "dep-change", Dir: dir, Template: &function.Template{Runtime: "python3.14"}}

	// v1 manifest.
	writeFile(t, dir, "requirements.txt", "six==1.16.0\n")
	if _, err := mPrepare(ctx, t, fn); err != nil {
		t.Fatalf("prepare v1: %v", err)
	}
	deps1 := newDepTagsSince(ctx, cli, depBefore)
	if len(deps1) != 1 {
		t.Fatalf("expected one NEW dep image after v1, got %v", deps1)
	}

	// v2 manifest: add a package. The function source and template are unchanged,
	// so only the dependencies differ.
	writeFile(t, dir, "requirements.txt", "six==1.16.0\nrequests==2.32.3\n")
	if _, err := mPrepare(ctx, t, fn); err != nil {
		t.Fatalf("prepare v2: %v", err)
	}
	deps2 := newDepTagsSince(ctx, cli, depBefore)
	if len(deps2) != 2 {
		t.Fatalf("expected TWO NEW dependency images to coexist after manifest change, got %v", deps2)
	}
	// The original layer (v1's new tag) is still present (never mutated or
	// pruned), found among the tags this test created vs its snapshot.
	stillPresent := false
	for _, d := range deps2 {
		if d == deps1[0] {
			stillPresent = true
		}
	}
	if !stillPresent {
		t.Errorf("original dependency image %s must coexist with the new layer", deps1[0])
	}
}

// TestIntegrationDepFingerprintRuntimeVersionDifferent verifies that the
// dependency fingerprint differs across runtime versions even with identical
// manifest content (via the fingerprint function directly, no second Python
// runtime needed): different spec.Name / BaseImage must not share a layer.
func TestIntegrationDepFingerprintRuntimeVersionDifferent(t *testing.T) {
	requireDocker(t) // this test needs no daemon image ops, but stays tagged integration for consistency

	dir := t.TempDir()
	writeFile(t, dir, "requirements.txt", "six==1.16.0\n")
	deps := plan.Deps{Files: []string{"requirements.txt"}, Install: "pip install --no-cache-dir -r requirements.txt", Dir: "/app"}

	specA := plan.Spec{Name: "python3.14", Engine: plan.EnginePython, BaseImage: "python:3.14-slim"}
	specB := plan.Spec{Name: "python3.15", Engine: plan.EnginePython, BaseImage: "python:3.15-slim"}

	fpA, err := DependencyFingerprint(arch, platform, specA, dir, deps)
	if err != nil {
		t.Fatalf("fingerprint A: %v", err)
	}
	fpB, err := DependencyFingerprint(arch, platform, specB, dir, deps)
	if err != nil {
		t.Fatalf("fingerprint B: %v", err)
	}
	if fpA == fpB {
		t.Error("different runtime versions must not share a dependency layer fingerprint")
	}
	if depImageRef(fpA) == depImageRef(fpB) {
		t.Error("different runtime versions must map to different dependency images")
	}
}

// TestIntegrationConcurrentDepBuilds verifies two concurrent Prepare calls for
// the same function version (two Manager instances, as two worker replicas
// would) both succeed and leave exactly ONE NEW dependency image tag — the
// shared, content-addressed layer is built once even under a build race. Only
// the delta vs a snapshot taken at test start is counted, so unrelated
// relay-dep-* layers from other tests/workers on the shared daemon are ignored.
func TestIntegrationConcurrentDepBuilds(t *testing.T) {
	cli := requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Snapshot the pre-existing dep images so the assertions AND the cleanup
	// count only what THIS test's concurrent builds add. Register cleanup FIRST
	// (before any Fatalf) so a mid-test failure never leaks the dep-layer nor
	// function images into the sibling tests that follow on the shared daemon.
	depBefore := depTagSet(ctx, cli)
	t.Cleanup(cleanupNewDepImagesSince(cli, depBefore))
	t.Cleanup(cleanupImagePrefixes(cli, "relay-fn-dep-race:"))

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: python3.14
events:
  - handler: handler.run
    pattern:
      status: [COMPLETED]
`)
	writeFile(t, dir, "handler.py", "def run(event):\n    print('ok')\n")
	writeFile(t, dir, "requirements.txt", "six==1.16.0\n")
	fn := function.Function{Name: "dep-race", Dir: dir, Template: &function.Template{Runtime: "python3.14"}}

	start := make(chan struct{})
	errs := make(chan error, 2)
	results := make(chan *Prepared, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			m, err := NewManager(slog.New(slog.NewTextHandler(io.Discard, nil)), nil, "test-host")
			if err != nil {
				errs <- err
				return
			}
			defer m.Close()
			p, err := m.Prepare(ctx, fn)
			if err != nil {
				errs <- err
				return
			}
			results <- p
		}()
	}
	close(start)
	for i := 0; i < 2; i++ {
		select {
		case err := <-errs:
			t.Fatalf("concurrent prepare failed: %v", err)
		case p := <-results:
			// Two concurrent Prepare calls build the SAME function image
			// ref. The second build can briefly un-tag/rebuild the ref while
			// the daemon finishes, so poll for the ref to exist rather than
			// asserting at the instant this goroutine finished.
			exists := false
			deadline := time.Now().Add(20 * time.Second)
			for time.Now().Before(deadline) {
				if imageExistsInDaemon(cli, ctx, p.Image) {
					exists = true
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			if !exists {
				t.Fatalf("concurrently prepared image %s must exist", p.Image)
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for concurrent prepares")
		}
	}

	// Exactly one NEW relay-dep-* image may exist after the race (both goroutines
	// built the same fingerprint; Docker racing same-content builds => one tag).
	// Count only the delta vs the snapshot so unrelated relay-dep-* layers from
	// other tests/workers on the shared daemon are ignored.
	newDeps := newDepTagsSince(ctx, cli, depBefore)
	if len(newDeps) != 1 {
		t.Errorf("expected exactly one new dependency image after concurrent builds, got %v", newDeps)
	}
}

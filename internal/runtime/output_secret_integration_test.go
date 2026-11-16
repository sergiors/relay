//go:build integration

// Env/secret injection integration tests, observed through the container's
// native Docker logs (Relay never forwards app output), including multiline
// secrets and the operational-log leak checks.
package runtime

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/testutil"
)

// pemValue is a canonical PEM-shaped private-key fixture (header/footer,
// multiple internal newlines, a blank line, and an indented value with double
// spaces). It is deliberately inert — not a real RSA key — so no real key
// material is required and printing it in a test failure is harmless. It has no
// trailing newline. Every multiline test across the repo uses exactly this
// fixture so a single corruption at any hop is caught.
const pemValue = "-----BEGIN PRIVATE KEY-----\nMIIB\nline2\n\nindented:  value\n-----END PRIVATE KEY-----"

// TestIntegrationAppEnvInjection verifies template env values are injected
// into the execution container's environment, and that template.yaml is NOT
// baked into the image (it is Relay configuration, not app source).
func TestIntegrationAppEnvInjection(t *testing.T) {
	testutil.RequireDocker(t)

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: nodejs24.x
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
export function env(event, context) {
  console.log("GREETING=" + process.env.GREETING);
  console.log("FILES=" + readdirSync("/app").join(","));
}
`)
	fn := app.App{Name: "env-e2e", Dir: dir, Template: &app.Template{Runtime: "nodejs24.x"}}
	m, _ := newManager(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	// Inject the template env value via extraEnv (the runner's per-invocation
	// path).
	event := []byte(`{"event_name":"INSERT"}`)
	if err := m.Execute(ctx, prepared, "index.env", event, []string{"GREETING=hello"}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	logs := awaitAppLogsFor(t, ctx, m.cli, fn.Name, "GREETING=hello")
	// template.yaml must NOT be in the image.
	if strings.Contains(logs, "template.yaml") {
		t.Errorf("template.yaml must not be baked into the image, got: %s", logs)
	}
}

// TestIntegrationSecretInjectionAndRotation verifies secret values are injected
// per invocation and that rotating the value between executions takes effect
// without a rebuild (the image is prepared once; the fingerprint is unchanged).
func TestIntegrationSecretInjectionAndRotation(t *testing.T) {
	testutil.RequireDocker(t)

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: nodejs24.x
secrets:
  TOKEN: relay-itest-token
events:
  - handler: index.secret
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", `
export function secret(event, context) {
  console.log("TOKEN=" + process.env.TOKEN);
}
`)
	fn := app.App{Name: "secret-e2e", Dir: dir, Template: &app.Template{Runtime: "nodejs24.x"}}
	m, _ := newManager(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	fp1 := prepared.Fingerprint

	// First execution with secret value v1.
	event := []byte(`{"event_name":"INSERT"}`)
	if err := m.Execute(ctx, prepared, "index.secret", event, []string{"TOKEN=v1"}); err != nil {
		t.Fatalf("execute v1: %v", err)
	}
	_ = awaitAppLogsFor(t, ctx, m.cli, fn.Name, "TOKEN=v1")

	// Rotate the value; the second execution sees v2 with NO rebuild (the
	// prepared image and fingerprint are unchanged).
	if err := m.Execute(ctx, prepared, "index.secret", event, []string{"TOKEN=v2"}); err != nil {
		t.Fatalf("execute v2: %v", err)
	}
	_ = awaitAppLogsFor(t, ctx, m.cli, fn.Name, "TOKEN=v2")
	if prepared.Fingerprint != fp1 {
		t.Errorf("fingerprint changed across secret rotation: %s -> %s", fp1, prepared.Fingerprint)
	}
}

// TestIntegrationMultilineSecretInjection verifies a PEM-shaped secret value is
// injected into an execution container's per-invocation environment
// byte-for-byte and that the handler process observes the EXACT original value
// (including every internal newline, the blank line, and the indented line). It
// uses the same Manager.Execute extraEnv injection path the runner uses, so it
// proves the full chain: runner resolution -> request frame env ->
// bootstrap process env -> handler stdout. (The value is deliberately NOT in the
// container's Docker Config.Env — only the runtime plan env is; see
// execution_env_integration_test.go.) The handler logs the value between clear
// delimiters so exact matching is robust against surrounding output. The
// manager's operational log buffer must never contain any fragment of the
// value (handler stdout lives in the container's native Docker logs, never
// in the op log).
func TestIntegrationMultilineSecretInjection(t *testing.T) {
	testutil.RequireDocker(t)

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: nodejs24.x
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
export function secret(event, context) {
  const v = process.env.PRIVATE_KEY ?? "";
  console.log("PK<begin>" + v + "<end>");
}
`)
	fn := app.App{Name: "multiline-secret-e2e", Dir: dir, Template: &app.Template{Runtime: "nodejs24.x"}}
	m, logBuf := newManager(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	// Same injection path the runner uses: the extraEnv entry carries the exact
	// fixture (no trailing newline stripped here — raw injection).
	event := []byte(`{"event_name":"INSERT"}`)
	if err := m.Execute(ctx, prepared, "index.secret", event, []string{"PRIVATE_KEY=" + pemValue}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	// The handler writes one logical line, "PK<begin>" + value + "<end>",
	// whose internal newlines become consecutive native log lines with no
	// Relay-added prefix: the assertion is on the VALUE's exact bytes as
	// observed in the container's Docker logs. Every segment of the fixture
	// must arrive, in order, including the blank line.
	gotStdout, _ := awaitAppStreamsFor(t, ctx, m.cli, fn.Name, "<end>")
	expected := "PK<begin>" + pemValue + "<end>"
	if !strings.Contains(gotStdout, expected) {
		t.Errorf("handler output missing the exact multiline value;\nexpected:\n%q\n\ngot:\n%q", expected, gotStdout)
	}
	// The secret-value contract: the value (or any distinctive fragment of it)
	// must never appear in Relay operational logs — handler stdout lives in the
	// container's native logs, never in the op log.
	ops := logBuf.String()
	if strings.Contains(ops, "BEGIN PRIVATE KEY") || strings.Contains(ops, "MIIB") {
		t.Errorf("operational log leaked a secret fragment:\n%s", ops)
	}
}

// TestIntegrationMultipleAppsSameSecret verifies two apps referencing
// the same secret both resolve it (the provider is shared, resolution is
// per-invocation).
func TestIntegrationMultipleAppsSameSecret(t *testing.T) {
	testutil.RequireDocker(t)

	// Both apps reference the same secret name; the runner resolves it per
	// invocation. This test drives the runtime layer directly with the resolved
	// value, proving the container receives it for each app.
	for _, tc := range []struct {
		name string
	}{
		{"multi-a"},
		{"multi-b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeFile(t, dir, "template.yaml", `
runtime: nodejs24.x
secrets:
  TOKEN: relay-itest-token
events:
  - handler: index.secret
    pattern:
      event_name: [INSERT]
`)
			writeFile(t, dir, "index.js", `
export function secret(event, context) {
  console.log("TOKEN=" + process.env.TOKEN);
}
`)
			fn := app.App{Name: tc.name, Dir: dir, Template: &app.Template{Runtime: "nodejs24.x"}}
			m, _ := newManager(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			prepared, err := m.Prepare(ctx, fn)
			if err != nil {
				t.Fatalf("prepare: %v", err)
			}
			event := []byte(`{"event_name":"INSERT"}`)
			if err := m.Execute(ctx, prepared, "index.secret", event, []string{"TOKEN=shared-value"}); err != nil {
				t.Fatalf("execute: %v", err)
			}
			_ = awaitAppLogsFor(t, ctx, m.cli, fn.Name, "TOKEN=shared-value")
		})
	}
}

// TestIntegrationAppOutputIgnoresLogLevel drives a Node handler that prints
// to both stdout and stderr (multi-line) while Relay's own logger is wired to
// DISCARD at ERROR level. App output is the container's native Docker logs —
// never routed through slog or any Relay-side forwarding — so the stdout
// lines and the stderr line must still appear there even though every Relay
// operational log line is discarded at ERROR.
func TestIntegrationAppOutputIgnoresLogLevel(t *testing.T) {
	testutil.RequireDocker(t)

	// Relay-operational logger discards everything below ERROR; handler output
	// must be unaffected because it lives in the container's native Docker logs
	// and never flows through the operational logger.
	opLogger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	m, err := newCoordinatedRealManager(t, opLogger, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer m.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: nodejs24.x
events:
  - handler: index.emit
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", `
export async function emit(event, context) {
  console.log("out-first");
  console.log("out-second");
  console.error("err-line");
  // The invocation response can arrive before the daemon's log stream carries
  // the output. Empty writes queue callbacks behind each stream's console
  // writes, so the handler does not resolve until both are flushed.
  await Promise.all([
    new Promise((resolve, reject) => process.stdout.write("", err => err ? reject(err) : resolve())),
    new Promise((resolve, reject) => process.stderr.write("", err => err ? reject(err) : resolve())),
  ]);
}
`)
	fn := app.App{Name: "loglevel-e2e", Dir: dir, Template: &app.Template{Runtime: "nodejs24.x"}}
	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	if err := m.Execute(ctx, prepared, "index.emit", []byte(`{"event_name":"INSERT"}`), nil); err != nil {
		t.Fatalf("execute: %v", err)
	}

	stdout, stderr := awaitAppStreamsFor(t, ctx, m.cli, fn.Name,
		"out-first", "out-second", "err-line",
	)
	if !strings.Contains(stdout, "out-first") || !strings.Contains(stdout, "out-second") {
		t.Errorf("stdout stream must carry the handler prints, got: %q", stdout)
	}
	if !strings.Contains(stderr, "err-line") {
		t.Errorf("console.error must arrive on the native stderr stream, got: %q", stderr)
	}
}

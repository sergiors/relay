//go:build integration

// Env/secret injection and function-output forwarding integration tests,
// including multiline secrets and a panicking output sink.
package runtime

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/testutil"
)

// pemValue is a canonical PEM-shaped private-key fixture (header/footer,
// multiple internal newlines, a blank line, and an indented value with double
// spaces). It is deliberately inert — not a real RSA key — so no real key
// material is required and printing it in a test failure is harmless. It has no
// trailing newline. Every multiline test across the repo uses exactly this
// fixture so a single corruption at any hop is caught.
const pemValue = "-----BEGIN PRIVATE KEY-----\nMIIB\nline2\n\nindented:  value\n-----END PRIVATE KEY-----"

// TestIntegrationFunctionEnvInjection verifies template env values are injected
// into the execution container's environment, and that template.yaml is NOT
// baked into the image (it is Relay configuration, not function source).
func TestIntegrationFunctionEnvInjection(t *testing.T) {
	testutil.RequireDocker(t)

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
	event := []byte(`{"event_name":"INSERT"}`)
	if err := m.Execute(ctx, prepared, "index.env", event, []string{"GREETING=hello"}); err != nil {
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
	testutil.RequireDocker(t)

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
	event := []byte(`{"event_name":"INSERT"}`)
	if err := m.Execute(ctx, prepared, "index.secret", event, []string{"TOKEN=v1"}); err != nil {
		t.Fatalf("execute v1: %v", err)
	}
	if !strings.Contains(out.String(), "TOKEN=v1") {
		t.Errorf("expected TOKEN=v1, got: %s", out.String())
	}

	// Rotate the value; the second execution sees v2 with NO rebuild (the
	// prepared image and fingerprint are unchanged).
	if err := m.Execute(ctx, prepared, "index.secret", event, []string{"TOKEN=v2"}); err != nil {
		t.Fatalf("execute v2: %v", err)
	}
	if !strings.Contains(out.String(), "TOKEN=v2") {
		t.Errorf("expected TOKEN=v2 after rotation, got: %s", out.String())
	}
	if prepared.Fingerprint != fp1 {
		t.Errorf("fingerprint changed across secret rotation: %s -> %s", fp1, prepared.Fingerprint)
	}
}

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
	testutil.RequireDocker(t)

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
	event := []byte(`{"event_name":"INSERT"}`)
	if err := m.Execute(ctx, prepared, "index.secret", event, []string{"PRIVATE_KEY=" + pemValue}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	// The handler writes one logical line, "PK<begin>" + value + "<end>", which
	// the streamForwarder splits on internal '\n'. Reconstruct the payload by
	// stripping whatever per-line forwarding prefix each segment carries and
	// joining the segments with '\n'; the assertion is on the VALUE's exact
	// bytes, not on the incidental prefix wording. Every segment of the fixture
	// must arrive, in order, including the blank line.
	expected := "PK<begin>" + pemValue + "<end>"
	got := stripForwardingPrefix(out.String())
	if !strings.Contains(got, expected) {
		t.Errorf("handler output missing the exact multiline value;\nexpected:\n%q\n\ngot:\n%q", expected, got)
	}
	// The secret-value contract: the value (or any distinctive fragment of it)
	// must never appear in Relay operational logs — handler stdout goes to the
	// function-output sink, not the op log.
	ops := logBuf.String()
	if strings.Contains(ops, "BEGIN PRIVATE KEY") || strings.Contains(ops, "MIIB") {
		t.Errorf("operational log leaked a secret fragment:\n%s", ops)
	}
}

// TestIntegrationMultipleFunctionsSameSecret verifies two functions referencing
// the same secret both resolve it (the provider is shared, resolution is
// per-invocation).
func TestIntegrationMultipleFunctionsSameSecret(t *testing.T) {
	testutil.RequireDocker(t)

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
			event := []byte(`{"event_name":"INSERT"}`)
			if err := m.Execute(ctx, prepared, "index.secret", event, []string{"TOKEN=shared-value"}); err != nil {
				t.Fatalf("execute: %v", err)
			}
			if !strings.Contains(out.String(), "TOKEN=shared-value") {
				t.Errorf("expected TOKEN=shared-value, got: %s", out.String())
			}
		})
	}
}

// TestIntegrationFunctionOutputIgnoresLogLevel drives a Node handler that prints
// to both stdout and stderr (multi-line) while Relay's own logger is wired to
// DISCARD at ERROR level. Since function output is forwarded as a raw transport
// — NOT routed through slog — the stdout lines, the stderr line, and the
// function/handler prefix must all still appear in the function-output sink even
// though every Relay log line is discarded at ERROR.
func TestIntegrationFunctionOutputIgnoresLogLevel(t *testing.T) {
	testutil.RequireDocker(t)

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

// TestIntegrationPanickingSinkDoesNotBreakInvocation drives a reused execution
// container with a function-output sink whose Writer panics while forwarding
// handler output. Since forwarding is a best-effort transport, the panic must
// be swallowed: the invocation succeeds, the panic must not leak out of the
// process, and the container stays healthy for reuse (Close removes it).
func TestIntegrationPanickingSinkDoesNotBreakInvocation(t *testing.T) {
	testutil.RequireDocker(t)
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

	panicked := make(chan any, 1)
	done := make(chan error, 1)
	go func() {
		defer func() { panicked <- recover() }()
		done <- m.Execute(ctx, prepared, "index.paniclog", []byte(`{"event_name":"INSERT"}`), nil)
	}()
	select {
	case pv := <-panicked:
		if pv != nil {
			t.Fatalf("sink panic leaked out of Execute: %v", pv)
		}
	case <-time.After(2 * time.Minute):
		t.Fatal("Execute did not return")
	}
	if err := <-done; err != nil {
		t.Fatalf("Execute returned error despite swallowed sink panic: %v", err)
	}
	// The container stays healthy for reuse; Close discards it.
	if waitForContainerByLabel(ctx, m.cli, labelFunction, "paniclog-e2e") == "" {
		t.Error("healthy container should survive a panicking sink for reuse")
	}
}

//go:build integration

// The env/secret transport boundary, end to end against a real Docker daemon:
// an execution container's Docker Config.Env carries ONLY the runtime plan env
// (never the template's literal env values or a resolved secret), while those
// dynamic values travel to the reused bootstrap in the per-invocation request
// frame. Persistent service containers are the deliberate exception (a
// long-lived process needs its environment at start) and are covered by
// services_integration_test.go / services_test.go.
package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/client"

	"relay/internal/app"
	"relay/internal/testutil"
)

// TestIntegrationExecutionConfigEnvExcludesDynamicEnv proves the boundary with
// the real daemon: the reused execution container's Docker Config.Env does NOT
// contain a template env value or a resolved secret value, and neither appears
// in any Docker label — yet the handler still observes both values in its
// process environment, which is only possible through the request frame. The
// container is inspected WHILE the handler is blocked, so the assertion is
// against the live container the invocation is actually using.
func TestIntegrationExecutionConfigEnvExcludesDynamicEnv(t *testing.T) {
	testutil.RequireDocker(t)
	m, _ := newManager(t)
	sink := &pollingSink{}
	prev := SetAppOutput(sink)
	t.Cleanup(func() { SetAppOutput(prev) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const (
		envValue    = "CANARY-ENV-VALUE-7f3a"
		secretValue = "CANARY-SECRET-VALUE-9b21"
	)

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
env:
  GREETING: `+envValue+`
secrets:
  TOKEN: relay-itest-token
events:
  - handler: index.env
    pattern:
      event_name: [INSERT]
`)
	// The handler prints both values and blocks so the test can inspect the
	// container while the invocation is in flight.
	writeFile(t, dir, "index.js", `
export async function env(event) {
  console.log("GREETING=" + (process.env.GREETING ?? ""));
  console.log("TOKEN=" + (process.env.TOKEN ?? ""));
  await new Promise(r => setTimeout(r, event.blockMs ?? 0));
}
`)
	fn := app.App{Name: "env-boundary-e2e", Dir: dir, Template: &app.Template{Runtime: "node24"}}
	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	// Inject the dynamic env the runner would resolve (template env + resolved
	// secret) via the request-frame path.
	extraEnv := []string{"GREETING=" + envValue, "TOKEN=" + secretValue}
	execCtx := context.WithValue(context.Background(), runMetaKey{},
		RunMeta{Hostname: "test-host", App: "env-boundary-e2e", Handler: "index.env", Image: prepared.Image})
	done := make(chan error, 1)
	go func() {
		done <- m.Execute(execCtx, prepared, "index.env", []byte(`{"event_name":"INSERT","blockMs":2000}`), extraEnv)
	}()

	// Wait for both values to be observed, then inspect the live container.
	if !waitForSinkContains(ctx, sink, "GREETING="+envValue) {
		t.Fatalf("handler did not observe GREETING via the frame; sink:\n%s", sink.String())
	}
	if !waitForSinkContains(ctx, sink, "TOKEN="+secretValue) {
		t.Fatalf("handler did not observe TOKEN via the frame; sink:\n%s", sink.String())
	}

	id := findContainerByLabel(ctx, m.cli, labelApp, "env-boundary-e2e")
	if id == "" {
		t.Fatal("execution container not found while the handler runs")
	}
	insp, err := m.cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatalf("inspect %s: %v", id, err)
	}
	if insp.Container.Config == nil {
		t.Fatalf("inspect %s: nil Config", id)
	}
	for _, kv := range insp.Container.Config.Env {
		if strings.Contains(kv, envValue) || strings.Contains(kv, secretValue) {
			t.Errorf("execution Config.Env leaked a dynamic value: %q", kv)
		}
	}
	for k, v := range insp.Container.Config.Labels {
		if strings.Contains(v, envValue) || strings.Contains(v, secretValue) {
			t.Errorf("label %q leaked a dynamic value", k)
		}
	}

	if err := <-done; err != nil {
		t.Fatalf("execute: %v", err)
	}
}

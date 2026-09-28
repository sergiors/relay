package runtime

import (
	"context"
	"strings"
	"sync"
	"testing"

	"relay/internal/function"
)

// capturingContainer is a reusableContainer that records the per-invocation
// request-frame env it receives, so a test can prove dynamic values travel in
// the frame rather than in the container's create-time environment.
type capturingContainer struct {
	mu    sync.Mutex
	envs  []map[string]string
	dying bool
}

func (c *capturingContainer) Invoke(_ context.Context, _ string, _ []byte, env map[string]string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := make(map[string]string, len(env))
	for k, v := range env {
		cp[k] = v
	}
	c.envs = append(c.envs, cp)
	return nil
}

func (c *capturingContainer) discard(string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dying = true
	return true
}
func (c *capturingContainer) discardReason() string { return "" }
func (c *capturingContainer) dead() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dying
}

func (c *capturingContainer) lastEnv() map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.envs) == 0 {
		return nil
	}
	return c.envs[len(c.envs)-1]
}

// TestExecuteDynamicEnvNotAttachedToSpans proves the runtime's
// acquire/invoke/execute spans never carry an env value or resolved secret: the
// dynamic env travels only in the request frame, and spans record stable,
// low-cardinality attributes (function/image) only.
func TestExecuteDynamicEnvNotAttachedToSpans(t *testing.T) {
	rec := withSpanRecorder(t)
	m := &Manager{maxConcurrency: 4}
	m.containers = newContainerCache()
	c := &capturingContainer{}
	m.startContainerFn = func(_ context.Context, _ string, _ resolvedImage, _ []string, _ function.ResourceLimits, _ RunMeta) (reusableContainer, error) {
		return c, nil
	}
	prepared := &Prepared{Name: "fn", Image: "relay-fn-fn:tag", Fingerprint: "fp", Concurrency: 1}
	const secret = "CANARY-SECRET-VALUE-in-spans"
	if err := m.Execute(context.Background(), prepared, "h", []byte(`{}`), []string{"SECRET=" + secret}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	spans := rec.spans(t)
	if len(spans) == 0 {
		t.Fatal("expected runtime spans")
	}
	for _, s := range spans {
		for _, kv := range s.Attributes {
			if strings.Contains(kv.Value.AsString(), secret) {
				t.Fatalf("span %q attribute %q leaked the secret value", s.Name, kv.Key)
			}
		}
	}
}

// TestExecuteContainerEnvIsPlanOnlyDynamicEnvIsFrameOnly pins the transport
// boundary: the execution container's create-time environment is EXACTLY the
// runtime plan env (never the template's literal env values and never a resolved
// secret), while those dynamic values are carried per invocation in the request
// frame's env map. It drives the real Manager.Execute path with the injectable
// start seam and a frame-capturing container, so no Docker daemon is needed.
func TestExecuteContainerEnvIsPlanOnlyDynamicEnvIsFrameOnly(t *testing.T) {
	m := &Manager{maxConcurrency: 4}
	m.containers = newContainerCache()

	const secretCanary = "CANARY-SECRET-VALUE-9b21"
	var mu sync.Mutex
	var seenCreateEnv []string
	c := &capturingContainer{}
	m.startContainerFn = func(_ context.Context, _ string, _ resolvedImage, env []string, _ function.ResourceLimits, _ RunMeta) (reusableContainer, error) {
		mu.Lock()
		seenCreateEnv = append([]string(nil), env...)
		mu.Unlock()
		return c, nil
	}

	prepared := &Prepared{
		Name:        "fn",
		Image:       "relay-fn-fn:tag",
		Fingerprint: "fp",
		Env:         []string{"PYTHONDONTWRITEBYTECODE=1"},
		Concurrency: 1,
	}
	extraEnv := []string{"FOO=bar", "SECRET=" + secretCanary}
	if err := m.Execute(context.Background(), prepared, "index.handler", []byte(`{}`), extraEnv); err != nil {
		t.Fatalf("execute: %v", err)
	}

	// Create-time Config.Env is the plan env only.
	mu.Lock()
	gotCreate := seenCreateEnv
	mu.Unlock()
	if len(gotCreate) != 1 || gotCreate[0] != "PYTHONDONTWRITEBYTECODE=1" {
		t.Fatalf("create env = %v, want exactly the plan env", gotCreate)
	}
	for _, kv := range gotCreate {
		if strings.Contains(kv, secretCanary) || strings.Contains(kv, "FOO=") {
			t.Fatalf("dynamic env/secret leaked into the execution container Config.Env: %q", kv)
		}
	}

	// The dynamic values arrived in the per-invocation request frame.
	frameEnv := c.lastEnv()
	if frameEnv["FOO"] != "bar" || frameEnv["SECRET"] != secretCanary {
		t.Fatalf("request-frame env = %v, want FOO=bar and SECRET set", frameEnv)
	}
	if len(frameEnv) != len(extraEnv) {
		t.Fatalf("request-frame env = %v, want exactly the injected dynamic env", frameEnv)
	}
}

// TestExecuteDynamicEnvChangesPerInvocationFrame verifies a rotated/removed
// dynamic value is reflected in the next request frame: the frame is rebuilt per
// invocation from the caller's extraEnv, so no value is cached in the container.
func TestExecuteDynamicEnvChangesPerInvocationFrame(t *testing.T) {
	m := &Manager{maxConcurrency: 4}
	m.containers = newContainerCache()

	c := &capturingContainer{}
	m.startContainerFn = func(_ context.Context, _ string, _ resolvedImage, _ []string, _ function.ResourceLimits, _ RunMeta) (reusableContainer, error) {
		return c, nil
	}
	prepared := &Prepared{Name: "fn", Image: "relay-fn-fn:tag", Fingerprint: "fp", Concurrency: 1}

	if err := m.Execute(context.Background(), prepared, "h", []byte(`{}`), []string{"TOKEN=v1"}); err != nil {
		t.Fatalf("execute v1: %v", err)
	}
	if got := c.lastEnv(); got["TOKEN"] != "v1" {
		t.Fatalf("frame v1 TOKEN = %q, want v1", got["TOKEN"])
	}
	if err := m.Execute(context.Background(), prepared, "h", []byte(`{}`), []string{"TOKEN=v2"}); err != nil {
		t.Fatalf("execute v2: %v", err)
	}
	if got := c.lastEnv(); got["TOKEN"] != "v2" {
		t.Fatalf("frame v2 TOKEN = %q, want v2 (rotated per invocation)", got["TOKEN"])
	}
	// A frame with no dynamic env carries an empty map.
	if err := m.Execute(context.Background(), prepared, "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("execute nil: %v", err)
	}
	if got := c.lastEnv(); len(got) != 0 {
		t.Fatalf("frame env = %v, want empty when the caller injects none", got)
	}
}

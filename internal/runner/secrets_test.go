package runner

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"relay/internal/function"
	"relay/internal/runtime"
	"relay/internal/stream"
)

// envCaptureExecutor records the extraEnv it receives, so tests can assert the
// runner resolved template env values and secrets into the per-invocation env.
type envCaptureExecutor struct {
	mu       sync.Mutex
	extraEnv []string
}

func (e *envCaptureExecutor) Execute(ctx context.Context, _ *runtime.Prepared, _ string, _ []byte, extraEnv []string) error {
	e.mu.Lock()
	e.extraEnv = append([]string(nil), extraEnv...)
	e.mu.Unlock()
	return nil
}

func (e *envCaptureExecutor) got() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.extraEnv...)
}

// fakeProvider resolves a fixed set of names to values.
type fakeProvider struct {
	mu    sync.Mutex
	vals  map[string]string
	err   error
	calls int
}

func (p *fakeProvider) Resolve(ctx context.Context, name string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.err != nil {
		return "", p.err
	}
	v, ok := p.vals[name]
	if !ok {
		return "", errors.New("secret not found")
	}
	return v, nil
}

func (p *fakeProvider) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// fnWithEnv builds a prepared function whose template carries env and secrets.
func fnWithEnv(t *testing.T, name string, executor Executor, env map[string]string, secrets map[string]function.SecretRef) *PreparedFunction {
	t.Helper()
	return NewPrepared(
		function.Function{
			Name: name,
			Template: &function.Template{
				Runtime: "node24",
				Rules:   []function.Rule{{Handler: "index.run", Pattern: function.Pattern{}, Timeout: 0, Retries: function.DefaultRetries}},
				Env:     env,
				Secrets: secrets,
			},
		},
		&runtime.Prepared{Name: name, Image: "x"},
		executor,
	)
}

// TestHandleInjectsEnvAndResolvedSecrets verifies the runner resolves template
// env values and secret references into the per-invocation extra env, in
// name-ordered form, and passes them to the executor.
func TestHandleInjectsEnvAndResolvedSecrets(t *testing.T) {
	exec := &envCaptureExecutor{}
	prov := &fakeProvider{vals: map[string]string{"db-url": "postgres://secret"}}
	r := NewWithMetrics([]*PreparedFunction{
		fnWithEnv(t, "user-events", exec,
			map[string]string{"API_URL": "https://api.example.com"},
			map[string]function.SecretRef{"DATABASE_URL": "db-url"}),
	}, silentLogger(), nil)
	r.SetSecretProvider(prov)

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	got := exec.got()
	want := []string{"API_URL=https://api.example.com", "DATABASE_URL=postgres://secret"}
	if len(got) != len(want) {
		t.Fatalf("extraEnv = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("extraEnv[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestHandleSecretResolutionFailureSchedulesRetry verifies a secret resolution
// error is a failed attempt: it records a retry backoff (RecordFailure) and the
// executor is never reached. The error names the reference but never a value.
func TestHandleSecretResolutionFailureSchedulesRetry(t *testing.T) {
	exec := &countingExecutor{}
	prov := &fakeProvider{err: errors.New("secret \"db-url\" not found")}
	r := NewWithMetrics([]*PreparedFunction{
		fnWithEnv(t, "user-events", exec, nil, map[string]function.SecretRef{"DATABASE_URL": "db-url"}),
	}, silentLogger(), nil)
	r.SetSecretProvider(prov)

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	if err == nil {
		t.Fatal("expected a resolution failure")
	}
	if !strings.Contains(err.Error(), "db-url") {
		t.Fatalf("error should name the secret reference, got: %v", err)
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls = %d, want 0 (resolution failed before execution)", exec.count())
	}
	if len(prog.failures) != 1 {
		t.Fatalf("failures = %v, want one RecordFailure", prog.failures)
	}
}

// TestHandleSecretNoProviderFails verifies a template that references a secret
// with no provider configured fails the invocation with a clear error naming
// the reference.
func TestHandleSecretNoProviderFails(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{
		fnWithEnv(t, "user-events", exec, nil, map[string]function.SecretRef{"TOKEN": "tok"}),
	}, silentLogger(), nil)
	// SetSecretProvider deliberately NOT called.

	err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"})
	if err == nil {
		t.Fatal("expected an error for a secret with no provider")
	}
	if !strings.Contains(err.Error(), "no secret provider") {
		t.Fatalf("error should mention the missing provider, got: %v", err)
	}
	if !strings.Contains(err.Error(), "tok") {
		t.Fatalf("error should name the reference, got: %v", err)
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls = %d, want 0", exec.count())
	}
}

// TestHandleResolvesSecretsPerInvocation verifies secrets are resolved per
// execution (not cached): two Handle calls each resolve through the provider,
// so rotating the underlying value between calls takes effect without a
// rebuild.
func TestHandleResolvesSecretsPerInvocation(t *testing.T) {
	exec := &envCaptureExecutor{}
	prov := &fakeProvider{vals: map[string]string{"tok": "v1"}}
	r := NewWithMetrics([]*PreparedFunction{
		fnWithEnv(t, "user-events", exec, nil, map[string]function.SecretRef{"TOKEN": "tok"}),
	}, silentLogger(), nil)
	r.SetSecretProvider(prov)

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle 1: %v", err)
	}
	if got := exec.got(); len(got) != 1 || got[0] != "TOKEN=v1" {
		t.Fatalf("extraEnv after v1 = %v, want [TOKEN=v1]", got)
	}

	// Rotate the value; the next invocation must see v2 (no rebuild).
	prov.mu.Lock()
	prov.vals["tok"] = "v2"
	prov.mu.Unlock()
	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle 2: %v", err)
	}
	if got := exec.got(); len(got) != 1 || got[0] != "TOKEN=v2" {
		t.Fatalf("extraEnv after v2 = %v, want [TOKEN=v2]", got)
	}
	if prov.count() != 2 {
		t.Fatalf("provider calls = %d, want 2 (resolved per invocation)", prov.count())
	}
}

// TestHandleSecretValueNeverLogged verifies a resolved secret value never
// appears in the runner's log output.
func TestHandleSecretValueNeverLogged(t *testing.T) {
	logger, buf := bufferLogger()
	exec := &envCaptureExecutor{}
	prov := &fakeProvider{vals: map[string]string{"tok": "SUPERSECRETVALUE"}}
	r := NewWithMetrics([]*PreparedFunction{
		fnWithEnv(t, "user-events", exec, nil, map[string]function.SecretRef{"TOKEN": "tok"}),
	}, logger, nil)
	r.SetSecretProvider(prov)

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if strings.Contains(buf.String(), "SUPERSECRETVALUE") {
		t.Fatalf("log leaked the secret value:\n%s", buf.String())
	}
}

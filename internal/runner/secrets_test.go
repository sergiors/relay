package runner

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/observability/metrics"
	"relay/internal/stream"
	"relay/internal/testutil"
)

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

// TestHandleInjectsEnvAndResolvedSecrets verifies the runner resolves template
// env values and secret references into the per-invocation extra env, in
// name-ordered form, and passes them to the executor.
func TestHandleInjectsEnvAndResolvedSecrets(t *testing.T) {
	exec := &captureExecutor{}
	prov := &fakeProvider{vals: map[string]string{"db-url": "postgres://secret"}}
	r := NewWithMetrics([]*PreparedApp{
		fnWithEnv(t, "user-events", exec,
			map[string]string{"API_URL": "https://api.example.com"},
			map[string]app.SecretRef{"DATABASE_URL": "db-url"}),
	}, testutil.DiscardLogger(), nil)
	r.SetSecretProvider(prov)

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	got := exec.gotEnv()
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
// error is a failed attempt: it records a retry backoff (RecordFailure), never
// reaches the executor, and counts the failure telemetry exactly once per
// failed attempt — one failure across the global, labeled, and per-app counters
// with a zero-duration observation, since nothing executed — and once again
// (never twice) for each retried attempt. The error names the reference but
// never a value.
func TestHandleSecretResolutionFailureSchedulesRetry(t *testing.T) {
	exec := &countingExecutor{}
	prov := &fakeProvider{err: errors.New("secret \"db-url\" not found")}
	m := metrics.New()
	r := NewWithMetrics([]*PreparedApp{
		fnWithEnv(t, "user-events", exec, nil, map[string]app.SecretRef{"DATABASE_URL": "db-url"}),
	}, testutil.DiscardLogger(), m)
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
	if got := prog.attempts["user-events/index.run"]; got != 1 {
		t.Fatalf("claimed attempt = %d, want 1 (resolution failed after TryStart)", got)
	}
	// The claimed, never-executed attempt counts exactly one zero-duration
	// failure across the global, labeled, and per-app counters.
	if got := m.Counter(metrics.MetricHandlerFailure); got != 1 {
		t.Fatalf("handler_failure_total = %d, want 1", got)
	}
	if got := m.CounterLabels(metrics.MetricHandlerInvocations, []metrics.Label{
		{Name: "outcome", Value: "failure"},
		{Name: "app", Value: "user-events"},
		{Name: "handler", Value: "index.run"},
	}); got != 1 {
		t.Fatalf("handler_invocations_total{outcome=failure} = %d, want 1", got)
	}
	if got := m.CounterLabels(metrics.MetricHandlerInvocations, []metrics.Label{
		{Name: "outcome", Value: "success"},
		{Name: "app", Value: "user-events"},
		{Name: "handler", Value: "index.run"},
	}); got != 0 {
		t.Fatalf("handler_invocations_total{outcome=success} = %d, want 0", got)
	}
	if got := m.CounterLabels(metrics.MetricFunctionHandlerFailure, []metrics.Label{{Name: "app", Value: "user-events"}}); got != 1 {
		t.Fatalf("function_handler_failure_total{app=user-events} = %d, want 1", got)
	}
	if count, sum := m.HistogramLabels(metrics.MetricHandlerDuration, []metrics.Label{
		{Name: "app", Value: "user-events"},
		{Name: "handler", Value: "index.run"},
	}); count != 1 || sum != 0 {
		t.Fatalf("handler_duration_seconds = (%d observations, sum %v), want exactly one zero-duration observation", count, sum)
	}

	// Past the first backoff the retry claims the next attempt and fails the
	// same way, raising the same telemetry exactly once more: each failed
	// attempt must charge the failure counters once (no duplicate charging)
	// while the executor is still never reached.
	prog.advance(2 * time.Minute)
	err = r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	if err == nil || !strings.Contains(err.Error(), "db-url") {
		t.Fatalf("retry error = %v, want the same resolution failure", err)
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls after retry = %d, want 0", exec.count())
	}
	if got := prog.attempts["user-events/index.run"]; got != 2 {
		t.Fatalf("attempts after retry = %d, want 2", got)
	}
	if len(prog.failures) != 2 || prog.failures[0] != time.Minute || prog.failures[1] != 2*time.Minute {
		t.Fatalf("failures = %v, want the retry backoffs [1m 2m]", prog.failures)
	}
	if got := m.Counter(metrics.MetricHandlerFailure); got != 2 {
		t.Fatalf("handler_failure_total after retry = %d, want 2 (one per failed attempt)", got)
	}
	if got := m.CounterLabels(metrics.MetricHandlerInvocations, []metrics.Label{
		{Name: "outcome", Value: "failure"},
		{Name: "app", Value: "user-events"},
		{Name: "handler", Value: "index.run"},
	}); got != 2 {
		t.Fatalf("handler_invocations_total{outcome=failure} after retry = %d, want 2", got)
	}
	if got := m.CounterLabels(metrics.MetricFunctionHandlerFailure, []metrics.Label{{Name: "app", Value: "user-events"}}); got != 2 {
		t.Fatalf("function_handler_failure_total{app=user-events} after retry = %d, want 2", got)
	}
	if count, sum := m.HistogramLabels(metrics.MetricHandlerDuration, []metrics.Label{
		{Name: "app", Value: "user-events"},
		{Name: "handler", Value: "index.run"},
	}); count != 2 || sum != 0 {
		t.Fatalf("handler_duration_seconds after retry = (%d observations, sum %v), want two zero-duration observations", count, sum)
	}
}

// TestHandleSecretNoProviderFails verifies a template that references a secret
// with no provider configured fails the invocation with a clear error naming
// the reference. Without invocation state there is no confirmed claim, so the
// preparation failure stays telemetry-free.
func TestHandleSecretNoProviderFails(t *testing.T) {
	exec := &countingExecutor{}
	m := metrics.New()
	r := NewWithMetrics([]*PreparedApp{
		fnWithEnv(t, "user-events", exec, nil, map[string]app.SecretRef{"TOKEN": "tok"}),
	}, testutil.DiscardLogger(), m)
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
	assertNoHandlerFailureTelemetry(t, m, "user-events", "index.run")
}

// TestHandleResolvesSecretsPerInvocation verifies secrets are resolved per
// execution (not cached): two Handle calls each resolve through the provider,
// so rotating the underlying value between calls takes effect without a
// rebuild.
func TestHandleResolvesSecretsPerInvocation(t *testing.T) {
	exec := &captureExecutor{}
	prov := &fakeProvider{vals: map[string]string{"tok": "v1"}}
	r := NewWithMetrics([]*PreparedApp{
		fnWithEnv(t, "user-events", exec, nil, map[string]app.SecretRef{"TOKEN": "tok"}),
	}, testutil.DiscardLogger(), nil)
	r.SetSecretProvider(prov)

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle 1: %v", err)
	}
	if got := exec.gotEnv(); len(got) != 1 || got[0] != "TOKEN=v1" {
		t.Fatalf("extraEnv after v1 = %v, want [TOKEN=v1]", got)
	}

	// Rotate the value; the next invocation must see v2 (no rebuild).
	prov.mu.Lock()
	prov.vals["tok"] = "v2"
	prov.mu.Unlock()
	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle 2: %v", err)
	}
	if got := exec.gotEnv(); len(got) != 1 || got[0] != "TOKEN=v2" {
		t.Fatalf("extraEnv after v2 = %v, want [TOKEN=v2]", got)
	}
	if prov.count() != 2 {
		t.Fatalf("provider calls = %d, want 2 (resolved per invocation)", prov.count())
	}
}

// TestResolveExtraEnvReflectsCurrentTemplateSet verifies the runner builds the
// per-invocation frame env from the CURRENT template set only: an env var or
// secret binding omitted from the template yields NO frame entry, so the reused
// bootstrap (which applies the frame exactly) removes a previously applied key.
// This is the runner half of secret/env removal; the bootstrap half is covered by
// the python/node bootstrap tests.
func TestResolveExtraEnvReflectsCurrentTemplateSet(t *testing.T) {
	prov := &fakeProvider{vals: map[string]string{"tok": "SECRET-VALUE"}}
	r := NewWithMetrics(nil, testutil.DiscardLogger(), nil)
	r.SetSecretProvider(prov)

	// A template that declares both an env var and a secret.
	full := &app.Template{
		Env:     map[string]string{"FOO": "bar"},
		Secrets: map[string]app.SecretRef{"TOKEN": "tok"},
	}
	got, err := r.resolveExtraEnv(context.Background(), full)
	if err != nil {
		t.Fatalf("resolve full: %v", err)
	}
	want := map[string]string{"FOO": "bar", "TOKEN": "SECRET-VALUE"}
	assertEnvEntries(t, got, want)

	// The same env var with the secret REMOVED: the frame must carry FOO only.
	withoutSecret := &app.Template{Env: map[string]string{"FOO": "bar"}}
	got, err = r.resolveExtraEnv(context.Background(), withoutSecret)
	if err != nil {
		t.Fatalf("resolve without secret: %v", err)
	}
	assertEnvEntries(t, got, map[string]string{"FOO": "bar"})
	for _, kv := range got {
		if strings.HasPrefix(kv, "TOKEN=") {
			t.Fatalf("frame still carries a removed secret binding: %v", got)
		}
	}

	// Both removed: the frame is empty (the bootstrap then clears prior keys).
	empty := &app.Template{}
	got, err = r.resolveExtraEnv(context.Background(), empty)
	if err != nil {
		t.Fatalf("resolve empty: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("frame = %v, want empty for a template with no env/secrets", got)
	}
}

// assertEnvEntries compares a "K=V" slice to a map, order-independently.
func assertEnvEntries(t *testing.T, got []string, want map[string]string) {
	t.Helper()
	seen := make(map[string]string, len(got))
	for _, kv := range got {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			t.Fatalf("malformed env entry %q", kv)
		}
		seen[k] = v
	}
	if len(seen) != len(want) {
		t.Fatalf("env entries = %v, want %v", got, want)
	}
	for k, v := range want {
		if seen[k] != v {
			t.Fatalf("env[%s] = %q, want %q (full: %v)", k, seen[k], v, got)
		}
	}
}

// TestHandleSecretValueNeverLogged verifies a resolved secret value never
// appears in the runner's log output.
func TestHandleSecretValueNeverLogged(t *testing.T) {
	logger, buf := bufferLogger()
	exec := &captureExecutor{}
	prov := &fakeProvider{vals: map[string]string{"tok": "SUPERSECRETVALUE"}}
	r := NewWithMetrics([]*PreparedApp{
		fnWithEnv(t, "user-events", exec, nil, map[string]app.SecretRef{"TOKEN": "tok"}),
	}, logger, nil)
	r.SetSecretProvider(prov)

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if strings.Contains(buf.String(), "SUPERSECRETVALUE") {
		t.Fatalf("log leaked the secret value:\n%s", buf.String())
	}
}

// pemValue is a canonical PEM-shaped private-key fixture (header/footer,
// multiple internal newlines, a blank line, and an indented value with double
// spaces). It is deliberately inert — not a real RSA key — so printing it in a
// test failure is harmless. It has no trailing newline. Every multiline test in
// this package uses exactly this fixture so a single corruption at any hop is
// caught.
const pemValue = "-----BEGIN PRIVATE KEY-----\nMIIB\nline2\n\nindented:  value\n-----END PRIVATE KEY-----"

// TestHandleInjectsMultilineSecret verifies a multiline (PEM-shaped) secret
// value survives being resolved and injected into the per-invocation extra env
// byte-for-byte: the executor receives exactly `NAME=<fixture>` with no
// trimming, no escaping, and no transformation of internal newlines.
func TestHandleInjectsMultilineSecret(t *testing.T) {
	exec := &captureExecutor{}
	prov := &fakeProvider{vals: map[string]string{"rsa-private-key": pemValue}}
	r := NewWithMetrics([]*PreparedApp{
		fnWithEnv(t, "user-events", exec, nil, map[string]app.SecretRef{"PRIVATE_KEY": "rsa-private-key"}),
	}, testutil.DiscardLogger(), nil)
	r.SetSecretProvider(prov)

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	want := []string{"PRIVATE_KEY=" + pemValue}
	got := exec.gotEnv()
	if len(got) != 1 {
		t.Fatalf("extraEnv = %v, want single entry of length %d", got, len(want[0]))
	}
	if got[0] != want[0] {
		t.Fatalf("extraEnv value length = %d, want %d (byte-for-byte)", len(got[0]), len(want[0]))
	}
}

// TestHandleMultilineSecretValueNeverLogged verifies a multiline (PEM-shaped)
// secret value never appears in the runner's log output — neither the full
// fixture nor distinctive fragments — and that Handle still succeeds (the log
// leak guard must not come at the cost of a false failure).
func TestHandleMultilineSecretValueNeverLogged(t *testing.T) {
	logger, buf := bufferLogger()
	exec := &captureExecutor{}
	prov := &fakeProvider{vals: map[string]string{"rsa-private-key": pemValue}}
	r := NewWithMetrics([]*PreparedApp{
		fnWithEnv(t, "user-events", exec, nil, map[string]app.SecretRef{"PRIVATE_KEY": "rsa-private-key"}),
	}, logger, nil)
	r.SetSecretProvider(prov)

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	logs := buf.String()
	if strings.Contains(logs, pemValue) {
		t.Fatalf("log leaked the full secret value (length %d)", len(pemValue))
	}
	for _, frag := range []string{"BEGIN PRIVATE KEY", "MIIB"} {
		if strings.Contains(logs, frag) {
			t.Fatalf("log leaked secret fragment %q", frag)
		}
	}
}

// TestHandleMultilineSecretResolutionFailureDoesNotLeak pins that a resolution
// failure for a multiline secret error names the secret REFERENCE only — never
// any value fragment — so a failed resolve on a PEM-shaped secret cannot leak
// its content through the error path.
func TestHandleMultilineSecretResolutionFailureDoesNotLeak(t *testing.T) {
	// Use the error-only provider: no value ever exists to leak.
	prov := &fakeProvider{err: errors.New("secret \"rsa-private-key\" not found")}
	r := NewWithMetrics([]*PreparedApp{
		fnWithEnv(t, "user-events", &countingExecutor{}, nil, map[string]app.SecretRef{"PRIVATE_KEY": "rsa-private-key"}),
	}, testutil.DiscardLogger(), nil)
	r.SetSecretProvider(prov)

	err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"})
	if err == nil {
		t.Fatal("expected a resolution failure")
	}
	if !strings.Contains(err.Error(), "rsa-private-key") {
		t.Fatalf("error should name the secret reference, got: %v", err)
	}
	if strings.Contains(err.Error(), "BEGIN PRIVATE KEY") {
		t.Fatalf("error leaked a secret fragment: %v", err)
	}
}

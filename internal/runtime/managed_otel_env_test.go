package runtime

import (
	"context"
	"strings"
	"sync"
	"testing"

	"relay/internal/app"
)

// These tests pin the runtime half of the managed Python and Node OpenTelemetry
// configuration: which environment reaches an execution container's create
// (provider configuration) versus the per-invocation request frame (application
// values and standard-precedence overlays), and how the private process-config
// identity joins the warm-pool generation key so a changed provider
// configuration rotates the warm process while ordinary env churn does not.

// otelContainer pairs the frame-capturing behavior with a record of the
// discard reasons the pool attributed to this container.
type otelContainer struct {
	capturingContainer
	rmu       sync.Mutex
	discarded []string
}

func (c *otelContainer) discard(reason string) bool {
	c.rmu.Lock()
	c.discarded = append(c.discarded, reason)
	c.rmu.Unlock()
	return c.capturingContainer.discard(reason)
}

// reasons returns the recorded discard reasons.
func (c *otelContainer) reasons() []string {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	return append([]string(nil), c.discarded...)
}

// invocations returns how many request frames this container served.
func (c *otelContainer) invocations() int {
	c.capturingContainer.mu.Lock()
	defer c.capturingContainer.mu.Unlock()
	return len(c.capturingContainer.envs)
}

// otelExecuteFixture drives the real Manager.Execute path with the injectable
// start seam: every container create records the create-time environment and
// returns a frame-capturing container, so no Docker daemon is needed.
type otelExecuteFixture struct {
	m    *Manager
	mu   sync.Mutex
	envs [][]string
	cs   []*otelContainer
}

func newOtelExecuteFixture() *otelExecuteFixture {
	m := &Manager{maxConcurrentInvocations: 4}
	m.containers = newContainerCache()
	f := &otelExecuteFixture{m: m}
	m.startContainerFn = func(
		_ context.Context, _ string, _ resolvedImage, env []string, _ app.ResourceLimits, _ RunMeta,
	) (reusableContainer, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		c := &otelContainer{}
		f.envs = append(f.envs, append([]string(nil), env...))
		f.cs = append(f.cs, c)
		return c, nil
	}
	return f
}

func (f *otelExecuteFixture) creates() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.cs)
}

// lastCreateEnv returns the recorded create-time environment of the most
// recent container create.
func (f *otelExecuteFixture) lastCreateEnv() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.envs) == 0 {
		return nil
	}
	return f.envs[len(f.envs)-1]
}

// lastFrameEnv returns the request-frame env the most recent container last
// served.
func (f *otelExecuteFixture) lastFrameEnv() map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.cs) == 0 {
		return nil
	}
	return f.cs[len(f.cs)-1].lastEnv()
}

// hasEntry reports whether a K=V slice carries the exact name=value entry.
func hasEntry(entries []string, want string) bool {
	for _, entry := range entries {
		if entry == want {
			return true
		}
	}
	return false
}

// hasName reports whether a K=V slice carries the given variable name.
func hasName(entries []string, name string) bool {
	return hasEntry(entries, name) || hasEntryPrefix(entries, name+"=")
}

func hasEntryPrefix(entries []string, prefix string) bool {
	for _, entry := range entries {
		if strings.HasPrefix(entry, prefix) {
			return true
		}
	}
	return false
}

// managedPrepared returns a hand-built Prepared shaped exactly like a managed
// Python app after Prepare+activate: plan env in Env, plan env plus the
// injected provider configuration in execEnv, and the private config identity.
func managedPrepared(otelConfig string) *Prepared {
	return &Prepared{
		Name:        "fn",
		Image:       "relay-app-fn:tag",
		Fingerprint: "fp",
		Env:         []string{"PYTHONDONTWRITEBYTECODE=1"},
		execEnv: []string{
			"PYTHONDONTWRITEBYTECODE=1",
			"OTEL_SERVICE_NAME=fn",
			"OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318",
		},
		otelConfig:  otelConfig,
		Concurrency: 1,
	}
}

// TestExecuteManagedOtelConfigChangeRotatesWarmPool proves an effective
// provider-configuration change supersedes the warm generation BEFORE the next
// invocation: the idle warm container is discarded (resources_changed) and a
// fresh one serves the new configuration, while repeating the SAME
// configuration keeps reusing it.
func TestExecuteManagedOtelConfigChangeRotatesWarmPool(t *testing.T) {
	rec := withSpanRecorder(t)
	f := newOtelExecuteFixture()
	const configA = "a11ce00000000000000000000000000000000000000000000000000000000001"
	const configB = "b22be00000000000000000000000000000000000000000000000000000000002"

	prepared := managedPrepared(configA)
	f.m.containers.setAppManagedConfig(prepared.Name, configA)

	if err := f.m.Execute(context.Background(), prepared, "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("first execute: %v", err)
	}
	if err := f.m.Execute(context.Background(), prepared, "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("second execute: %v", err)
	}
	if n := f.creates(); n != 1 {
		t.Fatalf("creates with a stable config = %d, want 1 (warm reuse)", n)
	}
	first := f.cs[0]

	// The next prepare+activate publishes the changed provider identity.
	prepared.otelConfig = configB
	f.m.containers.setAppManagedConfig(prepared.Name, configB)
	if err := f.m.Execute(context.Background(), prepared, "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("execute after config change: %v", err)
	}
	if n := f.creates(); n != 2 {
		t.Fatalf("creates after the config change = %d, want 2 (rotated)", n)
	}
	if got := first.reasons(); len(got) != 1 || got[0] != reasonResourcesChanged {
		t.Errorf("superseded warm container discards = %v, want [%s]", got, reasonResourcesChanged)
	}

	// The new generation is warm: another execute reuses it.
	if err := f.m.Execute(context.Background(), prepared, "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("execute after rotation: %v", err)
	}
	if n := f.creates(); n != 2 {
		t.Errorf("creates after rotation = %d, want 2 (new generation warmed)", n)
	}

	// The private identity never reaches spans or a container environment.
	for _, span := range rec.spans(t) {
		for _, kv := range span.Attributes {
			value := kv.Value.AsString()
			if strings.Contains(value, configA) || strings.Contains(value, configB) {
				t.Errorf("span %q attribute %q carries the managed config identity", span.Name, kv.Key)
			}
		}
	}
	for _, entry := range f.lastCreateEnv() {
		if strings.Contains(entry, configA) || strings.Contains(entry, configB) {
			t.Errorf("create env carries the managed config identity: %q", entry)
		}
	}
}

// TestExecuteOrdinaryEnvChangeKeepsWarmContainer proves the rotation is
// specific to OTel provider inputs: rotated ordinary per-invocation env (no
// OTEL name) leaves the generation identity — and the warm container — intact.
func TestExecuteOrdinaryEnvChangeKeepsWarmContainer(t *testing.T) {
	f := newOtelExecuteFixture()
	prepared := managedPrepared("a11ce00000000000000000000000000000000000000000000000000000000001")
	f.m.containers.setAppManagedConfig(prepared.Name, prepared.otelConfig)

	for _, extra := range [][]string{
		{"FOO=one"},
		{"FOO=two", "SECRET=rotated-value"},
		nil,
	} {
		if err := f.m.Execute(context.Background(), prepared, "h", []byte(`{}`), extra); err != nil {
			t.Fatalf("execute with %v: %v", extra, err)
		}
	}
	if n := f.creates(); n != 1 {
		t.Fatalf("creates = %d, want 1 (ordinary env churn must keep the warm container)", n)
	}
	if got := f.cs[0].invocations(); got != 3 {
		t.Errorf("warm container invocations = %d, want 3", got)
	}
	if got := f.cs[0].reasons(); len(got) != 0 {
		t.Errorf("warm container discards = %v, want none", got)
	}
}

// TestExecuteOtelFrameValueRotatesWarmContainer proves a per-invocation OTEL
// value (for example rotated exporter authentication, which never reaches
// Docker Config.Env) is part of the process's effective provider configuration:
// changing it rotates the warm process exactly like an injected config change.
func TestExecuteOtelFrameValueRotatesWarmContainer(t *testing.T) {
	f := newOtelExecuteFixture()
	prepared := managedPrepared("a11ce00000000000000000000000000000000000000000000000000000000001")
	f.m.containers.setAppManagedConfig(prepared.Name, prepared.otelConfig)

	auth := []string{"OTEL_EXPORTER_OTLP_HEADERS=authorization=tok-1"}
	if err := f.m.Execute(context.Background(), prepared, "h", []byte(`{}`), auth); err != nil {
		t.Fatalf("first execute: %v", err)
	}
	if err := f.m.Execute(context.Background(), prepared, "h", []byte(`{}`), auth); err != nil {
		t.Fatalf("same-auth execute: %v", err)
	}
	if n := f.creates(); n != 1 {
		t.Fatalf("creates with stable auth = %d, want 1", n)
	}

	rotated := []string{"OTEL_EXPORTER_OTLP_HEADERS=authorization=tok-2"}
	if err := f.m.Execute(context.Background(), prepared, "h", []byte(`{}`), rotated); err != nil {
		t.Fatalf("rotated-auth execute: %v", err)
	}
	if n := f.creates(); n != 2 {
		t.Fatalf("creates after auth rotation = %d, want 2 (the process must rotate)", n)
	}
	if got := f.cs[0].reasons(); len(got) != 1 || got[0] != reasonResourcesChanged {
		t.Errorf("superseded container discards = %v, want [%s]", got, reasonResourcesChanged)
	}

	// The secret itself stays on the frame path only: it never appears in a
	// create-time environment.
	for _, entry := range f.lastCreateEnv() {
		if strings.Contains(entry, "tok-1") || strings.Contains(entry, "tok-2") ||
			strings.Contains(entry, "OTEL_EXPORTER_OTLP_HEADERS") {
			t.Errorf("create env carries the frame authentication value: %q", entry)
		}
	}
}

// TestExecuteStalePreparedKeepsOwnProviderConfigIdentity is the
// prepared-snapshot generation regression: given an old Prepared activated,
// then a NEW provider config activated, when the already-admitted OLD
// Prepared executes, it must key and pool under ITS OWN snapshot — paired
// with its own execEnv — never under the newer activation. The new Prepared
// then rotates the generation and must get a fresh container with the new
// environment; a later stale execute is served only on a throwaway that is
// discarded, so old and new process configuration can never cross-reuse.
func TestExecuteStalePreparedKeepsOwnProviderConfigIdentity(t *testing.T) {
	const configOld = "a11ce00000000000000000000000000000000000000000000000000000000001"
	const configNew = "b22be00000000000000000000000000000000000000000000000000000000002"

	f := newOtelExecuteFixture()
	preparedOld := managedPrepared(configOld)
	preparedOld.execEnv = []string{
		"PYTHONDONTWRITEBYTECODE=1",
		"OTEL_SERVICE_NAME=fn",
		"OTEL_EXPORTER_OTLP_ENDPOINT=http://old-collector:4318",
	}
	preparedNew := managedPrepared(configNew)
	preparedNew.execEnv = []string{
		"PYTHONDONTWRITEBYTECODE=1",
		"OTEL_SERVICE_NAME=fn",
		"OTEL_EXPORTER_OTLP_ENDPOINT=http://new-collector:4318",
	}

	// The old configuration is activated first, then replaced by the new one
	// (prepare+activate of the replacement) — without any execution in
	// between, exactly the window a reconciler can create.
	f.m.containers.setAppManagedConfig(preparedOld.Name, configOld)
	f.m.containers.setAppManagedConfig(preparedOld.Name, configNew)

	// Given the already-admitted OLD Prepared, when it executes against the
	// NEWER activation, its environment must be keyed as the OLD config: a
	// create carrying the old environment, pooled under the old identity.
	if err := f.m.Execute(context.Background(), preparedOld, "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("execute with the stale Prepared: %v", err)
	}
	if n := f.creates(); n != 1 {
		t.Fatalf("creates after the stale execute = %d, want 1", n)
	}
	if !hasEntry(f.lastCreateEnv(), "OTEL_EXPORTER_OTLP_ENDPOINT=http://old-collector:4318") {
		t.Fatalf("stale execute created with the wrong environment: %v", f.lastCreateEnv())
	}

	// The new Prepared must NOT reuse that old-environment container: it
	// rotates the generation and creates with the new environment.
	if err := f.m.Execute(context.Background(), preparedNew, "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("execute with the new Prepared: %v", err)
	}
	if n := f.creates(); n != 2 {
		t.Fatalf("creates after the new Prepared = %d, want 2 (the old-environment container must not be reused)", n)
	}
	if !hasEntry(f.lastCreateEnv(), "OTEL_EXPORTER_OTLP_ENDPOINT=http://new-collector:4318") {
		t.Fatalf("new Prepared created with the wrong environment: %v", f.lastCreateEnv())
	}
	if got := f.cs[0].reasons(); len(got) != 1 || got[0] != reasonResourcesChanged {
		t.Errorf("superseded old-config container discards = %v, want [%s]", got, reasonResourcesChanged)
	}

	// A LATER stale execute can run at-least-once on a throwaway (its
	// generation is retired now), but that container is discarded on release
	// and never pooled under the new config.
	if err := f.m.Execute(context.Background(), preparedOld, "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("second stale execute: %v", err)
	}
	if n := f.creates(); n != 3 {
		t.Fatalf("creates after the retired-config execute = %d, want 3 (throwaway, never repooled)", n)
	}
	staleThrowaway := f.cs[2]
	if !hasEntry(f.lastCreateEnv(), "OTEL_EXPORTER_OTLP_ENDPOINT=http://old-collector:4318") {
		t.Fatalf("throwaway created with the wrong environment: %v", f.lastCreateEnv())
	}
	if !staleThrowaway.dead() {
		t.Error("the retired-config throwaway must be discarded on release, not pooled")
	}

	// The new configuration keeps its own warm container throughout.
	if err := f.m.Execute(context.Background(), preparedNew, "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("final new-config execute: %v", err)
	}
	if n := f.creates(); n != 3 {
		t.Errorf("creates at the end = %d, want 3 (the new-config container stays warm)", n)
	}
	if got := f.cs[1].invocations(); got != 2 {
		t.Errorf("new-config container invocations = %d, want 2 (it served both new executes)", got)
	}
	if !hasEntry(f.envs[1], "OTEL_EXPORTER_OTLP_ENDPOINT=http://new-collector:4318") {
		t.Errorf("new-config container create env = %v, want the new environment", f.envs[1])
	}
}

// TestExecuteProviderEnvScopedToManagedExecution pins the environment
// boundary: a managed invocation execution's container is created with the
// plan env PLUS the injected provider configuration, while an app without a
// managed provider (a service-only image, or a hand-built Prepared with no
// provider identity) keeps the historical plan-only create environment —
// which is also the only environment the service path consumes (worker
// services read Prepared.Env), so a service process never receives the
// provider configuration.
func TestExecuteProviderEnvScopedToManagedExecution(t *testing.T) {
	t.Run("managed python execution", func(t *testing.T) {
		f := newOtelExecuteFixture()
		prepared := managedPrepared("a11ce00000000000000000000000000000000000000000000000000000000001")
		f.m.containers.setAppManagedConfig(prepared.Name, prepared.otelConfig)

		if err := f.m.Execute(context.Background(), prepared, "h", []byte(`{}`), nil); err != nil {
			t.Fatalf("execute: %v", err)
		}
		got := f.lastCreateEnv()
		for _, want := range []string{
			"PYTHONDONTWRITEBYTECODE=1",
			"OTEL_SERVICE_NAME=fn",
			"OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318",
		} {
			if !hasEntry(got, want) {
				t.Errorf("create env = %v, want the entry %q", got, want)
			}
		}
		// The service path reads Prepared.Env: it must stay plan-only.
		for _, entry := range prepared.Env {
			if strings.HasPrefix(entry, "OTEL_") {
				t.Errorf(
					"Prepared.Env (the service environment) carries provider config: %q", entry,
				)
			}
		}
	})

	t.Run("app without a managed provider", func(t *testing.T) {
		f := newOtelExecuteFixture()
		prepared := &Prepared{
			Name:        "svc",
			Image:       "relay-app-svc:tag",
			Fingerprint: "fp",
			Env:         []string{"NODE_ENV=production"},
			Concurrency: 1,
		}
		if err := f.m.Execute(context.Background(), prepared, "h", []byte(`{}`), nil); err != nil {
			t.Fatalf("execute: %v", err)
		}
		got := f.lastCreateEnv()
		if len(got) != 1 || got[0] != "NODE_ENV=production" {
			t.Errorf("create env = %v, want exactly the plan env", got)
		}
		for _, entry := range got {
			if strings.HasPrefix(entry, "OTEL_") {
				t.Errorf("provider config leaked into a non-managed execution: %q", entry)
			}
		}
	})
}

// TestExecuteManagedFrameOverridesJoinRequestFrame proves the standard-precedence
// overlays travel in the per-invocation request frame (never in Docker
// Config.Env): an inherited worker trace-specific endpoint is explicitly
// cleared when the application supplies a generic endpoint, and the service-name
// rules restore or clear the injected app-name default as the application's
// frame values require.
func TestExecuteManagedFrameOverridesJoinRequestFrame(t *testing.T) {
	t.Run("app generic endpoint clears inherited trace-specific", func(t *testing.T) {
		f := newOtelExecuteFixture()
		prepared := managedPrepared("cfg")
		prepared.otelInheritedTraceSpecific = []string{"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"}
		f.m.containers.setAppManagedConfig(prepared.Name, prepared.otelConfig)

		extra := []string{
			"FOO=bar",
			"OTEL_EXPORTER_OTLP_ENDPOINT=http://app-collector:4318",
		}
		if err := f.m.Execute(context.Background(), prepared, "h", []byte(`{}`), extra); err != nil {
			t.Fatalf("execute: %v", err)
		}

		frame := f.lastFrameEnv()
		if value, ok := frame["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"]; !ok || value != "" {
			t.Errorf(
				"frame OTEL_EXPORTER_OTLP_TRACES_ENDPOINT = %q (present=%v), want an explicit empty override",
				value, ok,
			)
		}
		if frame["OTEL_EXPORTER_OTLP_ENDPOINT"] != "http://app-collector:4318" {
			t.Errorf("frame endpoint = %q, want the application value", frame["OTEL_EXPORTER_OTLP_ENDPOINT"])
		}
		if frame["FOO"] != "bar" {
			t.Errorf("frame env = %v, want the application's own values intact", frame)
		}
		// The override is frame-only: the create environment keeps the worker
		// value and never carries the application's endpoint.
		create := f.lastCreateEnv()
		if !hasEntry(create, "OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318") {
			t.Errorf("create env = %v, want the inherited worker endpoint", create)
		}
		if hasName(create, "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") {
			t.Errorf("create env carries the frame override: %v", create)
		}
	})

	t.Run("app resource attribute clears the injected service default", func(t *testing.T) {
		f := newOtelExecuteFixture()
		prepared := managedPrepared("cfg")
		f.m.containers.setAppManagedConfig(prepared.Name, prepared.otelConfig)

		extra := []string{"OTEL_RESOURCE_ATTRIBUTES=service.name=orders-attr"}
		if err := f.m.Execute(context.Background(), prepared, "h", []byte(`{}`), extra); err != nil {
			t.Fatalf("execute: %v", err)
		}
		frame := f.lastFrameEnv()
		if value, ok := frame["OTEL_SERVICE_NAME"]; !ok || value != "" {
			t.Errorf(
				"frame OTEL_SERVICE_NAME = %q (present=%v), want an explicit empty override so the Resource attribute wins",
				value, ok,
			)
		}
		if !hasEntry(f.lastCreateEnv(), "OTEL_SERVICE_NAME=fn") {
			t.Errorf("create env = %v, want the injected app-name default", f.lastCreateEnv())
		}
	})

	t.Run("explicitly empty app service name restores the app default", func(t *testing.T) {
		f := newOtelExecuteFixture()
		prepared := managedPrepared("cfg")
		f.m.containers.setAppManagedConfig(prepared.Name, prepared.otelConfig)

		if err := f.m.Execute(context.Background(), prepared, "h", []byte(`{}`),
			[]string{"OTEL_SERVICE_NAME="}); err != nil {
			t.Fatalf("execute: %v", err)
		}
		frame := f.lastFrameEnv()
		if frame["OTEL_SERVICE_NAME"] != "fn" {
			t.Errorf(
				"frame OTEL_SERVICE_NAME = %q, want the restored app-name default %q",
				frame["OTEL_SERVICE_NAME"], "fn",
			)
		}
	})
}

// TestExecuteAppOtelValuesStayOnFrame proves the corrected environment
// boundary at execution time: application OTel values carried per invocation
// (its own endpoint, exporter authentication, resource attributes) reach the
// request frame — which the bootstrap applies before initializing the
// provider — while the create-time Docker Config.Env keeps only the safe
// inherited worker configuration and the app service default.
func TestExecuteAppOtelValuesStayOnFrame(t *testing.T) {
	const authCanary = "CANARY-FRAME-AUTH-55"
	f := newOtelExecuteFixture()
	prepared := managedPrepared("cfg")
	prepared.otelInheritedTraceSpecific = []string{"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"}
	f.m.containers.setAppManagedConfig(prepared.Name, prepared.otelConfig)

	extra := []string{
		"OTEL_EXPORTER_OTLP_ENDPOINT=http://app-collector:4318",
		"OTEL_EXPORTER_OTLP_HEADERS=authorization=" + authCanary,
		"OTEL_RESOURCE_ATTRIBUTES=service.name=orders-attr,team=pay",
	}
	if err := f.m.Execute(context.Background(), prepared, "h", []byte(`{}`), extra); err != nil {
		t.Fatalf("execute: %v", err)
	}

	// The frame carries every application value the provider must see ...
	frame := f.lastFrameEnv()
	if frame["OTEL_EXPORTER_OTLP_ENDPOINT"] != "http://app-collector:4318" {
		t.Errorf(
			"frame endpoint = %q, want the application's own",
			frame["OTEL_EXPORTER_OTLP_ENDPOINT"],
		)
	}
	if frame["OTEL_EXPORTER_OTLP_HEADERS"] != "authorization="+authCanary {
		t.Errorf(
			"frame headers = %q, want the application's auth value",
			frame["OTEL_EXPORTER_OTLP_HEADERS"],
		)
	}
	if frame["OTEL_RESOURCE_ATTRIBUTES"] != "service.name=orders-attr,team=pay" {
		t.Errorf(
			"frame resource attributes = %q, want the application's own",
			frame["OTEL_RESOURCE_ATTRIBUTES"],
		)
	}
	// ... including the standard-precedence overlay for the injected default.
	if value, ok := frame["OTEL_SERVICE_NAME"]; !ok || value != "" {
		t.Errorf(
			"frame OTEL_SERVICE_NAME = %q (present=%v), want an explicit empty override so the Resource attribute wins",
			value, ok,
		)
	}

	// Docker Config.Env keeps only the safe inherited worker configuration and
	// the app service default: no application endpoint, auth, or resource
	// attribute may appear there.
	create := f.lastCreateEnv()
	if !hasEntry(create, "OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318") ||
		!hasEntry(create, "OTEL_SERVICE_NAME=fn") {
		t.Errorf("create env = %v, want the inherited worker endpoint and the app service default", create)
	}
	for _, entry := range create {
		if strings.Contains(entry, authCanary) {
			t.Errorf("application auth value leaked into Docker Config.Env: %q", entry)
		}
		if strings.Contains(entry, "app-collector") {
			t.Errorf("application endpoint leaked into Docker Config.Env: %q", entry)
		}
		name, _, _ := strings.Cut(entry, "=")
		if name == "OTEL_RESOURCE_ATTRIBUTES" || name == "OTEL_EXPORTER_OTLP_HEADERS" {
			t.Errorf("application-only %s leaked into Docker Config.Env: %q", name, entry)
		}
	}
}

// TestExecuteFrameOverlayGatedOnManagedIdentity is the regression for the
// overlay gate (F1): the standard-precedence frame overlays resolve values
// that ONLY a managed-provider execution injects (the app-name service
// default, inherited trace-specific provenance). A non-managed execution — a
// hand-built Prepared with no provider identity, such as a service-only
// image — must pass its request frame through VERBATIM: an application's
// explicit empty OTEL_SERVICE_NAME stays empty instead of being rewritten to
// an app name Relay does not manage tracing for. A managed execution keeps
// applying the app-name default, whichever invocation-capable engine (Python
// or Node) produced the Prepared.
func TestExecuteFrameOverlayGatedOnManagedIdentity(t *testing.T) {
	t.Run("non-managed prepared keeps the empty frame value unchanged", func(t *testing.T) {
		f := newOtelExecuteFixture()
		prepared := &Prepared{
			Name:        "svc",
			Image:       "relay-app-svc:tag",
			Fingerprint: "fp",
			Env:         []string{"NODE_ENV=production"},
			Concurrency: 1,
			// No otelConfig, no execEnv, no trace-specific provenance:
			// Relay does not manage tracing for this execution.
		}

		extra := []string{"OTEL_SERVICE_NAME="}
		if err := f.m.Execute(context.Background(), prepared, "h", []byte(`{}`), extra); err != nil {
			t.Fatalf("execute: %v", err)
		}

		frame := f.lastFrameEnv()
		value, ok := frame["OTEL_SERVICE_NAME"]
		if !ok {
			t.Fatalf("frame = %v, want the application's explicit OTEL_SERVICE_NAME entry", frame)
		}
		if value != "" {
			t.Errorf("frame OTEL_SERVICE_NAME = %q, want the explicit empty value passed through "+
				"unchanged (a non-managed execution must not gain an app-name default)", value)
		}
		// The whole frame is verbatim: no overlay may add anything either.
		if len(frame) != len(extra) {
			t.Errorf("frame = %v, want exactly the caller's %v for a non-managed execution", frame, extra)
		}
	})

	t.Run("managed prepared applies the app-name default", func(t *testing.T) {
		f := newOtelExecuteFixture()
		prepared := managedPrepared("cfg")
		f.m.containers.setAppManagedConfig(prepared.Name, prepared.otelConfig)

		if err := f.m.Execute(context.Background(), prepared, "h", []byte(`{}`),
			[]string{"OTEL_SERVICE_NAME="}); err != nil {
			t.Fatalf("execute: %v", err)
		}
		frame := f.lastFrameEnv()
		if frame["OTEL_SERVICE_NAME"] != "fn" {
			t.Errorf("frame OTEL_SERVICE_NAME = %q, want the managed app-name default %q",
				frame["OTEL_SERVICE_NAME"], "fn")
		}
	})

	t.Run("node-shaped managed prepared applies the app-name default", func(t *testing.T) {
		// The gate is the private provider identity, not the engine: a Node
		// invocation image resolves one at Prepare and therefore receives the
		// same standard-precedence overlays a Python image does.
		f := newOtelExecuteFixture()
		prepared := &Prepared{
			Name:        "web",
			Image:       "relay-app-web:tag",
			Fingerprint: "fp",
			Env:         []string{},
			execEnv: []string{
				"OTEL_SERVICE_NAME=web",
				"OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318",
			},
			otelConfig:  "cfg",
			Concurrency: 1,
		}
		f.m.containers.setAppManagedConfig(prepared.Name, prepared.otelConfig)

		if err := f.m.Execute(context.Background(), prepared, "h", []byte(`{}`),
			[]string{"OTEL_SERVICE_NAME="}); err != nil {
			t.Fatalf("execute: %v", err)
		}
		frame := f.lastFrameEnv()
		if frame["OTEL_SERVICE_NAME"] != "web" {
			t.Errorf("frame OTEL_SERVICE_NAME = %q, want the managed app-name default %q",
				frame["OTEL_SERVICE_NAME"], "web")
		}
		// The overlay stays frame-only: the create environment already carries
		// the injected default and must not gain a second copy or the overlay.
		if got := f.lastCreateEnv(); !hasEntry(got, "OTEL_SERVICE_NAME=web") {
			t.Errorf("create env = %v, want the injected app-name default", got)
		}
	})
}

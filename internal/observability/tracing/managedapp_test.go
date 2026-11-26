package tracing

import (
	"regexp"
	"strings"
	"testing"
)

// resolveFor runs the managed-app resolution with an explicit worker
// environment so tests never depend on the host's ambient OTEL_* variables.
func resolveFor(appName string, appEnv map[string]string, workerEnv ...string) ManagedAppConfig {
	return resolveManagedAppConfig(appName, appEnv, workerEnv)
}

// envOf renders K=V entries as a map for readable assertions.
func envOf(entries []string) map[string]string {
	out := make(map[string]string, len(entries))
	for _, kv := range entries {
		name, value, ok := strings.Cut(kv, "=")
		if !ok {
			out[kv] = "<no-equals>"
			continue
		}
		out[name] = value
	}
	return out
}

// containsExact reports whether entries contains the exact string value.
func containsExact(entries []string, want string) bool {
	for _, entry := range entries {
		if entry == want {
			return true
		}
	}
	return false
}

// TestManagedAppConfigExcludesWorkerIdentityAndCredentials proves the injected
// environment carries only the allowlisted worker inheritance plus the app-name
// service default: Relay's own service identity, resource attributes, exporter
// headers, certificates, client keys, and every unlisted OTEL variable stay out,
// and no credential value reaches an entry an operator can read from the
// container.
func TestManagedAppConfigExcludesWorkerIdentityAndCredentials(t *testing.T) {
	const headerCanary = "CANARY-HEADER-VALUE-xyz"
	const certCanary = "CANARY-CERT-VALUE-xyz"

	cfg := resolveFor("orders", nil,
		"OTEL_SERVICE_NAME=relay-worker",
		"OTEL_RESOURCE_ATTRIBUTES=service.name=relay,service.instance.id=i-1",
		"OTEL_EXPORTER_OTLP_HEADERS=authorization="+headerCanary,
		"OTEL_EXPORTER_OTLP_TRACES_HEADERS=x-api-key="+headerCanary,
		"OTEL_EXPORTER_OTLP_CERTIFICATE="+certCanary,
		"OTEL_EXPORTER_OTLP_CLIENT_KEY="+certCanary,
		"OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE=/certs/client.pem",
		"OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4317",
		"OTEL_EXPORTER_OTLP_PROTOCOL=grpc",
		"OTEL_EXPORTER_OTLP_COMPRESSION=gzip",
		"OTEL_TRACES_SAMPLER=parentbased_trace_id_ratio",
		"OTEL_TRACES_SAMPLER_ARG=0.25",
		"OTEL_BSP_SCHEDULE_DELAY=250",
		"OTEL_PROPAGATORS=b3",
		"OTEL_LOGS_EXPORTER=none",
	)

	got := envOf(cfg.Env)

	// The app identity replaces Relay's own; worker identity never appears.
	if got[envServiceName] != "orders" {
		t.Errorf("service name = %q, want the app-name default %q", got[envServiceName], "orders")
	}
	if _, ok := got[envResourceAttributes]; ok {
		t.Errorf("worker %s leaked into the injected env: %v", envResourceAttributes, cfg.Env)
	}
	if value, ok := got[envServiceName]; ok && value == "relay-worker" {
		t.Errorf("worker service identity inherited: %q", value)
	}

	// Allowlisted operational values are inherited.
	for name, want := range map[string]string{
		envEndpoint:         "http://collector:4317",
		envProtocol:         "grpc",
		envCompression:      "gzip",
		envTracesSampler:    "parentbased_trace_id_ratio",
		envTracesSamplerArg: "0.25",
		envBSPScheduleDelay: "250",
		envSDKDisabled:      "",
	} {
		if value, ok := got[name]; ok && value != want {
			t.Errorf("%s = %q, want %q", name, value, want)
		}
	}
	if _, ok := got[envEndpoint]; !ok {
		t.Errorf("allowlisted worker endpoint missing from %v", cfg.Env)
	}

	// Authentication and unlisted variables are excluded by name ...
	for _, name := range []string{
		"OTEL_EXPORTER_OTLP_HEADERS",
		"OTEL_EXPORTER_OTLP_TRACES_HEADERS",
		"OTEL_EXPORTER_OTLP_CERTIFICATE",
		"OTEL_EXPORTER_OTLP_CLIENT_KEY",
		"OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE",
		"OTEL_PROPAGATORS",
		"OTEL_LOGS_EXPORTER",
	} {
		if _, ok := got[name]; ok {
			t.Errorf("%s must not be injected into the managed process env", name)
		}
	}
	// ... and no credential VALUE reaches any injected entry.
	for _, entry := range cfg.Env {
		if strings.Contains(entry, headerCanary) || strings.Contains(entry, certCanary) {
			t.Fatalf("injected env leaks a credential value: %q", entry)
		}
	}
}

// TestManagedAppConfigDeterministic pins that identical inputs always resolve
// to the identical environment and fingerprint, so the warm-pool generation key
// derived from it cannot churn between reconciliations.
func TestManagedAppConfigDeterministic(t *testing.T) {
	worker := []string{
		"OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4317",
		"OTEL_EXPORTER_OTLP_PROTOCOL=grpc",
	}
	appEnv := map[string]string{"OTEL_EXPORTER_OTLP_HEADERS": "authorization=tok"}

	first := resolveFor("orders", appEnv, worker...)
	second := resolveFor("orders", appEnv, worker...)
	third := resolveFor("orders", map[string]string{"OTEL_EXPORTER_OTLP_HEADERS": "authorization=other"}, worker...)

	if strings.Join(first.Env, "\x00") != strings.Join(second.Env, "\x00") {
		t.Errorf("Env is not deterministic:\n first = %v\nsecond = %v", first.Env, second.Env)
	}
	if first.Fingerprint != second.Fingerprint {
		t.Errorf("Fingerprint is not deterministic: %q vs %q", first.Fingerprint, second.Fingerprint)
	}
	if first.Fingerprint == third.Fingerprint {
		t.Error("an app auth change must produce a different fingerprint")
	}
}

// TestManagedAppConfigEndpointSanitization covers the value shapes that must
// not reach a managed process: a credential-bearing or malformed worker
// endpoint is dropped to the standard "unset" empty value (never rewritten
// into a silently unauthenticated target), while ordinary http and bare
// authority forms pass through, and unsupported compression degrades to empty.
func TestManagedAppConfigEndpointSanitization(t *testing.T) {
	cases := []struct {
		name   string
		worker []string
		want   map[string]string
	}{
		{
			name:   "bare gRPC authority kept",
			worker: []string{envEndpoint + "=collector:4317"},
			want:   map[string]string{envEndpoint: "collector:4317"},
		},
		{
			name:   "plain http endpoint kept",
			worker: []string{envTracesEndpoint + "=http://collector:4318"},
			want:   map[string]string{envTracesEndpoint: "http://collector:4318"},
		},
		{
			name:   "userinfo credential dropped",
			worker: []string{envEndpoint + "=http://user:pass@collector:4317"},
			want:   map[string]string{envEndpoint: ""},
		},
		{
			name:   "query credential dropped",
			worker: []string{envEndpoint + "=http://collector:4318?apikey=secret"},
			want:   map[string]string{envEndpoint: ""},
		},
		{
			name:   "fragment dropped",
			worker: []string{envTracesEndpoint + "=http://collector:4318#token"},
			want:   map[string]string{envTracesEndpoint: ""},
		},
		{
			name:   "bare authority with credential marker dropped",
			worker: []string{envEndpoint + "=collector:4317?apikey=secret"},
			want:   map[string]string{envEndpoint: ""},
		},
		{
			name:   "malformed URL dropped",
			worker: []string{envEndpoint + "=http://collector:4318/%zz"},
			want:   map[string]string{envEndpoint: ""},
		},
		{
			name:   "compression gzip kept",
			worker: []string{envCompression + "=gzip"},
			want:   map[string]string{envCompression: "gzip"},
		},
		{
			name:   "unsupported compression emptied",
			worker: []string{envCompression + "=zstd"},
			want:   map[string]string{envCompression: ""},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := resolveFor("orders", nil, tc.worker...)
			got := envOf(cfg.Env)
			for name, want := range tc.want {
				value, ok := got[name]
				if !ok || value != want {
					t.Errorf("%s = %q (present=%v), want %q", name, value, ok, want)
					continue
				}
				// A dropped value is still an explicit empty entry (the
				// standard "unset"), so it clears any lower-tier value.
				if want == "" && !containsExact(cfg.Env, name+"=") {
					t.Errorf("%s dropped but not emitted as an explicit empty entry", name)
				}
			}
		})
	}
}

// TestManagedAppConfigWorkerTraceSpecificProvenance proves the resolver
// records exactly the NON-EMPTY trace-specific worker values as inherited
// provenance (names only), so the runtime can clear them per invocation when
// the application supplies a generic counterpart.
func TestManagedAppConfigWorkerTraceSpecificProvenance(t *testing.T) {
	cfg := resolveFor("orders", nil,
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://collector:4318",
		"OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=grpc",
		"OTEL_EXPORTER_OTLP_TRACES_TIMEOUT=",
	)

	want := []string{envTracesEndpoint, envTracesProtocol}
	if strings.Join(cfg.InheritedTraceSpecific, ",") != strings.Join(want, ",") {
		t.Errorf("InheritedTraceSpecific = %v, want %v (empty values excluded, sorted)",
			cfg.InheritedTraceSpecific, want)
	}
	for _, name := range cfg.InheritedTraceSpecific {
		if strings.Contains(name, "http") || strings.Contains(name, "grpc") {
			t.Fatalf("provenance must carry names only, got %q", name)
		}
	}

	// No worker trace-specific values, no provenance.
	clean := resolveFor("orders", nil, "OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318")
	if len(clean.InheritedTraceSpecific) != 0 {
		t.Errorf("InheritedTraceSpecific = %v, want empty when no trace-specific value is inherited",
			clean.InheritedTraceSpecific)
	}
}

// TestManagedAppInvocationEnvOverridesTraceSpecific proves the frame overlay
// that lets an application GENERIC endpoint/protocol/timeout/compression beat
// an inherited worker trace-specific value: the frame gets exactly one empty
// trace-specific entry, and an application value for the trace-specific name
// itself (or the absence of the generic one) produces no overlay.
func TestManagedAppInvocationEnvOverridesTraceSpecific(t *testing.T) {
	inherited := []string{envTracesEndpoint, envTracesProtocol}

	cases := []struct {
		name      string
		inherited []string
		extraEnv  []string
		want      map[string]string
	}{
		{
			name:      "app generic endpoint clears inherited trace-specific",
			inherited: inherited,
			extraEnv:  []string{envEndpoint + "=http://app-collector:4318"},
			want:      map[string]string{envTracesEndpoint: ""},
		},
		{
			name:      "app trace-specific wins, no empty overlay",
			inherited: inherited,
			extraEnv: []string{
				envEndpoint + "=http://app-collector:4318",
				envTracesEndpoint + "=http://app-collector:4318/v1/traces",
			},
			want: map[string]string{},
		},
		{
			name:      "app generic protocol clears inherited trace protocol",
			inherited: inherited,
			extraEnv:  []string{envProtocol + "=http/protobuf"},
			want:      map[string]string{envTracesProtocol: ""},
		},
		{
			name:      "no app generic value adds nothing",
			inherited: inherited,
			extraEnv:  []string{"FOO=bar"},
			want:      map[string]string{},
		},
		{
			name:      "no inherited provenance adds nothing",
			inherited: nil,
			extraEnv:  []string{envEndpoint + "=http://app-collector:4318"},
			want:      map[string]string{},
		},
		{
			name:      "app generic insecure clears inherited trace-insecure",
			inherited: []string{envTracesInsecure},
			extraEnv:  []string{envInsecure + "=false"},
			want:      map[string]string{envTracesInsecure: ""},
		},
		{
			name:      "app trace-specific insecure wins, no empty overlay",
			inherited: []string{envTracesInsecure},
			extraEnv: []string{
				envInsecure + "=false",
				envTracesInsecure + "=true",
			},
			want: map[string]string{},
		},
		{
			name:      "app insecure alone without inherited provenance adds nothing",
			inherited: nil,
			extraEnv:  []string{envInsecure + "=false"},
			want:      map[string]string{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ManagedAppInvocationEnvOverrides("orders", tc.inherited, tc.extraEnv)
			if len(got) != len(tc.want) {
				t.Fatalf("overrides = %v, want %v", got, tc.want)
			}
			for name, want := range tc.want {
				if value, ok := got[name]; !ok || value != want {
					t.Errorf("override[%s] = %q (present=%v), want %q", name, value, ok, want)
				}
			}
		})
	}
}

// TestManagedAppInvocationEnvOverridesServiceName pins the per-invocation
// service-name precedence: an explicit non-empty app name wins unchanged, an
// explicitly empty app name restores the app-name default (standard-unset),
// and a non-empty service.name inside the app's OTEL_RESOURCE_ATTRIBUTES
// clears the injected default so the Resource attribute takes effect.
func TestManagedAppInvocationEnvOverridesServiceName(t *testing.T) {
	cases := []struct {
		name     string
		extraEnv []string
		want     map[string]string
	}{
		{
			name:     "explicit app name wins unchanged",
			extraEnv: []string{envServiceName + "=checkout-svc"},
			want:     map[string]string{},
		},
		{
			name:     "explicitly empty app name restores the app default",
			extraEnv: []string{envServiceName + "="},
			want:     map[string]string{envServiceName: "orders"},
		},
		{
			name:     "resource attribute clears the injected default",
			extraEnv: []string{envResourceAttributes + "=service.name=orders-attr"},
			want:     map[string]string{envServiceName: ""},
		},
		{
			name: "empty service name with resource attribute leaves attrs to win",
			extraEnv: []string{
				envServiceName + "=",
				envResourceAttributes + "=service.name=orders-attr",
			},
			want: map[string]string{},
		},
		{
			name:     "resource attribute without service.name keeps the default",
			extraEnv: []string{envResourceAttributes + "=deployment.environment=prod"},
			want:     map[string]string{},
		},
		{
			// Standard-unset spelled INSIDE the resource attributes: an empty
			// service.name there must not clear the injected app-name default.
			name:     "empty service.name resource attribute keeps the default",
			extraEnv: []string{envResourceAttributes + "=service.name="},
			want:     map[string]string{},
		},
		{
			name:     "no app identity value adds nothing",
			extraEnv: []string{"FOO=bar"},
			want:     map[string]string{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ManagedAppInvocationEnvOverrides("orders", nil, tc.extraEnv)
			if len(got) != len(tc.want) {
				t.Fatalf("overrides = %v, want %v", got, tc.want)
			}
			for name, want := range tc.want {
				if value, ok := got[name]; !ok || value != want {
					t.Errorf("override[%s] = %q (present=%v), want %q", name, value, ok, want)
				}
			}
		})
	}
}

// TestManagedAppConfigInheritsInsecureFlags proves the standard insecure
// booleans are part of the SAFE worker inheritance (an authority-form endpoint
// such as "collector:4317" is plaintext only when OTEL_EXPORTER_OTLP_INSECURE is
// present), the trace-specific one is recorded as inherited provenance for the
// per-invocation precedence overlay, and neither boolean is treated as secret.
func TestManagedAppConfigInheritsInsecureFlags(t *testing.T) {
	cfg := resolveFor("orders", nil,
		"OTEL_EXPORTER_OTLP_ENDPOINT=collector:4317",
		envInsecure+"=true",
		envTracesInsecure+"=false",
	)
	got := envOf(cfg.Env)

	if got[envInsecure] != "true" {
		t.Errorf("%s = %q, want the inherited %q", envInsecure, got[envInsecure], "true")
	}
	if got[envTracesInsecure] != "false" {
		t.Errorf("%s = %q, want the inherited %q", envTracesInsecure, got[envTracesInsecure], "false")
	}
	if got[envEndpoint] != "collector:4317" {
		t.Errorf("%s = %q, want the bare authority kept", envEndpoint, got[envEndpoint])
	}
	if got[envServiceName] != "orders" {
		t.Errorf("service name = %q, want the app-name default", got[envServiceName])
	}
	// Only the TRACE-SPECIFIC name is provenance; the generic flag never is.
	provenance := strings.Join(cfg.InheritedTraceSpecific, ",")
	if provenance != envTracesInsecure {
		t.Errorf("InheritedTraceSpecific = %q, want exactly %q", provenance, envTracesInsecure)
	}

	// An EMPTY worker trace-insecure value is standard-unset and is not
	// recorded as provenance (nothing for an app generic value to clear).
	unset := resolveFor("orders", nil, envTracesInsecure+"=")
	for _, name := range unset.InheritedTraceSpecific {
		if name == envTracesInsecure {
			t.Errorf("an explicitly empty %s must not be inherited provenance", envTracesInsecure)
		}
	}
}

// TestManagedAppAppValuesStayOutOfInjectedEnv proves the environment boundary
// after the correction: application-specific OTEL settings — resource
// attributes, the app's own endpoint, exporter authentication, the ownership
// opt-out, and the Python tracer-provider selection — NEVER enter the injected
// environment that becomes Docker Config.Env. They remain visible only through
// the private identity (and the per-invocation frame), while the safe worker
// inheritance and the app service default are what the container actually
// receives.
func TestManagedAppAppValuesStayOutOfInjectedEnv(t *testing.T) {
	const authCanary = "CANARY-APP-AUTH-77"
	const headersName = "OTEL_EXPORTER_OTLP_HEADERS"
	appEnv := map[string]string{
		envResourceAttributes:         "service.name=orders-attr,team=pay",
		envEndpoint:                   "http://app-collector:4318",
		headersName:                   "authorization=" + authCanary,
		"RELAY_OTEL_PROVIDER":         "application",
		"OTEL_PYTHON_TRACER_PROVIDER": "appowned",
	}
	worker := []string{
		envEndpoint + "=http://worker-collector:4317",
		envInsecure + "=true",
	}
	cfg := resolveFor("orders", appEnv, worker...)

	appOnlyNames := map[string]bool{
		envResourceAttributes:         true,
		headersName:                   true,
		"RELAY_OTEL_PROVIDER":         true,
		"OTEL_PYTHON_TRACER_PROVIDER": true,
	}
	for _, entry := range cfg.Env {
		name, _, _ := strings.Cut(entry, "=")
		if appOnlyNames[name] {
			t.Errorf("application-only %s leaked into the injected environment: %q", name, entry)
		}
		if strings.Contains(entry, authCanary) {
			t.Errorf("application auth value leaked into the injected environment: %q", entry)
		}
		if strings.Contains(entry, "app-collector") {
			t.Errorf("application endpoint leaked into the injected environment: %q", entry)
		}
	}
	got := envOf(cfg.Env)
	if got[envEndpoint] != "http://worker-collector:4317" {
		t.Errorf("%s = %q, want the safe inherited worker endpoint", envEndpoint, got[envEndpoint])
	}
	if got[envInsecure] != "true" {
		t.Errorf("%s = %q, want the inherited worker value", envInsecure, got[envInsecure])
	}
	if got[envServiceName] != "orders" {
		t.Errorf("service name = %q, want the app-name default", got[envServiceName])
	}

	// The application values still change the private identity, so a template
	// edit rotates the warm process even though Config.Env does not move.
	base := resolveFor("orders", nil, worker...)
	if cfg.Fingerprint == base.Fingerprint {
		t.Error("application OTEL values must be covered by the private identity")
	}
}

// TestManagedAppFingerprintCoversAppOTelChanges proves the private identity
// rotates warm processes for ANY application configuration that can affect
// provider construction — exporter authentication, endpoint, the app-only
// ownership opt-out — while an unrelated application env change leaves it
// untouched. The fingerprint itself is never injected into the environment.
func TestManagedAppFingerprintCoversAppOTelChanges(t *testing.T) {
	worker := []string{"OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4317"}
	base := resolveFor("orders", nil, worker...)

	otelChanges := []struct {
		name   string
		appEnv map[string]string
	}{
		{"auth header change", map[string]string{"OTEL_EXPORTER_OTLP_HEADERS": "authorization=tok-1"}},
		{"auth header value change", map[string]string{"OTEL_EXPORTER_OTLP_HEADERS": "authorization=tok-2"}},
		{"endpoint override", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://other:4318"}},
		{"resource attributes", map[string]string{"OTEL_RESOURCE_ATTRIBUTES": "service.name=orders"}},
		{"service name", map[string]string{envServiceName: "checkout"}},
		{"provider ownership opt-out", map[string]string{"RELAY_OTEL_PROVIDER": "application"}},
		{"sdk disabled opt-out", map[string]string{"OTEL_SDK_DISABLED": "true"}},
		{"generic insecure flag", map[string]string{envInsecure: "false"}},
		{"trace-specific insecure flag", map[string]string{envTracesInsecure: "true"}},
		{"python tracer provider selection", map[string]string{"OTEL_PYTHON_TRACER_PROVIDER": "appowned"}},
		{"explicit empty value", map[string]string{"OTEL_EXPORTER_OTLP_COMPRESSION": ""}},
	}
	for _, tc := range otelChanges {
		if got := resolveFor("orders", tc.appEnv, worker...); got.Fingerprint == base.Fingerprint {
			t.Errorf("%s: fingerprint must change, but it stayed %q", tc.name, base.Fingerprint)
		}
	}

	unrelated := resolveFor("orders", map[string]string{
		"LOG_LEVEL":       "debug",
		"FEATURE_FLAG":    "on",
		"UNRELATED_ORDER": "1",
	}, worker...)
	if unrelated.Fingerprint != base.Fingerprint {
		t.Errorf("an unrelated app env change must not rotate the identity: %q != %q",
			unrelated.Fingerprint, base.Fingerprint)
	}

	// A worker-side change to an inherited operational value (the insecure
	// flag) also rotates the identity: the effective process configuration
	// changed even though the application did not move.
	withInsecure := func(value string) []string {
		return append(append([]string(nil), worker...), envInsecure+"="+value)
	}
	workerInsecureTrue := resolveFor("orders", nil, withInsecure("true")...)
	workerInsecureFalse := resolveFor("orders", nil, withInsecure("false")...)
	if workerInsecureTrue.Fingerprint == base.Fingerprint {
		t.Error("adding a worker insecure flag must rotate the identity")
	}
	if workerInsecureTrue.Fingerprint == workerInsecureFalse.Fingerprint {
		t.Error("a changed worker insecure value must rotate the identity")
	}

	// The identity is private: it never appears as (or inside) an injected
	// environment entry, and the raw values never appear inside it.
	reHex := regexp.MustCompile(`^[0-9a-f]+$`)
	if !reHex.MatchString(base.Fingerprint) {
		t.Errorf("fingerprint %q is not a bare hex identity", base.Fingerprint)
	}
	for _, entry := range base.Env {
		if strings.Contains(entry, base.Fingerprint) {
			t.Errorf("fingerprint leaked into the injected env: %q", entry)
		}
	}
}

// TestManagedAppInvocationFingerprint extends the prepared identity with the
// per-invocation frame values: a changed exporter-authentication secret (or
// any OTEL/app-ownership value) rotates the identity, while ordinary
// per-invocation env keeps it stable — and no value or fingerprint is exposed.
func TestManagedAppInvocationFingerprint(t *testing.T) {
	const base = "prepared-base-fingerprint"
	const authCanary = "CANARY-AUTH-SECRET-42"

	withAuth := ManagedAppInvocationFingerprint(base, []string{
		"OTEL_EXPORTER_OTLP_HEADERS=authorization=" + authCanary,
	})
	rotated := ManagedAppInvocationFingerprint(base, []string{
		"OTEL_EXPORTER_OTLP_HEADERS=authorization=other-secret",
	})
	optOut := ManagedAppInvocationFingerprint(base, []string{"RELAY_OTEL_PROVIDER=application"})
	insecure := ManagedAppInvocationFingerprint(base, []string{"OTEL_EXPORTER_OTLP_INSECURE=false"})
	providerOne := ManagedAppInvocationFingerprint(base, []string{"OTEL_PYTHON_TRACER_PROVIDER=one"})
	providerTwo := ManagedAppInvocationFingerprint(base, []string{"OTEL_PYTHON_TRACER_PROVIDER=two"})
	ordinary := ManagedAppInvocationFingerprint(base, []string{"FOO=bar", "SECRET=" + authCanary})
	empty := ManagedAppInvocationFingerprint(base, nil)

	if withAuth == base || withAuth == rotated || withAuth == optOut {
		t.Errorf("OTel frame values must rotate the identity: base=%q auth=%q rotated=%q optOut=%q",
			base, withAuth, rotated, optOut)
	}
	if insecure == base {
		t.Error("an insecure-flag frame value must rotate the identity")
	}
	if providerOne == base || providerOne == providerTwo {
		t.Errorf("the app-owned tracer-provider selection must rotate the identity: one=%q two=%q base=%q",
			providerOne, providerTwo, base)
	}
	if ordinary != base || empty != base {
		t.Errorf("non-OTel frame values must keep the prepared identity: ordinary=%q empty=%q base=%q",
			ordinary, empty, base)
	}
	if withAuth == ordinary {
		t.Error("an OTel value and an unrelated value produced the same identity")
	}
	// The secret value is hashed, never carried: the identity must not contain
	// any part of it (the canary cannot appear verbatim in a hex digest, but
	// also assert it does not appear as a substring at all).
	for _, identity := range []string{withAuth, rotated, optOut} {
		if strings.Contains(identity, authCanary) {
			t.Fatalf("identity leaks the secret value: %q", identity)
		}
	}
}

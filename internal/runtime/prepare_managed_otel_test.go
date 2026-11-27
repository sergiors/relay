package runtime

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/app"
)

// TestPrepareManagedOtelScopedToInvocationHandlerApps proves where the
// managed provider configuration is decided: only an invocation-capable engine
// app (Python or Node) that actually serves event/schedule handlers resolves
// an injected provider environment and a private config identity. A
// service-only image — Python or Node — resolves neither, and Prepared.Env
// (the environment every service container and the reconciler consume) never
// carries provider configuration for any app, so a service process cannot
// inherit it.
func TestPrepareManagedOtelScopedToInvocationHandlerApps(t *testing.T) {
	// Pin the worker tier so host-ambient OTEL values cannot change the
	// assertions. The worker's own identity and credentials are not part of
	// the allowlist; only operational values are inherited.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://worker-collector:4318")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
	t.Setenv("OTEL_EXPORTER_OTLP_COMPRESSION", "gzip")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")

	prepare := func(t *testing.T, fn app.App) *Prepared {
		t.Helper()
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound,
				body: `{"message":"no such image"}`},
			dockerRoute{method: http.MethodPost, path: "/build", body: `{}`},
		)
		m := newLifecycleManager(t, cli, context.Background())
		got, err := m.Prepare(context.Background(), fn)
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		return got
	}

	writeHandlerSource := func(t *testing.T) string {
		t.Helper()
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "main.py"),
			[]byte("def process(event, context):\n    pass\n"), 0o644); err != nil {
			t.Fatalf("write handler source: %v", err)
		}
		return dir
	}

	writeNodeHandlerSource := func(t *testing.T) string {
		t.Helper()
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "index.js"),
			[]byte("export function h(event, context){}\n"), 0o644); err != nil {
			t.Fatalf("write handler source: %v", err)
		}
		return dir
	}

	t.Run("python handler app gets the provider environment", func(t *testing.T) {
		const appAuthCanary = "CANARY-TEMPLATE-AUTH-99"
		got := prepare(t, app.App{
			Name: "orders",
			Dir:  writeHandlerSource(t),
			Template: &app.Template{
				Runtime: "python3.14",
				Env: map[string]string{
					"LOG_LEVEL": "info",
					// Application-specific OTel settings live on the template
					// env: they must reach only the private identity and the
					// per-invocation frame, never Docker Config.Env.
					"OTEL_RESOURCE_ATTRIBUTES":    "team=pay",
					"OTEL_EXPORTER_OTLP_ENDPOINT": "http://app-collector:4318",
					"OTEL_EXPORTER_OTLP_HEADERS":  "authorization=" + appAuthCanary,
					"RELAY_OTEL_PROVIDER":         "application",
					"OTEL_PYTHON_TRACER_PROVIDER": "appowned",
				},
				Events: []app.EventRule{{Handler: "main.process"}},
			},
		})

		if len(got.execEnv) == 0 {
			t.Fatal("managed Python execution resolved no execEnv")
		}
		// The container receives the SAFE inherited worker configuration plus
		// the app service default, and nothing else.
		for _, want := range []string{
			"OTEL_EXPORTER_OTLP_ENDPOINT=http://worker-collector:4318",
			"OTEL_EXPORTER_OTLP_PROTOCOL=grpc",
			"OTEL_EXPORTER_OTLP_COMPRESSION=gzip",
			"OTEL_EXPORTER_OTLP_INSECURE=true",
			"OTEL_SERVICE_NAME=orders",
		} {
			if !hasEntry(got.execEnv, want) {
				t.Errorf("execEnv = %v, want the entry %q", got.execEnv, want)
			}
		}
		if got.otelConfig == "" {
			t.Error("managed Python execution resolved an empty private config identity")
		}

		// The service path (and every other Prepared.Env consumer) must stay
		// free of provider configuration.
		for _, entry := range got.Env {
			if strings.HasPrefix(entry, "OTEL_") {
				t.Errorf("Prepared.Env carries provider config (services would inherit it): %q", entry)
			}
		}
		// Application-specific names and values never reach Config.Env: only
		// the plan env plus allowlisted worker entries may appear beyond it.
		allowedWorkerNames := map[string]bool{
			"OTEL_EXPORTER_OTLP_ENDPOINT":    true,
			"OTEL_EXPORTER_OTLP_PROTOCOL":    true,
			"OTEL_EXPORTER_OTLP_COMPRESSION": true,
			"OTEL_EXPORTER_OTLP_INSECURE":    true,
			"OTEL_SERVICE_NAME":              true,
		}
		appOnlyNames := map[string]bool{
			"OTEL_RESOURCE_ATTRIBUTES":    true,
			"OTEL_EXPORTER_OTLP_HEADERS":  true,
			"RELAY_OTEL_PROVIDER":         true,
			"OTEL_PYTHON_TRACER_PROVIDER": true,
		}
		planEnv := map[string]bool{}
		for _, entry := range got.Env {
			planEnv[entry] = true
		}
		for _, entry := range got.execEnv {
			if planEnv[entry] {
				continue
			}
			name, _, _ := strings.Cut(entry, "=")
			if appOnlyNames[name] {
				t.Errorf("application-only %s leaked into Docker Config.Env: %q", name, entry)
				continue
			}
			if !allowedWorkerNames[name] && !strings.HasPrefix(name, "OTEL_") {
				t.Errorf("unexpected execEnv entry beyond the plan env: %q", entry)
			}
			if strings.Contains(entry, appAuthCanary) || strings.Contains(entry, "app-collector") {
				t.Errorf("application value leaked into Docker Config.Env: %q", entry)
			}
		}
		// execEnv is exactly the plan env plus the injected configuration
		// (worker allowlist entries that are actually set, plus the default).
		if len(got.execEnv) < len(got.Env)+5 {
			t.Errorf("execEnv = %v, want the plan env plus the injected OTEL entries (Env = %v)",
				got.execEnv, got.Env)
		}
		for _, entry := range got.Env {
			if !hasEntry(got.execEnv, entry) {
				t.Errorf("execEnv = %v is missing the plan env entry %q", got.execEnv, entry)
			}
		}
	})

	t.Run("application OTEL settings rotate only the private identity", func(t *testing.T) {
		// Two identical Python handler apps, one with application OTel
		// settings on its template env: Config.Env (execEnv) must be
		// byte-identical between them, while the private provider identity
		// differs — a template OTEL edit rotates the warm process without
		// moving a value into Docker Config.Env.
		plain := prepare(t, app.App{
			Name: "orders",
			Dir:  writeHandlerSource(t),
			Template: &app.Template{
				Runtime: "python3.14",
				Env:     map[string]string{"LOG_LEVEL": "info"},
				Events:  []app.EventRule{{Handler: "main.process"}},
			},
		})
		withOtel := prepare(t, app.App{
			Name: "orders",
			Dir:  writeHandlerSource(t),
			Template: &app.Template{
				Runtime: "python3.14",
				Env: map[string]string{
					"LOG_LEVEL":                   "info",
					"OTEL_EXPORTER_OTLP_HEADERS":  "authorization=CANARY-TEMPLATE-AUTH-99",
					"OTEL_PYTHON_TRACER_PROVIDER": "appowned",
				},
				Events: []app.EventRule{{Handler: "main.process"}},
			},
		})

		if strings.Join(plain.execEnv, "\x00") != strings.Join(withOtel.execEnv, "\x00") {
			t.Errorf("application OTEL settings changed Docker Config.Env:\n plain = %v\nwithOtel = %v",
				plain.execEnv, withOtel.execEnv)
		}
		if plain.otelConfig == withOtel.otelConfig {
			t.Error("application OTEL settings must change the private provider identity")
		}
	})

	t.Run("node handler app gets the provider environment", func(t *testing.T) {
		const appAuthCanary = "CANARY-NODE-AUTH-71"
		got := prepare(t, app.App{
			Name: "web",
			Dir:  writeNodeHandlerSource(t),
			Template: &app.Template{
				Runtime: "nodejs24.x",
				Env: map[string]string{
					"LOG_LEVEL": "info",
					// Application-specific OTel settings stay on the template
					// env: they must reach only the private identity and the
					// per-invocation frame, never Docker Config.Env.
					"OTEL_RESOURCE_ATTRIBUTES":   "team=web",
					"OTEL_EXPORTER_OTLP_HEADERS": "authorization=" + appAuthCanary,
					"RELAY_OTEL_PROVIDER":        "application",
				},
				Events: []app.EventRule{{Handler: "index.h"}},
			},
		})

		if len(got.execEnv) == 0 {
			t.Fatal("managed Node execution resolved no execEnv")
		}
		// The container receives the SAFE inherited worker configuration plus
		// the app service default, and nothing else.
		for _, want := range []string{
			"OTEL_EXPORTER_OTLP_ENDPOINT=http://worker-collector:4318",
			"OTEL_EXPORTER_OTLP_PROTOCOL=grpc",
			"OTEL_EXPORTER_OTLP_COMPRESSION=gzip",
			"OTEL_EXPORTER_OTLP_INSECURE=true",
			"OTEL_SERVICE_NAME=web",
		} {
			if !hasEntry(got.execEnv, want) {
				t.Errorf("execEnv = %v, want the entry %q", got.execEnv, want)
			}
		}
		if got.otelConfig == "" {
			t.Error("managed Node execution resolved an empty private config identity")
		}
		if len(got.otelInheritedTraceSpecific) != 0 {
			t.Errorf("InheritedTraceSpecific = %v, want none (no worker trace-specific value set)",
				got.otelInheritedTraceSpecific)
		}

		// The service path (and every other Prepared.Env consumer) must stay
		// free of provider configuration, exactly like the Python path.
		for _, entry := range got.Env {
			if strings.HasPrefix(entry, "OTEL_") {
				t.Errorf("Prepared.Env carries provider config (services would inherit it): %q", entry)
			}
		}
		// Application-specific names and values never reach Config.Env: only
		// the plan env plus allowlisted worker entries may appear beyond it.
		planEnv := map[string]bool{}
		for _, entry := range got.Env {
			planEnv[entry] = true
		}
		allowedWorkerNames := map[string]bool{
			"OTEL_EXPORTER_OTLP_ENDPOINT":    true,
			"OTEL_EXPORTER_OTLP_PROTOCOL":    true,
			"OTEL_EXPORTER_OTLP_COMPRESSION": true,
			"OTEL_EXPORTER_OTLP_INSECURE":    true,
			"OTEL_SERVICE_NAME":              true,
		}
		for _, entry := range got.execEnv {
			if planEnv[entry] {
				continue
			}
			name, _, _ := strings.Cut(entry, "=")
			if !allowedWorkerNames[name] {
				t.Errorf("unexpected execEnv entry beyond the plan env: %q", entry)
			}
			if strings.Contains(entry, appAuthCanary) {
				t.Errorf("application auth value leaked into Docker Config.Env: %q", entry)
			}
		}
		for _, entry := range got.Env {
			if !hasEntry(got.execEnv, entry) {
				t.Errorf("execEnv = %v is missing the plan env entry %q", got.execEnv, entry)
			}
		}
	})

	t.Run("node application OTEL settings rotate only the private identity", func(t *testing.T) {
		// Two identical Node handler apps, one with application OTEL settings
		// on its template env: Config.Env (execEnv) must be byte-identical
		// between them, while the private provider identity differs — a
		// template OTEL edit rotates the warm process without moving a value
		// into Docker Config.Env.
		plain := prepare(t, app.App{
			Name: "web",
			Dir:  writeNodeHandlerSource(t),
			Template: &app.Template{
				Runtime: "nodejs24.x",
				Env:     map[string]string{"LOG_LEVEL": "info"},
				Events:  []app.EventRule{{Handler: "index.h"}},
			},
		})
		withOtel := prepare(t, app.App{
			Name: "web",
			Dir:  writeNodeHandlerSource(t),
			Template: &app.Template{
				Runtime: "nodejs24.x",
				Env: map[string]string{
					"LOG_LEVEL":                  "info",
					"OTEL_EXPORTER_OTLP_HEADERS": "authorization=CANARY-NODE-AUTH-71",
					"RELAY_OTEL_PROVIDER":        "application",
				},
				Events: []app.EventRule{{Handler: "index.h"}},
			},
		})

		if strings.Join(plain.execEnv, "\x00") != strings.Join(withOtel.execEnv, "\x00") {
			t.Errorf("application OTEL settings changed Docker Config.Env:\n plain = %v\nwithOtel = %v",
				plain.execEnv, withOtel.execEnv)
		}
		if plain.otelConfig == withOtel.otelConfig {
			t.Error("application OTEL settings must change the private provider identity")
		}
	})

	t.Run("python service-only app gets no provider environment", func(t *testing.T) {
		got := prepare(t, app.App{
			Name: "reporter",
			Dir:  writeHandlerSource(t),
			Template: &app.Template{
				Runtime: "python3.14",
				// No events and no schedules: service-only, so the image never
				// runs the invocation provider.
			},
		})

		if len(got.execEnv) != 0 {
			t.Errorf("execEnv = %v, want none for a service-only app", got.execEnv)
		}
		if got.otelConfig != "" {
			t.Errorf("private config identity = %q, want empty for a service-only app", got.otelConfig)
		}
	})

	t.Run("node service-only app gets no provider environment", func(t *testing.T) {
		got := prepare(t, app.App{
			Name: "web-service",
			Dir:  writeNodeHandlerSource(t),
			Template: &app.Template{
				Runtime: "nodejs24.x",
				// No events and no schedules: the service-only image never
				// runs the invocation bootstrap, so it must not receive the
				// managed provider environment or identity.
			},
		})

		if len(got.execEnv) != 0 {
			t.Errorf("execEnv = %v, want none for a Node service-only app", got.execEnv)
		}
		if got.otelConfig != "" {
			t.Errorf("private config identity = %q, want empty for a Node service-only app", got.otelConfig)
		}
		for _, entry := range got.Env {
			if strings.HasPrefix(entry, "OTEL_") {
				t.Errorf("Node service-only Prepared.Env carries provider config: %q", entry)
			}
		}
	})
}

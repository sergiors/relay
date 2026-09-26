package function

import (
	"os"
	"strings"
	"testing"
)

// A template without a `services` key renders a nil Services slice and parses
// exactly as before.
func TestParseNoServicesNil(t *testing.T) {
	tmpl := mustParse(t, `
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
`)
	if tmpl.Services != nil {
		t.Fatalf("expected nil Services, got %#v", tmpl.Services)
	}
	if len(tmpl.Events) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(tmpl.Events))
	}
}

// Omitted port and replicas resolve to their defaults (80 and 1).
func TestParseServiceDefaults(t *testing.T) {
	tmpl := mustParse(t, `
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - entrypoint: service.js
`)
	if len(tmpl.Services) != 1 {
		t.Fatalf("expected 1 service, got %d", len(tmpl.Services))
	}
	s := tmpl.Services[0]
	if s.Entrypoint != "service.js" {
		t.Fatalf("entrypoint = %q", s.Entrypoint)
	}
	if s.Port != DefaultServicePort {
		t.Fatalf("omitted port = %d, want default %d", s.Port, DefaultServicePort)
	}
	if s.Replicas != DefaultServiceReplicas {
		t.Fatalf("omitted replicas = %d, want default %d", s.Replicas, DefaultServiceReplicas)
	}
}

// Explicit port and replicas are honored.
func TestParseServiceExplicitPortAndReplicas(t *testing.T) {
	tmpl := mustParse(t, `
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - entrypoint: service.js
    port: 3000
    replicas: 2
`)
	if len(tmpl.Services) != 1 {
		t.Fatalf("expected 1 service, got %d", len(tmpl.Services))
	}
	s := tmpl.Services[0]
	if s.Port != 3000 {
		t.Fatalf("port = %d, want 3000", s.Port)
	}
	if s.Replicas != 2 {
		t.Fatalf("replicas = %d, want 2", s.Replicas)
	}
}

// Multiple services with distinct ports resolve independently.
func TestParseMultipleServices(t *testing.T) {
	tmpl := mustParse(t, `
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - entrypoint: api.js
    port: 3000
  - entrypoint: worker.js
    port: 4000
    replicas: 3
`)
	if len(tmpl.Services) != 2 {
		t.Fatalf("expected 2 services, got %d", len(tmpl.Services))
	}
	if tmpl.Services[0].Entrypoint != "api.js" || tmpl.Services[0].Port != 3000 ||
		tmpl.Services[0].Replicas != DefaultServiceReplicas {
		t.Fatalf("service 0 = %+v", tmpl.Services[0])
	}
	if tmpl.Services[1].Entrypoint != "worker.js" || tmpl.Services[1].Port != 4000 || tmpl.Services[1].Replicas != 3 {
		t.Fatalf("service 1 = %+v", tmpl.Services[1])
	}
}

// An invalid (non-integer or out-of-range) port is rejected with a message
// naming the field and the service.
func TestParseServicePortRejected(t *testing.T) {
	cases := []struct {
		name string
		port string
	}{
		{"zero", "0"},
		{"negative", "-1"},
		{"above max", "70000"},
		{"string", "abc"},
		{"float", "1.5"},
		{"bool", "true"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTemplate([]byte(`
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - entrypoint: service.js
    port: ` + tc.port + `
`))
			if err == nil {
				t.Fatalf("expected error for port %q", tc.port)
			}
			if !strings.Contains(err.Error(), "port") {
				t.Errorf("expected error to mention port, got: %v", err)
			}
			if !strings.Contains(err.Error(), "service.js") {
				t.Errorf("expected error to name the service, got: %v", err)
			}
		})
	}
}

// An invalid (non-integer or non-positive) replica count is rejected with a
// message naming the field and the service.
func TestParseServiceReplicasRejected(t *testing.T) {
	cases := []struct {
		name     string
		replicas string
	}{
		{"zero", "0"},
		{"negative", "-1"},
		{"string", "abc"},
		{"float", "2.5"},
		{"bool", "true"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTemplate([]byte(`
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - entrypoint: service.js
    replicas: ` + tc.replicas + `
`))
			if err == nil {
				t.Fatalf("expected error for replicas %q", tc.replicas)
			}
			if !strings.Contains(err.Error(), "replicas") {
				t.Errorf("expected error to mention replicas, got: %v", err)
			}
			if !strings.Contains(err.Error(), "service.js") {
				t.Errorf("expected error to name the service, got: %v", err)
			}
		})
	}
}

// A service with no source at all is rejected; an entrypoint with whitespace is
// rejected.
func TestParseServiceMissingSource(t *testing.T) {
	_, err := ParseTemplate([]byte(`
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - port: 3000
`))
	if err == nil || !strings.Contains(err.Error(), "service is missing a source") {
		t.Fatalf("err = %v, want missing-source error", err)
	}
}

// A service declaring more than one source is rejected: exactly one of
// entrypoint/image is allowed.
func TestParseServiceMultipleSourcesRejected(t *testing.T) {
	_, err := ParseTemplate([]byte(`
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - entrypoint: service.js
    image: nginx:1.27
`))
	if err == nil || !strings.Contains(err.Error(), "multiple sources") {
		t.Fatalf("err = %v, want multiple-sources rejection", err)
	}
}

// A `build:` key is no longer a recognized source. The YAML decoder ignores the
// unknown field, so a build-only service has no entrypoint and no image and is
// rejected by the existing missing-source validation rather than silently
// running an unexpected image.
func TestParseServiceUnknownBuildRejected(t *testing.T) {
	_, err := ParseTemplate([]byte(`
services:
  - build: Dockerfile
    port: 3000
`))
	if err == nil || !strings.Contains(err.Error(), "service is missing a source") {
		t.Fatalf("err = %v, want missing-source rejection for an unknown build source", err)
	}
}

func TestParseServiceEntrypointWhitespace(t *testing.T) {
	_, err := ParseTemplate([]byte(`
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - entrypoint: "my service.js"
`))
	if err == nil || !strings.Contains(err.Error(), "contains whitespace") {
		t.Fatalf("err = %v, want whitespace rejection", err)
	}
}

// Duplicate service identities within the services list are rejected: the
// configured source descriptor is the service identity, and duplicates would be
// ambiguous for reconciliation — regardless of source kind.
func TestParseServiceDuplicateIdentity(t *testing.T) {
	_, err := ParseTemplate([]byte(`
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - entrypoint: service.js
  - entrypoint: service.js
`))
	if err == nil || !strings.Contains(err.Error(), `duplicate service "service.js"`) {
		t.Fatalf("err = %v, want duplicate-service rejection", err)
	}
	// The same applies to image references with identical descriptor text.
	_, err = ParseTemplate([]byte(`
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - image: nginx:1.27
  - image: nginx:1.27
`))
	if err == nil || !strings.Contains(err.Error(), `duplicate service "nginx:1.27"`) {
		t.Fatalf("err = %v, want duplicate image-service rejection", err)
	}
}

// A template with both events AND services parses both correctly.
func TestParseEventsAndServices(t *testing.T) {
	tmpl := mustParse(t, `
runtime: node24
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
  - handler: events.updated.handler
    pattern:
      event_name: [MODIFY]
services:
  - entrypoint: service.js
    port: 3000
    replicas: 2
`)
	if len(tmpl.Events) != 2 {
		t.Fatalf("rules = %d, want 2", len(tmpl.Events))
	}
	if len(tmpl.Services) != 1 {
		t.Fatalf("services = %d, want 1", len(tmpl.Services))
	}
	if tmpl.Services[0].Entrypoint != "service.js" || tmpl.Services[0].Port != 3000 || tmpl.Services[0].Replicas != 2 {
		t.Fatalf("service = %+v", tmpl.Services[0])
	}
}

// A template with services AND schedules co-exists: all of rules, schedules,
// and services parse.
func TestParseServicesAndSchedules(t *testing.T) {
	tmpl := mustParse(t, `
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
schedules:
  - handler: jobs.cleanup.handler
    cron: 0 3 * * *
services:
  - entrypoint: service.js
    port: 8080
`)
	if len(tmpl.Events) != 1 {
		t.Fatalf("rules = %d, want 1", len(tmpl.Events))
	}
	if len(tmpl.Schedules) != 1 || tmpl.Schedules[0].Handler != "jobs.cleanup.handler" {
		t.Fatalf("schedules = %+v", tmpl.Schedules)
	}
	if len(tmpl.Services) != 1 || tmpl.Services[0].Entrypoint != "service.js" || tmpl.Services[0].Port != 8080 {
		t.Fatalf("services = %+v", tmpl.Services)
	}
}

// A port boundary: the minimum (1) and maximum (65535) are accepted.
func TestParseServicePortBoundaries(t *testing.T) {
	tmpl := mustParse(t, `
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - entrypoint: min.js
    port: 1
  - entrypoint: max.js
    port: 65535
`)
	if tmpl.Services[0].Port != 1 || tmpl.Services[1].Port != 65535 {
		t.Fatalf("boundary ports = %d, %d; want 1 and 65535", tmpl.Services[0].Port, tmpl.Services[1].Port)
	}
}

// A services-only template (no events key) parses: a persistent service does
// not consume events, so `services:` alone is a complete, valid template.
func TestParseServicesOnlyTemplate(t *testing.T) {
	tmpl, err := ParseTemplate([]byte(`
runtime: node24
services:
  - entrypoint: service.js
    port: 3000
    replicas: 2
`))
	if err != nil {
		t.Fatalf("services-only template must parse: %v", err)
	}
	if len(tmpl.Events) != 0 {
		t.Fatalf("rules = %d, want 0", len(tmpl.Events))
	}
	if len(tmpl.Services) != 1 || tmpl.Services[0].Entrypoint != "service.js" ||
		tmpl.Services[0].Port != 3000 || tmpl.Services[0].Replicas != 2 {
		t.Fatalf("services = %+v", tmpl.Services)
	}
}

// A template with neither events nor services is inert and rejected.
func TestParseInertTemplateRejected(t *testing.T) {
	if _, err := ParseTemplate([]byte("runtime: node24\n")); err == nil {
		t.Fatal("expected error for a template with no events and no services")
	}
	if _, err := ParseTemplate([]byte("runtime: node24\nevents: []\nservices: []\n")); err == nil {
		t.Fatal("expected error for explicitly empty events and services")
	}
}

// The shipped users-api-node example parses with its declared values — the
// example is services-only, so this also pins the services-only contract.
func TestExampleUsersAPITemplateParses(t *testing.T) {
	data, err := os.ReadFile("../../examples/functions/users-api-node/template.yaml")
	if err != nil {
		t.Skipf("example not present: %v", err)
	}
	tmpl, err := ParseTemplate(data)
	if err != nil {
		t.Fatalf("parse example template: %v", err)
	}
	if len(tmpl.Services) != 1 {
		t.Fatalf("services = %d, want 1", len(tmpl.Services))
	}
	s := tmpl.Services[0]
	if s.Entrypoint != "service.js" || s.Port != 3000 || s.Replicas != 1 {
		t.Fatalf("service = %+v, want {service.js 3000 1}", s)
	}
}

// The fastapi-service example is a python3.14 service whose entrypoint is a
// nested package module (app/main.py), demonstrating module execution.
func TestExampleFastAPITemplateParses(t *testing.T) {
	data, err := os.ReadFile("../../examples/functions/fastapi-service/template.yaml")
	if err != nil {
		t.Skipf("example not present: %v", err)
	}
	tmpl, err := ParseTemplate(data)
	if err != nil {
		t.Fatalf("parse example template: %v", err)
	}
	if tmpl.Runtime != "python3.14" {
		t.Fatalf("runtime = %q, want python3.14", tmpl.Runtime)
	}
	if len(tmpl.Services) != 1 {
		t.Fatalf("services = %d, want 1", len(tmpl.Services))
	}
	s := tmpl.Services[0]
	if s.Entrypoint != "app/main.py" || s.Host != "api.example.com" || s.Port != 8000 || s.Replicas != 1 {
		t.Fatalf("service = %+v, want {app/main.py api.example.com 8000 1}", s)
	}
}

// Nested entrypoints (a relative path inside the application directory) parse
// and round-trip into the Service entrypoint.
func TestParseServiceNestedEntrypoint(t *testing.T) {
	for _, ep := range []string{"app/service.js", "app/main.py", "a/b/c.js"} {
		t.Run(ep, func(t *testing.T) {
			tmpl := mustParse(t, `
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - entrypoint: `+ep+`
    port: 3000
`)
			if len(tmpl.Services) != 1 || tmpl.Services[0].Entrypoint != ep {
				t.Fatalf("services = %+v, want entrypoint %q", tmpl.Services, ep)
			}
			if tmpl.Services[0].Port != 3000 {
				t.Fatalf("port = %d, want 3000", tmpl.Services[0].Port)
			}
		})
	}
}

// A declared `host` parses into Service.Host; an empty host parses to "" (an
// internal unrouted service); an omitted host is nil -> "" too.
func TestParseServiceHost(t *testing.T) {
	tmpl := mustParse(t, `
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - entrypoint: routed.js
    host: api.example.com
  - entrypoint: blank.js
    host: ""
  - entrypoint: plain.js
`)
	if len(tmpl.Services) != 3 {
		t.Fatalf("services = %d, want 3", len(tmpl.Services))
	}
	if tmpl.Services[0].Host != "api.example.com" {
		t.Fatalf("routed host = %q, want api.example.com", tmpl.Services[0].Host)
	}
	if tmpl.Services[1].Host != "" {
		t.Fatalf("empty host = %q, want \"\" (unrouted)", tmpl.Services[1].Host)
	}
	if tmpl.Services[2].Host != "" {
		t.Fatalf("omitted host = %q, want \"\" (unrouted)", tmpl.Services[2].Host)
	}
}

// Invalid hosts are rejected, with the error naming the service entrypoint.
func TestParseServiceHostRejected(t *testing.T) {
	long := "a" + "." + strings.Repeat("b", 248) + ".com" // 254 chars total
	cases := []struct {
		name string
		host string
	}{
		{"space", "has space"},
		{"underscore", "under_score.com"},
		{"leading hyphen", "-leading.com"},
		{"trailing hyphen label", "trailing-.com"},
		{"empty label", "a..b.com"},
		{"too long", long},
		{"port suffix", "api.example.com:8080"},
		{"only dots", ".com"},
		{"hyphen in middle of nothing", "-"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTemplate([]byte(`
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - entrypoint: service.js
    host: "` + tc.host + `"
`))
			if err == nil {
				t.Fatalf("expected error for host %q", tc.host)
			}
			if !strings.Contains(err.Error(), "service.js") {
				t.Errorf("expected error to name the service, got: %v", err)
			}
			if !strings.Contains(err.Error(), "host") {
				t.Errorf("expected error to mention host, got: %v", err)
			}
		})
	}
}

// Hostname boundary: a 253-char host is accepted and a single-label host ("api")
// is accepted.
func TestParseServiceHostBoundaries(t *testing.T) {
	// 253 chars as four valid labels (each <= 63 chars, no hyphen bounds):
	// 63 + 1 + 63 + 1 + 62 + 1 + 62 = 253.
	host253 := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." +
		strings.Repeat("c", 62) + "." + strings.Repeat("d", 62)
	if len(host253) != 253 {
		t.Fatalf("test host length = %d, want 253", len(host253))
	}
	tmpl := mustParse(t, `
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - entrypoint: long.js
    host: "`+host253+`"
  - entrypoint: short.js
    host: api
  - entrypoint: numeric.js
    host: 123.io
`)
	if tmpl.Services[0].Host != host253 {
		t.Fatalf("253-char host = %q, want the full host accepted", tmpl.Services[0].Host)
	}
	if tmpl.Services[1].Host != "api" || tmpl.Services[2].Host != "123.io" {
		t.Fatalf("boundary hosts = %v", tmpl.Services)
	}
}

// A declared `path` parses into Service.Path; an omitted path and an explicit
// empty path both parse to "" (host-only routing, exactly as before paths).
func TestParseServicePath(t *testing.T) {
	tmpl := mustParse(t, `
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - entrypoint: routed.js
    host: api.example.com
    path: /v2
  - entrypoint: blank.js
    host: api.example.com
    path: ""
  - entrypoint: plain.js
    host: api.example.com
`)
	if len(tmpl.Services) != 3 {
		t.Fatalf("services = %d, want 3", len(tmpl.Services))
	}
	if tmpl.Services[0].Path != "/v2" {
		t.Fatalf("path = %q, want /v2", tmpl.Services[0].Path)
	}
	if tmpl.Services[1].Path != "" {
		t.Fatalf("empty path = %q, want \"\" (host-only)", tmpl.Services[1].Path)
	}
	if tmpl.Services[2].Path != "" {
		t.Fatalf("omitted path = %q, want \"\" (host-only)", tmpl.Services[2].Path)
	}
}

// Canonicalization: a non-root path drops trailing slashes; the root path stays
// exactly "/".
func TestParseServicePathCanonicalization(t *testing.T) {
	cases := []struct{ in, want string }{
		{"/v2", "/v2"},
		{"/v2/", "/v2"},
		{"/v2///", "/v2"},
		{"/", "/"},
		{"/a/b", "/a/b"},
		{"/a/b/", "/a/b"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			tmpl := mustParse(t, `
runtime: node24
services:
  - entrypoint: service.js
    host: api.example.com
    path: "`+tc.in+`"
`)
			if got := tmpl.Services[0].Path; got != tc.want {
				t.Fatalf("path %q canonicalized to %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// Invalid paths are rejected, with the error naming the service and the path.
func TestParseServicePathRejected(t *testing.T) {
	cases := []struct {
		name string
		path string
	}{
		{"relative", "v2"},
		{"whitespace", "/has space"},
		{"query", "/v2?x=1"},
		{"fragment", "/v2#frag"},
		{"backslash", `\v2`},
		{"double slash", "/v2//x"},
		{"double slash leading", "//v2"},
		{"triple slash", "///"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTemplate([]byte(`
runtime: node24
services:
  - entrypoint: service.js
    host: api.example.com
    path: "` + tc.path + `"
`))
			if err == nil {
				t.Fatalf("expected error for path %q", tc.path)
			}
			if !strings.Contains(err.Error(), "service.js") {
				t.Errorf("expected error to name the service, got: %v", err)
			}
			if !strings.Contains(err.Error(), "path") {
				t.Errorf("expected error to mention path, got: %v", err)
			}
		})
	}
}

// A path without a host is rejected: PathPrefix alone is not an externally
// addressable route and the no-host service must remain unrouted.
func TestParseServicePathWithoutHostRejected(t *testing.T) {
	_, err := ParseTemplate([]byte(`
runtime: node24
services:
  - entrypoint: service.js
    path: /v2
`))
	if err == nil {
		t.Fatal("expected error for a path without a host")
	}
	if !strings.Contains(err.Error(), "path requires host") {
		t.Fatalf("err = %v, want the path-requires-host error", err)
	}
}

// A service with a path but no host, and a service with neither, coexist: only
// the path-without-host entry fails validation.
func TestParseServicePathWithoutHostAfterHostedService(t *testing.T) {
	_, err := ParseTemplate([]byte(`
runtime: node24
services:
  - entrypoint: routed.js
    host: api.example.com
    path: /v1
  - entrypoint: bad.js
    path: /v2
`))
	if err == nil || !strings.Contains(err.Error(), "service \"bad.js\": path requires host") {
		t.Fatalf("err = %v, want service \"bad.js\": path requires host", err)
	}
}

// An `image` source parses: the reference is the service identity and it needs
// no runtime (the image carries its own ENTRYPOINT/CMD).
func TestParseServiceImageSourceNoRuntime(t *testing.T) {
	tmpl, err := ParseTemplate([]byte(`
services:
  - image: ghcr.io/acme/api:1.2
    port: 8080
    replicas: 3
`))
	if err != nil {
		t.Fatalf("image-only template must parse without a runtime: %v", err)
	}
	if len(tmpl.Services) != 1 {
		t.Fatalf("services = %d, want 1", len(tmpl.Services))
	}
	s := tmpl.Services[0]
	if s.Source() != ServiceSourceImage || s.SourceRef() != "ghcr.io/acme/api:1.2" {
		t.Fatalf("service = %+v, want an image source with identity ghcr.io/acme/api:1.2", s)
	}
	if s.Image != "ghcr.io/acme/api:1.2" || s.Entrypoint != "" {
		t.Fatalf("service source fields = %+v, want only Image set", s)
	}
}

// An `entrypoint` source parses with its file as identity and requires a runtime
// (Relay launches it with a runtime-specific command).
func TestParseServiceEntrypointSourceWithRuntime(t *testing.T) {
	tmpl, err := ParseTemplate([]byte(`
runtime: node24
services:
  - entrypoint: service.js
    port: 3000
`))
	if err != nil {
		t.Fatalf("entrypoint template must parse: %v", err)
	}
	s := tmpl.Services[0]
	if s.Source() != ServiceSourceEntrypoint || s.SourceRef() != "service.js" {
		t.Fatalf("service = %+v, want an entrypoint source with identity service.js", s)
	}
	if s.Entrypoint != "service.js" || s.Image != "" {
		t.Fatalf("service source fields = %+v, want only Entrypoint set", s)
	}
	if s.Port != 3000 || s.Replicas != DefaultServiceReplicas {
		t.Fatalf("service defaults = %+v, want port=3000 replicas=%d", s, DefaultServiceReplicas)
	}
}

// An `entrypoint` service still requires a runtime: Relay launches it with a
// runtime-specific command.
func TestParseServiceEntrypointRequiresRuntime(t *testing.T) {
	_, err := ParseTemplate([]byte(`
services:
  - entrypoint: service.js
`))
	if err == nil || !strings.Contains(err.Error(), "runtime is required") {
		t.Fatalf("err = %v, want runtime-required", err)
	}
}

// A mixed template (events alongside an image service) still requires a runtime
// for the event handlers.
func TestParseMixedTemplateRequiresRuntime(t *testing.T) {
	_, err := ParseTemplate([]byte(`
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - image: nginx:1.27
`))
	if err == nil || !strings.Contains(err.Error(), "runtime is required") {
		t.Fatalf("err = %v, want runtime-required for a mixed template", err)
	}
}

// A template with no runtime and only image services parses; the runtime is
// validated when explicitly present.
func TestParseServiceImageUnsupportedRuntimeRejected(t *testing.T) {
	_, err := ParseTemplate([]byte(`
runtime: rust
services:
  - image: nginx:1.27
`))
	if err == nil || !strings.Contains(err.Error(), `unsupported runtime "rust"`) {
		t.Fatalf("err = %v, want unsupported-runtime rejection", err)
	}
}

// Invalid image references are rejected.
func TestParseServiceImageValidation(t *testing.T) {
	cases := []struct {
		name    string
		svc     string
		wantErr string
	}{
		{"whitespace image", "  - image: \"bad image\"\n", "contains whitespace"},
		{"malformed image", "  - image: \"-leading\"\n", "not a valid container image reference"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTemplate([]byte("services:\n" + tc.svc))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

// Two services with distinct source descriptors (an entrypoint file and an image
// reference) are both retained and have distinct identities.
func TestParseServiceIdentityAcrossKinds(t *testing.T) {
	tmpl := mustParse(t, `
runtime: node24
services:
  - entrypoint: service.js
    port: 3000
  - image: nginx:1.27
    port: 8080
`)
	if len(tmpl.Services) != 2 {
		t.Fatalf("services = %d, want 2", len(tmpl.Services))
	}
	if tmpl.Services[0].SourceRef() == tmpl.Services[1].SourceRef() {
		t.Fatalf("distinct services share a source identity: %q", tmpl.Services[0].SourceRef())
	}
}

// The shipped external-image-service example is an image-source service with no
// runtime, so this pins the runtime-optional image-source contract end to end
// from an on-disk example.
func TestExampleExternalImageTemplateParses(t *testing.T) {
	data, err := os.ReadFile("../../examples/functions/external-image-service/template.yaml")
	if err != nil {
		t.Skipf("example not present: %v", err)
	}
	tmpl, err := ParseTemplate(data)
	if err != nil {
		t.Fatalf("parse example template: %v", err)
	}
	if tmpl.Runtime != "" {
		t.Fatalf("runtime = %q, want empty for an image-only example", tmpl.Runtime)
	}
	if len(tmpl.Services) != 1 {
		t.Fatalf("services = %d, want 1", len(tmpl.Services))
	}
	s := tmpl.Services[0]
	if s.Source() != ServiceSourceImage || s.Image != "nginx:1.27-alpine" || s.Port != 80 || s.Replicas != 1 {
		t.Fatalf("service = %+v, want image nginx:1.27-alpine port=80 replicas=1", s)
	}
}

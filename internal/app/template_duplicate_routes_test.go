package app

import (
	"strings"
	"testing"
)

// parseServices parses a template built from the given services block body.
func parseServices(t *testing.T, services string) (*Template, error) {
	t.Helper()
	return ParseTemplate([]byte("runtime: nodejs24.x\nservices:\n" + services))
}

// Given two services in one template declaring the same externally
// addressable route, when the template parses, then parsing fails with an
// error naming both service declarations and the offending host/path — because
// discovery would have to omit both at runtime anyway.
func TestParseRejectsDuplicateServiceRoute(t *testing.T) {
	for _, tc := range []struct {
		name    string
		first   string
		second  string
		wantErr string
	}{
		{
			name: "same host and path",
			first: "  - name: web\n" +
				"    entrypoint: web.js\n" +
				"    host: api.example.com\n" +
				"    path: /v2\n",
			second: "  - name: admin\n" +
				"    entrypoint: admin.js\n" +
				"    host: api.example.com\n" +
				"    path: /v2\n",
			wantErr: `duplicate route: services "web" and "admin" both declare ` +
				`host "api.example.com" path "/v2"`,
		},
		{
			name: "host comparison is case-insensitive",
			first: "  - name: one\n" +
				"    entrypoint: one.js\n" +
				"    host: API.example.com\n",
			second: "  - name: two\n" +
				"    entrypoint: two.js\n" +
				"    host: api.example.com\n",
			wantErr: `duplicate route: services "one" and "two" both declare ` +
				`host "api.example.com" path "/"`,
		},
		{
			name: "empty path equals the root path",
			first: "  - name: hostonly\n" +
				"    entrypoint: hostonly.js\n" +
				"    host: api.example.com\n" +
				"    path: \"\"\n",
			second: "  - name: rootpath\n" +
				"    entrypoint: rootpath.js\n" +
				"    host: api.example.com\n" +
				"    path: /\n",
			wantErr: `duplicate route: services "hostonly" and "rootpath" both ` +
				`declare host "api.example.com" path "/"`,
		},
		{
			name: "trailing slash canonicalizes to the same route",
			first: "  - name: first\n" +
				"    entrypoint: first.js\n" +
				"    host: api.example.com\n" +
				"    path: /v2\n",
			second: "  - name: second\n" +
				"    entrypoint: second.js\n" +
				"    host: api.example.com\n" +
				"    path: /v2/\n",
			wantErr: `duplicate route: services "first" and "second" both ` +
				`declare host "api.example.com" path "/v2"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmpl, err := parseServices(t, tc.first+tc.second)
			if err == nil {
				t.Fatalf("parse succeeded with %+v; want a duplicate-route error",
					tmpl)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %q, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

// Given several services on one host with distinct paths (including the
// host-only route), when the template parses, then every service survives with
// its declared name and path intact.
func TestParseAllowsDistinctRoutesOnSameHost(t *testing.T) {
	tmpl := mustParse(t, `
runtime: nodejs24.x
services:
  - name: web
    entrypoint: web.js
    host: api.example.com
    path: /v2
  - name: admin
    entrypoint: admin.js
    host: api.example.com
    path: /admin
  - name: root
    entrypoint: root.js
    host: api.example.com
`)
	if len(tmpl.Services) != 3 {
		t.Fatalf("services = %d, want 3", len(tmpl.Services))
	}
	wantNames := []string{"web", "admin", "root"}
	wantPaths := []string{"/v2", "/admin", ""}
	for i := range tmpl.Services {
		if tmpl.Services[i].Name != wantNames[i] {
			t.Errorf("service %d name = %q, want %q (names preserved)",
				i, tmpl.Services[i].Name, wantNames[i])
		}
		if tmpl.Services[i].Path != wantPaths[i] {
			t.Errorf("service %d path = %q, want %q",
				i, tmpl.Services[i].Path, wantPaths[i])
		}
		if tmpl.Services[i].Host != "api.example.com" {
			t.Errorf("service %d host = %q, want api.example.com",
				i, tmpl.Services[i].Host)
		}
	}
}

// The same path on different hosts addresses different routes and must parse.
func TestParseAllowsSamePathOnDifferentHosts(t *testing.T) {
	tmpl := mustParse(t, `
runtime: nodejs24.x
services:
  - name: alpha-web
    entrypoint: alpha.js
    host: alpha.example.com
    path: /v2
  - name: beta-web
    entrypoint: beta.js
    host: beta.example.com
    path: /v2
`)
	if len(tmpl.Services) != 2 {
		t.Fatalf("services = %d, want 2", len(tmpl.Services))
	}
	if tmpl.Services[0].Name != "alpha-web" || tmpl.Services[1].Name != "beta-web" {
		t.Fatalf("service names = %q/%q, want alpha-web/beta-web",
			tmpl.Services[0].Name, tmpl.Services[1].Name)
	}
}

// Services without a host are unrouted: they never claim an externally
// addressable route, so any number of them parses without conflict.
func TestParseAllowsUnroutedServicesWithoutRouteConflicts(t *testing.T) {
	tmpl := mustParse(t, `
runtime: nodejs24.x
services:
  - name: worker
    entrypoint: worker.js
  - name: cron
    entrypoint: cron.js
`)
	if len(tmpl.Services) != 2 {
		t.Fatalf("services = %d, want 2", len(tmpl.Services))
	}
	for _, svc := range tmpl.Services {
		if svc.Host != "" || svc.Path != "" {
			t.Fatalf("unrouted service %+v carries a route", svc)
		}
	}
}

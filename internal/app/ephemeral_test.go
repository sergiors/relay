package app

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestParseServiceEphemeralCanonicalAndDefault(t *testing.T) {
	tmpl := mustParse(t, `
runtime: nodejs24.x
services:
  - name: cache
    entrypoint: service.js
    ephemeral:
      - /var/cache/z
      - /run/cache
`)
	if got, want := tmpl.Services[0].Ephemeral, []string{"/run/cache", "/var/cache/z"}; !equalStrings(got, want) {
		t.Fatalf("ephemeral = %v, want canonical sorted %v", got, want)
	}

	withoutEphemeral := mustParse(t, `
runtime: nodejs24.x
services:
  - name: default
    entrypoint: service.js
`)
	if got := withoutEphemeral.Services[0].Ephemeral; len(got) != 0 {
		t.Fatalf("omitted ephemeral = %v, want empty", got)
	}
}

func TestParseServiceEphemeralRejectsInvalidPaths(t *testing.T) {
	cases := []struct {
		name, destination string
	}{
		{"empty", `""`},
		{"relative", "run/cache"},
		{"trailing slash", "/run/cache/"},
		{"dot segment", "/run/./cache"},
		{"parent segment", "/run/../cache"},
		{"empty segment", "/run//cache"},
		{"whitespace", `"/run/my cache"`},
		{"backslash", `"/run\\cache"`},
		{"comma", `"/run/cache,mode=777"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTemplate([]byte("runtime: nodejs24.x\nservices:\n  - name: svc\n    entrypoint: service.js\n    ephemeral: [" + tc.destination + "]\n"))
			if err == nil || !strings.Contains(err.Error(), "ephemeral") {
				t.Fatalf("parse error = %v, want ephemeral path rejection", err)
			}
		})
	}
}

func TestParseServiceEphemeralRejectsDuplicateOverlapAndProtectedPaths(t *testing.T) {
	cases := []struct {
		name string
		list string
	}{
		{"duplicate", "[/run/cache, /run/cache]"},
		{"parent-child overlap", "[/run, /run/cache]"},
		{"nested overlap", "[/var/cache, /var/cache/app]"},
		{"tmp protected", "[/tmp]"},
		{"tmp descendant protected", "[/tmp/cache]"},
		{"root protected", "[/]"},
		{"etc protected", "[/etc]"},
		{"var-run protected", "[/var/run/cache]"},
		{"proc protected", "[/proc/cache]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTemplate([]byte("runtime: nodejs24.x\nservices:\n  - name: svc\n    entrypoint: service.js\n    ephemeral: " + tc.list + "\n"))
			if err == nil || !strings.Contains(err.Error(), "ephemeral") {
				t.Fatalf("parse error = %v, want ephemeral rejection", err)
			}
		})
	}
}

func TestParseServiceRejectsRemovedTmpfsKey(t *testing.T) {
	_, err := ParseTemplate([]byte(`
runtime: nodejs24.x
services:
  - name: svc
    entrypoint: service.js
    tmpfs: [/run/cache]
`))
	if err == nil || !strings.Contains(err.Error(), "tmpfs is no longer supported") {
		t.Fatalf("parse error = %v, want a tmpfs migration rejection", err)
	}
}

// TestFingerprintIgnoresServiceEphemeral pins that ephemeral is treated like
// resources: it is applied to a container at create time, never baked into a
// built image, so an ephemeral-only edit must not change either fingerprint path.
// The service reconciler still detects the change through relay.ephemeral.
func TestFingerprintIgnoresServiceEphemeral(t *testing.T) {
	const base = `runtime: nodejs24.x
services:
  - name: web
    entrypoint: service.js
    ephemeral: [/run/cache]
`
	const changed = `runtime: nodejs24.x
services:
  - name: web
    entrypoint: service.js
    ephemeral: [/var/cache/nginx]
`
	const without = `runtime: nodejs24.x
services:
  - name: web
    entrypoint: service.js
`

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "template.yaml"), base)
	writeFile(t, filepath.Join(dir, "service.js"), "export function run(){}\n")
	baseline := fp(t, dir)
	for _, next := range []string{changed, without} {
		writeFile(t, filepath.Join(dir, "template.yaml"), next)
		if got := fp(t, dir); got != baseline {
			t.Fatalf("ephemeral-only edit changed the full-source fingerprint: "+
				"%s != %s", got, baseline)
		}
	}

	writeFile(t, filepath.Join(dir, "template.yaml"),
		strings.Replace(base, "name: web", "name: web2", 1))
	if got := fp(t, dir); got == baseline {
		t.Fatal("a non-ephemeral template edit must change the fingerprint")
	}

	// The template-only (no-runtime) path must ignore ephemeral too.
	const noRuntimeBase = `services:
  - name: web
    image: nginx:1.27
    ephemeral: [/run]
`
	const noRuntimeChanged = `services:
  - name: web
    image: nginx:1.27
    ephemeral: [/var/cache/nginx]
`
	noRuntimeDir := t.TempDir()
	writeFile(t, filepath.Join(noRuntimeDir, "template.yaml"), noRuntimeBase)
	tmpl, err := ParseTemplate([]byte(noRuntimeBase))
	if err != nil {
		t.Fatalf("parse no-runtime: %v", err)
	}
	before, err := FingerprintApp(noRuntimeDir, tmpl)
	if err != nil {
		t.Fatalf("fingerprint no-runtime: %v", err)
	}
	writeFile(t, filepath.Join(noRuntimeDir, "template.yaml"),
		noRuntimeChanged)
	tmpl2, err := ParseTemplate([]byte(noRuntimeChanged))
	if err != nil {
		t.Fatalf("parse changed no-runtime: %v", err)
	}
	after, err := FingerprintApp(noRuntimeDir, tmpl2)
	if err != nil {
		t.Fatalf("fingerprint changed no-runtime: %v", err)
	}
	if after != before {
		t.Fatalf("ephemeral-only edit changed the template-only fingerprint: "+
			"%s != %s", after, before)
	}
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

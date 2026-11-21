package app

import (
	"path/filepath"
	"testing"
)

// TestFingerprintIgnoresServiceHealthcheck pins that a healthcheck is treated
// like resources and tmpfs: it is applied to containers at create time, never
// baked into a built image, so a healthcheck-only edit must not change the
// image fingerprint. The service reconciler still detects the change through
// the container's relay.healthcheck identity, and the deployment drift
// backstop detects it through the raw template identity.
func TestFingerprintIgnoresServiceHealthcheck(t *testing.T) {
	const base = `runtime: nodejs24.x
services:
  - name: web
    entrypoint: service.js
    healthcheck:
      path: /health
`
	const changedProbe = `runtime: nodejs24.x
services:
  - name: web
    entrypoint: service.js
    healthcheck:
      path: /ready
      interval: 30s
      retries: 5
`
	const removed = `runtime: nodejs24.x
services:
  - name: web
    entrypoint: service.js
`

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "template.yaml"), base)
	writeFile(t, filepath.Join(dir, "service.js"), "export function run(){}\n")
	baseline := fp(t, dir)
	for _, next := range []string{changedProbe, removed} {
		writeFile(t, filepath.Join(dir, "template.yaml"), next)
		if got := fp(t, dir); got != baseline {
			t.Fatalf("healthcheck-only edit changed the fingerprint: %s != %s",
				got, baseline)
		}
	}

	// A non-healthcheck template edit still changes it: the strip is narrow.
	writeFile(t, filepath.Join(dir, "template.yaml"),
		"runtime: nodejs24.x\nservices:\n  - name: web2\n    entrypoint: service.js\n")
	if got := fp(t, dir); got == baseline {
		t.Fatal("a non-healthcheck template edit must change the fingerprint")
	}
}

// The strip is confined to the root services list: the same key elsewhere in
// the document is not runtime-only service configuration and still
// participates in the fingerprint.
func TestFingerprintKeepsHealthcheckOutsideRootServices(t *testing.T) {
	const withProbe = `runtime: nodejs24.x
healthcheck:
  path: /health
services:
  - name: web
    entrypoint: service.js
`
	const withoutProbe = `runtime: nodejs24.x
healthcheck:
  path: /ready
services:
  - name: web
    entrypoint: service.js
`
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "template.yaml"), withProbe)
	writeFile(t, filepath.Join(dir, "service.js"), "export function run(){}\n")
	baseline := fp(t, dir)

	writeFile(t, filepath.Join(dir, "template.yaml"), withoutProbe)
	if got := fp(t, dir); got == baseline {
		t.Fatal("a healthcheck edit outside the root services list must change the fingerprint")
	}
}

//go:build integration

package deployment

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"relay/internal/app"
	"relay/internal/state"
)

// healthcheckCoordinatorTemplate declares a service probe — the runtime-only
// configuration the content fingerprint deliberately excludes so probe edits
// never rebuild an image.
const healthcheckCoordinatorTemplate = `runtime: nodejs24.x
events:
  - handler: index.hi
    pattern:
      event_name: [INSERT]
services:
  - name: web
    entrypoint: service.js
    port: 3000
    healthcheck:
      path: /health
`

// editedHealthcheckCoordinatorTemplate differs from
// healthcheckCoordinatorTemplate ONLY in its probe block.
const editedHealthcheckCoordinatorTemplate = `runtime: nodejs24.x
events:
  - handler: index.hi
    pattern:
      event_name: [INSERT]
services:
  - name: web
    entrypoint: service.js
    port: 3000
    healthcheck:
      path: /ready
      interval: 30s
`

// writeHealthcheckApp materializes the healthcheck app into appsDir and returns
// its content fingerprint plus raw template identity.
func writeHealthcheckApp(t *testing.T, appsDir, raw string) (fingerprint, templateID string) {
	t.Helper()
	dir := filepath.Join(appsDir, "fn")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir app: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(raw), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.js"),
		[]byte("export function hi(e, context){}\n"), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "service.js"),
		[]byte("setInterval(() => {}, 1<<30);\n"), 0o644); err != nil {
		t.Fatalf("write service: %v", err)
	}
	tmpl, err := app.ParseTemplate([]byte(raw))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	fp, err := app.FingerprintApp(dir, tmpl)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	src, err := app.ReadTemplateSource(appsDir, "fn")
	if err != nil {
		t.Fatalf("template source: %v", err)
	}
	return fp, src.Identity
}

// The drift backstop must catch a healthcheck-only local template edit even
// though the content fingerprint deliberately does not change: the raw template
// identity covers ALL template bytes (including runtime-only probe
// configuration) and is compared before the cached-verification shortcut, so
// the edit requests re-materialization instead of staying invisible until the
// next reconciler pass.
func TestCheckDriftDetectsHealthcheckOnlyTemplateEdit(t *testing.T) {
	env := newCoordinatorEnv(t)
	fingerprint, templateID := writeHealthcheckApp(t, env.cfg.AppsDir,
		healthcheckCoordinatorTemplate)
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne,
		seededApp{name: "fn", fingerprint: fingerprint, templateID: templateID,
			phase: state.DeploymentAppPending})
	c := New(env.cfg)
	if _, err := c.ObserveStartup(context.Background()); err != nil {
		t.Fatalf("ObserveStartup: %v", err)
	}
	ctx := context.Background()

	// Control: the tree still matches the authorization byte-for-byte, so the
	// drift backstop requests nothing.
	c.checkDrift(ctx)
	select {
	case <-c.kick:
		t.Fatal("checkDrift requested rematerialization for a matching tree")
	default:
	}

	// Edit ONLY the healthcheck block: the content fingerprint must be
	// unchanged (no rebuild is warranted), while the raw template identity
	// changes.
	if err := os.WriteFile(filepath.Join(env.cfg.AppsDir, "fn", "template.yaml"),
		[]byte(editedHealthcheckCoordinatorTemplate), 0o644); err != nil {
		t.Fatalf("edit template: %v", err)
	}
	editedFP, editedID := writeHealthcheckAppIdentitiesOnly(t, env.cfg.AppsDir)
	if editedFP != fingerprint {
		t.Fatalf("fingerprint after a healthcheck-only edit = %q, want the unchanged %q "+
			"(the premise the raw-template-identity check exists to cover)",
			editedFP, fingerprint)
	}
	if editedID == templateID {
		t.Fatal("precondition: the raw template identity must change with the edited bytes")
	}

	// The raw template identity catches the edit: re-materialization is
	// requested even though no fingerprint or verification result changed.
	c.checkDrift(ctx)
	select {
	case <-c.kick:
	default:
		t.Fatal("checkDrift missed a healthcheck-only template edit: the raw template " +
			"identity must authorize every template byte, not just fingerprinted content")
	}
}

// writeHealthcheckAppIdentitiesOnly recomputes the on-disk fingerprint without
// rewriting the app files.
func writeHealthcheckAppIdentitiesOnly(t *testing.T, appsDir string) (fingerprint, templateID string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(appsDir, "fn", "template.yaml"))
	if err != nil {
		t.Fatalf("read template: %v", err)
	}
	tmpl, err := app.ParseTemplate(data)
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	fp, err := app.FingerprintApp(filepath.Join(appsDir, "fn"), tmpl)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	src, err := app.ReadTemplateSource(appsDir, "fn")
	if err != nil {
		t.Fatalf("template source: %v", err)
	}
	return fp, src.Identity
}

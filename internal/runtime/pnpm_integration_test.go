//go:build integration

// Node dependency integration tests for the pnpm-only install path: the pinned
// pnpm CLI is present even with no dependencies, a committed
// pnpm-lock.yaml installs the dependency tree frozen, and a missing or
// unsupported lock fails Prepare with an actionable error (no npm fallback).
package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/testutil"
)

// pnpmDependencyHandler imports the fixture dependency and prints its effect, so
// a successful Execute proves the dependency was installed into
// /app/node_modules by pnpm and resolves at runtime.
const pnpmDependencyHandler = `import colors from "picocolors";
export function run(event) {
  console.log(colors.red("pnpm dep " + event.event_id));
}
`

// newPnpmNodeApp builds a Node app directory whose handler imports picocolors,
// with the given manifests written into it.
func newPnpmNodeApp(t *testing.T, name string, manifests map[string]string) app.App {
	t.Helper()
	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: handler.run
    pattern:
      status: [COMPLETED]
`)
	writeFile(t, dir, "handler.js", pnpmDependencyHandler)
	for fileName, content := range manifests {
		writeFile(t, dir, fileName, content)
	}
	return app.App{Name: name, Dir: dir, Template: &app.Template{Runtime: "node24"}}
}

// TestIntegrationNodeNoDepsHasPnpmBinary verifies the pinned pnpm binary is
// present and directly executable in a Node runtime image even when the app
// declares no dependencies, by building the app image and running
// `test -x /usr/local/bin/pnpm && pnpm --version` in a one-off container: the
// explicit executable check pins the installed path, and the version pins the
// binary. It also asserts Node 24 is the managed runtime alongside it.
//
// The pinned pnpm release ships a statically linked musl standalone binary; the
// runtime tool downloads the checksum-pinned archive for the target architecture
// and extracts the bare `pnpm` binary to /usr/local/bin/pnpm, so it runs on
// node:24-alpine with no JS wrapper and no glibc dependency.
func TestIntegrationNodeNoDepsHasPnpmBinary(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	t.Cleanup(cleanupImagePrefixes(cli, "relay-app-pnpm-nodeps:"))

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: handler.run
    pattern:
      status: [COMPLETED]
`)
	writeFile(t, dir, "handler.js", "export function run(event) { console.log('ok'); }\n")
	fn := app.App{Name: "pnpm-nodeps", Dir: dir, Template: &app.Template{Runtime: "node24"}}

	m, _ := newManager(t)
	p, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if p.Dependency != "" {
		t.Errorf("a function with no deps must have no dependency layer, got %q", p.Dependency)
	}

	out, err := runImageCommand(ctx, t, p.Image, []string{"sh", "-c", "test -x /usr/local/bin/pnpm && pnpm --version"})
	if err != nil {
		t.Fatalf("run pnpm --version: %v", err)
	}
	if !strings.Contains(out, PnpmVersion) {
		t.Errorf("expected the pinned pnpm %s binary in the image, got: %q", PnpmVersion, out)
	}

	nodeOut, err := runImageCommand(ctx, t, p.Image, []string{"node", "--version"})
	if err != nil {
		t.Fatalf("run node --version: %v", err)
	}
	if !strings.Contains(nodeOut, "v24.") {
		t.Errorf("expected node v24 in the image, got: %q", nodeOut)
	}
}

// TestIntegrationNodePnpmInstallsDependencyAndExecutes verifies the pnpm
// dependency path end to end: package.json + a committed pnpm-lock.yaml build a
// frozen dependency layer, and the handler imports the dependency at runtime.
func TestIntegrationNodePnpmInstallsDependencyAndExecutes(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	depBefore := depTagSet(ctx, cli)
	t.Cleanup(cleanupNewDepImagesSince(cli, depBefore))
	t.Cleanup(cleanupImagePrefixes(cli, "relay-app-pnpm-dep:"))

	fn := newPnpmNodeApp(t, "pnpm-dep", map[string]string{
		"package.json":   `{"type":"module","dependencies":{"picocolors":"^1.0.0"}}`,
		"pnpm-lock.yaml": nodePnpmLockPicocolors,
	})
	m, _ := newManager(t)
	out := newAppOutputSink(t)

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if !strings.HasPrefix(prepared.Dependency, depRepoPrefix) {
		t.Fatalf("dependency = %q, want a relay-dep-* layer", prepared.Dependency)
	}
	if err := m.Execute(ctx, prepared, "handler.run", []byte(`{"status":"COMPLETED","event_id":"42"}`), nil); err != nil {
		t.Fatalf("execute: %v", err)
	}
	awaitAppOutput(t, ctx, out, "pnpm dep 42")
}

// TestIntegrationNodeMissingPnpmLockFailsPrepare pins the actionable
// missing-lock error: a package.json without pnpm-lock.yaml fails Prepare (there
// is no npm fallback).
func TestIntegrationNodeMissingPnpmLockFailsPrepare(t *testing.T) {
	testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	fn := newPnpmNodeApp(t, "pnpm-missing-lock", map[string]string{
		"package.json": `{"type":"module","dependencies":{"picocolors":"^1.0.0"}}`,
	})
	m, _ := newManager(t)
	_, err := m.Prepare(ctx, fn)
	if err == nil {
		t.Fatal("expected Prepare to fail for a missing pnpm-lock.yaml")
	}
	if !strings.Contains(err.Error(), "pnpm-lock.yaml") {
		t.Errorf("error = %v, want it to name pnpm-lock.yaml", err)
	}
}

// TestIntegrationNodeRejectsPackageLock pins that an npm package-lock.json is
// rejected rather than used, even alongside a valid pnpm lock.
func TestIntegrationNodeRejectsPackageLock(t *testing.T) {
	testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	fn := newPnpmNodeApp(t, "pnpm-reject-npm-lock", map[string]string{
		"package.json":      `{"type":"module","dependencies":{"picocolors":"^1.0.0"}}`,
		"pnpm-lock.yaml":    nodePnpmLockPicocolors,
		"package-lock.json": `{}`,
	})
	m, _ := newManager(t)
	_, err := m.Prepare(ctx, fn)
	if err == nil {
		t.Fatal("expected Prepare to reject package-lock.json")
	}
	if !strings.Contains(err.Error(), "package-lock.json") {
		t.Errorf("error = %v, want it to name package-lock.json", err)
	}
}

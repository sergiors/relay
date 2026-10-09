package runtime

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"relay/internal/app"
	"relay/internal/runtime/node"
	"relay/internal/runtime/plan"
)

// pythonSpec is the production python3.14 spec shared by the fingerprint tests.
func pythonSpec() plan.Spec {
	return plan.Spec{
		Name:      "python3.14",
		Engine:    plan.EnginePython,
		BaseImage: "python:3.14-slim",
		RuntimeTools: []plan.RuntimeTool{{
			From: UvImageTag, Source: "/uv", Destination: "/usr/local/bin/uv",
		}},
	}
}

// pythonRequirementsDeps is the conventional python dependency layer used by
// the fingerprint tests (a single requirements.txt installed into /app with uv).
func pythonRequirementsDeps() plan.Deps {
	return plan.Deps{
		Files:   []string{"requirements.txt"},
		Install: "uv pip install --system --no-cache -r requirements.txt",
		Dir:     "/app",
	}
}

// testDependencySnapshot stages the given name->content manifests into a fresh
// private root (exactly as snapshotDependency does) and returns the disk-backed
// snapshot, with release registered as cleanup. It is the seam tests use to
// build a dependencySnapshot without a real manifest tree on disk.
func testDependencySnapshot(t *testing.T, manifests map[string]string) dependencySnapshot {
	t.Helper()
	root := t.TempDir()
	names := make([]string, 0, len(manifests))
	for name := range manifests {
		names = append(names, name)
	}
	sort.Strings(names)
	snap := dependencySnapshot{root: root, files: make([]dependencyManifest, 0, len(names))}
	for _, name := range names {
		target := filepath.Join(root, filepath.FromSlash(name))
		if dir := filepath.Dir(target); dir != root {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatalf("mkdir for %s: %v", name, err)
			}
		}
		if err := os.WriteFile(target, []byte(manifests[name]), 0o644); err != nil {
			t.Fatalf("write manifest %s: %v", name, err)
		}
		snap.files = append(snap.files, dependencyManifest{name: name})
	}
	t.Cleanup(snap.release)
	return snap
}

// TestDependencyFingerprintDeterminism verifies the fingerprint is stable: the
// same inputs always yield the same digest, so a dependency image tagged by the
// fingerprint is safely reuseable across builds and processes.
func TestDependencyFingerprintDeterminism(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "requirements.txt"), []byte("six==1.16.0\n"), 0o644); err != nil {
		t.Fatalf("write requirements: %v", err)
	}
	spec := pythonSpec()
	deps := pythonRequirementsDeps()

	a, err := DependencyFingerprint("arm64", "linux", spec, dir, deps)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	b, err := DependencyFingerprint("arm64", "linux", spec, dir, deps)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	if a != b {
		t.Fatalf("fingerprint not deterministic: %s != %s", a, b)
	}
	if len(a) != 64 {
		t.Errorf("fingerprint length = %d, want 64 hex", len(a))
	}
}

// TestDependencyFingerprintSensitivity verifies every relevant input changes the
// digest. Each mutation must produce a DIFFERENT hash, proving the layer cache
// key covers everything that shapes the installed payload.
func TestDependencyFingerprintSensitivity(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "requirements.txt"), []byte("six==1.16.0\n"), 0o644); err != nil {
		t.Fatalf("write requirements: %v", err)
	}
	base := pythonSpec()
	deps := pythonRequirementsDeps()

	mutate := func(mk func(*plan.Spec, *plan.Deps)) string {
		s := base
		d := deps
		mk(&s, &d)
		fp, err := DependencyFingerprint("arm64", "linux", s, dir, d)
		if err != nil {
			t.Fatalf("fingerprint: %v", err)
		}
		return fp
	}

	orig, err := DependencyFingerprint("arm64", "linux", base, dir, deps)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}

	// Architecture and platform are separate fingerprint arguments.
	changedArch, err := DependencyFingerprint("amd64", "linux", base, dir, deps)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	if changedArch == orig {
		t.Error("changing the architecture must change the fingerprint")
	}
	changedPlatform, err := DependencyFingerprint("arm64", "darwin", base, dir, deps)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	if changedPlatform == orig {
		t.Error("changing the platform must change the fingerprint")
	}

	// Runtime name and base image.
	if fp := mutate(func(s *plan.Spec, _ *plan.Deps) { s.Name = "python3.15" }); fp == orig {
		t.Error("changing the runtime name must change the fingerprint")
	}
	if fp := mutate(func(s *plan.Spec, _ *plan.Deps) { s.BaseImage = "python:3.15-slim" }); fp == orig {
		t.Error("changing the base image must change the fingerprint")
	}
	// The pinned external install tool (uv) shapes the installed payload: a
	// version bump must yield a new layer.
	if fp := mutate(func(s *plan.Spec, _ *plan.Deps) {
		s.RuntimeTools = []plan.RuntimeTool{{From: "ghcr.io/astral-sh/uv:0.12.18", Source: "/uv", Destination: "/usr/local/bin/uv"}}
	}); fp == orig {
		t.Error("changing the pinned install tool version must change the fingerprint")
	}
	if fp := mutate(func(s *plan.Spec, _ *plan.Deps) { s.RuntimeTools = nil }); fp == orig {
		t.Error("removing the install tool must change the fingerprint")
	}
	// Install command and directory.
	if fp := mutate(func(_ *plan.Spec, d *plan.Deps) { d.Install = "pip install -r requirements.txt" }); fp == orig {
		t.Error("changing the install command must change the fingerprint")
	}
	if fp := mutate(func(_ *plan.Spec, d *plan.Deps) { d.Dir = "/opt/app" }); fp == orig {
		t.Error("changing the install dir must change the fingerprint")
	}
	// File name (rename) and file content. Rename is a content change: create a
	// second identically-contented file under a different name and confirm the
	// name is part of the digest.
	if err := os.WriteFile(filepath.Join(dir, "reqs.txt"), []byte("six==1.16.0\n"), 0o644); err != nil {
		t.Fatalf("write reqs.txt: %v", err)
	}
	if fp := mutate(func(_ *plan.Spec, d *plan.Deps) { d.Files = []string{"reqs.txt"} }); fp == orig {
		t.Error("renaming the manifest file must change the fingerprint")
	}
	if err := os.WriteFile(filepath.Join(dir, "requirements.txt"), []byte("six==1.17.0\n"), 0o644); err != nil {
		t.Fatalf("rewrite requirements: %v", err)
	}
	if fp, err := DependencyFingerprint("arm64", "linux", base, dir, deps); err != nil {
		t.Fatalf("fingerprint: %v", err)
	} else if fp == orig {
		t.Error("changing the manifest content must change the fingerprint")
	}
}

// TestDependencyFingerprintCanonicalFileOrder verifies that the same set of
// manifest files yields the same fingerprint regardless of the order the engine
// listed them: file order is sorted before hashing so two engines agreeing on a
// shared cache key are order-insensitive.
func TestDependencyFingerprintCanonicalFileOrder(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "package.json"), []byte(`{"type":"module"}`), 0o644); err != nil {
		t.Fatalf("write package.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "pnpm-lock.yaml"), []byte("lockfileVersion: '9.0'\n"), 0o644); err != nil {
		t.Fatalf("write lock: %v", err)
	}
	spec := plan.Spec{Name: "node24", Engine: plan.EngineNode, BaseImage: "node:24-alpine"}

	ab := plan.Deps{Files: []string{"package.json", "pnpm-lock.yaml"}, Install: "pnpm install --prod --frozen-lockfile", Dir: "/app"}
	ba := plan.Deps{Files: []string{"pnpm-lock.yaml", "package.json"}, Install: "pnpm install --prod --frozen-lockfile", Dir: "/app"}

	fpAB, err := DependencyFingerprint("arm64", "linux", spec, dir, ab)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	fpBA, err := DependencyFingerprint("arm64", "linux", spec, dir, ba)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	if fpAB != fpBA {
		t.Errorf("file order must not change the fingerprint: %s != %s", fpAB, fpBA)
	}
}

// TestDependencyFingerprintRuntimeVersionDifferent verifies that the dependency
// fingerprint differs across runtime versions even with identical manifest
// content: different spec.Name / BaseImage must not share a layer, and must map
// to different dependency images. Pure fingerprint logic, no daemon needed.
func TestDependencyFingerprintRuntimeVersionDifferent(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "requirements.txt"), []byte("six==1.16.0\n"), 0o644); err != nil {
		t.Fatalf("write requirements: %v", err)
	}
	deps := pythonRequirementsDeps()

	specA := plan.Spec{Name: "python3.14", Engine: plan.EnginePython, BaseImage: "python:3.14-slim"}
	specB := plan.Spec{Name: "python3.15", Engine: plan.EnginePython, BaseImage: "python:3.15-slim"}

	fpA, err := DependencyFingerprint("arm64", "linux", specA, dir, deps)
	if err != nil {
		t.Fatalf("fingerprint A: %v", err)
	}
	fpB, err := DependencyFingerprint("arm64", "linux", specB, dir, deps)
	if err != nil {
		t.Fatalf("fingerprint B: %v", err)
	}
	if fpA == fpB {
		t.Error("different runtime versions must not share a dependency layer fingerprint")
	}
	if depImageRef(fpA) == depImageRef(fpB) {
		t.Error("different runtime versions must map to different dependency images")
	}
}

// TestDependencyFingerprintNativePair pins the native uv dependency identity:
// both manifest files participate, so changing either the declared deps
// (pyproject.toml) or the resolved lock (uv.lock) yields a different layer, while
// an unchanged pair is stable.
func TestDependencyFingerprintNativePair(t *testing.T) {
	dir := t.TempDir()
	pyproject := "[project]\nname = \"x\"\nversion = \"0.1.0\"\ndependencies = [\"six==1.16.0\"]\n"
	if err := os.WriteFile(filepath.Join(dir, "pyproject.toml"), []byte(pyproject), 0o644); err != nil {
		t.Fatalf("write pyproject: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "uv.lock"), []byte("version = 1\n"), 0o644); err != nil {
		t.Fatalf("write uv.lock: %v", err)
	}
	spec := pythonSpec()
	deps := plan.Deps{
		Files:   []string{"pyproject.toml", "uv.lock"},
		Install: "uv export --locked --no-dev --no-emit-project && uv pip install --system",
		Dir:     "/app",
	}

	base, err := DependencyFingerprint("arm64", "linux", spec, dir, deps)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}

	// pyproject change (declared deps).
	changedPyproject := "[project]\nname = \"x\"\nversion = \"0.1.0\"\ndependencies = [\"six==1.17.0\"]\n"
	if err := os.WriteFile(filepath.Join(dir, "pyproject.toml"), []byte(changedPyproject), 0o644); err != nil {
		t.Fatalf("rewrite pyproject: %v", err)
	}
	pf, err := DependencyFingerprint("arm64", "linux", spec, dir, deps)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	if pf == base {
		t.Error("changing pyproject.toml must change the dependency fingerprint")
	}

	// uv.lock change (resolved set) after restoring pyproject.
	if err := os.WriteFile(filepath.Join(dir, "pyproject.toml"), []byte(pyproject), 0o644); err != nil {
		t.Fatalf("restore pyproject: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "uv.lock"), []byte("version = 1\n# changed\n"), 0o644); err != nil {
		t.Fatalf("rewrite uv.lock: %v", err)
	}
	lf, err := DependencyFingerprint("arm64", "linux", spec, dir, deps)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	if lf == base {
		t.Error("changing uv.lock must change the dependency fingerprint")
	}
}

// TestDependencyFingerprintNodePlanDepsPairSensitivity pins the Node counterpart
// of the native-pair rule using the ENGINE's OWN dependency declaration rather
// than a hand-built plan.Deps: it calls node.Engine{}.Plan on a real app dir and
// feeds the returned Deps to DependencyFingerprint. The Node engine must declare
// BOTH package.json and pnpm-lock.yaml, so changing either one ALONE (the other
// left byte-for-byte unchanged) re-fingerprints the shared relay-dep-* layer,
// while a source-only edit leaves the dependency fingerprint untouched because
// source is app identity, not a dependency input.
func TestDependencyFingerprintNodePlanDepsPairSensitivity(t *testing.T) {
	dir := t.TempDir()
	const pkg = `{"type":"module","dependencies":{"picocolors":"^1.0.0"}}`
	writeNodeTestFile(t, dir, "package.json", pkg)
	writeNodeTestFile(t, dir, "pnpm-lock.yaml", nodePnpmLockPicocolors)
	writeNodeTestFile(t, dir, "index.js", "export function handler(e) {}\n")

	spec, err := lookup("node24")
	if err != nil {
		t.Fatalf("lookup node24: %v", err)
	}

	// planDeps is the production path: the Node engine's actual Plan(...).Deps
	// for the current manifest bytes on disk.
	planDeps := func() plan.Deps {
		t.Helper()
		p, err := node.Engine{}.Plan(spec, dir, []string{"index"})
		if err != nil {
			t.Fatalf("node plan: %v", err)
		}
		if p.Deps.IsZero() {
			t.Fatalf("node plan returned zero Deps, want the package.json + pnpm-lock.yaml layer")
		}
		return p.Deps
	}
	fingerprint := func() string {
		t.Helper()
		fp, err := DependencyFingerprint(arch, platform, spec, dir, planDeps())
		if err != nil {
			t.Fatalf("dependency fingerprint: %v", err)
		}
		return fp
	}

	// Both manifests must be declared, so either one is an input to the layer.
	deps := planDeps()
	if len(deps.Files) != 2 || deps.Files[0] != "package.json" || deps.Files[1] != "pnpm-lock.yaml" {
		t.Fatalf("node Deps.Files = %v, want [package.json pnpm-lock.yaml]", deps.Files)
	}

	base := fingerprint()

	// Change package.json ALONE: pnpm-lock.yaml is left byte-for-byte unchanged.
	writeNodeTestFile(t, dir, "package.json", `{"type":"module","dependencies":{"picocolors":"^1.0.0","ms":"2.1.3"}}`)
	if got := fingerprint(); got == base {
		t.Error("changing package.json alone must change the Node dependency fingerprint")
	}

	// Restore package.json, then change pnpm-lock.yaml ALONE.
	writeNodeTestFile(t, dir, "package.json", pkg)
	writeNodeTestFile(t, dir, "pnpm-lock.yaml", nodePnpmLockPicocolorsMs)
	if got := fingerprint(); got == base {
		t.Error("changing pnpm-lock.yaml alone must change the Node dependency fingerprint")
	}

	// Restore the manifests: the fingerprint returns to base, proving the edits
	// above changed it through the manifest bytes and nothing else.
	writeNodeTestFile(t, dir, "pnpm-lock.yaml", nodePnpmLockPicocolors)
	if got := fingerprint(); got != base {
		t.Fatalf("restoring the manifests must restore the Node dependency fingerprint: %s != %s", got, base)
	}

	// A source-only edit is app identity, not dependency input: the dependency
	// fingerprint must not move.
	writeNodeTestFile(t, dir, "index.js", "export function handler(e) { return 1; }\n")
	if got := fingerprint(); got != base {
		t.Error("a source-only edit must not change the Node dependency fingerprint")
	}
}

// TestDependencyFingerprintPnpmTool pins that the pinned pnpm runtime tool is
// part of the Node dependency identity: bumping its image tag yields a different
// layer even with identical manifests.
func TestDependencyFingerprintPnpmTool(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "package.json"), []byte(`{"type":"module"}`), 0o644); err != nil {
		t.Fatalf("write package.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "pnpm-lock.yaml"), []byte("lockfileVersion: '9.0'\n"), 0o644); err != nil {
		t.Fatalf("write pnpm-lock.yaml: %v", err)
	}
	deps := plan.Deps{Files: []string{"package.json", "pnpm-lock.yaml"}, Install: "pnpm install --prod --frozen-lockfile", Dir: "/app"}
	base := plan.Spec{Name: "node24", Engine: plan.EngineNode, BaseImage: "node:24-alpine", RuntimeTools: []plan.RuntimeTool{pnpmTool}}

	orig, err := DependencyFingerprint("amd64", "linux", base, dir, deps)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}

	// The pnpm artifact's resolved identity is part of the layer: a bumped
	// archive URL or digest must yield a different layer even with identical
	// manifests.
	bumped := base
	bumpedVariant, ok := pnpmTool.Artifact.VariantForArch("amd64")
	if !ok {
		t.Fatal("pnpm tool has no amd64 variant")
	}
	bumpedVariant.SHA256 = strings.Repeat("0", 64)
	bumped.RuntimeTools = []plan.RuntimeTool{{
		Destination: pnpmTool.Destination,
		Artifact:    &plan.RuntimeArtifact{Variants: []plan.ArtifactVariant{bumpedVariant}},
	}}
	if got, err := DependencyFingerprint("amd64", "linux", bumped, dir, deps); err != nil {
		t.Fatalf("fingerprint bumped: %v", err)
	} else if got == orig {
		t.Error("a changed pnpm artifact digest must change the dependency fingerprint")
	}

	// An artifact with no variant for the target architecture is an error, so an
	// unsupported target never gets a cached layer for a tool it cannot install.
	if _, err := DependencyFingerprint("riscv64", "linux", base, dir, deps); err == nil {
		t.Error("an unsupported pnpm artifact architecture must fail the dependency fingerprint")
	}
}

// TestDependencyFingerprintRejectsMixedRuntimeToolForm verifies the dependency
// fingerprint resolver rejects a RuntimeTool that mixes image-copy fields with
// an Artifact, so a malformed tool never gets a content-addressed layer.
func TestDependencyFingerprintRejectsMixedRuntimeToolForm(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "requirements.txt"), []byte("six==1.16.0\n"), 0o644); err != nil {
		t.Fatalf("write requirements.txt: %v", err)
	}
	mixed := pythonSpec()
	mixed.RuntimeTools = []plan.RuntimeTool{{
		From:        UvImageTag,
		Source:      "/uv",
		Destination: "/usr/local/bin/pnpm",
		Artifact:    &plan.RuntimeArtifact{Variants: []plan.ArtifactVariant{{Arch: "amd64", URL: "https://example.test/x.tgz", SHA256: strings.Repeat("a", 64), Member: "pnpm"}}},
	}}
	if _, err := DependencyFingerprint("amd64", "linux", mixed, dir, pythonRequirementsDeps()); err == nil {
		t.Fatal("DependencyFingerprint accepted a runtime tool mixing From/Source with Artifact")
	}
}

// TestDependencyFingerprintMissingManifest verifies that a manifest file the
// engine declared but that is absent on disk is an error (a race / mid-reconcile
// state), not silently hashed as empty — an empty hash would poison the shared
// layer cache.
func TestDependencyFingerprintMissingManifest(t *testing.T) {
	dir := t.TempDir() // empty: no requirements.txt
	spec := pythonSpec()
	deps := pythonRequirementsDeps()
	if _, err := DependencyFingerprint("arm64", "linux", spec, dir, deps); err == nil {
		t.Error("expected an error for a missing manifest file")
	}
}

// TestDepImageRefTruncatesFingerprintToTag verifies the dependency image
// reference format and defensive truncation (mirroring ImageRef).
func TestDepImageRefTruncatesFingerprintToTag(t *testing.T) {
	fp := strings.Repeat("a", 16) + "bcdef"
	if got := depImageRef(fp); got != "relay-dep-"+strings.Repeat("a", 16) {
		t.Errorf("depImageRef = %q, want the first 16 hex chars", got)
	}
	// Shorter than the prefix len is used verbatim (never padded).
	if got := depImageRef("abc"); got != "relay-dep-abc" {
		t.Errorf("depImageRef(short) = %q, want relay-dep-abc", got)
	}
	if strings.HasPrefix(depImageRef(fp), depRepoPrefix) != true {
		t.Errorf("dep ref must carry the relay-dep- prefix")
	}
}

// TestIsDepRepoMatchesRelayDepPrefix verifies the relay-dep- prefix
// discriminates dependency repos.
func TestIsDepRepoMatchesRelayDepPrefix(t *testing.T) {
	if !isDepRepo("relay-dep-abcdef1234567890") {
		t.Error("expected a relay-dep repo to be detected")
	}
	if isDepRepo("relay-app-user") {
		t.Error("a relay-fn repo must NOT be a dep repo")
	}
	if isDepRepo("python:3.14-slim") {
		t.Error("a foreign repo must NOT be a dep repo")
	}
}

// TestDependencyFingerprintConcurrent verifies concurrent fingerprint calls for
// the same inputs are stable (exercised under -race).
func TestDependencyFingerprintConcurrent(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "requirements.txt"), []byte("six==1.16.0\n"), 0o644); err != nil {
		t.Fatalf("write requirements: %v", err)
	}
	spec := pythonSpec()
	deps := pythonRequirementsDeps()

	const n = 64
	results := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			fp, err := DependencyFingerprint("arm64", "linux", spec, dir, deps)
			if err != nil {
				t.Errorf("fingerprint: %v", err)
				return
			}
			results[i] = fp
		}(i)
	}
	wg.Wait()
	for i := 1; i < n; i++ {
		if results[i] != results[0] {
			t.Fatalf("concurrent fingerprints diverged: %s vs %s", results[i], results[0])
		}
	}
}

// TestTypeScriptEditsInvalidateAppNotDependency pins the artifact split for
// TypeScript handlers: a .ts source edit (or a tsconfig.json edit) changes the
// app fingerprint — the transpiled output is baked into the app image —
// while the dependency fingerprint is unchanged (it hashes the dependency
// manifests only). A TS change therefore rebuilds the app image but reuses
// the shared relay-dep-* layer.
func TestTypeScriptEditsInvalidateAppNotDependency(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "package.json"), []byte(`{"type":"module"}`), 0o644); err != nil {
		t.Fatalf("write package.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tsconfig.json"), []byte(`{"compilerOptions":{"strict":true}}`+"\n"), 0o644); err != nil {
		t.Fatalf("write tsconfig: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "src"), 0o755); err != nil {
		t.Fatalf("mkdir src: %v", err)
	}
	handlerPath := filepath.Join(dir, "src", "handler.ts")
	if err := os.WriteFile(handlerPath, []byte("export function handler(e) { return 1; }\n"), 0o644); err != nil {
		t.Fatalf("write handler.ts: %v", err)
	}

	spec := plan.Spec{Name: "node24", Engine: plan.EngineNode, BaseImage: "node:24-alpine"}
	deps := plan.Deps{Files: []string{"package.json"}, Install: "pnpm install --prod --frozen-lockfile", Dir: "/app"}

	funcBefore := fpOf(t, dir)
	depBefore, err := DependencyFingerprint("arm64", "linux", spec, dir, deps)
	if err != nil {
		t.Fatalf("dependency fingerprint: %v", err)
	}

	// Edit the TypeScript handler: it is app source, not dependency input.
	if err := os.WriteFile(handlerPath, []byte("export function handler(e) { return 2; }\n"), 0o644); err != nil {
		t.Fatalf("rewrite handler.ts: %v", err)
	}
	if got := fpOf(t, dir); got == funcBefore {
		t.Error("editing a .ts handler must change the function fingerprint")
	}
	depAfterTS, err := DependencyFingerprint("arm64", "linux", spec, dir, deps)
	if err != nil {
		t.Fatalf("dependency fingerprint: %v", err)
	}
	if depAfterTS != depBefore {
		t.Error("a .ts source edit must NOT change the dependency fingerprint")
	}

	// Edit tsconfig.json: also app source (it shapes transpilation).
	funcAfterTS := fpOf(t, dir)
	if err := os.WriteFile(filepath.Join(dir, "tsconfig.json"), []byte(`{"compilerOptions":{"strict":false}}`+"\n"), 0o644); err != nil {
		t.Fatalf("rewrite tsconfig: %v", err)
	}
	if got := fpOf(t, dir); got == funcAfterTS {
		t.Error("editing tsconfig.json must change the function fingerprint")
	}
	depAfterTSConfig, err := DependencyFingerprint("arm64", "linux", spec, dir, deps)
	if err != nil {
		t.Fatalf("dependency fingerprint: %v", err)
	}
	if depAfterTSConfig != depBefore {
		t.Error("a tsconfig.json edit must NOT change the dependency fingerprint")
	}
}

// TestTypeScriptIgnoredFileChangesNeitherFingerprint pins the shared selection
// policy for TypeScript: a .ts file matched by .gitignore is neither app
// source nor dependency input, so editing it changes neither digest.
func TestTypeScriptIgnoredFileChangesNeitherFingerprint(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".gitignore"), []byte("*.generated.ts\n"), 0o644); err != nil {
		t.Fatalf("write .gitignore: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "package.json"), []byte(`{"type":"module"}`), 0o644); err != nil {
		t.Fatalf("write package.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "handler.ts"), []byte("export function handler(e) {}\n"), 0o644); err != nil {
		t.Fatalf("write handler.ts: %v", err)
	}
	generatedPath := filepath.Join(dir, "scratch.generated.ts")
	if err := os.WriteFile(generatedPath, []byte("export const a = 1\n"), 0o644); err != nil {
		t.Fatalf("write generated.ts: %v", err)
	}

	spec := plan.Spec{Name: "node24", Engine: plan.EngineNode, BaseImage: "node:24-alpine"}
	deps := plan.Deps{Files: []string{"package.json"}, Install: "pnpm install --prod --frozen-lockfile", Dir: "/app"}

	funcBefore := fpOf(t, dir)
	depBefore, err := DependencyFingerprint("arm64", "linux", spec, dir, deps)
	if err != nil {
		t.Fatalf("dependency fingerprint: %v", err)
	}

	if err := os.WriteFile(generatedPath, []byte("export const a = 2\n"), 0o644); err != nil {
		t.Fatalf("rewrite generated.ts: %v", err)
	}
	if got := fpOf(t, dir); got != funcBefore {
		t.Error("editing a .gitignored .ts file must not change the function fingerprint")
	}
	depAfter, err := DependencyFingerprint("arm64", "linux", spec, dir, deps)
	if err != nil {
		t.Fatalf("dependency fingerprint: %v", err)
	}
	if depAfter != depBefore {
		t.Error("editing a .gitignored .ts file must not change the dependency fingerprint")
	}
}

// fpOf computes the app fingerprint for dir, failing the test on error.
func fpOf(t *testing.T, dir string) string {
	t.Helper()
	f, err := app.Fingerprint(dir)
	if err != nil {
		t.Fatalf("function fingerprint: %v", err)
	}
	return f
}

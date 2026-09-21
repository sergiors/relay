package runtime

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"relay/internal/runtime/plan"
)

// pythonSpec is the production python3.14 spec shared by the fingerprint tests.
func pythonSpec() plan.Spec {
	return plan.Spec{
		Name:      "python3.14",
		Engine:    plan.EnginePython,
		BaseImage: "python:3.14-slim",
		ToolCopies: []plan.ImageCopy{{
			From: UvImageTag, Source: "/uv", Dest: "/usr/local/bin/uv",
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
		s.ToolCopies = []plan.ImageCopy{{From: "ghcr.io/astral-sh/uv:0.12.18", Source: "/uv", Dest: "/usr/local/bin/uv"}}
	}); fp == orig {
		t.Error("changing the pinned install tool version must change the fingerprint")
	}
	if fp := mutate(func(s *plan.Spec, _ *plan.Deps) { s.ToolCopies = nil }); fp == orig {
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
	if err := os.WriteFile(filepath.Join(dir, "package-lock.json"), []byte("{}"), 0o644); err != nil {
		t.Fatalf("write lock: %v", err)
	}
	spec := plan.Spec{Name: "node24", Engine: plan.EngineNode, BaseImage: "node:24-alpine"}

	ab := plan.Deps{Files: []string{"package.json", "package-lock.json"}, Install: "npm ci --omit=dev", Dir: "/app"}
	ba := plan.Deps{Files: []string{"package-lock.json", "package.json"}, Install: "npm ci --omit=dev", Dir: "/app"}

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
	if isDepRepo("relay-fn-user") {
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

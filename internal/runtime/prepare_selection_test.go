package runtime

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"relay/internal/function"
	"relay/internal/runtime/plan"
	"relay/internal/source"
)

// stagedContextFiles returns the set of file paths (slash form) present in a
// build-context tar captured from the daemon request. It lets a test assert
// exactly which source the Manager staged without a real Docker daemon.
func stagedContextFiles(t *testing.T, raw []byte) map[string]bool {
	t.Helper()
	files := map[string]bool{}
	tr := tar.NewReader(bytes.NewReader(raw))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read build context tar: %v", err)
		}
		files[hdr.Name] = true
	}
	return files
}

// TestPrepareWithSelectionUsesSuppliedSelection is the selection-handoff
// regression for the runtime Manager: when the caller supplies the source
// selection it already fingerprinted, the build stages THAT selection and does
// not re-derive one. The proof: the on-disk .gitignore is edited AFTER the
// selection is resolved to exclude the handler; a freshly resolved selection
// would omit index.js from the context, while the supplied (pre-edit) selection
// still contains it. index.js being staged shows the supplied policy was used.
//
// The returned fingerprint is the SNAPSHOT-derived identity, not the caller's
// pre-edit value: the image is tagged with exactly the bytes this call staged
// (including the .gitignore that appeared after the caller's scan), so the tag
// and the image can never disagree. The next reconcile/audit compares the
// live tree against this built identity and rebuilds the drift.
func TestPrepareWithSelectionUsesSuppliedSelection(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function h(){}\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}

	// Resolve the selection BEFORE the rule edit, and fingerprint it.
	selection, err := source.ForDir(dir)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	fp, err := function.FingerprintSelection(selection)
	if err != nil {
		t.Fatalf("fingerprint selection: %v", err)
	}

	// Now change the policy so a re-resolution would exclude index.js.
	if err := os.WriteFile(filepath.Join(dir, ".gitignore"), []byte("index.js\n"), 0o644); err != nil {
		t.Fatalf("write gitignore: %v", err)
	}

	var contextTar []byte
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{
			method: http.MethodPost, path: "/build", body: `{}`,
			onBody: func(b []byte) { contextTar = append([]byte(nil), b...) },
		},
	)
	m := newLifecycleManager(t, cli, context.Background())

	fn := function.Function{Name: "selected", Dir: dir, Template: &function.Template{Runtime: "node24"}}
	got, err := m.PrepareWithFingerprintAndSelection(context.Background(), fn, fp, selection)
	if err != nil {
		t.Fatalf("prepare with supplied selection: %v", err)
	}
	// The tag is the snapshot-derived identity: it must be the digest over the
	// bytes actually staged under the supplied policy, not the caller's pre-edit
	// value (which predates the .gitignore).
	wantFP, err := function.FingerprintSelection(selection)
	if err != nil {
		t.Fatalf("reference snapshot fingerprint: %v", err)
	}
	if got.Fingerprint != wantFP {
		t.Fatalf("fingerprint = %q, want the snapshot-derived %q (not the caller's %q)", got.Fingerprint, wantFP, fp)
	}
	if got.Image != ImageRef(fn.Name, wantFP) {
		t.Fatalf("image = %q, want %q", got.Image, ImageRef(fn.Name, wantFP))
	}

	staged := stagedContextFiles(t, contextTar)
	if !staged["index.js"] {
		t.Fatalf("supplied selection must stage index.js (it was included when resolved), got %v", staged)
	}
	// The .gitignore that appeared after the caller's scan is staged too (the
	// supplied policy includes it), which is exactly why the built identity
	// differs from the caller's pre-edit value.
	if !staged[".gitignore"] {
		t.Fatalf("staged context must include the applicable .gitignore, got %v", staged)
	}
}

// TestEnsureDependencyImageUsesSuppliedFingerprint proves the dependency
// fingerprint is computed ONCE by the caller and threaded through: given a
// sentinel fingerprint, ensureDependencyImage tags the built image from the
// sentinel and never rehashes the snapshot. A rehash would ignore the sentinel
// and produce a different tag.
func TestEnsureDependencyImageUsesSuppliedFingerprint(t *testing.T) {
	const sentinel = "sentinel-fingerprint-not-derived-from-snapshot"

	var builtTag string
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{
			method: http.MethodPost, path: "/build", body: `{"stream":"ok"}`,
			onRequest: func(req *http.Request) { builtTag = req.URL.Query().Get("t") },
		},
	)
	m := newLifecycleManager(t, cli, context.Background())

	fn := function.Function{Name: "dep-once", Dir: t.TempDir(), Template: &function.Template{Runtime: "python3.14"}}
	spec, err := lookup("python3.14")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	deps := plan.Deps{Install: "pip install -r requirements.txt", Dir: "/app", Files: []string{"requirements.txt"}}
	snap := dependencySnapshot{files: []dependencyManifest{{name: "requirements.txt", content: []byte("six==1.16.0\n")}}}

	ref, err := m.ensureDependencyImage(context.Background(), fn, spec, deps, snap, sentinel, "")
	if err != nil {
		t.Fatalf("ensure dependency image: %v", err)
	}
	want := depImageRef(sentinel)
	if ref != want {
		t.Fatalf("dependency ref = %q, want %q (built from the supplied fingerprint)", ref, want)
	}
	if builtTag != want {
		t.Fatalf("built tag = %q, want %q", builtTag, want)
	}
	// A rehash would have produced the real content address, not the sentinel.
	if realFP := dependencyFingerprintFrom(arch, platform, spec, deps, snap); depImageRef(realFP) == want {
		t.Fatal("test sentinel collides with the real dependency fingerprint; pick a different sentinel")
	}
}

// TestPrepareComputesDependencyFingerprintOnce is the per-prepare counter
// regression for the dependency digest: Manager.depFingerprint is the narrow
// injectable seam (default: dependencyFingerprintFrom), and Prepare must call it
// EXACTLY once for a dependency-bearing function — the single value names the
// tag, is stamped as the label, and is threaded into ensureDependencyImage's
// build. A second computation (e.g. re-deriving the fingerprint inside
// ensureDependencyImage) would be counted here.
func TestPrepareComputesDependencyFingerprintOnce(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "main.py"), []byte("def handler(e): return 1\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "requirements.txt"), []byte("six==1.16.0\n"), 0o644); err != nil {
		t.Fatalf("write requirements: %v", err)
	}
	fn := function.Function{Name: "dep-once-prepare", Dir: dir, Template: &function.Template{Runtime: "python3.14"}}

	// Both the dependency image and the function image report absent, so the
	// dependency and function builds both run.
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{method: http.MethodPost, path: "/build", body: `{}`},
	)
	m := newLifecycleManager(t, cli, context.Background())

	depFingerprintCalls := 0
	m.depFingerprint = func(arch, platform string, spec plan.Spec, deps plan.Deps, snap dependencySnapshot) string {
		depFingerprintCalls++
		return dependencyFingerprintFrom(arch, platform, spec, deps, snap)
	}

	got, err := m.Prepare(context.Background(), fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if depFingerprintCalls != 1 {
		t.Fatalf("dependency fingerprint computations = %d, want exactly 1 per prepare", depFingerprintCalls)
	}
	if got.Dependency == "" {
		t.Fatal("a dependency-bearing function must return its dependency reference")
	}
}

// TestSelectAndFingerprintFunctionsNoRuntimeNoSelection pins the no-runtime half
// of the startup helper: a template needing no runtime yields a template-only
// fingerprint and a nil selection (no tree is walked or staged).
func TestSelectAndFingerprintFunctionsNoRuntimeNoSelection(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte("services:\n  - name: web\n    image: nginx:alpine\n    port: 80\n"), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	tmpl, err := function.ParseTemplate([]byte("services:\n  - name: web\n    image: nginx:alpine\n    port: 80\n"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selection, fp, err := function.SelectAndFingerprintFunction(dir, tmpl)
	if err != nil {
		t.Fatalf("select and fingerprint: %v", err)
	}
	if selection != nil {
		t.Fatalf("no-runtime function must not resolve a selection, got %v", selection)
	}
	if fp == "" {
		t.Fatal("no-runtime function must still yield a template-only fingerprint")
	}
}

// TestSelectAndFingerprintFunctionsRuntimeReturnsSelection pins the runtime
// half: the selection is resolved and the digest matches FingerprintSelection
// over it, so the two are coherent by construction.
func TestSelectAndFingerprintFunctionsRuntimeReturnsSelection(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function h(){}\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	selection, fp, err := function.SelectAndFingerprintFunction(dir, &function.Template{Runtime: "node24"})
	if err != nil {
		t.Fatalf("select and fingerprint: %v", err)
	}
	if selection == nil {
		t.Fatal("runtime-backed function must resolve a selection")
	}
	want, err := function.FingerprintSelection(selection)
	if err != nil {
		t.Fatalf("fingerprint selection: %v", err)
	}
	if fp != want {
		t.Fatalf("fingerprint = %q, want %q", fp, want)
	}
	if selection.Dir() != dir {
		t.Fatalf("selection dir = %q, want %q", selection.Dir(), dir)
	}
}

package runtime

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/app"
	"relay/internal/runtime/plan"
	"relay/internal/runtime/python"
)

// zuvloopAppDir writes a requirements.txt manifest so the plans carry a real
// dependency layer (exercisable by the fingerprint below) and returns the dir.
func zuvloopAppDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "requirements.txt"), []byte("six==1.16.0\n"), 0o644); err != nil {
		t.Fatalf("write requirements.txt: %v", err)
	}
	return dir
}

// zuvloopPlans plans the same app under both PYTHON_ZUVLOOP modes with a
// non-empty handler list (event/schedule invoking app).
func zuvloopPlans(t *testing.T, spec plan.Spec, dir string, handlers []string) (disabled, enabled plan.BuildPlan) {
	t.Helper()
	specOff := spec
	specOn := spec
	specOn.PythonZuvloop = true
	off, err := python.Engine{}.Plan(specOff, dir, handlers)
	if err != nil {
		t.Fatalf("plan disabled: %v", err)
	}
	on, err := python.Engine{}.Plan(specOn, dir, handlers)
	if err != nil {
		t.Fatalf("plan enabled: %v", err)
	}
	return off, on
}

// TestPythonZuvloopModeChangesBootstrapLabelIdentity pins what lets a worker
// restart with a flipped PYTHON_ZUVLOOP value rebuild instead of reusing a
// stale image: the injected bootstrap and the managed install differ, so the
// relay.bootstrap label hash (which covers exactly those inputs) differs across
// modes, while the service-only plan (no event/schedule handlers) is identical
// in both modes and must keep the same image identity.
func TestPythonZuvloopModeChangesBootstrapLabelIdentity(t *testing.T) {
	spec, err := lookup("python3.14")
	if err != nil {
		t.Fatalf("lookup python3.14: %v", err)
	}
	dir := zuvloopAppDir(t)

	// An app with invocation handlers: the mode is image identity.
	off, on := zuvloopPlans(t, spec, dir, []string{"handler", "jobs.report"})
	hOff, err := bootstrapHash(off, "amd64")
	if err != nil {
		t.Fatalf("bootstrapHash disabled: %v", err)
	}
	hOn, err := bootstrapHash(on, "amd64")
	if err != nil {
		t.Fatalf("bootstrapHash enabled: %v", err)
	}
	if hOff == hOn {
		t.Fatalf("bootstrap label hash must differ across PYTHON_ZUVLOOP modes for an invoking app (both %s)", hOff)
	}

	// The same app prepared in the reverse flip must land back on the original
	// identity: the hash is a pure function of the plan's injected content.
	if again, err := bootstrapHash(off, "amd64"); err != nil || again != hOff {
		t.Fatalf("disabled hash not stable: %q vs %q (err %v)", again, hOff, err)
	}

	// A service-only app (no event/schedule handler): both modes plan an
	// identical image, so flipping the setting must NOT rebuild its image.
	svcOff, svcOn := zuvloopPlans(t, spec, dir, nil)
	hSvcOff, err := bootstrapHash(svcOff, "amd64")
	if err != nil {
		t.Fatalf("bootstrapHash service-off: %v", err)
	}
	hSvcOn, err := bootstrapHash(svcOn, "amd64")
	if err != nil {
		t.Fatalf("bootstrapHash service-on: %v", err)
	}
	if hSvcOff != hSvcOn {
		t.Errorf("service-only bootstrap hash changed with the mode: %s vs %s (want identical)", hSvcOff, hSvcOn)
	}
	if hSvcOn == hOn {
		t.Errorf("service-only plan must not carry the zuvloop-enabled identity")
	}
}

// TestPythonZuvloopDoesNotChangeDependencyFingerprint pins that the mode never
// enters the shared dependency layer's content address: zuvloop is installed in
// the app image through the managed install path, the Deps the fingerprint
// hashes are byte-identical across modes, and enabling the setting therefore
// produces the same relay-dep-* identity (no dependency image churn, no shared
// layer mutation for other apps).
func TestPythonZuvloopDoesNotChangeDependencyFingerprint(t *testing.T) {
	spec, err := lookup("python3.14")
	if err != nil {
		t.Fatalf("lookup python3.14: %v", err)
	}
	dir := zuvloopAppDir(t)

	off, on := zuvloopPlans(t, spec, dir, []string{"handler"})
	if !on.Deps.Equal(off.Deps) {
		t.Fatalf("Deps changed with the mode: %+v vs %+v", on.Deps, off.Deps)
	}
	if strings.Contains(on.Deps.Install, "zuvloop") {
		t.Errorf("dependency install must not carry zuvloop, got %q", on.Deps.Install)
	}

	specOn := spec
	specOn.PythonZuvloop = true
	offFp, err := DependencyFingerprint("arm64", platform, spec, dir, off.Deps)
	if err != nil {
		t.Fatalf("dependency fingerprint disabled: %v", err)
	}
	onFp, err := DependencyFingerprint("arm64", platform, specOn, dir, on.Deps)
	if err != nil {
		t.Fatalf("dependency fingerprint enabled: %v", err)
	}
	if offFp != onFp {
		t.Errorf("dependency fingerprint changed with the PYTHON_ZUVLOOP mode: %s vs %s", offFp, onFp)
	}
}

// TestBootstrapHashNeverCoversDependencyLayer pins the complementary boundary
// that keeps the label a pure app-image identity: the dependency layer's
// manifest set and install command are NOT hashed into the relay.bootstrap
// label, so a dependency-only change (or a zuvloop install next to unchanged
// dep identity) cannot leak the dependency image's content into the bootstrap
// label.
func TestBootstrapHashNeverCoversDependencyLayer(t *testing.T) {
	spec, err := lookup("python3.14")
	if err != nil {
		t.Fatalf("lookup python3.14: %v", err)
	}
	dir := zuvloopAppDir(t)
	_, on := zuvloopPlans(t, spec, dir, []string{"handler"})

	depless := on
	depless.Deps = plan.Deps{}
	hWithDeps, err := bootstrapHash(on, "amd64")
	if err != nil {
		t.Fatalf("bootstrapHash: %v", err)
	}
	hDepless, err := bootstrapHash(depless, "amd64")
	if err != nil {
		t.Fatalf("bootstrapHash depless: %v", err)
	}
	if hWithDeps != hDepless {
		t.Errorf("bootstrap label hash changed with the dependency layer: %s vs %s (deps must not participate)", hWithDeps, hDepless)
	}
}

// TestPythonZuvloopWorkerOptionPropagation pins the runtime half of the worker
// wiring: worker_runtime.openRuntimeManager passes
// runtime.WithPythonZuvloop(cfg.PythonZuvloop) into NewManager, whose
// constructor resolves options through the pure resolveManagerOptions and maps
// resolved.pythonZuvloop onto the manager field the prepare path reads. The
// resolver is the unit-testable seam (NewManager itself dials Docker at
// construction, so the worker wiring line is integration-gated; the Docker E2E
// suite passes the same option through the same NewManager).
func TestPythonZuvloopWorkerOptionPropagation(t *testing.T) {
	// A worker that never wires the knob keeps the standard asyncio behavior.
	if resolved := resolveManagerOptions(nil); resolved.pythonZuvloop {
		t.Error("default resolveManagerOptions.pythonZuvloop = true, want false")
	}

	// The resolved worker-global setting reaches the manager options verbatim.
	if resolved := resolveManagerOptions([]ManagerOption{WithPythonZuvloop(true)}); !resolved.pythonZuvloop {
		t.Error("WithPythonZuvloop(true) did not resolve pythonZuvloop = true")
	}
	if resolved := resolveManagerOptions([]ManagerOption{WithPythonZuvloop(false)}); resolved.pythonZuvloop {
		t.Error("WithPythonZuvloop(false) resolved pythonZuvloop = true (explicit false must not coerce)")
	}

	// The mode composes with HOT_RELOAD's option in either order: two
	// independent startup knobs must never clobber each other.
	bothOrders := [][]ManagerOption{
		{WithSourceMount(true), WithPythonZuvloop(true)},
		{WithPythonZuvloop(true), WithSourceMount(true)},
	}
	for i, opts := range bothOrders {
		resolved := resolveManagerOptions(opts)
		if !resolved.pythonZuvloop || !resolved.sourceMount {
			t.Errorf("options #%d resolved (pythonZuvloop=%v, sourceMount=%v), want both true", i, resolved.pythonZuvloop, resolved.sourceMount)
		}
	}

	// A nil option in the worker's list is ignored, not fatal (the resolver is
	// total over the option slice, matching the existing option conventions).
	if resolved := resolveManagerOptions([]ManagerOption{nil, WithPythonZuvloop(true), nil}); !resolved.pythonZuvloop {
		t.Error("WithPythonZuvloop(true) beside nil options did not resolve true")
	}
}

// TestPythonZuvloopSourceMountedTagIdentity pins the mode's behavior under
// HOT_RELOAD: the source-mounted app image's tag derives from the rendered
// Dockerfile (which carries the build-time RUN install commands verbatim), so
// an invoking app's mounted tag DIFFERS across modes — a flipped mode re-tags
// and can never reuse the other mode's image even before the label probe runs.
// A service-only app's rendered Dockerfile is identical in both modes, so its
// mounted tag is shared and flipping the setting must not rebuild it.
func TestPythonZuvloopSourceMountedTagIdentity(t *testing.T) {
	spec, err := lookup("python3.14")
	if err != nil {
		t.Fatalf("lookup python3.14: %v", err)
	}
	dir := zuvloopAppDir(t)

	t.Run("invoking app re-tags on a mode flip", func(t *testing.T) {
		off, on := zuvloopPlans(t, spec, dir, []string{"handler"})

		offDockerfile, err := renderDockerfileWithSource(off, false)
		if err != nil {
			t.Fatalf("render disabled dockerfile: %v", err)
		}
		// The rendered Dockerfile must show the mode's build steps: the
		// standard image installs only the OTel API.
		if strings.Contains(offDockerfile, "zuvloop") {
			t.Errorf("disabled-mode source-mounted Dockerfile must not install zuvloop:\n%s", offDockerfile)
		}

		onDockerfile, err := renderDockerfileWithSource(on, false)
		if err != nil {
			t.Fatalf("render enabled dockerfile: %v", err)
		}
		// The renderer emits the managed install verbatim as a RUN step, so the
		// pinned spec (single-quoted) must appear on its own RUN line.
		if want := "RUN uv pip install --system --no-cache 'zuvloop==0.0.17'"; !strings.Contains(onDockerfile, want) {
			t.Errorf("enabled-mode source-mounted Dockerfile must run the managed zuvloop install %q:\n%s", want, onDockerfile)
		}

		offFp := sourceMountImageFingerprint(spec.Name, "", offDockerfile)
		onFp := sourceMountImageFingerprint(spec.Name, "", onDockerfile)
		if offFp == onFp {
			t.Errorf("source-mounted image fingerprint must differ across PYTHON_ZUVLOOP modes (both %s)", offFp)
		}
		if got := ImageRef("hot-reload-app", onFp); got == ImageRef("hot-reload-app", offFp) {
			t.Errorf("source-mounted app image ref must differ across modes: %s", got)
		}
	})

	t.Run("service-only app shares one mounted tag", func(t *testing.T) {
		offSvc, onSvc := zuvloopPlans(t, spec, dir, nil)

		offDockerfile, err := renderDockerfileWithSource(offSvc, false)
		if err != nil {
			t.Fatalf("render service-only disabled dockerfile: %v", err)
		}
		onDockerfile, err := renderDockerfileWithSource(onSvc, false)
		if err != nil {
			t.Fatalf("render service-only enabled dockerfile: %v", err)
		}
		if offDockerfile != onDockerfile {
			t.Errorf("service-only Dockerfile must be identical across modes:\n disabled:\n%s\n enabled:\n%s", offDockerfile, onDockerfile)
		}
	})
}

// zuvloopReuseApp writes a Python app with an event handler and no dependency
// manifest, so a scripted Prepare exercises the pure bootstrap-identity
// decision (mode flip changes the relay.bootstrap label the reuse probe
// compares).
func zuvloopReuseApp(t *testing.T) (app.App, string, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "handler.py"), []byte("def completed(event, context):\n    print('ok')\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	fn := app.App{Name: "zuvloop-reuse", Dir: dir, Template: &app.Template{
		Runtime: "python3.14",
		Events:  []app.EventRule{{Handler: "handler.completed"}},
	}}

	spec, err := lookup("python3.14")
	if err != nil {
		t.Fatalf("lookup python3.14: %v", err)
	}
	eng, err := engineFor(spec)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	off := spec
	on := spec
	on.PythonZuvloop = true
	offPlan, err := eng.Plan(off, fn.Dir, templateHandlers(fn))
	if err != nil {
		t.Fatalf("plan disabled: %v", err)
	}
	onPlan, err := eng.Plan(on, fn.Dir, templateHandlers(fn))
	if err != nil {
		t.Fatalf("plan enabled: %v", err)
	}
	offHash, err := bootstrapHash(offPlan, arch)
	if err != nil {
		t.Fatalf("bootstrap hash disabled: %v", err)
	}
	onHash, err := bootstrapHash(onPlan, arch)
	if err != nil {
		t.Fatalf("bootstrap hash enabled: %v", err)
	}
	return fn, offHash, onHash
}

// TestPythonZuvloopModeGatesImageReuse pins the reuse decision the stale-image
// rule depends on, against a scripted daemon (no Docker):
//
//   - an enabled worker probes its MODE-SCOPED baked tag; an image sitting at
//     that ref but labeled for the OTHER mode (a pre-flip build or an older
//     bootstrap) must NOT be reused: Prepare leaves the reuse path and attempts
//     a build (which the scripted build route fails, proving the attempt);
//   - an enabled worker facing a correctly-labeled image at its mode tag reuses
//     it, and the handle carries the mode tag plus the RAW source fingerprint;
//   - a disabled worker probes and reuses the RAW source tag as before;
//   - a service-only app stays on the RAW source tag even under an enabled
//     worker (only an enabled INVOKING app is mode-scoped).
//
// Together with the digest helper pins, these cases reproduce the cross-worker
// tag-collision rule: two workers with different settings never retag one
// mutable reference back and forth.
func TestPythonZuvloopModeGatesImageReuse(t *testing.T) {
	fn, offHash, onHash := zuvloopReuseApp(t)
	fp, err := app.FingerprintApp(fn.Dir, fn.Template)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	rawImage := ImageRef(fn.Name, fp)
	zuvImage := ImageRef(fn.Name, zuvloopImageFingerprint(fp))
	if zuvImage == rawImage {
		t.Fatalf("enabled mode tag %s must differ from the raw source tag %s", zuvImage, rawImage)
	}

	t.Run("enabled worker does not reuse an other-mode image at its mode tag", func(t *testing.T) {
		// The image exists at the ENABLED worker's mode-scoped ref but carries
		// the other mode's bootstrap label. Reuse would accept it; the label
		// mismatch must push Prepare into the build path, which this scripted
		// route fails loudly.
		cli := newScriptedDockerClient(t,
			dockerRoute{
				method: http.MethodGet, path: "/images/" + zuvImage + "/json",
				body: imageInspectJSON(zuvImage, offHash),
			},
			buildRoute(nil),
		)
		m := newLifecycleManager(t, cli, context.Background())
		m.pythonZuvloop = true

		if _, err := m.Prepare(context.Background(), fn); err == nil {
			t.Fatal("an enabled worker must not reuse an image labeled for the other mode")
		} else if !strings.Contains(err.Error(), "boom") {
			t.Fatalf("Prepare error = %v, want the scripted BUILD failure (a failure on the inspect route would mean it never attempted the build)", err)
		}
	})

	t.Run("enabled worker reuses its mode-scoped image", func(t *testing.T) {
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/images/" + zuvImage + "/json", body: imageInspectJSON(zuvImage, onHash)},
		)
		m := newLifecycleManager(t, cli, context.Background())
		m.pythonZuvloop = true

		prepared, err := m.Prepare(context.Background(), fn)
		if err != nil {
			t.Fatalf("prepare reuse: %v", err)
		}
		// The handle resolves the MODE tag but keeps the raw source identity:
		// the tag distinguishes the two immutable images, the fingerprint stays
		// what the reconciler compares and persists.
		if prepared.Image != zuvImage {
			t.Errorf("Prepared.Image = %s, want the mode-scoped tag %s", prepared.Image, zuvImage)
		}
		if prepared.Fingerprint != fp {
			t.Errorf("Prepared.Fingerprint = %s, want the raw source fingerprint %s", prepared.Fingerprint, fp)
		}
		if prepared.Lease() == nil {
			t.Fatal("a reused enabled image must yield an admitted lease")
		}
	})

	t.Run("disabled worker reuses the raw source tag", func(t *testing.T) {
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/images/" + rawImage + "/json", body: imageInspectJSON(rawImage, offHash)},
		)
		m := newLifecycleManager(t, cli, context.Background())

		prepared, err := m.Prepare(context.Background(), fn)
		if err != nil {
			t.Fatalf("prepare reuse (disabled worker, disabled image): %v", err)
		}
		if prepared.Image != rawImage {
			t.Errorf("Prepared.Image = %s, want the unchanged raw source tag %s", prepared.Image, rawImage)
		}
		if prepared.Fingerprint != fp {
			t.Errorf("Prepared.Fingerprint = %s, want the raw source fingerprint %s", prepared.Fingerprint, fp)
		}
	})

	t.Run("service-only app stays on the raw source tag even when enabled", func(t *testing.T) {
		// Only the raw ref is scripted and there is NO build route: if the
		// enabled worker wrongly probed a mode-scoped tag for a service-only
		// app, the inspect would miss and the build would fail the prepare.
		svcFn, svcHash := zuvloopServiceOnlyApp(t)
		svcFp, err := app.FingerprintApp(svcFn.Dir, svcFn.Template)
		if err != nil {
			t.Fatalf("service-only fingerprint: %v", err)
		}
		svcRawImage := ImageRef(svcFn.Name, svcFp)

		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/images/" + svcRawImage + "/json", body: imageInspectJSON(svcRawImage, svcHash)},
		)
		m := newLifecycleManager(t, cli, context.Background())
		m.pythonZuvloop = true

		prepared, err := m.Prepare(context.Background(), svcFn)
		if err != nil {
			t.Fatalf("prepare reuse (enabled worker, service-only image): %v", err)
		}
		if prepared.Image != svcRawImage {
			t.Errorf("service-only Prepared.Image = %s, want the unchanged raw source tag %s", prepared.Image, svcRawImage)
		}
		if prepared.Fingerprint != svcFp {
			t.Errorf("service-only Prepared.Fingerprint = %s, want %s", prepared.Fingerprint, svcFp)
		}
	})
}

// zuvloopServiceOnlyApp writes a Python app with NO event/schedule handlers
// (an entrypoint service only), the shape the mode must never mode-scope, and
// returns it with its relay.bootstrap label hash (mode-independent for a
// service-only plan).
func zuvloopServiceOnlyApp(t *testing.T) (app.App, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "service.py"), []byte("def main():\n    pass\n"), 0o644); err != nil {
		t.Fatalf("write service source: %v", err)
	}
	fn := app.App{Name: "zuvloop-svc-reuse", Dir: dir, Template: &app.Template{
		Runtime:  "python3.14",
		Services: []app.Service{{Name: "web", Entrypoint: "service.py", Port: 3000, Replicas: 1}},
	}}
	return fn, bootstrapForTest(t, fn)
}

// TestPythonZuvloopModeTagSeparation pins the mode-scoped baked-tag helper with
// no Docker: the digest is deterministic for a given source, differs from the
// source fingerprint itself, and REMAINS distinct after ImageRef's 16-character
// tag truncation — which is only guaranteed because the FULL source fingerprint
// is hashed before truncation. Two workers with different settings therefore
// resolve two immutable tag names for the same app, never one retag-able name.
func TestPythonZuvloopModeTagSeparation(t *testing.T) {
	fp := strings.Repeat("ab", 32) // a plausible 64-hex source fingerprint

	digest := zuvloopImageFingerprint(fp)
	if digest != zuvloopImageFingerprint(fp) {
		t.Fatal("zuvloopImageFingerprint is not deterministic for the same source")
	}
	if digest == fp {
		t.Fatal("the mode digest must differ from the source fingerprint it derives from")
	}
	if len(digest) != 64 {
		t.Errorf("mode digest length = %d, want the full sha256 hex (64)",
			len(digest))
	}
	// Different sources must also stay distinct under the mode digest.
	if zuvloopImageFingerprint(fp) == zuvloopImageFingerprint(fp[:63]+"cd") {
		t.Fatal("two different source fingerprints must not collide under the mode digest")
	}
	if got, want := ImageRef("app", digest), ImageRef("app", fp); got == want {
		t.Fatalf("mode separation must survive ImageRef's tag truncation: %s == %s", got, want)
	}
}

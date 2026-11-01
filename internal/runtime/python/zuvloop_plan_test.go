package python

import (
	"bytes"
	"io/fs"
	"strings"
	"testing"

	"relay/internal/runtime/plan"
)

// zuvloopSpec is a spec copy with the worker-global PYTHON_ZUVLOOP decision
// stamped, exactly as Manager.prepare does for the Python engine only.
func zuvloopSpec(enabled bool) plan.Spec {
	spec := testSpecs[0]
	spec.PythonZuvloop = enabled
	return spec
}

// enabledMarkers are the observable differences the zuvloop-selected bootstrap
// must carry: the module import (so a missing package fails startup), the
// enabled selector, and the loop factory on asyncio.run.
const (
	zuvloopImportLine     = "import zuvloop\n"
	zuvloopEnableLine     = "ZUVLOOP_ENABLED = True\n"
	zuvloopFactoryCall    = "loop_factory=zuvloop.new_event_loop"
	zuvloopDisabledMarker = "ZUVLOOP_ENABLED = False"
)

// TestPlanZuvloopDisabledKeepsStandardBootstrap pins the disabled mode (the
// zero-value default): the plan's bootstrap is the embedded file byte-for-byte
// and the only managed install stays the OTel API. This is the historical
// behavior exactly.
func TestPlanZuvloopDisabledKeepsStandardBootstrap(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec plan.Spec
	}{
		{"zero-value spec keeps the default", testSpecs[0]},
		{"explicit false", zuvloopSpec(false)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := Engine{}.Plan(tc.spec, t.TempDir(), []string{"handler"})
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			if got, want := p.Install, []string{otelInstall}; len(got) != 1 || got[0] != want[0] {
				t.Errorf("Install = %v, want exactly the managed OTel install %v", got, want)
			}
			assertBootstrapFile(t, p, Bootstrap)
			if bytes.Contains(Bootstrap, []byte(zuvloopImportLine)) {
				t.Error("the disabled bootstrap must not import zuvloop even when a handler list is given")
			}
		})
	}
}

// TestPlanZuvloopEnabledWithHandlersSwitchesBootstrapAndInstall pins the
// enabled mode for an app with invocation handlers: the injected bootstrap is
// the zuvloop-selected variant and the pinned package joins the managed uv
// install in the app image. The rest of the plan is unchanged.
func TestPlanZuvloopEnabledWithHandlersSwitchesBootstrapAndInstall(t *testing.T) {
	// The Python engine sees only the handler-module list's EMPTINESS; the
	// modules come from BOTH event rules and schedules (see
	// runtime.templateHandlers), so each invocation shape must enable the loop.
	for _, handlers := range [][]string{
		{"handler"},
		{"jobs.report"},
		{"handler", "src.order"},
	} {
		t.Run("handlers="+strings.Join(handlers, ","), func(t *testing.T) {
			p, err := Engine{}.Plan(zuvloopSpec(true), t.TempDir(), handlers)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			wantBootstrap, err := bootstrapFor(true)
			if err != nil {
				t.Fatalf("bootstrapFor(true): %v", err)
			}
			assertBootstrapFile(t, p, wantBootstrap)

			wantInstall := []string{otelInstall, zuvloopInstall}
			if len(p.Install) != len(wantInstall) {
				t.Fatalf("Install = %v, want %v", p.Install, wantInstall)
			}
			for i := range wantInstall {
				if p.Install[i] != wantInstall[i] {
					t.Errorf("Install[%d] = %q, want %q", i, p.Install[i], wantInstall[i])
				}
			}
			// The install is the managed uv path with the EXACT pinned release.
			if want := "uv pip install --system --no-cache 'zuvloop==0.0.17'"; zuvloopInstall != want {
				t.Errorf("zuvloopInstall = %q, want the managed uv pin %q", zuvloopInstall, want)
			}
		})
	}
}

// TestPlanZuvloopAppliesUnderSourceMountedImages pins the HOT_RELOAD
// composition: the Python engine ignores the per-preparation SourceMounted
// decision (Python's dependency layout mounts the live source at the workdir
// with everything else unchanged), so the loop mode behaves exactly as in the
// baked-image path — an invoking app gets the enabled bootstrap and the managed
// install, and a service-only app keeps the standard bootstrap and installs
// nothing extra. A mode flip therefore re-fingerprints a mounted image through
// the same plan content, not a separate mount-specific path.
func TestPlanZuvloopAppliesUnderSourceMountedImages(t *testing.T) {
	sameModePlans := func(t *testing.T, spec plan.Spec, handlers []string) (mounted, baked plan.BuildPlan) {
		t.Helper()
		mountedSpec := spec
		mountedSpec.SourceMounted = true
		mounted, err := Engine{}.Plan(mountedSpec, t.TempDir(), handlers)
		if err != nil {
			t.Fatalf("plan mounted: %v", err)
		}
		baked, err = Engine{}.Plan(spec, t.TempDir(), handlers)
		if err != nil {
			t.Fatalf("plan baked: %v", err)
		}
		return mounted, baked
	}

	t.Run("invoking app keeps the enabled mode under mount", func(t *testing.T) {
		mounted, baked := sameModePlans(t, zuvloopSpec(true), []string{"handler"})

		wantBootstrap, err := bootstrapFor(true)
		if err != nil {
			t.Fatalf("bootstrapFor(true): %v", err)
		}
		assertBootstrapFile(t, mounted, wantBootstrap)
		wantInstall := []string{otelInstall, zuvloopInstall}
		if strings.Join(mounted.Install, "\x00") != strings.Join(wantInstall, "\x00") {
			t.Errorf("mounted Install = %v, want %v", mounted.Install, wantInstall)
		}

		// Mounting must not reshape the mode's plan at all: the engine treats
		// HOT_RELOAD as a create-time source bind for Python, never a build
		// change. Compare every plan field the builder consumes.
		if got, want := findBootstrapFile(mounted).Content, findBootstrapFile(baked).Content; !bytes.Equal(got, want) {
			t.Error("mounted enabled bootstrap differs from the baked enabled bootstrap")
		}
		if got, want := mounted.Install, baked.Install; strings.Join(got, "\x00") != strings.Join(want, "\x00") {
			t.Errorf("mounted Install = %v, want the baked plan's %v", got, want)
		}
		if got, want := strings.Join(mounted.Entrypoint, "\x00"), strings.Join(baked.Entrypoint, "\x00"); got != want {
			t.Errorf("mounted entrypoint = %v, want the baked plan's %v", mounted.Entrypoint, baked.Entrypoint)
		}
		if !mounted.Deps.Equal(baked.Deps) {
			t.Errorf("mounted Deps = %+v, want the baked plan's %+v", mounted.Deps, baked.Deps)
		}
		if mounted.WorkDir != baked.WorkDir || mounted.User != baked.User || mounted.UserSetup != baked.UserSetup ||
			mounted.SourceMountTarget != baked.SourceMountTarget {
			t.Errorf("mounted plan changed a non-bootstrap field: %+v vs %+v", mounted, baked)
		}
	})

	t.Run("service-only app keeps the standard mode under mount", func(t *testing.T) {
		mounted, baked := sameModePlans(t, zuvloopSpec(true), nil)

		assertBootstrapFile(t, mounted, Bootstrap)
		if len(mounted.Install) != 1 || mounted.Install[0] != otelInstall {
			t.Errorf("mounted service-only Install = %v, want only the managed OTel install", mounted.Install)
		}
		if got, want := findBootstrapFile(mounted).Content, findBootstrapFile(baked).Content; !bytes.Equal(got, want) {
			t.Error("mounted service-only bootstrap differs from the baked service-only bootstrap")
		}
	})
}

// TestPlanZuvloopEnabledSharedAssertions pins, on top of the bootstrap/install
// switch, that the enabled variant still contains the standard bootstrap body
// and that every other plan field is unchanged relative to the disabled plan.
func TestPlanZuvloopEnabledSharedAssertions(t *testing.T) {
	dir := t.TempDir()
	disabled, err := Engine{}.Plan(zuvloopSpec(false), dir, []string{"handler"})
	if err != nil {
		t.Fatalf("disabled plan: %v", err)
	}
	enabled, err := Engine{}.Plan(zuvloopSpec(true), dir, []string{"handler"})
	if err != nil {
		t.Fatalf("enabled plan: %v", err)
	}

	// The enabled bootstrap is the disabled one with ONLY the marker replaced:
	// everything else (protocol, handler resolution, env rotation, main) stays.
	enabledContent := enabledFileContent(t, enabled).String()
	if got, want := strings.Count(enabledContent, zuvloopEnableLine), 1; got != want {
		t.Errorf("enabled bootstrap has %d %q lines, want %d", got, zuvloopEnableLine, want)
	}
	if strings.Contains(enabledContent, zuvloopDisabledMarker) {
		t.Error("enabled bootstrap must not carry the disabled marker")
	}
	for _, want := range []string{zuvloopImportLine, zuvloopFactoryCall, "def main():", `asyncio.run(runtime_loop()`} {
		if !strings.Contains(enabledContent, want) {
			t.Errorf("enabled bootstrap must contain %q", want)
		}
	}

	if enabled.BaseImage != disabled.BaseImage || enabled.WorkDir != disabled.WorkDir ||
		enabled.TargetArch != disabled.TargetArch || enabled.User != disabled.User ||
		enabled.UserSetup != disabled.UserSetup || enabled.SourceMountTarget != disabled.SourceMountTarget {
		t.Errorf("enabled plan changed a non-bootstrap field: %+v vs %+v", enabled, disabled)
	}
	if got, want := enabled.Entrypoint, disabled.Entrypoint; strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("enabled entrypoint = %v, want %v", got, want)
	}
	if got, want := strings.Join(enabled.Env, "\x00"), strings.Join(disabled.Env, "\x00"); got != want {
		t.Errorf("enabled env = %v, want %v", enabled.Env, disabled.Env)
	}
	if got, want := len(enabled.RuntimeTools), len(disabled.RuntimeTools); got != want {
		t.Errorf("enabled runtime tools count = %d, want %d", got, want)
	}
}

// TestPlanZuvloopEnabledServiceOnlyKeepsStandardBootstrap pins the service-only
// boundary: an app with NO event or schedule handler keeps the byte-identical
// standard bootstrap and installs nothing extra even when the worker-global
// setting is enabled. The whole plan therefore equals the disabled plan, so the
// built image (and its relay.bootstrap label) is identical for a service-only
// app under either mode.
func TestPlanZuvloopEnabledServiceOnlyKeepsStandardBootstrap(t *testing.T) {
	for _, handlers := range [][]string{nil, {}} {
		t.Run("handlers-nil-and-empty", func(t *testing.T) {
			dir := t.TempDir()
			enabled, err := Engine{}.Plan(zuvloopSpec(true), dir, handlers)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			disabled, err := Engine{}.Plan(zuvloopSpec(false), dir, handlers)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			assertBootstrapFile(t, enabled, Bootstrap)
			if len(enabled.Install) != 1 || enabled.Install[0] != otelInstall {
				t.Errorf("service-only Install = %v, want only the managed OTel install", enabled.Install)
			}
			if got, want := enabled.Install, disabled.Install; strings.Join(got, "\x00") != strings.Join(want, "\x00") {
				t.Errorf("service-only Install = %v, want the disabled plan's %v", got, want)
			}
			if !bytes.Equal(findBootstrapFile(enabled).Content, findBootstrapFile(disabled).Content) {
				t.Error("service-only bootstrap must be byte-identical across modes")
			}
		})
	}
}

// TestPlanZuvloopNeverTouchesDeps pins that the loop mode is app-image build
// knowledge only: the user's dependency manifests, the dependency install
// command, and the install dir are IDENTICAL under both modes, and the pinned
// zuvloop package never enters the reusable dependency layer. Both the classic
// requirements.txt path and the native uv project path are pinned.
func TestPlanZuvloopNeverTouchesDeps(t *testing.T) {
	layouts := []struct {
		name  string
		files map[string]string
	}{
		{
			name:  "requirements.txt",
			files: map[string]string{"requirements.txt": "six==1.16.0\n"},
		},
		{
			name: "native uv project",
			files: map[string]string{
				"pyproject.toml": "[project]\nname = \"x\"\nversion = \"0.1.0\"\n",
				"uv.lock":        "version = 1\n",
			},
		},
	}
	for _, layout := range layouts {
		t.Run(layout.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, content := range layout.files {
				write(t, dir, name, content)
			}

			disabled, err := Engine{}.Plan(zuvloopSpec(false), dir, []string{"handler"})
			if err != nil {
				t.Fatalf("disabled plan: %v", err)
			}
			enabled, err := Engine{}.Plan(zuvloopSpec(true), dir, []string{"handler"})
			if err != nil {
				t.Fatalf("enabled plan: %v", err)
			}

			if !enabled.Deps.Equal(disabled.Deps) {
				t.Errorf("enabled Deps = %+v, want the disabled plan's %+v", enabled.Deps, disabled.Deps)
			}
			if strings.Contains(enabled.Deps.Install, "zuvloop") {
				t.Errorf("dependency install must not carry zuvloop, got %q", enabled.Deps.Install)
			}
			if strings.Contains(enabled.Deps.Install, "opentelemetry") {
				t.Errorf("dependency install must not carry the managed OTel API, got %q", enabled.Deps.Install)
			}
		})
	}
}

// TestBootstrapForVariantContract pins the marker-replacement boundary the
// engine's bootstrap selection relies on: the disabled variant is the embedded
// file byte-for-byte; the enabled variant replaces exactly the one build-time
// marker (so the replacement is unambiguous) and keeps everything else.
func TestBootstrapForVariantContract(t *testing.T) {
	disabled, err := bootstrapFor(false)
	if err != nil {
		t.Fatalf("bootstrapFor(false): %v", err)
	}
	if !bytes.Equal(disabled, bootstrap) {
		t.Error("bootstrapFor(false) must return the embedded bootstrap byte-for-byte")
	}

	// The embedded bootstrap must carry the marker exactly once, or the
	// engine's single-marker replacement would be ambiguous.
	if got := bytes.Count(bootstrap, []byte(zuvloopBootstrapMarker)); got != 1 {
		t.Fatalf("embedded bootstrap carries %d zuvloop markers, want exactly 1", got)
	}
	markerIndex := bytes.Index(bootstrap, []byte(zuvloopBootstrapMarker))

	enabled, err := bootstrapFor(true)
	if err != nil {
		t.Fatalf("bootstrapFor(true): %v", err)
	}
	if strings.Contains(string(enabled), zuvloopDisabledMarker) {
		t.Error("enabled bootstrap must not carry the disabled marker")
	}
	if got, want := bytes.Count(enabled, []byte("ZUVLOOP_ENABLED = True\n")), 1; got != want {
		t.Errorf("enabled bootstrap carries %d %q lines, want %d", got, "ZUVLOOP_ENABLED = True", want)
	}
	// The replacement must be surgical: splicing the enabled snippet into the
	// embedded file at the marker window must reproduce the enabled variant
	// byte-for-byte, so nothing outside the marker (comments included) moves.
	if got := bytes.Index(enabled, []byte(zuvloopBootstrapEnabled)); got != markerIndex {
		t.Fatalf("enabled snippet starts at %d in the enabled bootstrap, want the marker position %d", got, markerIndex)
	}
	wantEnabled := make([]byte, 0, len(bootstrap)-len(zuvloopBootstrapMarker)+len(zuvloopBootstrapEnabled))
	wantEnabled = append(wantEnabled, bootstrap[:markerIndex]...)
	wantEnabled = append(wantEnabled, zuvloopBootstrapEnabled...)
	wantEnabled = append(wantEnabled, bootstrap[markerIndex+len(zuvloopBootstrapMarker):]...)
	if !bytes.Equal(enabled, wantEnabled) {
		t.Errorf("enabled bootstrap differs outside the marker replacement (len %d, want %d)", len(enabled), len(wantEnabled))
	}
}

// assertBootstrapFile finds the plan's /relay/bootstrap.py file and requires its
// content to equal want (and the historical 0644 mode).
func assertBootstrapFile(t *testing.T, p plan.BuildPlan, want []byte) {
	t.Helper()
	got := findBootstrapFile(p)
	if got == nil {
		t.Fatalf("plan has no /relay/bootstrap.py file: %+v", p.Files)
	}
	if !bytes.Equal(got.Content, want) {
		t.Errorf("bootstrap content differs from the expected variant (len %d vs %d)", len(got.Content), len(want))
	}
	if got.Mode != fs.FileMode(0o644) {
		t.Errorf("bootstrap mode = %v, want 0644", got.Mode)
	}
	if got.Path != "/relay/bootstrap.py" {
		t.Errorf("bootstrap path = %q, want /relay/bootstrap.py", got.Path)
	}
}

// enabledFileContent returns the enabled plan's bootstrap content as a
// printable buffer for substring assertions.
func enabledFileContent(t *testing.T, p plan.BuildPlan) *bytes.Buffer {
	t.Helper()
	f := findBootstrapFile(p)
	if f == nil {
		t.Fatalf("plan has no /relay/bootstrap.py file: %+v", p.Files)
	}
	return bytes.NewBuffer(f.Content)
}

// findBootstrapFile returns the plan's /relay/bootstrap.py file, or nil.
func findBootstrapFile(p plan.BuildPlan) *plan.File {
	for i := range p.Files {
		if p.Files[i].Path == "/relay/bootstrap.py" {
			return &p.Files[i]
		}
	}
	return nil
}

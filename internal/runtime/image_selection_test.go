package runtime

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/moby/moby/client"

	"relay/internal/app"
	"relay/internal/runtime/plan"
	"relay/internal/testutil"
)

// Runtime image pins. A Manager carries one immutable startup snapshot of
// validated runtime base-image selections ("pins"); the worker provides it from
// the state database at startup, and the Manager does not poll or live-refresh
// it. A pinned runtime builds FROM <configured-ref>@<sha256:index>,
// folds the immutable identity into dependency fingerprints and app image
// identities, stamps provenance labels, and leaves every UNPINNED path
// byte-for-byte unchanged.

// pinnedDigestA/B are shape-valid, distinct multi-platform index digests
// ("sha256:" + 64 lowercase hex) used to prove identity separation without
// registry semantics.
var (
	pinnedDigestA = "sha256:" + repeatHex('a')
	pinnedDigestB = "sha256:" + repeatHex('0')
)

// repeatHex renders 64 repetitions of a character, so the rendered digest is
// exactly the persisted shape length (64 hex characters after "sha256:").
func repeatHex(c byte) string {
	out := make([]byte, 64)
	for i := range out {
		out[i] = c
	}
	return string(out)
}

// pinnedPythonSelection is a valid pin for the python3.14 runtime.
func pinnedPythonSelection(digest string) RuntimeImageSelection {
	return RuntimeImageSelection{
		RuntimeID:   "python3.14",
		SourceImage: "python:3.14-slim",
		Digest:      digest,
	}
}

// TestNewManagerRejectsInvalidRuntimeImageSelection pins the fail-closed
// startup contract: a malformed or mismatched selection for a runtime this
// binary knows fails manager construction, so Relay can never silently fall
// back to an unpinned moving tag for a selection it was explicitly given.
func TestNewManagerRejectsInvalidRuntimeImageSelection(t *testing.T) {
	cases := []struct {
		name      string
		selection RuntimeImageSelection
		// duplicate makes the scenario pass the same selection twice.
		duplicate bool
		wantInErr string
	}{
		{
			name:      "source image does not match the configured base image",
			selection: RuntimeImageSelection{RuntimeID: "python3.14", SourceImage: "python:other", Digest: pinnedDigestA},
			wantInErr: "does not match configured base image",
		},
		{
			name:      "malformed digest",
			selection: RuntimeImageSelection{RuntimeID: "python3.14", SourceImage: "python:3.14-slim", Digest: "sha256:abcd"},
			wantInErr: "sha256:<64 lowercase hex>",
		},
		{
			name:      "empty runtime ID",
			selection: RuntimeImageSelection{SourceImage: "python:3.14-slim", Digest: pinnedDigestA},
			wantInErr: "runtime ID is empty",
		},
		{
			name:      "duplicate selection for one runtime",
			selection: pinnedPythonSelection(pinnedDigestA),
			duplicate: true,
			wantInErr: "duplicate image selection",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isolatedTmpdir(t)
			selections := []RuntimeImageSelection{tc.selection}
			if tc.duplicate {
				selections = append(selections, tc.selection)
			}
			_, err := NewManager(
				testutil.DiscardLogger(), nil, "test-host",
				WithRuntimeImageSelections(selections), withPing(noopPing),
			)
			if err == nil {
				t.Fatal("NewManager accepted an invalid runtime image selection")
			}
			if !strings.Contains(err.Error(), tc.wantInErr) {
				t.Fatalf("NewManager error = %v, want it to mention %q", err, tc.wantInErr)
			}
			if !strings.Contains(err.Error(), "runtime image selection") {
				t.Fatalf("NewManager error = %v, want the runtime image selection context", err)
			}
		})
	}
}

// TestNewManagerCarriesValidSelectionAndIgnoresUnknownRuntime pins the startup
// wiring: a valid pin is carried and names the runtime's exact configured base
// with the selected index digest, while a pin for a runtime ID this binary does
// not know is ignored (a newer Relay sharing the database may have written it;
// no app here can select that runtime).
func TestNewManagerCarriesValidSelectionAndIgnoresUnknownRuntime(t *testing.T) {
	isolatedTmpdir(t)
	var logs testutil.SyncBuffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	m, err := NewManager(
		logger, nil, "test-host",
		WithRuntimeImageSelections([]RuntimeImageSelection{
			pinnedPythonSelection(pinnedDigestA),
			{RuntimeID: "python9.9", SourceImage: "python:9.9-slim", Digest: pinnedDigestB},
		}),
		withPing(noopPing),
	)
	if err != nil {
		t.Fatalf("NewManager with one valid and one unknown selection: %v", err)
	}
	defer m.Close()

	got := m.imageSelectionFor("python3.14")
	if got.sourceImage != "python:3.14-slim" || got.digest != pinnedDigestA {
		t.Fatalf("python3.14 selection = {source=%q digest=%q}, want source %q digest %q",
			got.sourceImage, got.digest, "python:3.14-slim", pinnedDigestA)
	}
	if want := "python:3.14-slim@" + pinnedDigestA; got.baseImage() != want {
		t.Fatalf("pinned build base = %q, want %q", got.baseImage(), want)
	}
	if got := m.imageSelectionFor("python9.9"); got.pinned() {
		t.Fatalf("unknown runtime selection = %+v, want it ignored", got)
	}
	if !strings.Contains(logs.String(), "ignoring image selection for an unknown runtime") ||
		!strings.Contains(logs.String(), "python9.9") {
		t.Fatalf("expected the unknown-runtime ignore debug line, logs:\n%s", logs.String())
	}
}

// TestNewManagerSnapshotIsImmutableAfterConstruction pins that the Manager
// copies the startup snapshot: a caller mutating the slice it passed cannot
// retroactively change what construction validated.
func TestNewManagerSnapshotIsImmutableAfterConstruction(t *testing.T) {
	isolatedTmpdir(t)
	selections := []RuntimeImageSelection{pinnedPythonSelection(pinnedDigestA)}
	m, err := NewManager(
		testutil.DiscardLogger(), nil, "test-host",
		WithRuntimeImageSelections(selections), withPing(noopPing),
	)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer m.Close()

	selections[0].Digest = pinnedDigestB
	if got := m.imageSelectionFor("python3.14").digest; got != pinnedDigestA {
		t.Fatalf("mutating the caller's slice changed the manager's pin to %q, want %q", got, pinnedDigestA)
	}
}

// TestManagerWithoutSelectionsIsUnpinnedEverywhere pins the no-rows startup
// state: absent or empty selections (the no-selected-rows case the worker
// reads) leave EVERY runtime unpinned, and a pin for one runtime never leaks to
// another.
func TestManagerWithoutSelectionsIsUnpinnedEverywhere(t *testing.T) {
	cases := []struct {
		name       string
		opts       []ManagerOption
		pinnedIDs  []string // runtimes that must carry the given pin
		unpinnedID []string // every other runtime must stay unpinned
	}{
		{"no option supplied", nil, nil, []string{"python3.14", "python3.15", "nodejs24.x", "nodejs26.x"}},
		{"empty nil list", []ManagerOption{WithRuntimeImageSelections(nil)},
			nil, []string{"python3.14", "python3.15", "nodejs24.x", "nodejs26.x"}},
		{"empty non-nil list", []ManagerOption{WithRuntimeImageSelections([]RuntimeImageSelection{})},
			nil, []string{"python3.14", "python3.15", "nodejs24.x", "nodejs26.x"}},
		{"selection for python3.14 only",
			[]ManagerOption{WithRuntimeImageSelections([]RuntimeImageSelection{pinnedPythonSelection(pinnedDigestA)})},
			[]string{"python3.14"}, []string{"python3.15", "nodejs24.x", "nodejs26.x"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isolatedTmpdir(t)
			m, err := NewManager(
				testutil.DiscardLogger(), nil, "test-host",
				append(tc.opts, withPing(noopPing))...)
			if err != nil {
				t.Fatalf("NewManager: %v", err)
			}
			defer m.Close()
			for _, id := range tc.pinnedIDs {
				if got := m.imageSelectionFor(id); !got.pinned() {
					t.Fatalf("%s selection = %+v, want the carried pin", id, got)
				}
			}
			for _, id := range tc.unpinnedID {
				if got := m.imageSelectionFor(id); got.pinned() {
					t.Fatalf("%s selection = %+v, want unpinned", id, got)
				}
			}
		})
	}
}

// TestImageIdentityWithSelectionSeparation pins the app-image identity fold:
// a pinned identity separates by digest, configured source, and dependency
// reference (one app reference can never be retagged for two pins), the same
// inputs are stable, and an unpinned fold is the identity function.
func TestImageIdentityWithSelectionSeparation(t *testing.T) {
	pinned := imageSelection{sourceImage: "python:3.14-slim", digest: pinnedDigestA}
	base := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	// Unpinned: byte-for-byte the historical identity, with or without a dep.
	if got := imageIdentityWithSelection(base, imageSelection{}, "relay-dep-x"); got != base {
		t.Fatalf("unpinned fold = %q, want the base unchanged", got)
	}
	if got := imageIdentityWithSelection(base, imageSelection{}, ""); got != base {
		t.Fatalf("unpinned fold without dep = %q, want the base unchanged", got)
	}

	same := imageIdentityWithSelection(base, pinned, "relay-dep-x")
	if again := imageIdentityWithSelection(base, pinned, "relay-dep-x"); same != again {
		t.Fatalf("identity not deterministic for the same pin: %s != %s", same, again)
	}

	different := map[string]string{
		"digest":         imageIdentityWithSelection(base, imageSelection{sourceImage: pinned.sourceImage, digest: pinnedDigestB}, "relay-dep-x"),
		"source image":   imageIdentityWithSelection(base, imageSelection{sourceImage: "python:3.15-slim", digest: pinned.digest}, "relay-dep-x"),
		"dependency ref": imageIdentityWithSelection(base, pinned, "relay-dep-y"),
		"source fp":      imageIdentityWithSelection("other-source-identity", pinned, "relay-dep-x"),
	}
	for what, got := range different {
		if got == same {
			t.Errorf("changing the %s must change the pinned image identity", what)
		}
	}
}

// alternateArch is an architecture guaranteed to differ from the build host's
// current one. A hardcoded variant (for example "amd64") is identical to arch
// on that host and makes the separation assertion vacuous — the failure seen
// on linux/amd64.
func alternateArch() string {
	if arch == "amd64" {
		return "arm64"
	}
	return "amd64"
}

// TestDependencyFingerprintSelectionSeparation pins the dependency-layer
// identity: the selected (sourceImage, digest) pin is part of the dependency
// content address, separate from the runtime/arch/platform inputs; the same
// inputs plus the same pin are stable; and an unpinned layer hashes exactly the
// historical inputs byte-for-byte.
func TestDependencyFingerprintSelectionSeparation(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "requirements.txt"), []byte("six==1.16.0\n"), 0o644); err != nil {
		t.Fatalf("write requirements: %v", err)
	}
	spec := pythonSpec()
	deps := pythonRequirementsDeps()
	snap := testDependencySnapshot(t, map[string]string{"requirements.txt": "six==1.16.0\n"})

	selA := imageSelection{sourceImage: "python:3.14-slim", digest: pinnedDigestA}
	selB := imageSelection{sourceImage: "python:3.14-slim", digest: pinnedDigestB}
	selOtherSource := imageSelection{sourceImage: "python:other-tag", digest: pinnedDigestA}

	// The same inputs plus the same pin are stable across calls and processes.
	got, err := dependencyFingerprintFrom(arch, platform, spec, deps, snap, selA)
	if err != nil {
		t.Fatalf("pinned fingerprint: %v", err)
	}
	again, err := dependencyFingerprintFrom(arch, platform, spec, deps, snap, selA)
	if err != nil {
		t.Fatalf("pinned fingerprint again: %v", err)
	}
	if got != again {
		t.Fatalf("pinned fingerprint not deterministic: %s != %s", got, again)
	}

	// Separation: digest, configured source, arch, and runtime are distinct
	// inputs; each must change the layer identity under equal other inputs.
	different := map[string]func() (string, error){
		"digest": func() (string, error) { return dependencyFingerprintFrom(arch, platform, spec, deps, snap, selB) },
		"source": func() (string, error) {
			return dependencyFingerprintFrom(arch, platform, spec, deps, snap, selOtherSource)
		},
		"arch": func() (string, error) {
			return dependencyFingerprintFrom(alternateArch(), platform, spec, deps, snap, selA)
		},
		"runtime": func() (string, error) {
			s := spec
			s.Name = "python3.15"
			return dependencyFingerprintFrom(arch, platform, s, deps, snap, selA)
		},
	}
	for name, call := range different {
		other, err := call()
		if err != nil {
			t.Fatalf("%s variant fingerprint: %v", name, err)
		}
		if other == got {
			t.Errorf("changing the %s must change the pinned dependency fingerprint", name)
		}
	}

	// Unpinned compatibility: a missing selection, a zero selection, and the
	// exported never-pinning path hash exactly the historical inputs.
	historical, err := dependencyFingerprintFrom(arch, platform, spec, deps, snap)
	if err != nil {
		t.Fatalf("historical fingerprint: %v", err)
	}
	zeroSelection, err := dependencyFingerprintFrom(arch, platform, spec, deps, snap, imageSelection{})
	if err != nil {
		t.Fatalf("zero-selection fingerprint: %v", err)
	}
	exported, err := DependencyFingerprint(arch, platform, spec, dir, deps)
	if err != nil {
		t.Fatalf("exported fingerprint: %v", err)
	}
	if historical != zeroSelection || zeroSelection != exported {
		t.Fatalf("unpinned fingerprints diverged: historical=%s zero-selection=%s exported=%s",
			historical, zeroSelection, exported)
	}
	if got == historical {
		t.Fatal("a pinned fingerprint must differ from the unpinned one for the same inputs")
	}
	// The content-addressed tag differs too, so a pin can never reuse an
	// unpinned shared layer (nor vice versa).
	if depImageRef(got) == depImageRef(historical) {
		t.Fatal("pinned and unpinned dependency references must differ")
	}
}

// TestManagerDependencyFingerprintFoldWithSelection pins the manager-level fold
// used when the dependency-fingerprint seam is injected: an unpinned selection
// returns the seam's result unchanged, and a pinned one folds the selected
// identity so a pinned layer still gets a distinct content address.
func TestManagerDependencyFingerprintFoldWithSelection(t *testing.T) {
	m := &Manager{log: testutil.DiscardLogger()}
	seamResult := strings.Repeat("1", 64)
	m.depFingerprint = func(_ string, _ string, _ plan.Spec, _ plan.Deps, _ dependencySnapshot) (string, error) {
		return seamResult, nil
	}
	sel := imageSelection{sourceImage: "python:3.14-slim", digest: pinnedDigestA}

	unpinned, err := m.dependencyFingerprint(
		"arm64", "linux", pythonSpec(), pythonRequirementsDeps(), dependencySnapshot{}, imageSelection{})
	if err != nil {
		t.Fatalf("unpinned seam fold: %v", err)
	}
	if unpinned != seamResult {
		t.Fatalf("unpinned seam fingerprint = %s, want the seam result unchanged %s", unpinned, seamResult)
	}

	pinned, err := m.dependencyFingerprint(
		"arm64", "linux", pythonSpec(), pythonRequirementsDeps(), dependencySnapshot{}, sel)
	if err != nil {
		t.Fatalf("pinned seam fold: %v", err)
	}
	if pinned == seamResult || pinned != foldDependencySelection(seamResult, sel) {
		t.Fatalf("pinned seam fingerprint = %s, want the selection fold %s",
			pinned, foldDependencySelection(seamResult, sel))
	}
}

// expectedPinnedDependencyRef derives the content-addressed dependency image
// reference an app CURRENTLY resolves to under a selection, with the exact
// production helpers (lookup -> engine Plan -> snapshot ->
// dependencyFingerprintFrom). It fails the test when the app declares no
// dependency layer.
func expectedPinnedDependencyRef(t *testing.T, fn app.App, sel imageSelection) string {
	t.Helper()
	if fn.Template == nil {
		t.Fatalf("function %q has no template", fn.Name)
	}
	spec, err := lookup(fn.Template.Runtime)
	if err != nil {
		t.Fatalf("lookup %q: %v", fn.Template.Runtime, err)
	}
	eng, err := engineFor(spec)
	if err != nil {
		t.Fatalf("engine for %q: %v", fn.Template.Runtime, err)
	}
	p, err := eng.Plan(spec, fn.Dir, templateHandlers(fn))
	if err != nil {
		t.Fatalf("plan %q: %v", fn.Name, err)
	}
	if p.Deps.IsZero() {
		t.Fatalf("function %q declares no dependency layer", fn.Name)
	}
	snap, err := snapshotDependency(fn.Dir, p.Deps)
	if err != nil {
		t.Fatalf("snapshot %q: %v", fn.Name, err)
	}
	defer snap.release()
	fp, err := dependencyFingerprintFrom(arch, platform, spec, p.Deps, snap, sel)
	if err != nil {
		t.Fatalf("dependency fingerprint %q: %v", fn.Name, err)
	}
	return depImageRef(fp)
}

// capturedBuild records one POST /build the scripted daemon observed: the
// tagged image, the query parameters (explicit target platform and label JSON),
// and the full context tar (the rendered Dockerfile).
type capturedBuild struct {
	tag     string
	labels  map[string]string
	tar     []byte
	request *http.Request
}

func (b capturedBuild) platform() string { return b.request.URL.Query().Get("platform") }

func (b capturedBuild) dockerfile(t *testing.T) string {
	t.Helper()
	return string(stagedFileBytes(t, b.tar, "Dockerfile"))
}

// newBuildCaptureScripts scripts a daemon whose images are all absent and whose
// builds all succeed, recording every build in call order.
func newBuildCaptureScripts(t *testing.T) (*client.Client, *[]capturedBuild) {
	t.Helper()
	builds := &[]capturedBuild{}
	cl := newScriptedDockerClient(t,
		dockerRoute{
			method: http.MethodGet, path: "/images/",
			status: http.StatusNotFound, body: `{"message":"no such image"}`,
		},
		dockerRoute{
			method: http.MethodPost, path: "/build", body: `{"stream":"ok"}`,
			onRequest: func(r *http.Request) {
				*builds = append(*builds, capturedBuild{request: r})
			},
			onBody: func(b []byte) {
				last := &(*builds)[len(*builds)-1]
				last.tar = append([]byte(nil), b...)
				last.tag = last.request.URL.Query().Get("t")
				if labels := last.request.URL.Query().Get("labels"); labels != "" {
					if err := json.Unmarshal([]byte(labels), &last.labels); err != nil {
						t.Fatalf("decode build labels %q: %v", labels, err)
					}
				}
			},
		},
	)
	return cl, builds
}

// newSelectionManager wires a scripted-daemon Manager that carries the
// validated selection snapshot (through the SAME resolver the production option
// runs), plus an optional manager tweak (source mount, zuvloop).
func newSelectionManager(t *testing.T, cl *client.Client, selections []RuntimeImageSelection, tweak func(*Manager)) *Manager {
	t.Helper()
	pins, _, err := resolveImageSelections(selections)
	if err != nil {
		t.Fatalf("resolve selections: %v", err)
	}
	m := newLifecycleManager(t, cl, context.Background())
	m.imageSelections = pins
	if tweak != nil {
		tweak(m)
	}
	return m
}

// nodeHandlerFixture stages a dependency-less Node handler app: template.yaml
// parsed exactly as the loader would produce it (so the handler participates in
// planning) plus the index.js source.
func nodeHandlerFixture(t *testing.T, name string) (string, app.App) {
	t.Helper()
	dir := t.TempDir()
	templateYAML := "runtime: nodejs24.x\nevents:\n  - handler: index.handler\n    pattern:\n      status: [COMPLETED]\n"
	writeFixtureFile(t, dir, "template.yaml", templateYAML)
	tmpl, err := app.ParseTemplate([]byte(templateYAML))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	writeNodeTestFile(t, dir, "index.js", "export function handler(event) { return 1; }\n")
	return dir, app.App{Name: name, Dir: dir, Template: tmpl}
}

// writeFixtureFile writes one fixture file into dir, failing the test on error.
func writeFixtureFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

// pythonHandlerFixture stages a Python handler app; withRequirements adds the
// dependency manifest so the app declares a dependency layer. The template is
// parsed exactly as the loader would produce it, so its handlers participate in
// planning.
func pythonHandlerFixture(t *testing.T, dir, name string, withRequirements bool) app.App {
	t.Helper()
	templateYAML := "runtime: python3.14\nevents:\n  - handler: handler.run\n    pattern:\n      status: [COMPLETED]\n"
	writeFixtureFile(t, dir, "template.yaml", templateYAML)
	tmpl, err := app.ParseTemplate([]byte(templateYAML))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	writeFixtureFile(t, dir, "handler.py", "def run(event):\n    print('ok')\n")
	if withRequirements {
		writeFixtureFile(t, dir, "requirements.txt", "six==1.16.0\n")
	}
	return app.App{Name: name, Dir: dir, Template: tmpl}
}

// preparePinned runs Prepare on a fresh scripted daemon/manager scenario and
// returns the prepared app plus every build issued, in order.
func preparePinned(
	t *testing.T,
	fn app.App,
	selections []RuntimeImageSelection,
	tweak func(*Manager),
) (*Prepared, []capturedBuild) {
	t.Helper()
	cl, captured := newBuildCaptureScripts(t)
	m := newSelectionManager(t, cl, selections, tweak)
	got, err := m.Prepare(context.Background(), fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	return got, *captured
}

// TestPreparePinnedBakedAppBuildsFromSelectionRef is the core pin build
// behavior for a baked (no-dependency) app: the pinned Dockerfile is FROM
// <configured-ref>@<sha256:index> with the EXPLICIT target platform unchanged,
// a different digest yields a different tag while Prepared.Fingerprint stays
// the source-only identity, and the build stamps the provenance labels without
// touching the ownership classification. The unpinned build remains the old tag
// form.
func TestPreparePinnedBakedAppBuildsFromSelectionRef(t *testing.T) {
	type scenario struct {
		name       string
		selections []RuntimeImageSelection
		wantFrom   string
		wantDigest string
	}
	scenarios := []scenario{
		{name: "unpinned", selections: nil, wantFrom: "FROM node:24-alpine\n", wantDigest: ""},
		{name: "pinned A", selections: []RuntimeImageSelection{
			{RuntimeID: "nodejs24.x", SourceImage: "node:24-alpine", Digest: pinnedDigestA}},
			wantFrom: "FROM node:24-alpine@" + pinnedDigestA + "\n", wantDigest: pinnedDigestA},
		{name: "pinned B", selections: []RuntimeImageSelection{
			{RuntimeID: "nodejs24.x", SourceImage: "node:24-alpine", Digest: pinnedDigestB}},
			wantFrom: "FROM node:24-alpine@" + pinnedDigestB + "\n", wantDigest: pinnedDigestB},
	}

	type outcome struct {
		image string
		fp    string
	}
	var unpinned, pinnedA, pinnedB outcome
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			dir, fn := nodeHandlerFixture(t, "pin-node")
			got, builds := preparePinned(t, fn, sc.selections, nil)
			if len(builds) != 1 {
				t.Fatalf("builds = %d, want exactly the app build", len(builds))
			}
			b := builds[0]

			// The Dockerfile is FROM the selected ref@digest, or the historical
			// tag when unpinned.
			if df := b.dockerfile(t); !strings.HasPrefix(df, sc.wantFrom) {
				t.Fatalf("Dockerfile = %q, want it to start with %q", df, sc.wantFrom)
			}
			// The explicit target platform remains on every build.
			if want := targetPlatformLabel(); b.platform() != want {
				t.Fatalf("build platform param = %q, want the explicit target %q", b.platform(), want)
			}

			// Provenance labels: pinned builds add the digest, every build
			// records runtime/source/platform, and relay.type stays the strict
			// ownership classification.
			labels := b.labels
			if got := labels[labelType]; got != ImageTypeApp {
				t.Errorf("relay.type = %q, want %q", got, ImageTypeApp)
			}
			if got := labels[labelApp]; got != fn.Name {
				t.Errorf("relay.app = %q, want %q", got, fn.Name)
			}
			if got := labels[labelProvenanceRuntime]; got != "nodejs24.x" {
				t.Errorf("relay.provenance.runtime = %q, want nodejs24.x", got)
			}
			if got := labels[labelProvenanceSourceImage]; got != "node:24-alpine" {
				t.Errorf("relay.provenance.source_image = %q, want the configured ref", got)
			}
			if got := labels[labelProvenancePlatform]; got != targetPlatformLabel() {
				t.Errorf("relay.provenance.platform = %q, want %q", got, targetPlatformLabel())
			}
			if sc.wantDigest == "" {
				if got, ok := labels[labelProvenanceDigest]; ok {
					t.Errorf("unpinned build must not carry %s, got %q", labelProvenanceDigest, got)
				}
			} else if got := labels[labelProvenanceDigest]; got != sc.wantDigest {
				t.Errorf("relay.provenance.digest = %q, want the selected digest %q", got, sc.wantDigest)
			}
			if got := labels[labelFingerprint]; got != fpOf(t, dir) {
				t.Errorf("relay.fingerprint = %q, want the source fingerprint", got)
			}

			// The unpinned reference IS the historical tag, byte-for-byte.
			if sc.wantDigest == "" && got.Image != ImageRef(fn.Name, fpOf(t, dir)) {
				t.Errorf("unpinned image = %q, want the historical %q", got.Image, ImageRef(fn.Name, fpOf(t, dir)))
			}

			switch sc.name {
			case "unpinned":
				unpinned = outcome{got.Image, got.Fingerprint}
			case "pinned A":
				pinnedA = outcome{got.Image, got.Fingerprint}
			case "pinned B":
				pinnedB = outcome{got.Image, got.Fingerprint}
			}
		})
	}

	// Identity separation across scenarios (sources are equal bytes everywhere).
	if unpinned.image == pinnedA.image {
		t.Errorf("a pin must change the app image tag: unpinned and pinned both %q", pinnedA.image)
	}
	if pinnedA.image == pinnedB.image {
		t.Errorf("different digests must produce different tags, both %q", pinnedA.image)
	}
	for _, o := range []outcome{unpinned, pinnedA, pinnedB} {
		if o.fp == "" || o.fp != unpinned.fp {
			t.Errorf("Prepared.Fingerprint must stay the source-only identity %q, got %q", unpinned.fp, o.fp)
		}
	}
}

// TestPreparePinnedDependencyBuildFromSelectionRef pins the dependency path:
// the DEPENDENCY image's Dockerfile is FROM the selected ref@digest, its
// content-addressed reference folds the pin (a different digest cannot reuse an
// unpinned layer), the app image builds FROM that dependency with both images
// carrying provenance, and Prepared.Fingerprint stays source-only.
func TestPreparePinnedDependencyBuildFromSelectionRef(t *testing.T) {
	sel := imageSelection{sourceImage: "python:3.14-slim", digest: pinnedDigestA}

	dir := t.TempDir()
	fn := pythonHandlerFixture(t, dir, "pin-dep", true)
	unpinnedRef := expectedPinnedDependencyRef(t, fn, imageSelection{})
	pinnedRef := expectedPinnedDependencyRef(t, fn, sel)
	if pinnedRef == unpinnedRef {
		t.Fatal("fixture precondition: the pinned dependency reference must differ from the unpinned one")
	}

	// When: a pinned manager prepares the dependency-bearing app.
	got, builds := preparePinned(t, fn, []RuntimeImageSelection{pinnedPythonSelection(pinnedDigestA)}, nil)
	if len(builds) != 2 {
		t.Fatalf("builds = %d, want the dependency build then the app build", len(builds))
	}
	depBuild, appBuild := builds[0], builds[1]

	// Then: the dependency build names the folded reference and is FROM the
	// selected multi-platform index digest; the app build is FROM the
	// dependency and tags with the pin-folded identity.
	if want := pinnedRef; depBuild.tag != want {
		t.Fatalf("dependency build tag = %q, want the content-addressed %q", depBuild.tag, want)
	}
	if want := "FROM python:3.14-slim@" + pinnedDigestA + "\n"; !strings.HasPrefix(depBuild.dockerfile(t), want) {
		t.Fatalf("dependency Dockerfile = %q, want FROM %q", depBuild.dockerfile(t), want)
	}
	if depBuild.platform() != targetPlatformLabel() {
		t.Fatalf("dependency build platform = %q, want %q", depBuild.platform(), targetPlatformLabel())
	}
	if want := "FROM " + pinnedRef + "\n"; !strings.HasPrefix(appBuild.dockerfile(t), want) {
		t.Fatalf("app Dockerfile = %q, want FROM the dependency %q", appBuild.dockerfile(t), want)
	}
	if appBuild.platform() != targetPlatformLabel() {
		t.Fatalf("app build platform = %q, want %q", appBuild.platform(), targetPlatformLabel())
	}

	// Then: the prepared identity carries the folded dependency and the
	// source-only fingerprint.
	if got.Dependency != pinnedRef {
		t.Fatalf("Prepared.Dependency = %q, want %q", got.Dependency, pinnedRef)
	}
	if want := fpOf(t, dir); got.Fingerprint != want {
		t.Fatalf("Prepared.Fingerprint = %q, want the source-only %q", got.Fingerprint, want)
	}

	// Then: both managed images carry their provenance and ownership labels.
	assertPythonDependencyLabels(t, depBuild.labels, pinnedRef)
	if got := depBuild.labels[labelProvenanceDigest]; got != pinnedDigestA {
		t.Errorf("dep relay.provenance.digest = %q, want the selected digest", got)
	}
	if got := appBuild.labels[labelType]; got != ImageTypeApp {
		t.Errorf("app relay.type = %q, want app", got)
	}
	if got := appBuild.labels[labelProvenanceDigest]; got != pinnedDigestA {
		t.Errorf("app relay.provenance.digest = %q, want the selected digest", got)
	}
	if got := appBuild.labels[labelProvenanceSourceImage]; got != "python:3.14-slim" {
		t.Errorf("app relay.provenance.source_image = %q, want the configured ref (never ref@digest)", got)
	}
}

// assertPythonDependencyLabels pins the managed dependency-image label set:
// the strict ownership classification (relay.type=dependency + relay.runtime +
// relay.fingerprint) plus the provenance fields.
func assertPythonDependencyLabels(t *testing.T, labels map[string]string, depRef string) {
	t.Helper()
	if got := labels[labelType]; got != ImageTypeDependency {
		t.Errorf("dep relay.type = %q, want %q", got, ImageTypeDependency)
	}
	if got := labels[labelRuntime]; got != "python3.14" {
		t.Errorf("dep relay.runtime = %q, want python3.14", got)
	}
	if got := labels[labelProvenanceRuntime]; got != "python3.14" {
		t.Errorf("dep relay.provenance.runtime = %q, want python3.14", got)
	}
	if got := labels[labelProvenanceSourceImage]; got != "python:3.14-slim" {
		t.Errorf("dep relay.provenance.source_image = %q, want the configured ref", got)
	}
	if got := labels[labelProvenancePlatform]; got != targetPlatformLabel() {
		t.Errorf("dep relay.provenance.platform = %q, want %q", got, targetPlatformLabel())
	}
	if want := strings.TrimPrefix(depRef, depRepoPrefix); !strings.HasPrefix(labels[labelFingerprint], want) {
		t.Errorf("dep relay.fingerprint = %q, want it consistent with the reference (prefix %q)",
			labels[labelFingerprint], want)
	}
}

// TestPreparePinnedHOTReloadImageBuildsFromSelectionRef pins HOT_RELOAD under a
// pin: the rendered (source-mounted) Dockerfile is FROM the selected ref@digest
// and the source-independent tag differs per pin while Prepared.Fingerprint
// remains the live source identity. Unpinned stays the historical tag form.
func TestPreparePinnedHOTReloadImageBuildsFromSelectionRef(t *testing.T) {
	hotReload := func(m *Manager) { m.sourceMount = true }
	type scenario struct {
		name       string
		selections []RuntimeImageSelection
		wantFrom   string
	}
	scenarios := []scenario{
		{name: "unpinned", selections: nil, wantFrom: "FROM node:24-alpine\n"},
		{name: "pinned A", selections: []RuntimeImageSelection{
			{RuntimeID: "nodejs24.x", SourceImage: "node:24-alpine", Digest: pinnedDigestA}},
			wantFrom: "FROM node:24-alpine@" + pinnedDigestA + "\n"},
		{name: "pinned B", selections: []RuntimeImageSelection{
			{RuntimeID: "nodejs24.x", SourceImage: "node:24-alpine", Digest: pinnedDigestB}},
			wantFrom: "FROM node:24-alpine@" + pinnedDigestB + "\n"},
	}

	type outcome struct {
		image string
		fp    string
	}
	var unpinned, pinnedA, pinnedB outcome
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			dir, fn := nodeHandlerFixture(t, "pin-hotreload")
			got, builds := preparePinned(t, fn, sc.selections, hotReload)
			if len(builds) != 1 {
				t.Fatalf("builds = %d, want exactly the source-mounted app build", len(builds))
			}
			if df := builds[0].dockerfile(t); !strings.HasPrefix(df, sc.wantFrom) {
				t.Fatalf("Dockerfile = %q, want it to start with %q", df, sc.wantFrom)
			}
			if want := fpOf(t, dir); got.Fingerprint != want {
				t.Fatalf("Prepared.Fingerprint = %q, want the source identity %q", got.Fingerprint, want)
			}
			if got.Image == ImageRef(fn.Name, got.Fingerprint) {
				t.Fatalf("HOT_RELOAD image %q must not be tagged by the source fingerprint", got.Image)
			}
			switch sc.name {
			case "unpinned":
				unpinned = outcome{got.Image, got.Fingerprint}
			case "pinned A":
				pinnedA = outcome{got.Image, got.Fingerprint}
			case "pinned B":
				pinnedB = outcome{got.Image, got.Fingerprint}
			}
		})
	}
	if unpinned.image == pinnedA.image {
		t.Errorf("a pin must change the HOT_RELOAD image tag: both %q", pinnedA.image)
	}
	if pinnedA.image == pinnedB.image {
		t.Errorf("different digests must give different HOT_RELOAD tags, both %q", pinnedA.image)
	}
	for _, o := range []outcome{unpinned, pinnedA, pinnedB} {
		if o.fp == "" || o.fp != unpinned.fp {
			t.Errorf("Prepared.Fingerprint must stay the source identity %q, got %q", unpinned.fp, o.fp)
		}
	}
}

// TestPreparePinnedZuvloopImageSeparatesByDigest pins the zuvloop invocation
// image under a pin: the mode-scoped tag folds the selected identity (a digest
// change yields a distinct tag) while Prepared.Fingerprint stays the source
// identity, and the Dockerfile is FROM the selected ref@digest.
func TestPreparePinnedZuvloopImageSeparatesByDigest(t *testing.T) {
	zuvloop := func(m *Manager) { m.pythonZuvloop = true }
	type scenario struct {
		name       string
		selections []RuntimeImageSelection
		wantFrom   string
	}
	scenarios := []scenario{
		{name: "unpinned", selections: nil, wantFrom: "FROM python:3.14-slim\n"},
		{name: "pinned A", selections: []RuntimeImageSelection{pinnedPythonSelection(pinnedDigestA)},
			wantFrom: "FROM python:3.14-slim@" + pinnedDigestA + "\n"},
		{name: "pinned B", selections: []RuntimeImageSelection{pinnedPythonSelection(pinnedDigestB)},
			wantFrom: "FROM python:3.14-slim@" + pinnedDigestB + "\n"},
	}

	type outcome struct {
		image string
		fp    string
	}
	var unpinned, pinnedA, pinnedB outcome
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			dir := t.TempDir()
			fn := pythonHandlerFixture(t, dir, "pin-zuvloop", false)
			got, builds := preparePinned(t, fn, sc.selections, zuvloop)
			if len(builds) != 1 {
				t.Fatalf("builds = %d, want exactly the zuvloop app build", len(builds))
			}
			if df := builds[0].dockerfile(t); !strings.HasPrefix(df, sc.wantFrom) {
				t.Fatalf("Dockerfile = %q, want it to start with %q", df, sc.wantFrom)
			}
			if want := fpOf(t, dir); got.Fingerprint != want {
				t.Fatalf("Prepared.Fingerprint = %q, want the source identity %q", got.Fingerprint, want)
			}
			switch sc.name {
			case "unpinned":
				unpinned = outcome{got.Image, got.Fingerprint}
			case "pinned A":
				pinnedA = outcome{got.Image, got.Fingerprint}
			case "pinned B":
				pinnedB = outcome{got.Image, got.Fingerprint}
			}
		})
	}
	if unpinned.image == pinnedA.image {
		t.Errorf("a pin must change the zuvloop image tag: both %q", pinnedA.image)
	}
	if pinnedA.image == pinnedB.image {
		t.Errorf("different digests must give different zuvloop tags, both %q", pinnedA.image)
	}
}

// TestProvenanceLabelsStampOwnershipClassificationUnchanged pins the label
// contract at the builder boundary: provenance is DIAGNOSTIC and merges into
// the managed app/dependency label sets without ever changing the strict
// ownership/GC classification fields (relay.type et al.); the configured source
// reference is recorded (never the pinned ref@digest form); and a nil label set
// is a no-op.
func TestProvenanceLabelsStampOwnershipClassificationUnchanged(t *testing.T) {
	sel := imageSelection{sourceImage: "python:3.14-slim", digest: pinnedDigestA}
	spec := pythonSpec()
	unpinned := provenanceFor(spec, imageSelection{})
	pinned := provenanceFor(spec, sel)

	if unpinned.digest != "" {
		t.Errorf("unpinned provenance digest = %q, want empty (omitted)", unpinned.digest)
	}
	if pinned.sourceImage != "python:3.14-slim" || pinned.digest != pinnedDigestA {
		t.Errorf("pinned provenance = {source=%q digest=%q}, want the configured source and the selected digest",
			pinned.sourceImage, pinned.digest)
	}
	if pinned.sourceImage == sel.baseImage() {
		t.Errorf("provenance source must record the configured ref %q, never the pinned form %q",
			"python:3.14-slim", sel.baseImage())
	}
	wantPlatform := targetPlatformLabel()
	if unpinned.platform != wantPlatform || pinned.platform != wantPlatform {
		t.Errorf("provenance platform = %q/%q, want %q on every build",
			unpinned.platform, pinned.platform, wantPlatform)
	}

	// Ownership classification is untouched for app images: relay.type=app,
	// relay.app, relay.fingerprint, relay.dependency, and relay.bootstrap all
	// survive the merge.
	appLabels := appImageLabels("events", "abc123", "relay-dep-x", "deadbeef")
	pinned.apply(appLabels)
	if appLabels[labelType] != ImageTypeApp || appLabels[labelApp] != "events" ||
		appLabels[labelFingerprint] != "abc123" || appLabels[labelDependency] != "relay-dep-x" ||
		appLabels[labelBootstrap] != "deadbeef" {
		t.Fatalf("provenance altered the app ownership labels: %v", appLabels)
	}

	// ... and for dependency images: relay.type=dependency + relay.runtime.
	depLabels := dependencyImageLabels("python3.14", "def456")
	pinned.apply(depLabels)
	if depLabels[labelType] != ImageTypeDependency || depLabels[labelRuntime] != "python3.14" ||
		depLabels[labelFingerprint] != "def456" {
		t.Fatalf("provenance altered the dependency ownership labels: %v", depLabels)
	}
	if depLabels[labelProvenanceDigest] != pinnedDigestA {
		t.Errorf("dep provenance digest = %q, want the selected digest", depLabels[labelProvenanceDigest])
	}

	// A nil label set never panics and never gains provenance.
	nilSafe := imageProvenance{runtimeName: spec.Name, digest: pinnedDigestA}
	nilSafe.apply(nil)
}

package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/app"
	"relay/internal/runtime/node"
	"relay/internal/runtime/plan"
)

// TestBootstrapHashCoversRuntimeMetadataFile pins that the relay.bootstrap
// label hashes the runtime metadata plan file. The document is a plan file
// like the bootstrap itself, so a base-image change (a pin refresh) whose ONLY
// content change is the metadata document must make an image built by an older
// selection report a stale bootstrap label instead of being reused under the
// same tag.
func TestBootstrapHashCoversRuntimeMetadataFile(t *testing.T) {
	dir := t.TempDir()
	planFor := func(base string) plan.BuildPlan {
		t.Helper()
		p, err := (node.Engine{}).Plan(plan.Spec{Name: "nodejs24.x", BaseImage: base}, dir, nil)
		if err != nil {
			t.Fatalf("plan: %v", err)
		}
		return p
	}
	unpinned := mustBootstrapHash(t, planFor("node:24-alpine"))
	pinned := mustBootstrapHash(t, planFor("node:24-alpine@sha256:"+strings.Repeat("a", 64)))
	if unpinned == pinned {
		t.Error("a runtime metadata content change (pin) must change the relay.bootstrap label hash")
	}
}

// TestPlanMetadataFileHashChangesPlainPlan pins metadata coverage at the
// bootstrap-hash input level: adding the metadata file to an otherwise bare
// plan changes the hash the label pins, exactly like any other injected file.
func TestPlanMetadataFileHashChangesPlainPlan(t *testing.T) {
	bare := plan.BuildPlan{
		Files: []plan.File{{Path: "/relay/bootstrap.py", Content: []byte("boot")}},
	}
	bareHash, err := bootstrapHash(bare, "amd64")
	if err != nil {
		t.Fatalf("bootstrapHash: %v", err)
	}
	metadata, err := plan.RuntimeMetadataFile(plan.Spec{Name: "python3.14", BaseImage: "python:3.14-slim"})
	if err != nil {
		t.Fatalf("RuntimeMetadataFile: %v", err)
	}
	withMetadata := bare
	withMetadata.Files = append([]plan.File{metadata}, bare.Files...)
	withHash, err := bootstrapHash(withMetadata, "amd64")
	if err != nil {
		t.Fatalf("bootstrapHash: %v", err)
	}
	if withHash == bareHash {
		t.Error("adding the runtime metadata plan file must change the bootstrap label hash")
	}
}

// TestPrepareStagesRuntimeMetadataRecordsPinnedBaseImage drives a real
// Manager.Prepare against a scripted daemon for a PINNED runtime and pins the
// metadata SOURCE accuracy at the manager level: the engine plans the
// selection's ref@digest (the pinned buildSpec), so the staged build context
// carries a runtime-metadata.json whose base_image is exactly the pinned
// reference and a Dockerfile building FROM it. An unpinned preparation of the
// same app stages the configured tag instead.
func TestPrepareStagesRuntimeMetadataRecordsPinnedBaseImage(t *testing.T) {
	const nodeRuntime = "nodejs24.x"
	pin := RuntimeImageSelection{
		RuntimeID:   nodeRuntime,
		SourceImage: "node:24-alpine",
		Digest:      pinnedDigestA,
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function handler(e, c) {}\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	fn := app.App{Name: "meta-pin", Dir: dir, Template: &app.Template{Runtime: nodeRuntime}}

	pinnedDockerfile, pinnedMetadata := prepareAndStage(t, fn, pin.Digest)
	from := dockerfileFromLine(t, pinnedDockerfile)
	want := "node:24-alpine@" + pinnedDigestA
	if from != want {
		t.Errorf("pinned Dockerfile FROM = %q, want the selected pinned reference %q", from, want)
	}
	var doc plan.RuntimeMetadata
	if err := json.Unmarshal(pinnedMetadata, &doc); err != nil {
		t.Fatalf("staged metadata is not JSON (%q): %v", pinnedMetadata, err)
	}
	if doc.Runtime != nodeRuntime || doc.BaseImage != want {
		t.Errorf("pinned metadata = %+v, want runtime %s base %s", doc, nodeRuntime, want)
	}

	unpinnedDockerfile, unpinnedMetadata := prepareAndStage(t, fn, "")
	if from := dockerfileFromLine(t, unpinnedDockerfile); from != "node:24-alpine" {
		t.Errorf("unpinned Dockerfile FROM = %q, want the configured base image", from)
	}
	if err := json.Unmarshal(unpinnedMetadata, &doc); err != nil {
		t.Fatalf("staged metadata is not JSON (%q): %v", unpinnedMetadata, err)
	}
	if doc.Runtime != nodeRuntime || doc.BaseImage != "node:24-alpine" {
		t.Errorf("unpinned metadata = %+v, want runtime %s base node:24-alpine", doc, nodeRuntime)
	}
	if string(unpinnedMetadata) == string(pinnedMetadata) {
		t.Error("pinned and unpinned preparations must stage different runtime metadata")
	}
}

// TestPrepareHOTReloadRebuildsOnPinChange proves the HOT_RELOAD rebuild
// behavior on a base-pin refresh: with the source unchanged a pin move changes
// both the rendered Dockerfile's FROM (the metadata COPY line it carries) and
// the staged metadata bytes, so the source-independent image fingerprint
// changes. The second preparation must therefore issue a NEW build under a new
// tag — and only the two selections: a later refresh back to the built pin
// reuses the matching image (builds stay at 3).
func TestPrepareHOTReloadRebuildsOnPinChange(t *testing.T) {
	store := &selectionMemoryStore{rows: []RuntimeImageSelection{{
		RuntimeID:   "nodejs24.x",
		SourceImage: "node:24-alpine",
		Digest:      pinnedDigestA,
	}}}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function handler(e, c) {}\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	fn := app.App{Name: "mount-pin", Dir: dir, Template: &app.Template{Runtime: "nodejs24.x"}}

	// The scripted image probe always reports the image ABSENT for the first
	// two preparations and PRESENT afterwards with the matching bootstrap label,
	// modeling a daemon that has both built images. The reuse probe reads the
	// relay.bootstrap label, so the third preparation must reuse (no build).
	bootstrapHashA := hotReloadBootstrapHash(t, fn.Dir, pinnedDigestA)
	buildCount := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{
			method: http.MethodGet, path: "/images/",
			serve: func(*http.Request) (int, string) {
				if buildCount < 2 {
					return http.StatusNotFound, `{"message":"no such image"}`
				}
				return 200, fmt.Sprintf(`{"Id":"sha256:mounted","Config":{"Labels":{%q:%q}}}`,
					labelBootstrap, bootstrapHashA)
			},
		},
		dockerRoute{
			method: http.MethodPost, path: "/build", body: `{"stream":"ok"}`,
			onMatch: func() { buildCount++ },
		},
	)
	m := newLifecycleManager(t, cli, context.Background())
	m.sourceMount = true
	m.selectionStore = store

	pins, _, err := resolveImageSelections(store.rows)
	if err != nil {
		t.Fatalf("resolve selections: %v", err)
	}
	m.imageSelections = pins

	first, err := m.Prepare(context.Background(), fn)
	if err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	if buildCount != 1 {
		t.Fatalf("builds after first prepare = %d, want 1", buildCount)
	}

	// Move the pin to digest B and refresh the Manager snapshot, mirroring the
	// reconciliation pass that picks up a re-selected digest.
	store.replace(RuntimeImageSelection{
		RuntimeID:   "nodejs24.x",
		SourceImage: "node:24-alpine",
		Digest:      pinnedDigestB,
	})
	if _, err := m.RefreshRuntimeImageSelections(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	second, err := m.Prepare(context.Background(), fn)
	if err != nil {
		t.Fatalf("second prepare: %v", err)
	}
	if buildCount != 2 {
		t.Fatalf("builds after pin change = %d, want a new build", buildCount)
	}
	if second.Image == first.Image {
		t.Fatalf("a pin change must move the HOT_RELOAD image tag: %q", second.Image)
	}
	if second.Fingerprint != first.Fingerprint {
		t.Error("a pin change must not change the app source fingerprint")
	}

	// Back to pin A: the matching image exists with the current bootstrap label,
	// so the preparation reuses it and issues no build.
	store.replace(RuntimeImageSelection{
		RuntimeID:   "nodejs24.x",
		SourceImage: "node:24-alpine",
		Digest:      pinnedDigestA,
	})
	if _, err := m.RefreshRuntimeImageSelections(context.Background()); err != nil {
		t.Fatalf("refresh back: %v", err)
	}
	again, err := m.Prepare(context.Background(), fn)
	if err != nil {
		t.Fatalf("prepare at pin A again: %v", err)
	}
	if buildCount != 2 {
		t.Fatalf("builds after returning to pin A = %d, want reuse", buildCount)
	}
	if again.Image != first.Image {
		t.Errorf("reused image = %q, want the pin-A image %q", again.Image, first.Image)
	}
}

// hotReloadBootstrapHash returns the relay.bootstrap label hash a HOT_RELOAD
// Node preparation computes for the app, selected at the given pinned digest.
// The reuse probe in TestPrepareHOTReloadRebuildsOnPinChange reports it.
func hotReloadBootstrapHash(t *testing.T, dir, digest string) string {
	t.Helper()
	spec, err := lookup("nodejs24.x")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if digest != "" {
		spec.BaseImage = EffectiveBaseImage("node:24-alpine", digest)
	}
	spec.SourceMounted = true
	p, err := (node.Engine{}).Plan(spec, dir, nil)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	return mustBootstrapHash(t, p)
}

// prepareAndStage prepares the app on a fresh pinned-or-unpinned manager and
// returns the staged Dockerfile and runtime-metadata.json bytes from the build
// context it sent to the daemon.
func prepareAndStage(t *testing.T, fn app.App, digest string) (string, []byte) {
	t.Helper()
	var contextTar []byte
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{
			method: http.MethodPost, path: "/build", body: `{"stream":"ok"}`,
			onBody: func(b []byte) { contextTar = append([]byte(nil), b...) },
		},
	)
	m := newLifecycleManager(t, cli, context.Background())
	if digest != "" {
		pins, warnings, err := resolveImageSelections([]RuntimeImageSelection{{
			RuntimeID:   fn.Template.Runtime,
			SourceImage: "node:24-alpine",
			Digest:      digest,
		}})
		if err != nil {
			t.Fatalf("resolve selections: %v", err)
		}
		if len(warnings) != 0 {
			t.Fatalf("unexpected selection warnings: %v", warnings)
		}
		m.imageSelections = pins
	}
	if _, err := m.Prepare(context.Background(), fn); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	return string(stagedFileBytes(t, contextTar, "Dockerfile")),
		stagedFileBytes(t, contextTar, "relay/runtime-metadata.json")
}

// dockerfileFromLine returns the rendered Dockerfile's first FROM base.
func dockerfileFromLine(t *testing.T, dockerfile string) string {
	t.Helper()
	for _, line := range strings.Split(dockerfile, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "FROM ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "FROM "))
		}
	}
	t.Fatalf("Dockerfile carries no FROM instruction:\n%s", dockerfile)
	return ""
}

package runtime

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/moby/moby/api/types/mount"

	"relay/internal/app"
	"relay/internal/runtime/node"
	"relay/internal/runtime/plan"
	pyengine "relay/internal/runtime/python"
	"relay/internal/testutil"
)

// stagedFileBytes returns the exact bytes of one file from a captured build
// context tar, or fails when the file is absent. It is how the SOURCE_MOUNT
// tests read the Dockerfile the builder actually generated.
func stagedFileBytes(t *testing.T, raw []byte, name string) []byte {
	t.Helper()
	tr := tar.NewReader(bytes.NewReader(raw))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read build context tar: %v", err)
		}
		if hdr.Name == name {
			b, rerr := io.ReadAll(tr)
			if rerr != nil {
				t.Fatalf("read %s from context tar: %v", name, rerr)
			}
			return b
		}
	}
	t.Fatalf("file %q not present in build context", name)
	return nil
}

// TestSourceMountImageFingerprint pins the app-image identity used when the
// source is not baked: it is deterministic, sensitive to every non-source input
// (runtime, dependency, rendered Dockerfile), and carries no source bytes.
func TestSourceMountImageFingerprint(t *testing.T) {
	base := sourceMountImageFingerprint("python3.14", "relay-dep-000", "FROM x\n")
	if base != sourceMountImageFingerprint("python3.14", "relay-dep-000", "FROM x\n") {
		t.Fatal("source-mount fingerprint is not deterministic")
	}
	if len(base) != 64 {
		t.Fatalf("fingerprint length = %d, want 64 hex chars", len(base))
	}
	for _, tc := range []struct {
		name, runtime, dep, dockerfile string
	}{
		{"runtime", "python3.15", "relay-dep-000", "FROM x\n"},
		{"dependency", "python3.14", "relay-dep-111", "FROM x\n"},
		{"dockerfile", "python3.14", "relay-dep-000", "FROM y\n"},
	} {
		if got := sourceMountImageFingerprint(tc.runtime, tc.dep, tc.dockerfile); got == base {
			t.Errorf("%s change must change the source-mount fingerprint", tc.name)
		}
	}
}

// TestBindMountsReadOnly pins that a source mount is one read-only bind, and
// that a nil/zero mount yields no Docker mount at all (the exact pre-SOURCE_MOUNT
// shape).
func TestBindMountsReadOnly(t *testing.T) {
	if got := bindMounts(nil); got != nil {
		t.Fatalf("bindMounts(nil) = %v, want nil", got)
	}
	if got := bindMounts(&SourceMount{}); got != nil {
		t.Fatalf("bindMounts(zero) = %v, want nil", got)
	}
	got := bindMounts(&SourceMount{HostPath: "/apps/a", Target: "/app", Identity: "fp"})
	if len(got) != 1 {
		t.Fatalf("bindMounts = %v, want exactly one mount", got)
	}
	m := got[0]
	if m.Type != mount.TypeBind || m.Source != "/apps/a" || m.Target != "/app" || !m.ReadOnly {
		t.Fatalf("mount = %+v, want read-only bind /apps/a -> /app", m)
	}
}

// TestBindMountsMasks pins that each declared mask becomes an empty, READ-ONLY
// tmpfs at its own target after the source bind, so a whole-directory bind
// cannot shadow an image-owned path (Node's /app/src/node_modules) and the mask
// adds no writable surface.
func TestBindMountsMasks(t *testing.T) {
	got := bindMounts(&SourceMount{
		HostPath: "/apps/a", Target: "/app/src",
		Masks: []string{"/app/src/node_modules", ""},
	})
	if len(got) != 2 {
		t.Fatalf("bindMounts = %+v, want the bind plus one tmpfs mask (empty entries skipped)", got)
	}
	if got[0].Type != mount.TypeBind || !got[0].ReadOnly {
		t.Fatalf("first mount = %+v, want the read-only source bind", got[0])
	}
	mask := got[1]
	if mask.Type != mount.TypeTmpfs || mask.Target != "/app/src/node_modules" || !mask.ReadOnly {
		t.Fatalf("mask = %+v, want a read-only tmpfs at /app/src/node_modules", mask)
	}
	if mask.TmpfsOptions == nil || mask.TmpfsOptions.Mode != 0o555 {
		t.Fatalf("mask options = %+v, want mode 0555", mask.TmpfsOptions)
	}
}

// TestWithSourceMountResolves pins the option plumbing: WithSourceMount(true)
// reaches the resolved manager options, and the default stays false.
func TestWithSourceMountResolves(t *testing.T) {
	if got := resolveManagerOptions(nil); got.sourceMount {
		t.Fatal("default sourceMount must be false")
	}
	if got := resolveManagerOptions([]ManagerOption{WithSourceMount(true)}); !got.sourceMount {
		t.Fatal("WithSourceMount(true) must enable sourceMount")
	}
}

// TestPrepareSourceMountBakesNoSourceAndRecordsMount drives a real Manager.Prepare
// build (scripted daemon) for a Python app with SOURCE_MOUNT enabled and proves
// the three defining behaviors: the image tag is NOT the source fingerprint, the
// generated Dockerfile never copies the source, and the live mount is recorded
// with the app dir, engine workdir, and source fingerprint.
func TestPrepareSourceMountBakesNoSourceAndRecordsMount(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "handler.py"), []byte("def handler(event):\n    return 1\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	fn := app.App{Name: "mounted", Dir: dir, Template: &app.Template{Runtime: "python3.14"}}

	var tag, labelsJSON string
	var contextTar []byte
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{
			method: http.MethodPost, path: "/build", body: `{"stream":"ok"}`,
			onRequest: func(r *http.Request) {
				tag = r.URL.Query().Get("t")
				labelsJSON = r.URL.Query().Get("labels")
			},
			onBody: func(b []byte) { contextTar = append([]byte(nil), b...) },
		},
	)
	m := newLifecycleManager(t, cli, context.Background())
	m.sourceMount = true

	got, err := m.Prepare(context.Background(), fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if got.Image != tag {
		t.Fatalf("built tag = %q, want the returned image %q", tag, got.Image)
	}
	if got.Image == ImageRef(fn.Name, got.Fingerprint) {
		t.Fatalf("SOURCE_MOUNT image %q must not be tagged by the source fingerprint", got.Image)
	}
	df := string(stagedFileBytes(t, contextTar, "Dockerfile"))
	if strings.Contains(df, "COPY . ") {
		t.Fatalf("SOURCE_MOUNT build must not copy the source into the image:\n%s", df)
	}
	var labels map[string]string
	if err := json.Unmarshal([]byte(labelsJSON), &labels); err != nil {
		t.Fatalf("decode image labels %q: %v", labelsJSON, err)
	}
	if v, ok := labels[labelFingerprint]; !ok || v != "" {
		t.Fatalf("SOURCE_MOUNT image must clear the relay.fingerprint label so an inherited dependency label cannot freeze the generation (got %v)", labels)
	}
	sm, ok := m.sourceMountFor(fn.Name)
	if !ok {
		t.Fatal("SOURCE_MOUNT prepare must record a source mount")
	}
	if sm.HostPath != dir || sm.Target != "/app" || sm.Identity != got.Fingerprint {
		t.Fatalf("recorded mount = %+v, want host=%q target=/app identity=%q", sm, dir, got.Fingerprint)
	}
	if sm.WorkDir != "" || len(sm.Masks) != 0 {
		t.Fatalf("a Python mount must not override cwd or mask paths, got %+v", sm)
	}
}

// TestPrepareSourceMountReusesImageAcrossSourceOnlyChange proves the
// source-independence in the strongest way: a source-only edit reuses the exact
// same image (no build), advances Prepared.Fingerprint, and updates the recorded
// mount identity so execution and service generations can recycle.
func TestPrepareSourceMountReusesImageAcrossSourceOnlyChange(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "handler.py"), []byte("def handler(event):\n    return 1\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	fn := app.App{Name: "mounted", Dir: dir, Template: &app.Template{Runtime: "python3.14"}}

	spec, err := lookup("python3.14")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	builtPlan, err := pyengine.Engine{}.Plan(spec, dir, nil)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	boot := bootstrapHash(builtPlan)

	builds := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{
			method: http.MethodGet, path: "/images/",
			body: fmt.Sprintf(`{"Id":"sha256:deadbeef","Config":{"Labels":{%q:%q}}}`, labelBootstrap, boot),
		},
		dockerRoute{
			method: http.MethodPost, path: "/build", body: `{"stream":"ok"}`,
			onMatch: func() { builds++ },
		},
	)
	m := newLifecycleManager(t, cli, context.Background())
	m.sourceMount = true

	first, err := m.Prepare(context.Background(), fn)
	if err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	if builds != 0 {
		t.Fatalf("first prepare built %d time(s), want a reuse of the existing image", builds)
	}

	// Source-only edit: the image tag must not change, but the fingerprint must.
	if err := os.WriteFile(filepath.Join(dir, "handler.py"), []byte("def handler(event):\n    return 2\n"), 0o644); err != nil {
		t.Fatalf("edit source: %v", err)
	}
	second, err := m.Prepare(context.Background(), fn)
	if err != nil {
		t.Fatalf("second prepare: %v", err)
	}
	if builds != 0 {
		t.Fatalf("source-only change rebuilt the image %d time(s); it must reuse", builds)
	}
	if second.Image != first.Image {
		t.Fatalf("source-only change moved the image tag: %q -> %q", first.Image, second.Image)
	}
	if second.Fingerprint == first.Fingerprint {
		t.Fatal("source-only change must advance the content fingerprint (generation)")
	}
	sm, ok := m.sourceMountFor(fn.Name)
	if !ok || sm.Identity != second.Fingerprint {
		t.Fatalf("recorded mount identity = %+v, want the new fingerprint %q", sm, second.Fingerprint)
	}
}

// TestStartExecutionContainerSerializesSourceMount drives the real
// startExecutionContainer client path and inspects the serialized create request,
// proving the SOURCE_MOUNT bind reaches Docker's HostConfig.Mounts as a
// read-only bind.
func TestStartExecutionContainerSerializesSourceMount(t *testing.T) {
	var createBody []byte
	cli := newScriptedDockerClient(t, dockerRoute{
		method: http.MethodPost, path: "/containers/create",
		status: http.StatusInternalServerError,
		body:   `{"message":"scripted create failure"}`,
		onBody: func(b []byte) { createBody = append([]byte(nil), b...) },
	})
	m := &Manager{cli: cli, log: testutil.DiscardLogger()}

	mounts := bindMounts(&SourceMount{HostPath: "/apps/fn", Target: "/app"})
	_, err := startExecutionContainer(context.Background(), m.cli, m.log,
		"fn", "img", nil, nil, app.ResourceLimits{}, RunMeta{}, "", mounts...)
	if err == nil {
		t.Fatal("expected the scripted create failure")
	}
	req := decodeCreateRequest(t, createBody)
	if req.HostConfig == nil || len(req.HostConfig.Mounts) != 1 {
		t.Fatalf("HostConfig.Mounts = %+v, want one source bind", req.HostConfig)
	}
	mm := req.HostConfig.Mounts[0]
	if mm.Type != mount.TypeBind || mm.Source != "/apps/fn" || mm.Target != "/app" || !mm.ReadOnly {
		t.Fatalf("mount = %+v, want read-only bind /apps/fn -> /app", mm)
	}
	if req.Config.WorkingDir != "" {
		t.Fatalf("WorkingDir = %q, want empty (preserve the image workdir) for a target-equals-workdir mount", req.Config.WorkingDir)
	}
}

// TestStartExecutionContainerSerializesNodeSourceMountConfig pins the Node
// SOURCE_MOUNT create shape on the wire: the app root work dir and the
// node_modules mask both reach Docker alongside the read-only source bind, so
// cwd sees /app/src and host modules can never shadow /app/node_modules.
func TestStartExecutionContainerSerializesNodeSourceMountConfig(t *testing.T) {
	var createBody []byte
	cli := newScriptedDockerClient(t, dockerRoute{
		method: http.MethodPost, path: "/containers/create",
		status: http.StatusInternalServerError,
		body:   `{"message":"scripted create failure"}`,
		onBody: func(b []byte) { createBody = append([]byte(nil), b...) },
	})
	m := &Manager{cli: cli, log: testutil.DiscardLogger()}

	workDir := "/app/src"
	mounts := bindMounts(&SourceMount{
		HostPath: "/apps/fn", Target: "/app/src", WorkDir: "/app/src",
		Masks: []string{"/app/src/node_modules"},
	})
	_, err := startExecutionContainer(context.Background(), m.cli, m.log,
		"fn", "img", nil, nil, app.ResourceLimits{}, RunMeta{}, workDir, mounts...)
	if err == nil {
		t.Fatal("expected the scripted create failure")
	}
	req := decodeCreateRequest(t, createBody)
	if req.Config.WorkingDir != "/app/src" {
		t.Fatalf("WorkingDir = %q, want /app/src", req.Config.WorkingDir)
	}
	if req.HostConfig == nil || len(req.HostConfig.Mounts) != 2 {
		t.Fatalf("HostConfig.Mounts = %+v, want the source bind plus the node_modules mask", req.HostConfig)
	}
	mask := req.HostConfig.Mounts[1]
	if mask.Type != mount.TypeTmpfs || mask.Target != "/app/src/node_modules" || !mask.ReadOnly {
		t.Fatalf("mask = %+v, want a read-only tmpfs at /app/src/node_modules", mask)
	}
}

// TestPrepareDependencyFingerprintFailureRemovesSnapshot pins that the
// dependency manifest snapshot's private root is removed when the fingerprint
// computation fails after a successful capture, so a failed preparation never
// leaks a relay-dep-build-* directory.
func TestPrepareDependencyFingerprintFailureRemovesSnapshot(t *testing.T) {
	base := isolatedTmpdir(t)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "handler.py"), []byte("def run(event):\n    return 1\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "requirements.txt"), []byte("six==1.16.0\n"), 0o644); err != nil {
		t.Fatalf("write requirements: %v", err)
	}
	fn := app.App{Name: "dep-fp-fail", Dir: dir, Template: &app.Template{Runtime: "python3.14"}}

	// No route is needed: the fingerprint failure returns before any Docker call.
	cli := newScriptedDockerClient(t)
	m := newLifecycleManager(t, cli, context.Background())
	m.depFingerprint = func(arch, platform string, spec plan.Spec, deps plan.Deps, snap dependencySnapshot) (string, error) {
		return "", errors.New("fingerprint boom")
	}

	if _, err := m.Prepare(context.Background(), fn); err == nil {
		t.Fatal("Prepare must surface the dependency fingerprint failure")
	}
	if name, leaked := leakedBuildContextTempDir(base); leaked {
		t.Fatalf("dependency snapshot %q leaked after a fingerprint failure", name)
	}
}

// TestPrepareSourceMountNodeUsesDistinctTarget pins the Node SOURCE_MOUNT
// boundary: a Node app under SOURCE_MOUNT IS mounted, but at a DISTINCT target
// (/app/src) so the image's /app/node_modules and persisted esbuild stay
// visible. The image is source-independent (no COPY, no relay.fingerprint label)
// and the mount is recorded with the distinct target, exactly like Python's
// /app recording.
func TestPrepareSourceMountNodeUsesDistinctTarget(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "handler.js"), []byte("export function handler(event) {}\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	// A host node_modules makes the mount record carry the bounded mask.
	if err := os.MkdirAll(filepath.Join(dir, "node_modules"), 0o755); err != nil {
		t.Fatalf("mkdir node_modules: %v", err)
	}
	fn := app.App{Name: "node-mount", Dir: dir, Template: &app.Template{Runtime: "node24"}}

	var tag, labelsJSON string
	var contextTar []byte
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{
			method: http.MethodPost, path: "/build", body: `{"stream":"ok"}`,
			onRequest: func(r *http.Request) {
				tag = r.URL.Query().Get("t")
				labelsJSON = r.URL.Query().Get("labels")
			},
			onBody: func(b []byte) { contextTar = append([]byte(nil), b...) },
		},
	)
	m := newLifecycleManager(t, cli, context.Background())
	m.sourceMount = true

	got, err := m.Prepare(context.Background(), fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if got.Image != tag {
		t.Fatalf("built tag = %q, want the returned image %q", tag, got.Image)
	}
	if got.Image == ImageRef(fn.Name, got.Fingerprint) {
		t.Fatalf("Node SOURCE_MOUNT image %q must not be tagged by the source fingerprint", got.Image)
	}
	df := string(stagedFileBytes(t, contextTar, "Dockerfile"))
	if strings.Contains(df, "COPY . ") {
		t.Fatalf("Node SOURCE_MOUNT build must not copy the source into the image:\n%s", df)
	}
	if !strings.Contains(df, "/relay/esbuild") {
		t.Fatalf("Node SOURCE_MOUNT image must persist esbuild for runtime transpilation:\n%s", df)
	}
	var labels map[string]string
	if err := json.Unmarshal([]byte(labelsJSON), &labels); err != nil {
		t.Fatalf("decode image labels %q: %v", labelsJSON, err)
	}
	if v, ok := labels[labelFingerprint]; !ok || v != "" {
		t.Fatalf("Node SOURCE_MOUNT image must clear the relay.fingerprint label so an inherited dependency label cannot freeze the generation (got %v)", labels)
	}
	sm, ok := m.sourceMountFor(fn.Name)
	if !ok {
		t.Fatal("Node SOURCE_MOUNT prepare must record a source mount")
	}
	if sm.HostPath != dir || sm.Target != "/app/src" || sm.Identity != got.Fingerprint {
		t.Fatalf("recorded mount = %+v, want host=%q target=/app/src identity=%q", sm, dir, got.Fingerprint)
	}
	if sm.Target == "/app" {
		t.Fatal("Node must not mount over /app: node_modules would be hidden")
	}
	if sm.WorkDir != "/app/src" {
		t.Fatalf("recorded mount work dir = %q, want /app/src so cwd sees the app root", sm.WorkDir)
	}
	if len(sm.Masks) != 1 || sm.Masks[0] != "/app/src/node_modules" {
		t.Fatalf("recorded mount masks = %v, want [/app/src/node_modules] so host node_modules cannot shadow the dependency image", sm.Masks)
	}
}

// TestServiceEntryForSourceMountRewritesNodePath pins the entry re-rooting for a
// mounted entrypoint service: a node command token under the baked workdir is
// re-rooted at the mount target AND the shared ESM resolve hook is preloaded, so
// a mounted Node service resolves bare imports from the dependency image. A
// Python module command and a workdir target are returned unchanged, and the
// input slice is never mutated.
func TestServiceEntryForSourceMountRewritesNodePath(t *testing.T) {
	nodeEntry := []string{"node", "/app/app/service.js"}
	got := serviceEntryForSourceMount(nodeEntry, "/app/src", "node24")
	if len(got) != 4 || got[0] != "node" || got[1] != "--import" || got[2] != node.ResolveHookPath || got[3] != "/app/src/app/service.js" {
		t.Fatalf("node entry = %v, want [node --import %s /app/src/app/service.js]", got, node.ResolveHookPath)
	}
	if nodeEntry[1] != "/app/app/service.js" {
		t.Fatal("the input entry must not be mutated")
	}
	if got := serviceEntryForSourceMount([]string{"python", "-m", "app.main"}, "/app/src", "python3.14"); len(got) != 3 || got[2] != "app.main" {
		t.Fatalf("python entry = %v, want it unchanged", got)
	}
	if got := serviceEntryForSourceMount(nodeEntry, "/app", "node24"); len(got) != 2 || got[1] != "/app/app/service.js" {
		t.Fatalf("a workdir target must be a no-op (no hook preload), got %v", got)
	}
	if got := serviceEntryForSourceMount(nil, "/app/src", "node24"); got != nil {
		t.Fatalf("nil entry = %v, want nil", got)
	}
}

// TestResolveServiceImageNodeRemapsEntryAtMountTarget pins the service
// integration: a recorded Node source mount re-roots the entry command at the
// mount target, preloads the shared resolve hook, and exposes the mount (whose
// Identity stamps relay.source).
func TestResolveServiceImageNodeRemapsEntryAtMountTarget(t *testing.T) {
	m := &Manager{log: testutil.DiscardLogger()}
	m.setSourceMount("fn", SourceMount{HostPath: "/apps/fn", Target: "/app/src", WorkDir: "/app/src", Masks: []string{"/app/src/node_modules"}, Identity: "fp1"})

	tmpl := &app.Template{Runtime: "node24"}
	svc := app.Service{Name: "svc", Entrypoint: "app/service.js", Port: 3000}
	img, err := m.ResolveServiceImage(context.Background(), "fn", tmpl, svc, "relay-app-fn:tag")
	if err != nil {
		t.Fatalf("ResolveServiceImage: %v", err)
	}
	if img.Mount == nil || img.Mount.Target != "/app/src" || img.Mount.Identity != "fp1" {
		t.Fatalf("mount = %+v, want the recorded /app/src mount", img.Mount)
	}
	if img.Mount.WorkDir != "/app/src" {
		t.Fatalf("mount work dir = %q, want /app/src so the service runs at the app root", img.Mount.WorkDir)
	}
	if len(img.Entry) != 4 || img.Entry[0] != "node" || img.Entry[1] != "--import" || img.Entry[2] != node.ResolveHookPath || img.Entry[3] != "/app/src/app/service.js" {
		t.Fatalf("entry = %v, want [node --import %s /app/src/app/service.js]", img.Entry, node.ResolveHookPath)
	}
}

// TestResolveServiceImageNodeWithoutMountHasNoHook pins that a Node entrypoint
// service with no recorded source mount keeps the historical baked entry: no
// re-root and no resolve-hook preload (the hook file exists only in a
// source-mounted image).
func TestResolveServiceImageNodeWithoutMountHasNoHook(t *testing.T) {
	m := &Manager{log: testutil.DiscardLogger()}

	tmpl := &app.Template{Runtime: "node24"}
	svc := app.Service{Name: "svc", Entrypoint: "app/service.js", Port: 3000}
	img, err := m.ResolveServiceImage(context.Background(), "fn", tmpl, svc, "relay-app-fn:tag")
	if err != nil {
		t.Fatalf("ResolveServiceImage: %v", err)
	}
	if img.Mount != nil {
		t.Fatalf("mount = %+v, want nil without a recorded source mount", img.Mount)
	}
	if len(img.Entry) != 2 || img.Entry[0] != "node" || img.Entry[1] != "/app/app/service.js" {
		t.Fatalf("entry = %v, want [node /app/app/service.js]", img.Entry)
	}
}

// TestResolveServiceImagePythonMountKeepsEntry pins that a Python mount (target
// /app) leaves the module entry unchanged.
func TestResolveServiceImagePythonMountKeepsEntry(t *testing.T) {
	m := &Manager{log: testutil.DiscardLogger()}
	m.setSourceMount("fn", SourceMount{HostPath: "/apps/fn", Target: "/app", Identity: "fp1"})

	tmpl := &app.Template{Runtime: "python3.14"}
	svc := app.Service{Name: "svc", Entrypoint: "app/main.py", Port: 8000}
	img, err := m.ResolveServiceImage(context.Background(), "fn", tmpl, svc, "relay-app-fn:tag")
	if err != nil {
		t.Fatalf("ResolveServiceImage: %v", err)
	}
	if len(img.Entry) != 3 || img.Entry[0] != "python" || img.Entry[1] != "-m" || img.Entry[2] != "app.main" {
		t.Fatalf("entry = %v, want [python -m app.main]", img.Entry)
	}
}

// TestStartServiceSerializesSourceMountAndLabel proves StartService sends the
// bind and stamps relay.source for a SOURCE_MOUNT entrypoint service, and sends
// neither when the spec has no mount.
func TestStartServiceSerializesSourceMountAndLabel(t *testing.T) {
	withMount := decodeCreateRequest(t, captureServiceCreate(t, ServiceSpec{
		App: "fn", Name: "svc", SourceRef: "service.py", Port: 8000,
		Image: "relay-app-fn:tag", Entry: []string{"python", "-u", "/relay/bootstrap.py"},
		Env:         []string{"PORT=8000"},
		SourceMount: &SourceMount{HostPath: "/apps/fn", Target: "/app", Identity: "fp1"},
	}))
	if withMount.HostConfig == nil || len(withMount.HostConfig.Mounts) != 1 {
		t.Fatalf("HostConfig.Mounts = %+v, want one source bind", withMount.HostConfig)
	}
	mm := withMount.HostConfig.Mounts[0]
	if mm.Type != mount.TypeBind || mm.Source != "/apps/fn" || mm.Target != "/app" || !mm.ReadOnly {
		t.Fatalf("mount = %+v, want read-only bind /apps/fn -> /app", mm)
	}
	if got := withMount.Config.Labels[labelSource]; got != "fp1" {
		t.Fatalf("relay.source = %q, want fp1", got)
	}
	if withMount.Config.WorkingDir != "" {
		t.Fatalf("WorkingDir = %q, want empty for a mount with no work-dir override", withMount.Config.WorkingDir)
	}

	// A Node mount re-roots the source at /app/src, runs the service with the
	// app root as its working directory, and preloads the shared ESM resolve
	// hook; the node_modules mask keeps host modules from shadowing the
	// dependency image.
	nodeMount := decodeCreateRequest(t, captureServiceCreate(t, ServiceSpec{
		App: "fn", Name: "svc", SourceRef: "service.js", Port: 3000,
		Image: "relay-app-fn:tag", Entry: []string{"node", "--import", node.ResolveHookPath, "/app/src/service.js"},
		Env: []string{"PORT=3000"},
		SourceMount: &SourceMount{
			HostPath: "/apps/fn", Target: "/app/src", WorkDir: "/app/src",
			Masks: []string{"/app/src/node_modules"}, Identity: "fp1",
		},
	}))
	if nodeMount.Config.WorkingDir != "/app/src" {
		t.Fatalf("WorkingDir = %q, want /app/src", nodeMount.Config.WorkingDir)
	}
	if len(nodeMount.Config.Entrypoint) != 4 || nodeMount.Config.Entrypoint[1] != "--import" || nodeMount.Config.Entrypoint[2] != node.ResolveHookPath {
		t.Fatalf("Entrypoint = %v, want the shared resolve hook preloaded", nodeMount.Config.Entrypoint)
	}
	if nodeMount.HostConfig == nil || len(nodeMount.HostConfig.Mounts) != 2 {
		t.Fatalf("HostConfig.Mounts = %+v, want the source bind plus the node_modules mask", nodeMount.HostConfig)
	}
	if mask := nodeMount.HostConfig.Mounts[1]; mask.Type != mount.TypeTmpfs || mask.Target != "/app/src/node_modules" {
		t.Fatalf("mask = %+v, want a tmpfs at /app/src/node_modules", mask)
	}

	withoutMount := decodeCreateRequest(t, captureServiceCreate(t, ServiceSpec{
		App: "fn", Name: "svc", SourceRef: "service.py", Port: 8000,
		Image: "relay-app-fn:tag", Entry: []string{"python", "-u", "/relay/bootstrap.py"},
		Env: []string{"PORT=8000"},
	}))
	if len(withoutMount.HostConfig.Mounts) != 0 {
		t.Fatalf("a service without a source mount must have no binds: %+v", withoutMount.HostConfig.Mounts)
	}
	if _, ok := withoutMount.Config.Labels[labelSource]; ok {
		t.Fatal("a service without a source mount must not carry relay.source")
	}
}

// buildCounts tallies the app and dependency image builds a scripted daemon
// observed, classified by the build tag's namespace. Counting them separately is
// what lets a test prove a source-only edit never issued a dependency build.
type buildCounts struct {
	dep int
	app int
}

// countBuildsRoute returns a POST /build route that tallies each build by its
// dependency-prefixed or app-prefixed tag and optionally captures the last
// dependency and app build contexts (keyed by the tag's namespace).
func countBuildsRoute(c *buildCounts, depCtx, appCtx *[]byte) dockerRoute {
	var lastTag string
	return dockerRoute{
		method: http.MethodPost, path: "/build", body: `{"stream":"ok"}`,
		onRequest: func(r *http.Request) {
			lastTag = r.URL.Query().Get("t")
			if strings.HasPrefix(lastTag, depRepoPrefix) {
				c.dep++
			} else {
				c.app++
			}
		},
		onBody: func(b []byte) {
			if strings.HasPrefix(lastTag, depRepoPrefix) {
				if depCtx != nil {
					*depCtx = append([]byte(nil), b...)
				}
			} else if appCtx != nil {
				*appCtx = append([]byte(nil), b...)
			}
		},
	}
}

// TestPrepareSourceMountDependencyChangeBuildsNewGeneration pins the
// SOURCE_MOUNT dependency lifecycle: mutating the manifest yields a DIFFERENT
// dependency fingerprint/reference and a NEW dependency image generation (built
// from the edited bytes), and the app image is rebuilt FROM that new dependency
// reference. Dependency and app builds are counted separately, so the new
// generation cannot be masked by a source-only reuse.
func TestPrepareSourceMountDependencyChangeBuildsNewGeneration(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "handler.py"), []byte("def handler(event):\n    return 1\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "requirements.txt"), []byte("six==1.16.0\n"), 0o644); err != nil {
		t.Fatalf("write requirements: %v", err)
	}
	fn := app.App{Name: "dep-change", Dir: dir, Template: &app.Template{Runtime: "python3.14"}}

	var first buildCounts
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		countBuildsRoute(&first, nil, nil),
	)
	m := newLifecycleManager(t, cli, context.Background())
	m.sourceMount = true
	preparedFirst, err := m.Prepare(context.Background(), fn)
	if err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	if preparedFirst.Dependency == "" {
		t.Fatal("expected the app to build FROM a dependency image")
	}
	if first.dep != 1 || first.app != 1 {
		t.Fatalf("first prepare builds = dep %d app %d, want 1 and 1", first.dep, first.app)
	}
	depFirst := preparedFirst.Dependency
	imageFirst := preparedFirst.Image

	// Mutate the manifest: a dependency-blocking change.
	if err := os.WriteFile(filepath.Join(dir, "requirements.txt"), []byte("six==1.17.0\n"), 0o644); err != nil {
		t.Fatalf("edit requirements: %v", err)
	}

	var second buildCounts
	var depCtx, appCtx []byte
	cli2 := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		countBuildsRoute(&second, &depCtx, &appCtx),
	)
	m2 := newLifecycleManager(t, cli2, context.Background())
	m2.sourceMount = true
	preparedSecond, err := m2.Prepare(context.Background(), fn)
	if err != nil {
		t.Fatalf("second prepare: %v", err)
	}
	if preparedSecond.Dependency == depFirst {
		t.Fatalf("manifest change kept the dependency reference %q; want a new generation", depFirst)
	}
	if preparedSecond.Image == imageFirst {
		t.Fatalf("manifest change kept the app image %q; want a rebuild FROM the new dependency", imageFirst)
	}
	if second.dep != 1 || second.app != 1 {
		t.Fatalf("manifest change builds = dep %d app %d, want 1 and 1 (a new generation is BUILT)", second.dep, second.app)
	}
	if got := string(stagedFileBytes(t, depCtx, "requirements.txt")); !strings.Contains(got, "six==1.17.0") {
		t.Fatalf("dependency build staged %q, want the edited manifest six==1.17.0", got)
	}
	df := string(stagedFileBytes(t, appCtx, "Dockerfile"))
	if !strings.Contains(df, "FROM "+preparedSecond.Dependency+"\n") {
		t.Fatalf("app image must build FROM the new dependency reference:\n%s", df)
	}
}

// TestPrepareSourceMountSourceOnlyChangeDoesNotBuildDependency is the deterministic
// counterpart: after a SOURCE-ONLY edit, both the dependency fingerprint/reference
// and the app image are unchanged and NEITHER a dependency nor an app build is
// issued. It counts dependency builds separately from app builds so a source edit
// can never be mistaken for a dependency rebuild.
func TestPrepareSourceMountSourceOnlyChangeDoesNotBuildDependency(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "handler.py"), []byte("def handler(event):\n    return 1\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "requirements.txt"), []byte("six==1.16.0\n"), 0o644); err != nil {
		t.Fatalf("write requirements: %v", err)
	}
	fn := app.App{Name: "source-only", Dir: dir, Template: &app.Template{Runtime: "python3.14"}}

	var first buildCounts
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		countBuildsRoute(&first, nil, nil),
	)
	m := newLifecycleManager(t, cli, context.Background())
	m.sourceMount = true
	preparedFirst, err := m.Prepare(context.Background(), fn)
	if err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	if preparedFirst.Dependency == "" {
		t.Fatal("expected the app to build FROM a dependency image")
	}
	if first.dep != 1 || first.app != 1 {
		t.Fatalf("first prepare builds = dep %d app %d, want 1 and 1", first.dep, first.app)
	}

	// Source-only edit.
	if err := os.WriteFile(filepath.Join(dir, "handler.py"), []byte("def handler(event):\n    return 2\n"), 0o644); err != nil {
		t.Fatalf("edit source: %v", err)
	}

	// The dependency image and the (source-independent) app image are both present
	// with the current bootstrap label, so Prepare must reuse them.
	spec, err := lookup("python3.14")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	builtPlan, err := pyengine.Engine{}.Plan(spec, dir, nil)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	present := fmt.Sprintf(`{"Id":"sha256:deadbeef","Config":{"Labels":{%q:%q}}}`, labelBootstrap, bootstrapHash(builtPlan))

	var second buildCounts
	cli2 := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", body: present},
		countBuildsRoute(&second, nil, nil),
	)
	m2 := newLifecycleManager(t, cli2, context.Background())
	m2.sourceMount = true
	preparedSecond, err := m2.Prepare(context.Background(), fn)
	if err != nil {
		t.Fatalf("second prepare: %v", err)
	}
	if second.dep != 0 || second.app != 0 {
		t.Fatalf("source-only edit builds = dep %d app %d, want 0 and 0", second.dep, second.app)
	}
	if preparedSecond.Dependency != preparedFirst.Dependency {
		t.Fatalf("source-only edit changed the dependency reference: %q -> %q", preparedFirst.Dependency, preparedSecond.Dependency)
	}
	if preparedSecond.Image != preparedFirst.Image {
		t.Fatalf("source-only edit changed the app image: %q -> %q", preparedFirst.Image, preparedSecond.Image)
	}
	if preparedSecond.Fingerprint == preparedFirst.Fingerprint {
		t.Fatal("source-only edit must still advance the source fingerprint (the reconcile generation)")
	}
}

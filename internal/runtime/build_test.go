package runtime

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/app"
	"relay/internal/runtime/plan"
	"relay/internal/source"
)

// TestBuildImageOptionsRemoveIntermediateContainers verifies that Relay's build
// options request the daemon to remove intermediate containers after a
// successful build. The moby client v0.6.0 emits rm=0 when Remove is false
// (suppressing the daemon's default cleanup), so Remove must be true to restore
// rm=1. It also asserts ForceRemove is NOT set: failed builds must keep their
// intermediates for debugging.
func TestBuildImageOptionsRemoveIntermediateContainers(t *testing.T) {
	opts := buildImageOptions("relay-app-test:abc123", nil)

	if !opts.Remove {
		t.Error("expected Remove=true so the daemon removes intermediate containers after a " +
			"successful build (moby client v0.6.0 emits rm=0 when Remove is false)")
	}
	if opts.ForceRemove {
		t.Error("expected ForceRemove=false: failed builds must keep their intermediates for debugging")
	}
	if len(opts.Tags) != 1 || opts.Tags[0] != "relay-app-test:abc123" {
		t.Errorf("Tags = %v, want [relay-app-test:abc123]", opts.Tags)
	}
	if opts.Dockerfile != "Dockerfile" {
		t.Errorf("Dockerfile = %q, want %q", opts.Dockerfile, "Dockerfile")
	}
	if opts.Labels != nil {
		t.Errorf("Labels = %v, want nil for a plain build", opts.Labels)
	}
}

// TestBuildImageOptionsCarriesManagedLabels verifies that a managed build passes
// its ownership labels through to the daemon's ImageBuild options, so the
// resulting image config carries them (the build backend applies them as LABEL
// equivalents, the same mechanism Relay relies on for its label model).
func TestBuildImageOptionsCarriesManagedLabels(t *testing.T) {
	labels := map[string]string{labelType: ImageTypeApp, labelApp: "a", labelDependency: "relay-dep-abc"}
	opts := buildImageOptions("relay-app-a:abc123", labels)

	if got := opts.Labels[labelType]; got != ImageTypeApp {
		t.Errorf("opts.Labels[relay.type] = %q, want %q", got, ImageTypeApp)
	}
	if got := opts.Labels[labelApp]; got != "a" {
		t.Errorf("opts.Labels[relay.app] = %q, want a", got)
	}
	if got := opts.Labels[labelDependency]; got != "relay-dep-abc" {
		t.Errorf("opts.Labels[relay.dependency] = %q, want relay-dep-abc", got)
	}
}

func TestDockerfileTemplateEmbedsAppSource(t *testing.T) {
	p := plan.BuildPlan{
		BaseImage: "node:24-alpine",
		WorkDir:   "/app",
		Files: []plan.File{
			{Path: "/relay/bootstrap.mjs", Content: []byte("x"), Mode: fs.FileMode(0o644)},
		},
		Install:    []string{"pnpm install --prod --frozen-lockfile"},
		Entrypoint: []string{"node", "/relay/bootstrap.mjs"},
	}

	df := mustRenderDockerfile(t, p)

	if !strings.Contains(df, "FROM node:24-alpine") {
		t.Errorf("expected FROM line, got:\n%s", df)
	}
	if !strings.Contains(df, "WORKDIR /app") {
		t.Errorf("expected WORKDIR line, got:\n%s", df)
	}
	if !strings.Contains(df, "COPY . /app") {
		t.Errorf("expected COPY sources line, got:\n%s", df)
	}
	if !strings.Contains(df, "COPY relay/bootstrap.mjs /relay/") {
		t.Errorf("expected COPY bootstrap line, got:\n%s", df)
	}
	if !strings.Contains(df, "RUN pnpm install --prod --frozen-lockfile") {
		t.Errorf("expected RUN line, got:\n%s", df)
	}
	if !strings.Contains(df, `ENTRYPOINT ["node", "/relay/bootstrap.mjs"]`) {
		t.Errorf("expected quoted ENTRYPOINT JSON array, got:\n%s", df)
	}
}

func TestRenderDockerfileNoInstallNoEntrypoint(t *testing.T) {
	p := plan.BuildPlan{
		BaseImage: "python:3.14-slim",
		WorkDir:   "/app",
	}
	df := mustRenderDockerfile(t, p)
	if strings.Contains(df, "RUN ") {
		t.Errorf("did not expect RUN instruction, got:\n%s", df)
	}
	if strings.Contains(df, "ENTRYPOINT") {
		t.Errorf("did not expect ENTRYPOINT instruction, got:\n%s", df)
	}
	if !strings.Contains(df, "COPY . /app") {
		t.Errorf("expected COPY sources line, got:\n%s", df)
	}
}

// TestRenderDockerfileHardening verifies the hardening instructions are emitted
// in the correct order: FROM → WORKDIR → COPY → RUN install → RUN user setup →
// USER → ENTRYPOINT. The user setup must run as root AFTER install (so
// dependency installation is unaffected) and before the USER switch.
// pythonUserSetup is the runtime-user provisioning command the python plans
// emit; kept as a fixture so the long line does not repeat inline.
const pythonUserSetup = "groupadd -g 10001 app && useradd -u 10001 -g 10001 -m -d /home/app " +
	"-s /usr/sbin/nologin app && chown -R 10001:10001 /app /relay"

func TestRenderDockerfileHardening(t *testing.T) {
	p := plan.BuildPlan{
		BaseImage: "python:3.14-slim",
		WorkDir:   "/app",
		Files: []plan.File{
			{Path: "/relay/bootstrap.py", Content: []byte("x"), Mode: fs.FileMode(0o644)},
		},
		Install:    []string{"uv pip install --system --no-cache -r requirements.txt"},
		UserSetup:  pythonUserSetup,
		User:       "10001:10001",
		Entrypoint: []string{"python", "/relay/bootstrap.py"},
	}

	df := mustRenderDockerfile(t, p)

	// The full expected shape, in order.
	want := `FROM python:3.14-slim
WORKDIR /app
COPY . /app
COPY relay/bootstrap.py /relay/
RUN uv pip install --system --no-cache -r requirements.txt
RUN ` + pythonUserSetup + `
USER 10001:10001
ENTRYPOINT ["python", "/relay/bootstrap.py"]
`
	if df != want {
		t.Errorf("rendered dockerfile mismatch.\n--- got ---\n%s\n--- want ---\n%s", df, want)
	}

	// The USER line must come after the user-setup RUN and after the install RUN.
	userIdx := strings.Index(df, "USER 10001:10001")
	setupIdx := strings.Index(df, "RUN groupadd")
	installIdx := strings.Index(df, "RUN uv pip install")
	if userIdx < 0 || setupIdx < 0 || installIdx < 0 {
		t.Fatalf("expected USER, user-setup RUN, and install RUN lines, got:\n%s", df)
	}
	if !(installIdx < setupIdx && setupIdx < userIdx) {
		t.Errorf("expected order install RUN < user-setup RUN < USER, got install=%d setup=%d user=%d",
			installIdx, setupIdx, userIdx)
	}
}

// TestRenderDockerfileRuntimeTools verifies a runtime tool (the uv binary) is
// materialized as `COPY --from=<image> <src> <dest>` BEFORE the install RUN, so
// the install can use the tool. The order is the whole point: a tool after the
// install would make uv unavailable at install time. The full golden output also
// pins that a RuntimeTool renders byte-for-byte as the historical external tool
// copy did.
func TestRenderDockerfileRuntimeTools(t *testing.T) {
	p := plan.BuildPlan{
		BaseImage: "python:3.14-slim",
		WorkDir:   "/app",
		RuntimeTools: []plan.RuntimeTool{{
			From: "ghcr.io/astral-sh/uv:0.12.17", Source: "/uv", Destination: "/usr/local/bin/uv",
		}},
		Install: []string{"uv pip install --system --no-cache -r requirements.txt"},
	}

	df := mustRenderDockerfile(t, p)
	wantLine := "COPY --from=ghcr.io/astral-sh/uv:0.12.17 /uv /usr/local/bin/uv"
	if !strings.Contains(df, wantLine) {
		t.Fatalf("expected runtime tool line %q, got:\n%s", wantLine, df)
	}
	copyIdx := strings.Index(df, wantLine)
	installIdx := strings.Index(df, "RUN uv pip install")
	if copyIdx < 0 || installIdx < 0 || copyIdx > installIdx {
		t.Errorf("runtime tool must precede the install RUN (copy=%d install=%d):\n%s", copyIdx, installIdx, df)
	}
	// Never a floating tag: the renderer copies the pinned reference verbatim.
	if strings.Contains(df, "uv:latest") {
		t.Errorf("runtime tool must not use a floating latest tag:\n%s", df)
	}

	// The exact rendering is unchanged by the terminology rename: the tool is
	// still a COPY --from in the same position.
	want := `FROM python:3.14-slim
WORKDIR /app
COPY . /app
COPY --from=ghcr.io/astral-sh/uv:0.12.17 /uv /usr/local/bin/uv
RUN uv pip install --system --no-cache -r requirements.txt
`
	if df != want {
		t.Errorf("rendered dockerfile mismatch.\n--- got ---\n%s\n--- want ---\n%s", df, want)
	}
}

// TestRenderDockerfileNoUser verifies back-compat: a plan with empty User and
// UserSetup emits no USER or user-setup RUN line.
func TestRenderDockerfileNoUser(t *testing.T) {
	p := plan.BuildPlan{
		BaseImage:  "node:24-alpine",
		WorkDir:    "/app",
		Entrypoint: []string{"node", "/relay/bootstrap.mjs"},
	}
	df := mustRenderDockerfile(t, p)
	if strings.Contains(df, "USER ") {
		t.Errorf("did not expect USER line for unhardened plan, got:\n%s", df)
	}
	if strings.Contains(df, "RUN groupadd") || strings.Contains(df, "RUN addgroup") {
		t.Errorf("did not expect user-setup RUN for unhardened plan, got:\n%s", df)
	}
}

// TestRenderDockerfileDependencyBase verifies the synthetic plan the builder
// renders for a dependency image: FROM the runtime base -> WORKDIR the install
// dir -> COPY the staged manifests -> RUN install. It must carry NO user setup,
// USER, Env, or ENTRYPOINT — a dependency image is a base for the app
// image, not a runnable app.
func TestRenderDockerfileDependencyBase(t *testing.T) {
	// buildDependencyImage builds this plan (note Deps is intentionally zero so
	// the renderer emits the plain COPY . path, not a nested dep base). The
	// dependency base image is built FROM the raw runtime base, so it must
	// materialize the runtime's tool (uv) itself — this is how the dependency
	// install gets uv even though it does not inherit it from an app image.
	p := plan.BuildPlan{
		BaseImage: "python:3.14-slim",
		WorkDir:   "/app",
		RuntimeTools: []plan.RuntimeTool{{
			From: "ghcr.io/astral-sh/uv:0.12.17", Source: "/uv", Destination: "/usr/local/bin/uv",
		}},
		Install: []string{"uv pip install --system --no-cache -r requirements.txt"},
	}
	df := mustRenderDockerfile(t, p)

	if !strings.Contains(df, "FROM python:3.14-slim") {
		t.Errorf("expected FROM base line, got:\n%s", df)
	}
	if !strings.Contains(df, "WORKDIR /app") {
		t.Errorf("expected WORKDIR install-dir line, got:\n%s", df)
	}
	// The dep build context contains ONLY the manifest files, so COPY . /app
	// stages exactly them into the install dir.
	if !strings.Contains(df, "COPY . /app") {
		t.Errorf("expected COPY manifests line, got:\n%s", df)
	}
	if !strings.Contains(df, "COPY --from=ghcr.io/astral-sh/uv:0.12.17 /uv /usr/local/bin/uv") {
		t.Errorf("expected uv runtime tool line in the dependency base, got:\n%s", df)
	}
	if !strings.Contains(df, "RUN uv pip install --system --no-cache -r requirements.txt") {
		t.Errorf("expected RUN install line, got:\n%s", df)
	}
	// A base image must not get runtime concerns baked in.
	for _, banned := range []string{"USER ", "ENTRYPOINT", "groupadd", "addgroup", "PYTHONDONTWRITEBYTECODE"} {
		if strings.Contains(df, banned) {
			t.Errorf("dependency image must not contain %q, got:\n%s", banned, df)
		}
	}
}

// TestRenderDockerfileAppFromDependency verified the MANAGER rewrites the
// app image's FROM to the dependency reference when Deps are present; this
// test asserts the plan the manager hands to renderDockerfile produces the right
// FROM and no install RUN (engine moved install into Deps).
func TestRenderDockerfileAppFromDependency(t *testing.T) {
	p := plan.BuildPlan{
		BaseImage:  "relay-dep-abcdef1234567890",
		WorkDir:    "/app",
		Files:      []plan.File{{Path: "/relay/bootstrap.py", Content: []byte("x"), Mode: fs.FileMode(0o644)}},
		UserSetup:  "groupadd -g 10001 app && useradd -u 10001 -g 10001 app && chown -R 10001:10001 /app /relay",
		User:       "10001:10001",
		Entrypoint: []string{"python", "/relay/bootstrap.py"},
	}
	df := mustRenderDockerfile(t, p)

	if !strings.Contains(df, "FROM relay-dep-abcdef1234567890") {
		t.Errorf("function image must build FROM the dependency image, got:\n%s", df)
	}
	// The install is in the dependency layer; the app image has no install
	// RUN of its own.
	if strings.Contains(df, "RUN uv pip install") {
		t.Errorf("function image built FROM the dep layer must not re-run install, got:\n%s", df)
	}
	// The user setup is still needed (the dep layer's /app contents are owned by
	// root; chown hands them to the runtime user), and the user switch + entry
	// point stay in the app layer.
	if !strings.Contains(df, "RUN groupadd") {
		t.Errorf("expected user-setup RUN kept in the function image, got:\n%s", df)
	}
}

func TestRenderDockerfileEntrypointQuoting(t *testing.T) {
	p := plan.BuildPlan{
		BaseImage:  "example:1",
		WorkDir:    "/app",
		Entrypoint: []string{"program", "--flag value"},
	}
	df := mustRenderDockerfile(t, p)
	if !strings.Contains(df, `ENTRYPOINT ["program", "--flag value"]`) {
		t.Errorf("expected argument with spaces preserved, got:\n%s", df)
	}
}

// mustRenderDockerfile renders a plan and fails the test on a render error.
func mustRenderDockerfile(t *testing.T, p plan.BuildPlan) string {
	t.Helper()
	df, err := renderDockerfile(p)
	if err != nil {
		t.Fatalf("renderDockerfile: %v", err)
	}
	return df
}

// artifactPnpmTool is a two-architecture artifact tool fixture for the renderer
// tests. The member is nested ("bin/pnpm") so the extraction path is exercised.
func artifactPnpmTool() plan.RuntimeTool {
	return plan.RuntimeTool{
		Destination: "/usr/local/bin/pnpm",
		Artifact: &plan.RuntimeArtifact{Variants: []plan.ArtifactVariant{
			{Arch: "amd64", URL: "https://example.test/pnpm-x64.tgz", SHA256: strings.Repeat("a", 64), Member: "bin/pnpm"},
			{Arch: "arm64", URL: "https://example.test/pnpm-arm64.tgz", SHA256: strings.Repeat("b", 64), Member: "bin/pnpm"},
		}},
	}
}

// TestRenderDockerfileArtifactTool verifies an archive-artifact RuntimeTool is
// materialized as one fail-closed RUN that downloads the pinned archive for the
// TARGET architecture with wget (falling back to curl), verifies the SHA-256,
// extracts only the validated member, and moves it to its destination BEFORE
// the install RUN.
func TestRenderDockerfileArtifactTool(t *testing.T) {
	p := plan.BuildPlan{
		BaseImage:    "node:24-alpine",
		WorkDir:      "/app",
		TargetArch:   "amd64",
		RuntimeTools: []plan.RuntimeTool{artifactPnpmTool()},
		Install:      []string{"pnpm install --prod"},
	}
	df := mustRenderDockerfile(t, p)

	for _, want := range []string{
		"wget -q -O /tmp/relay-tool-0.tar.gz https://example.test/pnpm-x64.tgz",
		strings.Repeat("a", 64),
		"sha256sum -c -",
		"tar -xzf /tmp/relay-tool-0.tar.gz -C /tmp/relay-tool-0.unpack bin/pnpm",
		"mv /tmp/relay-tool-0.unpack/bin/pnpm /usr/local/bin/pnpm",
		"chmod 0755 /usr/local/bin/pnpm",
	} {
		if !strings.Contains(df, want) {
			t.Errorf("artifact Dockerfile missing %q:\n%s", want, df)
		}
	}
	// The whole acquisition (download, verify, extract, install, cleanup) is ONE
	// fail-closed RUN, so the downloaded archive never becomes a retained layer.
	if strings.Count(df, "RUN ") != 2 { // the artifact RUN plus the install RUN
		t.Errorf("artifact acquisition must be a single RUN:\n%s", df)
	}
	// Only the target architecture's artifact is referenced.
	if strings.Contains(df, "pnpm-arm64.tgz") {
		t.Errorf("amd64 render must not reference the arm64 artifact:\n%s", df)
	}
	fetchIdx := strings.Index(df, "wget -q -O /tmp/relay-tool-0.tar.gz")
	installIdx := strings.Index(df, "RUN pnpm install")
	if fetchIdx < 0 || installIdx < 0 || fetchIdx > installIdx {
		t.Errorf("artifact acquisition must precede the install RUN (fetch=%d install=%d):\n%s", fetchIdx, installIdx, df)
	}
}

// TestRenderDockerfileArtifactSelectsTargetArch verifies the same plan renders a
// different (only) artifact when the target architecture changes, so the tool
// and the image it is baked into can never disagree.
func TestRenderDockerfileArtifactSelectsTargetArch(t *testing.T) {
	p := plan.BuildPlan{
		BaseImage:    "node:24-alpine",
		TargetArch:   "arm64",
		RuntimeTools: []plan.RuntimeTool{artifactPnpmTool()},
	}
	df := mustRenderDockerfile(t, p)
	if !strings.Contains(df, "wget -q -O /tmp/relay-tool-0.tar.gz https://example.test/pnpm-arm64.tgz") {
		t.Errorf("arm64 render must reference the arm64 artifact:\n%s", df)
	}
	if strings.Contains(df, "pnpm-x64.tgz") {
		t.Errorf("arm64 render must not reference the amd64 artifact:\n%s", df)
	}
}

// TestRenderDockerfileArtifactUnsupportedArch verifies an artifact tool with no
// variant for the target architecture fails the render (fail unsupported),
// never emits a tool-less image.
func TestRenderDockerfileArtifactUnsupportedArch(t *testing.T) {
	p := plan.BuildPlan{
		BaseImage:    "node:24-alpine",
		TargetArch:   "riscv64",
		RuntimeTools: []plan.RuntimeTool{artifactPnpmTool()},
	}
	if _, err := renderDockerfile(p); err == nil {
		t.Fatal("renderDockerfile with an unsupported artifact architecture must fail")
	}
}

// TestRenderDockerfileArtifactMissingTargetArch verifies a plan carrying an
// artifact tool but no target architecture fails rather than rendering an
// unresolved tool.
func TestRenderDockerfileArtifactMissingTargetArch(t *testing.T) {
	p := plan.BuildPlan{
		BaseImage:    "node:24-alpine",
		RuntimeTools: []plan.RuntimeTool{artifactPnpmTool()},
	}
	if _, err := renderDockerfile(p); err == nil {
		t.Fatal("renderDockerfile with an artifact tool and no target architecture must fail")
	}
}

// TestRenderDockerfileArtifactRejectsMixedForm verifies the renderer rejects a
// RuntimeTool that sets both image-copy fields and an Artifact, instead of
// silently rendering the artifact form.
func TestRenderDockerfileArtifactRejectsMixedForm(t *testing.T) {
	mixed := artifactPnpmTool()
	mixed.From = "ghcr.io/astral-sh/uv:0.12.17"
	mixed.Source = "/uv"

	p := plan.BuildPlan{
		BaseImage:    "node:24-alpine",
		TargetArch:   "amd64",
		RuntimeTools: []plan.RuntimeTool{mixed},
	}
	if _, err := renderDockerfile(p); err == nil {
		t.Fatal("renderDockerfile accepted a runtime tool mixing From/Source with Artifact")
	}
}

// TestRenderDockerfileArtifactRejectsUnsafeData verifies the renderer fails
// closed on an unsafe member or a malformed digest/url instead of emitting a
// Dockerfile that could traverse or skip verification.
func TestRenderDockerfileArtifactRejectsUnsafeData(t *testing.T) {
	base := artifactPnpmTool()
	v := base.Artifact.Variants[0]

	cases := []struct {
		name   string
		mutate func(*plan.ArtifactVariant)
	}{
		{"traversal member", func(x *plan.ArtifactVariant) { x.Member = "../etc/passwd" }},
		{"absolute member", func(x *plan.ArtifactVariant) { x.Member = "/etc/passwd" }},
		{"option member", func(x *plan.ArtifactVariant) { x.Member = "--checkpoint=1" }},
		{"backend member", func(x *plan.ArtifactVariant) { x.Member = `a\b` }},
		{"non-https url", func(x *plan.ArtifactVariant) { x.URL = "http://example.test/x.tgz" }},
		{"short digest", func(x *plan.ArtifactVariant) { x.SHA256 = "abc" }},
		{"uppercase digest", func(x *plan.ArtifactVariant) { x.SHA256 = strings.Repeat("A", 64) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			x := v
			tc.mutate(&x)
			p := plan.BuildPlan{
				BaseImage:    "node:24-alpine",
				TargetArch:   "amd64",
				RuntimeTools: []plan.RuntimeTool{{Destination: "/usr/local/bin/pnpm", Artifact: &plan.RuntimeArtifact{Variants: []plan.ArtifactVariant{x}}}},
			}
			if _, err := renderDockerfile(p); err == nil {
				t.Fatalf("renderDockerfile accepted unsafe artifact data: %+v", x)
			}
		})
	}
}

// TestValidateArtifactMember pins the member validator directly: clean relative
// paths are accepted, traversal/absolute/option/backslash/empty-segment forms
// are rejected.
func TestValidateArtifactMember(t *testing.T) {
	valid := []string{"pnpm", "bin/pnpm", "a/b/c"}
	for _, m := range valid {
		if err := validateArtifactMember(m); err != nil {
			t.Errorf("validateArtifactMember(%q) = %v, want nil", m, err)
		}
	}
	invalid := []string{"", "/pnpm", "-pnpm", `a\b`, ".", "..", "a/../b", "a//b", "a/./b", "a/", "./a"}
	for _, m := range invalid {
		if err := validateArtifactMember(m); err == nil {
			t.Errorf("validateArtifactMember(%q) = nil, want an error", m)
		}
	}
}

// TestBuildImageOptionsSetsTargetPlatform verifies every build requests the
// resolved target platform explicitly (linux at the resolved arch) instead of
// letting the builder default to the daemon's platform.
func TestBuildImageOptionsSetsTargetPlatform(t *testing.T) {
	opts := buildImageOptions("relay-app-test:abc123", nil)
	if len(opts.Platforms) != 1 {
		t.Fatalf("Platforms = %+v, want exactly one", opts.Platforms)
	}
	if opts.Platforms[0].OS != "linux" || opts.Platforms[0].Architecture != arch {
		t.Errorf("Platforms[0] = %+v, want {linux %s}", opts.Platforms[0], arch)
	}
}

func TestQuoteEntrypointJSON(t *testing.T) {
	got := quoteEntrypointJSON([]string{"python", "/relay/bootstrap.py"})
	if got != `["python", "/relay/bootstrap.py"]` {
		t.Errorf("quoteEntrypointJSON = %q, want JSON array", got)
	}
}

// TestCaptureSourceSnapshotSkipsTemplateYaml verifies the build-context staging
// excludes template.yaml (Relay configuration, not app source) while staging
// every other captured file into the snapshot's private root (the build context).
// This is what keeps env values and secret references out of the image layers.
// The capture derives the staged bytes and the fingerprint from one read, so the
// assertion also covers the capture boundary.
func TestCaptureSourceSnapshotSkipsTemplateYaml(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "template.yaml"), []byte("runtime: node24\n"), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(src, "index.js"), []byte("export function h(){}\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(src, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir sub: %v", err)
	}
	if err := os.WriteFile(filepath.Join(src, "sub", "helper.js"), []byte("export const x=1;\n"), 0o644); err != nil {
		t.Fatalf("write helper: %v", err)
	}
	// A nested template.yaml is dead configuration (the loader only reads the
	// top-level one), and it must never reach an image: exclusion is by base
	// name anywhere in the tree.
	if err := os.WriteFile(filepath.Join(src, "sub", "template.yaml"), []byte("secrets:\n  X: x\n"), 0o644); err != nil {
		t.Fatalf("write nested template: %v", err)
	}

	selection, err := source.ForDir(src)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	snapshot, err := app.CaptureSourceSnapshot(selection)
	if err != nil {
		t.Fatalf("capture source snapshot: %v", err)
	}
	t.Cleanup(snapshot.Discard)
	dst := snapshot.Root()

	if _, err := os.Stat(filepath.Join(dst, "template.yaml")); !os.IsNotExist(err) {
		t.Fatalf("template.yaml should be excluded from the build context, stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "sub", "template.yaml")); !os.IsNotExist(err) {
		t.Fatalf("nested template.yaml should also be excluded, stat err = %v", err)
	}
	for _, want := range []string{"index.js", filepath.Join("sub", "helper.js")} {
		if _, err := os.Stat(filepath.Join(dst, want)); err != nil {
			t.Errorf("expected %s copied, got: %v", want, err)
		}
	}
}

// TestCaptureSourceSnapshotHonorsSelection verifies the build context stages
// exactly the selected source: files excluded by the app's .gitignore never
// reach the image, the applicable .gitignore itself does, and template.yaml is
// still excluded (Relay configuration must not leak into a layer).
func TestCaptureSourceSnapshotHonorsSelection(t *testing.T) {
	src := t.TempDir()
	write := func(name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(src, name), []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	write(".gitignore", "*.log\ntemplate.yaml-ignored\n")
	write("template.yaml", "runtime: node24\n")
	write("index.js", "export function h(){}\n")
	write("debug.log", "noise\n")

	selection, err := source.ForDir(src)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	snapshot, err := app.CaptureSourceSnapshot(selection)
	if err != nil {
		t.Fatalf("capture source snapshot: %v", err)
	}
	t.Cleanup(snapshot.Discard)
	dst := snapshot.Root()

	if _, err := os.Stat(filepath.Join(dst, "template.yaml")); !os.IsNotExist(err) {
		t.Fatalf("template.yaml must be excluded from the build context, stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "debug.log")); !os.IsNotExist(err) {
		t.Fatalf("ignored file must be excluded from the build context, stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "index.js")); err != nil {
		t.Errorf("included source missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, ".gitignore")); err != nil {
		t.Errorf("applicable .gitignore must be staged so the policy travels: %v", err)
	}
}

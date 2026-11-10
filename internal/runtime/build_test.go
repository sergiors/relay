package runtime

import (
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/moby/moby/api/types/build"

	"relay/internal/app"
	"relay/internal/runtime/plan"
	"relay/internal/source"
)

// TestBuildImageOptionsRemoveIntermediateContainers verifies that Relay's build
// options request the daemon to remove intermediate containers after a
// successful build. The moby client v0.6.0 emits rm=0 when Remove is false
// (suppressing the daemon's default cleanup), so Remove must be true to restore
// rm=1 (BuildKit ignores rm and keeps failed state in its own cache). It also
// asserts ForceRemove is NOT set: Relay never force-removes build diagnostics.
func TestBuildImageOptionsRemoveIntermediateContainers(t *testing.T) {
	opts := buildImageOptions("relay-app-test:abc123", nil, "session-1")

	if !opts.Remove {
		t.Error("expected Remove=true so the daemon removes intermediate containers after a " +
			"successful build (moby client v0.6.0 emits rm=0 when Remove is false)")
	}
	if opts.ForceRemove {
		t.Error("expected ForceRemove=false: build diagnostics must not be force-removed")
	}
	if len(opts.Tags) != 1 || opts.Tags[0] != "relay-app-test:abc123" {
		t.Errorf("Tags = %v, want [relay-app-test:abc123]", opts.Tags)
	}
	if opts.Dockerfile != "Dockerfile" {
		t.Errorf("Dockerfile = %q, want %q", opts.Dockerfile, "Dockerfile")
	}
	if opts.Version != build.BuilderBuildKit {
		t.Errorf("Version = %q, want explicit BuildKit version %q", opts.Version, build.BuilderBuildKit)
	}
	if opts.SessionID != "session-1" {
		t.Errorf("SessionID = %q, want the live BuildKit session", opts.SessionID)
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
	opts := buildImageOptions("relay-app-a:abc123", labels, "session-2")

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

// TestDockerfileTemplateEmbedsAppSource pins a Node-style baked-source plan's
// rendered Dockerfile byte-for-byte: FROM → WORKDIR → the whole-tree source
// COPY → one COPY per generated plan file → the install RUN → the ENTRYPOINT as
// a JSON array. No USER line: an unhardened Node plan emits no user switch.
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

	want := `FROM node:24-alpine
WORKDIR /app
COPY . /app
COPY relay/bootstrap.mjs /relay/
RUN pnpm install --prod --frozen-lockfile
ENTRYPOINT ["node", "/relay/bootstrap.mjs"]
`
	if df != want {
		t.Errorf("rendered dockerfile mismatch.\n--- got ---\n%s\n--- want ---\n%s", df, want)
	}
}

// TestRenderDockerfileNoInstallNoEntrypoint pins the minimal baked-source image
// byte-for-byte: a plan with no Install, UserSetup, User, or Entrypoint renders
// exactly FROM, WORKDIR, and the source COPY. The full match is what proves the
// absence: no optional instruction may appear in place of an absent one.
func TestRenderDockerfileNoInstallNoEntrypoint(t *testing.T) {
	p := plan.BuildPlan{
		BaseImage: "python:3.14-slim",
		WorkDir:   "/app",
	}
	df := mustRenderDockerfile(t, p)
	want := `FROM python:3.14-slim
WORKDIR /app
COPY . /app
`
	if df != want {
		t.Errorf("rendered dockerfile mismatch.\n--- got ---\n%s\n--- want ---\n%s", df, want)
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

// TestRenderDockerfileSourceMountedOmitsSourceCopy pins the HOT_RELOAD rendering
// byte-for-byte: with copySource=false the renderer emits everything the
// historical baked-source image did EXCEPT the whole-tree source COPY — the
// generated-plan-file COPYs, the runtime tool, the install, the user setup, the
// USER switch, and the ENTRYPOINT are unchanged, in the same order. The
// copySource=true form must stay byte-identical to renderDockerfile, so the
// flag is the ONLY difference between a baked and a mounted Dockerfile.
func TestRenderDockerfileSourceMountedOmitsSourceCopy(t *testing.T) {
	p := plan.BuildPlan{
		BaseImage: "python:3.14-slim",
		WorkDir:   "/app",
		Files: []plan.File{
			{Path: "/relay/bootstrap.py", Content: []byte("x"), Mode: fs.FileMode(0o644)},
		},
		RuntimeTools: []plan.RuntimeTool{{
			From: "ghcr.io/astral-sh/uv:0.12.17", Source: "/uv", Destination: "/usr/local/bin/uv",
		}},
		Install:    []string{"uv pip install --system --no-cache -r requirements.txt"},
		UserSetup:  pythonUserSetup,
		User:       "10001:10001",
		Entrypoint: []string{"python", "/relay/bootstrap.py"},
	}

	want := `FROM python:3.14-slim
WORKDIR /app
COPY relay/bootstrap.py /relay/
COPY --from=ghcr.io/astral-sh/uv:0.12.17 /uv /usr/local/bin/uv
RUN uv pip install --system --no-cache -r requirements.txt
RUN ` + pythonUserSetup + `
USER 10001:10001
ENTRYPOINT ["python", "/relay/bootstrap.py"]
`
	if got := mustRenderDockerfileWithSource(t, p, false); got != want {
		t.Errorf("source-mounted dockerfile mismatch.\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}

	// The baked form is the mounted form plus exactly the one source COPY line;
	// and copySource=true stays byte-identical to the historical renderDockerfile
	// entry point.
	wantBaked := strings.Replace(want, "WORKDIR /app\n", "WORKDIR /app\nCOPY . /app\n", 1)
	if got := mustRenderDockerfileWithSource(t, p, true); got != wantBaked {
		t.Errorf("copySource=true dockerfile mismatch.\n--- got ---\n%s\n--- want ---\n%s", got, wantBaked)
	}
	if got, baked := mustRenderDockerfile(t, p), mustRenderDockerfileWithSource(t, p, true); got != baked {
		t.Errorf("renderDockerfile must stay byte-identical to copySource=true.\n--- got ---\n%s\n--- want ---\n%s", got, baked)
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

// mustRenderDockerfileWithSource renders a plan with an explicit copySource flag
// and fails the test on a render error.
func mustRenderDockerfileWithSource(t *testing.T, p plan.BuildPlan, copySource bool) string {
	t.Helper()
	df, err := renderDockerfileWithSource(p, copySource)
	if err != nil {
		t.Fatalf("renderDockerfileWithSource: %v", err)
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

func artifactPnpmToolWithFallback() plan.RuntimeTool {
	tool := artifactPnpmTool()
	variant := tool.Artifact.Variants[0]
	variant.Fallbacks = []plan.ArtifactSource{{
		URL:    "https://registry.npmjs.org/@pnpm/exe.linux-x64-musl/-/exe-linux-x64-musl-12.10.1.tgz",
		SHA256: strings.Repeat("b", 64), Member: "package/pnpm",
	}}
	tool.Artifact.Variants[0] = variant
	return tool
}

// TestRenderDockerfileArtifactTool verifies artifacts use the explicit frontend
// and BuildKit cache, retry only bounded wget failures, verify cached/downloaded
// bytes, and install only after verification.
func TestRenderDockerfileArtifactTool(t *testing.T) {
	p := plan.BuildPlan{
		BaseImage:    "node:24-alpine",
		WorkDir:      "/app",
		TargetArch:   "amd64",
		RuntimeTools: []plan.RuntimeTool{artifactPnpmToolWithFallback()},
		Install:      []string{"pnpm install --prod"},
	}
	df := mustRenderDockerfile(t, p)

	digest := strings.Repeat("a", 64)
	body := artifactRunBody(t, df, digest, t.TempDir())
	for _, want := range []string{
		"FROM node:24-alpine\n",
		"RUN --mount=type=cache,id=relay-artifact-" + digest + ",target=/var/cache/relay-artifacts,sharing=locked <<'RELAY_ARTIFACT'",
		"use_cached_archive " + digest + " bin/pnpm",
		"try_download_archive https://example.test/pnpm-x64.tgz " + digest + " bin/pnpm",
		"try_download_archive https://registry.npmjs.org/@pnpm/exe.linux-x64-musl/-/exe-linux-x64-musl-12.10.1.tgz " + strings.Repeat("b", 64) + " package/pnpm",
		"busybox timeout -k 2 25 busybox wget -T 20 -O",
		"while [ \"$attempt\" -le 3 ]",
		"sleep \"$attempt\"",
		"artifact SHA-256 mismatch",
		"echo \"$artifact_sha256  $artifact_archive\" | sha256sum -c -",
		"tar -xzf \"$artifact_archive\" -C \"$artifact_unpack\" \"$artifact_member\"",
		"mv \"$artifact_unpack/$artifact_member\" \"$destination\"",
		"rm -rf \"$artifact_unpack\" \"$artifact_archive\"",
		"RUN pnpm install --prod\n",
	} {
		if !strings.Contains(df, want) && !strings.Contains(body, want) {
			t.Errorf("rendered artifact Dockerfile/shell body does not contain %q:\n%s", want, df)
		}
	}
	if strings.Contains(df, "curl") {
		t.Errorf("artifact Dockerfile must not use the unavailable curl fallback:\n%s", df)
	}
	if got := strings.Count(body, "busybox timeout -k 2 25 busybox wget -T 20 -O"); got != 1 {
		t.Errorf("download retry loop must contain one bounded wget invocation, got %d:\n%s", got, df)
	}
	if !strings.Contains(body, "echo 'artifact SHA-256 mismatch' >&2") {
		t.Errorf("a downloaded checksum mismatch must fail before cache publication:\n%s", df)
	}
	if strings.Index(body, "echo \"$artifact_sha256  $artifact_archive\" | sha256sum -c -") > strings.Index(body, "tar -xzf") {
		t.Errorf("artifact checksum verification must precede extraction:\n%s", df)
	}
	if strings.Index(body, "use_cached_archive "+digest+" ") > strings.Index(body, "try_download_archive https://example.test") ||
		strings.Index(body, "use_cached_archive "+strings.Repeat("b", 64)+" ") > strings.Index(body, "try_download_archive https://example.test") {
		t.Errorf("all source caches must be inspected before primary download:\n%s", body)
	}
	if strings.Index(body, "try_download_archive https://example.test") > strings.Index(body, "try_download_archive https://registry.npmjs.org") {
		t.Errorf("primary source must precede fallback source:\n%s", body)
	}
}

// TestRenderDockerfileArtifactShellRetriesAndReusesVerifiedCache executes the
// generated artifact RUN with fake download/checksum commands. It verifies the
// retry and checksum-keyed cache behavior rather than only matching shell text.
func TestRenderDockerfileArtifactShellRetriesAndReusesVerifiedCache(t *testing.T) {
	const digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	df := mustRenderDockerfile(t, plan.BuildPlan{
		BaseImage:    "node:24-alpine",
		TargetArch:   "amd64",
		RuntimeTools: []plan.RuntimeTool{artifactPnpmTool()},
		Install:      []string{"pnpm install --prod"},
	})
	workDir := t.TempDir()
	body := artifactRunBody(t, df, digest, workDir)
	commands, countFile := artifactShellCommands(t)
	cacheDir := filepath.Join(workDir, "cache")
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		t.Fatal(err)
	}
	configureArtifactShell(t, commands, countFile, digest, "transient")

	if output, err := runArtifactShell(body); err != nil {
		t.Fatalf("artifact RUN after transient wget failure: %v\n%s", err, output)
	}
	if got := readFile(t, countFile); got != "2\n" {
		t.Fatalf("wget calls after retry = %q, want 2", got)
	}
	if got := readFile(t, os.Getenv("SLEEP_LOG")); got != "1\n" {
		t.Errorf("retry backoff = %q, want one-second delay after the first failure", got)
	}
	cached := filepath.Join(cacheDir, digest+".tar.gz")
	if got := readFile(t, cached); got != "VALID" {
		t.Fatalf("cached artifact = %q, want verified content", got)
	}
	installed := filepath.Join(workDir, "bin", "pnpm")
	if info, err := os.Stat(installed); err != nil {
		t.Fatalf("installed artifact: %v", err)
	} else if info.Mode().Perm() != 0o755 {
		t.Errorf("installed artifact mode = %04o, want 0755", info.Mode().Perm())
	}
	if got := readFile(t, os.Getenv("TAR_ARGS")); got != strings.Join([]string{
		"-xzf", filepath.Join(workDir, "artifact.tar.gz"), "-C", filepath.Join(workDir, "unpack"), "bin/pnpm", "",
	}, "\n") {
		t.Errorf("tar arguments = %q, want exact archive/member extraction", got)
	}
	for _, path := range []string{filepath.Join(workDir, "artifact.tar.gz"), filepath.Join(workDir, "unpack")} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("artifact scratch path %q remains after install (stat error %v)", path, err)
		}
	}

	if output, err := runArtifactShell(body); err != nil {
		t.Fatalf("artifact RUN using verified cache: %v\n%s", err, output)
	}
	if got := readFile(t, countFile); got != "2\n" {
		t.Fatalf("wget calls after cache reuse = %q, want no additional download", got)
	}
}

// TestRenderDockerfileArtifactShellRejectsChecksumMismatch verifies a
// checksum-mismatched download fails closed before extraction or cache publish.
func TestRenderDockerfileArtifactShellRejectsChecksumMismatch(t *testing.T) {
	const digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	df := mustRenderDockerfile(t, plan.BuildPlan{
		BaseImage:    "node:24-alpine",
		TargetArch:   "amd64",
		RuntimeTools: []plan.RuntimeTool{artifactPnpmTool()},
		Install:      []string{"pnpm install --prod"},
	})
	workDir := t.TempDir()
	body := artifactRunBody(t, df, digest, workDir)
	commands, countFile := artifactShellCommands(t)
	cacheDir := filepath.Join(workDir, "cache")
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cacheDir, digest+".tar.gz"), []byte("CORRUPT"), 0o644); err != nil {
		t.Fatalf("write corrupt cache artifact: %v", err)
	}
	configureArtifactShell(t, commands, countFile, digest, "mismatch")

	output, err := runArtifactShell(body)
	if err == nil {
		t.Fatalf("artifact RUN succeeded for a mismatched checksum:\n%s", output)
	}
	if !strings.Contains(string(output), "artifact SHA-256 mismatch") {
		t.Errorf("failure output = %q, want checksum mismatch diagnostic", output)
	}
	if got := readFile(t, countFile); got != "1\n" {
		t.Errorf("wget calls = %q, want one mismatched download", got)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(countFile), "tar-called")); !os.IsNotExist(err) {
		t.Errorf("tar ran before checksum rejection (stat error %v)", err)
	}
	if _, err := os.Stat(filepath.Join(cacheDir, digest+".tar.gz")); !os.IsNotExist(err) {
		t.Errorf("unverified cached artifact was retained (stat error %v)", err)
	}
	if _, err := os.Stat(filepath.Join(cacheDir, "."+digest+".download")); !os.IsNotExist(err) {
		t.Errorf("mismatched temporary download was retained (stat error %v)", err)
	}
	if !strings.Contains(df, "RUN pnpm install --prod\n") ||
		strings.Index(df, "RUN pnpm install --prod\n") < strings.Index(df, "RUN --mount=type=cache,id=") {
		t.Errorf("pnpm install must follow the fail-closed artifact RUN:\n%s", df)
	}
}

func TestRenderDockerfileArtifactStopsAfterDownloadRetries(t *testing.T) {
	const digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	df := mustRenderDockerfile(t, plan.BuildPlan{
		BaseImage:    "node:24-alpine",
		TargetArch:   "amd64",
		RuntimeTools: []plan.RuntimeTool{artifactPnpmTool()},
		Install:      []string{"pnpm install --prod"},
	})
	workDir := t.TempDir()
	body := artifactRunBody(t, df, digest, workDir)
	commands, countFile := artifactShellCommands(t)
	cacheDir := filepath.Join(workDir, "cache")
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		t.Fatal(err)
	}
	configureArtifactShell(t, commands, countFile, digest, "failure")

	output, err := runArtifactShell(body)
	if err == nil {
		t.Fatalf("artifact RUN succeeded after every download failed:\n%s", output)
	}
	if !strings.Contains(string(output), "artifact download failed after 3 attempts") {
		t.Errorf("failure output = %q, want the bounded retry diagnostic", output)
	}
	if got := readFile(t, countFile); got != "3\n" {
		t.Errorf("wget calls = %q, want exactly 3", got)
	}
	if got := readFile(t, os.Getenv("SLEEP_LOG")); got != "1\n2\n" {
		t.Errorf("retry backoff = %q, want 1 then 2 seconds", got)
	}
	if _, err := os.Stat(filepath.Join(cacheDir, digest+".tar.gz")); !os.IsNotExist(err) {
		t.Errorf("failed download was published to cache (stat error %v)", err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(countFile), "tar-called")); !os.IsNotExist(err) {
		t.Errorf("tar ran after every download failed (stat error %v)", err)
	}
}

func TestRenderDockerfileArtifactFallsBackAndCachesBySourceDigest(t *testing.T) {
	const (
		primaryDigest  = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		fallbackDigest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	df := mustRenderDockerfile(t, plan.BuildPlan{
		BaseImage: "node:24-alpine", TargetArch: "amd64", RuntimeTools: []plan.RuntimeTool{artifactPnpmToolWithFallback()},
	})
	if !strings.Contains(df, "use_cached_archive "+primaryDigest) || !strings.Contains(df, "use_cached_archive "+fallbackDigest) {
		t.Fatalf("each source must have an independently digest-keyed cache entry:\n%s", df)
	}
	workDir := t.TempDir()
	body := artifactRunBody(t, df, primaryDigest, workDir)
	commands, countFile := artifactShellCommands(t)
	if err := os.MkdirAll(filepath.Join(workDir, "cache"), 0o755); err != nil {
		t.Fatal(err)
	}
	configureArtifactShell(t, commands, countFile, primaryDigest+","+fallbackDigest, "primary-fails")

	if output, err := runArtifactShell(body); err != nil {
		t.Fatalf("artifact RUN after primary source exhaustion: %v\n%s", err, output)
	}
	if got := readFile(t, countFile); got != "4\n" {
		t.Fatalf("download calls = %q, want three primary attempts then one fallback", got)
	}
	if got := readFile(t, os.Getenv("WGET_PRIMARY_COUNT")); got != "3\n" {
		t.Fatalf("primary source calls = %q, want 3", got)
	}
	if got := readFile(t, os.Getenv("WGET_FALLBACK_COUNT")); got != "1\n" {
		t.Fatalf("fallback source calls = %q, want 1", got)
	}
	cacheDir := filepath.Join(workDir, "cache")
	if got := readFile(t, filepath.Join(cacheDir, fallbackDigest+".tar.gz")); got != "VALID" {
		t.Fatalf("fallback cache = %q, want verified content", got)
	}
	if _, err := os.Stat(filepath.Join(cacheDir, primaryDigest+".tar.gz")); !os.IsNotExist(err) {
		t.Fatalf("failed primary source was cached (stat error %v)", err)
	}
	if _, err := os.Stat(filepath.Join(cacheDir, "."+fallbackDigest+".download")); !os.IsNotExist(err) {
		t.Fatalf("temporary fallback download was retained (stat error %v)", err)
	}
	// The generated RUN checks all source caches before network access. The
	// verified fallback archive therefore succeeds without retrying GitHub.
	if output, err := runArtifactShell(body); err != nil {
		t.Fatalf("artifact RUN from verified fallback cache: %v\n%s", err, output)
	}
	if got := readFile(t, countFile); got != "4\n" {
		t.Fatalf("download calls after fallback cache population = %q, want no additional download", got)
	}
}

func TestRenderDockerfileArtifactPrimarySuccessSkipsFallback(t *testing.T) {
	const (
		primaryDigest  = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		fallbackDigest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	df := mustRenderDockerfile(t, plan.BuildPlan{
		BaseImage:    "node:24-alpine",
		TargetArch:   "amd64",
		RuntimeTools: []plan.RuntimeTool{artifactPnpmToolWithFallback()},
	})
	workDir := t.TempDir()
	body := artifactRunBody(t, df, primaryDigest, workDir)
	commands, countFile := artifactShellCommands(t)
	if err := os.MkdirAll(filepath.Join(workDir, "cache"), 0o755); err != nil {
		t.Fatal(err)
	}
	configureArtifactShell(t, commands, countFile, primaryDigest+","+fallbackDigest, "primary-success")

	if output, err := runArtifactShell(body); err != nil {
		t.Fatalf("artifact RUN from primary source: %v\n%s", err, output)
	}
	if got := readFile(t, countFile); got != "1\n" {
		t.Fatalf("download calls = %q, want only the primary source", got)
	}
	if got := readFile(t, os.Getenv("WGET_PRIMARY_COUNT")); got != "1\n" {
		t.Errorf("primary source calls = %q, want 1", got)
	}
	if _, err := os.Stat(os.Getenv("WGET_FALLBACK_COUNT")); !os.IsNotExist(err) {
		t.Errorf("fallback was requested after primary success (stat error %v)", err)
	}
	if got := readFile(t, filepath.Join(workDir, "cache", primaryDigest+".tar.gz")); got != "VALID" {
		t.Fatalf("primary cache = %q, want verified content", got)
	}
}

func TestRenderDockerfileArtifactFallbackChecksumFailuresAreFatal(t *testing.T) {
	const (
		primaryDigest  = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		fallbackDigest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	for _, tc := range []struct {
		name string
		mode string
		want string
	}{
		{name: "primary", mode: "primary-mismatch", want: primaryDigest},
		{name: "fallback", mode: "fallback-mismatch", want: fallbackDigest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			df := mustRenderDockerfile(t, plan.BuildPlan{
				BaseImage: "node:24-alpine", TargetArch: "amd64", RuntimeTools: []plan.RuntimeTool{artifactPnpmToolWithFallback()},
			})
			workDir := t.TempDir()
			body := artifactRunBody(t, df, primaryDigest, workDir)
			commands, countFile := artifactShellCommands(t)
			cacheDir := filepath.Join(workDir, "cache")
			if err := os.MkdirAll(cacheDir, 0o755); err != nil {
				t.Fatal(err)
			}
			configureArtifactShell(t, commands, countFile, primaryDigest+","+fallbackDigest, tc.mode)
			output, err := runArtifactShell(body)
			if err == nil || !strings.Contains(string(output), "artifact SHA-256 mismatch") {
				t.Fatalf("checksum mismatch result = %v, output %q; want fatal mismatch", err, output)
			}
			if got := readFile(t, countFile); got == "" {
				t.Fatal("expected source download")
			}
			if tc.name == "primary" {
				if got := readFile(t, countFile); got != "1\n" {
					t.Errorf("primary checksum mismatch download calls = %q, want one", got)
				}
				if _, err := os.Stat(os.Getenv("WGET_FALLBACK_COUNT")); !os.IsNotExist(err) {
					t.Errorf("fallback was attempted after a primary checksum mismatch (stat error %v)", err)
				}
			} else if got := readFile(t, countFile); got != "4\n" {
				t.Errorf("fallback checksum mismatch download calls = %q, want three primary failures and one fallback", got)
			}
			if _, err := os.Stat(filepath.Join(cacheDir, tc.want+".tar.gz")); !os.IsNotExist(err) {
				t.Errorf("mismatched source was cached (stat error %v)", err)
			}
			if _, err := os.Stat(filepath.Join(cacheDir, "."+tc.want+".download")); !os.IsNotExist(err) {
				t.Errorf("mismatched temporary source was retained (stat error %v)", err)
			}
			if _, err := os.Stat(filepath.Join(filepath.Dir(countFile), "tar-called")); !os.IsNotExist(err) {
				t.Errorf("tar ran after checksum rejection (stat error %v)", err)
			}
		})
	}
}

func TestRenderDockerfileArtifactFallbackExhaustionIsBounded(t *testing.T) {
	const primaryDigest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	df := mustRenderDockerfile(t, plan.BuildPlan{
		BaseImage: "node:24-alpine", TargetArch: "amd64", RuntimeTools: []plan.RuntimeTool{artifactPnpmToolWithFallback()},
	})
	workDir := t.TempDir()
	body := artifactRunBody(t, df, primaryDigest, workDir)
	commands, countFile := artifactShellCommands(t)
	if err := os.MkdirAll(filepath.Join(workDir, "cache"), 0o755); err != nil {
		t.Fatal(err)
	}
	configureArtifactShell(t, commands, countFile, "", "failure")
	output, err := runArtifactShell(body)
	if err == nil || !strings.Contains(string(output), "all sources after 3 attempts each") {
		t.Fatalf("exhaustion result = %v, output %q; want bounded source exhaustion", err, output)
	}
	if got := readFile(t, countFile); got != "6\n" {
		t.Fatalf("wget calls = %q, want three per source", got)
	}
	if got := readFile(t, os.Getenv("SLEEP_LOG")); got != "1\n2\n1\n2\n" {
		t.Errorf("per-source retry backoff = %q, want 1,2 seconds for each source", got)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(countFile), "tar-called")); !os.IsNotExist(err) {
		t.Errorf("tar ran after every source failed (stat error %v)", err)
	}
}

func TestRenderDockerfileArtifactChecksEveryCacheBeforeDownloading(t *testing.T) {
	const (
		primaryDigest  = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		fallbackDigest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	df := mustRenderDockerfile(t, plan.BuildPlan{
		BaseImage: "node:24-alpine", TargetArch: "amd64", RuntimeTools: []plan.RuntimeTool{artifactPnpmToolWithFallback()},
	})
	workDir := t.TempDir()
	body := artifactRunBody(t, df, primaryDigest, workDir)
	commands, countFile := artifactShellCommands(t)
	cacheDir := filepath.Join(workDir, "cache")
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// A bad primary cache must be rejected, then the verified fallback cache
	// must be selected before any source is downloaded.
	if err := os.WriteFile(filepath.Join(cacheDir, primaryDigest+".tar.gz"), []byte("CORRUPT"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cacheDir, fallbackDigest+".tar.gz"), []byte("VALID"), 0o644); err != nil {
		t.Fatal(err)
	}
	configureArtifactShell(t, commands, countFile, primaryDigest+","+fallbackDigest, "failure")

	if output, err := runArtifactShell(body); err != nil {
		t.Fatalf("artifact RUN with a verified fallback cache: %v\n%s", err, output)
	}
	if _, err := os.Stat(countFile); !os.IsNotExist(err) {
		t.Errorf("a source was downloaded despite a valid cache (stat error %v)", err)
	}
	if _, err := os.Stat(filepath.Join(cacheDir, primaryDigest+".tar.gz")); !os.IsNotExist(err) {
		t.Errorf("unverified primary cache was retained (stat error %v)", err)
	}
	if got := readFile(t, os.Getenv("TAR_ARGS")); !strings.HasSuffix(got, "package/pnpm\n") {
		t.Errorf("extraction member = %q, want the verified fallback member", got)
	}
	if got := readFile(t, filepath.Join(workDir, "bin", "pnpm")); got != "" {
		t.Errorf("installed pnpm contents = %q, want the verified fallback archive member", got)
	}
}

func artifactRunBody(t *testing.T, dockerfile, digest, workDir string) string {
	t.Helper()
	const mount = "RUN --mount=type=cache,id=relay-artifact-"
	start := strings.Index(dockerfile, mount)
	if start < 0 {
		t.Fatalf("Dockerfile has no artifact RUN:\n%s", dockerfile)
	}
	rest := dockerfile[start:]
	lineEnd := strings.IndexByte(rest, '\n')
	if lineEnd < 0 {
		t.Fatalf("artifact RUN header has no heredoc body: %q", rest)
	}
	header := strings.TrimSpace(rest[:lineEnd])
	if !strings.Contains(header, "sharing=locked <<'RELAY_ARTIFACT'") {
		t.Fatalf("artifact RUN has no expected locked cache mount/heredoc: %q", header)
	}
	if !strings.Contains(header, digest) {
		t.Fatalf("artifact RUN is not keyed by expected digest %q: %s", digest, header)
	}
	const terminator = "\nRELAY_ARTIFACT\n"
	bodyStart := lineEnd + 1
	bodyEnd := strings.Index(rest[bodyStart:], terminator)
	if bodyEnd < 0 {
		t.Fatalf("artifact RUN has no closing RELAY_ARTIFACT delimiter:\n%s", rest)
	}
	body := rest[bodyStart : bodyStart+bodyEnd+1]
	body = strings.ReplaceAll(body, "/var/cache/relay-artifacts", filepath.Join(workDir, "cache"))
	body = strings.ReplaceAll(body, "/tmp/relay-tool-0.tar.gz", filepath.Join(workDir, "artifact.tar.gz"))
	body = strings.ReplaceAll(body, "/tmp/relay-tool-0.unpack", filepath.Join(workDir, "unpack"))
	body = strings.ReplaceAll(body, "/usr/local/bin/pnpm", filepath.Join(workDir, "bin", "pnpm"))
	body = strings.ReplaceAll(body, "/usr/local/bin", filepath.Join(workDir, "bin"))
	return body
}

func artifactShellCommands(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	countFile := filepath.Join(dir, "wget-count")
	commands := filepath.Join(dir, "bin")
	if err := os.Mkdir(commands, 0o755); err != nil {
		t.Fatal(err)
	}
	writeCommand := func(name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(commands, name), []byte("#!/bin/sh\n"+body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	writeCommand("busybox", strings.Join([]string{
		"set -eu",
		"[ \"$1\" = timeout ] && [ \"$2\" = -k ] && [ \"$3\" = 2 ] && [ \"$4\" = 25 ] || exit 2",
		"shift 4",
		"[ \"$1\" = busybox ] || exit 2",
		"shift",
		"[ \"$1\" = wget ] || exit 2",
		"shift",
		"output=",
		"url=",
		"while [ \"$#\" -gt 0 ]; do",
		"  if [ \"$1\" = -O ]; then shift; output=$1; fi",
		"  case \"$1\" in https://*) url=$1 ;; esac",
		"  shift",
		"done",
		"count=$(cat \"$WGET_COUNT\" 2>/dev/null || echo 0)",
		"count=$((count + 1))",
		"printf '%s\\n' \"$count\" > \"$WGET_COUNT\"",
		"if [ \"$url\" = https://example.test/pnpm-x64.tgz ]; then source=primary; else source=fallback; fi",
		"if [ \"$source\" = primary ]; then source_count=$(cat \"$WGET_PRIMARY_COUNT\" 2>/dev/null || echo 0); else source_count=$(cat \"$WGET_FALLBACK_COUNT\" 2>/dev/null || echo 0); fi",
		"source_count=$((source_count + 1))",
		"if [ \"$source\" = primary ]; then printf '%s\\n' \"$source_count\" > \"$WGET_PRIMARY_COUNT\"; else printf '%s\\n' \"$source_count\" > \"$WGET_FALLBACK_COUNT\"; fi",
		"if [ \"$WGET_MODE\" = failure ]; then exit 1; fi",
		"if [ \"$WGET_MODE\" = transient ] && [ \"$count\" -eq 1 ]; then exit 1; fi",
		"if [ \"$WGET_MODE\" = primary-fails ] && [ \"$source\" = primary ]; then exit 1; fi",
		"if [ \"$WGET_MODE\" = primary-mismatch ] && [ \"$source\" = primary ]; then printf INVALID > \"$output\"; exit 0; fi",
		"if [ \"$WGET_MODE\" = fallback-mismatch ] && [ \"$source\" = primary ]; then exit 1; fi",
		"if [ \"$WGET_MODE\" = fallback-mismatch ] && [ \"$source\" = fallback ]; then printf INVALID > \"$output\"; exit 0; fi",
		"if [ \"$WGET_MODE\" = mismatch ]; then printf INVALID > \"$output\"; else printf VALID > \"$output\"; fi",
	}, "\n")+"\n")
	writeCommand("sha256sum", strings.Join([]string{
		"set -eu",
		"[ \"$1\" = -c ] && [ \"$2\" = - ] || exit 2",
		"IFS= read -r line",
		"expected=${line%%  *}",
		"file=${line#*  }",
		"expected_ok=no",
		"old_ifs=$IFS; IFS=,; for allowed in $EXPECTED_DIGEST; do [ \"$expected\" = \"$allowed\" ] && expected_ok=yes; done; IFS=$old_ifs",
		"[ \"$expected_ok\" = yes ] && [ \"$(cat \"$file\")\" = VALID ]",
	}, "\n")+"\n")
	writeCommand("sleep", "printf '%s\\n' \"$1\" >> \"$SLEEP_LOG\"\n")
	writeCommand("tar", strings.Join([]string{
		"printf called > \"$TAR_CALLED\"",
		"printf '%s\\n' \"$@\" > \"$TAR_ARGS\"",
		"destination=",
		"member=",
		"while [ \"$#\" -gt 0 ]; do",
		"  if [ \"$1\" = -C ]; then shift; destination=$1; fi",
		"  case \"$1\" in -*) ;; *) member=$1 ;; esac",
		"  shift",
		"done",
		"mkdir -p \"$destination/$(dirname \"$member\")\"",
		": > \"$destination/$member\"",
	}, "\n")+"\n")
	t.Setenv("TAR_CALLED", filepath.Join(dir, "tar-called"))
	t.Setenv("TAR_ARGS", filepath.Join(dir, "tar-args"))
	t.Setenv("SLEEP_LOG", filepath.Join(dir, "sleep-log"))
	t.Setenv("WGET_PRIMARY_COUNT", filepath.Join(dir, "primary-count"))
	t.Setenv("WGET_FALLBACK_COUNT", filepath.Join(dir, "fallback-count"))
	return commands, countFile
}

func configureArtifactShell(t *testing.T, commands, countFile, digest, mode string) {
	t.Helper()
	t.Setenv("PATH", commands+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("WGET_COUNT", countFile)
	t.Setenv("EXPECTED_DIGEST", digest)
	t.Setenv("WGET_MODE", mode)
}

func runArtifactShell(body string) ([]byte, error) {
	cmd := exec.Command("/bin/sh", "-c", body)
	cmd.Env = os.Environ()
	return cmd.CombinedOutput()
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(contents)
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
	if !strings.Contains(df, "https://example.test/pnpm-arm64.tgz") {
		t.Errorf("arm64 render must reference the arm64 artifact:\n%s", df)
	}
	if strings.Contains(df, "pnpm-x64.tgz") {
		t.Errorf("arm64 render must not reference the amd64 artifact:\n%s", df)
	}
	if !strings.Contains(df, "id=relay-artifact-"+strings.Repeat("b", 64)) {
		t.Errorf("arm64 cache identity must be keyed by its artifact checksum:\n%s", df)
	}
	if strings.Contains(df, "id=relay-artifact-"+strings.Repeat("a", 64)) {
		t.Errorf("arm64 cache identity must not reuse the amd64 artifact cache:\n%s", df)
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
	base := artifactPnpmToolWithFallback()
	v := base.Artifact.Variants[0]

	cases := []struct {
		name   string
		mutate func(*plan.ArtifactVariant)
	}{
		{"traversal member", func(x *plan.ArtifactVariant) { x.Member = "../etc/passwd" }},
		{"absolute member", func(x *plan.ArtifactVariant) { x.Member = "/etc/passwd" }},
		{"newline member", func(x *plan.ArtifactVariant) { x.Member = "pnpm\nRUN touch /tmp/injected" }},
		{"option member", func(x *plan.ArtifactVariant) { x.Member = "--checkpoint=1" }},
		{"backend member", func(x *plan.ArtifactVariant) { x.Member = `a\b` }},
		{"non-https url", func(x *plan.ArtifactVariant) { x.URL = "http://example.test/x.tgz" }},
		{"NUL in primary url", func(x *plan.ArtifactVariant) { x.URL = "https://example.test/x\x00.tgz" }},
		{"ESC in primary url", func(x *plan.ArtifactVariant) { x.URL = "https://example.test/x\x1b.tgz" }},
		{"short digest", func(x *plan.ArtifactVariant) { x.SHA256 = "abc" }},
		{"uppercase digest", func(x *plan.ArtifactVariant) { x.SHA256 = strings.Repeat("A", 64) }},
		{"NUL in fallback url", func(x *plan.ArtifactVariant) { x.Fallbacks[0].URL = "https://example.test/x\x00.tgz" }},
		{"ESC in fallback url", func(x *plan.ArtifactVariant) { x.Fallbacks[0].URL = "https://example.test/x\x1b.tgz" }},
		{"fallback newline member", func(x *plan.ArtifactVariant) {
			x.Fallbacks[0].Member = "package/pnpm\nRUN touch /tmp/injected"
		}},
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

func TestRenderDockerfileArtifactAcceptsURLQueryAndPunctuation(t *testing.T) {
	tool := artifactPnpmTool()
	tool.Artifact.Variants[0].URL = "https://example.test/pnpm.tgz?download=1&signature=abc%2Fdef!$(),;:@"
	p := plan.BuildPlan{
		BaseImage:    "node:24-alpine",
		TargetArch:   "amd64",
		RuntimeTools: []plan.RuntimeTool{tool},
	}
	if _, err := renderDockerfile(p); err != nil {
		t.Fatalf("renderDockerfile rejected a normal HTTPS artifact URL with query/percent/punctuation: %v", err)
	}
}

// TestRenderDockerfileArtifactRejectsUnsafeDestination verifies destinations
// cannot inject Dockerfile instructions or terminate the static-delimiter
// artifact heredoc, and rejects other control characters as well.
func TestRenderDockerfileArtifactRejectsUnsafeDestination(t *testing.T) {
	for _, tc := range []struct {
		name        string
		destination string
	}{
		{name: "newline heredoc delimiter", destination: "/usr/local/bin/pnpm\nRELAY_ARTIFACT\nRUN touch /tmp/injected"},
		{name: "tab", destination: "/usr/local/bin/pnpm\tunsafe"},
		{name: "NUL", destination: "/usr/local/bin/pnpm\x00unsafe"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tool := artifactPnpmTool()
			tool.Destination = tc.destination
			p := plan.BuildPlan{
				BaseImage:    "node:24-alpine",
				TargetArch:   "amd64",
				RuntimeTools: []plan.RuntimeTool{tool},
			}
			if _, err := renderDockerfile(p); err == nil {
				t.Fatalf("renderDockerfile accepted unsafe artifact destination %q", tc.destination)
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
	invalid := []string{"", "/pnpm", "-pnpm", `a\b`, "pnpm\nRUN x", "pnpm\rx", "pnpm\x00x", ".", "..", "a/../b", "a//b", "a/./b", "a/", "./a"}
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
	opts := buildImageOptions("relay-app-test:abc123", nil, "session-1")
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
	if err := os.WriteFile(filepath.Join(src, "template.yaml"), []byte("runtime: nodejs24.x\n"), 0o644); err != nil {
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
	write("template.yaml", "runtime: nodejs24.x\n")
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

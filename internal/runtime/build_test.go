package runtime

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
	opts := buildImageOptions("relay-fn-test:abc123", nil)

	if !opts.Remove {
		t.Error("expected Remove=true so the daemon removes intermediate containers after a " +
			"successful build (moby client v0.6.0 emits rm=0 when Remove is false)")
	}
	if opts.ForceRemove {
		t.Error("expected ForceRemove=false: failed builds must keep their intermediates for debugging")
	}
	if len(opts.Tags) != 1 || opts.Tags[0] != "relay-fn-test:abc123" {
		t.Errorf("Tags = %v, want [relay-fn-test:abc123]", opts.Tags)
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
	labels := map[string]string{labelType: ImageTypeFunction, labelFunction: "a", labelDependency: "relay-dep-abc"}
	opts := buildImageOptions("relay-fn-a:abc123", labels)

	if got := opts.Labels[labelType]; got != ImageTypeFunction {
		t.Errorf("opts.Labels[relay.type] = %q, want %q", got, ImageTypeFunction)
	}
	if got := opts.Labels[labelFunction]; got != "a" {
		t.Errorf("opts.Labels[relay.function] = %q, want a", got)
	}
	if got := opts.Labels[labelDependency]; got != "relay-dep-abc" {
		t.Errorf("opts.Labels[relay.dependency] = %q, want relay-dep-abc", got)
	}
}

func TestDockerfileTemplateEmbedsFunctionSource(t *testing.T) {
	p := plan.BuildPlan{
		BaseImage: "node:24-alpine",
		WorkDir:   "/app",
		Files: []plan.File{
			{Path: "/relay/bootstrap.mjs", Content: []byte("x"), Mode: fs.FileMode(0o644)},
		},
		Install:    []string{"npm ci --omit=dev"},
		Entrypoint: []string{"node", "/relay/bootstrap.mjs"},
	}

	df := renderDockerfile(p)

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
	if !strings.Contains(df, "RUN npm ci --omit=dev") {
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
	df := renderDockerfile(p)
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

	df := renderDockerfile(p)

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

// TestRenderDockerfileToolCopies verifies external tool copies (the uv binary)
// are emitted as `COPY --from=<image> <src> <dest>` BEFORE the install RUN, so
// the install can use the tool. The order is the whole point: a copy after the
// install would make uv unavailable at install time.
func TestRenderDockerfileToolCopies(t *testing.T) {
	p := plan.BuildPlan{
		BaseImage: "python:3.14-slim",
		WorkDir:   "/app",
		ToolCopies: []plan.ImageCopy{{
			From: "ghcr.io/astral-sh/uv:0.12.17", Source: "/uv", Dest: "/usr/local/bin/uv",
		}},
		Install: []string{"uv pip install --system --no-cache -r requirements.txt"},
	}

	df := renderDockerfile(p)
	wantLine := "COPY --from=ghcr.io/astral-sh/uv:0.12.17 /uv /usr/local/bin/uv"
	if !strings.Contains(df, wantLine) {
		t.Fatalf("expected tool copy line %q, got:\n%s", wantLine, df)
	}
	copyIdx := strings.Index(df, wantLine)
	installIdx := strings.Index(df, "RUN uv pip install")
	if copyIdx < 0 || installIdx < 0 || copyIdx > installIdx {
		t.Errorf("tool copy must precede the install RUN (copy=%d install=%d):\n%s", copyIdx, installIdx, df)
	}
	// Never a floating tag: the renderer copies the pinned reference verbatim.
	if strings.Contains(df, "uv:latest") {
		t.Errorf("tool copy must not use a floating latest tag:\n%s", df)
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
	df := renderDockerfile(p)
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
// USER, Env, or ENTRYPOINT — a dependency image is a base for the function
// image, not a runnable function.
func TestRenderDockerfileDependencyBase(t *testing.T) {
	// buildDependencyImage builds this plan (note Deps is intentionally zero so
	// the renderer emits the plain COPY . path, not a nested dep base). The
	// dependency base image is built FROM the raw runtime base, so it must copy
	// the runtime's tool (uv) itself — this is how the dependency install gets
	// uv even though it does not inherit it from a function image.
	p := plan.BuildPlan{
		BaseImage: "python:3.14-slim",
		WorkDir:   "/app",
		ToolCopies: []plan.ImageCopy{{
			From: "ghcr.io/astral-sh/uv:0.12.17", Source: "/uv", Dest: "/usr/local/bin/uv",
		}},
		Install: []string{"uv pip install --system --no-cache -r requirements.txt"},
	}
	df := renderDockerfile(p)

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
		t.Errorf("expected uv tool copy line in the dependency base, got:\n%s", df)
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

// TestRenderDockerfileFunctionFromDependency verified the MANAGER rewrites the
// function image's FROM to the dependency reference when Deps are present; this
// test asserts the plan the manager hands to renderDockerfile produces the right
// FROM and no install RUN (engine moved install into Deps).
func TestRenderDockerfileFunctionFromDependency(t *testing.T) {
	p := plan.BuildPlan{
		BaseImage:  "relay-dep-abcdef1234567890",
		WorkDir:    "/app",
		Files:      []plan.File{{Path: "/relay/bootstrap.py", Content: []byte("x"), Mode: fs.FileMode(0o644)}},
		UserSetup:  "groupadd -g 10001 app && useradd -u 10001 -g 10001 app && chown -R 10001:10001 /app /relay",
		User:       "10001:10001",
		Entrypoint: []string{"python", "/relay/bootstrap.py"},
	}
	df := renderDockerfile(p)

	if !strings.Contains(df, "FROM relay-dep-abcdef1234567890") {
		t.Errorf("function image must build FROM the dependency image, got:\n%s", df)
	}
	// The install is in the dependency layer; the function image has no install
	// RUN of its own.
	if strings.Contains(df, "RUN uv pip install") {
		t.Errorf("function image built FROM the dep layer must not re-run install, got:\n%s", df)
	}
	// The user setup is still needed (the dep layer's /app contents are owned by
	// root; chown hands them to the runtime user), and the user switch + entry
	// point stay in the function layer.
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
	df := renderDockerfile(p)
	if !strings.Contains(df, `ENTRYPOINT ["program", "--flag value"]`) {
		t.Errorf("expected argument with spaces preserved, got:\n%s", df)
	}
}

func TestQuoteEntrypointJSON(t *testing.T) {
	got := quoteEntrypointJSON([]string{"python", "/relay/bootstrap.py"})
	if got != `["python", "/relay/bootstrap.py"]` {
		t.Errorf("quoteEntrypointJSON = %q, want JSON array", got)
	}
}

// TestCopyDirSkipsTemplateYaml verifies the build-context staging excludes
// template.yaml (Relay configuration, not function source) while copying every
// other file. This is what keeps env values and secret references out of the
// image layers.
func TestCopyDirSkipsTemplateYaml(t *testing.T) {
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
	dst := t.TempDir()
	if err := copySourceDir(selection, dst); err != nil {
		t.Fatalf("copySourceDir: %v", err)
	}

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

// TestCopySourceDirHonorsSelection verifies the build context stages exactly the
// selected source: files excluded by the function's .gitignore never reach the
// image, the applicable .gitignore itself does, and template.yaml is still
// excluded (Relay configuration must not leak into a layer).
func TestCopySourceDirHonorsSelection(t *testing.T) {
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
	dst := t.TempDir()
	if err := copySourceDir(selection, dst); err != nil {
		t.Fatalf("copySourceDir: %v", err)
	}

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

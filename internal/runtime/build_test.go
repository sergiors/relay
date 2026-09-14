package runtime

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/runtime/plan"
)

// TestBuildImageOptionsRemoveIntermediateContainers verifies that Relay's build
// options request the daemon to remove intermediate containers after a
// successful build. The moby client v0.6.0 emits rm=0 when Remove is false
// (suppressing the daemon's default cleanup), so Remove must be true to restore
// rm=1. It also asserts ForceRemove is NOT set: failed builds must keep their
// intermediates for debugging.
func TestBuildImageOptionsRemoveIntermediateContainers(t *testing.T) {
	opts := buildImageOptions("relay-fn-test:abc123")

	if !opts.Remove {
		t.Error("expected Remove=true so the daemon removes intermediate containers after a successful build (moby client v0.6.0 emits rm=0 when Remove is false)")
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
}

func TestRenderDockerfile(t *testing.T) {
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
func TestRenderDockerfileHardening(t *testing.T) {
	p := plan.BuildPlan{
		BaseImage: "python:3.14-slim",
		WorkDir:   "/app",
		Files: []plan.File{
			{Path: "/relay/bootstrap.py", Content: []byte("x"), Mode: fs.FileMode(0o644)},
		},
		Install:    []string{"pip install --no-cache-dir -r requirements.txt"},
		UserSetup:  "groupadd -g 10001 app && useradd -u 10001 -g 10001 -m -d /home/app -s /usr/sbin/nologin app && chown -R 10001:10001 /app /relay",
		User:       "10001:10001",
		Entrypoint: []string{"python", "/relay/bootstrap.py"},
	}

	df := renderDockerfile(p)

	// The full expected shape, in order.
	want := `FROM python:3.14-slim
WORKDIR /app
COPY . /app
COPY relay/bootstrap.py /relay/
RUN pip install --no-cache-dir -r requirements.txt
RUN groupadd -g 10001 app && useradd -u 10001 -g 10001 -m -d /home/app -s /usr/sbin/nologin app && chown -R 10001:10001 /app /relay
USER 10001:10001
ENTRYPOINT ["python", "/relay/bootstrap.py"]
`
	if df != want {
		t.Errorf("rendered dockerfile mismatch.\n--- got ---\n%s\n--- want ---\n%s", df, want)
	}

	// The USER line must come after the user-setup RUN and after the install RUN.
	userIdx := strings.Index(df, "USER 10001:10001")
	setupIdx := strings.Index(df, "RUN groupadd")
	installIdx := strings.Index(df, "RUN pip install")
	if userIdx < 0 || setupIdx < 0 || installIdx < 0 {
		t.Fatalf("expected USER, user-setup RUN, and install RUN lines, got:\n%s", df)
	}
	if !(installIdx < setupIdx && setupIdx < userIdx) {
		t.Errorf("expected order install RUN < user-setup RUN < USER, got install=%d setup=%d user=%d", installIdx, setupIdx, userIdx)
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
	// the renderer emits the plain COPY . path, not a nested dep base).
	p := plan.BuildPlan{
		BaseImage: "python:3.14-slim",
		WorkDir:   "/app",
		Install:   []string{"pip install --no-cache-dir -r requirements.txt"},
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
	if !strings.Contains(df, "RUN pip install --no-cache-dir -r requirements.txt") {
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
	if strings.Contains(df, "RUN pip install") {
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

	dst := t.TempDir()
	if err := copyDir(src, dst, map[string]bool{"template.yaml": true}); err != nil {
		t.Fatalf("copyDir: %v", err)
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

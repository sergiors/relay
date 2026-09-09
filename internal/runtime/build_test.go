package runtime

import (
	"io/fs"
	"strings"
	"testing"

	"relay/internal/runtime/plan"
)

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

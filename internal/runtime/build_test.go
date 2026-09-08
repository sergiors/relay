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

package runtime

import (
	"path/filepath"
	"strconv"
	"strings"

	"relay/internal/runtime/plan"
)

// renderDockerfile is the ONLY Dockerfile renderer, shared by every engine; an
// engine must express its concerns as plan data rather than generate a
// Dockerfile. It emits FROM/WORKDIR/COPY/RUN/USER/ENTRYPOINT from the plan.
func renderDockerfile(p plan.BuildPlan) string {
	var b strings.Builder

	b.WriteString("FROM " + p.BaseImage + "\n")
	if p.WorkDir != "" {
		b.WriteString("WORKDIR " + p.WorkDir + "\n")
	}

	// Function sources are copied into WorkDir.
	dest := p.WorkDir
	if dest == "" {
		dest = "/"
	}
	b.WriteString("COPY . " + dest + "\n")

	// The builder mirrors each file's absolute Path into a relative context path
	// (leading '/' stripped), so COPY sources that same relative path and writes
	// it into the file's parent directory in the image.
	for _, f := range p.Files {
		rel := strings.TrimPrefix(filepath.Clean(f.Path), "/")
		dir := "/"
		if i := strings.LastIndex(f.Path, "/"); i > 0 {
			dir = f.Path[:i]
		}
		b.WriteString("COPY " + rel + " " + dir + "/\n")
	}

	for _, cmd := range p.Install {
		if cmd != "" {
			b.WriteString("RUN " + cmd + "\n")
		}
	}

	// UserSetup runs as root after Install so dependency installation (which
	// needs root for site-packages/node_modules) is unaffected by the runtime
	// user. It creates the user and hands the writable paths to it.
	if p.UserSetup != "" {
		b.WriteString("RUN " + p.UserSetup + "\n")
	}
	if p.User != "" {
		b.WriteString("USER " + p.User + "\n")
	}

	if len(p.Entrypoint) > 0 {
		b.WriteString("ENTRYPOINT " + quoteEntrypointJSON(p.Entrypoint) + "\n")
	}
	return b.String()
}

// quoteEntrypointJSON renders an ENTRYPOINT as a JSON array so that arguments
// with special characters are preserved.
func quoteEntrypointJSON(args []string) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, a := range args {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(strconv.Quote(a))
	}
	b.WriteByte(']')
	return b.String()
}

// Package plan holds the concrete, shared types that describe a runtime and
// its build plan. It lives between the runtime package (which orchestrates
// Prepare/Execute) and the language-specific engines so an engine can build a
// plan without importing the package that dispatches to it, avoiding an import
// cycle.
package plan

import "io/fs"

type Engine string

const (
	EnginePython Engine = "python"
	EngineNode   Engine = "node"
)

type Spec struct {
	Name      string
	Engine    Engine
	BaseImage string
}

// File is a file the builder mirrors into the build context: Path is the
// absolute destination in the image (e.g. "/relay/bootstrap.py"), and the
// Dockerfile renders a COPY that writes it there.
type File struct {
	Path    string
	Content []byte
	Mode    fs.FileMode
}

// BuildPlan is how a function directory becomes an image. Engines answer "what
// does this runtime need?" by producing a plan; the Docker builder answers "how
// do I build the image?" by rendering it into a single generic Dockerfile.
type BuildPlan struct {
	BaseImage string
	WorkDir   string
	// Files are additional files for the builder to write (the bootstrap, an
	// injected package.json, etc.).
	Files []File
	// Install are shell commands run inside the image at build time (e.g. "pip
	// install ..."). Empty when there is nothing to install.
	Install []string
	// Entrypoint is the container entrypoint as a JSON-array ENTRYPOINT.
	Entrypoint []string
}

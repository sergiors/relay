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
	// UserSetup is a single RUN command that creates the runtime user and
	// prepares the writable paths it needs (e.g. "groupadd ... && useradd ...
	// && chown ..."). It runs as root AFTER Install so dependency installation
	// is unaffected. Empty when the image should keep its default user.
	UserSetup string
	// User is the USER instruction rendered after UserSetup (e.g. "10001:10001").
	// Empty when no USER line should be emitted (back-compat with unhardened
	// plans).
	User string
	// Env are environment variables applied to the execution container at
	// runtime (not build time). They are merged after the base RELAY_HANDLER
	// variable. Empty when the runtime needs no extra environment.
	Env []string
	// Entrypoint is the container entrypoint as a JSON-array ENTRYPOINT.
	Entrypoint []string
}

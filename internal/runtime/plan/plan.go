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
	// ToolCopies are external-image COPY --from directives every image for this
	// runtime needs (e.g. a pinned tool binary). They are applied to BOTH the
	// function image and its dependency base image, because the dependency image
	// is built FROM BaseImage (not from the function image) and still needs the
	// tool to run its install. Empty when the runtime needs no external tool.
	ToolCopies []ImageCopy
}

// File is a file the builder mirrors into the build context: Path is the
// absolute destination in the image (e.g. "/relay/bootstrap.py"), and the
// Dockerfile renders a COPY that writes it there.
type File struct {
	Path    string
	Content []byte
	Mode    fs.FileMode
}

// ImageCopy is a build-time COPY --from directive that pulls a file out of an
// EXTERNAL image into the image being built (e.g. the pinned uv binary). It is
// how a runtime acquires a versioned external tool without changing its base
// image; the single generic Dockerfile renderer emits it, so engines express
// the copy as plan data rather than a Dockerfile.
type ImageCopy struct {
	// From is the source image reference. Callers should pin it (tag or
	// digest); a moving tag would make otherwise identical builds differ.
	From string
	// Source is the path copied out of From (e.g. "/uv").
	Source string
	// Dest is the destination path in the image being built (e.g.
	// "/usr/local/bin/uv").
	Dest string
}

// BuildPlan is how a function directory becomes an image. Engines answer "what
// does this runtime need?" by producing a plan; the Docker builder answers "how
// do I build the image?" by rendering it into a single generic Dockerfile.
// Deps describes the function's dependencies as a reusable layer: the manifest
// files the engine reads (paths relative to the function dir), the install
// command, and the directory the dependencies land in. Zero value = no deps.
type Deps struct {
	// Files are the dependency manifest files, RELATIVE to the function dir
	// (e.g. "requirements.txt"; node: "package.json", "package-lock.json").
	Files []string
	// Install is the shell command that installs the dependencies from the
	// manifest files into InstallDir, run inside the dependency-image build.
	Install string
	// Dir is the absolute path the dependencies are installed into (the layer's
	// payload; e.g. /app). It must equal the function image's WorkDir so a
	// FROM of the dependency image inherits everything in place.
	Dir string
}

// IsZero reports whether no dependency layer is declared. Files is the driving
// field (an install into an empty manifest set is meaningless); it also lets
// callers compare a Deps value without relying on slice comparability.
func (d Deps) IsZero() bool {
	return len(d.Files) == 0
}

// Equal reports whether two Deps describe the same dependency layer. It exists
// because Deps contains slices, which Go cannot compare with ==; tests and
// callers use it instead.
func (d Deps) Equal(o Deps) bool {
	if d.Install != o.Install || d.Dir != o.Dir || len(d.Files) != len(o.Files) {
		return false
	}
	for i := range d.Files {
		if d.Files[i] != o.Files[i] {
			return false
		}
	}
	return true
}

type BuildPlan struct {
	BaseImage string
	WorkDir   string
	// Files are additional files for the builder to write (the bootstrap, an
	// injected package.json, etc.).
	Files []File
	// Deps is the function's reusable dependency layer (manifest files +
	// install command + install directory). When non-zero, the builder renders
	// a separate dependency image whose contents are installed into Deps.Dir,
	// and the function image's Dockerfile builds FROM that dependency image
	// instead of BaseImage. Zero value = no dependency layer.
	Deps Deps
	// Install are shell commands run inside the image at build time. With the
	// dependency install moved into Deps, engines that have no dependency stage
	// leave this empty; it remains for any future non-dependency build step.
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
	// ToolCopies are build-time COPY --from directives pulling external tool
	// binaries into this image (see Spec.ToolCopies). The builder applies them
	// before the dependency install so the install can use the tool. Empty when
	// the image needs no external tool.
	ToolCopies []ImageCopy
	// Entrypoint is the container entrypoint as a JSON-array ENTRYPOINT.
	Entrypoint []string
}

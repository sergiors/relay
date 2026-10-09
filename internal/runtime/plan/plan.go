// Package plan holds the concrete, shared types that describe a runtime and
// its build plan. It lives between the runtime package (which orchestrates
// Prepare/Execute) and the language-specific engines so an engine can build a
// plan without importing the package that dispatches to it, avoiding an import
// cycle.
package plan

import (
	"fmt"
	"io/fs"
)

type Engine string

const (
	EnginePython Engine = "python"
	EngineNode   Engine = "node"
)

type Spec struct {
	Name      string
	Engine    Engine
	BaseImage string
	// MountableSource reports whether this runtime's dependency layout lets the
	// app source be bind-mounted read-only instead of baked (SOURCE_MOUNT)
	// WITHOUT hiding installed dependencies. It is true for Python (whose
	// dependencies live outside the workdir, in the system site-packages) and for
	// Node (whose dependencies stay under the workdir while the live source is
	// mounted at a distinct path OUTSIDE the dependency tree; see
	// BuildPlan.SourceMountTarget). A false value means the source is always
	// copied into the image, exactly as when SOURCE_MOUNT is disabled.
	MountableSource bool
	// SourceMounted is the RESOLVED per-preparation SOURCE_MOUNT decision: the
	// global switch is enabled AND this runtime is MountableSource. It is set by
	// the caller (runtime.Manager.prepare) on the plan's value copy and lets an
	// engine shape a source-mounted image (e.g. Node persists its pinned esbuild
	// for runtime TypeScript transpilation instead of transpiling at build time).
	// A false value means the historical baked-source image, byte-for-byte.
	SourceMounted bool
	// RuntimeTools are the external tools this runtime requires in every image
	// it builds (e.g. a pinned binary such as uv or pnpm). A RuntimeTool is an
	// external dependency of the runtime, not a Dockerfile concept; the builder
	// materializes each one (as an image COPY or, for a remote archive, a
	// download/verify/extract RUN — an implementation detail) and the type names
	// the tool's source and destination. They are applied to BOTH the app image
	// and its dependency base image, because the dependency image is built FROM
	// BaseImage (not from the app image) and still needs the tool to run its
	// install. Empty when the runtime needs no external tool.
	RuntimeTools []RuntimeTool
}

// File is a file the builder mirrors into the build context: Path is the
// absolute destination in the image (e.g. "/relay/bootstrap.py"), and the
// Dockerfile renders a COPY that writes it there.
type File struct {
	Path    string
	Content []byte
	Mode    fs.FileMode
}

// RuntimeTool is an external tool a runtime requires in its images (e.g. the
// pinned uv binary or the pinned pnpm binary). It is how a runtime acquires a
// versioned external tool without changing its base image. A tool is expressed
// in exactly one of two forms, and the single generic Dockerfile renderer
// materializes it (an implementation detail of the builder rather than part of
// the type's meaning), so engines express the tool as plan data rather than a
// Dockerfile:
//
//   - an image copy (From/Source set): the tool already exists in a pinned
//     external image and is copied out of it (uv);
//   - a remote archive artifact (Artifact set): the tool is a file inside a
//     pinned, checksummed archive downloaded at build time (pnpm's standalone
//     musl binary). Artifact is per-architecture because the archive differs by
//     target GOARCH.
//
// Destination is common to both forms: the absolute path the tool is written to
// in the image being built (e.g. "/usr/local/bin/uv").
type RuntimeTool struct {
	// From is the source image reference the tool is taken from in the image-copy
	// form. Callers should pin it (tag or digest); a moving tag would make
	// otherwise identical builds differ. Empty in the artifact form.
	From string
	// Source is the path copied out of From in the image-copy form (e.g. "/uv").
	// Empty in the artifact form.
	Source string
	// Destination is the path the tool is written to in the image being built
	// (e.g. "/usr/local/bin/uv").
	Destination string
	// Artifact, when non-nil, is the remote-archive form: a pinned archive per
	// target architecture whose Member is extracted to Destination. Mutually
	// exclusive with From/Source.
	Artifact *RuntimeArtifact
}

// ValidateForm reports whether the tool is expressed in exactly one of its two
// forms: an image copy (From/Source set, Artifact nil) or a remote archive
// artifact (Artifact set, From/Source empty). A tool that sets both forms — or
// neither — is rejected rather than one form silently winning. It is checked
// everywhere a tool is resolved (rendering, dependency fingerprinting, and the
// bootstrap identity hash), so a malformed tool can never be rendered with one
// form while it is hashed as another.
func (t RuntimeTool) ValidateForm() error {
	hasImage := t.From != "" || t.Source != ""
	switch {
	case t.Artifact != nil && hasImage:
		return fmt.Errorf("runtime tool %q: From/Source and Artifact are mutually exclusive", t.Destination)
	case t.Artifact == nil && !hasImage:
		return fmt.Errorf("runtime tool %q: one of From/Source or Artifact must be set", t.Destination)
	}
	return nil
}

// RuntimeArtifact is the remote-archive form of a RuntimeTool: a pinned archive
// per target architecture, downloaded and checksum-verified at image-build time.
// The archive is a gzip-compressed tar (`.tar.gz`) and only its named Member is
// extracted to the tool's Destination; the builder's renderer invokes
// `tar -xzf`, so a plain (uncompressed) tar is not accepted.
type RuntimeArtifact struct {
	// Variants are the per-architecture artifacts, in plan order. Exactly one
	// must match the build's target GOARCH; a tool with no matching variant
	// fails the build (never silently omitted).
	Variants []ArtifactVariant
}

// ArtifactVariant is one architecture's pinned archive artifact.
type ArtifactVariant struct {
	// Arch is the target GOARCH this variant serves (e.g. "amd64", "arm64").
	Arch string
	// URL is the pinned download URL of the archive. It should be immutable
	// (a versioned release asset), because SHA256 is the integrity check.
	URL string
	// SHA256 is the lowercase hex SHA-256 of the downloaded archive. The build
	// verifies it and fails closed on a mismatch.
	SHA256 string
	// Member is the path of the file to extract from the archive, relative to
	// the archive root (e.g. "pnpm"). It must be a clean relative path; the
	// renderer rejects traversal, absolute, and option-like members.
	Member string
}

// VariantForArch returns the artifact variant for arch, reporting false when the
// artifact declares none (an unsupported target architecture the caller must
// fail on rather than build a wrong-architecture image).
func (a *RuntimeArtifact) VariantForArch(arch string) (ArtifactVariant, bool) {
	if a == nil {
		return ArtifactVariant{}, false
	}
	for _, v := range a.Variants {
		if v.Arch == arch {
			return v, true
		}
	}
	return ArtifactVariant{}, false
}

// BuildPlan is how an app directory becomes an image. Engines answer "what
// does this runtime need?" by producing a plan; the Docker builder answers "how
// do I build the image?" by rendering it into a single generic Dockerfile.
// Deps describes the app's dependencies as a reusable layer: the manifest
// files the engine reads (paths relative to the app dir), the install
// command, and the directory the dependencies land in. Zero value = no deps.
type Deps struct {
	// Files are the dependency manifest files, RELATIVE to the app dir
	// (e.g. "requirements.txt"; node: "package.json", "pnpm-lock.yaml").
	Files []string
	// Install is the shell command that installs the dependencies from the
	// manifest files into InstallDir, run inside the dependency-image build.
	Install string
	// Dir is the absolute path the dependencies are installed into (the layer's
	// payload; e.g. /app). It must equal the app image's WorkDir so a
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
	// TargetArch is the GOARCH the image is built for (e.g. "amd64", "arm64").
	// It selects a RuntimeTool's artifact variant and is passed to the Docker
	// build as the target platform, so an arch-specific tool and the image it is
	// baked into can never disagree. Empty only for plans that carry no artifact
	// tool (the renderer needs it only to resolve one).
	TargetArch string
	// Files are additional files for the builder to write (the bootstrap, an
	// injected package.json, etc.).
	Files []File
	// Deps is the app's reusable dependency layer (manifest files +
	// install command + install directory). When non-zero, the builder renders
	// a separate dependency image whose contents are installed into Deps.Dir,
	// and the app image's Dockerfile builds FROM that dependency image
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
	// RuntimeTools are the external tools this image requires (see
	// Spec.RuntimeTools). The builder materializes them before the dependency
	// install so the install can use the tool. Empty when the image needs no
	// external tool.
	RuntimeTools []RuntimeTool
	// Entrypoint is the container entrypoint as a JSON-array ENTRYPOINT.
	Entrypoint []string
	// SourceMountTarget, when non-empty, is the in-container path a SOURCE_MOUNT
	// bind of the live app source is mounted at, distinct from WorkDir when the
	// runtime keeps dependencies (and generated files) under WorkDir so the
	// mount cannot hide them (Node mounts the source at WorkDir/src while
	// /app/node_modules stays visible). Empty means the source is mounted at
	// WorkDir (Python) or baked, depending on the caller's SOURCE_MOUNT
	// decision.
	SourceMountTarget string
	// SourceMountWorkDir, when non-empty, is the in-container working directory
	// a SOURCE_MOUNT container runs with, so process.cwd() and relative
	// filesystem operations see the app's SOURCE ROOT. It is set only when that
	// root differs from the image WorkDir (Node's source root is /app/src);
	// empty preserves the image WorkDir (Python, whose source root equals its
	// workdir, and every baked image).
	SourceMountWorkDir string
	// SourceMountMasks are in-container paths masked with an empty, read-only
	// filesystem for a SOURCE_MOUNT container, so a bind of the live app
	// directory can never shadow a path the image itself owns. Node masks
	// <SourceMountTarget>/node_modules when the host already carries it, as
	// best-effort defense-in-depth; the shared resolve hook (written to
	// node.ResolveHookPath and preloaded by the bootstrap and by a mounted Node
	// entrypoint service) is the unconditional guarantee that bare imports and
	// requires resolve from the dependency image's /app/node_modules, so a host
	// node_modules that appears only after preparation cannot shadow it either.
	// Empty means no mask.
	SourceMountMasks []string
}

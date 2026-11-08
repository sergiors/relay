package runtime

import (
	"fmt"
	"sort"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"relay/internal/runtime/plan"
)

// UvImageTag is the pinned official uv distroless image tag Relay takes the uv
// binary from. It is a package constant (and a test asserts it) so the version
// is explicit, deterministic, and upgrades are a one-line change — never a
// floating "latest".
const UvImageTag = "ghcr.io/astral-sh/uv:0.12.17"

// uvTool is the external tool the Python runtime requires: the pinned uv binary.
// The distroless uv image contains only the binaries, so a single copy of /uv
// is the whole tool. It is intentionally applied to every Python image
// (including one with no dependencies) so `uv` is always present in the runtime
// and the dependency image can inherit it.
var uvTool = plan.RuntimeTool{
	From:        UvImageTag,
	Source:      "/uv",
	Destination: "/usr/local/bin/uv",
}

// PnpmVersion is the pinned pnpm release Relay provides in every Node image. It
// is a package constant (and a test asserts it) so the version is explicit,
// deterministic, and upgrades are a one-line change — never a floating
// "latest". pnpm 12 ships self-contained, statically linked musl binaries, so
// Relay downloads the official Linux musl artifact directly instead of copying
// a JS distribution out of an image and wrapping it around the base Node
// binary.
const PnpmVersion = "12.10.1"

// pnpmReleaseBaseURL is the immutable GitHub release download root for the
// pinned pnpm version. Each artifact is content-addressed by the SHA-256
// recorded in pnpmTool, so the build verifies exactly the release bytes.
const pnpmReleaseBaseURL = "https://github.com/pnpm/pnpm/releases/download/v" + PnpmVersion + "/"

// pnpmMember is the archive member holding the standalone pnpm executable, and
// pnpmDestination is where every Node image exposes it. The binary is
// statically linked (musl), so it runs on the Node Alpine base images
// (node:24-alpine and node:26-alpine) with no wrapper and no dependency on the
// base image's Node interpreter.
const (
	pnpmMember      = "pnpm"
	pnpmDestination = "/usr/local/bin/pnpm"
)

// pnpmTool is the external tool required by the Node runtime: the pinned pnpm
// standalone binary, acquired as a checksum-pinned remote archive per target
// architecture. Only the musl amd64 and arm64 artifacts are supported; a build
// for any other architecture fails rather than baking a wrong-architecture
// binary.
var pnpmTool = plan.RuntimeTool{
	Destination: pnpmDestination,
	Artifact: &plan.RuntimeArtifact{Variants: []plan.ArtifactVariant{
		{
			Arch:   "amd64",
			URL:    pnpmReleaseBaseURL + "pnpm-linux-x64-musl.tar.gz",
			SHA256: "f2ad050a705ab433e6c1c4fd57d584462896062b048e9d29a90548ad6d91f91b",
			Member: pnpmMember,
			Fallbacks: []plan.ArtifactSource{{
				URL:    "https://registry.npmjs.org/@pnpm/exe.linux-x64-musl/-/exe.linux-x64-musl-" + PnpmVersion + ".tgz",
				SHA256: "7313bee7b14e8e2ccffec28c11dc1302640a5e8de9f75625dab07ebab244c3ea",
				Member: "package/pnpm",
			}},
		},
		{
			Arch:   "arm64",
			URL:    pnpmReleaseBaseURL + "pnpm-linux-arm64-musl.tar.gz",
			SHA256: "906259045aaef37a3fe6145fabdc1cc3195da523a788900875bf97be3b40333e",
			Member: pnpmMember,
			Fallbacks: []plan.ArtifactSource{{
				URL:    "https://registry.npmjs.org/@pnpm/exe.linux-arm64-musl/-/exe.linux-arm64-musl-" + PnpmVersion + ".tgz",
				SHA256: "0c1f2736a856a766044d7a8706e60aa1284ea17080606897d3a5167caa5c0c55",
				Member: "package/pnpm",
			}},
		},
	}},
}

// Supported runtimes. Runtime versions exist only here; adding a version (e.g.
// python3.15 or nodejs26.x) needs just another entry. Both engines are
// version-generic, so a new version differs only in its base image and the
// template identifier. The map key is the template runtime identifier; Name is
// the identity hashed into dependency fingerprints and stamped on images, so
// the two must stay in lockstep.
var specs = map[string]plan.Spec{
	"python3.14": {
		Name:         "python3.14",
		Engine:       plan.EnginePython,
		BaseImage:    "python:3.14-slim",
		RuntimeTools: []plan.RuntimeTool{uvTool},
		// Python installs dependencies in system site-packages, outside /app,
		// so mounting live source at /app does not hide runtime dependencies.
		MountableSource: true,
	},
	"python3.15": {
		Name:         "python3.15",
		Engine:       plan.EnginePython,
		BaseImage:    "python:3.15-slim",
		RuntimeTools: []plan.RuntimeTool{uvTool},
		// See python3.14: dependencies live outside the mounted source.
		MountableSource: true,
	},
	"nodejs24.x": {
		Name:      "nodejs24.x",
		Engine:    plan.EngineNode,
		BaseImage: "node:24-alpine",
		// Node installs dependencies with the pinned standalone pnpm binary
		// (see pnpmTool).
		RuntimeTools: []plan.RuntimeTool{pnpmTool},
		// Node keeps dependencies under /app, so live source is mounted at
		// /app/src to keep /app/node_modules available.
		MountableSource: true,
	},
	"nodejs26.x": {
		Name:            "nodejs26.x",
		Engine:          plan.EngineNode,
		BaseImage:       "node:26-alpine",
		RuntimeTools:    []plan.RuntimeTool{pnpmTool},
		MountableSource: true,
	},
}

// RuntimeInfo is the immutable, read-only identity of one managed runtime. It
// deliberately exposes only the configured identity and base image; the build
// plan, external tools, and engine-specific shape stay internal to the runtime
// package. The slice returned by Runtimes is a copy, so a caller cannot mutate
// the registry.
type RuntimeInfo struct {
	// ID is the template runtime identifier (e.g. "python3.14").
	ID string
	// Engine is the language engine that prepares this runtime.
	Engine plan.Engine
	// BaseImage is the EXACT configured base image reference (without a
	// digest). A runtime image selection must bind to this value unchanged.
	BaseImage string
	// TargetPlatform is the OCI platform every image for this runtime is built
	// for. It comes from the same buildTargetPlatform contract used by image
	// builds and dependency fingerprints, not a second architecture mapping.
	TargetPlatform ocispec.Platform
}

// Runtimes returns every configured managed runtime sorted by ID. The result is
// a fresh immutable snapshot: adding, removing, or reordering entries in the
// returned slice cannot change the registry. It is the single listing used by
// the CLI, so no command duplicates the runtime IDs.
func Runtimes() []RuntimeInfo {
	target := buildTargetPlatform()
	infos := make([]RuntimeInfo, 0, len(specs))
	for id, spec := range specs {
		infos = append(infos, RuntimeInfo{
			ID:             id,
			Engine:         spec.Engine,
			BaseImage:      spec.BaseImage,
			TargetPlatform: target,
		})
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].ID < infos[j].ID })
	return infos
}

// LookupRuntime resolves one runtime ID against the immutable registry. The
// boolean reports whether the ID is configured; an unknown ID is never treated
// as an unpinned runtime.
func LookupRuntime(name string) (RuntimeInfo, bool) {
	spec, ok := specs[name]
	if !ok {
		return RuntimeInfo{}, false
	}
	return RuntimeInfo{
		ID:             name,
		Engine:         spec.Engine,
		BaseImage:      spec.BaseImage,
		TargetPlatform: buildTargetPlatform(),
	}, true
}

func lookup(name string) (plan.Spec, error) {
	return lookupIn(specs, name)
}

// lookupIn resolves name against an explicit spec table. It exists so tests can
// exercise multi-version resolution on a copied table without mutating the
// global production registry.
func lookupIn(table map[string]plan.Spec, name string) (plan.Spec, error) {
	spec, ok := table[name]
	if !ok {
		return plan.Spec{}, fmt.Errorf("unsupported runtime %q", name)
	}
	return spec, nil
}

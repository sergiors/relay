package runtime

import (
	"fmt"

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
// statically linked (musl), so it runs on node:24-alpine with no wrapper and no
// dependency on the base image's Node interpreter.
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
		},
		{
			Arch:   "arm64",
			URL:    pnpmReleaseBaseURL + "pnpm-linux-arm64-musl.tar.gz",
			SHA256: "906259045aaef37a3fe6145fabdc1cc3195da523a788900875bf97be3b40333e",
			Member: pnpmMember,
		},
	}},
}

// Supported runtimes. Runtime versions exist only here; adding a version (e.g.
// python3.15 or node26) needs just another entry.
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
	"node24": {
		Name:      "node24",
		Engine:    plan.EngineNode,
		BaseImage: "node:24-alpine",
		// Node installs dependencies with pnpm (copied from the official pnpm
		// image; see pnpmTool for the Alpine-compatibility blocker).
		RuntimeTools: []plan.RuntimeTool{pnpmTool},
		// Node keeps dependencies under /app, so live source is mounted at
		// /app/src to keep /app/node_modules available.
		MountableSource: true,
	},
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

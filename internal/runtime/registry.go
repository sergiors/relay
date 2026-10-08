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

// PnpmImageTag is the pinned official pnpm image tag Relay takes the pnpm
// JavaScript CLI distribution from. It is a package constant (and a test
// asserts it) so the version is explicit, deterministic, and upgrades are a
// one-line change — never a floating "latest". pnpm 11 is used deliberately:
// its image ships a runnable JS CLI at /opt/pnpm/dist, whereas pnpm 12's image
// ships only a glibc-native binary that cannot run on node:24-alpine.
const PnpmImageTag = "ghcr.io/pnpm/pnpm:11.24.0"

// pnpmTool is the external tool the Node runtime requires: the pinned pnpm JS
// CLI distribution, copied out of the official pnpm image. The WHOLE
// /opt/pnpm/dist directory is copied because pnpm's CLI resolves its worker and
// vendored modules relative to its own location. The engine creates the
// /usr/local/bin/pnpm wrapper (see node.pnpmWrapperInstall) so the runtime has
// a directly-executable pnpm; the dependency image invokes the CLI directly via
// `node /opt/pnpm/dist/pnpm.mjs`.
//
// The official pnpm 12 image is glibc/Debian and its native binary cannot run
// on node:24-alpine (musl); pnpm 11's JS CLI runs under the base image's Node.
var pnpmTool = plan.RuntimeTool{
	From:        PnpmImageTag,
	Source:      "/opt/pnpm/dist",
	Destination: "/opt/pnpm/dist",
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

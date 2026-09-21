package runtime

import (
	"fmt"

	"relay/internal/runtime/plan"
)

// UvImageTag is the pinned official uv distroless image tag Relay copies the uv
// binary from. It is a package constant (and a test asserts it) so the version
// is explicit, deterministic, and upgrades are a one-line change — never a
// floating "latest".
const UvImageTag = "ghcr.io/astral-sh/uv:0.12.17"

// uvImage is the external image the Python runtime copies its uv binary from.
// The distroless uv image contains only the binaries, so a single copy of /uv
// is the whole installation. It is intentionally applied to every Python image
// (including one with no dependencies) so `uv` is always present in the runtime
// and the dependency image can inherit it.
var uvImage = plan.ImageCopy{
	From:   UvImageTag,
	Source: "/uv",
	Dest:   "/usr/local/bin/uv",
}

// Supported runtimes. Runtime versions exist only here; adding a version (e.g.
// python3.15 or node26) needs just another entry.
var specs = map[string]plan.Spec{
	"python3.14": {
		Name:       "python3.14",
		Engine:     plan.EnginePython,
		BaseImage:  "python:3.14-slim",
		ToolCopies: []plan.ImageCopy{uvImage},
	},
	"node24": {
		Name:      "node24",
		Engine:    plan.EngineNode,
		BaseImage: "node:24-alpine",
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

package runtime

import (
	"fmt"

	"relay/internal/runtime/plan"
)

// Supported runtimes. Runtime versions exist only here; adding a version (e.g.
// python3.15 or node26) needs just another entry.
var specs = map[string]plan.Spec{
	"python3.14": {
		Name:      "python3.14",
		Engine:    plan.EnginePython,
		BaseImage: "python:3.14-slim",
	},
	"node24": {
		Name:      "node24",
		Engine:    plan.EngineNode,
		BaseImage: "node:24-alpine",
	},
}

func lookup(name string) (plan.Spec, error) {
	spec, ok := specs[name]
	if !ok {
		return plan.Spec{}, fmt.Errorf("unsupported runtime %q", name)
	}
	return spec, nil
}

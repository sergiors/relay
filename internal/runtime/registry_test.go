package runtime

import (
	"strings"
	"testing"

	"relay/internal/runtime/plan"
)

// TestLookupResolvesRuntimeSpecAndRejectsUnknown verifies the runtime registry
// resolves each supported runtime to its engine + base image and rejects an
// unknown runtime with an "unsupported runtime" error.
func TestLookupResolvesRuntimeSpecAndRejectsUnknown(t *testing.T) {
	spec, err := lookup("python3.14")
	if err != nil {
		t.Fatalf("lookup python3.14: %v", err)
	}
	if spec.Engine != plan.EnginePython {
		t.Errorf("python3.14 engine = %q, want %q", spec.Engine, plan.EnginePython)
	}
	if spec.BaseImage != "python:3.14-slim" {
		t.Errorf("python3.14 base image = %q, want python:3.14-slim", spec.BaseImage)
	}

	spec, err = lookup("node24")
	if err != nil {
		t.Fatalf("lookup node24: %v", err)
	}
	if spec.Engine != plan.EngineNode {
		t.Errorf("node24 engine = %q, want %q", spec.Engine, plan.EngineNode)
	}
	if spec.BaseImage != "node:24-alpine" {
		t.Errorf("node24 base image = %q, want node:24-alpine", spec.BaseImage)
	}

	if _, err := lookup("ruby5"); err == nil {
		t.Error("lookup(ruby5) succeeded, want error")
	} else if !strings.Contains(err.Error(), "unsupported runtime") {
		t.Errorf(`lookup(ruby5) error = %q, want "unsupported runtime"`, err)
	}
}

// TestPythonSpecPinsUvImage verifies the Python runtime declares the pinned uv
// tool copy (source ghcr.io/astral-sh/uv:<pinned>) so every Python image has the
// uv binary. It also guards against a floating "latest" tag: the version must
// be an explicit semver so builds are reproducible.
func TestPythonSpecPinsUvImage(t *testing.T) {
	spec, err := lookup("python3.14")
	if err != nil {
		t.Fatalf("lookup python3.14: %v", err)
	}
	if len(spec.ToolCopies) != 1 {
		t.Fatalf("python3.14 tool copies = %+v, want exactly the uv copy", spec.ToolCopies)
	}
	tc := spec.ToolCopies[0]
	if tc.From != UvImageTag {
		t.Errorf("uv image = %q, want %q", tc.From, UvImageTag)
	}
	if tc.Source != "/uv" || tc.Dest != "/usr/local/bin/uv" {
		t.Errorf("uv copy = %+v, want /uv -> /usr/local/bin/uv", tc)
	}
	if !strings.HasPrefix(tc.From, "ghcr.io/astral-sh/uv:") {
		t.Errorf("uv image = %q, want the official ghcr.io/astral-sh/uv source", tc.From)
	}
	version := strings.TrimPrefix(tc.From, "ghcr.io/astral-sh/uv:")
	if version == "latest" || version == "" {
		t.Errorf("uv image version = %q, must be a pinned version, never latest", version)
	}
	if strings.Count(version, ".") != 2 {
		t.Errorf("uv image version = %q, want a full pinned major.minor.patch version", version)
	}
}

// TestNodeSpecHasNoToolCopies verifies the tool-copy mechanism is opt-in per
// runtime: node does not copy the uv binary.
func TestNodeSpecHasNoToolCopies(t *testing.T) {
	spec, err := lookup("node24")
	if err != nil {
		t.Fatalf("lookup node24: %v", err)
	}
	if len(spec.ToolCopies) != 0 {
		t.Errorf("node24 tool copies = %+v, want none", spec.ToolCopies)
	}
}

// testSpecTable returns a COPY of the production registry with the two
// test-only multi-version specs added, so the shared-engine tests exercise the
// real resolution/dispatch path without mutating the global specs map (which
// races any other test that reads it).
func testSpecTable() (map[string]plan.Spec, string, string) {
	const tempPython = "python3.15-test-only"
	const tempNode = "node26-test-only"
	table := make(map[string]plan.Spec, len(specs)+2)
	for k, v := range specs {
		table[k] = v
	}
	table[tempPython] = plan.Spec{Name: tempPython, Engine: plan.EnginePython, BaseImage: "python:3.15-slim"}
	table[tempNode] = plan.Spec{Name: tempNode, Engine: plan.EngineNode, BaseImage: "node:26-alpine"}
	return table, tempPython, tempNode
}

// A test-only spec can be resolved through the same mechanism (lookupIn on a
// copied table) and share an engine with an existing runtime, without enabling a
// real new runtime. It validates that a single engine serves more than one
// version, in both directions.
func TestEngineForRuntimeResolvesSharedEngine(t *testing.T) {
	table, tempPython, tempNode := testSpecTable()

	// A second Python version shares EnginePython with the production spec.
	py, err := lookupIn(table, tempPython)
	if err != nil {
		t.Fatalf("lookup(%s): %v", tempPython, err)
	}
	if py.Engine != plan.EnginePython {
		t.Errorf("%s engine = %q, want %q (shared engine)", tempPython, py.Engine, plan.EnginePython)
	}
	if specs["python3.14"].Engine != py.Engine {
		t.Errorf("python3.14 and %s do not share an engine", tempPython)
	}

	// A second Node version shares EngineNode with the production spec.
	nd, err := lookupIn(table, tempNode)
	if err != nil {
		t.Fatalf("lookup(%s): %v", tempNode, err)
	}
	if nd.Engine != plan.EngineNode {
		t.Errorf("%s engine = %q, want %q (shared engine)", tempNode, nd.Engine, plan.EngineNode)
	}
	if specs["node24"].Engine != nd.Engine {
		t.Errorf("node24 and %s do not share an engine", tempNode)
	}
}

// TestEngineForSharedEngineDispatch dispatches both the production and the
// test-only specs to the same engine type, so the shared engine property holds
// through the actual dispatch path, not just the registry.
func TestEngineForSharedEngineDispatch(t *testing.T) {
	table, tempPython, tempNode := testSpecTable()

	// Both python specs must produce a plan with the python entrypoint and the
	// spec's own base image; both node specs likewise for node.
	for _, name := range []string{"python3.14", tempPython} {
		spec, err := lookupIn(table, name)
		if err != nil {
			t.Fatalf("lookup(%s): %v", name, err)
		}
		eng, err := engineFor(spec)
		if err != nil {
			t.Fatalf("engineFor(%s): %v", name, err)
		}
		p, err := eng.Plan(spec, t.TempDir(), nil)
		if err != nil {
			t.Fatalf("plan(%s): %v", name, err)
		}
		if p.BaseImage != spec.BaseImage {
			t.Errorf("%s plan base image = %q, want %q", name, p.BaseImage, spec.BaseImage)
		}
		if p.Entrypoint[0] != "python" {
			t.Errorf("%s plan entrypoint = %v, want python entrypoint", name, p.Entrypoint)
		}
	}

	for _, name := range []string{"node24", tempNode} {
		spec, err := lookupIn(table, name)
		if err != nil {
			t.Fatalf("lookup(%s): %v", name, err)
		}
		eng, err := engineFor(spec)
		if err != nil {
			t.Fatalf("engineFor(%s): %v", name, err)
		}
		p, err := eng.Plan(spec, t.TempDir(), nil)
		if err != nil {
			t.Fatalf("plan(%s): %v", name, err)
		}
		if p.BaseImage != spec.BaseImage {
			t.Errorf("%s plan base image = %q, want %q", name, p.BaseImage, spec.BaseImage)
		}
		if len(p.Entrypoint) != 2 || p.Entrypoint[0] != "node" {
			t.Errorf("%s plan entrypoint = %v, want node entrypoint", name, p.Entrypoint)
		}
	}
}

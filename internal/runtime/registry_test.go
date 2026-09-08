package runtime

import (
	"strings"
	"testing"

	"relay/internal/runtime/plan"
)

func TestImageRef(t *testing.T) {
	// imageRef is a plain concatenation: names are validated at load time, so
	// every name below is already safe for a docker tag.
	cases := []struct {
		in   string
		want string
	}{
		{"user-events", "relay-fn-user-events"},
		{"welcome_email", "relay-fn-welcome_email"},
		{"jobs.v2", "relay-fn-jobs.v2"},
	}
	for _, tc := range cases {
		if got := imageRef(tc.in); got != tc.want {
			t.Errorf("imageRef(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestImageRefNoCollisionSanitization(t *testing.T) {
	// Names that the old sanitizer would have collapsed together (e.g. "my fn"
	// and "my-fn") are now rejected at load time rather than normalized, so
	// imageRef never needs a disambiguating hash suffix.
	if got, want := imageRef("my-fn"), "relay-fn-my-fn"; got != want {
		t.Errorf("imageRef(my-fn) = %q, want %q", got, want)
	}
}

func TestLookup(t *testing.T) {
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

// A test-only spec can be registered temporarily through the same mechanism
// (the specs map) and share an engine with an existing runtime, without enabling
// a real new runtime. It validates that a single engine serves more than one
// version, in both directions.
func TestLookupSharedEngine(t *testing.T) {
	const tempPython = "python3.15-test-only"
	const tempNode = "node26-test-only"
	defer delete(specs, tempPython)
	defer delete(specs, tempNode)
	specs[tempPython] = plan.Spec{Name: tempPython, Engine: plan.EnginePython, BaseImage: "python:3.15-slim"}
	specs[tempNode] = plan.Spec{Name: tempNode, Engine: plan.EngineNode, BaseImage: "node:26-alpine"}

	// A second Python version shares EnginePython with the production spec.
	py, err := lookup(tempPython)
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
	nd, err := lookup(tempNode)
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

// engineFor dispatches both the production and the test-only specs to the same
// engine type, so the shared engine property holds through the actual dispatch
// path, not just the registry.
func TestEngineForSharedEngine(t *testing.T) {
	const tempPython = "python3.15-test-only"
	const tempNode = "node26-test-only"
	defer delete(specs, tempPython)
	defer delete(specs, tempNode)
	specs[tempPython] = plan.Spec{Name: tempPython, Engine: plan.EnginePython, BaseImage: "python:3.15-slim"}
	specs[tempNode] = plan.Spec{Name: tempNode, Engine: plan.EngineNode, BaseImage: "node:26-alpine"}

	// Both python specs must produce a plan with the python entrypoint and the
	// spec's own base image; both node specs likewise for node.
	for _, name := range []string{"python3.14", tempPython} {
		spec, err := lookup(name)
		if err != nil {
			t.Fatalf("lookup(%s): %v", name, err)
		}
		eng, err := engineFor(spec)
		if err != nil {
			t.Fatalf("engineFor(%s): %v", name, err)
		}
		p, err := eng.Plan(spec, t.TempDir())
		if err != nil {
			t.Fatalf("plan(%s): %v", name, err)
		}
		if p.BaseImage != spec.BaseImage {
			t.Errorf("%s plan base image = %q, want %q", name, p.BaseImage, spec.BaseImage)
		}
		if len(p.Entrypoint) != 2 || p.Entrypoint[0] != "python" {
			t.Errorf("%s plan entrypoint = %v, want python entrypoint", name, p.Entrypoint)
		}
	}

	for _, name := range []string{"node24", tempNode} {
		spec, err := lookup(name)
		if err != nil {
			t.Fatalf("lookup(%s): %v", name, err)
		}
		eng, err := engineFor(spec)
		if err != nil {
			t.Fatalf("engineFor(%s): %v", name, err)
		}
		p, err := eng.Plan(spec, t.TempDir())
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

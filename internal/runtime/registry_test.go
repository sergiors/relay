package runtime

import (
	"strings"
	"testing"

	"relay/internal/runtime/plan"
)

// TestImageRef verifies the fingerprint-versioned format
// "relay-fn-<name>:<16hex>" and that it is deterministic.
func TestImageRef(t *testing.T) {
	fp := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	cases := []struct {
		name, fp, want string
	}{
		{"user-events", fp, "relay-fn-user-events:0123456789abcdef"},
		{"welcome_email", fp, "relay-fn-welcome_email:0123456789abcdef"},
		{"jobs.v2", fp, "relay-fn-jobs.v2:0123456789abcdef"},
		// The tag is only the first 16 hex chars; the rest is dropped.
		{"a", "abcdef1234567890xyz", "relay-fn-a:abcdef1234567890"},
	}
	for _, tc := range cases {
		if got := ImageRef(tc.name, tc.fp); got != tc.want {
			t.Errorf("ImageRef(%q) = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestImageRefDeterministic asserts the same (name, fingerprint) always maps to
// the same reference, and distinct fingerprints map to distinct references (so
// two source versions can never collide on one image tag).
func TestImageRefDeterministic(t *testing.T) {
	fp1 := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	fp2 := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if ImageRef("fn", fp1) != ImageRef("fn", fp1) {
		t.Fatal("same fingerprint must yield the same reference")
	}
	if ImageRef("fn", fp1) == ImageRef("fn", fp2) {
		t.Fatal("distinct fingerprints must yield distinct references")
	}
}

// TestImageRefShortFingerprint verifies no panic and a sensible prefix when the
// fingerprint is shorter than 16 chars (defensive; production fingerprints are
// always 64 hex chars).
func TestImageRefShortFingerprint(t *testing.T) {
	if got, want := ImageRef("fn", "abc"), "relay-fn-fn:abc"; got != want {
		t.Errorf("ImageRef(fn, abc) = %q, want %q", got, want)
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

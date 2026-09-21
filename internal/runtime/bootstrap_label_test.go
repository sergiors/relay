package runtime

import (
	"context"
	"testing"

	"relay/internal/runtime/plan"
	"relay/internal/testutil"
)

// TestBootstrapHashIsDeterministic verifies the same plan always yields the same
// 16-hex bootstrap hash, so image labels are stable across builds/processes.
func TestBootstrapHashIsDeterministic(t *testing.T) {
	p := plan.BuildPlan{
		Files: []plan.File{
			{Path: "/relay/bootstrap.py", Content: []byte("print('boot')")},
			{Path: "/relay/package.json", Content: []byte(`{"type":"module"}`)},
		},
		Entrypoint: []string{"python", "/relay/bootstrap.py"},
	}
	a := bootstrapHash(p)
	b := bootstrapHash(p)
	if a != b {
		t.Fatalf("bootstrapHash not deterministic: %s != %s", a, b)
	}
	if len(a) != 16 {
		t.Errorf("bootstrapHash length = %d, want 16 hex chars", len(a))
	}
}

// TestBootstrapHashChangesWithInjectedContent verifies every content input the
// hash pins changes the digest: a file content change, a file path change, and
// an entrypoint change each yield a different hash. This is what lets Prepare
// detect an image built with a stale bootstrap under an unchanged source tag.
func TestBootstrapHashChangesWithInjectedContent(t *testing.T) {
	base := plan.BuildPlan{
		Files:      []plan.File{{Path: "/relay/bootstrap.py", Content: []byte("boot-v1")}},
		Entrypoint: []string{"python", "/relay/bootstrap.py"},
	}
	orig := bootstrapHash(base)

	contentChanged := base
	contentChanged.Files = []plan.File{{Path: "/relay/bootstrap.py", Content: []byte("boot-v2")}}
	if got := bootstrapHash(contentChanged); got == orig {
		t.Error("changing injected file content must change the bootstrap hash")
	}

	pathChanged := base
	pathChanged.Files = []plan.File{{Path: "/relay/other.py", Content: []byte("boot-v1")}}
	if got := bootstrapHash(pathChanged); got == orig {
		t.Error("changing an injected file path must change the bootstrap hash")
	}

	entryChanged := base
	entryChanged.Entrypoint = []string{"python", "/relay/other.py"}
	if got := bootstrapHash(entryChanged); got == orig {
		t.Error("changing the entrypoint must change the bootstrap hash")
	}

	// The pinned external tool (uv) is part of the image content: a uv version
	// bump must change the hash so an otherwise source-current image is rebuilt.
	toolChanged := base
	toolChanged.ToolCopies = []plan.ImageCopy{{From: "ghcr.io/astral-sh/uv:0.12.17", Source: "/uv", Dest: "/usr/local/bin/uv"}}
	if got := bootstrapHash(toolChanged); got == orig {
		t.Error("changing the external tool copy must change the bootstrap hash")
	}
	toolBumped := toolChanged
	toolBumped.ToolCopies = []plan.ImageCopy{{From: "ghcr.io/astral-sh/uv:0.12.18", Source: "/uv", Dest: "/usr/local/bin/uv"}}
	if got := bootstrapHash(toolBumped); got == bootstrapHash(toolChanged) {
		t.Error("changing the pinned tool version must change the bootstrap hash")
	}
}

// TestBootstrapHashPinsFileBoundaries verifies the hash length-prefixes/separates
// fields so two different plans cannot collide by concatenation ("ab"+"c" vs
// "a"+"bc").
func TestBootstrapHashPinsFileBoundaries(t *testing.T) {
	a := plan.BuildPlan{Files: []plan.File{{Path: "ab", Content: []byte("c")}}}
	b := plan.BuildPlan{Files: []plan.File{{Path: "a", Content: []byte("bc")}}}
	if bootstrapHash(a) == bootstrapHash(b) {
		t.Error("distinct (path, content) splits must not collide under the flat hash")
	}
}

// TestBootstrapLabelMatches pins bootstrapLabelMatches' conservative contract
// against a scripted daemon: a matching label is accepted; a stale/missing label
// and an inspect failure both report a mismatch (rebuild) so a stale-bootstrap
// image is never reused. An empty want short-circuits to true.
func TestBootstrapLabelMatches(t *testing.T) {
	const image = "relay-fn-a:0123456789abcdef"

	newMgr := func(t *testing.T, body string, status int) *Manager {
		t.Helper()
		cli := newScriptedDockerClient(t, dockerRoute{
			method: "GET", path: "/images/", body: body, status: status,
		})
		return &Manager{cli: cli, log: testutil.DiscardLogger()}
	}

	t.Run("matching label is accepted", func(t *testing.T) {
		m := newMgr(t, `{"Config":{"Labels":{"relay.bootstrap":"abc123"}}}`, 0)
		if !m.bootstrapLabelMatches(context.Background(), image, "abc123") {
			t.Error("bootstrapLabelMatches = false for a matching label, want true")
		}
	})

	t.Run("stale label forces rebuild", func(t *testing.T) {
		m := newMgr(t, `{"Config":{"Labels":{"relay.bootstrap":"old"}}}`, 0)
		if m.bootstrapLabelMatches(context.Background(), image, "new") {
			t.Error("bootstrapLabelMatches = true for a stale label, want false")
		}
	})

	t.Run("missing label forces rebuild", func(t *testing.T) {
		m := newMgr(t, `{"Config":{"Labels":{"relay.function":"a"}}}`, 0)
		if m.bootstrapLabelMatches(context.Background(), image, "new") {
			t.Error("bootstrapLabelMatches = true for a missing label, want false")
		}
	})

	t.Run("inspect failure forces rebuild", func(t *testing.T) {
		m := newMgr(t, `{"message":"not found"}`, 404)
		if m.bootstrapLabelMatches(context.Background(), image, "new") {
			t.Error("bootstrapLabelMatches = true on inspect failure, want false")
		}
	})

	t.Run("empty want is a no-op", func(t *testing.T) {
		m := &Manager{}
		if !m.bootstrapLabelMatches(context.Background(), image, "") {
			t.Error("bootstrapLabelMatches = false with no bootstrap to pin, want true")
		}
	})
}

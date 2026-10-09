package runtime

import (
	"context"
	"strings"
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
	a, err := bootstrapHash(p, "amd64")
	if err != nil {
		t.Fatalf("bootstrapHash: %v", err)
	}
	b, err := bootstrapHash(p, "amd64")
	if err != nil {
		t.Fatalf("bootstrapHash: %v", err)
	}
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
	hash := func(p plan.BuildPlan) string {
		t.Helper()
		got, err := bootstrapHash(p, "amd64")
		if err != nil {
			t.Fatalf("bootstrapHash: %v", err)
		}
		return got
	}

	base := plan.BuildPlan{
		Files:      []plan.File{{Path: "/relay/bootstrap.py", Content: []byte("boot-v1")}},
		Entrypoint: []string{"python", "/relay/bootstrap.py"},
	}
	orig := hash(base)

	contentChanged := base
	contentChanged.Files = []plan.File{{Path: "/relay/bootstrap.py", Content: []byte("boot-v2")}}
	if got := hash(contentChanged); got == orig {
		t.Error("changing injected file content must change the bootstrap hash")
	}

	pathChanged := base
	pathChanged.Files = []plan.File{{Path: "/relay/other.py", Content: []byte("boot-v1")}}
	if got := hash(pathChanged); got == orig {
		t.Error("changing an injected file path must change the bootstrap hash")
	}

	entryChanged := base
	entryChanged.Entrypoint = []string{"python", "/relay/other.py"}
	if got := hash(entryChanged); got == orig {
		t.Error("changing the entrypoint must change the bootstrap hash")
	}

	// The pinned external tool (uv) is part of the image content: a uv version
	// bump must change the hash so an otherwise source-current image is rebuilt.
	toolChanged := base
	toolChanged.RuntimeTools = []plan.RuntimeTool{{From: "ghcr.io/astral-sh/uv:0.12.17", Source: "/uv", Destination: "/usr/local/bin/uv"}}
	if got := hash(toolChanged); got == orig {
		t.Error("changing the runtime tool must change the bootstrap hash")
	}
	toolBumped := toolChanged
	toolBumped.RuntimeTools = []plan.RuntimeTool{{From: "ghcr.io/astral-sh/uv:0.12.18", Source: "/uv", Destination: "/usr/local/bin/uv"}}
	if got := hash(toolBumped); got == hash(toolChanged) {
		t.Error("changing the pinned tool version must change the bootstrap hash")
	}

	// The pinned pnpm artifact tool is image content too: its resolved URL,
	// digest, member, or destination must change the hash so an otherwise
	// source-current image is rebuilt.
	pnpmTool := plan.RuntimeTool{
		Destination: "/usr/local/bin/pnpm",
		Artifact: &plan.RuntimeArtifact{Variants: []plan.ArtifactVariant{{
			Arch: "amd64", URL: "https://example.test/pnpm-12.tgz", SHA256: strings.Repeat("a", 64), Member: "pnpm",
		}}},
	}
	pnpmToolChanged := base
	pnpmToolChanged.RuntimeTools = []plan.RuntimeTool{pnpmTool}
	if got := hash(pnpmToolChanged); got == orig {
		t.Error("adding the pnpm artifact runtime tool must change the bootstrap hash")
	}
	pnpmToolBumped := pnpmToolChanged
	bumped := pnpmTool
	bumpedVariant := bumped.Artifact.Variants[0]
	bumpedVariant.SHA256 = strings.Repeat("b", 64)
	bumped.Artifact = &plan.RuntimeArtifact{Variants: []plan.ArtifactVariant{bumpedVariant}}
	pnpmToolBumped.RuntimeTools = []plan.RuntimeTool{bumped}
	if got := hash(pnpmToolBumped); got == hash(pnpmToolChanged) {
		t.Error("changing the pinned pnpm artifact digest must change the bootstrap hash")
	}

	// Build-time Install commands (e.g. the pinned esbuild TypeScript
	// transpilation) are image content too: a different compile step or esbuild
	// version must change the hash so an older image is rebuilt under the same
	// app fingerprint tag.
	installAdded := base
	installAdded.Install = []string{"pnpm add -C /tmp/relay-esbuild --store-dir /tmp/relay-pnpm-store --save-exact --allow-build=esbuild esbuild@0.28.2"}
	if got := hash(installAdded); got == orig {
		t.Error("adding a build Install command must change the bootstrap hash")
	}
	installBumped := installAdded
	installBumped.Install = []string{"pnpm add -C /tmp/relay-esbuild --store-dir /tmp/relay-pnpm-store --save-exact --allow-build=esbuild esbuild@0.29.0"}
	if got := hash(installBumped); got == hash(installAdded) {
		t.Error("changing the pinned esbuild version must change the bootstrap hash")
	}
}

// mustBootstrapHash computes the bootstrap label hash for the package's
// resolved target architecture, failing the test on an unsupported artifact
// tool architecture.
func mustBootstrapHash(t *testing.T, p plan.BuildPlan) string {
	t.Helper()
	h, err := bootstrapHash(p, arch)
	if err != nil {
		t.Fatalf("bootstrapHash: %v", err)
	}
	return h
}

// TestBootstrapHashArtifactUnsupportedArch pins that an artifact runtime tool
// with no variant for the target architecture is an error, never a hash of an
// unresolved tool: an unsupported target must fail rather than produce an image
// with a wrong-architecture binary.
func TestBootstrapHashArtifactUnsupportedArch(t *testing.T) {
	p := plan.BuildPlan{
		RuntimeTools: []plan.RuntimeTool{{
			Destination: "/usr/local/bin/pnpm",
			Artifact: &plan.RuntimeArtifact{Variants: []plan.ArtifactVariant{{
				Arch: "amd64", URL: "https://example.test/pnpm.tgz", SHA256: strings.Repeat("a", 64), Member: "pnpm",
			}}},
		}},
	}
	if _, err := bootstrapHash(p, "riscv64"); err == nil {
		t.Fatal("bootstrapHash with an unsupported artifact architecture must fail")
	}
}

// TestBootstrapHashRejectsMixedRuntimeToolForm verifies the bootstrap identity
// hash rejects a RuntimeTool that mixes image-copy fields with an Artifact, so
// the label can never pin an ambiguous tool.
func TestBootstrapHashRejectsMixedRuntimeToolForm(t *testing.T) {
	p := plan.BuildPlan{
		RuntimeTools: []plan.RuntimeTool{{
			From:        UvImageTag,
			Source:      "/uv",
			Destination: "/usr/local/bin/pnpm",
			Artifact:    &plan.RuntimeArtifact{Variants: []plan.ArtifactVariant{{Arch: "amd64", URL: "https://example.test/x.tgz", SHA256: strings.Repeat("a", 64), Member: "pnpm"}}},
		}},
	}
	if _, err := bootstrapHash(p, "amd64"); err == nil {
		t.Fatal("bootstrapHash accepted a runtime tool mixing From/Source with Artifact")
	}
}

// TestBootstrapHashPinsFileBoundaries verifies the hash length-prefixes/separates
// fields so two different plans cannot collide by concatenation ("ab"+"c" vs
// "a"+"bc").
func TestBootstrapHashPinsFileBoundaries(t *testing.T) {
	a := plan.BuildPlan{Files: []plan.File{{Path: "ab", Content: []byte("c")}}}
	b := plan.BuildPlan{Files: []plan.File{{Path: "a", Content: []byte("bc")}}}
	ha, err := bootstrapHash(a, "amd64")
	if err != nil {
		t.Fatalf("bootstrapHash: %v", err)
	}
	hb, err := bootstrapHash(b, "amd64")
	if err != nil {
		t.Fatalf("bootstrapHash: %v", err)
	}
	if ha == hb {
		t.Error("distinct (path, content) splits must not collide under the flat hash")
	}
}

// TestBootstrapLabelMatches pins bootstrapLabelMatches' conservative contract
// against a scripted daemon: a matching label is accepted; a stale/missing label
// and an inspect failure both report a mismatch (rebuild) so a stale-bootstrap
// image is never reused. An empty want short-circuits to true.
func TestBootstrapLabelMatches(t *testing.T) {
	const image = "relay-app-a:0123456789abcdef"

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
		m := newMgr(t, `{"Config":{"Labels":{"relay.app":"a"}}}`, 0)
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

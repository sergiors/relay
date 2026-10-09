package plan

import "testing"

// TestDepsIsZero verifies the driving field is Files: a Deps with no manifest
// files is zero regardless of the install command/dir, and any manifest makes it
// non-zero.
func TestDepsIsZero(t *testing.T) {
	cases := []struct {
		name string
		deps Deps
		want bool
	}{
		{"zero value", Deps{}, true},
		{"install only", Deps{Install: "pip install", Dir: "/app"}, true},
		{"files present", Deps{Files: []string{"requirements.txt"}}, false},
		{"full", Deps{Files: []string{"package.json"}, Install: "pnpm install --prod --frozen-lockfile", Dir: "/app"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.deps.IsZero(); got != tc.want {
				t.Errorf("IsZero() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestDepsEqualSymmetryAndFieldSensitivity verifies Equal is symmetric and that
// each distinct field (Install, Dir, file name, file order) makes two Deps
// unequal, while identical values compare equal.
func TestDepsEqualSymmetryAndFieldSensitivity(t *testing.T) {
	base := Deps{Files: []string{"package.json", "pnpm-lock.yaml"}, Install: "pnpm install --prod --frozen-lockfile", Dir: "/app"}

	if !base.Equal(base) {
		t.Fatal("Deps must equal itself")
	}

	// Equal must be symmetric.
	same := Deps{Files: []string{"package.json", "pnpm-lock.yaml"}, Install: "pnpm install --prod --frozen-lockfile", Dir: "/app"}
	if !base.Equal(same) || !same.Equal(base) {
		t.Fatal("identical Deps must compare equal in both directions")
	}

	cases := []struct {
		name string
		o    Deps
	}{
		{"install differs", Deps{Files: []string{"package.json", "pnpm-lock.yaml"}, Install: "pnpm install", Dir: "/app"}},
		{"dir differs", Deps{Files: []string{"package.json", "pnpm-lock.yaml"},
			Install: "pnpm install --prod --frozen-lockfile", Dir: "/opt"}},
		{"file name differs", Deps{Files: []string{"package.json", "other-lock.yaml"}, Install: "pnpm install --prod --frozen-lockfile", Dir: "/app"}},
		{"file reordered", Deps{Files: []string{"pnpm-lock.yaml", "package.json"},
			Install: "pnpm install --prod --frozen-lockfile", Dir: "/app"}},
		{"file count differs", Deps{Files: []string{"package.json"}, Install: "pnpm install --prod --frozen-lockfile", Dir: "/app"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if base.Equal(tc.o) || tc.o.Equal(base) {
				t.Errorf("Equal reported true for %+v vs %+v", base, tc.o)
			}
		})
	}
}

// TestRuntimeToolCarriesPinnedReference pins the external-tool shape: the
// source image reference, the file to copy, and the destination are all carried
// verbatim, so an engine can express a pinned (never latest) runtime tool.
func TestRuntimeToolCarriesPinnedReference(t *testing.T) {
	tool := RuntimeTool{From: "ghcr.io/astral-sh/uv:0.12.17", Source: "/uv", Destination: "/usr/local/bin/uv"}
	if tool.From == "" || tool.Source == "" || tool.Destination == "" {
		t.Fatalf("RuntimeTool fields must be carried, got %+v", tool)
	}
}

// TestRuntimeToolValidateForm pins the exactly-one-form invariant: an image
// copy (From/Source) and an artifact (Artifact) are each valid alone, but a
// tool mixing both forms — or declaring neither — is rejected so no resolver
// silently picks one.
func TestRuntimeToolValidateForm(t *testing.T) {
	artifact := &RuntimeArtifact{Variants: []ArtifactVariant{{Arch: "amd64", URL: "https://example.test/x.tgz", SHA256: "aa", Member: "pnpm"}}}

	valid := []struct {
		name string
		tool RuntimeTool
	}{
		{"image copy", RuntimeTool{From: "ghcr.io/astral-sh/uv:0.12.17", Source: "/uv", Destination: "/usr/local/bin/uv"}},
		{"artifact", RuntimeTool{Destination: "/usr/local/bin/pnpm", Artifact: artifact}},
	}
	for _, tc := range valid {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.tool.ValidateForm(); err != nil {
				t.Errorf("ValidateForm(%+v) = %v, want nil", tc.tool, err)
			}
		})
	}

	invalid := []struct {
		name string
		tool RuntimeTool
	}{
		{"both forms", RuntimeTool{From: "ghcr.io/astral-sh/uv:0.12.17", Source: "/uv", Destination: "/usr/local/bin/pnpm", Artifact: artifact}},
		{"from plus artifact", RuntimeTool{From: "ghcr.io/astral-sh/uv:0.12.17", Destination: "/usr/local/bin/pnpm", Artifact: artifact}},
		{"source plus artifact", RuntimeTool{Source: "/uv", Destination: "/usr/local/bin/pnpm", Artifact: artifact}},
		{"neither form", RuntimeTool{Destination: "/usr/local/bin/pnpm"}},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.tool.ValidateForm(); err == nil {
				t.Errorf("ValidateForm(%+v) = nil, want an error", tc.tool)
			}
		})
	}
}

// TestRuntimeArtifactVariantForArch pins the target-architecture selection: a
// matching variant is returned, and a nil artifact or an unknown architecture
// reports false so the caller can fail unsupported rather than build a
// wrong-architecture tool.
func TestRuntimeArtifactVariantForArch(t *testing.T) {
	amd64 := ArtifactVariant{Arch: "amd64", URL: "https://example.test/x.tgz", SHA256: "aa", Member: "pnpm"}
	arm64 := ArtifactVariant{Arch: "arm64", URL: "https://example.test/y.tgz", SHA256: "bb", Member: "pnpm"}
	artifact := &RuntimeArtifact{Variants: []ArtifactVariant{amd64, arm64}}

	if got, ok := artifact.VariantForArch("amd64"); !ok || got != amd64 {
		t.Errorf("VariantForArch(amd64) = %+v, %v; want %+v, true", got, ok, amd64)
	}
	if got, ok := artifact.VariantForArch("arm64"); !ok || got != arm64 {
		t.Errorf("VariantForArch(arm64) = %+v, %v; want %+v, true", got, ok, arm64)
	}
	if _, ok := artifact.VariantForArch("riscv64"); ok {
		t.Error("VariantForArch(riscv64) = true, want false for an unsupported architecture")
	}

	var nilArtifact *RuntimeArtifact
	if _, ok := nilArtifact.VariantForArch("amd64"); ok {
		t.Error("a nil artifact must report no variant")
	}
}

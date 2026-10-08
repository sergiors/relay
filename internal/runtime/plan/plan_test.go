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

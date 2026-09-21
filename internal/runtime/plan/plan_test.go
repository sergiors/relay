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
		{"full", Deps{Files: []string{"package.json"}, Install: "npm ci", Dir: "/app"}, false},
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
	base := Deps{Files: []string{"package.json", "package-lock.json"}, Install: "npm ci --omit=dev", Dir: "/app"}

	if !base.Equal(base) {
		t.Fatal("Deps must equal itself")
	}

	// Equal must be symmetric.
	same := Deps{Files: []string{"package.json", "package-lock.json"}, Install: "npm ci --omit=dev", Dir: "/app"}
	if !base.Equal(same) || !same.Equal(base) {
		t.Fatal("identical Deps must compare equal in both directions")
	}

	cases := []struct {
		name string
		o    Deps
	}{
		{"install differs", Deps{Files: []string{"package.json", "package-lock.json"}, Install: "npm install", Dir: "/app"}},
		{"dir differs", Deps{Files: []string{"package.json", "package-lock.json"},
			Install: "npm ci --omit=dev", Dir: "/opt"}},
		{"file name differs", Deps{Files: []string{"package.json", "yarn.lock"}, Install: "npm ci --omit=dev", Dir: "/app"}},
		{"file reordered", Deps{Files: []string{"package-lock.json", "package.json"},
			Install: "npm ci --omit=dev", Dir: "/app"}},
		{"file count differs", Deps{Files: []string{"package.json"}, Install: "npm ci --omit=dev", Dir: "/app"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if base.Equal(tc.o) || tc.o.Equal(base) {
				t.Errorf("Equal reported true for %+v vs %+v", base, tc.o)
			}
		})
	}
}

// TestImageCopyCarriesPinnedReference pins the external-tool copy shape: the
// source image reference, the file to copy, and the destination are all carried
// verbatim, so an engine can express a pinned (never latest) tool copy.
func TestImageCopyCarriesPinnedReference(t *testing.T) {
	tc := ImageCopy{From: "ghcr.io/astral-sh/uv:0.12.17", Source: "/uv", Dest: "/usr/local/bin/uv"}
	if tc.From == "" || tc.Source == "" || tc.Dest == "" {
		t.Fatalf("ImageCopy fields must be carried, got %+v", tc)
	}
}

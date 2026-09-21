package python

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/runtime/plan"
)

// testUvTag is a stand-in for the production pinned uv image tag. The engine
// treats ToolCopies as opaque plan data passed through from the spec, so any
// pinned tag exercises it; the real pin is asserted in the runtime registry
// tests (runtime.UvImageTag).
const testUvTag = "ghcr.io/astral-sh/uv:0.12.17"

// Two runtime versions served by the same Python engine. The engine must
// produce identical build logic for both, differing only in the base image
// (and tool copies) taken from the spec. The second spec is test-only and is
// never registered in the runtime registry.
var testSpecs = []plan.Spec{
	{
		Name:      "python3.14",
		Engine:    plan.EnginePython,
		BaseImage: "python:3.14-slim",
		ToolCopies: []plan.ImageCopy{{
			From: testUvTag, Source: "/uv", Dest: "/usr/local/bin/uv",
		}},
	},
	{
		Name:      "python3.15",
		Engine:    plan.EnginePython,
		BaseImage: "python:3.15-slim",
		ToolCopies: []plan.ImageCopy{{
			From: testUvTag, Source: "/uv", Dest: "/usr/local/bin/uv",
		}},
	},
}

// write puts a file into dir, failing the test on error.
func write(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

func TestPlanBootstrapAndBase(t *testing.T) {
	for _, spec := range testSpecs {
		t.Run(spec.Name, func(t *testing.T) {
			dir := t.TempDir()

			p, err := Engine{}.Plan(spec, dir, nil)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			if p.BaseImage != spec.BaseImage {
				t.Errorf("base image = %q, want %q", p.BaseImage, spec.BaseImage)
			}
			if p.WorkDir != "/app" {
				t.Errorf("work dir = %q, want /app", p.WorkDir)
			}
			if !p.Deps.IsZero() {
				t.Errorf("expected zero Deps without a manifest, got %+v", p.Deps)
			}
			if len(p.Install) != 0 {
				t.Errorf("expected no build-step install without a manifest, got %v", p.Install)
			}
			wantEntry := []string{"python", "-u", "/relay/bootstrap.py"}
			if len(p.Entrypoint) != len(wantEntry) {
				t.Fatalf("entrypoint = %v, want %v", p.Entrypoint, wantEntry)
			}
			for i := range wantEntry {
				if p.Entrypoint[i] != wantEntry[i] {
					t.Errorf("entrypoint[%d] = %q, want %q", i, p.Entrypoint[i], wantEntry[i])
				}
			}
			if p.User != "10001:10001" {
				t.Errorf("user = %q, want 10001:10001", p.User)
			}
			if !strings.Contains(p.UserSetup, "useradd -u 10001") {
				t.Errorf("user setup = %q, want a useradd for uid 10001", p.UserSetup)
			}
			if len(p.Env) != 1 || p.Env[0] != "PYTHONDONTWRITEBYTECODE=1" {
				t.Errorf("env = %v, want [PYTHONDONTWRITEBYTECODE=1]", p.Env)
			}

			// The uv tool copy is carried through even with no dependencies, so
			// uv is always present in a Python runtime image.
			if len(p.ToolCopies) != 1 || p.ToolCopies[0].Dest != "/usr/local/bin/uv" {
				t.Errorf("tool copies = %+v, want the uv copy", p.ToolCopies)
			}

			var found bool
			for _, f := range p.Files {
				if f.Path == "/relay/bootstrap.py" && string(f.Content) == string(Bootstrap) {
					found = true
					if f.Mode != fs.FileMode(0o644) {
						t.Errorf("bootstrap mode = %v, want 0644", f.Mode)
					}
				}
			}
			if !found {
				t.Error("expected bootstrap file /relay/bootstrap.py in plan, matched on Bootstrap content")
			}
		})
	}
}

// TestPlanWithRequirements pins the classic path: a requirements.txt is
// installed with uv's pip interface into the system environment (not pip, not a
// project-local .venv), and it stays valid even beside an unrelated
// pyproject.toml.
func TestPlanWithRequirements(t *testing.T) {
	for _, spec := range testSpecs {
		t.Run(spec.Name, func(t *testing.T) {
			dir := t.TempDir()
			write(t, dir, "requirements.txt", "# comment\n")

			p, err := Engine{}.Plan(spec, dir, nil)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			if len(p.Install) != 0 {
				t.Errorf("expected the dependency install to move out of Install into Deps, got %v", p.Install)
			}
			want := plan.Deps{
				Files:   []string{"requirements.txt"},
				Install: "uv pip install --system --no-cache -r requirements.txt",
				Dir:     "/app",
			}
			if !p.Deps.Equal(want) {
				t.Errorf("deps = %+v, want %+v", p.Deps, want)
			}
		})
	}
}

// TestPlanRequirementsBesideUnrelatedPyproject verifies the precedence rule:
// with requirements.txt present and NO uv.lock, requirements.txt wins even if a
// (tool-config-only) pyproject.toml also exists.
func TestPlanRequirementsBesideUnrelatedPyproject(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "requirements.txt", "six==1.16.0\n")
	write(t, dir, "pyproject.toml", "[tool.black]\nline-length = 100\n")

	p, err := Engine{}.Plan(testSpecs[0], dir, nil)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	want := plan.Deps{
		Files:   []string{"requirements.txt"},
		Install: "uv pip install --system --no-cache -r requirements.txt",
		Dir:     "/app",
	}
	if !p.Deps.Equal(want) {
		t.Errorf("deps = %+v, want the requirements path %+v", p.Deps, want)
	}
}

// TestPlanNativeUvProject pins the native path: pyproject.toml + uv.lock select
// the frozen export+install workflow, both files participate (so a change to
// either re-fingerprints), and requirements.txt is ignored when a committed
// lock is present.
func TestPlanNativeUvProject(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "pyproject.toml", "[project]\nname = \"x\"\nversion = \"0.1.0\"\n")
	write(t, dir, "uv.lock", "version = 1\n")
	// requirements.txt is deliberately also present: the committed lock must win.
	write(t, dir, "requirements.txt", "six==1.16.0\n")

	p, err := Engine{}.Plan(testSpecs[0], dir, nil)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	want := plan.Deps{
		Files:   []string{"pyproject.toml", "uv.lock"},
		Install: nativeInstall,
		Dir:     "/app",
	}
	if !p.Deps.Equal(want) {
		t.Errorf("deps = %+v, want the native uv path %+v", p.Deps, want)
	}
	for _, banned := range []string{" -r requirements.txt", "pip install --no-cache-dir"} {
		if strings.Contains(p.Deps.Install, banned) {
			t.Errorf("native install must not use %q: %q", banned, p.Deps.Install)
		}
	}
	// The workflow is frozen/locked: it must not resolve a fresh set.
	for _, wantFlag := range []string{"uv export --locked", "--no-emit-project", "uv pip install --system"} {
		if !strings.Contains(p.Deps.Install, wantFlag) {
			t.Errorf("native install %q missing %q", p.Deps.Install, wantFlag)
		}
	}
}

// TestPlanNativeIncompleteErrors pins the deterministic incomplete-native
// behavior: uv.lock without pyproject.toml and pyproject.toml without uv.lock
// both error rather than guessing, while a lone requirements.txt stays valid.
func TestPlanNativeIncompleteErrors(t *testing.T) {
	cases := []struct {
		name   string
		files  map[string]string
		wantOK bool
	}{
		{
			name:  "lock without pyproject",
			files: map[string]string{"uv.lock": "version = 1\n"},
		},
		{
			name:  "pyproject without lock",
			files: map[string]string{"pyproject.toml": "[project]\nname = \"x\"\n"},
		},
		{
			// A lone lock is never trusted, even beside a requirements.txt:
			// the lock cannot be validated without its pyproject.
			name: "lock without pyproject errors even with requirements",
			files: map[string]string{
				"uv.lock":          "version = 1\n",
				"requirements.txt": "six==1.16.0\n",
			},
		},
		{
			name:   "requirements only stays valid",
			files:  map[string]string{"requirements.txt": "six==1.16.0\n"},
			wantOK: true,
		},
		{
			name: "lock wins when complete",
			files: map[string]string{
				"pyproject.toml":   "[project]\nname = \"x\"\n",
				"uv.lock":          "version = 1\n",
				"requirements.txt": "six==1.16.0\n",
			},
			wantOK: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, content := range tc.files {
				write(t, dir, name, content)
			}
			_, err := Engine{}.Plan(testSpecs[0], dir, nil)
			if tc.wantOK && err != nil {
				t.Fatalf("plan: unexpected error %v", err)
			}
			if !tc.wantOK {
				if err == nil {
					t.Fatal("expected an error for an incomplete native uv project")
				}
				if !strings.Contains(err.Error(), "incomplete") {
					t.Errorf("error = %q, want it to mention an incomplete native project", err)
				}
			}
		})
	}
}

// TestPlanRequirementsOnlyInstallUsesUv pins that the classic path never shells
// out to pip: the install command must invoke the copied uv binary.
func TestPlanRequirementsOnlyInstallUsesUv(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "requirements.txt", "six==1.16.0\n")

	p, err := Engine{}.Plan(testSpecs[0], dir, nil)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if strings.HasPrefix(p.Deps.Install, "pip ") {
		t.Errorf("requirements install must use uv, got %q", p.Deps.Install)
	}
	if !strings.Contains(p.Deps.Install, "--system") {
		t.Errorf("requirements install must target the system environment, got %q", p.Deps.Install)
	}
}

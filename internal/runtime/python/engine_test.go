package python

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/runtime/plan"
)

// Two runtime versions served by the same Python engine. The engine must
// produce identical build logic for both, differing only in the base image
// taken from the spec. The second spec is test-only and is never registered in
// the runtime registry.
var testSpecs = []plan.Spec{
	{Name: "python3.14", Engine: plan.EnginePython, BaseImage: "python:3.14-slim"},
	{Name: "python3.15", Engine: plan.EnginePython, BaseImage: "python:3.15-slim"},
}

func TestPlanBootstrapAndBase(t *testing.T) {
	for _, spec := range testSpecs {
		t.Run(spec.Name, func(t *testing.T) {
			dir := t.TempDir()

			p, err := Engine{}.Plan(spec, dir)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			if p.BaseImage != spec.BaseImage {
				t.Errorf("base image = %q, want %q", p.BaseImage, spec.BaseImage)
			}
			if p.WorkDir != "/app" {
				t.Errorf("work dir = %q, want /app", p.WorkDir)
			}
			if len(p.Install) != 0 {
				t.Errorf("expected no install without requirements.txt, got %v", p.Install)
			}
			if len(p.Entrypoint) != 2 || p.Entrypoint[0] != "python" || p.Entrypoint[1] != "/relay/bootstrap.py" {
				t.Errorf("entrypoint = %v, want [python /relay/bootstrap.py]", p.Entrypoint)
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

func TestPlanWithRequirements(t *testing.T) {
	for _, spec := range testSpecs {
		t.Run(spec.Name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "requirements.txt"), []byte("# comment\n"), 0o644); err != nil {
				t.Fatalf("write requirements: %v", err)
			}

			p, err := Engine{}.Plan(spec, dir)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			if len(p.Install) != 1 || p.Install[0] != "pip install --no-cache-dir -r requirements.txt" {
				t.Errorf("install = %v, want [pip install --no-cache-dir -r requirements.txt]", p.Install)
			}
		})
	}
}

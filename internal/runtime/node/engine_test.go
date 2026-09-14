package node

import (
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/runtime/plan"
)

// Two runtime versions served by the same Node engine. The engine must produce
// identical build logic for both, differing only in the base image taken from
// the spec. The second spec is test-only and is never registered in the runtime
// registry.
var testSpecs = []plan.Spec{
	{
		Name:      "node24",
		Engine:    plan.EngineNode,
		BaseImage: "node:24-alpine",
	},
	{
		Name:      "node26",
		Engine:    plan.EngineNode,
		BaseImage: "node:26-alpine",
	},
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
			if !p.Deps.IsZero() {
				t.Errorf("expected zero Deps without package files, got %+v", p.Deps)
			}
			if len(p.Install) != 0 {
				t.Errorf("expected no build-step install without package files, got %v", p.Install)
			}
			if len(p.Entrypoint) != 2 || p.Entrypoint[0] != "node" || p.Entrypoint[1] != "/relay/bootstrap.mjs" {
				t.Errorf("entrypoint = %v, want [node /relay/bootstrap.mjs]", p.Entrypoint)
			}
			if p.User != "10001:10001" {
				t.Errorf("user = %q, want 10001:10001", p.User)
			}
			if !strings.Contains(p.UserSetup, "adduser -D -u 10001") {
				t.Errorf("user setup = %q, want an adduser for uid 10001", p.UserSetup)
			}
			if len(p.Env) != 0 {
				t.Errorf("env = %v, want none for node", p.Env)
			}

			var foundBootstrap bool
			var foundESM bool
			for _, f := range p.Files {
				if f.Path == "/relay/bootstrap.mjs" && string(f.Content) == string(Bootstrap) {
					foundBootstrap = true
					if f.Mode != fs.FileMode(0o644) {
						t.Errorf("bootstrap mode = %v, want 0644", f.Mode)
					}
				}
				if f.Path == filepath.Join("/app", "package.json") {
					foundESM = true
					var m map[string]string
					if err := json.Unmarshal(f.Content, &m); err != nil {
						t.Fatalf("injected package.json not valid JSON: %v", err)
					}
					if m["type"] != "module" {
						t.Errorf("injected package.json type = %q, want module", m["type"])
					}
				}
			}
			if !foundBootstrap {
				t.Error("expected bootstrap file /relay/bootstrap.mjs in plan")
			}
			if !foundESM {
				t.Error("expected injected ESM package.json in plan when no package.json exists")
			}
		})
	}
}

func TestPlanWithPackageJSONOnly(t *testing.T) {
	for _, spec := range testSpecs {
		t.Run(spec.Name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "package.json"), []byte(`{"type":"module"}`), 0o644); err != nil {
				t.Fatalf("write package.json: %v", err)
			}

			p, err := Engine{}.Plan(spec, dir)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			if len(p.Install) != 0 {
				t.Errorf("expected the dependency install to move out of Install into Deps, got %v", p.Install)
			}
			want := plan.Deps{Files: []string{"package.json"}, Install: "npm install --omit=dev", Dir: "/app"}
			if !p.Deps.Equal(want) {
				t.Errorf("deps = %+v, want %+v", p.Deps, want)
			}
			for _, f := range p.Files {
				if f.Path == filepath.Join("/app", "package.json") {
					t.Error("did not expect injected package.json when one exists")
				}
			}
		})
	}
}

func TestPlanWithLock(t *testing.T) {
	for _, spec := range testSpecs {
		t.Run(spec.Name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "package.json"), []byte(`{"type":"module"}`), 0o644); err != nil {
				t.Fatalf("write package.json: %v", err)
			}
			if err := os.WriteFile(filepath.Join(dir, "package-lock.json"), []byte("{}"), 0o644); err != nil {
				t.Fatalf("write package-lock.json: %v", err)
			}

			p, err := Engine{}.Plan(spec, dir)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			if len(p.Install) != 0 {
				t.Errorf("expected the dependency install to move out of Install into Deps, got %v", p.Install)
			}
			// Both the lock and the manifest are listed so a lock change (a
			// different pinned tree) re-fingerprints the layer even when the
			// manifest is unchanged.
			want := plan.Deps{Files: []string{"package.json", "package-lock.json"}, Install: "npm ci --omit=dev", Dir: "/app"}
			if !p.Deps.Equal(want) {
				t.Errorf("deps = %+v, want %+v", p.Deps, want)
			}
			for _, f := range p.Files {
				if f.Path == filepath.Join("/app", "package.json") {
					t.Error("did not expect injected package.json when a lock exists")
				}
			}
		})
	}
}

func TestPlanWithLockOnly(t *testing.T) {
	for _, spec := range testSpecs {
		t.Run(spec.Name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "package-lock.json"), []byte("{}"), 0o644); err != nil {
				t.Fatalf("write package-lock.json: %v", err)
			}

			p, err := Engine{}.Plan(spec, dir)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			// A lock without a manifest is unusual; only the present file is
			// listed so fingerprinting/the dep build never read a missing file.
			want := plan.Deps{Files: []string{"package-lock.json"}, Install: "npm ci --omit=dev", Dir: "/app"}
			if !p.Deps.Equal(want) {
				t.Errorf("deps = %+v, want %+v", p.Deps, want)
			}
		})
	}
}

func TestPlanWithLockOnlyNoInject(t *testing.T) {
	for _, spec := range testSpecs {
		t.Run(spec.Name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "package-lock.json"), []byte("{}"), 0o644); err != nil {
				t.Fatalf("write package-lock.json: %v", err)
			}

			p, err := Engine{}.Plan(spec, dir)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			// Lock-only still routes through the Deps install path: npm ci must
			// fail loudly during the build (no package.json), never silently be
			// covered up by injecting an ESM package.json here.
			want := plan.Deps{Files: []string{"package-lock.json"}, Install: "npm ci --omit=dev", Dir: "/app"}
			if !p.Deps.Equal(want) {
				t.Errorf("deps = %+v, want %+v", p.Deps, want)
			}
			for _, f := range p.Files {
				if f.Path == filepath.Join("/app", "package.json") {
					t.Error("did not expect injected package.json for a lock-only function")
				}
			}
		})
	}
}

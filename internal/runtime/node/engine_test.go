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
				t.Errorf("expected zero Deps without package files, got %+v", p.Deps)
			}
			if len(p.Install) != 1 || !strings.Contains(p.Install[0], "@opentelemetry/api@"+otelAPIVersion) {
				t.Errorf("expected the managed OTel API install, got %v", p.Install)
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

			p, err := Engine{}.Plan(spec, dir, nil)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			if len(p.Install) != 1 || !strings.Contains(p.Install[0], "@opentelemetry/api@"+otelAPIVersion) {
				t.Errorf("expected the managed OTel API install, got %v", p.Install)
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

			p, err := Engine{}.Plan(spec, dir, nil)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			if len(p.Install) != 1 || !strings.Contains(p.Install[0], "@opentelemetry/api@"+otelAPIVersion) {
				t.Errorf("expected the managed OTel API install, got %v", p.Install)
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

			p, err := Engine{}.Plan(spec, dir, nil)
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

			p, err := Engine{}.Plan(spec, dir, nil)
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

// writeSource puts a handler source file under dir, creating parent
// directories. It fails the test on error.
func writeSource(t *testing.T, dir, rel, content string) {
	t.Helper()
	path := filepath.Join(dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir for %s: %v", rel, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", rel, err)
	}
}

// specByName returns the test spec with the given runtime name.
func specByName(t *testing.T, name string) plan.Spec {
	t.Helper()
	for _, s := range testSpecs {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("no test spec named %q", name)
	return plan.Spec{}
}

// requireInstall returns the TypeScript build command. The managed API install
// is intentionally a separate runtime dependency step.
func requireInstall(t *testing.T, p plan.BuildPlan) string {
	t.Helper()
	for _, cmd := range p.Install {
		if strings.Contains(cmd, "esbuild@") {
			return cmd
		}
	}
	t.Fatalf("Install = %v, want an esbuild command", p.Install)
	return ""
}

// TestPlanJSHandlersNoBuild asserts the JavaScript path is untouched: a .js
// handler, a nil handler list, and an all-JS multi-handler app all produce
// no Install step and an unchanged dependency layer. JavaScript must never be
// forced through TypeScript tooling.
func TestPlanJSHandlersNoBuild(t *testing.T) {
	for _, spec := range testSpecs {
		t.Run(spec.Name, func(t *testing.T) {
			dir := t.TempDir()
			writeSource(t, dir, "handler.js", "export function handler(e) {}\n")
			writeSource(t, dir, "src/order.js", "export function handler(e) {}\n")

			for _, handlers := range [][]string{nil, {"handler"}, {"handler", "src.order"}} {
				p, err := Engine{}.Plan(spec, dir, handlers)
				if err != nil {
					t.Fatalf("plan(%v): %v", handlers, err)
				}
				if len(p.Install) != 1 || !strings.Contains(p.Install[0], "@opentelemetry/api@"+otelAPIVersion) {
					t.Errorf("handlers %v: Install = %v, want the managed OTel API install", handlers, p.Install)
				}
				if !p.Deps.IsZero() {
					t.Errorf("handlers %v: Deps = %+v, want zero", handlers, p.Deps)
				}
			}
		})
	}
}

// TestPlanTSHandlerBuilds pins the TypeScript build command: one RUN that installs
// the pinned esbuild, bundles the .ts source to a sibling .mjs with the runtime's
// own version as --target, keeps packages external, and removes the tooling and
// npm cache in the same layer.
func TestPlanTSHandlerBuilds(t *testing.T) {
	dir := t.TempDir()
	writeSource(t, dir, "src/order.ts", "export function handler(e) {}\n")

	p, err := Engine{}.Plan(specByName(t, "node24"), dir, []string{"src.order"})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	cmd := requireInstall(t, p)

	for _, want := range []string{
		"npm install --prefix /tmp/relay-esbuild",
		"--cache /tmp/relay-npm-cache",
		"esbuild@" + esbuildVersion,
		"--bundle /app/src/order.ts",
		"--outfile=/app/src/order.mjs",
		"--format=esm",
		"--platform=node",
		"--target=node24",
		"--packages=external",
		"--log-level=warning",
		"rm -rf /tmp/relay-esbuild /tmp/relay-npm-cache",
	} {
		if !strings.Contains(cmd, want) {
			t.Errorf("Install command missing %q:\n%s", want, cmd)
		}
	}
	// No tsconfig.json in the app dir: the flag must be absent.
	if strings.Contains(cmd, "--tsconfig") {
		t.Errorf("Install command must not pass --tsconfig when no tsconfig.json exists:\n%s", cmd)
	}
	// The tooling install must precede the first bundle and the cleanup must
	// follow the last one, all joined into a single RUN.
	if strings.Index(cmd, "npm install") > strings.Index(cmd, "--bundle") {
		t.Errorf("npm install must come before the bundling:\n%s", cmd)
	}
	if strings.Index(cmd, "--bundle") > strings.Index(cmd, "rm -rf") {
		t.Errorf("cleanup must come after the bundling:\n%s", cmd)
	}
	// esbuild rejects the space-separated --outfile form; pin the flag=value form.
	if strings.Contains(cmd, "--outfile /") {
		t.Errorf("esbuild requires --outfile=<path>, found the space-separated form:\n%s", cmd)
	}
}

// TestPlanTSTargetUsesSpecName pins that --target tracks the managed runtime
// version from the spec, so the emitted syntax matches the runtime that executes
// it.
func TestPlanTSTargetUsesSpecName(t *testing.T) {
	dir := t.TempDir()
	writeSource(t, dir, "index.ts", "export function handler(e) {}\n")

	p, err := Engine{}.Plan(specByName(t, "node26"), dir, []string{"index"})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if cmd := requireInstall(t, p); !strings.Contains(cmd, "--target=node26") {
		t.Errorf("Install command must target node26 from the spec, got:\n%s", cmd)
	}
}

// TestPlanTSResolutionAndPrecedence pins the TypeScript candidate order
// (.mts over .ts, index variants) and the mirrored output path.
func TestPlanTSResolutionAndPrecedence(t *testing.T) {
	cases := []struct {
		name    string
		files   []string
		module  string
		wantSrc string
		wantOut string
	}{
		{
			name:    "mts beats ts",
			files:   []string{"x.mts", "x.ts"},
			module:  "x",
			wantSrc: "/app/x.mts",
			wantOut: "/app/x.mjs",
		},
		{
			name:    "ts file",
			files:   []string{"x.ts"},
			module:  "x",
			wantSrc: "/app/x.ts",
			wantOut: "/app/x.mjs",
		},
		{
			name:    "index mts",
			files:   []string{"x/index.mts", "x/index.ts"},
			module:  "x",
			wantSrc: "/app/x/index.mts",
			wantOut: "/app/x/index.mjs",
		},
		{
			name:    "index ts",
			files:   []string{"x/index.ts"},
			module:  "x",
			wantSrc: "/app/x/index.ts",
			wantOut: "/app/x/index.mjs",
		},
		{
			name:    "nested module",
			files:   []string{"src/order.ts"},
			module:  "src.order",
			wantSrc: "/app/src/order.ts",
			wantOut: "/app/src/order.mjs",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for _, f := range tc.files {
				writeSource(t, dir, f, "export function handler(e) {}\n")
			}
			p, err := Engine{}.Plan(specByName(t, "node24"), dir, []string{tc.module})
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			cmd := requireInstall(t, p)
			if !strings.Contains(cmd, "--bundle "+tc.wantSrc) {
				t.Errorf("expected source %s in:\n%s", tc.wantSrc, cmd)
			}
			if !strings.Contains(cmd, "--outfile="+tc.wantOut) {
				t.Errorf("expected output %s in:\n%s", tc.wantOut, cmd)
			}
		})
	}
}

// TestPlanJSResolutionUnchanged pins the bootstrap's exact JS candidate order,
// including .mjs-over-.js precedence and the index fallbacks.
func TestPlanJSResolutionUnchanged(t *testing.T) {
	cases := []struct {
		name   string
		files  []string
		module string
	}{
		{name: "mjs beats js", files: []string{"x.mjs", "x.js"}, module: "x"},
		{name: "js file", files: []string{"x.js"}, module: "x"},
		{name: "index mjs", files: []string{"x/index.mjs", "x/index.js"}, module: "x"},
		{name: "index js", files: []string{"x/index.js"}, module: "x"},
		{name: "file beats index", files: []string{"x.mjs", "x/index.mjs"}, module: "x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for _, f := range tc.files {
				writeSource(t, dir, f, "export function handler(e) {}\n")
			}
			p, err := Engine{}.Plan(specByName(t, "node24"), dir, []string{tc.module})
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			if len(p.Install) != 1 || !strings.Contains(p.Install[0], "@opentelemetry/api@"+otelAPIVersion) {
				t.Errorf("JS-only resolution must produce the managed OTel API install, got %v", p.Install)
			}
		})
	}
}

// TestPlanTSWithTsconfig pins that a tsconfig.json present in the app dir
// is passed to esbuild; Relay never type-checks, esbuild only reads
// compilerOptions from it.
func TestPlanTSWithTsconfig(t *testing.T) {
	dir := t.TempDir()
	writeSource(t, dir, "index.ts", "export function handler(e) {}\n")
	writeSource(t, dir, "tsconfig.json", `{"compilerOptions":{"strict":true}}`)

	p, err := Engine{}.Plan(specByName(t, "node24"), dir, []string{"index"})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	cmd := requireInstall(t, p)
	if !strings.Contains(cmd, "--tsconfig=/app/tsconfig.json") {
		t.Errorf("expected --tsconfig=/app/tsconfig.json, got:\n%s", cmd)
	}
}

// TestPlanSourceMountedPersistsEsbuildAndMountsSource pins the SOURCE_MOUNT
// Node plan: no source is baked, the pinned esbuild is installed PERSISTENTLY
// (never removed) so the bootstrap can bundle TypeScript at container startup,
// the live source is mounted at the DISTINCT SourceMountTarget (/app/src, so
// /app/node_modules stays visible), and the bootstrap ENTRYPOINT carries the
// --source-mount flag. A mixed JS+TS handler set produces no build-time bundle.
func TestPlanSourceMountedPersistsEsbuildAndMountsSource(t *testing.T) {
	dir := t.TempDir()
	writeSource(t, dir, "src/order.ts", "export function handler(e) {}\n")
	writeSource(t, dir, "src/plain.js", "export function handler(e) {}\n")
	// A host node_modules triggers the bounded node_modules mask (the path Docker
	// can mount over because the bind carries it).
	if err := os.MkdirAll(filepath.Join(dir, "node_modules"), 0o755); err != nil {
		t.Fatalf("mkdir node_modules: %v", err)
	}

	spec := specByName(t, "node24")
	spec.SourceMounted = true
	p, err := Engine{}.Plan(spec, dir, []string{"src.order", "src.plain"})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if p.WorkDir != "/app" {
		t.Errorf("work dir = %q, want /app", p.WorkDir)
	}
	if p.SourceMountTarget != SourceMountTarget || p.SourceMountTarget == "/app" {
		t.Fatalf("source mount target = %q, want %q (distinct from /app)", p.SourceMountTarget, SourceMountTarget)
	}
	if p.SourceMountWorkDir != SourceMountTarget {
		t.Errorf("source mount work dir = %q, want %q (the app root, so cwd sees the source)", p.SourceMountWorkDir, SourceMountTarget)
	}
	if len(p.SourceMountMasks) != 1 || p.SourceMountMasks[0] != SourceMountNodeModules {
		t.Errorf("source mount masks = %v, want [%s] (host node_modules must not shadow the dependency image)", p.SourceMountMasks, SourceMountNodeModules)
	}
	if SourceMountNodeModules != SourceMountTarget+"/node_modules" {
		t.Fatalf("mask %q must be the node_modules under the mount target %q", SourceMountNodeModules, SourceMountTarget)
	}
	if len(p.Entrypoint) != 3 || p.Entrypoint[0] != "node" || p.Entrypoint[1] != "/relay/bootstrap.mjs" || p.Entrypoint[2] != sourceMountFlag {
		t.Errorf("entrypoint = %v, want [node /relay/bootstrap.mjs %s]", p.Entrypoint, sourceMountFlag)
	}
	if len(p.Install) != 2 {
		t.Fatalf("Install = %v, want the managed OTel install plus one persistent esbuild install", p.Install)
	}
	if !strings.Contains(p.Install[0], "@opentelemetry/api@"+otelAPIVersion) {
		t.Errorf("Install[0] = %q, want the managed OTel API install", p.Install[0])
	}
	esb := p.Install[1]
	for _, want := range []string{
		"npm install --prefix /relay/esbuild",
		"esbuild@" + esbuildVersion,
		"--cache /tmp/relay-npm-cache",
		"rm -rf /tmp/relay-npm-cache",
	} {
		if !strings.Contains(esb, want) {
			t.Errorf("persistent esbuild install missing %q:\n%s", want, esb)
		}
	}
	// The persistent prefix must NOT be removed (the runtime needs the tool).
	if strings.Contains(esb, "rm -rf /relay/esbuild") {
		t.Errorf("persistent esbuild must survive the build:\n%s", esb)
	}
	// No build-time transpilation: bundling moved to container startup.
	if strings.Contains(strings.Join(p.Install, "\n"), "--bundle") {
		t.Errorf("source-mounted plan must not bundle at build time: %v", p.Install)
	}
}

// TestPlanSourceMountedShipsSharedResolveHook pins that a source-mounted Node
// plan writes the shared ESM resolve hook at ResolveHookPath, while a baked plan
// never does: a baked image has no mount, and shipping the file would change the
// baked plan byte-for-byte.
func TestPlanSourceMountedShipsSharedResolveHook(t *testing.T) {
	dir := t.TempDir()
	writeSource(t, dir, "index.js", "export function handler(e) {}\n")

	mounted := specByName(t, "node24")
	mounted.SourceMounted = true
	p, err := Engine{}.Plan(mounted, dir, []string{"index"})
	if err != nil {
		t.Fatalf("mounted plan: %v", err)
	}
	var found bool
	for _, f := range p.Files {
		if f.Path == ResolveHookPath {
			found = true
			if string(f.Content) != string(ResolveHook) {
				t.Error("resolve hook plan file content must be the embedded ResolveHook")
			}
			if f.Mode != fs.FileMode(0o644) {
				t.Errorf("resolve hook mode = %v, want 0644", f.Mode)
			}
		}
	}
	if !found {
		t.Fatalf("mounted plan must include %s", ResolveHookPath)
	}
	if ResolveHookPath != "/relay/resolve-hook.mjs" {
		t.Fatalf("ResolveHookPath = %q, want /relay/resolve-hook.mjs", ResolveHookPath)
	}

	baked, err := Engine{}.Plan(specByName(t, "node24"), dir, []string{"index"})
	if err != nil {
		t.Fatalf("baked plan: %v", err)
	}
	for _, f := range baked.Files {
		if f.Path == ResolveHookPath {
			t.Fatalf("baked plan must NOT include the resolve hook, got %s", f.Path)
		}
	}
}

// TestPlanBakedHasNoSourceMountLayout pins the SOURCE_MOUNT=false invariant: a
// baked plan declares no work-dir override and no mask, so its rendered
// Dockerfile and container create shape are byte-for-byte the historical one.
func TestPlanBakedHasNoSourceMountLayout(t *testing.T) {
	dir := t.TempDir()
	writeSource(t, dir, "index.js", "export function handler(e) {}\n")
	p, err := Engine{}.Plan(specByName(t, "node24"), dir, []string{"index"})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if p.SourceMountTarget != "" || p.SourceMountWorkDir != "" || len(p.SourceMountMasks) != 0 {
		t.Fatalf("baked plan must declare no source-mount layout, got target=%q workdir=%q masks=%v",
			p.SourceMountTarget, p.SourceMountWorkDir, p.SourceMountMasks)
	}
}

// TestPlanSourceMountedMasksHostNodeModulesOnlyWhenPresent pins the bounded mask
// decision: a host node_modules directory yields the single /app/src/node_modules
// mask (the only path that can shadow the dependency tree), while its absence
// yields NO mask because Docker cannot create a mountpoint inside the read-only
// source bind.
func TestPlanSourceMountedMasksHostNodeModulesOnlyWhenPresent(t *testing.T) {
	spec := specByName(t, "node24")
	spec.SourceMounted = true

	absent := t.TempDir()
	writeSource(t, absent, "index.js", "export function handler(e) {}\n")
	p, err := Engine{}.Plan(spec, absent, []string{"index"})
	if err != nil {
		t.Fatalf("plan without node_modules: %v", err)
	}
	if len(p.SourceMountMasks) != 0 {
		t.Fatalf("masks = %v, want none when the host has no node_modules", p.SourceMountMasks)
	}

	present := t.TempDir()
	writeSource(t, present, "index.js", "export function handler(e) {}\n")
	if err := os.MkdirAll(filepath.Join(present, "node_modules", "dep"), 0o755); err != nil {
		t.Fatalf("mkdir node_modules: %v", err)
	}
	p, err = Engine{}.Plan(spec, present, []string{"index"})
	if err != nil {
		t.Fatalf("plan with node_modules: %v", err)
	}
	if len(p.SourceMountMasks) != 1 || p.SourceMountMasks[0] != SourceMountNodeModules {
		t.Fatalf("masks = %v, want [%s]", p.SourceMountMasks, SourceMountNodeModules)
	}
}

// TestPlanSourceMountedDepsUnchanged pins that mount mode changes only the
// source-carrying concerns: the dependency layer, base image, user setup, and
// injected ESM package.json are identical to the baked plan, so a dependency
// manifest change still flows through the normal dependency fingerprint and the
// app image rebuild.
func TestPlanSourceMountedDepsUnchanged(t *testing.T) {
	dir := t.TempDir()
	writeSource(t, dir, "package.json", `{"type":"module"}`)
	writeSource(t, dir, "package-lock.json", `{}`)

	baked, err := Engine{}.Plan(specByName(t, "node24"), dir, nil)
	if err != nil {
		t.Fatalf("baked plan: %v", err)
	}
	spec := specByName(t, "node24")
	spec.SourceMounted = true
	mounted, err := Engine{}.Plan(spec, dir, nil)
	if err != nil {
		t.Fatalf("mounted plan: %v", err)
	}
	if !baked.Deps.Equal(mounted.Deps) {
		t.Errorf("deps changed under SOURCE_MOUNT: baked %+v mounted %+v", baked.Deps, mounted.Deps)
	}
	if baked.BaseImage != mounted.BaseImage {
		t.Errorf("base image changed under SOURCE_MOUNT: %q -> %q", baked.BaseImage, mounted.BaseImage)
	}
	if baked.UserSetup != mounted.UserSetup || baked.User != mounted.User {
		t.Errorf("user setup changed under SOURCE_MOUNT")
	}
}

// TestPlanSourceMountedStillValidatesHandlers pins that mount mode keeps the
// deterministic build-time module validation: a missing or ambiguous handler
// still fails Plan, so a source-mounted image never defers the error to every
// invocation.
func TestPlanSourceMountedStillValidatesHandlers(t *testing.T) {
	spec := specByName(t, "node24")
	spec.SourceMounted = true

	dir := t.TempDir()
	writeSource(t, dir, "x.ts", "export function handler(e) {}\n")
	if _, err := (Engine{}).Plan(spec, dir, []string{"missing"}); err == nil {
		t.Error("expected a missing-module error under SOURCE_MOUNT")
	}

	ambiguous := t.TempDir()
	writeSource(t, ambiguous, "x.js", "export function handler(e) {}\n")
	writeSource(t, ambiguous, "x.ts", "export function handler(e) {}\n")
	if _, err := (Engine{}).Plan(spec, ambiguous, []string{"x"}); err == nil {
		t.Error("expected an ambiguity error under SOURCE_MOUNT")
	}
}

// TestPlanMultipleHandlersOneInstallDedup pins batching: several TypeScript
// handlers share exactly one esbuild install, and the caller's sorted+deduped
// list is compiled in order.
func TestPlanMultipleHandlersOneInstallDedup(t *testing.T) {
	dir := t.TempDir()
	writeSource(t, dir, "events/created.ts", "export function handler(e) {}\n")
	writeSource(t, dir, "events/deleted.ts", "export function handler(e) {}\n")

	p, err := Engine{}.Plan(specByName(t, "node24"), dir, []string{"events.created", "events.deleted"})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	cmd := requireInstall(t, p)
	if got := strings.Count(cmd, "npm install --prefix"); got != 1 {
		t.Errorf("npm install count = %d, want exactly 1", got)
	}
	if got := strings.Count(cmd, "--bundle "); got != 2 {
		t.Errorf("esbuild bundle count = %d, want 2:\n%s", got, cmd)
	}
	if !strings.Contains(cmd, "--bundle /app/events/created.ts") || !strings.Contains(cmd, "--bundle /app/events/deleted.ts") {
		t.Errorf("expected both handlers compiled:\n%s", cmd)
	}
}

// TestPlanAmbiguousHandlerErrors pins the ambiguity rule: a module resolving to
// both a JavaScript and a TypeScript source fails Plan rather than silently
// picking one (the generated .mjs would shadow or confuse resolution).
func TestPlanAmbiguousHandlerErrors(t *testing.T) {
	cases := []struct {
		name   string
		files  []string
		module string
	}{
		{name: "js beside ts", files: []string{"x.js", "x.ts"}, module: "x"},
		{name: "mjs beside ts", files: []string{"x.mjs", "x.ts"}, module: "x"},
		{name: "index js beside index ts", files: []string{"x/index.js", "x/index.ts"}, module: "x"},
		{name: "nested", files: []string{"src/a.js", "src/a.mts"}, module: "src.a"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for _, f := range tc.files {
				writeSource(t, dir, f, "export function handler(e) {}\n")
			}
			_, err := Engine{}.Plan(specByName(t, "node24"), dir, []string{tc.module})
			if err == nil {
				t.Fatal("expected an ambiguity error")
			}
			if !strings.Contains(err.Error(), "ambiguous") {
				t.Errorf("error = %q, want it to mention ambiguity", err)
			}
		})
	}
}

// TestPlanMissingHandlerErrors pins the build-time missing-module surface: a
// handler whose module has no .mjs/.js/.mts/.ts source fails Plan instead of
// every invocation failing later with module-not-found.
func TestPlanMissingHandlerErrors(t *testing.T) {
	dir := t.TempDir()
	writeSource(t, dir, "index.ts", "export function handler(e) {}\n")

	_, err := Engine{}.Plan(specByName(t, "node24"), dir, []string{"missing"})
	if err == nil {
		t.Fatal("expected a missing-module error")
	}
	if !strings.Contains(err.Error(), `"missing"`) {
		t.Errorf("error = %q, want it to name the missing module", err)
	}
}

// TestPlanInvalidModuleErrors pins the defensive path-traversal rejection: a
// module whose segments are empty or "."/".." is rejected before any filesystem
// access, so a crafted handler can never escape /app in the generated RUN.
func TestPlanInvalidModuleErrors(t *testing.T) {
	for _, module := range []string{"", ".", "..", "a..b", "a.", ".a", "../evil", "../../etc/passwd", "src/../etc"} {
		t.Run(module, func(t *testing.T) {
			dir := t.TempDir()
			_, err := Engine{}.Plan(specByName(t, "node24"), dir, []string{module})
			if err == nil {
				t.Fatal("expected an invalid-module error")
			}
			if !strings.Contains(err.Error(), "invalid handler module") {
				t.Errorf("error = %q, want an invalid-handler-module error", err)
			}
		})
	}
}

// TestPlanTSHandlersWithoutTsconfigNoFlag is covered by TestPlanTSHandlerBuilds;
// this guards the mirror case explicitly across both specs: a tsconfig is not
// auto-injected into the plan files.
func TestPlanTSDoesNotInjectTsconfig(t *testing.T) {
	dir := t.TempDir()
	writeSource(t, dir, "index.ts", "export function handler(e) {}\n")

	p, err := Engine{}.Plan(specByName(t, "node24"), dir, []string{"index"})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	for _, f := range p.Files {
		if strings.HasSuffix(f.Path, "tsconfig.json") {
			t.Errorf("did not expect an injected tsconfig.json, got %s", f.Path)
		}
	}
}

// TestShellQuoteArg pins the POSIX shell-word contract used for every dynamic
// value interpolated into the generated RUN: ordinary path bytes stay bare,
// while every metacharacter (or quote/whitespace/non-ASCII byte) forces
// single-quote quoting, with the sole escape being for an embedded single quote.
func TestShellQuoteArg(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"/app/src/order.ts", "/app/src/order.ts"},
		{"esbuild@0.28.2", "esbuild@0.28.2"},
		{"node24", "node24"},
		{"", "''"},
		{"a b", "'a b'"},
		{"$HOME", "'$HOME'"},
		{"`id`", "'`id`'"},
		{"a;b", "'a;b'"},
		{"a|b", "'a|b'"},
		{"a>b", "'a>b'"},
		{"(x)", "'(x)'"},
		{"it's", `'it'\''s'`},
		{"tab\there", "'tab\there'"},
		{"café.ts", "'café.ts'"},
	}
	for _, tc := range cases {
		if got := shellQuoteArg(tc.in); got != tc.want {
			t.Errorf("shellQuoteArg(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestPlanShellQuotesHandlerDerivedPaths pins that every handler-derived value in
// the generated esbuild RUN is a single shell argument: a source/output path
// containing shell metacharacters ($, $(), backticks, ;, |, >, parentheses,
// spaces, quotes) is single-quote quoted, so the shell can neither expand nor
// split it nor start a new command. Nested TS/JS paths remain byte-identical to
// the previous bare form when they contain no metacharacters.
func TestPlanShellQuotesHandlerDerivedPaths(t *testing.T) {
	const source = "export function handler(e) {}\n"
	cases := []struct {
		name   string
		rel    string
		module string
		// wantArg is the exact quoted argument expected after --bundle.
		wantArg string
	}{
		{"plain nested", "src/order.ts", "src.order", "/app/src/order.ts"},
		{"dollar", "costs$total.ts", "costs$total", `'/app/costs$total.ts'`},
		{"command substitution", "cmd$(id).ts", "cmd$(id)", `'/app/cmd$(id).ts'`},
		{"backticks", "tick`id`.ts", "tick`id`", "'/app/tick`id`.ts'"},
		{"semicolon", "semi;rm.ts", "semi;rm", "'/app/semi;rm.ts'"},
		{"pipe", "pipe|cat.ts", "pipe|cat", "'/app/pipe|cat.ts'"},
		{"redirect", "redir>out.ts", "redir>out", "'/app/redir>out.ts'"},
		{"parens", "paren(a).ts", "paren(a)", "'/app/paren(a).ts'"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeSource(t, dir, tc.rel, source)
			p, err := Engine{}.Plan(specByName(t, "node24"), dir, []string{tc.module})
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			cmd := requireInstall(t, p)
			if !strings.Contains(cmd, "--bundle "+tc.wantArg) {
				t.Errorf("expected --bundle %s in:\n%s", tc.wantArg, cmd)
			}
			// The output path mirrors the source with the extension replaced;
			// it must be quoted too.
			outExt := strings.TrimSuffix(tc.rel, ".ts") + ".mjs"
			wantOut := shellQuoteArg("/app/" + outExt)
			if !strings.Contains(cmd, "--outfile="+wantOut) {
				t.Errorf("expected --outfile=%s in:\n%s", wantOut, cmd)
			}
		})
	}
}

// TestPlanShellQuoteNestedJSAndTSUnchanged pins that the added quoting does not
// perturb ordinary nested paths: an all-safe source/output path is emitted bare,
// exactly as before.
func TestPlanShellQuoteNestedJSAndTSUnchanged(t *testing.T) {
	dir := t.TempDir()
	writeSource(t, dir, "src/deep/order.ts", "export function handler(e) {}\n")

	p, err := Engine{}.Plan(specByName(t, "node24"), dir, []string{"src.deep.order"})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	cmd := requireInstall(t, p)
	for _, want := range []string{
		"--bundle /app/src/deep/order.ts",
		"--outfile=/app/src/deep/order.mjs",
		"npm install --prefix /tmp/relay-esbuild",
		"--cache /tmp/relay-npm-cache",
		"rm -rf /tmp/relay-esbuild /tmp/relay-npm-cache",
	} {
		if !strings.Contains(cmd, want) {
			t.Errorf("command missing unchanged %q:\n%s", want, cmd)
		}
	}
}

// TestPlanTraversalStillRejected pins that path traversal is rejected before any
// filesystem access, so the shell-safety work never widens the addressable path
// set. A metacharacter-bearing traversal is rejected by module validation, not
// merely quoted.
func TestPlanTraversalStillRejected(t *testing.T) {
	for _, module := range []string{"../etc/passwd", "a/../../etc/passwd", "..", "a..b"} {
		t.Run(module, func(t *testing.T) {
			dir := t.TempDir()
			_, err := Engine{}.Plan(specByName(t, "node24"), dir, []string{module})
			if err == nil {
				t.Fatal("expected a traversal/invalid-module rejection")
			}
			if !strings.Contains(err.Error(), "invalid handler module") {
				t.Errorf("error = %q, want an invalid-handler-module error", err)
			}
		})
	}
}

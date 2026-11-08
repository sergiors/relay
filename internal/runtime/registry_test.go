package runtime

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/runtime/plan"
)

// TestLookupResolvesRuntimeSpecAndRejectsUnknown verifies the runtime registry
// resolves each supported runtime to its engine + base image and rejects an
// unknown runtime with an "unsupported runtime" error.
func TestLookupResolvesRuntimeSpecAndRejectsUnknown(t *testing.T) {
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

	spec, err = lookup("nodejs24.x")
	if err != nil {
		t.Fatalf("lookup nodejs24.x: %v", err)
	}
	if spec.Engine != plan.EngineNode {
		t.Errorf("nodejs24.x engine = %q, want %q", spec.Engine, plan.EngineNode)
	}
	if spec.BaseImage != "node:24-alpine" {
		t.Errorf("nodejs24.x base image = %q, want node:24-alpine", spec.BaseImage)
	}

	// node24 is the removed identifier: it must be rejected, never aliased.
	for _, name := range []string{"ruby5", "node24"} {
		if _, err := lookup(name); err == nil {
			t.Errorf("lookup(%s) succeeded, want error", name)
		} else if !strings.Contains(err.Error(), "unsupported runtime") {
			t.Errorf(`lookup(%s) error = %q, want "unsupported runtime"`, name, err)
		}
	}
}

// TestLookupResolvesAllManagedRuntimes pins the complete supported-runtime set
// and its official base images. The map key must equal Spec.Name: it is the
// identity hashed into dependency fingerprints and stamped on images, so a
// mismatch would silently merge or split image identities.
func TestLookupResolvesAllManagedRuntimes(t *testing.T) {
	cases := []struct {
		name      string
		engine    plan.Engine
		baseImage string
	}{
		{"python3.14", plan.EnginePython, "python:3.14-slim"},
		{"python3.15", plan.EnginePython, "python:3.15-slim"},
		{"nodejs24.x", plan.EngineNode, "node:24-alpine"},
		{"nodejs26.x", plan.EngineNode, "node:26-alpine"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec, err := lookup(tc.name)
			if err != nil {
				t.Fatalf("lookup %s: %v", tc.name, err)
			}
			if spec.Name != tc.name {
				t.Errorf("spec name = %q, want the registry key %q", spec.Name, tc.name)
			}
			if spec.Engine != tc.engine {
				t.Errorf("engine = %q, want %q", spec.Engine, tc.engine)
			}
			if spec.BaseImage != tc.baseImage {
				t.Errorf("base image = %q, want %q", spec.BaseImage, tc.baseImage)
			}
			if !spec.MountableSource {
				t.Error("managed runtimes must support HOT_RELOAD source mounting")
			}
		})
	}
}

// TestManagedRuntimeDependencyIdentitiesAreDistinct verifies that every
// registry identifier has its own dependency fingerprint. Distinct fingerprints
// map to distinct dependency image tags, so a template runtime change can never
// reuse another runtime's installed layer.
func TestManagedRuntimeDependencyIdentitiesAreDistinct(t *testing.T) {
	dir := t.TempDir()
	const manifest = "manifest.txt"
	if err := os.WriteFile(filepath.Join(dir, manifest), []byte("same\n"), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	// Identical manifest bytes and install command isolate the runtime identity
	// inputs (name, base image, and pinned tool) in the fingerprint.
	deps := plan.Deps{Files: []string{manifest}, Install: "install", Dir: "/app"}

	names := []string{
		"python3.14", "python3.15",
		"nodejs24.x", "nodejs26.x",
	}
	seen := make(map[string]string, len(names))
	for _, name := range names {
		spec, err := lookup(name)
		if err != nil {
			t.Fatalf("lookup %s: %v", name, err)
		}
		fp, err := DependencyFingerprint("arm64", "linux", spec, dir, deps)
		if err != nil {
			t.Fatalf("%s dependency fingerprint: %v", name, err)
		}
		if prev, ok := seen[fp]; ok {
			t.Errorf("%s and %s share dependency fingerprint %s", prev, name, fp)
		}
		seen[fp] = name
	}
}

// TestPythonSpecPinsUvTool verifies every Python runtime declares the pinned uv
// runtime tool (source ghcr.io/astral-sh/uv:<pinned>) so every Python image has
// the uv binary. It also guards against a floating "latest" tag: the version
// must be an explicit semver so builds are reproducible.
func TestPythonSpecPinsUvTool(t *testing.T) {
	for _, name := range []string{"python3.14", "python3.15"} {
		t.Run(name, func(t *testing.T) {
			spec, err := lookup(name)
			if err != nil {
				t.Fatalf("lookup %s: %v", name, err)
			}
			if len(spec.RuntimeTools) != 1 {
				t.Fatalf("%s runtime tools = %+v, want exactly the uv tool", name, spec.RuntimeTools)
			}
			tool := spec.RuntimeTools[0]
			if tool.From != UvImageTag {
				t.Errorf("uv tool source = %q, want %q", tool.From, UvImageTag)
			}
			if tool.Source != "/uv" || tool.Destination != "/usr/local/bin/uv" {
				t.Errorf("uv tool = %+v, want /uv -> /usr/local/bin/uv", tool)
			}
			if !strings.HasPrefix(tool.From, "ghcr.io/astral-sh/uv:") {
				t.Errorf("uv tool source = %q, want the official ghcr.io/astral-sh/uv source", tool.From)
			}
			version := strings.TrimPrefix(tool.From, "ghcr.io/astral-sh/uv:")
			if version == "latest" || version == "" {
				t.Errorf("uv tool version = %q, must be a pinned version, never latest", version)
			}
			if strings.Count(version, ".") != 2 {
				t.Errorf("uv tool version = %q, want a full pinned major.minor.patch version", version)
			}
		})
	}
}

// TestNodeSpecPinsPnpmTool verifies every Node runtime declares the pinned pnpm
// runtime tool as a per-architecture remote archive artifact: the official
// pnpm musl standalone binary, downloaded and checksum-verified at build time.
// It pins the version (never a floating "latest"), the two supported
// architectures (amd64/arm64), the extracted member, and the digest shape.
func TestNodeSpecPinsPnpmTool(t *testing.T) {
	for _, name := range []string{"nodejs24.x", "nodejs26.x"} {
		t.Run(name, func(t *testing.T) {
			spec, err := lookup(name)
			if err != nil {
				t.Fatalf("lookup %s: %v", name, err)
			}
			if len(spec.RuntimeTools) != 1 {
				t.Fatalf("%s runtime tools = %+v, want exactly the pnpm tool", name, spec.RuntimeTools)
			}
			tool := spec.RuntimeTools[0]
			if tool.Artifact == nil {
				t.Fatalf("pnpm tool = %+v, want the remote-archive artifact form", tool)
			}
			if tool.From != "" || tool.Source != "" {
				t.Errorf("pnpm artifact tool must not be an image copy (from=%q source=%q)", tool.From, tool.Source)
			}
			if tool.Destination != "/usr/local/bin/pnpm" {
				t.Errorf("pnpm tool destination = %q, want /usr/local/bin/pnpm", tool.Destination)
			}
			if PnpmVersion == "latest" || PnpmVersion == "" || strings.Count(PnpmVersion, ".") != 2 {
				t.Errorf("PnpmVersion = %q, want a full pinned major.minor.patch version", PnpmVersion)
			}
			want := map[string]string{
				"amd64": pnpmReleaseBaseURL + "pnpm-linux-x64-musl.tar.gz",
				"arm64": pnpmReleaseBaseURL + "pnpm-linux-arm64-musl.tar.gz",
			}
			wantSHA256 := map[string]string{
				"amd64": "f2ad050a705ab433e6c1c4fd57d584462896062b048e9d29a90548ad6d91f91b",
				"arm64": "906259045aaef37a3fe6145fabdc1cc3195da523a788900875bf97be3b40333e",
			}
			wantFallback := map[string]plan.ArtifactSource{
				"amd64": {
					URL:    "https://registry.npmjs.org/@pnpm/exe.linux-x64-musl/-/exe.linux-x64-musl-12.10.1.tgz",
					SHA256: "7313bee7b14e8e2ccffec28c11dc1302640a5e8de9f75625dab07ebab244c3ea",
					Member: "package/pnpm",
				},
				"arm64": {
					URL:    "https://registry.npmjs.org/@pnpm/exe.linux-arm64-musl/-/exe.linux-arm64-musl-12.10.1.tgz",
					SHA256: "0c1f2736a856a766044d7a8706e60aa1284ea17080606897d3a5167caa5c0c55",
					Member: "package/pnpm",
				},
			}
			if len(tool.Artifact.Variants) != len(want) {
				t.Fatalf("pnpm artifact variants = %+v, want exactly %d", tool.Artifact.Variants, len(want))
			}
			for arch, url := range want {
				variant, ok := tool.Artifact.VariantForArch(arch)
				if !ok {
					t.Fatalf("pnpm artifact has no variant for %s", arch)
				}
				if variant.URL != url {
					t.Errorf("pnpm %s url = %q, want %q", arch, variant.URL, url)
				}
				if !strings.HasPrefix(variant.URL, "https://github.com/pnpm/pnpm/releases/download/v"+PnpmVersion+"/") {
					t.Errorf("pnpm %s url = %q, want the pinned pnpm release download", arch, variant.URL)
				}
				if variant.Member != pnpmMember {
					t.Errorf("pnpm %s member = %q, want %q", arch, variant.Member, pnpmMember)
				}
				if variant.SHA256 != wantSHA256[arch] {
					t.Errorf("pnpm %s sha256 = %q, want exact pinned digest %q", arch, variant.SHA256, wantSHA256[arch])
				}
				if len(variant.Fallbacks) != 1 || variant.Fallbacks[0] != wantFallback[arch] {
					t.Errorf("pnpm %s fallback = %+v, want exact registry source %+v", arch, variant.Fallbacks, wantFallback[arch])
				}
			}
		})
	}
}

// TestNodeRuntimePnpmCacheIsReusableAcrossVersions verifies Node 24 and Node 26
// render the same BuildKit cache IDs for the same pinned artifacts. The cache
// therefore survives their distinct base-image layers, while the architecture-
// specific checksums keep the amd64 and arm64 artifact entries isolated.
func TestNodeRuntimePnpmCacheIsReusableAcrossVersions(t *testing.T) {
	for _, arch := range []string{"amd64", "arm64"} {
		var cacheID string
		for _, runtimeID := range []string{"nodejs24.x", "nodejs26.x"} {
			t.Run(runtimeID+"/"+arch, func(t *testing.T) {
				spec, err := lookup(runtimeID)
				if err != nil {
					t.Fatalf("lookup %s: %v", runtimeID, err)
				}
				p := plan.BuildPlan{
					BaseImage:    spec.BaseImage,
					TargetArch:   arch,
					RuntimeTools: spec.RuntimeTools,
				}
				df, err := renderDockerfile(p)
				if err != nil {
					t.Fatalf("render %s for %s: %v", runtimeID, arch, err)
				}
				lineStart := strings.Index(df, "RUN --mount=type=cache,id=")
				if lineStart < 0 {
					t.Fatalf("%s Dockerfile has no artifact cache mount:\n%s", runtimeID, df)
				}
				mount := strings.TrimPrefix(df[lineStart:], "RUN --mount=type=cache,id=")
				mount = mount[:strings.IndexByte(mount, ' ')]
				id := mount
				variant, ok := spec.RuntimeTools[0].Artifact.VariantForArch(arch)
				if !ok {
					t.Fatalf("%s has no pnpm variant for %s", runtimeID, arch)
				}
				if want := "relay-artifact-" + variant.SHA256 + ",target=/var/cache/relay-artifacts,sharing=locked"; id != want {
					t.Fatalf("%s %s cache mount = %q, want %q", runtimeID, arch, id, want)
				}
				fallback := variant.Fallbacks[0]
				for _, want := range []string{
					fallback.URL,
					fallback.SHA256 + ".tar.gz",
					"tar -xzf /tmp/relay-tool-0.tar.gz -C /tmp/relay-tool-0.unpack \"$archive_member\"",
				} {
					if !strings.Contains(df, want) {
						t.Errorf("%s %s artifact Dockerfile missing fallback/cache identity %q:\n%s", runtimeID, arch, want, df)
					}
				}
				if cacheID != "" && cacheID != id {
					t.Errorf("%s and %s must reuse the same %s cache ID: %q != %q", "nodejs24.x", runtimeID, arch, cacheID, id)
				}
				cacheID = id
			})
		}
	}
}

// testSpecTable returns a COPY of the production registry with the two
// test-only multi-version specs added, so the shared-engine tests exercise the
// real resolution/dispatch path without mutating the global specs map (which
// races any other test that reads it).
func testSpecTable() (map[string]plan.Spec, string, string) {
	const tempPython = "python3.15-test-only"
	const tempNode = "node26-test-only"
	table := make(map[string]plan.Spec, len(specs)+2)
	for k, v := range specs {
		table[k] = v
	}
	table[tempPython] = plan.Spec{Name: tempPython, Engine: plan.EnginePython, BaseImage: "python:3.15-slim"}
	table[tempNode] = plan.Spec{Name: tempNode, Engine: plan.EngineNode, BaseImage: "node:26-alpine"}
	return table, tempPython, tempNode
}

// A test-only spec can be resolved through the same mechanism (lookupIn on a
// copied table) and share an engine with an existing runtime, without enabling a
// real new runtime. It validates that a single engine serves more than one
// version, in both directions.
func TestEngineForRuntimeResolvesSharedEngine(t *testing.T) {
	table, tempPython, tempNode := testSpecTable()

	// A second Python version shares EnginePython with the production spec.
	py, err := lookupIn(table, tempPython)
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
	nd, err := lookupIn(table, tempNode)
	if err != nil {
		t.Fatalf("lookup(%s): %v", tempNode, err)
	}
	if nd.Engine != plan.EngineNode {
		t.Errorf("%s engine = %q, want %q (shared engine)", tempNode, nd.Engine, plan.EngineNode)
	}
	if specs["nodejs24.x"].Engine != nd.Engine {
		t.Errorf("nodejs24.x and %s do not share an engine", tempNode)
	}
}

// TestEngineForSharedEngineDispatch dispatches both the production and the
// test-only specs to the same engine type, so the shared engine property holds
// through the actual dispatch path, not just the registry.
func TestEngineForSharedEngineDispatch(t *testing.T) {
	table, tempPython, tempNode := testSpecTable()

	// Both python specs must produce a plan with the python entrypoint and the
	// spec's own base image; both node specs likewise for node.
	for _, name := range []string{"python3.14", tempPython} {
		spec, err := lookupIn(table, name)
		if err != nil {
			t.Fatalf("lookup(%s): %v", name, err)
		}
		eng, err := engineFor(spec)
		if err != nil {
			t.Fatalf("engineFor(%s): %v", name, err)
		}
		p, err := eng.Plan(spec, t.TempDir(), nil)
		if err != nil {
			t.Fatalf("plan(%s): %v", name, err)
		}
		if p.BaseImage != spec.BaseImage {
			t.Errorf("%s plan base image = %q, want %q", name, p.BaseImage, spec.BaseImage)
		}
		if p.Entrypoint[0] != "python" {
			t.Errorf("%s plan entrypoint = %v, want python entrypoint", name, p.Entrypoint)
		}
	}

	for _, name := range []string{"nodejs24.x", tempNode} {
		spec, err := lookupIn(table, name)
		if err != nil {
			t.Fatalf("lookup(%s): %v", name, err)
		}
		eng, err := engineFor(spec)
		if err != nil {
			t.Fatalf("engineFor(%s): %v", name, err)
		}
		p, err := eng.Plan(spec, t.TempDir(), nil)
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

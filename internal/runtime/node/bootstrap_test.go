package node

import (
	"io/fs"
	"regexp"
	"strings"
	"testing"
)

// embeddedModuleSources returns the shared embedded Node runtime module set:
// the bootstrap plus every sibling module it imports, exactly as the engine
// embeds and ships them into the /relay image directory. The resolve hook is
// excluded because it is planned separately into source-mounted images only,
// never carried by the shared module set.
func embeddedModuleSources(t *testing.T) map[string]string {
	t.Helper()
	entries, err := fs.ReadDir(runtimeModules, ".")
	if err != nil {
		t.Fatalf("read embedded node modules: %v", err)
	}
	sources := make(map[string]string)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".mjs") || name == resolveHookFile {
			continue
		}
		content, err := runtimeModules.ReadFile(name)
		if err != nil {
			t.Fatalf("read embedded module %s: %v", name, err)
		}
		sources[name] = string(content)
	}
	if len(sources) == 0 {
		t.Fatal("no embedded node modules found")
	}
	return sources
}

// joinedModuleSources joins the embedded module set's contents, so a constant
// that the split bootstrap moved into one of its sibling modules still holds a
// single assertion point for bootstrap/engine agreement.
func joinedModuleSources(t *testing.T) string {
	t.Helper()
	sources := embeddedModuleSources(t)
	joined := make([]string, 0, len(sources))
	for _, name := range []string{"bootstrap.mjs", "lifecycle.mjs", "protocol.mjs", "invocation.mjs", "handler-loader.mjs"} {
		content, ok := sources[name]
		if !ok {
			t.Fatalf("the shared module set is missing %s, got %v", name, moduleNames(sources))
		}
		joined = append(joined, content)
	}
	return strings.Join(joined, "\n")
}

// moduleNames returns the map's keys in embedded (sorted) order for diagnostics.
func moduleNames(sources map[string]string) []string {
	names := make([]string, 0, len(sources))
	for name := range sources {
		names = append(names, name)
	}
	for i := 1; i < len(names); i++ {
		for j := i; j > 0 && names[j] < names[j-1]; j-- {
			names[j], names[j-1] = names[j-1], names[j]
		}
	}
	return names
}

// TestBootstrapIsEmbeddedAndResolvesModulesByStat asserts the two things static
// source inspection is actually needed for. The integration process tests
// cover the rest of the invocation protocol (sentinel framing, module caching,
// env rotation, error handling), so they are no longer duplicated as brittle
// substring checks. The stat-sync assertion is kept because it pins the specific
// module-resolution contract: modules are resolved via the filesystem (statSync),
// never by attempting an import of a path that may not exist. The assertion
// covers the SPLIT module set (the bootstrap and the handlers module resolution
// it imports), not just the entrypoint file.
func TestBootstrapIsEmbeddedAndResolvesModulesByStat(t *testing.T) {
	if len(Bootstrap) == 0 {
		t.Fatal("embedded node bootstrap is empty")
	}
	joined := joinedModuleSources(t)
	if !strings.Contains(joined, "statSync") {
		t.Error("node bootstrap must resolve modules via filesystem stat (statSync), not import attempts")
	}
}

// TestBootstrapSourceMountConstantsMatchEngine pins the module/engine
// agreement on the HOT_RELOAD layout: the argv flag, the mount target, the
// persisted esbuild path, and the shared resolve-hook preload are embedded
// verbatim in the module the bootstrap uses (handler-loader.mjs) and must equal
// the engine's constants, since the plan writes the matching ENTRYPOINT, install
// command, and resolve-hook file.
func TestBootstrapSourceMountConstantsMatchEngine(t *testing.T) {
	joined := joinedModuleSources(t)
	for _, want := range []string{
		`process.argv.includes("` + sourceMountFlag + `")`,
		`const MOUNT_ROOT = "` + SourceMountTarget + `";`,
		`const ESBUILD_BIN = "` + mountedEsbuildBin + `";`,
		// The bootstrap delegates the ESM resolve hook to the shared module; it
		// must import it (relative to /relay/bootstrap.mjs) rather than inline it.
		`await import("./resolve-hook.mjs")`,
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("bootstrap modules must contain %q", want)
		}
	}
	// The hook implementation lives in the shared module, not the bootstrap.
	if strings.Contains(joined, "registerHooks(") {
		t.Error("bootstrap modules must not inline registerHooks; the shared resolve hook owns it")
	}
	hook := string(ResolveHook)
	for _, want := range []string{
		`const MOUNT_ROOT = "` + SourceMountTarget + `";`,
		`const BAKED_ROOT = "/app";`,
		// The generated TypeScript overlay is matched separately so a bundled
		// handler's external self-reference is intercepted there too.
		`const GENERATED_ROOT = "/tmp/relay-gen";`,
		`registerHooks({`,
		"installResolveHook();",
		// The CommonJS branch resolves through a dependency-root require and
		// defers builtins; without it Node's default require resolver ignores a
		// re-anchored parentURL and a host node_modules would shadow the image.
		"createRequire(",
		"isBuiltin(",
		"isRequireContext(",
		"shortCircuit: true",
		// The app self-reference exception resolves through the mounted manifest
		// with native ESM/require semantics.
		"isAppSelfReference(",
		"resolveAppSelfReference(",
		"appSourceAnchor",
		"appSourceRequire",
	} {
		if !strings.Contains(hook, want) {
			t.Errorf("resolve hook must contain %q", want)
		}
	}
	// The bootstrap must NOT reintroduce the filesystem-layout alias: it bypasses
	// the mounted manifest's "exports" and resolves the wrong file when a target
	// remaps. Resolution lives entirely in the shared hook.
	if strings.Contains(joined, "selfPackageAlias") || strings.Contains(joined, "--alias:") {
		t.Error("bootstrap modules must not carry the esbuild self-reference alias; the shared hook owns self-reference resolution")
	}
}

// TestBootstrapImportsOnlyEmbeddedModules pins the packaging contract the
// embed-glob is supposed to guarantee: every runtime module the entrypoint
// imports by a relative specifier is embedded beside it, so the split bootstrap
// can never import a module the engine forgot to ship into the image.
func TestBootstrapImportsOnlyEmbeddedModules(t *testing.T) {
	sources := embeddedModuleSources(t)
	specifiers := regexp.MustCompile(`(?:\bfrom\s+|\bimport\()\s*"(\./[^"]+)"`)
	found := map[string]bool{}
	for _, spec := range specifiers.FindAllStringSubmatch(sources["bootstrap.mjs"], -1) {
		name := strings.TrimPrefix(spec[1], "./")
		found[name] = true
		if _, ok := sources[name]; !ok {
			t.Errorf("bootstrap imports ./%s, which is not embedded beside it (embedded: %v)", name, moduleNames(sources))
		}
	}
	for _, required := range []string{"lifecycle.mjs", "protocol.mjs", "invocation.mjs", "handler-loader.mjs"} {
		if !found[required] {
			t.Errorf("bootstrap must import the split module %s, found imports %v", required, found)
		}
	}
}

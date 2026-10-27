package node

import (
	"strings"
	"testing"
)

// TestBootstrapIsEmbeddedAndResolvesModulesByStat asserts the two things static
// source inspection is actually needed for. The integration process tests
// cover the rest of the invocation protocol (sentinel framing, module caching,
// env rotation, error handling), so they are no longer duplicated as brittle
// substring checks. The stat-sync assertion is kept because it pins the specific
// module-resolution contract: modules are resolved via the filesystem (statSync),
// never by attempting an import of a path that may not exist.
func TestBootstrapIsEmbeddedAndResolvesModulesByStat(t *testing.T) {
	if len(Bootstrap) == 0 {
		t.Fatal("embedded node bootstrap is empty")
	}
	if !strings.Contains(string(Bootstrap), "statSync") {
		t.Error("node bootstrap must resolve modules via filesystem stat (statSync), not import attempts")
	}
}

// TestBootstrapSourceMountConstantsMatchEngine pins the bootstrap/engine
// agreement on the HOT_RELOAD layout: the argv flag, the mount target, the
// persisted esbuild path, and the shared resolve-hook preload are embedded
// verbatim in the bootstrap (or the hook module it imports) and must equal the
// engine's constants, since the plan writes the matching ENTRYPOINT, install
// command, and resolve-hook file.
func TestBootstrapSourceMountConstantsMatchEngine(t *testing.T) {
	src := string(Bootstrap)
	for _, want := range []string{
		`process.argv.includes("` + sourceMountFlag + `")`,
		`const MOUNT_ROOT = "` + SourceMountTarget + `";`,
		`const ESBUILD_BIN = "` + mountedEsbuildBin + `";`,
		// The bootstrap delegates the ESM resolve hook to the shared module; it
		// must import it (relative to /relay/bootstrap.mjs) rather than inline it.
		`await import("./resolve-hook.mjs")`,
	} {
		if !strings.Contains(src, want) {
			t.Errorf("bootstrap must contain %q", want)
		}
	}
	// The hook implementation lives in the shared module, not the bootstrap.
	if strings.Contains(src, "registerHooks(") {
		t.Error("bootstrap must not inline registerHooks; the shared resolve hook owns it")
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
	if strings.Contains(src, "selfPackageAlias") || strings.Contains(src, "--alias:") {
		t.Error("bootstrap must not carry the esbuild self-reference alias; the shared hook owns self-reference resolution")
	}
}

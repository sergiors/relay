// Package node is the Node.js runtime engine. It holds the language-specific
// preparation knowledge for Node.js (ESM) functions: the embedded bootstrap,
// the dependency handling (package-lock.json / package.json), the TypeScript
// handler transpilation, and the container entrypoint. It does NOT run docker
// build or generate Dockerfiles; it produces a generic runtime.BuildPlan that
// the Docker builder renders.
package node

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"relay/internal/runtime/plan"
)

//go:embed bootstrap.mjs
var bootstrap []byte

// Bootstrap exposes the embedded bootstrap so tests can verify its content.
var Bootstrap = bootstrap

type Engine struct{}

var entrypoint = []string{"node", "/relay/bootstrap.mjs"}

const workDir = "/app"
const pkgFile = "package.json"
const lockFile = "package-lock.json"

// tsconfigFile is the optional TypeScript compiler configuration esbuild reads
// when it is present in the function directory. Relay never type-checks; the
// file only supplies compilerOptions (strict, experimentalDecorators, paths,
// ...) that shape the transpilation.
const tsconfigFile = "tsconfig.json"

// esbuildVersion pins the esbuild the Node engine transpiles TypeScript
// handlers with. Package constant like relay.UvImageTag: upgrades are a
// one-line change, never a floating "latest". Relay installs it per build
// with the base image's own npm into an ephemeral layer (removed again), so
// the user's package.json never needs esbuild and the tooling stays out of
// the final execution layer. Note there is no public esbuild image to COPY
// a binary from (ghcr.io/evanw/esbuild does not exist).
const esbuildVersion = "0.28.2"

// Ephemeral build-tooling paths. Both live under /tmp and are created AND
// removed in the same RUN, so neither the pinned esbuild nor its npm cache ever
// becomes part of the execution image.
const (
	esbuildPrefix = "/tmp/relay-esbuild"
	esbuildCache  = "/tmp/relay-npm-cache"
	esbuildBin    = esbuildPrefix + "/node_modules/.bin/esbuild"
)

// The runtime user is a fixed numeric identity (10001:10001) shared by every
// Relay function so the container never runs as root and the identity is stable
// across images and rebuilds; the numeric id is what the kernel enforces, so the
// user/group name is cosmetic. node:24-alpine (BusyBox) has no pre-created user,
// so the Dockerfile creates one at build time via UserSetup. Node needs only read
// access to /app (ESM modules are read, not compiled to a cache by default), so
// /app is chowned to the user for symmetry with the Python engine; the read-only
// rootfs is never written at runtime.
const (
	userID   = "10001:10001"
	userName = "app"
	// userSetup runs as root at build time. /relay holds the bootstrap, which the
	// user only needs to read. chown uses the numeric id so it is independent of
	// the user/group name.
	userSetup = "addgroup -g 10001 app && adduser -D -u 10001 -G app -h /home/app -s /sbin/nologin app && chown -R 10001:10001 /app /relay"
)

// esmPackageJSON is a minimal package.json injected when the function has none,
// so that .js files are interpreted as ESM.
var esmPackageJSON = func() []byte {
	b, err := json.Marshal(map[string]string{"type": "module"})
	if err != nil {
		panic(err) // static construction; cannot fail
	}
	return b
}()

// handlerSource is one handler module resolved to the file the runtime will
// load. A JS source is executed by the bootstrap unchanged; a TS source must be
// transpiled to a generated .mjs beside it at build time.
type handlerSource struct {
	// rel is the resolved source path relative to fnDir, slash separated, e.g.
	// "src/order.ts" or "src/order/index.mts".
	rel string
	// ts reports whether rel needs esbuild transpilation.
	ts bool
}

// srcPath is the absolute path of the source inside the image (WorkDir is
// /app, and the builder stages the selected sources there).
func (s handlerSource) srcPath() string { return path.Join(workDir, s.rel) }

// outPath is the absolute path of the generated .mjs. It mirrors srcPath with
// the TypeScript extension replaced by .mjs, so it lands next to the source and
// the bootstrap's standard extension order (.mjs before .js) finds it first.
func (s handlerSource) outPath() string {
	ext := path.Ext(s.rel)
	return path.Join(workDir, strings.TrimSuffix(s.rel, ext)+".mjs")
}

// Plan returns the generic build plan for a Node.js function. It only inspects
// fnDir (for package-lock.json, package.json, tsconfig.json, and the handler
// sources) and never writes into it; any injected file is returned for the
// builder to write.
//
// handlers are the handler MODULE parts declared by the function's template
// (sorted and deduped by the caller). Each is resolved to a source file under
// fnDir; a TypeScript source is transpiled to a generated .mjs at BUILD time
// with a pinned esbuild, and the existing bootstrap then executes the generated
// file unchanged. A JavaScript source needs no build work, so a function with no
// TypeScript handlers produces no Install step at all. A module that resolves to
// neither a JS nor a TS source — or to both — fails Plan, turning what would be a
// per-invocation "module not found" into a deterministic build-time error.
func (Engine) Plan(spec plan.Spec, fnDir string, handlers []string) (plan.BuildPlan, error) {
	files := []plan.File{{
		Path:    "/relay/bootstrap.mjs",
		Content: bootstrap,
		Mode:    fs.FileMode(0o644),
	}}

	// A package.json / package-lock.json declares the function's dependencies:
	// a reusable layer that installs them into /app (the function WORKDIR).
	var deps plan.Deps

	lockErr := stat(fnDir, lockFile)
	pkgErr := stat(fnDir, pkgFile)

	switch {
	case lockErr == nil && pkgErr == nil:
		// A lockfile pins the exact tree, so npm ci reproduces it: deterministic,
		// correct, and the fastest install. Both files are listed so a lock change
		// (a different pinned tree) re-fingerprints the layer even when the
		// manifest is unchanged.
		deps = plan.Deps{Files: []string{pkgFile, lockFile}, Install: "npm ci --omit=dev", Dir: workDir}
	case pkgErr == nil:
		// No lock: npm install resolves from the manifest.
		deps = plan.Deps{Files: []string{pkgFile}, Install: "npm install --omit=dev", Dir: workDir}
	case lockErr == nil:
		// A lock without a manifest is unusual (npm ci needs both) but preserve
		// the previous engine's intent to ci-install from the lock. Only the
		// present file is listed so fingerprinting and the dep build never read a
		// missing manifest.
		//
		// Intentional surface: npm ci fails without a package.json, so this
		// branch's build fails loudly — the same failure mode as the previous
		// single-stage behavior. Operators get an explicit error rather than
		// silent misbehavior. No ESM package.json is injected here because the
		// Deps path replaces the inject.
		deps = plan.Deps{Files: []string{lockFile}, Install: "npm ci --omit=dev", Dir: workDir}
	default:
		// No package.json or lock: inject a minimal ESM package.json so .js files
		// are treated as ESM. There are no dependencies, so no Deps and no
		// dependency image.
		files = append(files, plan.File{
			Path:    filepath.Join(workDir, pkgFile),
			Content: esmPackageJSON,
			Mode:    fs.FileMode(0o644),
		})
	}

	// Resolve every handler module so a missing or ambiguous module fails the
	// build now, and collect the TypeScript ones into ONE combined install/compile
	// RUN: esbuild is installed exactly once regardless of handler count.
	tsSources, err := handlerSources(fnDir, handlers)
	if err != nil {
		return plan.BuildPlan{}, err
	}
	var install []string
	if len(tsSources) > 0 {
		install = []string{esbuildCommand(spec.Name, tsSources, fileExists(fnDir, tsconfigFile))}
	}

	return plan.BuildPlan{
		BaseImage:  spec.BaseImage,
		WorkDir:    workDir,
		Files:      files,
		Deps:       deps,
		Install:    install,
		ToolCopies: spec.ToolCopies,
		UserSetup:  userSetup,
		User:       userID,
		Entrypoint: entrypoint,
	}, nil
}

// handlerSources resolves every handler module and returns the TypeScript ones
// (in the caller's canonical order), or an error naming the first module that is
// missing, ambiguous, or invalid. JavaScript handlers are validated but produce
// no build work.
func handlerSources(fnDir string, handlers []string) ([]handlerSource, error) {
	var ts []handlerSource
	for _, module := range handlers {
		src, err := resolveHandlerSource(fnDir, module)
		if err != nil {
			return nil, err
		}
		if src.ts {
			ts = append(ts, src)
		}
	}
	return ts, nil
}

// resolveHandlerSource resolves a handler module part to the source file the
// runtime bootstrap would load. The JavaScript candidate order is EXACTLY the
// bootstrap's (see bootstrap.mjs resolveModulePath): base+".mjs", base+".js",
// base+"/index.mjs", base+"/index.js", and a found JavaScript candidate is used
// unchanged — preserving existing behavior including the .mjs-over-.js
// precedence.
//
// TypeScript candidates are probed in the engine's fixed precedence
// base+".mts", base+".ts", base+"/index.mts", base+"/index.ts" (.mts outranks
// .ts, mirroring .mjs over .js). A TypeScript source is selected only when the
// module has NO JavaScript candidate. When BOTH a JavaScript and a TypeScript
// candidate exist the resolution is ambiguous and is an error rather than a
// silent pick: the generated .mjs output would shadow or confuse the bootstrap's
// resolution, so the author must remove one. A module with neither is also an
// error — a deterministic build-time failure instead of a per-invocation
// module-not-found.
func resolveHandlerSource(fnDir, module string) (handlerSource, error) {
	if err := validateHandlerModule(module); err != nil {
		return handlerSource{}, err
	}
	base := strings.ReplaceAll(module, ".", "/")

	js := firstExisting(fnDir, jsCandidates(base))
	ts := firstExisting(fnDir, tsCandidates(base))
	switch {
	case js != "" && ts != "":
		return handlerSource{}, fmt.Errorf(
			"ambiguous handler module %q: both %s and %s exist; remove one or delete the stale transpiled file",
			module, js, ts,
		)
	case js != "":
		return handlerSource{rel: js}, nil
	case ts != "":
		return handlerSource{rel: ts, ts: true}, nil
	default:
		return handlerSource{}, fmt.Errorf(
			"handler module %q does not exist under /app (no .mjs, .js, .mts, or .ts source)", module)
	}
}

// validateHandlerModule rejects a module part that could escape the function
// directory when its dots are turned into path separators. Every dot-separated
// segment must be non-empty and must not be "." or "..". (A literal ".." always
// yields an empty segment under this split, so the rule covers traversal forms
// such as "../../etc" as well as the bare "."/".." tokens.) A module MAY itself
// contain "/" — the runtime accepts the slash form (src/order.handler -> module
// "src/order") — and the subsequent filepath/path joins are lexical, so a
// leading slash cannot escape /app either. Validation runs BEFORE any filesystem
// access so a crafted handler can never make the generated Dockerfile RUN address
// a path outside /app.
func validateHandlerModule(module string) error {
	for seg := range strings.SplitSeq(module, ".") {
		if seg == "" || seg == "." || seg == ".." {
			return fmt.Errorf("invalid handler module %q", module)
		}
	}
	return nil
}

// jsCandidates returns the module's JavaScript candidate paths (fnDir-relative,
// slash separated) in the bootstrap's exact resolution order.
func jsCandidates(base string) []string {
	var out []string
	for _, ext := range []string{".mjs", ".js"} {
		out = append(out, base+ext, base+"/index"+ext)
	}
	return out
}

// tsCandidates returns the module's TypeScript candidate paths in the engine's
// defined precedence order: a file candidate outranks its index fallback, and
// .mts outranks .ts.
func tsCandidates(base string) []string {
	return []string{
		base + ".mts",
		base + ".ts",
		base + "/index.mts",
		base + "/index.ts",
	}
}

// firstExisting returns the first candidate that is an existing regular file
// relative to fnDir, or "" when none is. A stat error other than not-exist is
// treated as absent: the resolver reports the module missing/ambiguous, which is
// the deterministic surface, rather than propagating a transient stat error.
func firstExisting(fnDir string, candidates []string) string {
	for _, rel := range candidates {
		info, err := os.Stat(filepath.Join(fnDir, filepath.FromSlash(rel)))
		if err == nil && info.Mode().IsRegular() {
			return rel
		}
	}
	return ""
}

// esbuildCommand builds ONE RUN command that installs the pinned esbuild into an
// ephemeral /tmp prefix, transpiles every TypeScript handler, then removes the
// tooling and the npm cache in the SAME layer. A single install serves every
// handler, and the cleanup keeps the build tooling out of the final execution
// layer. Paths are absolute in the image and esbuild is invoked with its own
// flag=value syntax (esbuild rejects the space-separated form).
//
// Flags: --bundle follows the user's local .ts module graph; --packages=external
// keeps bare node_modules imports out of the bundle so they resolve at runtime
// from the dependency layer; --format=esm and the .mjs output make the result
// unconditionally ESM, which the bootstrap imports dynamically; --target is the
// runtime spec name (e.g. node24) so the emitted syntax matches the managed Node.
// --tsconfig is added only when the function ships a tsconfig.json.
func esbuildCommand(specName string, sources []handlerSource, tsconfig bool) string {
	parts := []string{
		"npm install --prefix " + esbuildPrefix +
			" --no-save --cache " + esbuildCache +
			" --silent esbuild@" + esbuildVersion,
	}
	tsconfigFlag := ""
	if tsconfig {
		tsconfigFlag = " --tsconfig=" + path.Join(workDir, tsconfigFile)
	}
	for _, s := range sources {
		parts = append(parts, fmt.Sprintf(
			"%s --bundle %s --outfile=%s --format=esm --platform=node --target=%s --packages=external%s --log-level=warning",
			esbuildBin, s.srcPath(), s.outPath(), specName, tsconfigFlag,
		))
	}
	parts = append(parts, "rm -rf "+esbuildPrefix+" "+esbuildCache)
	return strings.Join(parts, " && ")
}

func stat(fnDir, name string) error {
	_, err := os.Stat(filepath.Join(fnDir, name))
	return err
}

// fileExists reports whether name exists in fnDir as a regular file.
func fileExists(fnDir, name string) bool {
	info, err := os.Stat(filepath.Join(fnDir, name))
	return err == nil && info.Mode().IsRegular()
}

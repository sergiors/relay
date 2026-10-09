// Package node is the Node.js runtime engine. It holds the language-specific
// preparation knowledge for Node.js (ESM) apps: the embedded bootstrap,
// the dependency handling (package.json / pnpm-lock.yaml, installed with pnpm),
// the TypeScript handler transpilation, and the container entrypoint. It does
// NOT run docker build or generate Dockerfiles; it produces a generic
// runtime.BuildPlan that the Docker builder renders.
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

//go:embed resolve-hook.mjs
var resolveHook []byte

// ResolveHook exposes the embedded shared resolve hook module (ESM and
// CommonJS) so tests can verify its content and the plan writes it into a
// source-mounted image.
var ResolveHook = resolveHook

// ResolveHookPath is the in-image path of the shared resolve hook module
// (see resolve-hook.mjs). The mounted plan writes the embedded ResolveHook here;
// the bootstrap dynamically imports it (./resolve-hook.mjs, relative to
// /relay/bootstrap.mjs) and a mounted Node entrypoint service preloads it with
// `node --import <ResolveHookPath>`. It is present ONLY in a source-mounted
// image; a baked image has no mount and never receives the file.
const ResolveHookPath = "/relay/resolve-hook.mjs"

type Engine struct{}

var entrypoint = []string{"node", "/relay/bootstrap.mjs"}

const workDir = "/app"
const pkgFile = "package.json"
const pnpmLockFile = "pnpm-lock.yaml"

// npmLockFile is the npm lockfile. Relay installs Node dependencies with pnpm
// only, so its presence is an error (see dependencyPlan): silently ignoring it
// would let an app believe its pinned npm tree was installed when Relay would
// instead install a pnpm tree it never resolved.
const npmLockFile = "package-lock.json"

// tsconfigFile is the optional TypeScript compiler configuration esbuild reads
// when it is present in the app directory. Relay never type-checks; the
// file only supplies compilerOptions (strict, experimentalDecorators, paths,
// ...) that shape the transpilation.
const tsconfigFile = "tsconfig.json"

// esbuildVersion pins the esbuild the Node engine transpiles TypeScript
// handlers with. Package constant like runtime.PnpmVersion: upgrades are a
// one-line change, never a floating "latest". Relay installs it per build with
// pnpm into an ephemeral layer (removed again), so the user's package.json never
// needs esbuild and the tooling stays out of the final execution layer.
const esbuildVersion = "0.28.2"

// The bootstrap and user instrumentation must resolve the same API singleton
// from /app so a user-installed SDK observes the managed invocation context.
const otelAPIVersion = "1.9.0"

// pnpmStoreDir is the single scratch content-addressable store every pnpm
// install uses. It is created and removed in the same RUN as the install that
// populated it, so the store (and the pnpm metadata it holds) never becomes
// part of an image; the installed node_modules entries are hardlinks that keep
// working after the store directory is removed.
const pnpmStoreDir = "/tmp/relay-pnpm-store"

// otelProjectDir is the private pnpm project the managed OpenTelemetry API is
// installed into. It is kept separate from /app so the managed install never
// rewrites the app's package.json or pnpm-lock.yaml (pnpm add always saves), and
// its single package is linked into /app/node_modules (see otelInstall).
const otelProjectDir = "/relay/otel"

// pnpmBin is the directly-executable pnpm the runtime image exposes. It is the
// standalone musl binary materialized by the Node runtime's artifact
// RuntimeTool (registry.pnpmTool) at /usr/local/bin/pnpm, so no wrapper and no
// `node` invocation are needed.
const pnpmBin = "/usr/local/bin/pnpm"

// pnpmInstall installs an app's production dependency tree from the committed
// pnpm lock, in the DEPENDENCY image (and in a no-dependency app image, where
// the same standalone binary was materialized). --frozen-lockfile fails the
// build if package.json and pnpm-lock.yaml disagree instead of silently
// re-resolving, and --prod drops devDependencies.
// --config.dangerouslyAllowAllBuilds=true restores npm's long-standing behavior
// of running dependency lifecycle (build) scripts: pnpm blocks them by default
// (ERR_PNPM_IGNORED_BUILDS), which would leave any dependency with a
// native/install step broken. The scratch store is removed in the same RUN so it
// never bloats the layer (the node_modules entries are hardlinks and keep
// working after the store directory is gone).
const pnpmInstall = pnpmBin + " install --prod --frozen-lockfile --config.dangerouslyAllowAllBuilds=true --store-dir " + pnpmStoreDir +
	" && rm -rf " + pnpmStoreDir

// Ephemeral build-tooling paths. Both live under /tmp and are created AND
// removed in the same RUN, so neither the pinned esbuild nor the pnpm store ever
// becomes part of the execution image.
const (
	esbuildPrefix = "/tmp/relay-esbuild"
	esbuildBin    = esbuildPrefix + "/node_modules/.bin/esbuild"
)

// Source-mount (SOURCE_MOUNT) layout for Node.
//
// The live app source is bind-mounted read-only at SourceMountTarget, a
// subdirectory of the image workdir, so /app/node_modules (installed by the
// dependency image and the app image's managed API install) stays VISIBLE
// underneath it: Node resolves a bare import from the mounted handler by walking
// up to /app/node_modules. The distinct target is what lets Node be mountable at
// all (mounting the source over /app would hide the dependencies).
//
// The whole app directory is mounted, so a host node_modules would sit at
// SourceMountTarget/node_modules and Node would search it BEFORE /app/node_modules,
// shadowing the dependency image. The unconditional guarantee is the shared
// synchronous resolve hook in resolve-hook.mjs (written to ResolveHookPath only
// for a source-mounted image): it re-anchors bare specifiers whose parent module
// lives under the mount (or under the generated TypeScript overlay) to
// /app/node_modules — through Node's own resolver for an ESM import, and through
// a dependency-root require() for a CommonJS require, because Node's default
// require resolver ignores a re-anchored parentURL — so a host node_modules that
// appears AFTER preparation — and therefore carries no mask — cannot shadow the
// dependency image. The one exception is the app's own package self-reference
// (a manifest with name+exports), which is resolved through the MOUNTED
// /app/src/package.json with native semantics, because its exports targets are
// app source paths under the mount rather than dependency-tree paths. The same module
// is consumed by both the pooled invocation bootstrap (dynamically imported when
// mounted) and a mounted Node entrypoint service, which never runs the bootstrap
// and instead preloads the hook with `node --import /relay/resolve-hook.mjs`. In
// addition, Relay masks that one bounded path (SourceMountNodeModules) with an
// empty read-only filesystem whenever the host app root already carries it; the
// mask is best-effort defense-in-depth for resolution paths the hook cannot
// intercept, but it can only be created when the path exists at prepare time
// because Docker cannot create a mountpoint inside the read-only source bind.
// Relay runs the container with SourceMountTarget as its working directory so
// process.cwd() and relative paths see the app root.
//
// The bootstrap learns it is mounted from the --source-mount flag the plan puts
// on the image ENTRYPOINT (argv, not environment, so a template env value can
// never redirect module resolution), resolves handlers from SourceMountTarget,
// and bundles any TypeScript handler at container startup with the same pinned
// esbuild the baked build used. That is why the mounted image PERSISTS esbuild at
// mountedEsbuildPrefix instead of removing it in the build layer.
const (
	// sourceMountFlag marks a source-mounted image's bootstrap invocation.
	sourceMountFlag = "--source-mount"
	// SourceMountTarget is where a SOURCE_MOUNT bind mounts the app source.
	SourceMountTarget = "/app/src"
	// SourceMountNodeModules is the path masked with an empty read-only
	// filesystem under SOURCE_MOUNT WHEN the host already carries it at prepare
	// time. The host app directory is mounted whole at SourceMountTarget, so a
	// host node_modules would sit at /app/src/node_modules and Node's module
	// resolution would find it BEFORE /app/node_modules, letting host modules
	// shadow the dependency image. The mask is best-effort defense-in-depth for
	// resolution paths outside the hook; the shared resolve hook
	// (resolve-hook.mjs) already re-anchors bare imports AND requires to
	// /app/node_modules whether or not a mask exists, so a host node_modules
	// created after preparation cannot shadow the dependency image either.
	SourceMountNodeModules = SourceMountTarget + "/node_modules"
	// mountedEsbuildPrefix is the persistent image path the pinned esbuild is
	// installed into for a source-mounted app.
	mountedEsbuildPrefix = "/relay/esbuild"
	// mountedEsbuildBin is the esbuild executable the bootstrap runs.
	mountedEsbuildBin = mountedEsbuildPrefix + "/node_modules/.bin/esbuild"
)

// otelInstall is the managed OpenTelemetry API install, applied to EVERY Node
// image (mounted or baked) so the bootstrap and user modules resolve one shared
// API singleton from /app/node_modules.
//
// It installs with pnpm into a PRIVATE project (otelProjectDir) and links the
// single package into /app/node_modules rather than running `pnpm add` in /app:
// pnpm add always saves, so installing in /app would rewrite the app's
// package.json and pnpm-lock.yaml. @opentelemetry/api has no dependencies, so
// one symlink is the whole install and the bootstrap's createRequire from /app
// resolves the same singleton user modules do. The store is removed in the same
// RUN. This preserves the previous npm `--no-save` behavior (the managed version
// wins over a declared one) without touching the app's manifest. It invokes the
// standalone pnpm binary the runtime tool materialized at pnpmBin.
//
// The target package path is removed before the link is created: the app's
// vendored node_modules may already carry a REAL @opentelemetry/api directory,
// and `ln -sfn` treats an existing real directory as a destination to link
// INSIDE (creating .../api/api or failing) rather than replacing it. Removing
// only the package path (never the parent @opentelemetry scope, which may hold
// other packages) makes the managed version win unconditionally. `rm -rf` does
// not follow a symlink, so it is also correct when a previous install already
// left a link there.
const otelInstall = "mkdir -p " + otelProjectDir +
	" && printf '%s' '{\"name\":\"relay-otel\",\"private\":true}' > " + otelProjectDir + "/package.json" +
	" && pnpm add -C " + otelProjectDir + " --store-dir " + pnpmStoreDir + " --save-exact @opentelemetry/api@" + otelAPIVersion +
	" && mkdir -p " + workDir + "/node_modules/@opentelemetry" +
	" && rm -rf " + workDir + "/node_modules/@opentelemetry/api" +
	" && ln -s " + otelProjectDir + "/node_modules/@opentelemetry/api " + workDir + "/node_modules/@opentelemetry/api" +
	" && rm -rf " + pnpmStoreDir

// The runtime user is a fixed numeric identity (10001:10001) shared by every
// Relay app so the container never runs as root and the identity is stable
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

// esmPackageJSON is a minimal package.json injected when the app has none,
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

// Plan returns the generic build plan for a Node.js app. It only inspects
// fnDir (for package.json, pnpm-lock.yaml, tsconfig.json, and the handler
// sources) and never writes into it; any injected file is returned for the
// builder to write.
//
// Dependency detection is deterministic and pnpm-only (see dependencyPlan):
// package.json + pnpm-lock.yaml install with pnpm; a package.json without a
// pnpm lock, a pnpm lock without a package.json, or any package-lock.json are
// errors with actionable messages. No npm fallback exists.
//
// handlers are the handler MODULE parts declared by the app's template
// (sorted and deduped by the caller). Each is resolved to a source file under
// fnDir; a TypeScript source is transpiled to a generated .mjs at BUILD time
// with a pinned esbuild, and the existing bootstrap then executes the generated
// file unchanged. A JavaScript source needs no build work, so an app with no
// TypeScript handlers produces no Install step at all. A module that resolves to
// neither a JS nor a TS source — or to both — fails Plan, turning what would be a
// per-invocation "module not found" into a deterministic build-time error.
//
// When spec.SourceMounted is set the plan instead describes a
// source-independent image: no source is baked (the caller omits the COPY), the
// live source is mounted at SourceMountTarget, the shared resolve hook is
// written to ResolveHookPath (the bootstrap imports it and a mounted Node
// entrypoint service preloads it), and the pinned esbuild is
// installed PERSISTENTLY so the bootstrap can bundle TypeScript handlers at
// container startup (a source edit is not a rebuild). Handler resolution still
// runs at plan time, so a missing or ambiguous module fails the build before any
// container starts. The mounted plan is a deterministic function of non-source
// inputs only, so a source-only edit reuses the same image tag.
func (Engine) Plan(spec plan.Spec, fnDir string, handlers []string) (plan.BuildPlan, error) {
	files := []plan.File{{
		Path:    "/relay/bootstrap.mjs",
		Content: bootstrap,
		Mode:    fs.FileMode(0o644),
	}}

	// The app's dependency layer is derived from its manifests. dependencyPlan
	// enforces the pnpm-only policy: a package.json must ship a pnpm-lock.yaml,
	// and any package-lock.json is rejected. When the app declares neither a
	// package.json nor a lock, a minimal ESM package.json is injected so .js
	// files are treated as ESM and there is no dependency image.
	deps, err := dependencyPlan(fnDir)
	if err != nil {
		return plan.BuildPlan{}, err
	}
	if deps.IsZero() && !fileExists(fnDir, pkgFile) {
		files = append(files, plan.File{
			Path:    filepath.Join(workDir, pkgFile),
			Content: esmPackageJSON,
			Mode:    fs.FileMode(0o644),
		})
	}

	// Resolve every handler module so a missing or ambiguous module fails the
	// build now. For a baked image the TypeScript ones are collected into ONE
	// combined install/compile RUN (esbuild is installed exactly once regardless
	// of handler count); for a source-mounted image the same resolution is a pure
	// validation step because transpilation happens at container startup.
	tsSources, err := handlerSources(fnDir, handlers)
	if err != nil {
		return plan.BuildPlan{}, err
	}

	// entry is the image ENTRYPOINT. A source-mounted image adds the
	// --source-mount flag so the bootstrap resolves from the mounted root and
	// bundles TypeScript on demand; a baked image keeps the two-element form
	// byte-for-byte.
	entry := entrypoint
	// mountTarget is the in-container path a SOURCE_MOUNT bind targets (empty for
	// a baked image, where the caller mounts nothing).
	var mountTarget string
	// mountWorkDir is the working directory a SOURCE_MOUNT container runs with so
	// process.cwd() and relative paths see the app root (empty preserves the
	// image workdir).
	var mountWorkDir string
	// mountMasks are the bounded in-container paths masked with an empty,
	// read-only filesystem so the whole-directory bind cannot shadow them.
	var mountMasks []string
	// The runtime tool materializes the standalone pnpm binary at pnpmBin before
	// any Install RUN; the app image then installs the managed API and any build
	// tooling through it.
	install := []string{otelInstall}
	if spec.SourceMounted {
		entry = []string{"node", "/relay/bootstrap.mjs", sourceMountFlag}
		mountTarget = SourceMountTarget
		mountWorkDir = SourceMountTarget
		// The shared resolve hook is written into the image ONLY for a
		// source-mounted app: the bootstrap imports it and a mounted Node
		// entrypoint service preloads it, and a baked image has no mount so it
		// must never carry the file (it would otherwise change the baked plan).
		files = append(files, plan.File{
			Path:    ResolveHookPath,
			Content: resolveHook,
			Mode:    fs.FileMode(0o644),
		})
		// Mask the host node_modules only when it exists: the whole-directory bind
		// already carries the mountpoint in that case, whereas Docker cannot create
		// a mountpoint inside the read-only bind when the path is absent. This is
		// best-effort defense-in-depth for resolution paths outside the hook; the
		// shared resolve hook is the unconditional guarantee for bare import and
		// require resolution, so a host node_modules created after this plan
		// cannot shadow the dependency image for an invocation or an entrypoint
		// service.
		if dirExists(fnDir, "node_modules") {
			mountMasks = []string{SourceMountNodeModules}
		}
		install = append(install, mountedEsbuildInstall())
	} else if len(tsSources) > 0 {
		install = append(install, esbuildCommand(spec.Name, tsSources, fileExists(fnDir, tsconfigFile)))
	}

	return plan.BuildPlan{
		BaseImage:          spec.BaseImage,
		WorkDir:            workDir,
		Files:              files,
		Deps:               deps,
		Install:            install,
		RuntimeTools:       spec.RuntimeTools,
		UserSetup:          userSetup,
		User:               userID,
		Entrypoint:         entry,
		SourceMountTarget:  mountTarget,
		SourceMountWorkDir: mountWorkDir,
		SourceMountMasks:   mountMasks,
	}, nil
}

// dependencyPlan resolves fnDir's Node dependency manifests into a Deps value
// (zero when the app declares none) or an actionable error. Node dependencies
// are installed with pnpm only:
//
//   - package.json + pnpm-lock.yaml: a frozen production install from the
//     committed lock. Both files are listed so a change to either the declared
//     dependency set or the resolved lock re-fingerprints the layer.
//   - package.json without pnpm-lock.yaml: an error — Relay never resolves a
//     fresh tree, so the app must commit a pnpm lock.
//   - pnpm-lock.yaml without package.json: an error — a lock alone cannot be
//     installed.
//   - package-lock.json (npm), with or without a pnpm lock: an error — npm is
//     not a supported package manager for Relay-managed Node apps, and silently
//     ignoring an npm lock would let an app believe its pinned tree was used.
//   - none: no dependency layer (Plan injects an ESM package.json).
func dependencyPlan(fnDir string) (plan.Deps, error) {
	pkg := fileExists(fnDir, pkgFile)
	pnpmLock := fileExists(fnDir, pnpmLockFile)
	npmLock := fileExists(fnDir, npmLockFile)

	if npmLock {
		if pnpmLock {
			return plan.Deps{}, fmt.Errorf(
				"%s is not supported: Relay installs Node dependencies with pnpm. Remove %s and keep only %s (run `pnpm install --lockfile-only`)",
				npmLockFile, npmLockFile, pnpmLockFile)
		}
		return plan.Deps{}, fmt.Errorf(
			"%s is not supported: Relay installs Node dependencies with pnpm. Remove it and commit a %s (run `pnpm install --lockfile-only`)",
			npmLockFile, pnpmLockFile)
	}

	switch {
	case pkg && pnpmLock:
		return plan.Deps{Files: []string{pkgFile, pnpmLockFile}, Install: pnpmInstall, Dir: workDir}, nil
	case pkg:
		return plan.Deps{}, fmt.Errorf(
			"%s is present but %s is missing: Relay installs Node dependencies only from a committed pnpm lock. Run `pnpm install --lockfile-only` and commit %s",
			pkgFile, pnpmLockFile, pnpmLockFile)
	case pnpmLock:
		return plan.Deps{}, fmt.Errorf(
			"%s is present without %s: commit both files together (run `pnpm install --lockfile-only`)",
			pnpmLockFile, pkgFile)
	default:
		return plan.Deps{}, nil
	}
}

// pnpmEsbuildAdd returns the command that installs the pinned esbuild into
// prefix with pnpm. prefix is created first because pnpm's --dir requires an
// existing directory, and --allow-build=esbuild lets esbuild's install script
// validate its prebuilt platform binary (pnpm blocks dependency build scripts by
// default). --save-exact pins the installed version. The caller removes the
// scratch pnpm store in the same RUN.
func pnpmEsbuildAdd(prefix string) string {
	return "mkdir -p " + shellQuoteArg(prefix) +
		" && pnpm add -C " + shellQuoteArg(prefix) +
		" --store-dir " + shellQuoteArg(pnpmStoreDir) +
		" --save-exact --allow-build=esbuild esbuild@" + shellQuoteArg(esbuildVersion)
}

// mountedEsbuildInstall returns the single RUN that installs the pinned esbuild
// into the image's PERSISTENT path (mountedEsbuildPrefix) for a source-mounted
// app. Unlike the baked build, the tool is deliberately NOT removed: the
// bootstrap runs it at container startup to bundle TypeScript handlers from the
// live mount. The pnpm store is still created and removed in the SAME layer so
// it never bloats the image.
func mountedEsbuildInstall() string {
	return pnpmEsbuildAdd(mountedEsbuildPrefix) + " && rm -rf " + shellQuoteArg(pnpmStoreDir)
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

// validateHandlerModule rejects a module part that could escape the app
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
// ephemeral /tmp prefix with pnpm, transpiles every TypeScript handler, then
// removes the tooling and the pnpm store in the SAME layer. A single install
// serves every handler, and the cleanup keeps the build tooling out of the final
// execution layer. Paths are absolute in the image and esbuild is invoked with
// its own flag=value syntax (esbuild rejects the space-separated form).
//
// Flags: --bundle follows the user's local .ts module graph; --packages=external
// keeps bare node_modules imports out of the bundle so they resolve at runtime
// from the dependency layer; --format=esm and the .mjs output make the result
// unconditionally ESM, which the bootstrap imports dynamically; --target is the
// runtime spec name (e.g. node24) so the emitted syntax matches the managed Node.
// --tsconfig is added only when the app ships a tsconfig.json.
//
// Shell safety: every value interpolated into the RUN is passed through
// shellQuoteArg, so a handler-derived path (a user may name a source file with
// shell metacharacters, e.g. "o$rder.ts" or "a;b.ts") is always a single
// argument and is never expanded, split, or executed by the build shell. The
// fixed prefixes/stores/versions are constants and pass through quoted-or-bare
// like any other value.
func esbuildCommand(specName string, sources []handlerSource, tsconfig bool) string {
	parts := []string{pnpmEsbuildAdd(esbuildPrefix)}
	tsconfigFlag := ""
	if tsconfig {
		tsconfigFlag = " --tsconfig=" + shellQuoteArg(path.Join(workDir, tsconfigFile))
	}
	for _, s := range sources {
		parts = append(parts, fmt.Sprintf(
			"%s --bundle %s --outfile=%s --format=esm --platform=node --target=%s --packages=external%s --log-level=warning",
			shellQuoteArg(esbuildBin),
			shellQuoteArg(s.srcPath()),
			shellQuoteArg(s.outPath()),
			shellQuoteArg(specName),
			tsconfigFlag,
		))
	}
	parts = append(parts, "rm -rf "+shellQuoteArg(esbuildPrefix)+" "+shellQuoteArg(pnpmStoreDir))
	return strings.Join(parts, " && ")
}

// shellQuoteArg renders s as exactly one POSIX shell word. A string made only of
// unambiguously safe bytes (ASCII letters/digits and the conservative set
// "_@%+=:,./-") is emitted bare so ordinary absolute paths keep their familiar,
// readable form; ANY other byte — every shell metacharacter ("$", backtick,
// ";", "|", "&", "(", ")", "<", ">", "*", "?", "[", "]", "{", "}", "!", "~",
// "#", double quote, single quote, backslash, whitespace, and every non-ASCII
// byte) — forces POSIX single-quote quoting. The only escape needed inside a
// single-quoted word is for an embedded single quote. Inside single quotes the
// shell performs no expansion, splitting, or command substitution, so the value
// can never escape its argument position.
func shellQuoteArg(s string) string {
	if s == "" {
		return "''"
	}
	if isShellSafeArg(s) {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// isShellSafeArg reports whether every byte of s is in the conservative
// shell-safe set. It is deliberately strict: a byte outside the set always
// triggers quoting rather than relying on positional shell behavior.
func isShellSafeArg(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '_', c == '@', c == '%', c == '+', c == '=', c == ':', c == ',', c == '.', c == '/', c == '-':
		default:
			return false
		}
	}
	return true
}

// fileExists reports whether name exists in fnDir as a regular file.
func fileExists(fnDir, name string) bool {
	info, err := os.Stat(filepath.Join(fnDir, name))
	return err == nil && info.Mode().IsRegular()
}

// dirExists reports whether name exists in fnDir as a directory (following
// symlinks). It is used to decide whether the whole-directory SOURCE_MOUNT bind
// carries a node_modules that could shadow the dependency image; a non-directory
// entry is not a shadowing tree.
func dirExists(fnDir, name string) bool {
	info, err := os.Stat(filepath.Join(fnDir, name))
	return err == nil && info.IsDir()
}

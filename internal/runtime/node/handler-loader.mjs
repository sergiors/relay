import { statSync, mkdirSync, symlinkSync } from "node:fs";
import { spawnSync } from "node:child_process";

// Source-mount (HOT_RELOAD) support, mirroring internal/runtime/node/engine.go
// (SourceMountTarget, the mounted esbuild path, and the pinned esbuild version).
//
// A source-mounted image appends --source-mount to the ENTRYPOINT and bind-mounts
// the live app source read-only at MOUNT_ROOT, a SUBDIRECTORY of the image workdir,
// so BAKED_ROOT/node_modules (the dependency layer plus the managed OTel API) and
// the persisted esbuild stay visible. Handlers resolve from SOURCE_ROOT; a
// TypeScript handler is bundled on demand with the same pinned esbuild the baked
// build used, into the writable /tmp overlay, and the bare packages it leaves
// external resolve through the overlay's node_modules symlink to
// BAKED_ROOT/node_modules. The mode is carried on argv (never the environment),
// so a template env value can never redirect module resolution.
//
// Bare package resolution from the mounted source is FIXED to the dependency tree
// by the shared synchronous resolve hook in /relay/resolve-hook.mjs,
// independent of the prepare-time mask: a host node_modules created under
// MOUNT_ROOT AFTER preparation (so it carries no mask) can no longer shadow
// BAKED_ROOT/node_modules. The hook re-anchors bare specifiers for both module
// systems — an ESM import through Node's own resolver with the parent URL
// re-anchored at BAKED_ROOT, and a CommonJS require() through a require() bound to
// BAKED_ROOT, because Node's default require resolver ignores a re-anchored
// parentURL. The one exception is the app's own package self-reference (a
// manifest with name+exports), which resolves through the mounted manifest
// instead. The same module is preloaded by a mounted Node entrypoint service
// (`node --import /relay/resolve-hook.mjs`), which never runs this bootstrap. The
// hook re-anchors only bare specifiers; relative, absolute, URL, and
// package-imports specifiers keep MOUNT_ROOT as their resolution root.
const MOUNTED = process.argv.includes("--source-mount");
const MOUNT_ROOT = "/app/src";
const BAKED_ROOT = "/app";
const SOURCE_ROOT = MOUNTED ? MOUNT_ROOT : BAKED_ROOT;
const GENERATED_ROOT = "/tmp/relay-gen";
const ESBUILD_BIN = "/relay/esbuild/node_modules/.bin/esbuild";
// The esbuild --target tracks the running Node major (nodejs26.x -> node26).
const NODE_TARGET = "node" + process.versions.node.split(".")[0];

// A mounted container runs Node 24 or 26, whose synchronous registerHooks API the
// shared hook relies on. Import /relay/resolve-hook.mjs (present only in a
// source-mounted image) for its side effect: it self-installs the resolve hook
// so a bare import OR require from the mounted source always resolves from the
// dependency tree, whether or not the plan emitted a mask. The same module is
// preloaded by a mounted Node entrypoint service, which does not run this
// bootstrap. Top-level await is safe here: user modules are only imported later,
// inside an invocation, so the hook is installed before any user code loads.
if (MOUNTED) {
  await import("./resolve-hook.mjs");
}

// moduleCache caches, per resolved file path, the dynamic import() promise so
// a module is imported exactly once and its state persists.
const moduleCache = new Map();

// isFile reports whether candidate is an existing regular file. A stat error
// other than not-exist is treated as absent, so resolution falls through to the
// next candidate rather than failing on a transient error.
function isFile(candidate) {
  try {
    return statSync(candidate).isFile();
  } catch (e) {
    return false;
  }
}

// resolveModulePath resolves a module part to a file under SOURCE_ROOT without
// importing anything: "index" -> ./index.js/.mjs or ./index/index.js/.mjs;
// "src.email" -> ./src/email.js/.mjs. Candidates are checked via statSync, not
// import attempts, so exactly one module is imported and a broken user module
// surfaces its own real error instead of being mistaken for "module not found".
// The resolved path is cached per module part.
//
// The JavaScript candidate order is EXACTLY the historical one (.mjs before .js,
// file before index). For a source-mounted image, when no JavaScript candidate
// exists a TypeScript source is resolved and transpiled on demand (see
// transpileHandler); a baked image already carries the generated .mjs beside the
// source, so the JavaScript probe finds it and this fallback never runs.
function resolveModulePath(modulePart) {
  const cacheKey = "path:" + modulePart;
  if (moduleCache.has(cacheKey)) {
    return moduleCache.get(cacheKey);
  }
  const parts = modulePart.split(".");
  const base = SOURCE_ROOT + "/" + parts.join("/");
  let modPath = null;

  for (const ext of [".mjs", ".js"]) {
    for (const cand of [base + ext, base + "/index" + ext]) {
      if (isFile(cand)) {
        modPath = cand;
        break;
      }
    }
    if (modPath !== null) break;
  }

  if (modPath === null && MOUNTED) {
    modPath = transpileHandler(base);
  }

  if (modPath !== null) {
    moduleCache.set(cacheKey, modPath);
  }
  return modPath;
}

// transpileHandler resolves a TypeScript handler with the engine's fixed
// precedence (.mts before .ts, file before index; a JS source always wins and is
// handled before this is reached) and bundles it to a generated .mjs beside the
// same relative path under GENERATED_ROOT, using the pinned esbuild persisted in
// the image. --bundle follows the user's local module graph, --format=esm and the
// .mjs output make the result unconditionally ESM, and the tsconfig is passed
// only when the app ships one. Returns null when the module has no TypeScript
// source.
//
// --packages=external keeps bare node_modules imports out of the bundle for
// runtime resolution, and package subpath imports ("#...") are resolved at
// bundle time. A package SELF-REFERENCE (a handler importing its own package by
// name, e.g. "myapp/lib/util") is emitted as an EXTERNAL import. The generated
// bundle lives under GENERATED_ROOT, outside the app's package scope, so Node
// cannot resolve that self-reference from the generated tree alone; the shared
// resolve hook (resolve-hook.mjs) intercepts it and resolves it through the
// MOUNTED app manifest's own "exports" and conditions, so the manifest's real
// targets apply. esbuild is deliberately given no filesystem-layout alias: an
// alias would bypass the manifest's "exports" (a target that remaps elsewhere
// would resolve the wrong file or fail).
function transpileHandler(base) {
  let source = null;
  for (const cand of [
    base + ".mts",
    base + ".ts",
    base + "/index.mts",
    base + "/index.ts",
  ]) {
    if (isFile(cand)) {
      source = cand;
      break;
    }
  }

  if (source === null) {
    return null;
  }

  ensureGeneratedOverlay();
  const rel = source.slice(MOUNT_ROOT.length + 1);
  const out = GENERATED_ROOT + "/" + rel.replace(/\.(mts|ts)$/, ".mjs");
  try {
    mkdirSync(out.slice(0, out.lastIndexOf("/")), { recursive: true });
  } catch (e) {
    // Let esbuild surface a clearer error if the output directory is unusable.
  }
  const args = [
    "--bundle",
    source,
    "--outfile=" + out,
    "--format=esm",
    "--platform=node",
    "--target=" + NODE_TARGET,
    "--packages=external",
    "--log-level=warning",
  ];
  const tsconfig = MOUNT_ROOT + "/tsconfig.json";
  if (isFile(tsconfig)) {
    args.push("--tsconfig=" + tsconfig);
  }

  const res = spawnSync(ESBUILD_BIN, args, { encoding: "utf8" });
  if (res.error) {
    throw new Error("esbuild failed to start: " + res.error.message);
  }
  if (res.status !== 0) {
    throw new Error(
      "esbuild failed for " + source + ": " + (res.stderr || "").trim(),
    );
  }

  return out;
}

let generatedOverlayReady = false;

// ensureGeneratedOverlay creates the writable generated-module root and a
// node_modules symlink to /app/node_modules, so a generated .mjs resolves bare
// packages (including @opentelemetry/api, keeping the one API singleton the
// bootstrap and user modules share) exactly as a handler inside /app would.
function ensureGeneratedOverlay() {
  if (generatedOverlayReady) {
    return;
  }

  mkdirSync(GENERATED_ROOT, { recursive: true });
  try {
    symlinkSync("/app/node_modules", GENERATED_ROOT + "/node_modules");
  } catch (e) {
    // Already present; reuse it.
  }

  generatedOverlayReady = true;
}

// loadModule resolves and imports the module, caching the import promise.
// Any error (a broken dependency, a syntax error) propagates as the real
// error of an INVOCATION (ok:false), keeping the container healthy.
async function loadModule(modPath) {
  const cacheKey = "mod:" + modPath;
  if (!moduleCache.has(cacheKey)) {
    moduleCache.set(cacheKey, import(modPath));
  }
  return await moduleCache.get(cacheKey);
}

export async function loadHandler(handler) {
  const idx = handler.lastIndexOf(".");
  const modulePart = idx > 0 ? handler.slice(0, idx) : "";
  const funcName =
    idx > 0 && idx < handler.length - 1 ? handler.slice(idx + 1) : "";
  if (!modulePart || !funcName) {
    throw new Error("invalid handler " + JSON.stringify(handler));
  }

  const modPath = resolveModulePath(modulePart);
  if (modPath === null) {
    throw new Error("module not found: " + JSON.stringify(modulePart));
  }

  const mod = await loadModule(modPath);
  let fn = mod[funcName];
  if (fn === undefined && mod.default && typeof mod.default === "object") {
    fn = mod.default[funcName];
  }
  if (typeof fn !== "function") {
    throw new Error(
      "module " +
        JSON.stringify(modulePart) +
        " has no function " +
        JSON.stringify(funcName),
    );
  }
  return fn;
}

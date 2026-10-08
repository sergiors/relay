// Relay's Node.js function bootstrap.
//
// One execution container per function STAYS ALIVE between
// invocations and serves a persistent, line-delimited JSON protocol on
// stdin/stdout (see internal/runtime/protocol.go in the Relay source):
//
//   Relay -> stdin:  {"id":"<hex>","handler":"mod.func","event":<raw>,"env":{...}}
//   stdout:          @@RELAY@@{"id":"<id>","ok":true}
//                    @@RELAY@@{"id":"<id>","ok":false,"error":"..."}
//
// Everything not sentinel-prefixed is user output, proxied by Relay as-is.
//
// Process-model notes (intended semantics, not a bug):
//   - Handler errors (including async rejections caught at the await) fail
//     THAT invocation (ok:false) and the loop continues: the container is
//     kept healthy.
//   - A malformed request line is a fatal protocol failure: process.exit(1)
//     so Relay discards the container. No uncaughtException/unhandledRejection
//     handlers are installed: a fatal crash must crash the process.
//   - EOF on stdin means Relay is done with the container; exit 0 cleanly.
//   - A dynamic import() promise is CACHED per resolved module path, so
//     module-level state persists across invocations; per-request env is
//     applied EXACTLY per invocation: a key absent from the new frame is
//     restored to its pre-Relay baseline (or removed) and unrelated
//     OS/system/container variables are preserved (see applyEnv).
//     RELAY_HANDLER is reserved and overwritten every invocation.

import { statSync, mkdirSync, symlinkSync } from "node:fs";
import * as readline from "node:readline";
import { createRequire } from "node:module";
import { spawnSync } from "node:child_process";

// Source-mount (SOURCE_MOUNT) support, mirroring internal/runtime/node/engine.go
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
// The esbuild --target tracks the managed runtime version (node24 -> node24).
const NODE_TARGET = "node" + process.versions.node.split(".")[0];

// Resolve from BAKED_ROOT so the bootstrap and user modules share one API
// singleton. The managed API always lives in BAKED_ROOT/node_modules, whether the
// source is baked or mounted.
const { context, propagation } = createRequire(BAKED_ROOT + "/package.json")(
  "@opentelemetry/api",
);

// Must match the Relay-side constant "@@RELAY@@" (internal/runtime/protocol.go).
const SENTINEL = "@@RELAY@@";

// Response error strings are operator log text only; bound them. The cap is the
// UTF-8 byte length of the WHOLE response frame — the sentinel, the JSON
// envelope, and the error — so a frame can never exceed the cap and be split
// mid-line by Relay's 4 KiB stdout line demuxer (Go's maxPending). It matches the
// Go bootstrap-side cap maxResponseFrame (3 KiB), leaving ~1 KiB of margin under
// the demuxer limit. Truncation is applied on a Unicode code-point boundary, so a
// multibyte message can never be cut mid-character and the emitted frame is
// always valid UTF-8.
const MAX_FRAME_BYTES = 3 * 1024;

// moduleCache caches, per resolved file path, the dynamic import() promise so
// a module is imported exactly once and its state persists.
const moduleCache = new Map();

// Relay-managed invocation env: exact per-request application without wiping
// the process's OS/system/container variables.
//
// The bootstrap is a long-lived, reused process, so per-request env keys would
// otherwise linger from an earlier invocation. Instead of blindly writing every
// frame value into process.env (which leaked a removed/rotated key into later
// invocations), the bootstrap tracks only the keys Relay itself applies:
//   appliedEnv  — keys currently owned by Relay in process.env
//   baselineEnv — for each owned key, the value process.env had BEFORE Relay
//                 first overrode it, or MISSING when it did not exist then
// applyEnv applies one frame's env EXACTLY: a previously applied key absent
// from the new frame is restored to its pre-Relay baseline (or deleted), a
// present key is set to the new value (its baseline captured once, on first
// application), and every unrelated variable is left untouched. Cleanup runs at
// the top of each request, before the next env is applied.
const MISSING = Symbol("relay-env-missing");
const appliedEnv = new Set();
const baselineEnv = new Map();

function applyEnv(env) {
  const incoming = new Map();
  if (env && typeof env === "object") {
    for (const [k, v] of Object.entries(env)) {
      incoming.set(String(k), String(v));
    }
  }
  for (const key of Array.from(appliedEnv)) {
    if (!incoming.has(key)) {
      if (baselineEnv.get(key) === MISSING || !baselineEnv.has(key)) {
        delete process.env[key];
      } else {
        process.env[key] = baselineEnv.get(key);
      }
      baselineEnv.delete(key);
      appliedEnv.delete(key);
    }
  }
  for (const [key, value] of incoming) {
    if (!appliedEnv.has(key)) {
      baselineEnv.set(
        key,
        Object.prototype.hasOwnProperty.call(process.env, key)
          ? process.env[key]
          : MISSING,
      );
    }
    process.env[key] = value;
    appliedEnv.add(key);
  }
}

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

// A mounted container runs Node 24, whose synchronous registerHooks API the
// shared hook relies on. Import /relay/resolve-hook.mjs (present only in a
// source-mounted image) for its side effect: it self-installs the resolve hook
// so a bare import OR require from the mounted source always resolves from the
// dependency tree, whether or not the plan emitted a mask. The same module is
// preloaded by a mounted Node entrypoint service, which does not run this
// bootstrap. Top-level await is safe here: user modules are only imported later,
// inside handle(), so the hook is installed before any user code loads.
if (MOUNTED) {
  await import("./resolve-hook.mjs");
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
  if (moduleCache.has(cacheKey)) return moduleCache.get(cacheKey);
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
  if (modPath !== null) moduleCache.set(cacheKey, modPath);
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
  if (source === null) return null;
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
  if (isFile(tsconfig)) args.push("--tsconfig=" + tsconfig);
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
  if (generatedOverlayReady) return;
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

function respond(id, ok, error) {
  const frame = { id, ok };
  if (!ok) {
    frame["error"] = boundErrorFrame(id, String(error ?? ""));
  }
  process.stdout.write(SENTINEL + JSON.stringify(frame) + "\n");
}

// boundErrorFrame returns an error string such that the whole serialized
// response frame (sentinel + JSON envelope + error) fits in MAX_FRAME_BYTES
// UTF-8 bytes, while always remaining valid UTF-8.
//
// The budget is the frame cap minus the exact size of the sentinel, the envelope
// with an empty error, and the trailing newline. The message is then accumulated
// one Unicode code point at a time (for...of iterates code points, never UTF-16
// halves, so a surrogate pair or multibyte character is never split). Each code
// point's contribution is its EXACT serialized length — including any JSON
// escaping of quotes, backslashes, and control characters — computed with
// JSON.stringify, so the frame is bounded precisely and the result always
// round-trips through JSON.stringify (the emitted frame is parseable).
function boundErrorFrame(id, message) {
  const overhead = Buffer.byteLength(
    SENTINEL + JSON.stringify({ id, ok: false, error: "" }) + "\n",
    "utf8",
  );
  const budget = MAX_FRAME_BYTES - overhead;
  if (budget <= 0) {
    return "";
  }
  let msg = "";
  let used = 0;
  for (const cp of message) {
    const n = jsonEscapedBytes(cp);
    if (used + n > budget) {
      break;
    }
    msg += cp;
    used += n;
  }
  return msg;
}

// jsonEscapedBytes is the number of UTF-8 bytes one code point contributes
// inside a JSON string (its escaped form, without the surrounding quotes).
// JSON escaping is per-character, so summing these is exactly the error field's
// serialized contribution.
function jsonEscapedBytes(cp) {
  const quoted = JSON.stringify(cp);
  return Buffer.byteLength(quoted.slice(1, -1), "utf8");
}

async function handle(line) {
  let req;
  try {
    req = JSON.parse(line);
  } catch (e) {
    // Malformed request frame: fatal protocol failure (the id is not
    // recoverable, so no response is possible). Terminate so Relay discards.
    console.error("invalid request frame: " + (e && e.message ? e.message : e));
    process.exit(1);
  }

  const id = typeof req.id === "string" ? req.id : "";
  const handler = typeof req.handler === "string" ? req.handler : "";

  // Per-request environment: applied EXACTLY (see applyEnv). Keys this
  // invocation no longer carries are restored to their pre-Relay baseline (or
  // removed), so a rotated/removed value never leaks forward; every unrelated
  // OS/system/container variable is preserved.
  applyEnv(req.env);
  process.env.RELAY_HANDLER = handler;

  let error = null;
  try {
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

    // Extract and install the remote context only for this handler promise.
    // context.with restores the warm process's previous context on both
    // resolution and rejection, so one invocation cannot leak into the next.
    const parent = propagation.extract(context.active(), req.trace ?? {});
    await context.with(parent, () => fn(req.event ?? null));
  } catch (e) {
    // Module resolution, import errors, and handler errors are all
    // invocation failures (ok:false): the container stays healthy.
    const stack =
      e && e.stack ? e.stack : e && e.message ? e.message : String(e);
    error = "handler " + JSON.stringify(handler) + " failed: " + stack;
    // Mirror the operator-visible stderr output of the one-shot bootstrap.
    console.error(
      "handler " +
        JSON.stringify(handler) +
        " failed: " +
        (e && e.stack ? e.stack : e),
    );
  }

  respond(id, error === null, error ?? undefined);
}

const rl = readline.createInterface({ input: process.stdin });

// Process request lines strictly sequentially: the handler chain serializes
// on itself, so a pipelined burst still runs one invocation at a time.
let chain = Promise.resolve();
rl.on("line", (line) => {
  if (line === "") return; // ignore blank lines rather than dying on them
  chain = chain
    .then(() => handle(line))
    .catch((e) => {
      // A error escaping the handler's own try/catch (e.g. the env loop) is a
      // fatal protocol failure: crash the process so Relay discards.
      console.error(
        "bootstrap internal error: " + (e && e.stack ? e.stack : e),
      );
      process.exit(1);
    });
});
rl.on("close", () => {
  // Relay closed stdin (container discard or shutdown): exit cleanly once the
  // in-flight chain completes.
  chain.then(
    () => process.exit(0),
    () => process.exit(1),
  );
});

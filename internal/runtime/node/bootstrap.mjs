// Relay's Node.js function bootstrap.
//
// One execution container per function STAYS ALIVE between invocations and
// serves Relay's authenticated, length-prefixed JSON protocol over TCP on
// 0.0.0.0:7000. Application stdout/stderr remain native Docker logs:
//
//   Relay -> TCP: framed hello, then framed invoke requests
//   TCP -> Relay: framed auth, then framed invocation results
//
// Process-model notes (intended semantics, not a bug):
//   - Handler errors (including async rejections caught at the await) fail
//     THAT invocation (ok:false) and the loop continues: the container is
//     kept healthy.
//   - Malformed authenticated protocol input is fatal so Relay discards the
//     container. No uncaughtException/unhandledRejection handlers are installed.
//   - Once the authenticated connection ends, the bootstrap exits cleanly.
//   - A dynamic import() promise is CACHED per resolved module path, so
//     module-level state persists across invocations; per-request env is
//     applied EXACTLY per invocation: a key absent from the new frame is
//     restored to its pre-Relay baseline (or removed) and unrelated
//     OS/system/container variables are preserved (see applyEnv).
//     RELAY_HANDLER is reserved and overwritten every invocation.

import { statSync, mkdirSync, symlinkSync } from "node:fs";
import { createRequire } from "node:module";
import { spawnSync } from "node:child_process";
import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import { createServer } from "node:net";

const executionSecret = process.env.RELAY_EXECUTION_SECRET;
const executionPortValue = process.env.RELAY_EXECUTION_PORT;
delete process.env.RELAY_EXECUTION_SECRET;

if (!/^[0-9a-f]{64}$/.test(executionSecret ?? "")) {
  throw new Error("invalid Relay execution configuration");
}
if (!/^\d+$/.test(executionPortValue ?? "")) {
  throw new Error("invalid Relay execution configuration");
}
const executionPort = Number(executionPortValue);
if (!Number.isInteger(executionPort) || executionPort < 1 || executionPort > 65535) {
  throw new Error("invalid Relay execution configuration");
}

const MAX_HANDSHAKE_BYTES = 4 * 1024;
const MAX_INVOKE_BYTES = 8 * 1024 * 1024;
const MAX_RESPONSE_BYTES = 1024 * 1024;
const MAX_ERROR_BYTES = 64 * 1024;
const HANDSHAKE_TIMEOUT_MS = 2000;
const MAX_PENDING_HANDSHAKES = 8;
const utf8Decoder = new TextDecoder("utf-8", { fatal: true });

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

// Resolve from BAKED_ROOT so the bootstrap and user modules share one API
// singleton. The managed API always lives in BAKED_ROOT/node_modules, whether the
// source is baked or mounted.
const { context, propagation } = createRequire(BAKED_ROOT + "/package.json")(
  "@opentelemetry/api",
);

class HandlerContext {
  constructor({
    requestId,
    attempt,
    maxAttempts,
    deadline,
    messageId,
    traceId,
    app,
    handler,
  } = {}) {
    this.requestId = requestId;
    this.attempt = attempt;
    this.maxAttempts = maxAttempts;
    this.deadline = deadline;
    this.messageId = messageId;
    this.traceId = traceId;
    this.app = app;
    this.handler = handler;
  }

  getRemainingTimeMs() {
    const deadlineMs = Date.parse(this.deadline);
    return Number.isFinite(deadlineMs)
      ? Math.max(0, deadlineMs - Date.now())
      : 0;
  }
}

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
      if (k === "RELAY_EXECUTION_SECRET") continue;
      incoming.set(String(k), String(v));
    }
  }
  delete process.env.RELAY_EXECUTION_SECRET;
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

// A mounted container runs Node 24 or 26, whose synchronous registerHooks API the
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

function boundedError(id, message) {
  const emptyResult = JSON.stringify({ type: "result", id, ok: false, error: "" });
  const budget = Math.min(
    MAX_ERROR_BYTES,
    MAX_RESPONSE_BYTES - Buffer.byteLength(emptyResult, "utf8"),
  );
  if (budget <= 0) return "";

  let result = "";
  let used = 0;
  for (const cp of message) {
    const quoted = JSON.stringify(cp);
    const bytes = Buffer.byteLength(quoted.slice(1, -1), "utf8");
    if (used + bytes > budget) break;
    result += cp;
    used += bytes;
  }
  return result;
}

function makeResult(id, ok, error) {
  if (ok) return { type: "result", id, ok: true };
  return {
    type: "result",
    id,
    ok: false,
    error: boundedError(id, String(error ?? "")),
  };
}

async function handle(req) {
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
    if (
      req.context === null ||
      typeof req.context !== "object" ||
      Array.isArray(req.context)
    ) {
      throw new Error("invalid request context: expected an object");
    }

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
    const handlerContext = new HandlerContext(req.context);
    await context.with(parent, () => fn(req.event ?? null, handlerContext));
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

  return makeResult(id, error === null, error ?? undefined);
}

class ProtocolError extends Error {}

class ChunkReader {
  constructor(socket) {
    this.iterator = socket[Symbol.asyncIterator]();
    this.chunk = null;
    this.offset = 0;
  }

  async readExact(size, allowCleanEnd = false) {
    const result = Buffer.allocUnsafe(size);
    let received = 0;
    while (received < size) {
      if (this.chunk === null || this.offset === this.chunk.length) {
        const next = await this.iterator.next();
        if (next.done) {
          if (received === 0 && allowCleanEnd) return null;
          throw new ProtocolError("incomplete frame");
        }
        this.chunk = next.value;
        this.offset = 0;
        continue;
      }
      const length = Math.min(size - received, this.chunk.length - this.offset);
      this.chunk.copy(result, received, this.offset, this.offset + length);
      this.offset += length;
      received += length;
    }
    return result;
  }
}

async function readFrame(reader, limit, allowCleanEnd = false) {
  const header = await reader.readExact(4, allowCleanEnd);
  if (header === null) return null;
  const size = header.readUInt32BE(0);
  if (size === 0 || size > limit) throw new ProtocolError("invalid frame size");
  const payload = await reader.readExact(size);
  let text;
  try {
    text = utf8Decoder.decode(payload);
    return JSON.parse(text);
  } catch {
    throw new ProtocolError("invalid JSON frame");
  }
}

async function writeFrame(socket, value, limit) {
  const payload = Buffer.from(JSON.stringify(value), "utf8");
  if (payload.length === 0 || payload.length > limit) {
    throw new ProtocolError("response frame too large");
  }
  const frame = Buffer.allocUnsafe(payload.length + 4);
  frame.writeUInt32BE(payload.length, 0);
  payload.copy(frame, 4);
  await new Promise((resolve, reject) => {
    socket.write(frame, (error) => (error ? reject(error) : resolve()));
  });
}

function isObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function parseInvoke(value) {
  if (
    !isObject(value) ||
    value.type !== "invoke" ||
    typeof value.id !== "string" ||
    value.id.length === 0 ||
    typeof value.handler !== "string" ||
    (value.env !== null && !isObject(value.env)) ||
    !Object.hasOwn(value, "event") ||
    !isObject(value.context)
  ) {
    throw new ProtocolError("invalid invocation request");
  }
  const contextFields = [
    "requestId",
    "attempt",
    "maxAttempts",
    "deadline",
    "app",
    "handler",
  ];
  if (
    contextFields.some((field) => !(field in value.context)) ||
    value.context.handler !== value.handler ||
    typeof value.context.requestId !== "string" ||
    !Number.isInteger(value.context.attempt) ||
    !Number.isInteger(value.context.maxAttempts) ||
    typeof value.context.deadline !== "string" ||
    typeof value.context.app !== "string" ||
    (Object.hasOwn(value.context, "messageId") &&
      typeof value.context.messageId !== "string") ||
    (Object.hasOwn(value.context, "traceId") &&
      typeof value.context.traceId !== "string")
  ) {
    throw new ProtocolError("invalid invocation context");
  }
  if (value.trace !== undefined && !isObject(value.trace)) {
    throw new ProtocolError("invalid trace carrier");
  }
  if (
    Buffer.byteLength(
      JSON.stringify({ type: "result", id: value.id, ok: false, error: "" }),
      "utf8",
    ) > MAX_RESPONSE_BYTES
  ) {
    throw new ProtocolError("invocation id exceeds response limit");
  }
  return value;
}

function macFor(prefix, nonce) {
  return createHmac("sha256", Buffer.from(executionSecret, "ascii"))
    .update(prefix + nonce, "ascii")
    .digest();
}

function validMac(candidate, expected) {
  if (typeof candidate !== "string" || !/^[0-9a-f]{64}$/.test(candidate)) {
    return false;
  }
  return timingSafeEqual(Buffer.from(candidate, "hex"), expected);
}

let server;
let activeSocket = null;
let shuttingDown = false;
let pendingHandshakes = 0;
const sockets = new Set();

function stop(exitCode) {
  if (shuttingDown) return;
  shuttingDown = true;
  process.exitCode = exitCode;
  if (server.listening) server.close();
  for (const socket of sockets) socket.destroy();
}

async function serve(socket, onAuthenticated) {
  socket.setNoDelay(true);
  const reader = new ChunkReader(socket);
  const handshakeTimer = setTimeout(
    () => socket.destroy(),
    HANDSHAKE_TIMEOUT_MS,
  );
  try {
    const hello = await readFrame(reader, MAX_HANDSHAKE_BYTES);
    if (
      !isObject(hello) ||
      hello.type !== "hello" ||
      hello.version !== 1 ||
      typeof hello.nonce !== "string" ||
      !/^[0-9a-f]{64}$/.test(hello.nonce)
    ) {
      throw new ProtocolError("invalid handshake");
    }

    const nonce = randomBytes(32).toString("hex");
    await writeFrame(
      socket,
      {
        type: "auth",
        version: 1,
        mac: macFor("relay-bootstrap-auth-v1:", hello.nonce).toString("hex"),
        nonce,
      },
      MAX_HANDSHAKE_BYTES,
    );

    const ready = await readFrame(reader, MAX_HANDSHAKE_BYTES);
    if (
      !isObject(ready) ||
      ready.type !== "ready" ||
      ready.version !== 1 ||
      !validMac(ready.mac, macFor("relay-client-auth-v1:", nonce))
    ) {
      throw new ProtocolError("invalid authentication");
    }

    if (activeSocket !== null) throw new ProtocolError("connection already active");
    activeSocket = socket;
    clearTimeout(handshakeTimer);
    onAuthenticated();
    for (const pendingSocket of sockets) {
      if (pendingSocket !== socket) pendingSocket.destroy();
    }
    socket.once("close", () => {
      if (activeSocket === socket) stop(0);
    });

    while (!socket.destroyed) {
      const raw = await readFrame(reader, MAX_INVOKE_BYTES, true);
      if (raw === null) return;
      const req = parseInvoke(raw);
      const result = await handle(req);
      await writeFrame(socket, result, MAX_RESPONSE_BYTES);
    }
  } finally {
    clearTimeout(handshakeTimer);
  }
}

server = createServer((socket) => {
  if (shuttingDown || activeSocket !== null || pendingHandshakes >= MAX_PENDING_HANDSHAKES) {
    socket.destroy();
    return;
  }
  pendingHandshakes++;
  sockets.add(socket);
  socket.on("close", () => sockets.delete(socket));
  let authenticated = false;
  serve(socket, () => {
    authenticated = true;
    pendingHandshakes--;
  })
    .catch(() => {
      if (activeSocket === socket) stop(1);
      else socket.destroy();
    })
    .finally(() => {
      if (!authenticated) pendingHandshakes--;
      if (activeSocket === socket) {
        activeSocket = null;
        stop(0);
      }
    });
});

server.on("error", () => {
  stop(1);
});

await new Promise((resolve, reject) => {
  server.once("error", reject);
  server.listen(executionPort, "0.0.0.0", resolve);
});

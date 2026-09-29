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

import { statSync } from "node:fs";
import * as readline from "node:readline";
import { createRequire } from "node:module";

// Resolve from /app so the bootstrap and user modules share one API singleton.
const { context, propagation } = createRequire("/app/package.json")(
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

// resolveModulePath resolves a module part to a file under /app without
// importing anything: "index" -> ./index.js/.mjs or ./index/index.js/.mjs;
// "src.email" -> ./src/email.js/.mjs. Candidates are checked via statSync, not
// import attempts, so exactly one module is imported and a broken user module
// surfaces its own real error instead of being mistaken for "module not
// found". The resolved path is cached per module part.
function resolveModulePath(modulePart) {
  const cacheKey = "path:" + modulePart;
  if (moduleCache.has(cacheKey)) return moduleCache.get(cacheKey);
  const parts = modulePart.split(".");
  const base = "/app/" + parts.join("/");
  const candidates = [];
  for (const ext of [".mjs", ".js"]) {
    candidates.push(base + ext);
    candidates.push(base + "/index" + ext);
  }
  let modPath = null;
  for (const cand of candidates) {
    try {
      if (statSync(cand).isFile()) {
        modPath = cand;
        break;
      }
    } catch (e) {
      // Not a file (or not present); try the next candidate.
    }
  }
  if (modPath !== null) moduleCache.set(cacheKey, modPath);
  return modPath;
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

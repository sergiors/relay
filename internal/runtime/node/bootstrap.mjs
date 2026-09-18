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
//     module-level state persists across invocations; per-request env values
//     applied to process.env likewise persist across invocations.

import { statSync } from "node:fs";
import * as readline from "node:readline";

// Must match the Relay-side constant "@@RELAY@@" (internal/runtime/protocol.go).
const SENTINEL = "@@RELAY@@";

// Response error strings are operator log text only; bound them. The cap keeps
// the whole response frame well under Relay's 4 KiB stdout line cap, so a frame
// is never split mid-line by the demuxer.
const MAX_ERROR_BYTES = 3 * 1024;

// moduleCache caches, per resolved file path, the dynamic import() promise so
// a module is imported exactly once and its state persists.
const moduleCache = new Map();

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
    frame["error"] = String(error ?? "").slice(0, MAX_ERROR_BYTES);
  }
  process.stdout.write(SENTINEL + JSON.stringify(frame) + "\n");
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

  // Per-request environment: set/overwrite; values persist for later
  // invocations unless overwritten (documented process-global behavior).
  if (req.env && typeof req.env === "object") {
    for (const [k, v] of Object.entries(req.env)) {
      process.env[k] = String(v);
    }
  }
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

    await fn(req.event ?? null);
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

import { readFileSync, statSync } from "node:fs";

const handler = process.env.RELAY_HANDLER || "";
if (!handler) {
  console.error("RELAY_HANDLER is not set");
  process.exit(1);
}

const idx = handler.lastIndexOf(".");
if (idx <= 0 || idx === handler.length - 1) {
  console.error("invalid RELAY_HANDLER " + JSON.stringify(handler));
  process.exit(1);
}
const modulePart = handler.slice(0, idx);
const funcName = handler.slice(idx + 1);

// Resolve the module part to a file under /app without importing anything:
// "index" -> ./index.js/.mjs or ./index/index.js/.mjs; "src.email" ->
// ./src/email.js/.mjs. Candidates are checked via statSync, not import attempts,
// so exactly one module is imported and a broken user module surfaces its own
// real error instead of being mistaken for "module not found".
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
if (modPath === null) {
  console.error("module not found: " + JSON.stringify(modulePart));
  process.exit(1);
}

// Import exactly once. Any error here (a broken dependency, a syntax error,
// or an "import missing-package" inside the module) is the real error and
// propagates as-is.
let mod;
try {
  mod = await import(modPath);
} catch (e) {
  console.error("failed to import module " + JSON.stringify(modulePart) + ": " + (e && e.stack ? e.stack : e));
  process.exit(1);
}

let fn = mod[funcName];
if (fn === undefined && mod.default && typeof mod.default === "object") {
  fn = mod.default[funcName];
}
if (typeof fn !== "function") {
  console.error("module " + JSON.stringify(modulePart) + " has no function " + JSON.stringify(funcName));
  process.exit(1);
}

let event;
try {
  event = JSON.parse(readFileSync(0, "utf8"));
} catch (e) {
  console.error("failed to read event from stdin: " + e.message);
  process.exit(1);
}

try {
  const result = fn(event);
  if (result && typeof result.then === "function") {
    await result;
  }
} catch (e) {
  console.error("handler " + JSON.stringify(handler) + " failed: " + (e && e.stack ? e.stack : e));
  process.exit(1);
}

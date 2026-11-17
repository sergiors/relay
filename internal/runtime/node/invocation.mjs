import { createRequire } from "node:module";

const BAKED_ROOT = "/app";
const MAX_RESPONSE_BYTES = 1024 * 1024;
const MAX_ERROR_BYTES = 64 * 1024;
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

async function invoke(req, loadHandler) {
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

    const fn = await loadHandler(handler);

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


export function createInvoker(loadHandler) {
  return (req) => invoke(req, loadHandler);
}

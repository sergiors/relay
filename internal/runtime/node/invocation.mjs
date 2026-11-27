import { types } from "node:util";

import * as telemetry from "./telemetry.mjs";
import * as loggingContext from "./logging-context.mjs";

const MAX_RESPONSE_BYTES = 1024 * 1024;
const MAX_ERROR_BYTES = 64 * 1024;
const MAX_ERROR_TEXT_LENGTH = 64 * 1024;
const NATIVE_ERROR_STACK_GETTER_SOURCE = "function () { [native code] }";
// Node 24 and 26 use this exact default formatter implementation.
const NODE_DEFAULT_PREPARE_STACK_TRACE =
  "function ErrorPrepareStackTrace(error, trace) {\n" +
  "  return internalPrepareStackTrace(error, trace);\n" +
  "}";
const reflectApply = Reflect.apply;
const functionToString = Function.prototype.toString;
const nativeErrorStack = captureNativeErrorStack();

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
  const emptyResult = JSON.stringify({
    type: "result",
    id,
    ok: false,
    error: "",
  });
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

    if (used + bytes > budget) {
      break;
    }

    result += cp;
    used += bytes;
  }

  return result;
}

function makeResult(id, ok, error) {
  if (ok) {
    return { type: "result", id, ok: true };
  }

  return {
    type: "result",
    id,
    ok: false,
    error: boundedError(id, typeof error === "string" ? error : ""),
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
  telemetry.ensureStarted();

  return loggingContext.runInvocation(req.context, async () => {
    let error = null;
    try {
      if (
        req.context === null ||
        typeof req.context !== "object" ||
        Array.isArray(req.context)
      ) {
        throw new Error("invalid request context: expected an object");
      }

      const handlerContext = new HandlerContext(req.context);
      await telemetry.withParent(req.trace ?? {}, async () => {
        const fn = await loadHandler(handler);
        return fn(req.event ?? null, handlerContext);
      });
    } catch (e) {
      // Module resolution, import errors, and handler errors are all
      // invocation failures (ok:false): the container stays healthy.
      const detail = thrownErrorText(e);
      error = "handler " + JSON.stringify(handler) + " failed: " + detail;
      // Mirror the operator-visible stderr output of the one-shot bootstrap.
      try {
        console.error("handler " + JSON.stringify(handler) + " failed: " + detail);
      } catch {}
    }

    return makeResult(id, error === null, error ?? undefined);
  });
}

function thrownErrorText(value) {
  try {
    const valueType = typeof value;
    if (
      value === null ||
      valueType === "undefined" ||
      valueType === "string" ||
      valueType === "number" ||
      valueType === "boolean" ||
      valueType === "bigint" ||
      valueType === "symbol"
    ) {
      return boundedErrorText(String(value));
    }
    if (!types.isNativeError(value)) return "non-Error value thrown";

    const stack = nativeErrorStackText(value);
    if (stack !== undefined) return stack;

    const message = ownDataProperty(value, "message");
    if (typeof message !== "string" || message.length === 0) return "Error";
    return boundedErrorText(`${errorName(value)}: ${message}`);
  } catch {
    return "Error details unavailable";
  }
}

function nativeErrorStackText(error) {
  const descriptor = ownDescriptor(error, "stack");
  if (descriptor === undefined) return undefined;
  if (Object.hasOwn(descriptor, "value")) {
    return typeof descriptor.value === "string"
      ? boundedErrorText(descriptor.value)
      : undefined;
  }
  if (
    nativeErrorStack === null ||
    descriptor.get !== nativeErrorStack.getter ||
    nativeErrorConstructor(error) !== nativeErrorStack.errorConstructor ||
    !hasNodeDefaultPrepareStackTrace()
  ) {
    return undefined;
  }

  try {
    const stack = reflectApply(nativeErrorStack.getter, error, []);
    return typeof stack === "string" ? boundedErrorText(stack) : undefined;
  } catch {
    return undefined;
  }
}

function captureNativeErrorStack() {
  try {
    const probe = new Error();
    if (!types.isNativeError(probe)) return null;
    const descriptor = ownDescriptor(probe, "stack");
    if (
      typeof descriptor?.get !== "function" ||
      types.isProxy(descriptor.get) ||
      ownDataProperty(descriptor.get, "name") !== "" ||
      ownDataProperty(descriptor.get, "length") !== 0 ||
      reflectApply(functionToString, descriptor.get, []) !==
        NATIVE_ERROR_STACK_GETTER_SOURCE
    ) {
      return null;
    }

    const errorConstructor = nativeErrorConstructor(probe);
    const prepareStackTrace =
      errorConstructor === null
        ? undefined
        : ownDataProperty(errorConstructor, "prepareStackTrace");
    if (
      errorConstructor === null ||
      typeof prepareStackTrace !== "function" ||
      !isNodeDefaultPrepareStackTrace(prepareStackTrace)
    ) {
      return null;
    }
    return { getter: descriptor.get, errorConstructor, prepareStackTrace };
  } catch {
    return null;
  }
}

function nativeErrorConstructor(error) {
  let prototype;
  try {
    prototype = Object.getPrototypeOf(error);
  } catch {
    return null;
  }

  let found = null;
  while (prototype !== null) {
    if (types.isProxy(prototype)) return null;
    const constructor = ownDataProperty(prototype, "constructor");
    if (
      typeof constructor === "function" &&
      ownDataProperty(constructor, "prototype") === prototype &&
      ownDataProperty(constructor, "name") === "Error"
    ) {
      found = constructor;
    }
    try {
      prototype = Object.getPrototypeOf(prototype);
    } catch {
      return null;
    }
  }
  return found;
}

function hasNodeDefaultPrepareStackTrace() {
  if (nativeErrorStack === null) return false;
  const descriptor = ownDescriptor(
    nativeErrorStack.errorConstructor,
    "prepareStackTrace",
  );
  return (
    descriptor !== undefined &&
    Object.hasOwn(descriptor, "value") &&
    descriptor.value === nativeErrorStack.prepareStackTrace &&
    isNodeDefaultPrepareStackTrace(descriptor.value)
  );
}

function isNodeDefaultPrepareStackTrace(value) {
  if (typeof value !== "function" || types.isProxy(value)) return false;
  try {
    return (
      ownDataProperty(value, "name") === "ErrorPrepareStackTrace" &&
      ownDataProperty(value, "length") === 2 &&
      reflectApply(functionToString, value, []) ===
        NODE_DEFAULT_PREPARE_STACK_TRACE
    );
  } catch {
    return false;
  }
}

function errorName(value) {
  try {
    let current = value;
    while (current !== null) {
      if (types.isProxy(current)) return "Error";
      const name = ownDataProperty(current, "name");
      if (typeof name === "string" && name.length > 0) return name;
      current = Object.getPrototypeOf(current);
    }
  } catch {}
  return "Error";
}

function ownDescriptor(value, key) {
  try {
    return Object.getOwnPropertyDescriptor(value, key);
  } catch {
    return undefined;
  }
}

function ownDataProperty(value, key) {
  const descriptor = ownDescriptor(value, key);
  return descriptor !== undefined && Object.hasOwn(descriptor, "value")
    ? descriptor.value
    : undefined;
}

function boundedErrorText(text) {
  if (text.length <= MAX_ERROR_TEXT_LENGTH) return text;
  const marker = "\n[error text truncated]";
  return text.slice(0, MAX_ERROR_TEXT_LENGTH - marker.length) + marker;
}

export function createInvoker(loadHandler) {
  return (req) => invoke(req, loadHandler);
}

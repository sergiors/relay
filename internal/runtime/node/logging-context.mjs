import { AsyncLocalStorage } from "node:async_hooks";

const OWNER_KEY = Symbol.for("relay.runtime.logging.context");
const EMPTY_FIELDS = Object.freeze({});
const EMPTY_MAP = new Map();
let invocationCount = 0;

function invocationFromFrame(frame) {
  if (frame === null || typeof frame !== "object" || Array.isArray(frame)) {
    return null;
  }
  if (
    typeof frame.app !== "string" ||
    typeof frame.handler !== "string" ||
    typeof frame.requestId !== "string" ||
    typeof frame.attempt !== "number" ||
    !Number.isInteger(frame.attempt)
  ) {
    return null;
  }

  const traceId =
    typeof frame.traceId === "string" &&
    /^[0-9a-f]{32}$/.test(frame.traceId) &&
    !/^0{32}$/.test(frame.traceId)
      ? frame.traceId
      : undefined;
  return {
    app: frame.app,
    handler: frame.handler,
    request_id: frame.requestId,
    attempt: frame.attempt,
    ...(traceId === undefined ? {} : { trace_id: traceId }),
  };
}

function createOwner() {
  const storage = new AsyncLocalStorage();
  const empty = {
    invocation: null,
    lease: null,
    temporary: EMPTY_MAP,
  };

  return {
    async runInvocation(frame, callback) {
      const invocation = invocationFromFrame(frame);
      if (invocation === null) return callback();

      const lease = { active: true };
      const state = {
        invocation: {
          ...invocation,
          cold_start: ++invocationCount === 1,
        },
        lease,
        temporary: EMPTY_MAP,
      };

      return storage.run(state, async () => {
        try {
          return await callback();
        } finally {
          lease.active = false;
        }
      });
    },

    snapshot(scope) {
      const state = storage.getStore() ?? empty;
      if (state.lease !== null && !state.lease.active) {
        return {
          invocation: null,
          temporary: EMPTY_FIELDS,
          expired: true,
        };
      }

      return {
        invocation: state.invocation,
        temporary: scope === undefined
          ? EMPTY_FIELDS
          : state.temporary.get(scope) ?? EMPTY_FIELDS,
        expired: false,
      };
    },

    withFields(scope, fields, callback) {
      const state = storage.getStore() ?? empty;
      const temporary = new Map(state.temporary);
      temporary.set(scope, {
        ...(temporary.get(scope) ?? EMPTY_FIELDS),
        ...snapshotFields(fields),
      });
      return storage.run({ ...state, temporary }, callback);
    },
  };
}

function snapshotFields(fields) {
  const snapshot = Object.create(null);
  let descriptors;
  try {
    descriptors = Object.getOwnPropertyDescriptors(fields);
  } catch {
    return { $unsupported: "object" };
  }
  for (const [key, descriptor] of Object.entries(descriptors)) {
    if (!descriptor.enumerable) continue;
    snapshot[key] = Object.hasOwn(descriptor, "value")
      ? descriptor.value
      : { $unsupported: "accessor" };
  }
  return snapshot;
}

const globalState = globalThis;
let owner = globalState[OWNER_KEY];
if (owner === undefined) {
  owner = createOwner();
  Object.defineProperty(globalState, OWNER_KEY, {
    configurable: false,
    enumerable: false,
    value: owner,
    writable: false,
  });
}

export const runInvocation = (frame, callback) =>
  owner.runInvocation(frame, callback);

import { AsyncLocalStorage } from "node:async_hooks";

export interface InvocationLogContext {
  app: string;
  handler: string;
  request_id: string;
  attempt: number;
  trace_id?: string;
  cold_start: boolean;
}

export interface ContextSnapshot {
  invocation: InvocationLogContext | null;
  temporary: Readonly<Record<string, unknown>>;
  expired: boolean;
}

interface State {
  invocation: InvocationLogContext | null;
  lease: { active: boolean } | null;
  temporary: Map<string, Readonly<Record<string, unknown>>>;
}

export interface LoggingContextOwner {
  runInvocation<T>(frame: unknown, callback: () => T | Promise<T>): Promise<T>;
  snapshot(scope?: string): ContextSnapshot;
  withFields<T>(
    scope: string,
    fields: Readonly<Record<string, unknown>>,
    callback: () => T,
  ): T;
}

const OWNER_KEY = Symbol.for("relay.runtime.logging.context");
const EMPTY_FIELDS = Object.freeze({});
const EMPTY_MAP = new Map<string, Readonly<Record<string, unknown>>>();
let invocationCount = 0;

function invocationFromFrame(frame: unknown): Omit<
  InvocationLogContext,
  "cold_start"
> | null {
  if (frame === null || typeof frame !== "object" || Array.isArray(frame)) {
    return null;
  }

  const value = frame as Record<string, unknown>;
  if (
    typeof value.app !== "string" ||
    typeof value.handler !== "string" ||
    typeof value.requestId !== "string" ||
    typeof value.attempt !== "number" ||
    !Number.isInteger(value.attempt)
  ) {
    return null;
  }

  const traceId =
    typeof value.traceId === "string" &&
    /^[0-9a-f]{32}$/.test(value.traceId) &&
    !/^0{32}$/.test(value.traceId)
      ? value.traceId
      : undefined;

  return {
    app: value.app,
    handler: value.handler,
    request_id: value.requestId,
    attempt: value.attempt,
    ...(traceId === undefined ? {} : { trace_id: traceId }),
  };
}

function createOwner(): LoggingContextOwner {
  const storage = new AsyncLocalStorage<State>();
  const empty: State = {
    invocation: null,
    lease: null,
    temporary: EMPTY_MAP,
  };

  return {
    async runInvocation(frame, callback) {
      const invocation = invocationFromFrame(frame);
      if (invocation === null) return callback();

      const lease = { active: true };
      const state: State = {
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

function snapshotFields(
  fields: Readonly<Record<string, unknown>>,
): Record<string, unknown> {
  const snapshot = Object.create(null) as Record<string, unknown>;
  let descriptors: PropertyDescriptorMap;
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

const globalState = globalThis as typeof globalThis & Record<symbol, unknown>;
const existingOwner = globalState[OWNER_KEY] as
  | LoggingContextOwner
  | undefined;
export const loggingContext =
  existingOwner ??
  (() => {
    const owner = createOwner();
    Object.defineProperty(globalState, OWNER_KEY, {
      configurable: false,
      enumerable: false,
      value: owner,
      writable: false,
    });
    return owner;
  })();

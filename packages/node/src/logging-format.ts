export interface LogRecord {
  timestamp: string;
  level: string;
  service: string;
  message: string;
  logger: string;
  location: string;
  app?: string;
  handler?: string;
  request_id?: string;
  attempt?: number;
  cold_start?: boolean;
  trace_id?: string;
  span_id?: string;
  attributes?: Readonly<Record<string, unknown>>;
  exception?: unknown;
  stack_info?: string;
}

const MAX_DEPTH = 16;
const MAX_ITEMS = 10_000;
const MAX_LINE_LENGTH = 1024 * 1024;
const FALLBACK_MESSAGE = "log record could not be serialized";
const MINIMAL_FALLBACK_LINE =
  '{"level":"ERROR","logger":"relay.logging",' +
  '"message":"log record could not be serialized"}';

export class JsonFormatter {
  format(record: LogRecord): string {
    let safeRecord: Partial<LogRecord>;
    try {
      safeRecord = snapshotRecord(record);
    } catch {
      return MINIMAL_FALLBACK_LINE;
    }
    try {
      const payload = toPayload(safeRecord as LogRecord);
      const line = stringify(payload);
      if (line.length > MAX_LINE_LENGTH) {
        return fallbackLine(safeRecord, "RangeError");
      }
      return line;
    } catch (error) {
      return fallbackLine(safeRecord, typeName(error));
    }
  }
}

function toPayload(record: LogRecord): Record<string, unknown> {
  const payload: Record<string, unknown> = {
    timestamp: stringOr(record.timestamp, new Date().toISOString()),
    level: stringOr(record.level, "ERROR"),
    logger: stringOr(record.logger, "relay.logging"),
    message: stringOr(record.message, FALLBACK_MESSAGE),
    location: stringOr(record.location, "unknown.<anonymous>:0"),
    service: stringOr(record.service, stringOr(record.logger, "relay.logging")),
  };

  for (const name of [
    "app",
    "handler",
    "request_id",
    "trace_id",
    "span_id",
  ] as const) {
    if (typeof record[name] === "string") payload[name] = record[name];
  }
  if (Number.isInteger(record.attempt)) payload.attempt = record.attempt;
  if (typeof record.cold_start === "boolean") {
    payload.cold_start = record.cold_start;
  }

  if (record.attributes !== undefined) {
    const attributes = jsonValue(record.attributes);
    if (
      attributes !== null &&
      typeof attributes === "object" &&
      !Array.isArray(attributes)
        ? Object.keys(attributes).length > 0
        : true
    ) {
      payload.attributes = attributes;
    }
  }
  if (Object.hasOwn(record, "exception")) {
    payload.exception = errorRecord(record.exception);
  }
  if (record.stack_info) payload.stack_info = record.stack_info;
  return payload;
}

function jsonValue(
  value: unknown,
  depth = 0,
  active = new Set<object>(),
): unknown {
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "string"
  ) {
    return value;
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : { $unsupported: "float" };
  }

  if (value instanceof Error) return errorRecord(value, depth, active);
  if (value instanceof Date) {
    const timestamp = Date.prototype.getTime.call(value);
    return Number.isFinite(timestamp)
      ? Date.prototype.toISOString.call(value)
      : { $unsupported: "Date" };
  }
  if (typeof value !== "object") {
    return { $unsupported: typeName(value) };
  }
  if (depth >= MAX_DEPTH) return { $truncated: true };
  if (active.has(value)) return { $circular: true };

  if (Array.isArray(value)) {
    active.add(value);
    try {
      const length = ownDataProperty(value, "length");
      if (typeof length !== "number") return { $unsupported: "Array" };
      const result: unknown[] = [];
      for (let index = 0; index < Math.min(length, MAX_ITEMS); index += 1) {
        const descriptor = ownDescriptor(value, String(index));
        if (descriptor === undefined) {
          result.push(null);
        } else if (!Object.hasOwn(descriptor, "value")) {
          result.push({ $unsupported: "accessor" });
        } else {
          result.push(jsonValue(descriptor.value, depth + 1, active));
        }
      }
      if (length > MAX_ITEMS) result.push({ $truncated: true });
      return result;
    } finally {
      active.delete(value);
    }
  }

  let prototype: object | null;
  try {
    prototype = Object.getPrototypeOf(value);
  } catch {
    return { $unsupported: typeName(value) };
  }
  if (prototype !== Object.prototype && prototype !== null) {
    return { $unsupported: typeName(value) };
  }

  active.add(value);
  try {
    const descriptors = Object.getOwnPropertyDescriptors(value);
    const keys = Object.keys(descriptors).filter(
      (key) => descriptors[key].enumerable,
    );
    if (keys.length > MAX_ITEMS) {
      return { $truncated: true };
    }
    const result = Object.create(null) as Record<string, unknown>;
    for (const key of keys) {
      const descriptor = descriptors[key];
      result[key] = Object.hasOwn(descriptor, "value")
        ? jsonValue(descriptor.value, depth + 1, active)
        : { $unsupported: "accessor" };
    }
    return result;
  } finally {
    active.delete(value);
  }
}

function errorRecord(
  value: unknown,
  depth = 0,
  active = new Set<object>(),
): Record<string, unknown> {
  if (depth >= MAX_DEPTH) return { $truncated: true };
  if (value !== null && typeof value === "object" && active.has(value)) {
    return { $circular: true };
  }
  if (!(value instanceof Error)) {
    const type = typeName(value);
    const message = thrownMessage(value);
    return {
      type,
      message,
      stacktrace: `${type}: ${message}`,
    };
  }

  active.add(value);
  try {
    const type = errorType(value);
    const message = errorMessage(value);
    let stacktrace = errorStack(value) ?? `${type}: ${message}`;
    const cause = ownDataProperty(value, "cause");
    if (cause !== undefined) {
      stacktrace += `\nCaused by: ${causeText(cause, depth + 1, active)}`;
    }
    const record: Record<string, unknown> = { type, message, stacktrace };
    const errors = aggregateErrors(value);
    if (Array.isArray(errors)) {
      record.errors = jsonValue(errors, depth + 1, active);
    }
    return record;
  } finally {
    active.delete(value);
  }
}

function causeText(
  cause: unknown,
  depth: number,
  active: Set<object>,
): string {
  if (depth >= MAX_DEPTH) return "[cause truncated]";
  if (cause !== null && typeof cause === "object" && active.has(cause)) {
    return "[circular cause]";
  }
  if (cause instanceof Error) {
    const record = errorRecord(cause, depth, active);
    return typeof record.stacktrace === "string"
      ? record.stacktrace
      : `${String(record.type)}: ${String(record.message)}`;
  }
  return `${typeName(cause)}: ${thrownMessage(cause)}`;
}

function errorType(error: Error): string {
  try {
    let prototype = Object.getPrototypeOf(error);
    while (prototype !== null) {
      const constructor = ownDataProperty(prototype, "constructor");
      if (typeof constructor === "function") {
        const name = ownDataProperty(constructor, "name");
        if (typeof name === "string" && name.length > 0) return name;
      }
      prototype = Object.getPrototypeOf(prototype);
    }
    return "Error";
  } catch {
    return "Error";
  }
}

function errorMessage(error: Error): string {
  const message = ownDataProperty(error, "message");
  return typeof message === "string" ? message : "";
}

function errorStack(error: Error): string | undefined {
  const stack = ownDataProperty(error, "stack");
  return typeof stack === "string" ? stack : undefined;
}

function thrownMessage(value: unknown): string {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    typeof value === "bigint" ||
    typeof value === "symbol" ||
    value === undefined
  ) {
    return String(value);
  }
  try {
    const encoded = JSON.stringify(jsonValue(value));
    if (encoded !== undefined) return encoded;
  } catch {
    return `[${typeName(value)}]`;
  }
  return `[${typeName(value)}]`;
}

function aggregateErrors(error: Error): unknown {
  try {
    let prototype = Object.getPrototypeOf(error);
    while (prototype !== null) {
      const constructor = ownDataProperty(prototype, "constructor");
      const name =
        typeof constructor === "function"
          ? ownDataProperty(constructor, "name")
          : undefined;
      if (name === "AggregateError") {
        return ownDataProperty(error, "errors");
      }
      prototype = Object.getPrototypeOf(prototype);
    }
  } catch {
    return undefined;
  }
  return undefined;
}

function stringify(payload: Record<string, unknown>): string {
  return JSON.stringify(payload).replace(/[^\x00-\x7f]/g, (character) =>
    `\\u${character.charCodeAt(0).toString(16).padStart(4, "0")}`,
  );
}

function snapshotRecord(record: LogRecord): Partial<LogRecord> {
  const descriptors = Object.getOwnPropertyDescriptors(record);
  const snapshot = Object.create(null) as Record<string, unknown>;
  for (const [key, descriptor] of Object.entries(descriptors)) {
    if (!descriptor.enumerable) continue;
    snapshot[key] = Object.hasOwn(descriptor, "value")
      ? descriptor.value
      : { $unsupported: "accessor" };
  }
  return snapshot;
}

function ownDescriptor(
  value: object,
  key: PropertyKey,
): PropertyDescriptor | undefined {
  try {
    return Object.getOwnPropertyDescriptor(value, key);
  } catch {
    return undefined;
  }
}

function ownDataProperty<T>(value: object, key: PropertyKey): T | undefined {
  const descriptor = ownDescriptor(value, key);
  return descriptor !== undefined && Object.hasOwn(descriptor, "value")
    ? (descriptor.value as T)
    : undefined;
}

function fallbackLine(record: Partial<LogRecord>, error: string): string {
  try {
    const logger = stringOr(record.logger, "relay.logging");
    const line = stringify({
      timestamp: stringOr(record.timestamp, new Date().toISOString()),
      level: stringOr(record.level, "ERROR"),
      logger,
      message: FALLBACK_MESSAGE,
      service: stringOr(record.service, logger),
      ...(typeof record.location === "string"
        ? { location: record.location }
        : {}),
      error,
    });
    return line.length <= MAX_LINE_LENGTH ? line : MINIMAL_FALLBACK_LINE;
  } catch {
    return MINIMAL_FALLBACK_LINE;
  }
}

function stringOr(value: unknown, fallback: string): string {
  return typeof value === "string" ? value : fallback;
}

function typeName(value: unknown): string {
  if (value instanceof Error) return errorType(value);
  if (typeof value === "bigint") return "BigInt";
  if (typeof value === "symbol") return "Symbol";
  if (typeof value === "function") return "Function";
  if (value === null) return "NoneType";
  if (typeof value !== "object") return typeof value;
  try {
    const constructor = ownDataProperty(
      Object.getPrototypeOf(value) ?? {},
      "constructor",
    );
    const name =
      typeof constructor === "function"
        ? ownDataProperty(constructor, "name")
        : undefined;
    return typeof name === "string" ? name : "Object";
  } catch {
    return "Object";
  }
}

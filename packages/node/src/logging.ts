import { createRequire } from "node:module";
import { basename, extname } from "node:path";
import { format } from "node:util";
import { fileURLToPath } from "node:url";

import {
  loggingContext,
  type InvocationLogContext,
} from "./logging-context.js";
import { JsonFormatter, type LogRecord } from "./logging-format.js";

export { JsonFormatter };
export type { LogRecord };

export type LogLevelName =
  | "DEBUG"
  | "INFO"
  | "WARN"
  | "WARNING"
  | "ERROR"
  | "CRITICAL";
export type LogLevel = LogLevelName | number;
export type LogMessage = unknown;
export type LogAttributes = Readonly<Record<string, unknown>>;
export type LogAttributesFactory = () => LogAttributes;
type LogAttributesArgument = LogAttributes | LogAttributesFactory;

export interface LogOptions {
  attributes?: LogAttributes | LogAttributesFactory;
  extra?: LogAttributes | LogAttributesFactory;
  stackInfo?: boolean;
}

export interface LoggerOptions {
  service?: string;
  level?: LogLevel;
}

interface WriteOptions extends LogOptions {
  exception?: unknown;
}

const LEVELS: Readonly<Record<LogLevelName, number>> = {
  DEBUG: 10,
  INFO: 20,
  WARNING: 30,
  ERROR: 40,
  CRITICAL: 50,
  WARN: 30,
};
const loggerModulePath = fileURLToPath(import.meta.url);
const require = createRequire(import.meta.url);
const MINIMAL_LOG_LINE =
  '{"level":"ERROR","logger":"relay.logging",' +
  '"message":"log record could not be serialized"}';
let traceAPI:
  | {
      context?: { active?: () => unknown };
      trace?: {
        getSpan?: (context: unknown) => unknown;
        isSpanContextValid?: (context: unknown) => boolean;
      };
    }
  | null
  | undefined;

interface ActiveSpanContext {
  traceId: string;
  spanId: string;
}

interface ActiveSpanLookup {
  apiAvailable: boolean;
  span: ActiveSpanContext | null;
}

export class Logger {
  readonly name: string;
  readonly service: string;
  private threshold: number;
  private persistentFields: Record<string, unknown> = {};
  private readonly formatter = new JsonFormatter();

  constructor(service?: string, level?: LogLevel);
  constructor(options?: LoggerOptions);
  constructor(
    serviceOrOptions:
      | string
      | LoggerOptions
      | undefined = undefined,
    requestedLevel?: LogLevel,
  ) {
    const options =
      typeof serviceOrOptions === "object" && serviceOrOptions !== null
        ? serviceOrOptions
        : undefined;
    const service =
      options === undefined
        ? serviceOrOptions as string | undefined
        : dataProperty<string>(options, "service");
    const level =
      options === undefined
        ? requestedLevel
        : dataProperty<LogLevel>(options, "level");
    this.service = resolveService(service);
    this.name = `relay_runtime.${this.service}`;
    this.threshold =
      level === undefined ? configuredLevel() : resolveLevel(level);
  }

  debug(message: LogMessage, attributes: LogAttributesArgument): void;
  debug(message: LogMessage, ...formatArgs: unknown[]): void;
  debug(message: LogMessage, ...args: unknown[]): void {
    this.write("DEBUG", message, args);
  }

  info(message: LogMessage, attributes: LogAttributesArgument): void;
  info(message: LogMessage, ...formatArgs: unknown[]): void;
  info(message: LogMessage, ...args: unknown[]): void {
    this.write("INFO", message, args);
  }

  warning(message: LogMessage, attributes: LogAttributesArgument): void;
  warning(message: LogMessage, ...formatArgs: unknown[]): void;
  warning(message: LogMessage, ...args: unknown[]): void {
    this.write("WARNING", message, args);
  }

  warn(message: LogMessage, attributes: LogAttributesArgument): void;
  warn(message: LogMessage, ...formatArgs: unknown[]): void;
  warn(message: LogMessage, ...args: unknown[]): void {
    this.warning(message, ...args);
  }

  error(message: LogMessage, attributes: LogAttributesArgument): void;
  error(message: LogMessage, ...formatArgs: unknown[]): void;
  error(message: LogMessage, ...args: unknown[]): void {
    this.write("ERROR", message, args);
  }

  exception(
    message: LogMessage,
    thrown?: unknown,
    options?: LogOptions,
  ): void {
    if (arguments.length === 1 && message instanceof Error) {
      this.write(
        "ERROR",
        dataProperty<string>(message, "message") ?? "Error",
        [],
        { exception: message },
      );
      return;
    }
    this.write(
      "ERROR",
      message,
      [],
      arguments.length >= 2
        ? { ...snapshotFields(options, false), exception: thrown }
        : options,
    );
  }

  critical(message: LogMessage, attributes: LogAttributesArgument): void;
  critical(message: LogMessage, ...formatArgs: unknown[]): void;
  critical(message: LogMessage, ...args: unknown[]): void {
    this.write("CRITICAL", message, args);
  }

  log(
    level: LogLevel,
    message: LogMessage,
    attributes: LogAttributesArgument,
  ): void;
  log(level: LogLevel, message: LogMessage, ...formatArgs: unknown[]): void;
  log(level: LogLevel, message: LogMessage, ...args: unknown[]): void {
    const resolved = resolveLevel(level);
    this.write(levelLabel(level, resolved), message, args, undefined, resolved);
  }

  setLevel(level: LogLevel): void {
    this.threshold = resolveLevel(level);
  }

  isEnabledFor(level: LogLevel): boolean {
    return this.threshold <= resolveLevel(level);
  }

  appendKeys(fields: Readonly<Record<string, unknown>>): void {
    validateFields(fields);
    this.persistentFields = {
      ...this.persistentFields,
      ...snapshotFields(fields),
    };
  }

  removeKeys(...names: Array<string | readonly string[]>): void {
    const keys = names.flatMap((name) =>
      typeof name === "string" ? [name] : name,
    );
    const remaining = { ...this.persistentFields };
    for (const key of keys) delete remaining[key];
    this.persistentFields = remaining;
  }

  withContextKeys<T>(
    fields: Readonly<Record<string, unknown>>,
    callback: () => T,
  ): T {
    if (typeof callback !== "function") {
      throw new TypeError("callback must be a function");
    }
    validateFields(fields);
    return loggingContext.withFields(
      this.name,
      snapshotFields(fields),
      callback,
    );
  }

  private write(
    level: string,
    message: LogMessage,
    args: unknown[],
    explicitOptions?: WriteOptions,
    numericLevel = LEVELS[level as LogLevelName] ?? 0,
  ): void {
    if (this.threshold > numericLevel) return;
    const {
      options: parsedOptions,
      formatArgs,
      directAttributes,
    } = splitCallArguments(message, args);
    const options = {
      ...snapshotFields(parsedOptions, false),
      ...snapshotFields(explicitOptions, false),
    } as WriteOptions;
    const attributes = options.attributes ?? options.extra;

    try {
      const resolvedMessage =
        typeof message === "function" ? message() : message;
      const renderedMessage = format(resolvedMessage, ...formatArgs);
      const selectedAttributes = attributes ?? directAttributes;
      const extraFields =
        typeof selectedAttributes === "function"
          ? selectedAttributes()
          : selectedAttributes;
      if (extraFields !== undefined) validateFields(extraFields);
      const applicationFields =
        extraFields === undefined ? {} : snapshotFields(extraFields);

      const snapshot = loggingContext.snapshot(this.name);
      const mergedFields = {
        ...this.persistentFields,
        ...snapshot.temporary,
        ...applicationFields,
      };
      const active = snapshot.expired ? null : activeSpan();
      const span = active?.span ?? null;
      const invocation = snapshot.invocation;
      const traceId =
        span?.traceId ??
        (active?.apiAvailable === false ? invocation?.trace_id : undefined);
      const stack = new Error().stack;
      const stackInfo = options.stackInfo
        ? applicationStackInfo(stack)
        : undefined;
      const record: LogRecord = {
        timestamp: new Date().toISOString(),
        level,
        service: this.service,
        message: renderedMessage,
        logger: this.name,
        location: callsite(stack),
        ...(invocation === null ? {} : invocationFields(invocation)),
        ...(traceId === undefined ? {} : { trace_id: traceId }),
        ...(span?.spanId === undefined ? {} : { span_id: span.spanId }),
        ...(Object.keys(mergedFields).length === 0
          ? {}
          : { attributes: mergedFields }),
        ...(!Object.hasOwn(options, "exception")
          ? {}
          : { exception: options.exception }),
        ...(stackInfo === undefined
          ? {}
          : { stack_info: stackInfo }),
      };
      this.emit(record);
    } catch (error) {
      this.emit(
        {
          timestamp: new Date().toISOString(),
          level: "ERROR",
          service: this.service,
          message: "log record could not be serialized",
          logger: this.name,
          location: callsite(new Error().stack),
        },
        error,
      );
    }
  }

  private emit(record: LogRecord, cause?: unknown): void {
    let line: string;
    try {
      line = this.formatter.format(record);
    } catch {
      line = MINIMAL_LOG_LINE;
    }
    if (cause !== undefined) {
      try {
        const fallback = JSON.parse(line) as Record<string, unknown>;
        fallback.error = errorName(cause);
        line = JSON.stringify(fallback);
      } catch {
        // Keep the formatter's safe fallback line.
      }
    }
    try {
      process.stderr.write(line + "\n");
    } catch {
      // Logging output must not replace the handler's outcome.
    }
  }
}

function resolveService(service: string | undefined): string {
  if (service !== undefined) {
    if (typeof service !== "string") {
      throw new TypeError("service must be a string");
    }
    const explicit = service.trim();
    if (explicit.length === 0) {
      throw new RangeError("service must be a non-empty string");
    }
    return explicit;
  }

  const configured = process.env.OTEL_SERVICE_NAME?.trim();
  if (configured) return configured;
  const invocation = loggingContext.snapshot().invocation;
  return invocation?.app.trim() || "standalone";
}

function resolveLevel(level: LogLevel): number {
  if (typeof level === "number" && Number.isFinite(level)) return level;
  if (typeof level === "string") {
    const normalized = level.toUpperCase();
    const value =
      LEVELS[(normalized === "WARN" ? "WARNING" : normalized) as LogLevelName];
    if (value !== undefined) return value;
    throw new RangeError(`unknown log level: ${level}`);
  }
  throw new TypeError("level must be a finite number or a level name");
}

function configuredLevel(): number {
  const value = process.env.LOG_LEVEL?.trim();
  if (!value) return LEVELS.INFO;
  return resolveLevel(value as LogLevel);
}

function levelLabel(level: LogLevel, numeric: number): LogLevelName | string {
  if (typeof level === "string") {
    const normalized = level.toUpperCase();
    return normalized === "WARN" ? "WARNING" : normalized;
  }
  return (
    Object.entries(LEVELS).find(([, value]) => value === numeric)?.[0] ??
    String(numeric)
  );
}

function splitCallArguments(message: LogMessage, args: unknown[]): {
  options: WriteOptions;
  formatArgs: unknown[];
  directAttributes?: LogAttributesArgument;
} {
  let remaining = args;
  let options: WriteOptions = {};
  const last = remaining.at(-1);
  const placeholderCount = formatPlaceholderCount(message);
  if (
    placeholderCount > 0 &&
    remaining.length > placeholderCount &&
    isOptionsObject(last)
  ) {
    options = last;
    remaining = remaining.slice(0, -1);
  }

  const hasFormatSpecifier = placeholderCount > 0;
  const first = remaining[0];
  if (
    !hasFormatSpecifier &&
    (isRecord(first) || typeof first === "function")
  ) {
    return {
      options,
      directAttributes: first as
        | Readonly<Record<string, unknown>>
        | (() => Readonly<Record<string, unknown>>),
      formatArgs: remaining.slice(1),
    };
  }

  return {
    options,
    formatArgs: remaining,
  };
}

function formatPlaceholderCount(message: LogMessage): number {
  if (typeof message !== "string") return 0;
  const placeholders = message.match(/%%|%[sdifjoOc]/g) ?? [];
  return placeholders.filter((placeholder) => placeholder !== "%%").length;
}

function isOptionsObject(value: unknown): value is LogOptions {
  try {
    return (
      isRecord(value) &&
      ["attributes", "extra", "stackInfo"].some((key) =>
        Object.hasOwn(value, key),
      )
    );
  } catch {
    return false;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }
  try {
    const prototype = Object.getPrototypeOf(value);
    return prototype === Object.prototype || prototype === null;
  } catch {
    return false;
  }
}

function validateFields(
  fields: Readonly<Record<string, unknown>>,
): void {
  if (!isRecord(fields)) {
    throw new TypeError("fields must be an object");
  }
}

function snapshotFields(
  fields: object | undefined,
  markAccessors = true,
): Record<string, unknown> {
  if (fields === undefined) return {};
  const result = Object.create(null) as Record<string, unknown>;
  let descriptors: PropertyDescriptorMap;
  try {
    descriptors = Object.getOwnPropertyDescriptors(fields);
  } catch {
    return { $unsupported: "object" };
  }
  for (const [key, descriptor] of Object.entries(descriptors)) {
    if (!descriptor.enumerable) continue;
    if (Object.hasOwn(descriptor, "value")) {
      result[key] = descriptor.value;
    } else if (markAccessors) {
      result[key] = { $unsupported: "accessor" };
    }
  }
  return result;
}

function dataProperty<T>(
  value: object,
  key: PropertyKey,
): T | undefined {
  try {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    return descriptor !== undefined && Object.hasOwn(descriptor, "value")
      ? (descriptor.value as T)
      : undefined;
  } catch {
    return undefined;
  }
}

function invocationFields(
  invocation: InvocationLogContext,
): Pick<
  LogRecord,
  "app" | "handler" | "request_id" | "attempt" | "cold_start"
> {
  return {
    app: invocation.app,
    handler: invocation.handler,
    request_id: invocation.request_id,
    attempt: invocation.attempt,
    cold_start: invocation.cold_start,
  };
}

function activeSpan(): ActiveSpanLookup {
  if (traceAPI === undefined) {
    try {
      traceAPI = require("@opentelemetry/api") as typeof traceAPI;
    } catch {
      traceAPI = null;
    }
  }
  if (traceAPI === null) return { apiAvailable: false, span: null };

  try {
    const activeContext = traceAPI?.context?.active?.();
    const span = traceAPI?.trace?.getSpan?.(activeContext) as
      | { spanContext?: () => { traceId?: unknown; spanId?: unknown } }
      | undefined;
    const context = span?.spanContext?.();
    if (
      context === undefined ||
      traceAPI?.trace?.isSpanContextValid?.(context) !== true
    ) {
      return { apiAvailable: true, span: null };
    }
    const traceId =
      typeof context?.traceId === "string" &&
      /^[0-9a-f]{32}$/.test(context.traceId) &&
      !/^0{32}$/.test(context.traceId)
        ? context.traceId
        : undefined;
    const spanId =
      typeof context?.spanId === "string" &&
      /^[0-9a-f]{16}$/.test(context.spanId) &&
      !/^0{16}$/.test(context.spanId)
        ? context.spanId
        : undefined;
    return {
      apiAvailable: true,
      span:
        traceId === undefined || spanId === undefined
          ? null
          : { traceId, spanId },
    };
  } catch {
    return { apiAvailable: true, span: null };
  }
}

function callsite(stack: string | undefined): string {
  for (const line of applicationStackFrames(stack)) {
    const match = /^\s*at (?:(.*?) \()?(.+?):(\d+):\d+\)?$/.exec(line);
    if (match === null) continue;
    const file = match[2].replace(/^file:\/\//, "");
    const module = basename(file, extname(file)) || "unknown";
    const functionName = match[1]?.trim() || "<anonymous>";
    return `${module}.${functionName}:${match[3]}`;
  }
  return "unknown.<anonymous>:0";
}

function applicationStackInfo(stack: string | undefined): string | undefined {
  const frames = applicationStackFrames(stack);
  return frames.length === 0 ? undefined : frames.join("\n");
}

function applicationStackFrames(stack: string | undefined): string[] {
  if (typeof stack !== "string") return [];
  return stack
    .split("\n")
    .slice(1)
    .filter(
      (line) => !line.includes(loggerModulePath) && !line.includes("node:"),
    );
}

function errorName(error: unknown): string {
  if (!(error instanceof Error)) return "Error";
  try {
    let prototype = Object.getPrototypeOf(error);
    while (prototype !== null) {
      const constructor = dataProperty<unknown>(prototype, "constructor");
      const name =
        typeof constructor === "function"
          ? dataProperty<string>(constructor, "name")
          : undefined;
      if (typeof name === "string" && name.length > 0) return name;
      prototype = Object.getPrototypeOf(prototype);
    }
  } catch {
    return "Error";
  }
  return "Error";
}

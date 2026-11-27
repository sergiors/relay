import { createRequire } from "node:module";
import { types } from "node:util";

import type {
  Attributes,
  Span,
  SpanKind,
  Tracer as OpenTelemetryTracer,
} from "@opentelemetry/api";

export interface TracingSpanOptions {
  attributes?: Attributes;
  kind?: SpanKind;
  recordException?: boolean;
}

export interface CaptureMethodOptions extends TracingSpanOptions {
  name?: string;
}

type SpanCallback<T> = (span: Span) => T;
type SpanResult<T> = T extends PromiseLike<unknown> ? Promise<Awaited<T>> : T;

const require = createRequire(import.meta.url);
const api = loadOpenTelemetryAPI();
const statusCodeError = api.SpanStatusCode.ERROR;
const defaultSpanKind = api.SpanKind.INTERNAL;

export class Tracer {
  private readonly name: string;

  constructor(name: string) {
    if (typeof name !== "string") {
      throw new TypeError("name must be a string");
    }
    this.name = name;
  }

  span<T>(name: string, callback: SpanCallback<T>): SpanResult<T>;
  span<T>(
    name: string,
    options: TracingSpanOptions,
    callback: SpanCallback<T>,
  ): SpanResult<T>;
  span<T>(
    name: string,
    optionsOrCallback: TracingSpanOptions | SpanCallback<T>,
    callback?: SpanCallback<T>,
  ): SpanResult<T> {
    if (typeof name !== "string") {
      throw new TypeError("name must be a string");
    }

    const options =
      typeof optionsOrCallback === "function" ? undefined : optionsOrCallback;
    const run =
      typeof optionsOrCallback === "function" ? optionsOrCallback : callback;

    if (typeof run !== "function") {
      throw new TypeError("span requires a callback");
    }

    const {
      attributes,
      kind = defaultSpanKind,
      recordException = false,
    } = options ?? {};
    const tracer: OpenTelemetryTracer = api.trace.getTracer(this.name);
    const span = tracer.startSpan(name, { attributes, kind });
    const activeContext = api.trace.setSpan(api.context.active(), span);
    let result: T;

    try {
      result = api.context.with(activeContext, () => run(span));
    } catch (error) {
      finishError(span, error, recordException);
      throw error;
    }

    if (
      result === null ||
      (typeof result !== "object" && typeof result !== "function")
    ) {
      endSpanBestEffort(span);
      return result as SpanResult<T>;
    }

    let then: unknown;
    try {
      then = (result as { then?: unknown }).then;
    } catch (error) {
      finishError(span, error, recordException);
      throw error;
    }

    if (typeof then !== "function") {
      endSpanBestEffort(span);
      return result as SpanResult<T>;
    }

    const onceReadThenable = {
      then(
        resolve: (value: unknown) => void,
        reject: (reason: unknown) => void,
      ) {
        return Reflect.apply(then, result, [resolve, reject]);
      },
    };
    const settled = api.context.with(activeContext, () =>
      Promise.resolve(onceReadThenable),
    );
    return settled.then(
      (value) => {
        endSpanBestEffort(span);
        return value;
      },
      (error: unknown) => {
        finishError(span, error, recordException);
        throw error;
      },
    ) as SpanResult<T>;
  }

  captureMethod<F extends (...args: never[]) => unknown>(
    fn: F,
    options: CaptureMethodOptions = {},
  ): F {
    if (options === null) {
      throw new TypeError("captureMethod options must be an object");
    }
    validateCapturable(fn);
    if (options.name !== undefined && typeof options.name !== "string") {
      throw new TypeError("name must be a string or undefined");
    }
    const { name = fn.name || "anonymous", ...spanOptions } = options;
    const tracer = this;
    const wrapped = function (this: unknown, ...args: unknown[]) {
      return tracer.span(
        name,
        spanOptions,
        (span) => Reflect.apply(fn, this, args) as ReturnType<F>,
      );
    };

    return wrapped as unknown as F;
  }
}

function finishError(
  span: Span,
  error: unknown,
  recordException: boolean,
): void {
  tryBestEffort(() => span.setStatus({ code: statusCodeError }));
  if (recordException) {
    tryBestEffort(() => span.recordException(toException(error)));
  }
  endSpanBestEffort(span);
}

function endSpanBestEffort(span: Span): void {
  tryBestEffort(() => span.end());
}

function tryBestEffort(operation: () => void): void {
  try {
    operation();
  } catch {}
}

function toException(error: unknown): Error | string {
  if (typeof error === "string") return error;
  if (types.isNativeError(error)) return error as Error;
  return new Error("Non-Error value thrown");
}

function validateCapturable(fn: Function): void {
  if (typeof fn !== "function") {
    throw new TypeError("captureMethod requires a function");
  }

  const source = Function.prototype.toString.call(fn);
  const kind = Object.prototype.toString.call(fn);
  if (
    /^\s*class(?:\s|{)/.test(source) ||
    kind === "[object GeneratorFunction]" ||
    kind === "[object AsyncGeneratorFunction]"
  ) {
    throw new TypeError(
      "captureMethod does not support classes or generator functions",
    );
  }
}

function loadOpenTelemetryAPI(): typeof import("@opentelemetry/api") {
  try {
    return require("@opentelemetry/api") as typeof import("@opentelemetry/api");
  } catch (cause) {
    throw new Error(
      "Tracing requires the optional @opentelemetry/api package at version 1.10.0. Install it with `pnpm add @opentelemetry/api@1.10.0` or `npm install @opentelemetry/api@1.10.0`.",
      { cause },
    );
  }
}

# `@sergiors/relay-runtime`

## Installation

This is the official Node.js library for developing Relay applications and
using Relay's public handler interfaces. It is optional for Relay's execution;
add it as a normal application dependency when your application imports its
declarations.

The library supports Node.js 24 and newer and is ESM-only.

```sh
pnpm add @sergiors/relay-runtime
```

Or with npm:

```sh
npm install @sergiors/relay-runtime
```

The package exports the `HandlerContext` TypeScript declaration, optional
structured logging from `@sergiors/relay-runtime/logging`, and tracing from
`@sergiors/relay-runtime/tracing`. Relay's Node.js bootstrap constructs the
actual context object for each invocation and passes it to the handler.

TypeScript:

```ts
import type { HandlerContext } from "@sergiors/relay-runtime";

export async function handler(
  event: Record<string, unknown>,
  context: HandlerContext,
): Promise<void> {
  console.log(
    `request ${context.requestId} has ${context.getRemainingTimeMs()}ms left`,
  );
}
```

JavaScript with `checkJs`:

```js
/** @typedef {import("@sergiors/relay-runtime").HandlerContext} HandlerContext */

/**
 * @param {Record<string, unknown>} event
 * @param {HandlerContext} context
 */
export async function handler(event, context) {
  console.log(
    `request ${context.requestId} has ${context.getRemainingTimeMs()}ms left`,
  );
}
```

`requestId`, `attempt`, `maxAttempts`, `deadline`, `app`, and `handler` are
always present. `deadline` is an RFC 3339 timestamp string.
`messageId` and `traceId` are present only when available.
`getRemainingTimeMs()` returns the non-negative remaining time in milliseconds.

## Structured logging

`@sergiors/relay-runtime/logging` provides an opt-in `Logger` and
`JsonFormatter`. The default logger writes one compact JSON record per line to
stderr; it does not change `console`, raw stream writes, or Relay's lifecycle
lines.

```ts
import type { HandlerContext } from "@sergiors/relay-runtime";
import {
  Logger,
  type LoggerOptions,
} from "@sergiors/relay-runtime/logging";

const loggerOptions: LoggerOptions = { service: "users", level: "INFO" };
const log = new Logger(loggerOptions);

export async function handler(
  event: Record<string, unknown>,
  context: HandlerContext,
): Promise<void> {
  log.info("User created", {
    user_id: event.userId,
    request_id: context.requestId,
  });

  await log.withContextKeys({ operation: "create" }, async () => {
    log.info("Starting downstream work");
  });
}
```

The package is ESM-only. Use native `import` in ESM applications. CommonJS
applications can load the logging subpath with dynamic `import()`:

```js
async function main() {
  const { Logger } = await import("@sergiors/relay-runtime/logging");
  const log = new Logger("users");
  log.info("User created", { user_id: "u-42" });
}

void main();
```

There is no native `require()` export.

`Logger` accepts an explicit service and level. If service is omitted, it is
resolved once from `OTEL_SERVICE_NAME`, the active Relay app at construction, or
`standalone`, in that order. The level comes from the constructor when supplied,
otherwise `LOG_LEVEL`, otherwise `INFO`. The methods
`debug`, `info`, `warning`, `warn`, `error`, `critical`, and `log` support
printf-style formatting when the message contains a format placeholder.
Otherwise, a plain object as the first extra argument is nested under
`attributes`, including fields named `attributes`, `extra`, or `stackInfo`.
Options are recognized only as a trailing argument after all message
placeholders have their formatting arguments; they may set `attributes` or
`stackInfo`, and `extra` aliases `attributes`. For example:

```ts
log.info("User %s created", userId, {
  attributes: { user_id: userId },
});
```

Message and attribute callbacks run only when the level is enabled.
`exception(message, thrown, options?)` emits the thrown value as structured
exception data. `setLevel` and `isEnabledFor` control and check filtering.
Level precedence is constructor option (including positional `level`),
`LOG_LEVEL`, then `INFO`. `LOG_LEVEL` accepts `DEBUG`, `INFO`, `WARN`/`WARNING`,
`ERROR`, and `CRITICAL`. An invalid, non-empty value throws during construction.

`appendKeys(fields)` and `removeKeys(...names)` manage persistent attributes on
that Logger instance. `withContextKeys(fields, callback)` (also available as
`withContext`) adds temporary async-context attributes. Fields merge in this
order: persistent values, temporary values, then per-call attributes.
Concurrent async work gets isolated temporary scopes. Reserved names such as
`service` and `trace_id` are valid application attributes; they stay nested and
cannot replace Relay's top-level metadata. Relay binds `app`, `handler`,
`request_id`, `attempt`, and `cold_start` before handler imports and expires
inherited invocation context after the handler ends.

Every JSON record includes `timestamp`, `level`, `service`, `message`, `logger`,
and `location`. Active OpenTelemetry spans add `trace_id` and `span_id`;
otherwise a valid invocation trace ID is used. `attributes` is omitted when
empty. Records include `app`, `handler`, and `request_id` for Loki/Tempo
correlation; no Grafana-specific behavior is assumed.

The logger reads the optional official OpenTelemetry API but never initializes
or replaces a provider or exports logs. Use the public `Tracer` facade to create
spans that automatically correlate with logger records; see [Tracing](#tracing).

In standalone use, logging still works if the OTel API is not installed; records
simply omit trace fields. Relay-managed Node images provide the API.

Exceptions use `exception: {"type": ..., "message": ..., "stacktrace": ...}`.
`logger.exception(message, thrown)` accepts Error and non-Error thrown values.
Error attributes and exceptions include bounded cause details; `AggregateError`
also includes a bounded `errors` list. Serialization limits depth, collection
size, and output line length. Circular, unsupported, and accessor values are
marked; accessor properties and `toJSON` methods are not invoked.

Detached async work is not stopped when its handler returns. If it logs after
the invocation ends, its inherited context lease is expired, so invocation
fields and trace correlation are omitted.

The JSDoc example in `examples/jsdoc-handler.mjs` can be checked after building
the declarations with `pnpm run check`. The corresponding TypeScript example is
in `examples/typescript-handler.ts`. Relay's bootstrap constructs each runtime
context.

## Tracing

The tracing subpath provides `Tracer`, a small facade over the official
OpenTelemetry API. Its optional peer supports API versions `>=1.10.0 <1.11.0`;
Relay-managed Node images use `1.10.0`. Keep the application on that API minor
version so its provider registration is visible to Relay. In Relay-managed
applications, install the runtime package; the Node runtime supplies the API:

```sh
pnpm add @sergiors/relay-runtime
```

The application or runtime owns provider and context-manager setup;
`Tracer` never registers, replaces, or shuts down either. It resolves its
tracer through the official `trace.getTracer` API for each span, so a `Tracer`
constructed before provider registration works after a provider is registered.
In Relay-managed containers, Relay configures the provider when an OTLP endpoint
is set. Without an endpoint, spans are no-op unless the application already
registered a provider; no trace data is recorded or exported. With
`OTEL_SDK_DISABLED=true`, W3C context propagation remains available, but the
Relay-managed SDK does not record or export spans.

Set `RELAY_OTEL_PROVIDER=application` in the application environment to opt out
of Relay-managed provider creation and shutdown. The application must then
configure and own its provider and exporter; this setting alone does not enable
recording. Relay still installs an async context manager when none is registered.
Standalone applications must also install `@opentelemetry/api@1.10.0` and
configure their own OpenTelemetry SDK/provider before expecting spans to be
recorded or exported:

```sh
pnpm add @sergiors/relay-runtime @opentelemetry/api@1.10.0
# or
npm install @sergiors/relay-runtime @opentelemetry/api@1.10.0
```

```ts
import type { HandlerContext } from "@sergiors/relay-runtime";
import { Logger } from "@sergiors/relay-runtime/logging";
import { Tracer } from "@sergiors/relay-runtime/tracing";

const log = new Logger({ service: "orders" });
const tracer = new Tracer("orders");

export async function handler(
  event: { orderId: string },
  context: HandlerContext,
): Promise<void> {
  await tracer.span("orders.process", async (span) => {
    log.info("Processing order", { order_id: event.orderId });
    span.addEvent("order.processing.started", {
      remaining_ms: context.getRemainingTimeMs(),
    });
    await processOrder(event.orderId);
  });
}
```

`span(name, callback)` and `span(name, options, callback)` pass the native
OpenTelemetry `Span` to the callback. The facade owns its lifecycle: do not call
`span.end()` yourself. Synchronous callbacks return synchronously. Promise and
thenable callbacks keep the span active until settlement; their returned value
is a Promise, so Promise/thenable identity is not preserved. Span options are
limited to OpenTelemetry `attributes`, `kind` (default `SpanKind.INTERNAL`), and
Relay's `recordException` flag.

Thrown or rejected callbacks set generic `ERROR` status and rethrow the original
value. Exception events are not recorded by default; opt in with
`{ recordException: true }`. When enabled, native `Error` and string values are
recorded; other thrown values are represented by a generic error without
serializing or inspecting the value. Opt in only when exception messages and
stacks are safe to export.

`captureMethod(fn, { name, recordException })` preserves the wrapped function's
call signature, receiver, arguments, result, and thrown errors. Arguments and
results are passed through, not captured or attached to spans automatically.
For example:

```ts
const formatOrder = tracer.captureMethod(
  function formatOrder(orderId: string): string {
    return `order:${orderId}`;
  },
  { name: "orders.format" },
);

const saveOrder = tracer.captureMethod(
  async function saveOrder(orderId: string): Promise<void> {
    await persistOrder(orderId);
  },
  { name: "orders.save" },
);
```

Captured methods must be ordinary functions; classes, generator functions, and
async-generator functions are rejected. Do not launch detached work inside a
span and return before awaiting it: that work is not covered by the span. Relay
expires invocation context when the handler returns, so detached work does not
extend the invocation's tracing or logging lease.

Prefer stable names such as `<domain>.<action>` (`orders.save`), and add only
low-cardinality, non-sensitive explicit attributes. The public `Logger`
automatically includes the active span's trace and span IDs in log records;
this trace/log correlation is Grafana-independent.

This package is ESM-only. CommonJS applications can load tracing with dynamic
`import()`:

```js
async function main() {
  const { Tracer } = await import("@sergiors/relay-runtime/tracing");
  const tracer = new Tracer("users");
  await tracer.span("users.load", async () => loadUsers());
}

void main();
```

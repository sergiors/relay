// Optional OpenTelemetry correlation: standalone records carry no invocation
// or trace fields, span ids — valid unsampled, nested, and dynamic — are read
// through the shared global registry, an invocation with the API present but
// no active span (including an explicit ROOT_CONTEXT) omits correlation
// instead of falling back to the frame trace id, an expired detached lease
// drops correlation, and without the API an invocation record falls back to
// the frame trace id. The logger never registers or replaces a provider, and
// the absence of the optional API is proven in child processes where its
// resolution fails.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { test } from "node:test";
import { context, ROOT_CONTEXT, trace } from "@opentelemetry/api";

import { runInvocation } from "../../../internal/runtime/node/logging-context.mjs";
import { Logger } from "../dist/logging.js";
import {
  captureRecords,
  oneRecord,
  sleep,
} from "./helpers/logging.mjs";

const require = createRequire(import.meta.url);
const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const TRACE_ONE = "0123456789abcdef0123456789abcdef";
const SPAN_A = "aaaabbbbccccdddd";
const SPAN_B = "eeeeffff00001111";
const SPAN_C = "9999888877776666";

// The API's default context manager is a no-op, so context.with() is only
// observable through a registered manager — the same seam Relay's bootstrap
// installs in managed images. Registering before any test runs makes span
// contexts active exactly as they are in a managed process.
let activeContext = context.active();
context.setGlobalContextManager({
  active() {
    return activeContext;
  },
  with(ctx, fn, thisArg, ...args) {
    const previous = activeContext;
    activeContext = ctx;
    try {
      return fn.call(thisArg, ...args);
    } finally {
      activeContext = previous;
    }
  },
  bind(_ctx, target) {
    return target;
  },
  enable() {
    return true;
  },
  disable() {
    return true;
  },
});

function spanContext(traceId, spanId, traceFlags = 0) {
  return { traceId, spanId, traceFlags, isRemote: false };
}

function withSpan(sc, callback) {
  return context.with(trace.setSpanContext(context.active(), sc), callback);
}

test("standalone records omit invocation and trace fields", async () => {
  const log = new Logger({ service: "standalone", level: "INFO" });

  const [record] = await captureRecords(() => log.info("no-context"));
  for (const key of ["app", "handler", "request_id", "attempt", "cold_start", "trace_id", "span_id"]) {
    assert.ok(!(key in record), `standalone record must omit ${key}`);
  }
});

test("a valid unsampled span context contributes trace_id and span_id", async () => {
  const log = new Logger({ service: "spans", level: "INFO" });

  const records = await captureRecords(() => {
    withSpan(spanContext(TRACE_ONE, SPAN_A), () => {
      log.info("unsampled-span");
    });
    withSpan(spanContext("0".repeat(32), "0".repeat(16)), () => {
      log.info("invalid-span");
    });
  });

  const unsampled = oneRecord(records, "unsampled-span");
  assert.equal(unsampled.trace_id, TRACE_ONE, "an unsampled but valid span still correlates");
  assert.equal(unsampled.span_id, SPAN_A);

  const invalid = oneRecord(records, "invalid-span");
  assert.ok(!("trace_id" in invalid), "an all-zero span context must not correlate");
  assert.ok(!("span_id" in invalid));
});

test("nested spans report the innermost id and dynamic ids are not sticky", async () => {
  const log = new Logger({ service: "nested-spans", level: "INFO" });

  const records = await captureRecords(() => {
    withSpan(spanContext(TRACE_ONE, SPAN_A), () => {
      log.info("nest-outer-before");
      withSpan(spanContext(TRACE_ONE, SPAN_B), () => {
        log.info("nest-inner");
      });
      log.info("nest-outer-after");
    });
    withSpan(spanContext(TRACE_ONE, SPAN_C), () => {
      log.info("nest-dynamic");
    });
  });

  assert.equal(oneRecord(records, "nest-outer-before").span_id, SPAN_A);
  const inner = oneRecord(records, "nest-inner");
  assert.equal(inner.span_id, SPAN_B, "the innermost active span wins");
  assert.equal(inner.trace_id, TRACE_ONE, "the trace id is shared across the nest");
  assert.equal(oneRecord(records, "nest-outer-after").span_id, SPAN_A, "the outer span restores");
  assert.equal(oneRecord(records, "nest-dynamic").span_id, SPAN_C, "each span contributes its own id");
});

test("records never initialize, replace, or depend on a provider", async () => {
  // the CJS copy is the instance dist/logging.js requires
  const api = require("@opentelemetry/api");
  const provider = api.trace.getTracerProvider();
  const fakeProvider = { getTracer: () => ({}) };

  // no tracer provider has been registered by any record emitted so far in
  // this process — the API's default delegate is still the registered state
  assert.equal(provider.getDelegate().constructor.name, "NoopTracerProvider");

  assert.equal(
    api.trace.setGlobalTracerProvider(fakeProvider),
    true,
    "the test owns the provider registry",
  );
  assert.equal(provider.getDelegate(), fakeProvider);

  let tracerRegistrations = 0;
  let loggerRegistrations = 0;
  const originalSetTracer = api.trace.setGlobalTracerProvider;
  const originalSetLogger = api.logs.setGlobalLoggerProvider;
  api.trace.setGlobalTracerProvider = (...args) => {
    tracerRegistrations += 1;
    return originalSetTracer.apply(api.trace, args);
  };
  api.logs.setGlobalLoggerProvider = (...args) => {
    loggerRegistrations += 1;
    return originalSetLogger.apply(api.logs, args);
  };

  try {
    const records = await captureRecords(() => {
      new Logger({ service: "provider-probe", level: "INFO" }).info("provider-one");
      new Logger({ service: "provider-probe", level: "ERROR" }).error("provider-two");
    });
    assert.equal(records.length, 2);

    assert.equal(tracerRegistrations, 0, "logging must not register a tracer provider");
    assert.equal(loggerRegistrations, 0, "logging must not register a logs provider");
    assert.equal(
      api.trace.getTracerProvider(),
      provider,
      "logging must not replace the provider",
    );
    assert.equal(
      provider.getDelegate(),
      fakeProvider,
      "the registered provider must be untouched",
    );
  } finally {
    api.trace.setGlobalTracerProvider = originalSetTracer;
    api.logs.setGlobalLoggerProvider = originalSetLogger;
  }
});

test("an invocation with the API present but no active span omits correlation", async () => {
  // with the API importable, only a valid active span context may correlate a
  // record: the frame's trace id must never be paired with a span that does
  // not exist, even for an explicit ROOT_CONTEXT
  const log = new Logger({ service: "invocation-trace", level: "INFO" });
  const frame = (traceId) => ({
    app: "trace-app",
    handler: "index.trace",
    requestId: "r-trace",
    attempt: 1,
    ...(traceId === undefined ? {} : { traceId }),
  });

  const records = await captureRecords(async () => {
    await runInvocation(frame("d".repeat(32)), async () => {
      log.info("frame-trace-only");
      context.with(ROOT_CONTEXT, () => {
        log.info("root-context-record");
      });
      withSpan(spanContext(TRACE_ONE, SPAN_A), () => {
        log.info("span-wins");
      });
    });
    await runInvocation(frame("0".repeat(32)), async () => {
      log.info("invalid-frame-trace");
    });
    await runInvocation(frame(undefined), async () => {
      log.info("no-frame-trace");
    });
  });

  const fromFrame = oneRecord(records, "frame-trace-only");
  assert.ok(
    !("trace_id" in fromFrame),
    `a frame trace id must not pair with a missing span: ${JSON.stringify(fromFrame)}`,
  );
  assert.ok(!("span_id" in fromFrame), "no span means no span id");
  // the invocation's own correlation metadata is untouched by the omission
  assert.equal(fromFrame.app, "trace-app");
  assert.equal(fromFrame.request_id, "r-trace");

  const root = oneRecord(records, "root-context-record");
  assert.ok(!("trace_id" in root), "ROOT_CONTEXT must not correlate");
  assert.ok(!("span_id" in root), "ROOT_CONTEXT must not correlate");

  const spanWins = oneRecord(records, "span-wins");
  assert.equal(spanWins.trace_id, TRACE_ONE, "an active span correlates");
  assert.equal(spanWins.span_id, SPAN_A);

  const invalid = oneRecord(records, "invalid-frame-trace");
  assert.ok(!("trace_id" in invalid), "an all-zero frame trace id must not correlate");

  const none = oneRecord(records, "no-frame-trace");
  assert.ok(!("trace_id" in none));
});

test("an expired detached lease drops invocation and span correlation", async () => {
  const log = new Logger({ service: "expired", level: "INFO" });
  let detached;

  const records = await captureRecords(async () => {
    await runInvocation(
      {
        app: "expired-app",
        handler: "index.detached",
        requestId: "r-expired",
        attempt: 1,
        traceId: "d".repeat(32),
      },
      async () => {
        log.info("live-invocation");
        // work deliberately outlives the invocation: the callback returns
        // without awaiting it, so the lease expires before it logs
        detached = (async () => {
          await sleep(20);
          withSpan(spanContext(TRACE_ONE, SPAN_A), () => {
            log.info("expired-span");
          });
        })();
      },
    );
    await detached;
  });

  const live = oneRecord(records, "live-invocation");
  assert.equal(live.app, "expired-app");
  assert.equal(live.request_id, "r-expired");
  // the API is present with no active span for this record: correlation is
  // omitted rather than falling back to the frame trace id
  assert.ok(
    !("trace_id" in live),
    "a live record without an active span must not correlate",
  );
  assert.ok(!("span_id" in live));

  const expired = oneRecord(records, "expired-span");
  for (const key of ["app", "handler", "request_id", "attempt", "cold_start", "trace_id", "span_id"]) {
    assert.ok(
      !(key in expired),
      `detached post-invocation work must omit ${key} even with an active span context`,
    );
  }
});

test("logging without the OpenTelemetry API installed still emits records", () => {
  const child = spawnSync(
    process.execPath,
    [
      "--require",
      fileURLToPath(new URL("./helpers/block-otel.cjs", import.meta.url)),
      fileURLToPath(new URL("./fixtures/no-otel-standalone.mjs", import.meta.url)),
    ],
    {
      cwd: packageRoot,
      encoding: "utf8",
      env: Object.fromEntries(
        Object.entries(process.env).filter(
          ([name]) => name !== "LOG_LEVEL" && name !== "OTEL_SERVICE_NAME",
        ),
      ),
    },
  );

  assert.equal(child.status, 0, `child process failed:\n${child.stderr}`);
  const records = child.stderr
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.startsWith("{"))
    .map((line) => JSON.parse(line));
  const [record] = records.filter((entry) => entry.message === "api-absent-record");
  assert.ok(record, `child emitted no record; stderr:\n${child.stderr}`);
  assert.equal(record.level, "INFO");
  assert.equal(record.service, "no-otel");
  assert.deepEqual(record.attributes, { standalone: true });
  assert.ok(!("trace_id" in record) && !("span_id" in record));
  assert.ok(!("app" in record));
  assert.ok(
    !child.stderr.includes("MODULE_NOT_FOUND"),
    "the missing optional API must degrade silently",
  );
});

test("without the API an invocation record falls back to the frame trace id", () => {
  const child = spawnSync(
    process.execPath,
    [
      "--require",
      fileURLToPath(new URL("./helpers/block-otel.cjs", import.meta.url)),
      fileURLToPath(new URL("./fixtures/no-otel-invocation.mjs", import.meta.url)),
    ],
    {
      cwd: packageRoot,
      encoding: "utf8",
      env: Object.fromEntries(
        Object.entries(process.env).filter(
          ([name]) => name !== "LOG_LEVEL" && name !== "OTEL_SERVICE_NAME",
        ),
      ),
    },
  );

  assert.equal(child.status, 0, `child process failed:\n${child.stderr}`);
  const records = child.stderr
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.startsWith("{"))
    .map((line) => JSON.parse(line));
  const [record] = records.filter(
    (entry) => entry.message === "api-absent-invocation",
  );
  assert.ok(record, `child emitted no record; stderr:\n${child.stderr}`);

  // with no span source available, the frame's validated trace id is the
  // only correlation left — no span id can exist without a span
  assert.equal(record.trace_id, "d".repeat(32));
  assert.ok(!("span_id" in record), "no span means no span id");
  assert.equal(record.app, "no-otel-app");
  assert.equal(record.request_id, "r-no-otel");
  assert.ok(
    !child.stderr.includes("MODULE_NOT_FOUND"),
    "the missing optional API must degrade silently",
  );
});

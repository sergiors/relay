// Tracer composes with the shared registries instead of owning them: while the
// global provider is untouched, spans stay noop and tracing registers nothing;
// a provider the application registers afterwards gives spans whose ids Logger
// records carry inside a span, restoring the outer span around nested ones and
// dropping correlation outside any span.
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import {
  TracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { test } from "node:test";

import { Logger } from "../dist/logging.js";
import { Tracer } from "../dist/tracing.js";
import { captureRecords, oneRecord } from "./helpers/logging.mjs";

const require = createRequire(import.meta.url);
// the CJS copy is the instance dist/tracing.js and dist/logging.js require
const api = require("@opentelemetry/api");

const contextManager = new AsyncLocalStorageContextManager().enable();
assert.equal(api.context.setGlobalContextManager(contextManager), true);

test("Tracer never registers or replaces the provider or context manager", () => {
  // given: the registry still holds the API defaults
  const provider = api.trace.getTracerProvider();
  assert.equal(provider.getDelegate().constructor.name, "NoopTracerProvider");

  let providerRegistrations = 0;
  let managerRegistrations = 0;
  const originalSetTracer = api.trace.setGlobalTracerProvider;
  const originalSetManager = api.context.setGlobalContextManager;
  api.trace.setGlobalTracerProvider = (...args) => {
    providerRegistrations += 1;
    return originalSetTracer.apply(api.trace, args);
  };
  api.context.setGlobalContextManager = (...args) => {
    managerRegistrations += 1;
    return originalSetManager.apply(api.context, args);
  };

  try {
    // when: tracing runs against the defaults
    const tracer = new Tracer("relay.test.ownership");
    tracer.span("noop-span", () => 1);
    tracer.captureMethod(() => "captured")();

    // then: the registries are exactly as the test left them
    assert.equal(providerRegistrations, 0, "tracing must not register a provider");
    assert.equal(
      managerRegistrations,
      0,
      "tracing must not register a context manager",
    );
    assert.equal(
      api.trace.getTracerProvider(),
      provider,
      "tracing must not replace the provider",
    );
    assert.equal(
      api.trace.getTracerProvider().getDelegate().constructor.name,
      "NoopTracerProvider",
      "the registered delegate must be untouched",
    );
  } finally {
    api.trace.setGlobalTracerProvider = originalSetTracer;
    api.context.setGlobalContextManager = originalSetManager;
  }
});

test("records inside traced spans correlate with the active span", async () => {
  // given: the application registers a provider after the ownership check
  const exporter = new InMemorySpanExporter();
  const provider = new TracerProvider({
    spanProcessors: [new SimpleSpanProcessor({ exporter })],
  });
  assert.equal(api.trace.setGlobalTracerProvider(provider), true);
  const log = new Logger({ service: "traced", level: "INFO" });
  const tracer = new Tracer("relay.test.logger");
  let outerContext;
  let innerContext;

  // when
  const records = await captureRecords(async () => {
    await tracer.span("traced-work", async (span) => {
      outerContext = span.spanContext();
      log.info("inside-span");
      await tracer.span("traced-inner", (inner) => {
        innerContext = inner.spanContext();
        log.info("inside-inner");
      });
      log.info("back-in-outer");
    });
    log.info("outside-spans");
  });

  // then
  const inside = oneRecord(records, "inside-span");
  assert.equal(inside.trace_id, outerContext.traceId);
  assert.equal(inside.span_id, outerContext.spanId);

  const nested = oneRecord(records, "inside-inner");
  assert.equal(nested.trace_id, outerContext.traceId, "nested spans share");
  assert.equal(nested.span_id, innerContext.spanId, "the innermost span wins");

  assert.equal(
    oneRecord(records, "back-in-outer").span_id,
    outerContext.spanId,
    "the outer span restores after the nested one",
  );

  const outside = oneRecord(records, "outside-spans");
  assert.ok(!("trace_id" in outside), "no span means no correlation");
  assert.ok(!("span_id" in outside));

  assert.deepEqual(
    exporter.getFinishedSpans().map((span) => span.name),
    ["traced-inner", "traced-work"],
  );
});

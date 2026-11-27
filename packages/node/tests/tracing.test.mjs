import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { context, SpanStatusCode, trace } from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import {
  TracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

import { Tracer } from "../dist/tracing.js";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const exporter = new InMemorySpanExporter();
const provider = new TracerProvider({
  spanProcessors: [new SimpleSpanProcessor({ exporter })],
});
const contextManager = new AsyncLocalStorageContextManager().enable();
assert.equal(context.setGlobalContextManager(contextManager), true);

test("late provider registration, sync results, async context, and thenables", async () => {
  const tracer = new Tracer("relay.test.late");
  assert.equal(trace.getTracerProvider().getDelegate().constructor.name, "NoopTracerProvider");
  assert.equal(trace.setGlobalTracerProvider(provider), true);

  const syncResult = tracer.span("sync", (span) => {
    assert.equal(trace.getSpan(context.active()), span);
    return 42;
  });
  assert.equal(syncResult, 42, "synchronous callbacks remain synchronous");
  assert.equal(trace.getSpan(context.active()), undefined);

  const asyncResult = await tracer.span("async", async (span) => {
    assert.equal(trace.getSpan(context.active()), span);
    await Promise.resolve();
    assert.equal(trace.getSpan(context.active()), span);
    return "settled";
  });
  assert.equal(asyncResult, "settled");

  let settleThenable;
  let thenableSpan;
  const thenable = {
    then(resolve) {
      assert.equal(trace.getSpan(context.active()), thenableSpan);
      settleThenable = resolve;
    },
  };
  const thenableResult = tracer.span("thenable", (span) => {
    thenableSpan = span;
    assert.equal(trace.getSpan(context.active()), span);
    return thenable;
  });
  assert.equal(typeof thenableResult.then, "function");
  assert.equal(exporter.getFinishedSpans().some((span) => span.name === "thenable"), false);
  await Promise.resolve();
  assert.equal(typeof settleThenable, "function");
  settleThenable("thenable settled");
  assert.equal(await thenableResult, "thenable settled");

  assert.deepEqual(
    exporter.getFinishedSpans().map((span) => span.name),
    ["sync", "async", "thenable"],
  );
});

test("failures set generic ERROR status and record exceptions only when enabled", async () => {
  const tracer = new Tracer("relay.test.errors");
  const error = new Error("sensitive details");

  assert.throws(
    () => tracer.span("sync-error", () => {
      throw error;
    }),
    (thrown) => thrown === error,
  );
  await assert.rejects(
    tracer.span("async-error", async () => {
      throw error;
    }),
    (thrown) => thrown === error,
  );
  await assert.rejects(
    tracer.span("recorded-error", { recordException: true }, async () => {
      throw error;
    }),
    (thrown) => thrown === error,
  );

  const spans = exporter.getFinishedSpans();
  const sync = spans.find((span) => span.name === "sync-error");
  const async = spans.find((span) => span.name === "async-error");
  const recorded = spans.find((span) => span.name === "recorded-error");
  for (const span of [sync, async, recorded]) {
    assert.equal(span.status.code, SpanStatusCode.ERROR);
    assert.equal(span.ended, true);
  }
  assert.equal(sync.events.length, 0);
  assert.equal(async.events.length, 0);
  assert.equal(recorded.events.length, 1);
  assert.equal(recorded.events[0].name, "exception");
  assert.equal(recorded.events[0].attributes["exception.message"], error.message);
});

test("captureMethod preserves receiver, arguments, results, and thrown values", async () => {
  const tracer = new Tracer("relay.test.methods");
  const receiver = {
    prefix: "hello",
    greet(name) {
      return `${this.prefix} ${name}`;
    },
  };
  const greet = tracer.captureMethod(receiver.greet);
  assert.equal(greet.call(receiver, "world"), "hello world");

  const asyncMethod = tracer.captureMethod(async function (value) {
    await Promise.resolve();
    return value * 2;
  }, { name: "double" });
  assert.equal(await asyncMethod(21), 42);

  const thrown = new Error("method failed");
  const fails = tracer.captureMethod(() => {
    throw thrown;
  }, { recordException: true });
  assert.throws(fails, (error) => error === thrown);

  function* generator() {
    yield 1;
  }
  async function* asyncGenerator() {
    yield 1;
  }
  class Example {}
  for (const unsupported of [generator, asyncGenerator, Example]) {
    assert.throws(
      () => tracer.captureMethod(unsupported),
      /does not support classes or generator functions/,
    );
  }
});

test("tracing reports how to install the optional API when it is unavailable", () => {
  const result = spawnSync(
    process.execPath,
    [
      "--require",
      new URL("./helpers/block-otel.cjs", import.meta.url).pathname,
      "--input-type=module",
      "-e",
      'await import("./dist/tracing.js")',
    ],
    { cwd: packageRoot, encoding: "utf8" },
  );
  assert.notEqual(result.status, 0);
  assert.match(
    result.stderr,
    /Tracing requires the optional @opentelemetry\/api package.*pnpm add @opentelemetry\/api/,
  );
});

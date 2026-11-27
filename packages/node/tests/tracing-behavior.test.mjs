// Span options — attributes and kind — reach the native span without leaking
// recordException, invalid inputs — a missing callback or a non-string name —
// fail before any callback or a span starts, primitive throws carry an ERROR
// status and are recorded only when opted in, a failing then accessor ends the
// span as an error, a thenable's then getter is read exactly once before it
// settles, nested spans parent to the active span and restore it, concurrent
// spans stay isolated across interleaving, and captureMethod forwards apply
// calls, bound receivers, and derived span names.
import assert from "node:assert/strict";
import { context, SpanKind, SpanStatusCode, trace } from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import {
  TracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import { test } from "node:test";

import { Tracer } from "../dist/tracing.js";

const exporter = new InMemorySpanExporter();
const provider = new TracerProvider({
  spanProcessors: [new SimpleSpanProcessor({ exporter })],
});
const contextManager = new AsyncLocalStorageContextManager().enable();
assert.equal(context.setGlobalContextManager(contextManager), true);
assert.equal(trace.setGlobalTracerProvider(provider), true);

function finished(name) {
  const span = exporter.getFinishedSpans().find((entry) => entry.name === name);
  assert.ok(span, `expected a finished span named ${name}`);
  return span;
}

test("span options reach the native span while recordException stays an option", () => {
  // given
  const tracer = new Tracer("relay.test.options");

  // when
  tracer.span(
    "attributed",
    {
      attributes: { "messaging.operation": "publish", attempt: 2 },
      recordException: true,
    },
    () => "published",
  );
  tracer.span("served", { kind: SpanKind.SERVER }, () => "handled");

  // then
  const span = finished("attributed");
  assert.equal(span.attributes["messaging.operation"], "publish");
  assert.equal(span.attributes.attempt, 2);
  assert.ok(
    !("recordException" in span.attributes),
    "the wrapper option must not leak into span attributes",
  );
  assert.equal(
    span.kind,
    SpanKind.INTERNAL,
    "an omitted kind defaults to INTERNAL",
  );

  const served = finished("served");
  assert.equal(served.kind, SpanKind.SERVER, "an explicit kind is forwarded");
});

test("invalid span and captureMethod inputs fail before any span starts", () => {
  // given
  const tracer = new Tracer("relay.test.validation");
  const before = exporter.getFinishedSpans().length;

  // when / then
  assert.throws(
    () => tracer.span("missing-callback"),
    { name: "TypeError", message: /span requires a callback/ },
  );
  assert.throws(
    () => tracer.span("missing-callback", { attributes: { attempt: 1 } }),
    { name: "TypeError", message: /span requires a callback/ },
  );
  assert.throws(
    () => tracer.captureMethod(42),
    { name: "TypeError", message: /captureMethod requires a function/ },
  );
  // null options must hit the explicit guard, not an incidental property read
  assert.throws(
    () => tracer.captureMethod(() => {}, null),
    { name: "TypeError", message: /captureMethod options must be an object/ },
  );

  assert.equal(
    exporter.getFinishedSpans().length,
    before,
    "rejected inputs must not export spans",
  );
});

test("non-string names fail before the callback or a span runs", () => {
  // given
  const before = exporter.getFinishedSpans().length;
  let invoked = 0;

  // when / then: the tracer's own name
  for (const invalid of [42, null, {}]) {
    assert.throws(
      () => new Tracer(invalid),
      { name: "TypeError", message: /name must be a string/ },
    );
  }
  assert.doesNotThrow(() => new Tracer(""), "any string is a valid name");

  const tracer = new Tracer("relay.test.names");

  // then: span names are checked before the callback and the span, in both
  // overloads
  assert.throws(
    () =>
      tracer.span(42, () => {
        invoked += 1;
      }),
    { name: "TypeError", message: /name must be a string/ },
  );
  assert.throws(
    () =>
      tracer.span(null, { attributes: { attempt: 1 } }, () => {
        invoked += 1;
      }),
    { name: "TypeError", message: /name must be a string/ },
  );
  assert.equal(invoked, 0, "the callback must not run for an invalid name");

  // then: captureMethod rejects non-string and null option names up front
  function identity() {
    return "value";
  }
  assert.throws(
    () => tracer.captureMethod(identity, { name: 42 }),
    { name: "TypeError", message: /name must be a string or undefined/ },
  );
  assert.throws(
    () => tracer.captureMethod(identity, { name: null }),
    { name: "TypeError", message: /name must be a string or undefined/ },
  );

  assert.equal(
    exporter.getFinishedSpans().length,
    before,
    "rejected names must not export spans",
  );

  // then: valid string names and the default name keep working
  assert.equal(tracer.span("valid-name", () => "ok"), "ok");
  assert.equal(finished("valid-name").name, "valid-name");

  const defaulted = tracer.captureMethod(identity, { name: undefined });
  assert.equal(defaulted(), "value");
  assert.equal(finished("identity").name, "identity");
});

test("primitive throws carry an error status and record only when enabled", async () => {
  // given
  const tracer = new Tracer("relay.test.primitives");

  // when: a synchronous string throw with recording at its default
  assert.throws(
    () =>
      tracer.span("unrecorded-string", () => {
        throw "plain failure";
      }),
    (thrown) => thrown === "plain failure",
  );
  // when: opted-in recording of a string and a number throw
  await assert.rejects(
    tracer.span("recorded-string", { recordException: true }, async () => {
      throw "plain failure";
    }),
    (thrown) => thrown === "plain failure",
  );
  await assert.rejects(
    tracer.span("recorded-number", { recordException: true }, async () => {
      throw 42;
    }),
    (thrown) => thrown === 42,
  );
  // when: an Error throw without opting in
  await assert.rejects(
    tracer.span("unrecorded-error", async () => {
      throw new Error("default off");
    }),
    (thrown) => thrown instanceof Error && thrown.message === "default off",
  );

  // then
  const recordedString = finished("recorded-string");
  assert.equal(recordedString.status.code, SpanStatusCode.ERROR);
  assert.equal(recordedString.events.length, 1);
  assert.equal(recordedString.events[0].name, "exception");
  assert.equal(
    recordedString.events[0].attributes["exception.message"],
    "plain failure",
    "a thrown string is recorded verbatim",
  );

  const recordedNumber = finished("recorded-number");
  assert.equal(recordedNumber.status.code, SpanStatusCode.ERROR);
  assert.equal(recordedNumber.events.length, 1);
  assert.equal(
    recordedNumber.events[0].attributes["exception.message"],
    "Non-Error value thrown",
    "a non-native thrown value records a generic message",
  );

  for (const name of ["unrecorded-string", "unrecorded-error"]) {
    const span = finished(name);
    assert.equal(span.status.code, SpanStatusCode.ERROR);
    assert.equal(span.ended, true);
    assert.equal(span.events.length, 0, `${name} must record no exception`);
  }
});

test("a failing then accessor ends the span as an error and rethrows", () => {
  // given
  const tracer = new Tracer("relay.test.thenable");
  const failure = new Error("broken then accessor");
  const hostile = {
    get then() {
      throw failure;
    },
  };

  // when / then
  assert.throws(
    () =>
      tracer.span("hostile-thenable", { recordException: true }, () => hostile),
    (thrown) => thrown === failure,
  );

  const span = finished("hostile-thenable");
  assert.equal(span.status.code, SpanStatusCode.ERROR);
  assert.equal(span.ended, true);
  assert.equal(span.events.length, 1);
  assert.equal(span.events[0].attributes["exception.message"], failure.message);
});

test("a thenable's then getter is read once, keeps the span active, and ends it", async () => {
  // given: a valid thenable whose then arrives through a counted getter
  const tracer = new Tracer("relay.test.thenable-once");
  const spanName = "counted-thenable";
  let reads = 0;
  let invocations = 0;
  let settle;
  let activeWhileSettling;
  let thenableSpan;
  const thenable = {
    get then() {
      reads += 1;
      return (resolve) => {
        invocations += 1;
        activeWhileSettling = trace.getSpan(context.active());
        settle = resolve;
      };
    },
  };

  const ended = () =>
    exporter.getFinishedSpans().some((entry) => entry.name === spanName);

  // when
  const result = tracer.span(spanName, (span) => {
    thenableSpan = span;
    return thenable;
  });

  // then: detection reads the getter exactly once and keeps the span open
  assert.equal(reads, 1, "detection must read the then getter exactly once");
  assert.equal(ended(), false, "the span stays open while the thenable waits");

  await Promise.resolve();

  // then: settling runs the captured then without re-reading the getter, and
  // the span is active during settlement
  assert.equal(reads, 1, "settling must not re-read the then getter");
  assert.equal(typeof settle, "function", "the captured then was invoked");
  assert.equal(invocations, 1, "the captured then is invoked exactly once");
  assert.equal(
    activeWhileSettling,
    thenableSpan,
    "the span stays active while the thenable settles",
  );
  assert.equal(ended(), false, "the span does not end before settlement");

  // when: the thenable settles
  settle("counted settled");

  // then: the span ends on completion
  assert.equal(await result, "counted settled");
  assert.equal(finished(spanName).ended, true);
});

test("nested spans parent to the active span and restore it afterwards", () => {
  // given
  const tracer = new Tracer("relay.test.nesting");
  const order = [];

  // when
  const outerResult = tracer.span("outer", (outerSpan) => {
    assert.equal(trace.getSpan(context.active()), outerSpan);
    const innerResult = tracer.span("inner", (innerSpan) => {
      assert.equal(trace.getSpan(context.active()), innerSpan);
      order.push("inner");
      return "inner-value";
    });
    assert.equal(innerResult, "inner-value", "the inner span stays synchronous");
    assert.equal(
      trace.getSpan(context.active()),
      outerSpan,
      "the outer span is active again after the inner one",
    );
    order.push("outer");
    return "outer-value";
  });

  // then
  assert.equal(outerResult, "outer-value");
  assert.deepEqual(order, ["inner", "outer"]);

  const finishedSpans = exporter.getFinishedSpans();
  const outer = finished("outer");
  const inner = finished("inner");
  assert.ok(
    finishedSpans.indexOf(inner) < finishedSpans.indexOf(outer),
    "the inner span ends before the outer one",
  );
  assert.equal(
    inner.spanContext().traceId,
    outer.spanContext().traceId,
    "nested spans share the trace",
  );
  assert.equal(
    inner.parentSpanContext && inner.parentSpanContext.spanId,
    outer.spanContext().spanId,
    "the inner span parents to the outer one",
  );
  assert.equal(
    trace.getSpan(context.active()),
    undefined,
    "the context restores after the outer span",
  );
});

test("concurrent spans keep their own context across interleaving", async () => {
  // given
  const tracer = new Tracer("relay.test.concurrency");
  const tick = () => new Promise((resolve) => setImmediate(resolve));
  const seen = {};

  // when
  await Promise.all([
    tracer.span("concurrent-a", async (span) => {
      await tick();
      assert.equal(
        trace.getSpan(context.active()),
        span,
        "span a stays active while b runs",
      );
      seen.a = span;
      await tick();
      assert.equal(trace.getSpan(context.active()), span);
    }),
    tracer.span("concurrent-b", async (span) => {
      await tick();
      assert.equal(
        trace.getSpan(context.active()),
        span,
        "span b stays active while a runs",
      );
      seen.b = span;
      await tick();
      assert.equal(trace.getSpan(context.active()), span);
    }),
  ]);

  // then
  assert.notEqual(seen.a, seen.b, "each callback sees its own span");
  finished("concurrent-a");
  finished("concurrent-b");
  assert.equal(
    trace.getSpan(context.active()),
    undefined,
    "the context restores after both spans",
  );
});

test("captureMethod forwards apply calls, bound receivers, and span names", () => {
  // given
  const tracer = new Tracer("relay.test.capture");

  // when: apply with an argument array
  function subtract(a, b) {
    return a - b;
  }
  const captured = tracer.captureMethod(subtract);
  assert.equal(captured.apply(null, [10, 4]), 6);
  assert.equal(finished("subtract").name, "subtract", "the function name");

  // when: a bound receiver keeps its captured this
  const receiver = {
    factor: 3,
    scale(value) {
      return this.factor * value;
    },
  };
  const boundScale = receiver.scale.bind(receiver);
  const bound = tracer.captureMethod(boundScale);
  assert.equal(bound(5), 15);
  finished(boundScale.name);

  // when: an unnamed function falls back to "anonymous"
  const anonymous = tracer.captureMethod(function () {
    return "no-name";
  });
  assert.equal(anonymous(), "no-name");
  finished("anonymous");

  // when: an explicit name wins over the function name
  const renamed = tracer.captureMethod(subtract, { name: "math.subtract" });
  assert.equal(renamed(7, 2), 5);
  const renamedSpan = finished("math.subtract");
  assert.ok(
    !("name" in renamedSpan.attributes),
    "the wrapper name must not leak into span attributes",
  );
});

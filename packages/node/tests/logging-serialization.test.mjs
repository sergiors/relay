// Bounded, descriptor-safe JSON serialization: structured exception records
// for Error (with cause and AggregateError details) and non-Error thrown
// values, markers for circular/BigInt/undefined/non-finite/accessor/toJSON
// values without invoking them, verified native stack frames only (custom
// prepareStackTrace hooks and hostile stack accessors are skipped), depth
// bounds, and the fallback lines emitted when a record cannot be built,
// sized, or written.
import assert from "node:assert/strict";
import { test } from "node:test";

import { JsonFormatter, Logger } from "../dist/logging.js";
import { captureRecords, oneRecord } from "./helpers/logging.mjs";

function freshLogger(service) {
  return new Logger({ service, level: "DEBUG" });
}

test("an Error exception carries type, message, and stacktrace", async () => {
  const log = freshLogger("errors");

  const records = await captureRecords(() => {
    log.exception("failed-work", new TypeError("bad type"));
    log.exception(new Error("standalone"));
  });

  const typed = oneRecord(records, "failed-work");
  assert.equal(typed.level, "ERROR");
  assert.equal(typed.exception.type, "TypeError");
  assert.equal(typed.exception.message, "bad type");
  assert.ok(
    typed.exception.stacktrace.startsWith("TypeError: bad type"),
    `stacktrace = ${JSON.stringify(typed.exception.stacktrace)}`,
  );

  // the single-argument form uses the error's own message
  const standalone = oneRecord(records, "standalone");
  assert.equal(standalone.level, "ERROR");
  assert.equal(standalone.exception.type, "Error");
  assert.equal(standalone.exception.message, "standalone");
});

test("a cause chain appends to the stacktrace", async () => {
  const log = freshLogger("causes");

  const records = await captureRecords(() => {
    log.exception("outer-cause", new Error("outer", { cause: new RangeError("inner") }));
    log.exception("string-cause", new Error("host", { cause: "raw-string" }));
    const selfCaused = new Error("self");
    selfCaused.cause = selfCaused;
    log.exception("circular-cause", selfCaused);
  });

  assert.ok(
    oneRecord(records, "outer-cause").exception.stacktrace.includes(
      "Caused by: RangeError: inner",
    ),
  );
  assert.ok(
    oneRecord(records, "string-cause").exception.stacktrace.includes(
      "Caused by: string: raw-string",
    ),
  );
  assert.ok(
    oneRecord(records, "circular-cause").exception.stacktrace.includes(
      "Caused by: [circular cause]",
    ),
  );
});

test("an AggregateError carries its bounded errors list", async () => {
  const log = freshLogger("aggregates");
  const aggregate = new AggregateError(
    [new TypeError("first"), "plain"],
    "many failures",
  );

  const [record] = await captureRecords(() => log.exception("aggregate", aggregate));
  assert.equal(record.exception.type, "AggregateError");
  assert.equal(record.exception.message, "many failures");
  assert.equal(record.exception.errors.length, 2);
  const [first] = record.exception.errors;
  assert.equal(first.type, "TypeError");
  assert.equal(first.message, "first");
  assert.ok(first.stacktrace.startsWith("TypeError: first"));
  assert.equal(record.exception.errors[1], "plain");
});

test("non-Error thrown values become structured exception records", async () => {
  const log = freshLogger("non-errors");

  const records = await captureRecords(() => {
    log.exception("thrown-string", "just a string");
    log.exception("thrown-null", null);
    log.exception("thrown-object", { code: 7 });
  });

  assert.deepEqual(oneRecord(records, "thrown-string").exception, {
    type: "string",
    message: "just a string",
    stacktrace: "string: just a string",
  });
  assert.deepEqual(oneRecord(records, "thrown-null").exception, {
    type: "NoneType",
    message: "null",
    stacktrace: "NoneType: null",
  });
  assert.deepEqual(oneRecord(records, "thrown-object").exception, {
    type: "Object",
    message: '{"code":7}',
    stacktrace: 'Object: {"code":7}',
  });
});

test("an Error attribute serializes like an exception", async () => {
  const log = freshLogger("error-attributes");

  const [record] = await captureRecords(() =>
    log.info("attribute-error", { err: new RangeError("range") }),
  );
  assert.equal(record.attributes.err.type, "RangeError");
  assert.equal(record.attributes.err.message, "range");
  assert.ok(record.attributes.err.stacktrace.startsWith("RangeError: range"));
});

test("a custom Error.prepareStackTrace hook is never called while formatting", () => {
  // given a custom hook installed on the shared Error constructor: the
  // formatter must treat the stack as unverifiable instead of invoking it
  const formatter = new JsonFormatter();
  let hookCalls = 0;
  const previousHook = Error.prepareStackTrace;
  Error.prepareStackTrace = () => {
    hookCalls += 1;
    return "hooked";
  };

  let line;
  try {
    line = formatter.format({
      message: "hooked-error",
      level: "ERROR",
      exception: new TypeError("hooked-path"),
    });
  } finally {
    Error.prepareStackTrace = previousHook;
  }

  assert.equal(hookCalls, 0, "Error.prepareStackTrace must not be called");
  const record = JSON.parse(line);
  assert.equal(record.exception.type, "TypeError");
  assert.equal(record.exception.message, "hooked-path");
  // without verifiable native frames the exception keeps type and message
  assert.equal(record.exception.stacktrace, "TypeError: hooked-path");
});

test("a hostile stack accessor is never invoked while formatting", () => {
  const formatter = new JsonFormatter();
  let getterCalls = 0;
  const error = new Error("own-getter");
  Object.defineProperty(error, "stack", {
    configurable: true,
    get() {
      getterCalls += 1;
      return "forbidden";
    },
  });

  const line = formatter.format({
    message: "getter-error",
    level: "ERROR",
    exception: error,
  });

  assert.equal(getterCalls, 0, "a custom stack accessor must not run");
  const record = JSON.parse(line);
  assert.equal(record.exception.type, "Error");
  assert.equal(record.exception.message, "own-getter");
  assert.equal(record.exception.stacktrace, "Error: own-getter");
});

test("an oversized own stack string is truncated at the stacktrace bound", () => {
  const formatter = new JsonFormatter();
  const error = new Error("oversized");
  Object.defineProperty(error, "stack", {
    configurable: true,
    value: "x".repeat(70_000),
  });

  const record = JSON.parse(
    formatter.format({
      message: "oversized-error",
      level: "ERROR",
      exception: error,
    }),
  );

  const stacktrace = record.exception.stacktrace;
  assert.ok(stacktrace.endsWith("\n[stacktrace truncated]"), "marker missing");
  assert.ok(
    stacktrace.length <= 64 * 1024,
    `stacktrace length ${stacktrace.length} must stay within 64 KiB`,
  );
});

test("unsupported values are marked instead of thrown", async () => {
  const log = freshLogger("unsupported");

  const circular = { kind: "loop" };
  circular.self = circular;
  class Widget {}

  const [record] = await captureRecords(() =>
    log.info("unsupported-values", {
      circular,
      big: 10n,
      missing: undefined,
      notANumber: NaN,
      infinite: Infinity,
      widget: new Widget(),
      when: new Date("2024-01-02T03:04:05.678Z"),
      brokenDate: new Date("not-a-date"),
    }),
  );

  assert.deepEqual(record.attributes, {
    circular: { kind: "loop", self: { $circular: true } },
    big: { $unsupported: "BigInt" },
    missing: { $unsupported: "undefined" },
    notANumber: { $unsupported: "float" },
    infinite: { $unsupported: "float" },
    widget: { $unsupported: "Widget" },
    when: "2024-01-02T03:04:05.678Z",
    brokenDate: { $unsupported: "Date" },
  });
});

test("accessor properties and toJSON are never invoked", async () => {
  const log = freshLogger("descriptors");

  let getterCalls = 0;
  let toJSONCalls = 0;
  const withGetter = Object.defineProperty(
    { present: 1 },
    "computed",
    {
      enumerable: true,
      get() {
        getterCalls += 1;
        return "evaluated";
      },
    },
  );
  const withToJSON = {
    data: 1,
    toJSON() {
      toJSONCalls += 1;
      return "serialized";
    },
  };
  const hidden = {};
  Object.defineProperty(hidden, "secret", { value: 1, enumerable: false });
  hidden.shown = 2;
  hidden[Symbol("keyed")] = 3;

  const [record] = await captureRecords(() =>
    log.info("descriptor-values", { withGetter, withToJSON, hidden }),
  );

  assert.equal(getterCalls, 0, "a getter must never run during serialization");
  assert.equal(toJSONCalls, 0, "toJSON must never run during serialization");
  assert.deepEqual(record.attributes.withGetter, {
    present: 1,
    computed: { $unsupported: "accessor" },
  });
  assert.deepEqual(record.attributes.withToJSON, {
    data: 1,
    toJSON: { $unsupported: "Function" },
  });
  assert.deepEqual(
    record.attributes.hidden,
    { shown: 2 },
    "non-enumerable and symbol keys are skipped",
  );

  // the attributes object itself may be an accessor-carrying holder
  const holder = Object.defineProperty({}, "direct", {
    enumerable: true,
    get() {
      getterCalls += 1;
      return "direct";
    },
  });
  const [direct] = await captureRecords(() => log.info("descriptor-holder", holder));
  assert.deepEqual(direct.attributes, { direct: { $unsupported: "accessor" } });
  assert.equal(getterCalls, 0, "the holder's getter must never run either");
});

test("depth beyond the bound is truncated", async () => {
  const log = freshLogger("depth");
  let deep = { end: true };
  for (let level = 0; level < 25; level += 1) {
    deep = { k: deep };
  }

  const [record] = await captureRecords(() => log.info("deep-record", { deep }));

  let cursor = record.attributes.deep;
  let levels = 0;
  while (cursor !== undefined && cursor.k !== undefined) {
    cursor = cursor.k;
    levels += 1;
  }
  assert.equal(levels, 15, "serialization stops at the documented depth");
  assert.deepEqual(cursor, { $truncated: true });
});

test("stack_info follows the stackInfo option", async () => {
  const log = freshLogger("stack-info");

  const records = await captureRecords(() => {
    log.info("with-stack %s", "x", { stackInfo: true, attributes: { a: 1 } });
    log.info("without-stack");
  });

  const withStack = oneRecord(records, "with-stack x");
  assert.deepEqual(withStack.attributes, { a: 1 });
  assert.equal(typeof withStack.stack_info, "string");
  assert.match(withStack.stack_info, /^\s+at /, "frames follow the header");
  assert.ok(withStack.stack_info.includes("\n"), "stack_info carries frames");
  assert.ok(
    !("stack_info" in oneRecord(records, "without-stack")),
    "stack_info only appears when requested",
  );
});

test("a record that cannot be built or sized falls back safely", async () => {
  const log = freshLogger("fallbacks");

  const records = await captureRecords(() => {
    log.info("throwing-attributes", () => {
      throw new TypeError("attributes exploded");
    });
    log.info(() => {
      throw new RangeError("message exploded");
    });
    log.info("oversized", { blob: "x".repeat(1_100_000) });
  });

  assert.equal(records.length, 3);

  const badAttributes = records.find((record) => record.error === "TypeError");
  assert.ok(badAttributes, "a throwing attribute factory must emit a fallback");
  assert.equal(badAttributes.level, "ERROR");
  assert.equal(badAttributes.message, "log record could not be serialized");
  assert.equal(badAttributes.service, "fallbacks");

  const badMessage = records.find(
    (record) => record.error === "RangeError" && record.level === "ERROR",
  );
  assert.ok(badMessage, `no fallback record for the throwing message: ${JSON.stringify(records.map((r) => r.message))}`);
  assert.equal(badMessage.message, "log record could not be serialized");

  const oversized = records.find((record) => record.error === "RangeError" && record.level !== "ERROR");
  assert.ok(oversized, "an oversized record must emit the size fallback");
  assert.equal(oversized.message, "log record could not be serialized");
});

test("a failing output stream never replaces the handler outcome", () => {
  const log = freshLogger("output-failure");
  const original = process.stderr.write;

  process.stderr.write = () => {
    throw new Error("stream down");
  };
  try {
    assert.doesNotThrow(() => log.error("survives-stream-failure"));
  } finally {
    process.stderr.write = original;
  }
});

// Regression (independent review): an invalid explicit per-call options value
// must be contained exactly like a failing lazy factory — never thrown into
// the caller, never emitted as a normal record.
test("an invalid per-call options value is contained like a failing factory", async () => {
  const log = freshLogger("invalid-options");

  const records = await captureRecords(() => {
    // an array through the trailing options of a formatted call
    assert.doesNotThrow(
      () => log.info("item %s", "x", { attributes: [] }),
      "an array options.attributes must never throw into the caller",
    );
    // a non-object through the extra alias of the same shape
    assert.doesNotThrow(
      () => log.info("extra %s", "y", { extra: "not-a-map" }),
      "a non-object options.extra must never throw into the caller",
    );
    // a failing lazy factory — the documented containment reference shape
    assert.doesNotThrow(
      () =>
        log.info("factory-containment", () => {
          throw new TypeError("factory exploded");
        }),
      "a failing lazy factory must never throw into the caller",
    );
  });

  assert.equal(
    records.length,
    3,
    `each contained call emits exactly one record: ${JSON.stringify(records)}`,
  );

  // every contained call collapses to the same fallback a failing lazy
  // factory produces — including the cause's type name — so the original
  // messages never reach the output
  const expected = {
    level: "ERROR",
    message: "log record could not be serialized",
    error: "TypeError",
    service: "invalid-options",
  };
  for (const record of records) {
    assert.deepEqual(
      {
        level: record.level,
        message: record.message,
        error: record.error,
        service: record.service,
      },
      expected,
    );
  }
});

// Regression (independent review): an enabled stackInfo must start at the
// application caller and carry no frames from inside the SDK, while
// preserving the caller frame that produced the record's location.
test("an enabled stackInfo starts at the application caller without SDK frames", async () => {
  const log = freshLogger("stack-filter");

  const [record] = await captureRecords(() =>
    applicationCaller(log, "stack-filter-call"),
  );
  assert.equal(record.message, "stack-filter-call");
  assert.equal(typeof record.stack_info, "string");

  const frames = record.stack_info
    .split("\n")
    .filter((line) => line.trim() !== "");

  // the stack starts at the application caller in the application's own file
  assert.match(frames[0], /^\s+at\s/);
  assert.ok(
    frames[0].includes("applicationCaller"),
    `stack_info must start at the application caller; first frame: ${frames[0]}`,
  );
  assert.ok(
    frames[0].includes("logging-serialization.test.mjs"),
    `stack_info must start in the application's file; first frame: ${frames[0]}`,
  );

  // the caller frame is preserved with the exact line reported as location
  assert.match(
    record.location,
    /^logging-serialization\.test\.applicationCaller:\d+$/,
  );
  const locationLine = record.location.slice(record.location.lastIndexOf(":") + 1);
  assert.ok(
    frames[0].includes(`:${locationLine}:`),
    `the caller frame must carry the location line ${locationLine}; first frame: ${frames[0]}`,
  );

  // and no frame comes from inside the SDK
  for (const marker of ["logging.js", "Logger.write", "Logger.info"]) {
    assert.ok(
      !record.stack_info.includes(marker),
      `stack_info must not carry the SDK frame ${marker}:\n${record.stack_info}`,
    );
  }
});

function applicationCaller(log, message) {
  // stackInfo is a trailing option, so it follows a formatted placeholder
  log.info("%s", message, {
    attributes: { caller: "application" },
    stackInfo: true,
  });
}

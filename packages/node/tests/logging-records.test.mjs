// Observable Logger record emission: the Phase 3 structured logging contract
// for direct attributes, warn, LOG_LEVEL precedence, lazy evaluation,
// level filtering without stack/OTel work, reserved field nesting, and the
// core JSON record schema (keys, timestamp, source location).
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

import { Logger } from "../dist/logging.js";
import {
  captureRecords,
  oneRecord,
  withEnv,
} from "./helpers/logging.mjs";

const require = createRequire(import.meta.url);
const CORE_KEYS = [
  "timestamp",
  "level",
  "logger",
  "message",
  "location",
  "service",
];

test("a filtered record captures no stack and performs no OTel lookup", async () => {
  // given a logger whose threshold hides DEBUG, with the optional OpenTelemetry
  // API still unloaded in this fresh process
  const log = new Logger({ service: "filter-probe", level: "INFO" });
  assert.equal(
    Object.keys(require.cache).some((key) =>
      key.includes("@opentelemetry/api"),
    ),
    false,
    "the OpenTelemetry API must not be loaded before the first emitted record",
  );

  // when every Error construction (the stack capture) is counted
  const realError = globalThis.Error;
  let stackCaptures = 0;
  class CountingError extends realError {
    constructor(...args) {
      stackCaptures += 1;
      super(...args);
    }
  }
  globalThis.Error = CountingError;
  let records;
  try {
    records = await captureRecords(() => {
      log.debug("filtered %d", 1);
    });
  } finally {
    globalThis.Error = realError;
  }

  // then the call emits nothing, captures no stack, and never loads the API
  assert.deepEqual(records, []);
  assert.equal(stackCaptures, 0, "a filtered record must not capture a stack");
  assert.equal(
    Object.keys(require.cache).some((key) => key.includes("@opentelemetry/api")),
    false,
    "a filtered record must not reach the OpenTelemetry lookup",
  );

  // when an enabled record runs, the API loads once (by design) and every
  // per-record context lookup is counted
  await captureRecords(() => log.info("enabled"));
  const api = require("@opentelemetry/api");
  let otelLookups = 0;
  const targets = [
    [api.context, "active"],
    [api.trace, "getActiveSpan"],
    [api.trace, "getSpan"],
    [api.trace, "getSpanContext"],
    [api.trace, "isSpanContextValid"],
  ];
  const patched = [];
  for (const [target, name] of targets) {
    const original = target?.[name];
    if (typeof original !== "function") continue;
    const hadOwn = Object.hasOwn(target, name);
    target[name] = (...args) => {
      otelLookups += 1;
      return original.apply(target, args);
    };
    patched.push([target, name, original, hadOwn]);
  }
  try {
    await captureRecords(() => {
      log.debug("filtered-again");
      log.debug(() => {
        throw new Error("a filtered record must not evaluate its message");
      });
    });
    assert.equal(otelLookups, 0, "a filtered record must not look up a span");
    await captureRecords(() => log.info("enabled-again"));
    assert.ok(otelLookups >= 1, "an enabled record must read the active span");
  } finally {
    for (const [target, name, original] of patched) {
      delete target[name];
      target[name] = original;
    }
  }
});

test("a direct attributes object is nested under attributes", async () => {
  // given
  const log = new Logger({ service: "direct", level: "INFO" });

  // when
  const records = await captureRecords(() => {
    log.info("user created", { user_id: "u-42", count: 3 });
  });

  // then the message stays the message and the plain object becomes the
  // structured attributes, not format arguments
  const record = oneRecord(records, "user created");
  assert.deepEqual(record.attributes, { user_id: "u-42", count: 3 });
  assert.equal(record.level, "INFO");
});

test("placeholders format arguments and a trailing options object wins", async () => {
  const log = new Logger({ service: "format", level: "INFO" });

  const records = await captureRecords(() => {
    log.info("User %s created", "u-42", { attributes: { user_id: "u-42" } });
    log.info("retry %d of %d", 2, 3);
  });

  const created = oneRecord(records, "User u-42 created");
  assert.deepEqual(created.attributes, { user_id: "u-42" });
  oneRecord(records, "retry 2 of 3");
});

test("reserved field names stay nested and cannot replace record metadata", async () => {
  const log = new Logger({ service: "reserved-svc", level: "INFO" });

  const records = await captureRecords(() => {
    log.info("reserved", {
      level: "nope",
      message: "other",
      service: "spoof",
      trace_id: "spoof",
      timestamp: "spoof",
      logger: "spoof",
    });
  });

  const record = oneRecord(records, "reserved");
  assert.equal(record.level, "INFO");
  assert.equal(record.message, "reserved");
  assert.equal(record.service, "reserved-svc");
  assert.equal(record.logger, "relay_runtime.reserved-svc");
  assert.ok(!("trace_id" in record), "spoofed trace_id must stay nested");
  assert.deepEqual(record.attributes, {
    level: "nope",
    message: "other",
    service: "spoof",
    trace_id: "spoof",
    timestamp: "spoof",
    logger: "spoof",
  });
});

test("a lone plain object with option-like field names stays nested", async () => {
  const log = new Logger({ service: "nested", level: "INFO" });

  const records = await captureRecords(() => {
    log.info("option-shaped", {
      attributes: "literal",
      extra: 1,
      stackInfo: false,
    });
  });

  const record = oneRecord(records, "option-shaped");
  assert.deepEqual(record.attributes, {
    attributes: "literal",
    extra: 1,
    stackInfo: false,
  });
  assert.ok(!("stack_info" in record), "stack_info only follows stackInfo");
});

test("warn emits at WARNING and is filtered below the threshold", async () => {
  const records = await captureRecords(() => {
    new Logger({ service: "warn-enabled", level: "WARNING" }).warn(
      "careful",
      { reason: "quota" },
    );
  });
  const record = oneRecord(records, "careful");
  assert.equal(record.level, "WARNING");
  assert.deepEqual(record.attributes, { reason: "quota" });

  const silent = new Logger({ service: "warn-silent", level: "ERROR" });
  const filtered = await captureRecords(() => {
    silent.warn("suppressed");
  });
  assert.deepEqual(filtered, []);
  assert.equal(silent.isEnabledFor("WARNING"), false);

  const noisy = new Logger({ service: "warn-noisy", level: "DEBUG" });
  assert.equal(noisy.isEnabledFor("warn"), true);
});

test("an unset LOG_LEVEL keeps the documented INFO default", async () => {
  await withEnv("LOG_LEVEL", undefined, async () => {
    const log = new Logger({ service: "env-default" });
    const records = await captureRecords(() => {
      log.debug("env-debug");
      log.info("env-info");
    });
    assert.deepEqual(recordsWithMessages(records, ["env-debug"]), []);
    assert.equal(records.length, 1);
    assert.equal(log.isEnabledFor("DEBUG"), false);
  });
});

test("LOG_LEVEL supplies the default threshold", async () => {
  await withEnv("LOG_LEVEL", "DEBUG", async () => {
    const log = new Logger({ service: "env-debug" });
    const records = await captureRecords(() => log.debug("env-debug"));
    assert.equal(records.length, 1);
    assert.equal(log.isEnabledFor("DEBUG"), true);
  });
});

test("an explicit constructor level wins over LOG_LEVEL", async () => {
  await withEnv("LOG_LEVEL", "DEBUG", async () => {
    const options = new Logger({ service: "env-override", level: "INFO" });
    const positional = new Logger("env-override-positional", "INFO");
    const records = await captureRecords(() => {
      options.debug("override-options");
      positional.debug("override-positional");
    });
    assert.deepEqual(records, []);
    // setLevel still changes the threshold after construction
    options.setLevel("DEBUG");
    const after = await captureRecords(() => options.debug("override-after"));
    assert.equal(after.length, 1);
  });
});

test("LOG_LEVEL accepts the documented names and rejects invalid values", async () => {
  await withEnv("LOG_LEVEL", "warn", async () => {
    const log = new Logger({ service: "env-warn" });
    assert.equal(log.isEnabledFor("WARNING"), true);
    assert.equal(log.isEnabledFor("ERROR"), true);
    assert.equal(log.isEnabledFor("DEBUG"), false);
  });
  await withEnv("LOG_LEVEL", "NOPE", async () => {
    assert.throws(() => new Logger({ service: "env-invalid" }), RangeError);
  });
});

test("lazy messages and attributes run only when the level is enabled", async () => {
  const silent = new Logger({ service: "lazy-silent", level: "ERROR" });
  let messageCalls = 0;
  let attributeCalls = 0;
  const filtered = await captureRecords(() => {
    silent.debug(
      () => {
        messageCalls += 1;
        return "never";
      },
      () => {
        attributeCalls += 1;
        return { never: true };
      },
    );
  });
  assert.deepEqual(filtered, []);
  assert.equal(messageCalls, 0, "a filtered record must not build its message");
  assert.equal(attributeCalls, 0, "a filtered record must not build fields");

  const eager = new Logger({ service: "lazy-eager", level: "DEBUG" });
  const records = await captureRecords(() => {
    eager.debug(
      () => {
        messageCalls += 1;
        return "lazy %s";
      },
      "message",
    );
    eager.info("lazy-attributes", () => {
      attributeCalls += 1;
      return { computed: 41 + 1 };
    });
  });
  oneRecord(records, "lazy message");
  assert.deepEqual(oneRecord(records, "lazy-attributes").attributes, {
    computed: 42,
  });
  assert.equal(messageCalls, 1, "the message callback runs once when enabled");
  assert.equal(attributeCalls, 1, "the attribute callback runs once when enabled");
});

test("every record carries the core schema, a fresh timestamp, and location", async () => {
  const log = new Logger({ service: "schema", level: "INFO" });
  const before = Date.now();
  const records = await captureRecords(() => emitFromHelper(log));
  const after = Date.now();

  const record = oneRecord(records, "schema-probe");
  assert.deepEqual(Object.keys(record), [...CORE_KEYS, "attributes"]);
  assert.match(
    record.timestamp,
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/,
  );
  const stamped = Date.parse(record.timestamp);
  assert.ok(stamped >= before && stamped <= after, "timestamp is not current");
  assert.equal(record.level, "INFO");
  assert.equal(record.logger, "relay_runtime.schema");
  assert.equal(record.service, "schema");
  assert.deepEqual(record.attributes, { probe: true });

  // the location is this file, the emitting call
  assert.match(record.location, /^logging-records\.test\..+:\d+$/);
  const line = Number(record.location.slice(record.location.lastIndexOf(":") + 1));
  const source = readFileSync(fileURLToPath(import.meta.url), "utf8").split("\n");
  assert.match(source[line - 1], /log\.info\("schema-probe"/);
});

test("a record with no attributes omits the attributes key", async () => {
  const log = new Logger({ service: "bare", level: "INFO" });
  const records = await captureRecords(() => log.info("bare-record"));
  const record = oneRecord(records, "bare-record");
  assert.deepEqual(Object.keys(record), CORE_KEYS);
});

function emitFromHelper(log) {
  log.info("schema-probe", { probe: true });
}

function recordsWithMessages(records, messages) {
  return records.filter((record) => messages.includes(record.message));
}

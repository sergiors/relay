// Logger construction and level configuration: service resolution order
// (explicit, OTEL_SERVICE_NAME, active invocation app, standalone), fail-fast
// rejection of invalid services and levels, numeric level thresholds, and the
// log()/setLevel/isEnabledFor level contract. LOG_LEVEL precedence over the
// default is pinned in logging-records.test.mjs.
import assert from "node:assert/strict";
import { test } from "node:test";

import { runInvocation } from "../../../internal/runtime/node/logging-context.mjs";
import { Logger } from "../dist/logging.js";
import { captureRecords, oneRecord, withEnv } from "./helpers/logging.mjs";

test("the service resolves from the explicit value, environment, or invocation", async () => {
  // given explicit services, positional and by options
  const positional = new Logger("  users  ");
  const options = new Logger({ service: "orders" });

  // then the explicit value wins and is trimmed
  assert.equal(positional.service, "users");
  assert.equal(positional.name, "relay_runtime.users");
  assert.equal(options.service, "orders");
  assert.equal(options.name, "relay_runtime.orders");

  // when only the environment carries a service
  await withEnv("OTEL_SERVICE_NAME", "env-service", async () => {
    // then the environment wins over the standalone default...
    assert.equal(new Logger().service, "env-service");
    // ...but an explicit service still wins over the environment
    assert.equal(new Logger({ service: "explicit" }).service, "explicit");
  });

  // when neither carries one, the logger stands alone
  await withEnv("OTEL_SERVICE_NAME", undefined, async () => {
    assert.equal(new Logger().service, "standalone");
    assert.equal(new Logger({ level: "INFO" }).service, "standalone");
  });

  // when a Relay invocation is active, its app supplies the service
  await withEnv("OTEL_SERVICE_NAME", undefined, async () => {
    let constructed;
    await runInvocation(
      { app: "invoked-app", handler: "index.run", requestId: "r-svc", attempt: 1 },
      () => {
        constructed = new Logger();
      },
    );
    assert.equal(constructed.service, "invoked-app");
    assert.equal(constructed.name, "relay_runtime.invoked-app");
  });
});

test("an invalid service fails construction", () => {
  assert.throws(() => new Logger(""), RangeError);
  assert.throws(() => new Logger("   "), RangeError);
  assert.throws(() => new Logger(42), TypeError);
  assert.throws(() => new Logger({ service: 7 }), TypeError);
});

test("an invalid constructor level fails fast", () => {
  assert.throws(() => new Logger({ level: "NOPE" }), RangeError);
  assert.throws(() => new Logger("svc", "nope"), RangeError);
  assert.throws(() => new Logger({ level: NaN }), TypeError);
  assert.throws(() => new Logger({ level: Infinity }), TypeError);
  assert.throws(() => new Logger({ level: {} }), TypeError);
});

test("the constructor level wins over LOG_LEVEL", async () => {
  await withEnv("LOG_LEVEL", "DEBUG", async () => {
    // given an environment that would enable DEBUG
    // when constructed with an explicit level
    const byOptions = new Logger({ level: "INFO" });
    const byPosition = new Logger("svc", "INFO");

    // then both loggers keep the constructor threshold
    assert.equal(byOptions.isEnabledFor("DEBUG"), false);
    assert.equal(byPosition.isEnabledFor("DEBUG"), false);

    // and with no constructor level the environment applies
    assert.equal(new Logger().isEnabledFor("DEBUG"), true);
  });
});

test("numeric levels act as exact thresholds", () => {
  const log = new Logger({ service: "numeric", level: 15 });

  assert.equal(log.isEnabledFor("DEBUG"), false);
  assert.equal(log.isEnabledFor("INFO"), true);
  assert.equal(log.isEnabledFor(15), true);
  assert.equal(log.isEnabledFor(14), false);
  assert.throws(() => log.isEnabledFor("NOPE"), RangeError);
});

test("log applies level labels and respects the threshold", async () => {
  const log = new Logger({ service: "levels", level: "INFO" });

  const records = await captureRecords(() => {
    log.log(15, "filtered-numeric");
    log.debug("filtered-debug");
    log.log(20, "at-info");
    log.log("warn", "via-warn-name");
    log.log(50, "via-critical-number");
    log.critical("via-critical");
    log.warning("via-warning");
  });

  // then only the enabled records appear, each with its documented label
  assert.deepEqual(
    records.map((record) => [record.level, record.message]),
    [
      ["INFO", "at-info"],
      ["WARNING", "via-warn-name"],
      ["CRITICAL", "via-critical-number"],
      ["CRITICAL", "via-critical"],
      ["WARNING", "via-warning"],
    ],
  );
  assert.throws(() => log.log("NOPE", "bad-level"), RangeError);

  // when the threshold moves, the previously filtered level emits
  log.setLevel("DEBUG");
  const after = await captureRecords(() => log.debug("now-visible"));
  assert.equal(after.length, 1);
  assert.equal(oneRecord(after, "now-visible").level, "DEBUG");
});

// Scoped and persistent log attributes: appendKeys/removeKeys instance state
// across scopes, invocations, and loggers, the documented merge precedence
// (persistent < temporary < per-call), and withContextKeys semantics — sync
// and async return passthrough, nesting, concurrency, error propagation, and
// validation. The former withContext/appendContextKeys aliases are gone.
import assert from "node:assert/strict";
import { test } from "node:test";

import { runInvocation } from "../../../internal/runtime/node/logging-context.mjs";
import { Logger } from "../dist/logging.js";
import {
  captureRecords,
  oneRecord,
  recordsWithMessage,
  sleep,
} from "./helpers/logging.mjs";

function freshLogger(service) {
  return new Logger({ service, level: "INFO" });
}

test("persistent keys merge into records, override each other, and can be removed", async () => {
  const log = freshLogger("persistent");

  const records = await captureRecords(() => {
    log.appendKeys({ tenant: "acme", region: "eu" });
    log.info("persist-both");
    log.appendKeys({ tenant: "globex" });
    log.info("persist-overridden");
    log.removeKeys("tenant");
    log.info("persist-removed-one");
    log.removeKeys("region", "never-present");
    log.info("persist-removed-all");
  });

  assert.deepEqual(oneRecord(records, "persist-both").attributes, {
    tenant: "acme",
    region: "eu",
  });
  // appending an existing key replaces its value
  assert.deepEqual(oneRecord(records, "persist-overridden").attributes, {
    tenant: "globex",
    region: "eu",
  });
  // removeKeys accepts bare names, lists, and mixes of both
  assert.deepEqual(
    oneRecord(records, "persist-removed-one").attributes,
    { region: "eu" },
    "only the removed key disappears",
  );
  assert.equal(
    oneRecord(records, "persist-removed-all").attributes,
    undefined,
    "removing the remaining and absent keys leaves nothing",
  );

  const mixed = freshLogger("persistent-mixed");
  mixed.appendKeys({ a: 1, b: 2, c: 3, d: 4 });
  mixed.removeKeys("a", ["b", "c"]);
  const [remaining] = await captureRecords(() => mixed.info("persist-mixed"));
  assert.deepEqual(remaining.attributes, { d: 4 });
});

test("appendKeys snapshots the fields at call time", async () => {
  const log = freshLogger("snapshot");
  const fields = { stable: "before" };

  log.appendKeys(fields);
  fields.stable = "mutated";
  fields.added = "later";

  const [record] = await captureRecords(() => log.info("snapshot-key"));
  assert.deepEqual(record.attributes, { stable: "before" });
});

test("per-call attributes win over scoped fields, which win over persistent keys", async () => {
  const log = freshLogger("precedence");
  log.appendKeys({ source: "persistent", persistentOnly: true });

  const records = await captureRecords(() =>
    log.withContextKeys({ source: "temporary", temporaryOnly: true }, () => {
      log.info("precedence-inside", { source: "call", callOnly: true });
      log.info("precedence-scoped");
    }),
  );

  assert.deepEqual(oneRecord(records, "precedence-inside").attributes, {
    source: "call",
    persistentOnly: true,
    temporaryOnly: true,
    callOnly: true,
  });
  assert.deepEqual(oneRecord(records, "precedence-scoped").attributes, {
    source: "temporary",
    persistentOnly: true,
    temporaryOnly: true,
  });
});

test("temporary scoped keys apply only to their logger instance and scope", async () => {
  const scoped = freshLogger("scope-a");
  const bystander = freshLogger("scope-b");

  const records = await captureRecords(() => {
    scoped.withContextKeys({ operation: "only-a" }, () => {
      scoped.info("scoped-a");
      bystander.info("bystander-b");
    });
    scoped.info("after-scope-a");
  });

  assert.deepEqual(oneRecord(records, "scoped-a").attributes, {
    operation: "only-a",
  });
  assert.equal(
    oneRecord(records, "bystander-b").attributes,
    undefined,
    "another logger instance must not observe the scope",
  );
  assert.equal(
    oneRecord(records, "after-scope-a").attributes,
    undefined,
    "the scope must end when the callback returns",
  );
});

test("withContextKeys returns synchronous and asynchronous results", async () => {
  const log = freshLogger("returns");
  const returns = [];

  const records = await captureRecords(async () => {
    returns.push(
      log.withContextKeys({ variant: "sync" }, () => {
        log.info("return-sync");
        return 42;
      }),
    );
    returns.push(
      await log.withContextKeys({ variant: "async" }, async () => {
        await sleep(1);
        log.info("return-async");
        return "async-value";
      }),
    );
  });

  assert.deepEqual(returns, [42, "async-value"]);
  assert.deepEqual(
    records.map((record) => [record.message, record.attributes.variant]),
    [
      ["return-sync", "sync"],
      ["return-async", "async"],
    ],
  );
});

test("nested scopes restore outward state and return inner results", async () => {
  const log = freshLogger("nested");

  let result;
  const during = await captureRecords(() => {
    result = log.withContextKeys({ layer: "outer", outerOnly: "1" }, () => {
      const inner = log.withContextKeys(
        { layer: "inner", innerOnly: "2" },
        () => {
          log.info("nested-inner");
          return "inner-result";
        },
      );
      log.info("nested-outer");
      return inner;
    });
  });
  assert.equal(result, "inner-result");
  assert.deepEqual(oneRecord(during, "nested-inner").attributes, {
    layer: "inner",
    outerOnly: "1",
    innerOnly: "2",
  });
  assert.deepEqual(oneRecord(during, "nested-outer").attributes, {
    layer: "outer",
    outerOnly: "1",
  });

  const after = await captureRecords(() => log.info("nested-after"));
  assert.equal(
    oneRecord(after, "nested-after").attributes,
    undefined,
    "no scoped keys escape the root",
  );
});

test("nested scopes merge their fields while active", async () => {
  const log = freshLogger("nested-merge");
  log.appendKeys({ persistent: true });

  const records = await captureRecords(() =>
    log.withContextKeys({ layer: "outer", outerOnly: "1" }, () => {
      log.info("merge-outer");
      log.withContextKeys({ layer: "inner", innerOnly: "2" }, () => {
        log.info("merge-inner");
      });
      log.info("merge-outer-again");
    }),
  );

  assert.deepEqual(oneRecord(records, "merge-outer").attributes, {
    persistent: true,
    layer: "outer",
    outerOnly: "1",
  });
  assert.deepEqual(oneRecord(records, "merge-inner").attributes, {
    persistent: true,
    layer: "inner",
    outerOnly: "1",
    innerOnly: "2",
  });
  assert.deepEqual(oneRecord(records, "merge-outer-again").attributes, {
    persistent: true,
    layer: "outer",
    outerOnly: "1",
  });
});

test("concurrent scopes stay isolated across async interleaving", async () => {
  const log = freshLogger("concurrent");

  const records = await captureRecords(() =>
    Promise.all([
      log.withContextKeys({ lane: "slow" }, async () => {
        await sleep(15);
        log.info("lane-slow");
        return "slow";
      }),
      log.withContextKeys({ lane: "fast" }, async () => {
        log.info("lane-fast");
        await sleep(25);
        log.info("lane-fast-after-wait");
        return "fast";
      }),
    ]),
  );

  assert.deepEqual(oneRecord(records, "lane-slow").attributes, { lane: "slow" });
  assert.deepEqual(oneRecord(records, "lane-fast").attributes, { lane: "fast" });
  assert.deepEqual(
    oneRecord(records, "lane-fast-after-wait").attributes,
    { lane: "fast" },
    "a scope must survive awaits inside its callback",
  );
  assert.equal(recordsWithMessage(records, "lane-slow").length, 1);
});

test("a throwing scope callback propagates and leaks no fields", async () => {
  const log = freshLogger("scope-errors");

  assert.throws(
    () =>
      log.withContextKeys({ leaked: true }, () => {
        throw new Error("scope boom");
      }),
    /scope boom/,
  );
  await assert.rejects(
    () =>
      log.withContextKeys({ leaked: true }, async () => {
        throw new TypeError("async scope boom");
      }),
    TypeError,
  );

  const [record] = await captureRecords(() => log.info("after-scope-error"));
  assert.equal(
    record.attributes,
    undefined,
    "a failed scope must not leave its fields behind",
  );
});

test("invalid scope inputs fail before the callback runs", () => {
  const log = freshLogger("scope-validation");

  assert.throws(
    () => log.withContextKeys({ a: 1 }, null),
    { name: "TypeError", message: "callback must be a function" },
  );

  let ran = false;
  assert.throws(
    () =>
      log.withContextKeys(null, () => {
        ran = true;
      }),
    { name: "TypeError", message: "fields must be an object" },
  );
  assert.equal(ran, false, "invalid fields must fail before the callback");

  assert.throws(() => log.appendKeys(null), TypeError);
  assert.throws(() => log.appendKeys([]), TypeError);
  assert.throws(() => log.appendKeys("not-an-object"), TypeError);
});

test("the removed scope aliases are absent from the Logger", async () => {
  const log = freshLogger("alias-gone");

  // when the deprecated alias names are probed on a live instance
  const records = await captureRecords(() =>
    log.withContextKeys({ canonical: true }, () => log.info("alias-gone-record")),
  );

  // then only withContextKeys remains part of the public surface
  assert.equal(typeof log.withContextKeys, "function");
  assert.equal("withContext" in log, false, "withContext must stay removed");
  assert.equal(
    "appendContextKeys" in log,
    false,
    "appendContextKeys must stay removed",
  );
  assert.equal(Object.hasOwn(log, "withContext"), false);
  assert.equal(Object.hasOwn(log, "appendContextKeys"), false);
  assert.deepEqual(records[0].attributes, { canonical: true });
});

test("persistent keys and removals survive invocations and stay per logger", async () => {
  const persistent = freshLogger("keeps-keys");
  const other = freshLogger("other-keys");
  persistent.appendKeys({ tenant: "acme", transient: 1 });

  const frame = (requestId) => ({
    app: "scoped-app",
    handler: "index.run",
    requestId,
    attempt: 1,
  });

  const records = await captureRecords(async () => {
    await runInvocation(frame("r-one"), async () => {
      persistent.info("invocation-one");
      other.info("other-one");
    });
    persistent.removeKeys("transient");
    await runInvocation(frame("r-two"), async () => {
      persistent.info("invocation-two");
      other.info("other-two");
    });
  });

  assert.deepEqual(oneRecord(records, "invocation-one").attributes, {
    tenant: "acme",
    transient: 1,
  });
  assert.equal(oneRecord(records, "invocation-one").request_id, "r-one");
  const second = oneRecord(records, "invocation-two");
  assert.deepEqual(
    second.attributes,
    { tenant: "acme" },
    "a removal between invocations applies to the next invocation",
  );
  assert.equal(second.request_id, "r-two");

  for (const message of ["other-one", "other-two"]) {
    assert.equal(
      oneRecord(records, message).attributes,
      undefined,
      `${message} must not inherit another logger's persistent keys`,
    );
  }
});

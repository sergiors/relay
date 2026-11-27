// The embedded Node runtime's invocation path, observed through the SDK
// Logger: module import, warm sequential execution with per-invocation
// context, failure mapping (handler, loader, and invalid frame), the expired
// detached lease with no leakage into later invocations, and the shared
// global-symbol owner that joins the runtime's logging-context module with
// the separately built SDK copy.
import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

import { runInvocation } from "../../../internal/runtime/node/logging-context.mjs";
import { createInvoker } from "../../../internal/runtime/node/invocation.mjs";
import { Logger } from "../dist/logging.js";
import {
  captureRecords,
  captureStderr,
  oneRecord,
  recordsWithMessage,
  sleep,
} from "./helpers/logging.mjs";

const log = new Logger({ service: "runtime-invocation", level: "INFO" });

function contextFrame(overrides = {}) {
  return {
    requestId: "r-1",
    attempt: 1,
    maxAttempts: 3,
    deadline: new Date(Date.now() + 3_600_000).toISOString(),
    app: "warmapp",
    handler: "index.run",
    ...overrides,
  };
}

test("warm sequential invocations each see their own context and cold-start state", async () => {
  const seen = [];
  const invoker = createInvoker(async (name) => {
    // handler loading runs under the CURRENT invocation's context: Relay
    // binds app/handler/request identity before handler imports
    log.info("handler-loading", { requested: name });
    if (name !== "index.run") throw new Error(`unknown handler: ${name}`);
    return async (event, context) => {
      seen.push([event.n, context.requestId, context.app]);
      log.info(`warm-run-${event.n}`);
    };
  });

  const results = [];
  const records = await captureRecords(async () => {
    results.push(
      await invoker({
        id: "t1",
        handler: "index.run",
        event: { n: 1 },
        env: {},
        context: contextFrame({ requestId: "r-1" }),
      }),
    );
    results.push(
      await invoker({
        id: "t2",
        handler: "index.run",
        event: { n: 2 },
        env: {},
        context: contextFrame({ requestId: "r-2" }),
      }),
    );
  });

  assert.deepEqual(results, [
    { type: "result", id: "t1", ok: true },
    { type: "result", id: "t2", ok: true },
  ]);
  assert.deepEqual(seen, [
    [1, "r-1", "warmapp"],
    [2, "r-2", "warmapp"],
  ]);

  // import-time and handler-time records each carry their own invocation
  const [loadOne, loadTwo] = recordsWithMessage(records, "handler-loading");
  assert.equal(loadOne.request_id, "r-1");
  assert.equal(loadOne.cold_start, true, "the first invocation is the cold start");
  assert.deepEqual(loadOne.attributes, { requested: "index.run" });
  assert.equal(loadTwo.request_id, "r-2");
  assert.equal(loadTwo.cold_start, false, "the warm process is no longer cold");

  const runOne = oneRecord(records, "warm-run-1");
  assert.equal(runOne.app, "warmapp");
  assert.equal(runOne.handler, "index.run");
  assert.equal(runOne.request_id, "r-1");
  assert.equal(runOne.attempt, 1);
  assert.equal(runOne.attributes, undefined);

  const runTwo = oneRecord(records, "warm-run-2");
  assert.equal(runTwo.request_id, "r-2");
  assert.equal(runTwo.cold_start, false);
});

test("loader and handler failures map to failed results without breaking the channel", async () => {
  const missing = createInvoker(async () => {
    throw new Error("Cannot find module 'absent-handler'");
  });
  const thrower = createInvoker(async () => async () => {
    throw new TypeError("handler exploded");
  });
  const invalid = createInvoker(async () => {
    throw new Error("loader must not run for an invalid context");
  });
  const healthy = createInvoker(async () => async () => {});

  const results = [];
  const stderr = await captureStderr(async () => {
    results.push(await missing({
      id: "f1",
      handler: "index.missing",
      env: {},
      context: contextFrame(),
    }));
    results.push(await thrower({
      id: "f2",
      handler: "index.thrower",
      env: {},
      context: contextFrame({ handler: "index.thrower" }),
    }));
    results.push(await invalid({
      id: "f3",
      handler: "index.invalid",
      env: {},
      context: null,
    }));
    results.push(await healthy({
      id: "f4",
      handler: "index.ok",
      env: {},
      context: contextFrame({ handler: "index.ok" }),
    }));
  });

  assert.equal(results[0].ok, false);
  assert.match(
    results[0].error,
    /handler "index\.missing" failed: Error: Cannot find module 'absent-handler'/,
  );
  assert.equal(results[1].ok, false);
  assert.match(results[1].error, /handler "index\.thrower" failed: TypeError: handler exploded/);
  assert.equal(results[2].ok, false);
  assert.match(results[2].error, /invalid request context: expected an object/);
  // the operator-visible mirror line reached stderr
  assert.ok(
    stderr.includes(`handler "index.thrower" failed: TypeError: handler exploded`),
    `stderr must mirror the handler failure; stderr:\n${stderr}`,
  );
  // the channel stays healthy for the next invocation
  assert.deepEqual(results[3], { type: "result", id: "f4", ok: true });
});

test("detached work logs after its invocation with no leakage into later ones", async () => {
  let detached;
  const invoker = createInvoker(
    () =>
      async (_event, context) => {
        log.info(`invoke-${context.requestId}`);
        // only the first invocation schedules outliving work; the second must
        // start clean with no pending detached tasks from its predecessor
        if (context.requestId === "r-detached") {
          detached = (async () => {
            await sleep(20);
            log.info("detached-later");
          })();
        }
      },
  );

  const records = await captureRecords(async () => {
    await invoker({
      id: "d1",
      handler: "index.run",
      event: null,
      env: {},
      context: contextFrame({ requestId: "r-detached" }),
    });
    await detached;
    await invoker({
      id: "d2",
      handler: "index.run",
      event: null,
      env: {},
      context: contextFrame({ requestId: "r-after" }),
    });
  });

  const late = oneRecord(records, "detached-later");
  for (const key of ["app", "handler", "request_id", "attempt", "cold_start", "trace_id", "span_id"]) {
    assert.ok(!(key in late), `post-invocation logging must omit ${key}`);
  }

  const first = oneRecord(records, "invoke-r-detached");
  assert.equal(first.request_id, "r-detached");
  assert.equal(first.app, "warmapp");

  const after = oneRecord(records, "invoke-r-after");
  assert.equal(after.request_id, "r-after", "the next invocation starts clean");
  assert.equal(after.cold_start, false);
});

test("the runtime and SDK module copies share one invocation context", async () => {
  // the two modules are physically distinct files: the runtime's copy beside
  // the bootstrap and the SDK's compiled copy — Symbol.for must join them
  const runtimeCopy = fileURLToPath(
    new URL("../../../internal/runtime/node/logging-context.mjs", import.meta.url),
  );
  const sdkCopy = fileURLToPath(new URL("../dist/logging-context.js", import.meta.url));
  assert.notEqual(runtimeCopy, sdkCopy, "the test needs two separate module copies");

  const records = await captureRecords(() =>
    runInvocation(
      {
        app: "shared-app",
        handler: "index.shared",
        requestId: "r-shared",
        attempt: 7,
        traceId: "c".repeat(32),
      },
      () => {
        log.info("shared-record");
        new Logger({ service: "runtime-invocation", level: "INFO" }).withContextKeys(
          { scoped: true },
          () => log.info("shared-scoped"),
        );
      },
    ),
  );

  const shared = oneRecord(records, "shared-record");
  assert.equal(shared.app, "shared-app");
  assert.equal(shared.handler, "index.shared");
  assert.equal(shared.request_id, "r-shared");
  assert.equal(shared.attempt, 7);
  assert.equal(shared.trace_id, "c".repeat(32));
  assert.equal(shared.cold_start, false, "the owner's invocation count is process-wide");

  assert.deepEqual(oneRecord(records, "shared-scoped").attributes, { scoped: true });
});

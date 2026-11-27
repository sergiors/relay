// Cross-SDK schema contract: the Node and Python structured loggers must
// emit the same core record shape — key set, value types, nested attributes,
// and exception structure — so one operator pipeline can parse either SDK's
// stderr. The Python side runs the repository's own sources unmodified; no
// Python code is changed and no framework is introduced.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

import { Logger } from "../dist/logging.js";
import { captureRecords, oneRecord } from "./helpers/logging.mjs";

const repoRoot = fileURLToPath(new URL("../../..", import.meta.url));
const pythonSources = join(repoRoot, "packages", "python", "src");

const PYTHON_EMITTER = `
from relay_runtime.logging import Logger

log = Logger(service="schema-contract")
log.info("contract-info", extra={"user": "u-1", "count": 2})
try:
    raise ValueError("contract boom")
except Exception:
    log.exception("contract-exception")
`;

// runPythonEmitter executes the emitter under a candidate interpreter and
// returns its parsed records, or the failure that disqualified it. A
// candidate qualifies only by RUNNING the repository's own sources — a
// version probe alone would accept an interpreter the SDK cannot import.
function runPythonEmitter(candidate) {
  const emitted = spawnSync(candidate, ["-c", PYTHON_EMITTER], {
    encoding: "utf8",
    env: { ...process.env, PYTHONPATH: pythonSources },
  });
  if (emitted.status === 0) {
    return { records: jsonRecords(emitted.stderr) };
  }
  const detail = emitted.error
    ? emitted.error.message
    : (emitted.stderr || emitted.stdout || "").trim().split("\n").slice(-3).join(" ");
  return { failure: `${candidate}: ${detail}` };
}

function jsonRecords(text) {
  return text
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.startsWith("{"))
    .map((line) => JSON.parse(line));
}

test("the Node and Python structured loggers share one record schema", async () => {
  // the project venv first, then the ambient interpreters; every candidate
  // must actually run the repository's Python sources to qualify
  const candidates = [
    join(repoRoot, "packages", "python", ".venv", "bin", "python3"),
    "python3",
    "python",
  ];
  const failures = [];
  let pythonRecords = null;
  for (const candidate of candidates) {
    const attempt = runPythonEmitter(candidate);
    if (attempt.failure === undefined) {
      pythonRecords = attempt.records;
      break;
    }
    failures.push(attempt.failure);
  }
  assert.ok(
    pythonRecords !== null,
    `environmental blocker: no Python interpreter could run the cross-SDK schema contract:\n` +
      failures.join("\n"),
  );

  const log = new Logger({ service: "schema-contract", level: "INFO" });
  const records = await captureRecords(() => {
    log.info("contract-info", { user: "u-1", count: 2 });
    try {
      throw new Error("contract boom");
    } catch (error) {
      log.exception("contract-exception", error);
    }
  });

  const pythonInfo = oneRecord(pythonRecords, "contract-info");
  const nodeInfo = oneRecord(records, "contract-info");

  // same core schema on both SDKs
  assert.deepEqual(
    Object.keys(nodeInfo).sort(),
    Object.keys(pythonInfo).sort(),
    "core record keys must match across SDKs",
  );
  for (const key of ["timestamp", "level", "service", "message", "logger", "location"]) {
    assert.equal(typeof nodeInfo[key], "string", `node ${key} is a string`);
    assert.equal(typeof pythonInfo[key], "string", `python ${key} is a string`);
  }
  assert.equal(nodeInfo.level, "INFO");
  assert.equal(pythonInfo.level, "INFO");
  assert.equal(nodeInfo.service, "schema-contract");
  assert.equal(pythonInfo.service, "schema-contract");
  assert.equal(nodeInfo.logger, "relay_runtime.schema-contract");
  assert.equal(pythonInfo.logger, nodeInfo.logger, "logger naming matches");
  for (const record of [nodeInfo, pythonInfo]) {
    assert.ok(
      Number.isFinite(Date.parse(record.timestamp)),
      `timestamp must be parseable: ${record.timestamp}`,
    );
    assert.match(record.timestamp, /^\d{4}-\d{2}-\d{2}T/);
    assert.match(record.location, /:\d+$/, "location ends with a line number");
  }

  // attributes stay nested with identical payloads
  assert.deepEqual(nodeInfo.attributes, { user: "u-1", count: 2 });
  assert.deepEqual(pythonInfo.attributes, nodeInfo.attributes);

  const pythonException = oneRecord(pythonRecords, "contract-exception");
  const nodeException = oneRecord(records, "contract-exception");
  assert.deepEqual(
    Object.keys(nodeException).sort(),
    Object.keys(pythonException).sort(),
    "exception record keys must match across SDKs",
  );
  assert.equal(nodeException.level, "ERROR");
  assert.equal(pythonException.level, "ERROR");

  // the exception payload is {type, message, stacktrace} in both SDKs
  assert.deepEqual(
    Object.keys(nodeException.exception).sort(),
    Object.keys(pythonException.exception).sort(),
  );
  assert.deepEqual(Object.keys(nodeException.exception).sort(), [
    "message",
    "stacktrace",
    "type",
  ]);
  assert.equal(nodeException.exception.message, "contract boom");
  assert.equal(pythonException.exception.message, "contract boom");
  assert.equal(nodeException.exception.type, "Error");
  assert.equal(pythonException.exception.type, "ValueError");
  assert.equal(typeof nodeException.exception.stacktrace, "string");
  assert.equal(typeof pythonException.exception.stacktrace, "string");
  assert.ok(nodeException.exception.stacktrace.length > 0);
  assert.ok(pythonException.exception.stacktrace.length > 0);
});

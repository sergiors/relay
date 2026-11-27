import assert from "node:assert/strict";

// captureRecords runs callback while process.stderr — the Logger's documented
// output channel — is collected, and returns the parsed JSON records it
// received. Non-JSON writes are ignored, so lifecycle-style output can never
// fail a record assertion.
export async function captureRecords(callback) {
  const chunks = [];
  const original = process.stderr.write;
  process.stderr.write = (chunk) => {
    chunks.push(String(chunk));
    return true;
  };
  try {
    await callback();
  } finally {
    process.stderr.write = original;
  }
  return chunks
    .map((chunk) => chunk.trim())
    .filter((line) => line.startsWith("{"))
    .map((line) => JSON.parse(line));
}

// recordsWithMessage returns the records carrying exactly the given message,
// in emission order.
export function recordsWithMessage(records, message) {
  return records.filter((record) => record.message === message);
}

// oneRecord asserts exactly one record carries the message and returns it.
export function oneRecord(records, message) {
  const matches = recordsWithMessage(records, message);
  assert.equal(
    matches.length,
    1,
    `records with message ${JSON.stringify(message)} = ${matches.length}, ` +
      `want exactly 1; records: ${JSON.stringify(records)}`,
  );
  return matches[0];
}

// withEnv sets a process environment entry for the duration of callback and
// restores the previous value (or absence) afterwards.
export async function withEnv(name, value, callback) {
  const previous = process.env[name];
  if (value === undefined) delete process.env[name];
  else process.env[name] = value;
  try {
    return await callback();
  } finally {
    if (previous === undefined) delete process.env[name];
    else process.env[name] = previous;
  }
}

// captureStderr runs callback while process.stderr is collected and returns
// the raw text — for assertions on the non-JSON lifecycle/failure lines that
// captureRecords deliberately ignores.
export async function captureStderr(callback) {
  const chunks = [];
  const original = process.stderr.write;
  process.stderr.write = (chunk) => {
    chunks.push(String(chunk));
    return true;
  };
  try {
    await callback();
  } finally {
    process.stderr.write = original;
  }
  return chunks.join("");
}

export function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

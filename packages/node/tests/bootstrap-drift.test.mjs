import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import {
  runtimeContextMembers,
  interfaceContract,
} from "./helpers/contract.mjs";

const declarationPath = fileURLToPath(
  new URL("../dist/index.d.ts", import.meta.url),
);
// The runtime module that owns the HandlerContext class: since the bootstrap
// was split, the class lives in the invocation module imported beside
// bootstrap.mjs (both ship under /relay), so the contract is pinned against
// that module.
const contextModulePath = fileURLToPath(
  new URL("../../../internal/runtime/node/invocation.mjs", import.meta.url),
);

const expectedContract = [
  { name: "requestId", kind: "property", type: "string", optional: false },
  { name: "attempt", kind: "property", type: "number", optional: false },
  { name: "maxAttempts", kind: "property", type: "number", optional: false },
  { name: "deadline", kind: "property", type: "string", optional: false },
  { name: "messageId", kind: "property", type: "string", optional: true },
  { name: "traceId", kind: "property", type: "string", optional: true },
  { name: "app", kind: "property", type: "string", optional: false },
  { name: "handler", kind: "property", type: "string", optional: false },
  {
    name: "getRemainingTimeMs",
    kind: "method",
    params: "",
    type: "number",
    optional: false,
  },
];

test("the public HandlerContext matches Relay's bootstrap contract", () => {
  assert.ok(
    existsSync(contextModulePath),
    `missing Relay runtime module: ${contextModulePath}`,
  );
  const runtime = runtimeContextMembers(contextModulePath);
  const declaration = interfaceContract(declarationPath, "HandlerContext");

  assert.deepEqual(declaration, expectedContract);
  assert.deepEqual(
    declaration
      .filter((member) => member.kind === "property")
      .map((member) => member.name),
    runtime.fields,
  );
  assert.deepEqual(
    declaration
      .filter((member) => member.kind === "method")
      .map((member) => member.name),
    runtime.methods,
  );
});

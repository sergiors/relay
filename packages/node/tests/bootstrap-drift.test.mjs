import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import {
  bootstrapContextMembers,
  interfaceContract,
} from "./helpers/contract.mjs";

const declarationPath = fileURLToPath(
  new URL("../dist/index.d.ts", import.meta.url),
);
const bootstrapPath = fileURLToPath(
  new URL("../../../internal/runtime/node/bootstrap.mjs", import.meta.url),
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
    existsSync(bootstrapPath),
    `missing Relay bootstrap: ${bootstrapPath}`,
  );
  const runtime = bootstrapContextMembers(bootstrapPath);
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

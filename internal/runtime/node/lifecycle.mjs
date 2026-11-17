import { readFileSync } from "node:fs";

const RUNTIME_METADATA_PATH = "/relay/runtime-metadata.json";
const nativeLog = console.log.bind(console);

function writeLifecycleLine(line) {
  try {
    nativeLog("%s", line);
  } catch {
    // Lifecycle output must never affect invocation results or startup.
  }
}

export function initializeLifecycle() {
  let metadata;
  try {
    metadata = JSON.parse(readFileSync(RUNTIME_METADATA_PATH, "utf8"));
  } catch {
    return;
  }

  if (
    typeof metadata?.runtime !== "string" ||
    typeof metadata?.base_image !== "string"
  ) {
    return;
  }

  writeLifecycleLine(
    `INIT_START Runtime: ${metadata.runtime} BaseImage: ${metadata.base_image}`,
  );
}

export function writeInvocationStart(id) {
  writeLifecycleLine(`START RequestId: ${id}`);
}

export function writeInvocationEnd(id) {
  writeLifecycleLine(`END RequestId: ${id}`);
}

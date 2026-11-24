import { readFileSync } from "node:fs";

const RUNTIME_METADATA_PATH = "/relay/runtime-metadata.json";
const MIB = 1024n * 1024n;
const nativeLog = console.log.bind(console);

function memoryMiB(bytes) {
  return bytes == null ? null : (bytes / MIB).toString();
}

export function formatInvocationReport(id, durationMs, memory) {
  const fields = [`Duration: ${durationMs.toFixed(2)} ms`];
  const currentMiB = memoryMiB(memory?.currentBytes);
  const peakMiB = memoryMiB(memory?.peakBytes);
  if (currentMiB !== null) {
    fields.push(`Memory Used: ${currentMiB} MiB`);
  }
  if (peakMiB !== null) {
    fields.push(`Container Memory Peak: ${peakMiB} MiB`);
  }
  return `REPORT RequestId: ${id} ${fields.join(" ")}`;
}

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

export function writeInvocationReport(id, durationMs, memory) {
  try {
    writeLifecycleLine(formatInvocationReport(id, durationMs, memory));
  } catch {
    // Reporting is diagnostic only and must not affect invocation outcomes.
  }
}

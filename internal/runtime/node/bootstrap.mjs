// Relay's Node.js function bootstrap. The authenticated protocol is shared
// across warm invocations; handlers, invocation scope, and lifecycle output each
// live in their own /relay module so HOT_RELOAD source mounts cannot shadow them.
import { format } from "node:util";
import { performance } from "node:perf_hooks";
import {
  initializeCgroupMemory,
  readCgroupMemory,
} from "./cgroup-memory.mjs";
import {
  initializeLifecycle,
  writeInvocationEnd,
  writeInvocationReport,
  writeInvocationStart,
} from "./lifecycle.mjs";

initializeLifecycle();
initializeCgroupMemory();

for (const [method, level] of Object.entries({
  log: "INFO",
  info: "INFO",
  warn: "WARN",
  error: "ERROR",
  debug: "DEBUG",
})) {
  const nativeMethod = console[method].bind(console);
  console[method] = (...args) => {
    nativeMethod(
      "%s",
      `${new Date().toISOString()} ${level} ${format(...args)}`,
    );
  };
}

const [{ runProtocol }, { createInvoker }, { loadHandler }] = await Promise.all([
  import("./protocol.mjs"),
  import("./invocation.mjs"),
  import("./handler-loader.mjs"),
]);

const invoke = createInvoker(loadHandler);
await runProtocol(async (request) => {
  writeInvocationStart(request.context.requestId);

  const startedAt = performance.now();
  const result = await invoke(request);
  const durationMs = performance.now() - startedAt;
  const memory = readCgroupMemory();

  writeInvocationEnd(request.context.requestId);
  writeInvocationReport(request.context.requestId, durationMs, memory);
  return result;
});

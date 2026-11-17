// Relay's Node.js function bootstrap. The authenticated protocol is shared
// across warm invocations; handlers, invocation scope, and lifecycle output each
// live in their own /relay module so HOT_RELOAD source mounts cannot shadow them.
import {
  initializeLifecycle,
  writeInvocationEnd,
  writeInvocationStart,
} from "./lifecycle.mjs";

initializeLifecycle();

const [{ runProtocol }, { createInvoker }, { loadHandler }] = await Promise.all([
  import("./protocol.mjs"),
  import("./invocation.mjs"),
  import("./handler-loader.mjs"),
]);

const invoke = createInvoker(loadHandler);
await runProtocol(async (request) => {
  writeInvocationStart(request.context.requestId);
  const result = await invoke(request);
  writeInvocationEnd(request.context.requestId);
  return result;
});

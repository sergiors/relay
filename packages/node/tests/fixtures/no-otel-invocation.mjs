// Emits one record inside an invocation in a process where
// @opentelemetry/api cannot be resolved (see helpers/block-otel.cjs): with no
// span source available, the Logger falls back to the invocation frame's
// trace ID instead of dropping correlation.
import { runInvocation } from "../../../../internal/runtime/node/logging-context.mjs";
import { Logger } from "../../dist/logging.js";

const log = new Logger({ service: "no-otel-invocation", level: "INFO" });

await runInvocation(
  {
    app: "no-otel-app",
    handler: "index.run",
    requestId: "r-no-otel",
    attempt: 1,
    traceId: "d".repeat(32),
  },
  async () => {
    log.info("api-absent-invocation");
  },
);

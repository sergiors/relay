// Emits one record in a process where @opentelemetry/api cannot be resolved
// (see helpers/block-otel.cjs): standalone logging must still write valid
// records to stderr with no trace fields and no diagnostics.
import { Logger } from "../../dist/logging.js";

const log = new Logger({ service: "no-otel", level: "INFO" });
log.info("api-absent-record", { standalone: true });

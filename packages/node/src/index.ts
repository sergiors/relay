/**
 * Public invocation context interface for Relay Node.js applications.
 *
 * Relay's Node.js bootstrap constructs the actual per-invocation context.
 * This declaration describes that object for application development.
 */
export interface HandlerContext {
  requestId: string;
  attempt: number;
  maxAttempts: number;
  /** The invocation deadline as an RFC 3339 timestamp string. */
  deadline: string;
  messageId?: string;
  traceId?: string;
  app: string;
  handler: string;
  getRemainingTimeMs(): number;
}

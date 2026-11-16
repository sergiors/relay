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

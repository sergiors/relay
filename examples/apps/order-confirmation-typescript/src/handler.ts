import { confirmation } from "./order-message";
import type { OrderCreatedEvent } from "./order";

interface HandlerContext {
  requestId: string;
  attempt: number;
  maxAttempts: number;
  deadline: string;
  messageId?: string;
  traceId?: string;
  app: string;
  handler: string;
  getRemainingTimeMs(): number;
}

export function handler(
  event: OrderCreatedEvent | null,
  context: HandlerContext,
): void {
  console.log(`request ${context.requestId}: ${confirmation(event)}`);
}

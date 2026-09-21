import { confirmation } from "./order-message";
import type { OrderCreatedEvent } from "./order";

export function handler(event: OrderCreatedEvent | null): void {
  console.log(confirmation(event));
}

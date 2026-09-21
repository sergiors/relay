import type { OrderCreatedEvent } from "./order";

export function confirmation(event: OrderCreatedEvent | null): string {
  if (!event) return "Order confirmation sent.";
  return `Order ${event.order_id} confirmation sent to ${event.customer_email} (total $${event.total.toFixed(2)})`;
}

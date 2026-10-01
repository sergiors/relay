export interface OrderCreatedEvent {
  type: string;
  order_id: string;
  customer_email: string;
  total: number;
}

/**
 * Order-related types.
 * Mirrors: com.auction.order.controller.dto.*
 */

export type OrderStatus = 'CREATED' | 'PAID' | 'CANCELLED' | 'PAYMENT_PENDING' | 'PAYMENT_FAILED';

export interface Order {
  id: number;
  userId: number;
  type: string;
  referenceId: number;
  amount: number;
  status: OrderStatus;
  createdAt: string;
  paidAt?: string;
}

export interface CreateOrderRequest {
  reservationId: number;
  amount: number;
}

export interface PayResponse {
  orderId: number;
  status: string;
  paymentStatus: string;
  message: string;
}
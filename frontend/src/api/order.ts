import { get, post } from './client';
import type { CreateOrderRequest, Order, PayResponse, SpringPage } from '@/types';

/**
 * POST /api/orders
 */
export function createOrder(payload: CreateOrderRequest): Promise<Order> {
  return post<Order>('/api/orders', payload);
}

/**
 * GET /api/orders/{id}
 */
export function getOrder(id: number | string): Promise<Order> {
  return get<Order>(`/api/orders/${id}`);
}

/**
 * GET /api/orders?page=&size=
 */
export function listOrders(page = 1, size = 10): Promise<SpringPage<Order>> {
  return get<SpringPage<Order>>('/api/orders', { page, size });
}

/**
 * POST /api/orders/{id}/pay
 */
export function payOrder(id: number | string): Promise<PayResponse> {
  return post<PayResponse>(`/api/orders/${id}/pay`);
}

/**
 * POST /api/orders/{id}/cancel
 */
export function cancelOrder(id: number | string): Promise<void> {
  return post<void>(`/api/orders/${id}/cancel`);
}
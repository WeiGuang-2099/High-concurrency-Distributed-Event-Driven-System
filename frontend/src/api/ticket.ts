import { get, post } from './client';
import type { ReserveRequest, ReserveResponse, TicketStock } from '@/types';

/**
 * GET /api/tickets/events/{eventId}
 */
export function getStockByEvent(eventId: number | string): Promise<TicketStock[]> {
  return get<TicketStock[]>(`/api/tickets/events/${eventId}`);
}

/**
 * POST /api/tickets/reserve
 */
export function reserve(payload: ReserveRequest): Promise<ReserveResponse> {
  return post<ReserveResponse>('/api/tickets/reserve', payload);
}

// Confirming and releasing a reservation are not client calls. The gateway does not route
// POST /api/tickets/{id}/confirm or DELETE /api/tickets/{id}: order-service confirms after
// payment, and a reservation is released by cancelling its order (cancelOrder in order.ts).

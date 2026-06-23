import { del, get, post } from './client';
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

/**
 * POST /api/tickets/{reservationId}/confirm
 */
export function confirmReservation(reservationId: number | string): Promise<void> {
  return post<void>(`/api/tickets/${reservationId}/confirm`);
}

/**
 * DELETE /api/tickets/{reservationId}
 */
export function cancelReservation(reservationId: number | string): Promise<void> {
  return del<void>(`/api/tickets/${reservationId}`);
}

/**
 * Ticket-related types.
 * Mirrors: com.auction.ticket.controller.dto.*
 */

export interface TicketStock {
  stockId: number;
  eventId: number;
  ticketType: string;
  totalQuantity: number;
  availableQuantity: number;
  reservedQuantity: number;
  soldQuantity: number;
}

export interface CreateTicketRequest {
  eventId: number;
  ticketType: string;
  totalQuantity: number;
}

export interface ReserveRequest {
  eventId: number;
  ticketType: string;
  quantity: number;
}

export interface ReserveResponse {
  reservationId: number;
  eventId: number;
  ticketType: string;
  quantity: number;
  expireAt: string;
}

/**
 * WebSocket push: ticket stock changes.
 * Destination: /topic/ticket/{eventId}/reserved
 */
export interface StockReservedEvent {
  reservationId: number;
  ticketEventId: number;
  ticketType: string;
  quantity: number;
  userId: number;
}

/**
 * Destination: /topic/ticket/{eventId}/released
 */
export interface StockReleasedEvent {
  reservationId: number;
  ticketEventId: number;
  ticketType: string;
  quantity: number;
}

/**
 * Destination: /topic/ticket/confirmed
 */
export interface StockConfirmedEvent {
  reservationId: number;
  userId: number;
}
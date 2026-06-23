import { post } from './client';
import type { Auction, CreateAuctionRequest, CreateTicketRequest, TicketStock } from '@/types';

/**
 * POST /api/admin/auctions
 * Requires ADMIN role (enforced server-side).
 */
export function createAuction(payload: CreateAuctionRequest): Promise<Auction> {
  return post<Auction>('/api/admin/auctions', payload);
}

/**
 * POST /api/admin/tickets
 * Requires ADMIN role (enforced server-side).
 */
export function createTicketStock(payload: CreateTicketRequest): Promise<TicketStock> {
  return post<TicketStock>('/api/admin/tickets', payload);
}
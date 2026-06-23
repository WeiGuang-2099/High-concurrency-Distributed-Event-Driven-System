import { get, post } from './client';
import type {
  Auction,
  BidHistoryItem,
  PageResponse,
  PlaceBidRequest,
  PlaceBidResponse,
} from '@/types';

/**
 * GET /api/auctions/hot
 */
export function getHotAuctions(): Promise<Auction[]> {
  return get<Auction[]>('/api/auctions/hot');
}

/**
 * GET /api/auctions?page=&size=
 */
export function listAuctions(page = 0, size = 20): Promise<PageResponse<Auction>> {
  return get<PageResponse<Auction>>('/api/auctions', { page, size });
}

/**
 * GET /api/auctions/{id}
 */
export function getAuction(id: number | string): Promise<Auction> {
  return get<Auction>(`/api/auctions/${id}`);
}

/**
 * GET /api/auctions/{auctionId}/bids?page=&size=
 */
export function listBids(
  auctionId: number | string,
  page = 0,
  size = 20,
): Promise<PageResponse<BidHistoryItem>> {
  return get<PageResponse<BidHistoryItem>>(`/api/auctions/${auctionId}/bids`, { page, size });
}

/**
 * POST /api/auctions/{auctionId}/bids
 */
export function placeBid(
  auctionId: number | string,
  payload: PlaceBidRequest,
): Promise<PlaceBidResponse> {
  return post<PlaceBidResponse>(`/api/auctions/${auctionId}/bids`, payload);
}
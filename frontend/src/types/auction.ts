/**
 * Auction-related types.
 * Mirrors: com.auction.auction.controller.dto.*
 */

export type AuctionStatus = 'PENDING' | 'ACTIVE' | 'SETTLED' | 'EXPIRED' | 'CANCELLED';

export interface Auction {
  id: number;
  eventName: string;
  description?: string;
  ticketTypeId?: number;
  startingPrice: number;
  currentHighestBid?: number;
  currentHighestBidderId?: number;
  status: AuctionStatus;
  startTime: string;
  endTime: string;
  winnerId?: number;
  remainingSeconds: number;
}

export interface CreateAuctionRequest {
  eventName: string;
  description?: string;
  ticketTypeId?: number;
  startingPrice: number;
  startTime: string;
  endTime: string;
}

export interface BidHistoryItem {
  bidderId: number;
  bidderUsername: string;
  amount: number;
  bidTime: string;
}

export interface PlaceBidRequest {
  amount: number;
}

export interface PlaceBidResponse {
  auctionId: number;
  bidderId: number;
  amount: number;
  bidTime: string;
}

/**
 * WebSocket push messages from notification-service.
 * Destination: /topic/auction/{auctionId}
 */
export interface BidUpdateMessage {
  auctionId: number;
  bidderName: string;
  amount: number;
  timestamp: string;
}

/**
 * Destination: /topic/auction/{auctionId}/outbid
 */
export interface OutbidNotificationMessage {
  auctionId: number;
  outbidUserId: number;
  outbidAmount: number;
  newBidderId: number;
  newAmount: number;
}

/**
 * Destination: /topic/auction/{auctionId}/settled
 */
export interface AuctionSettledMessage {
  auctionId: number;
  winnerId: number;
  finalAmount: number;
}

/**
 * Destination: /topic/auction/{auctionId}/expired
 */
export interface AuctionExpiredMessage {
  auctionId: number;
  reason: string;
}
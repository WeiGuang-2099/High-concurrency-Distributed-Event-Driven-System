/**
 * Notification-related types.
 * Mirrors: com.auction.notification.domain.entity.Notification
 */

export type NotificationType =
  | 'OUTBID'
  | 'AUCTION_SETTLED'
  | 'STOCK_RESERVED'
  | 'STOCK_CONFIRMED';

export interface Notification {
  id: string;
  userId: number;
  type: NotificationType | string;
  title: string;
  content: string;
  read: boolean;
  createdAt: string;
}
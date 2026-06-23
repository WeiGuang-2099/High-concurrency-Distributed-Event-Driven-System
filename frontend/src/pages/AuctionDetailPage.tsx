import { useCallback, useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';
import {
  Typography,
  Card,
  Row,
  Col,
  Tag,
  Divider,
  Skeleton,
  Result,
  Statistic,
  notification,
} from 'antd';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { getAuction, listBids } from '@/api/auction';
import type { BidHistoryItem, BidUpdateMessage } from '@/types';
import CountdownTimer from '@/components/auction/CountdownTimer';
import BidHistoryList from '@/components/auction/BidHistoryList';
import BidForm from '@/components/auction/BidForm';
import { useStompSubscription } from '@/hooks/useStompSubscription';

const { Title, Paragraph, Text } = Typography;

const STATUS_COLORS: Record<string, string> = {
  PENDING: 'default',
  ACTIVE: 'processing',
  SETTLED: 'success',
  EXPIRED: 'default',
  CANCELLED: 'error',
};

/**
 * Auction detail page with real-time bidding via WebSocket.
 *
 * REST:
 *   GET /api/auctions/{id}          -> auction details
 *   GET /api/auctions/{id}/bids     -> bid history
 *
 * WebSocket subscriptions:
 *   /topic/auction/{id}             -> BidUpdateMessage (new bid)
 *   /topic/auction/{id}/outbid      -> OutbidNotificationMessage
 *   /topic/auction/{id}/settled     -> AuctionSettledMessage
 *   /topic/auction/{id}/expired     -> AuctionExpiredMessage
 */
export default function AuctionDetailPage() {
  const { id } = useParams<{ id: string }>();
  const auctionId = id!;
  const queryClient = useQueryClient();

  // Local state for real-time bid history prepend.
  const [liveBids, setLiveBids] = useState<BidHistoryItem[]>([]);

  const {
    data: auction,
    isLoading: auctionLoading,
    isError: auctionError,
  } = useQuery({
    queryKey: ['auction', auctionId],
    queryFn: () => getAuction(auctionId),
    enabled: !!auctionId,
  });

  const { data: bidsPage, isLoading: bidsLoading } = useQuery({
    queryKey: ['bids', auctionId],
    queryFn: () => listBids(auctionId, 0, 20),
    enabled: !!auctionId,
  });

  /* ------------------- WebSocket handlers ------------------- */

  // New bid arrived: update current price + prepend to history.
  const handleBidUpdate = useCallback(
    (msg: BidUpdateMessage) => {
      // Optimistic local prepend.
      setLiveBids((prev) => {
        if (prev.some((b) => b.amount === msg.amount && b.bidderUsername === msg.bidderName)) {
          return prev;
        }
        const newBid: BidHistoryItem = {
          bidderId: 0, // not provided in WS message
          bidderUsername: msg.bidderName,
          amount: msg.amount,
          bidTime: msg.timestamp,
        };
        return [newBid, ...prev];
      });

      // Invalidate auction detail so current price refreshes.
      queryClient.invalidateQueries({ queryKey: ['auction', auctionId] });
    },
    [auctionId, queryClient],
  );

  // Handle outbid event.
  const handleOutbid = useCallback((msg: { auctionId: number; newAmount: number }) => {
    notification.warning({
      message: 'You have been outbid!',
      description: `A new bid of $${msg.newAmount.toFixed(2)} has been placed.`,
      placement: 'bottomRight',
      duration: 5,
    });
  }, []);

  // Handle settled.
  const handleSettled = useCallback(
    (msg: { auctionId: number; winnerId: number; finalAmount: number }) => {
      notification.success({
        message: 'Auction Settled',
        description: `Winner placed $${msg.finalAmount.toFixed(2)}.`,
        placement: 'bottomRight',
        duration: 0,
      });
      queryClient.invalidateQueries({ queryKey: ['auction', auctionId] });
    },
    [auctionId, queryClient],
  );

  // Handle expired.
  const handleExpired = useCallback(
    (msg: { auctionId: number; reason: string }) => {
      notification.info({
        message: 'Auction Ended',
        description: msg.reason || 'The auction has expired.',
        placement: 'bottomRight',
        duration: 0,
      });
      queryClient.invalidateQueries({ queryKey: ['auction', auctionId] });
    },
    [auctionId, queryClient],
  );

  // Subscribe to WebSocket topics.
  useStompSubscription<BidUpdateMessage>(
    auction ? `/topic/auction/${auctionId}` : null,
    handleBidUpdate,
  );
  useStompSubscription(
    auction ? `/topic/auction/${auctionId}/outbid` : null,
    handleOutbid,
  );
  useStompSubscription(
    auction ? `/topic/auction/${auctionId}/settled` : null,
    handleSettled,
  );
  useStompSubscription(
    auction ? `/topic/auction/${auctionId}/expired` : null,
    handleExpired,
  );

  // Reset live bids when the REST data changes.
  useEffect(() => {
    if (bidsPage?.items) {
      setLiveBids(bidsPage.items);
    }
  }, [bidsPage]);

  if (auctionError) {
    return (
      <Result
        status="404"
        title="Auction not found"
        subTitle="The auction you are looking for does not exist or has been removed."
      />
    );
  }

  if (auctionLoading || !auction) {
    return <Skeleton active paragraph={{ rows: 8 }} />;
  }

  const currentPrice = auction.currentHighestBid ?? auction.startingPrice;
  const mergedBids = [...liveBids];

  return (
    <div style={{ padding: 24 }}>
      <Row gutter={24}>
        {/* Left: auction info */}
        <Col xs={24} lg={16}>
          <Card>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <Title level={2} style={{ margin: 0 }}>
                {auction.eventName}
              </Title>
              <Tag color={STATUS_COLORS[auction.status] || 'default'} style={{ fontSize: 14 }}>
                {auction.status}
              </Tag>
            </div>

            <Paragraph style={{ marginTop: 16, fontSize: 15, color: '#666' }}>
              {auction.description || 'No description provided for this auction.'}
            </Paragraph>

            <Divider />

            <Row gutter={32}>
              <Col>
                <Statistic
                  title="Current Highest Bid"
                  value={currentPrice}
                  precision={2}
                  prefix="$"
                  valueStyle={{ color: '#cf1322', fontSize: 36, fontWeight: 700 }}
                />
              </Col>
              <Col>
                <Statistic title="Starting Price" value={auction.startingPrice} precision={2} prefix="$" />
              </Col>
            </Row>

            {auction.status === 'ACTIVE' && (
              <div style={{ marginTop: 24 }}>
                <Text type="secondary" style={{ fontSize: 16 }}>
                  Time Remaining:
                </Text>
                <div style={{ marginTop: 8 }}>
                  <CountdownTimer remainingSeconds={auction.remainingSeconds} endTime={auction.endTime} />
                </div>
              </div>
            )}
          </Card>

          {/* Bid Form */}
          {auction.status === 'ACTIVE' && (
            <Card title="Place a Bid" style={{ marginTop: 24 }}>
              <BidForm auctionId={auctionId} currentPrice={currentPrice} />
            </Card>
          )}

          {auction.status !== 'ACTIVE' && (
            <Card style={{ marginTop: 24 }}>
              <Result
                status="info"
                title={
                  auction.status === 'PENDING'
                    ? 'This auction has not started yet.'
                    : auction.status === 'SETTLED'
                      ? `Auction won by user #${auction.winnerId ?? 'N/A'}`
                      : 'This auction is no longer active.'
                }
              />
            </Card>
          )}
        </Col>

        {/* Right: bid history */}
        <Col xs={24} lg={8}>
          <Card title="Bid History" style={{ maxHeight: 600, overflow: 'auto' }}>
            <BidHistoryList bids={mergedBids} loading={bidsLoading} />
          </Card>
        </Col>
      </Row>
    </div>
  );
}
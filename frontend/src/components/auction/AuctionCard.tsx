import { Card, Tag, Typography } from 'antd';
import { FireOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import type { Auction } from '@/types';
import CountdownTimer from './CountdownTimer';

const { Text, Paragraph } = Typography;

const STATUS_COLORS: Record<string, string> = {
  PENDING: 'default',
  ACTIVE: 'processing',
  SETTLED: 'success',
  EXPIRED: 'default',
  CANCELLED: 'error',
};

interface AuctionCardProps {
  auction: Auction;
}

/**
 * Compact auction card displayed in the homepage grid.
 * Shows event name, current price, countdown, and bid count placeholder.
 */
export function AuctionCard({ auction }: AuctionCardProps) {
  const navigate = useNavigate();

  return (
    <Card
      hoverable
      onClick={() => navigate(`/auctions/${auction.id}`)}
      cover={
        <div
          style={{
            height: 160,
            background: 'linear-gradient(135deg, #1890ff 0%, #36cfc9 100%)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}
        >
          <FireOutlined style={{ fontSize: 48, color: '#fff' }} />
        </div>
      }
    >
      <Card.Meta
        title={
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Text ellipsis style={{ maxWidth: 180 }}>
              {auction.eventName}
            </Text>
            <Tag color={STATUS_COLORS[auction.status] || 'default'}>{auction.status}</Tag>
          </div>
        }
        description={
          <Paragraph ellipsis={{ rows: 2 }} style={{ minHeight: 44, marginBottom: 8 }}>
            {auction.description || 'No description provided.'}
          </Paragraph>
        }
      />

      <div style={{ marginTop: 12 }}>
        <div style={{ marginBottom: 8 }}>
          <Text type="secondary">Current Bid: </Text>
          <Text strong style={{ fontSize: 20, color: '#cf1322' }}>
            ${(auction.currentHighestBid ?? auction.startingPrice).toFixed(2)}
          </Text>
        </div>
        {auction.status === 'ACTIVE' && (
          <div>
            <Text type="secondary">Ends in:</Text>
            <div style={{ marginTop: 4 }}>
              <CountdownTimer remainingSeconds={auction.remainingSeconds} />
            </div>
          </div>
        )}
        {auction.status !== 'ACTIVE' && (
          <Text type="secondary">Starts: {new Date(auction.startTime).toLocaleString()}</Text>
        )}
      </div>
    </Card>
  );
}

export default AuctionCard;
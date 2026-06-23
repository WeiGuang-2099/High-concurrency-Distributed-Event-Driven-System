import { Row, Col, Typography, Skeleton, Empty, Button } from 'antd';
import { useQuery } from '@tanstack/react-query';
import { getHotAuctions } from '@/api/auction';
import AuctionCard from '@/components/auction/AuctionCard';

const { Title } = Typography;

/**
 * Home page: displays a grid of hot auctions with countdown timers.
 * Endpoint: GET /api/auctions/hot
 */
export default function HomePage() {
  const { data: auctions, isLoading, isError, refetch } = useQuery({
    queryKey: ['auctions', 'hot'],
    queryFn: getHotAuctions,
    staleTime: 30_000,
  });

  return (
    <div style={{ padding: '24px' }}>
      <Title level={2} style={{ marginBottom: 24 }}>
        Hot Auctions
      </Title>

      {isLoading && (
        <Row gutter={[16, 16]}>
          {Array.from({ length: 8 }).map((_, i) => (
            <Col key={i} xs={24} sm={12} md={8} lg={6}>
              <Skeleton active avatar paragraph={{ rows: 4 }} />
            </Col>
          ))}
        </Row>
      )}

      {isError && (
        <Empty
          description="Failed to load auctions"
          style={{ marginTop: 48 }}
        >
          <Button type="primary" onClick={() => refetch()}>
            Retry
          </Button>
        </Empty>
      )}

      {auctions && auctions.length === 0 && (
        <Empty description="No active auctions right now" style={{ marginTop: 48 }} />
      )}

      {auctions && auctions.length > 0 && (
        <Row gutter={[16, 16]}>
          {auctions.map((auction) => (
            <Col key={auction.id} xs={24} sm={12} md={8} lg={6}>
              <AuctionCard auction={auction} />
            </Col>
          ))}
        </Row>
      )}
    </div>
  );
}
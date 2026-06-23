import { Card, Col, Row, Statistic, Typography, Skeleton } from 'antd';
import {
  CrownOutlined,
  ThunderboltOutlined,
  ShoppingCartOutlined,
  DollarOutlined,
} from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { getHotAuctions } from '@/api/auction';
import { listOrders } from '@/api/order';
import type { Auction, Order } from '@/types';

const { Title } = Typography;

/**
 * Admin dashboard: shows summary statistics.
 *
 * Note: the backend does not yet expose a dedicated aggregation endpoint,
 * so we derive stats from existing list endpoints (best-effort for demo).
 */
export default function DashboardPage() {
  const { data: auctions, isLoading: auctionsLoading } = useQuery({
    queryKey: ['auctions', 'hot'],
    queryFn: getHotAuctions,
  });

  const { data: ordersPage, isLoading: ordersLoading } = useQuery({
    queryKey: ['orders', 'admin-dashboard'],
    queryFn: () => listOrders(1, 100),
  });

  const activeAuctions = (auctions ?? []).filter((a: Auction) => a.status === 'ACTIVE').length;
  const totalBids = (auctions ?? []).reduce(
    (sum: number, a: Auction) => sum + (a.currentHighestBid ? 1 : 0),
    0,
  );
  const orders = ordersPage?.content ?? [];
  const totalOrders = orders.length;
  const totalRevenue = orders
    .filter((o: Order) => o.status === 'PAID')
    .reduce((sum: number, o: Order) => sum + o.amount, 0);

  if (auctionsLoading || ordersLoading) {
    return <Skeleton active paragraph={{ rows: 4 }} />;
  }

  return (
    <div>
      <Title level={3}>Dashboard</Title>
      <Row gutter={16}>
        <Col xs={24} sm={12} md={6}>
          <Card>
            <Statistic
              title="Active Auctions"
              value={activeAuctions}
              prefix={<CrownOutlined />}
              valueStyle={{ color: '#1890ff' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} md={6}>
          <Card>
            <Statistic
              title="Total Bids"
              value={totalBids}
              prefix={<ThunderboltOutlined />}
              valueStyle={{ color: '#cf1322' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} md={6}>
          <Card>
            <Statistic
              title="Total Orders"
              value={totalOrders}
              prefix={<ShoppingCartOutlined />}
              valueStyle={{ color: '#52c41a' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} md={6}>
          <Card>
            <Statistic
              title="Total Revenue"
              value={totalRevenue}
              precision={2}
              prefix={<DollarOutlined />}
              valueStyle={{ color: '#722ed1' }}
            />
          </Card>
        </Col>
      </Row>
    </div>
  );
}
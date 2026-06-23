import { useState } from 'react';
import { Card, Tabs, Table, Tag, Typography, Button, Empty } from 'antd';
import { useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { listOrders } from '@/api/order';
import type { Order, OrderStatus } from '@/types';
import dayjs from 'dayjs';

const { Title, Text } = Typography;

const STATUS_TAG_COLOR: Record<OrderStatus, string> = {
  CREATED: 'orange',
  PAID: 'green',
  CANCELLED: 'red',
  PAYMENT_PENDING: 'blue',
  PAYMENT_FAILED: 'red',
};

/**
 * User center: displays order history.
 * The "My Bids" tab is a placeholder because the backend does not yet expose
 * a user-specific bid history endpoint (GET /api/users/me/bids).
 */
export default function ProfilePage() {
  const navigate = useNavigate();
  const [page, setPage] = useState(1);

  const { data: ordersPage, isLoading } = useQuery({
    queryKey: ['orders', page],
    queryFn: () => listOrders(page, 10),
  });

  const orderColumns = [
    {
      title: 'Order ID',
      dataIndex: 'id',
      key: 'id',
      render: (id: number) => <Text strong>#{id}</Text>,
    },
    {
      title: 'Type',
      dataIndex: 'type',
      key: 'type',
    },
    {
      title: 'Amount',
      dataIndex: 'amount',
      key: 'amount',
      render: (amount: number) => (
        <Text strong style={{ color: '#cf1322' }}>
          ${amount.toFixed(2)}
        </Text>
      ),
    },
    {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
      render: (status: OrderStatus) => <Tag color={STATUS_TAG_COLOR[status]}>{status}</Tag>,
    },
    {
      title: 'Created At',
      dataIndex: 'createdAt',
      key: 'createdAt',
      render: (ts: string) => dayjs(ts).format('YYYY-MM-DD HH:mm'),
    },
    {
      title: 'Action',
      key: 'action',
      render: (_: unknown, record: Order) => (
        <Button type="link" onClick={() => navigate(`/orders/${record.id}/pay`)}>
          View
        </Button>
      ),
    },
  ];

  return (
    <div style={{ padding: 24 }}>
      <Title level={2}>My Account</Title>

      <Card>
        <Tabs
          defaultActiveKey="orders"
          items={[
            {
              key: 'orders',
              label: 'My Orders',
              children: (
                <Table<Order>
                  rowKey="id"
                  columns={orderColumns}
                  dataSource={ordersPage?.content}
                  loading={isLoading}
                  pagination={{
                    current: page,
                    total: ordersPage?.totalElements ?? 0,
                    pageSize: 10,
                    onChange: (p) => setPage(p),
                  }}
                />
              ),
            },
            {
              key: 'bids',
              label: 'My Bids',
              children: (
                <Empty
                  description="Bid history will be available here once the backend exposes a user-specific bid endpoint."
                  style={{ padding: 48 }}
                >
                  <Button type="primary" onClick={() => navigate('/')}>
                    Browse Auctions
                  </Button>
                </Empty>
              ),
            },
          ]}
        />
      </Card>
    </div>
  );
}
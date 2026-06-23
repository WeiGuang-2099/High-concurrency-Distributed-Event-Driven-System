import { useParams, useNavigate } from 'react-router-dom';
import { Card, Typography, Button, Descriptions, Result, Spin, Space } from 'antd';
import { CheckCircleOutlined, CloseCircleOutlined } from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getOrder, payOrder } from '@/api/order';
import { message } from 'antd';

const { Title } = Typography;

const STATUS_COLOR: Record<string, string> = {
  CREATED: 'orange',
  PAID: 'green',
  CANCELLED: 'red',
  PAYMENT_PENDING: 'blue',
  PAYMENT_FAILED: 'red',
};

/**
 * Order payment page with mock payment.
 * REST: GET /api/orders/{id}, POST /api/orders/{id}/pay
 */
export default function OrderPayPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const { data: order, isLoading } = useQuery({
    queryKey: ['order', id],
    queryFn: () => getOrder(id!),
    enabled: !!id,
  });

  const payMutation = useMutation({
    mutationFn: () => payOrder(id!),
    onSuccess: () => {
      message.success('Payment successful!');
      queryClient.invalidateQueries({ queryKey: ['order', id] });
      queryClient.invalidateQueries({ queryKey: ['orders'] });
    },
  });

  if (isLoading || !order) {
    return (
      <div style={{ padding: 24, textAlign: 'center' }}>
        <Spin size="large" />
      </div>
    );
  }

  // Payment success state.
  if (order.status === 'PAID') {
    return (
      <div style={{ padding: 24 }}>
        <Result
          icon={<CheckCircleOutlined style={{ color: '#52c41a' }} />}
          status="success"
          title="Payment Successful!"
          subTitle={`Order #${order.id} has been confirmed.`}
          extra={[
            <Button type="primary" key="profile" onClick={() => navigate('/profile')}>
              View My Orders
            </Button>,
            <Button key="home" onClick={() => navigate('/')}>
              Back to Home
            </Button>,
          ]}
        />
      </div>
    );
  }

  // Cancelled / failed state.
  if (order.status === 'CANCELLED' || order.status === 'PAYMENT_FAILED') {
    return (
      <div style={{ padding: 24 }}>
        <Result
          icon={<CloseCircleOutlined style={{ color: '#ff4d4f' }} />}
          status="error"
          title={order.status === 'CANCELLED' ? 'Order Cancelled' : 'Payment Failed'}
          subTitle={`Order #${order.id} - ${order.status}`}
          extra={[
            <Button type="primary" key="retry" onClick={() => payMutation.mutate()}>
              Retry Payment
            </Button>,
            <Button key="home" onClick={() => navigate('/')}>
              Back to Home
            </Button>,
          ]}
        />
      </div>
    );
  }

  // Default: payment form.
  return (
    <div style={{ padding: 24, maxWidth: 600, margin: '0 auto' }}>
      <Title level={2}>Order Summary</Title>
      <Card>
        <Descriptions column={1} bordered>
          <Descriptions.Item label="Order ID">{order.id}</Descriptions.Item>
          <Descriptions.Item label="Type">{order.type}</Descriptions.Item>
          <Descriptions.Item label="Reference ID">{order.referenceId}</Descriptions.Item>
          <Descriptions.Item label="Amount">
            <Typography.Text strong style={{ fontSize: 20, color: '#cf1322' }}>
              ${order.amount.toFixed(2)}
            </Typography.Text>
          </Descriptions.Item>
          <Descriptions.Item label="Status">
            <Typography.Text style={{ color: STATUS_COLOR[order.status] || 'default' }}>
              {order.status}
            </Typography.Text>
          </Descriptions.Item>
          <Descriptions.Item label="Created At">
            {new Date(order.createdAt).toLocaleString()}
          </Descriptions.Item>
        </Descriptions>

        <div style={{ marginTop: 24, textAlign: 'center' }}>
          <Space>
            <Button
              type="primary"
              size="large"
              loading={payMutation.isPending}
              onClick={() => payMutation.mutate()}
            >
              Pay Now (${order.amount.toFixed(2)})
            </Button>
            <Button size="large" onClick={() => navigate('/profile')}>
              Cancel
            </Button>
          </Space>
        </div>
      </Card>
    </div>
  );
}
import { useState } from 'react';
import { Card, Button, InputNumber, Typography, Tag, Space } from 'antd';
import { ShoppingCartOutlined } from '@ant-design/icons';
import type { TicketStock } from '@/types';

interface TicketCardProps {
  stock: TicketStock;
  onBuy: (ticketType: string, quantity: number) => void;
  loading?: boolean;
}

/**
 * Ticket purchase card with quantity selector.
 * Shows real-time stock count (updated via WebSocket by parent).
 */
export function TicketCard({ stock, onBuy, loading }: TicketCardProps) {
  const [quantity, setQuantity] = useState(1);
  const outOfStock = stock.availableQuantity <= 0;

  return (
    <Card
      title={
        <Space>
          <Typography.Text strong>{stock.ticketType}</Typography.Text>
          {outOfStock && <Tag color="error">SOLD OUT</Tag>}
        </Space>
      }
      extra={<Tag color={outOfStock ? 'error' : 'success'}>Event #{stock.eventId}</Tag>}
    >
      <div style={{ marginBottom: 16 }}>
        <Typography.Text type="secondary">Available: </Typography.Text>
        <Typography.Text strong style={{ fontSize: 18, color: outOfStock ? '#999' : '#52c41a' }}>
          {stock.availableQuantity}
        </Typography.Text>
        <Typography.Text type="secondary"> / {stock.totalQuantity}</Typography.Text>
      </div>

      <div style={{ marginBottom: 16 }}>
        <Typography.Text type="secondary">Sold: {stock.soldQuantity}</Typography.Text>
        <br />
        <Typography.Text type="secondary">Reserved: {stock.reservedQuantity}</Typography.Text>
      </div>

      <Space style={{ width: '100%', justifyContent: 'space-between' }}>
        <div>
          <Typography.Text type="secondary">Quantity:</Typography.Text>
          <InputNumber
            min={1}
            max={Math.max(1, stock.availableQuantity)}
            value={quantity}
            onChange={(v) => setQuantity(v ?? 1)}
            disabled={outOfStock}
            style={{ marginLeft: 8, width: 80 }}
          />
        </div>
        <Button
          type="primary"
          icon={<ShoppingCartOutlined />}
          disabled={outOfStock}
          loading={loading}
          onClick={() => onBuy(stock.ticketType, quantity)}
        >
          {outOfStock ? 'Out of Stock' : 'Buy'}
        </Button>
      </Space>
    </Card>
  );
}

export default TicketCard;
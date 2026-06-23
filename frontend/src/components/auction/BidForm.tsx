import { useState } from 'react';
import { InputNumber, Button, Form, Typography, Alert } from 'antd';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { message } from 'antd';
import { placeBid } from '@/api/auction';
import { useAuthStore } from '@/store/authStore';
import { useNavigate } from 'react-router-dom';

interface BidFormProps {
  auctionId: number | string;
  /** Current highest bid or starting price (minimum for the next bid). */
  currentPrice: number;
}

/**
 * Bid input form with client-side validation.
 * - Requires login (button shows "Login to bid" when unauthenticated).
 * - Amount must be greater than current price.
 * - On success: shows success toast and invalidates bid history cache.
 * - On failure: shows error toast (handled by axios interceptor).
 */
export function BidForm({ auctionId, currentPrice }: BidFormProps) {
  const [form] = Form.useForm();
  const [amount, setAmount] = useState<number | null>(null);
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const isAuthenticated = useAuthStore((s) => s.isAuthenticated);

  const minBid = currentPrice + 0.01;

  const mutation = useMutation({
    mutationFn: (amt: number) => placeBid(auctionId, { amount: amt }),
    onSuccess: () => {
      message.success('Bid placed!');
      form.resetFields();
      setAmount(null);
      // Invalidate caches so bid history and auction detail refresh.
      queryClient.invalidateQueries({ queryKey: ['auction', auctionId] });
      queryClient.invalidateQueries({ queryKey: ['bids', auctionId] });
    },
  });

  const handleSubmit = async () => {
    try {
      const values = await form.validateFields();
      mutation.mutate(values.amount);
    } catch {
      // validation error is shown by the Form
    }
  };

  if (!isAuthenticated) {
    return (
      <Alert
        message="You need to log in to place a bid."
        type="info"
        showIcon
        action={
          <Button size="small" type="primary" onClick={() => navigate('/login')}>
            Login to bid
          </Button>
        }
      />
    );
  }

  return (
    <Form form={form} layout="inline" name="bid-form">
      <Form.Item
        name="amount"
        label={<Typography.Text strong>Bid Amount</Typography.Text>}
        rules={[
          { required: true, message: 'Please enter a bid amount' },
          {
            validator: (_, value: number) => {
              if (value === undefined || value === null) return Promise.reject();
              if (value <= currentPrice) {
                return Promise.reject(
                  new Error(`Bid must be greater than $${currentPrice.toFixed(2)}`),
                );
              }
              return Promise.resolve();
            },
          },
        ]}
      >
        <InputNumber
          prefix="$"
          min={minBid}
          step={1}
          precision={2}
          style={{ width: 160 }}
          placeholder={`Min $${minBid.toFixed(2)}`}
          value={amount}
          onChange={(v) => setAmount(v)}
        />
      </Form.Item>
      <Form.Item>
        <Button type="primary" loading={mutation.isPending} onClick={handleSubmit}>
          Place Bid
        </Button>
      </Form.Item>
    </Form>
  );
}

export default BidForm;
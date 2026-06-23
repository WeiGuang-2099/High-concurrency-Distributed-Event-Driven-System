import { Card, Form, Input, InputNumber, Button, DatePicker, Typography, App } from 'antd';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { createAuction } from '@/api/admin';
import dayjs from 'dayjs';

const { Title } = Typography;
const { RangePicker } = DatePicker;

interface FormValues {
  eventName: string;
  description?: string;
  ticketTypeId?: number;
  startingPrice: number;
  timeRange: [dayjs.Dayjs, dayjs.Dayjs];
}

/**
 * Admin page: create a new auction.
 * POST /api/admin/auctions (requires ADMIN role).
 */
export default function CreateAuctionPage() {
  const [form] = Form.useForm<FormValues>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { message } = App.useApp();

  const mutation = useMutation({
    mutationFn: (values: FormValues) =>
      createAuction({
        eventName: values.eventName,
        description: values.description,
        ticketTypeId: values.ticketTypeId,
        startingPrice: values.startingPrice,
        startTime: values.timeRange[0].format('YYYY-MM-DDTHH:mm:ss'),
        endTime: values.timeRange[1].format('YYYY-MM-DDTHH:mm:ss'),
      }),
    onSuccess: (auction) => {
      message.success(`Auction "${auction.eventName}" created!`);
      queryClient.invalidateQueries({ queryKey: ['auctions'] });
      navigate(`/auctions/${auction.id}`);
    },
  });

  const handleSubmit = async () => {
    try {
      const values = await form.validateFields();
      mutation.mutate(values);
    } catch {
      // validation handled by form
    }
  };

  return (
    <div style={{ maxWidth: 640 }}>
      <Title level={3}>Create Auction</Title>
      <Card>
        <Form form={form} layout="vertical">
          <Form.Item
            name="eventName"
            label="Event Name"
            rules={[{ required: true, message: 'Please enter an event name' }]}
          >
            <Input placeholder="e.g. Summer Concert VIP Pass" maxLength={200} />
          </Form.Item>

          <Form.Item name="description" label="Description">
            <Input.TextArea rows={3} placeholder="Auction description" maxLength={1000} />
          </Form.Item>

          <Form.Item name="ticketTypeId" label="Linked Ticket Type ID">
            <InputNumber style={{ width: '100%' }} placeholder="Optional: link to a ticket type" />
          </Form.Item>

          <Form.Item
            name="startingPrice"
            label="Starting Price ($)"
            rules={[{ required: true, message: 'Please enter a starting price' }]}
          >
            <InputNumber prefix="$" min={0.01} precision={2} style={{ width: '100%' }} />
          </Form.Item>

          <Form.Item
            name="timeRange"
            label="Auction Duration"
            rules={[{ required: true, message: 'Please select start and end time' }]}
          >
            <RangePicker showTime format="YYYY-MM-DD HH:mm:ss" style={{ width: '100%' }} />
          </Form.Item>

          <Form.Item>
            <Button type="primary" loading={mutation.isPending} onClick={handleSubmit}>
              Create Auction
            </Button>
          </Form.Item>
        </Form>
      </Card>
    </div>
  );
}
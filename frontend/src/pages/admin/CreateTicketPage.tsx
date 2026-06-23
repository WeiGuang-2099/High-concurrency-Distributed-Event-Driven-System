import { Card, Form, Input, InputNumber, Button, Typography, App } from 'antd';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { createTicketStock } from '@/api/admin';

const { Title } = Typography;

interface FormValues {
  eventId: number;
  ticketType: string;
  totalQuantity: number;
}

/**
 * Admin page: create ticket stock.
 * POST /api/admin/tickets (requires ADMIN role).
 */
export default function CreateTicketPage() {
  const [form] = Form.useForm<FormValues>();
  const queryClient = useQueryClient();
  const { message } = App.useApp();

  const mutation = useMutation({
    mutationFn: (values: FormValues) =>
      createTicketStock({
        eventId: values.eventId,
        ticketType: values.ticketType,
        totalQuantity: values.totalQuantity,
      }),
    onSuccess: (stock) => {
      message.success(`Created ${stock.totalQuantity} "${stock.ticketType}" tickets!`);
      queryClient.invalidateQueries({ queryKey: ['tickets'] });
      form.resetFields();
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
      <Title level={3}>Create Ticket Stock</Title>
      <Card>
        <Form form={form} layout="vertical">
          <Form.Item
            name="eventId"
            label="Event ID"
            rules={[{ required: true, message: 'Please enter an event ID' }]}
          >
            <InputNumber style={{ width: '100%' }} placeholder="e.g. 1" />
          </Form.Item>

          <Form.Item
            name="ticketType"
            label="Ticket Type"
            rules={[{ required: true, message: 'Please enter a ticket type' }]}
          >
            <Input placeholder="e.g. VIP, General Admission, Early Bird" />
          </Form.Item>

          <Form.Item
            name="totalQuantity"
            label="Total Quantity"
            rules={[{ required: true, message: 'Please enter quantity' }]}
          >
            <InputNumber min={1} style={{ width: '100%' }} placeholder="e.g. 100" />
          </Form.Item>

          <Form.Item>
            <Button type="primary" loading={mutation.isPending} onClick={handleSubmit}>
              Create Tickets
            </Button>
          </Form.Item>
        </Form>
      </Card>
    </div>
  );
}
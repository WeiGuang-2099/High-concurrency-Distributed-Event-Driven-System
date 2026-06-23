import { useCallback } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { Row, Col, Typography, Skeleton, Empty, message } from 'antd';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { getStockByEvent, reserve } from '@/api/ticket';
import { createOrder } from '@/api/order';
import TicketCard from '@/components/ticket/TicketCard';
import { useStompSubscription } from '@/hooks/useStompSubscription';
import type { StockReservedEvent, StockReleasedEvent } from '@/types';

const { Title } = Typography;

/**
 * Ticket purchase page with real-time stock updates via WebSocket.
 *
 * REST:
 *   GET  /api/tickets/events/{eventId}
 *   POST /api/tickets/reserve
 *   POST /api/orders
 *
 * WebSocket subscriptions:
 *   /topic/ticket/{eventId}/reserved  -> stock decreased
 *   /topic/ticket/{eventId}/released  -> stock restored
 */
export default function TicketPurchasePage() {
  const { eventId } = useParams<{ eventId: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const { data: stocks, isLoading } = useQuery({
    queryKey: ['tickets', eventId],
    queryFn: () => getStockByEvent(eventId!),
    enabled: !!eventId,
  });

  // Reserve -> Create Order flow.
  const reserveMutation = useMutation({
    mutationFn: (params: { ticketType: string; quantity: number }) =>
      reserve({ eventId: Number(eventId), ticketType: params.ticketType, quantity: params.quantity }),
    onSuccess: async (reservation) => {
      // After reservation, create an order and navigate to payment.
      const order = await createOrder({
        reservationId: reservation.reservationId,
        amount: 0, // backend will compute; amount may be required by validation
      });
      message.success('Reservation created! Redirecting to payment...');
      navigate(`/orders/${order.id}/pay`);
    },
  });

  // WS: stock reserved -> refresh stock counts.
  const handleStockReserved = useCallback(
    (_msg: StockReservedEvent) => {
      queryClient.invalidateQueries({ queryKey: ['tickets', eventId] });
    },
    [eventId, queryClient],
  );

  // WS: stock released -> refresh stock counts.
  const handleStockReleased = useCallback(
    (_msg: StockReleasedEvent) => {
      queryClient.invalidateQueries({ queryKey: ['tickets', eventId] });
    },
    [eventId, queryClient],
  );

  useStompSubscription<StockReservedEvent>(
    eventId ? `/topic/ticket/${eventId}/reserved` : null,
    handleStockReserved,
  );
  useStompSubscription<StockReleasedEvent>(
    eventId ? `/topic/ticket/${eventId}/released` : null,
    handleStockReleased,
  );

  if (isLoading) {
    return (
      <div style={{ padding: 24 }}>
        <Skeleton active paragraph={{ rows: 6 }} />
      </div>
    );
  }

  if (!stocks || stocks.length === 0) {
    return (
      <div style={{ padding: 24 }}>
        <Empty description="No tickets available for this event" />
      </div>
    );
  }

  return (
    <div style={{ padding: 24 }}>
      <Title level={2}>Buy Tickets - Event #{eventId}</Title>
      <Row gutter={[16, 16]}>
        {stocks.map((stock) => (
          <Col key={stock.stockId} xs={24} sm={12} md={8}>
            <TicketCard
              stock={stock}
              onBuy={(ticketType, quantity) =>
                reserveMutation.mutate({ ticketType, quantity })
              }
              loading={reserveMutation.isPending}
            />
          </Col>
        ))}
      </Row>
    </div>
  );
}
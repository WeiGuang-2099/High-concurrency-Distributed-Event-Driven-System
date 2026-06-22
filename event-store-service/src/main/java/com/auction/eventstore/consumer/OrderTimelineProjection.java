package com.auction.eventstore.consumer;

import com.auction.common.event.KafkaTopics;
import com.auction.common.event.order.OrderCancelledEvent;
import com.auction.common.event.order.OrderCreatedEvent;
import com.auction.common.event.order.OrderExpiredEvent;
import com.auction.common.event.order.PaymentCompletedEvent;
import com.auction.common.event.order.PaymentInitiatedEvent;
import com.auction.eventstore.domain.OrderTimelineEntry;
import com.auction.eventstore.repository.OrderTimelineRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Projection: appends an order status timeline entry per order-event.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderTimelineProjection {

    private final OrderTimelineRepository repository;

    @KafkaListener(
            topics = KafkaTopics.ORDER_EVENTS,
            groupId = "projection-order-timeline",
            containerFactory = "kafkaListenerContainerFactory")
    public void on(Object payload, Acknowledgment ack) {
        try {
            if (payload instanceof OrderCreatedEvent) {
                OrderCreatedEvent e = (OrderCreatedEvent) payload;
                save(e.getEventId(), e.getOrderId(), e.getUserId(), "CREATED",
                        e.getEventType(), "Order created, amount=" + e.getAmount(), e.getTimestamp());
            } else if (payload instanceof PaymentInitiatedEvent) {
                PaymentInitiatedEvent e = (PaymentInitiatedEvent) payload;
                save(e.getEventId(), e.getOrderId(), e.getUserId(), "PAYING",
                        e.getEventType(), "Payment initiated", e.getTimestamp());
            } else if (payload instanceof PaymentCompletedEvent) {
                PaymentCompletedEvent e = (PaymentCompletedEvent) payload;
                save(e.getEventId(), e.getOrderId(), e.getUserId(), "PAID",
                        e.getEventType(), "Payment completed, amount=" + e.getAmount(), e.getTimestamp());
            } else if (payload instanceof OrderCancelledEvent) {
                OrderCancelledEvent e = (OrderCancelledEvent) payload;
                save(e.getEventId(), e.getOrderId(), e.getUserId(), "CANCELLED",
                        e.getEventType(), "Order cancelled: " + e.getReason(), e.getTimestamp());
            } else if (payload instanceof OrderExpiredEvent) {
                OrderExpiredEvent e = (OrderExpiredEvent) payload;
                save(e.getEventId(), e.getOrderId(), e.getUserId(), "EXPIRED",
                        e.getEventType(), "Order expired", e.getTimestamp());
            }
        } catch (Exception e) {
            log.error("OrderTimelineProjection failed: {}", e.getMessage(), e);
        } finally {
            ack.acknowledge();
        }
    }

    private void save(String eventId, Long orderId, Long userId, String status,
                      String eventType, String detail, java.time.Instant ts) {
        repository.save(OrderTimelineEntry.builder()
                .eventId(eventId)
                .orderId(orderId)
                .userId(userId)
                .status(status)
                .eventType(eventType)
                .detail(detail)
                .timestamp(ts)
                .build());
    }
}

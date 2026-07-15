package com.auction.eventstore.consumer;

import com.auction.common.event.BaseEvent;
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
    // Typed as BaseEvent: an Object parameter would receive the raw ConsumerRecord
    // (no JSON conversion) and every instanceof check below would silently miss.
    public void on(BaseEvent payload, Acknowledgment ack) {
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
        } catch (RuntimeException e) {
            // Do NOT ack: a swallowed failure drops the row and bypasses the
            // dead-letter recoverer, which only fires on exceptions that escape.
            log.error("OrderTimelineProjection failed for eventId={}, will retry/DLT: {}",
                    payload.getEventId(), e.getMessage(), e);
            throw e;
        }
        ack.acknowledge();
    }

    private void save(String eventId, Long orderId, Long userId, String status,
                      String eventType, String detail, java.time.Instant ts) {
        repository.save(OrderTimelineEntry.builder()
                // _id = eventId makes save() an upsert, so a redelivered event rewrites
                // its own row instead of appending a duplicate timeline entry.
                .id(eventId)
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

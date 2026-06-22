package com.auction.eventstore.consumer;

import com.auction.common.event.BaseEvent;
import com.auction.common.event.KafkaTopics;
import com.auction.eventstore.service.CacheInvalidationService;
import com.auction.eventstore.service.EventStoreService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Write-side consumer: subscribes to all business event topics and appends each event
 * to the append-only MongoDB store. Also invalidates the relevant Redis cache so the
 * read-side projection can repopulate on next access.
 *
 * <p>This is the single source of truth; projections run in their own consumer groups.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EventStoreConsumer {

    private final EventStoreService eventStoreService;
    private final CacheInvalidationService cacheInvalidationService;

    @KafkaListener(
            topics = {KafkaTopics.AUCTION_EVENTS, KafkaTopics.TICKET_EVENTS, KafkaTopics.ORDER_EVENTS},
            groupId = "event-store-writer",
            containerFactory = "kafkaListenerContainerFactory")
    public void onEvent(Object payload,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                        Acknowledgment ack) {
        if (!(payload instanceof BaseEvent)) {
            log.warn("Ignoring non-event payload of type: {}", payload.getClass().getName());
            ack.acknowledge();
            return;
        }
        BaseEvent event = (BaseEvent) payload;
        String aggregateType = inferAggregateType(topic);
        try {
            eventStoreService.append(event, topic, aggregateType);
            invalidateCacheFor(event);
        } catch (Exception e) {
            log.error("Failed to store event eventId={} type={}: {}",
                    event.getEventId(), event.getEventType(), e.getMessage(), e);
        } finally {
            ack.acknowledge();
        }
    }

    private String inferAggregateType(String topic) {
        switch (topic) {
            case KafkaTopics.AUCTION_EVENTS: return "Auction";
            case KafkaTopics.TICKET_EVENTS: return "Ticket";
            case KafkaTopics.ORDER_EVENTS: return "Order";
            default: return "Unknown";
        }
    }

    /**
     * Write-side cache invalidation per PRD US-003.
     */
    private void invalidateCacheFor(BaseEvent event) {
        try {
            switch (event.getEventType()) {
                case "BidPlaced":
                case "AuctionSettled":
                case "AuctionExpired":
                case "AuctionActivated":
                    cacheInvalidationService.invalidateAuctionSummary(Long.valueOf(event.getAggregateId()));
                    break;
                case "StockReserved":
                case "StockConfirmed":
                case "StockReleased":
                    cacheInvalidationService.invalidateStock(Long.valueOf(event.getAggregateId()), "*");
                    break;
                case "OrderCreated":
                case "PaymentInitiated":
                case "PaymentCompleted":
                case "OrderCancelled":
                case "OrderExpired":
                    cacheInvalidationService.invalidateOrder(Long.valueOf(event.getAggregateId()));
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            log.warn("Cache invalidation skipped for eventId={}: {}", event.getEventId(), e.getMessage());
        }
    }
}

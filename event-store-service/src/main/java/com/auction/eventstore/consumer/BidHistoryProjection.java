package com.auction.eventstore.consumer;

import com.auction.common.event.BaseEvent;
import com.auction.common.event.KafkaTopics;
import com.auction.common.event.auction.BidPlacedEvent;
import com.auction.eventstore.domain.BidHistoryEntry;
import com.auction.eventstore.repository.BidHistoryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Projection: appends one bid_history row per BidPlaced event.
 * Used for paginated bid history queries on the read side.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BidHistoryProjection {

    private final BidHistoryRepository repository;

    @KafkaListener(
            topics = KafkaTopics.AUCTION_EVENTS,
            groupId = "projection-bid-history",
            containerFactory = "kafkaListenerContainerFactory")
    // Typed as BaseEvent: an Object parameter would receive the raw ConsumerRecord
    // (no JSON conversion) and every instanceof check below would silently miss.
    public void on(BaseEvent payload, Acknowledgment ack) {
        try {
            if (payload instanceof BidPlacedEvent) {
                BidPlacedEvent e = (BidPlacedEvent) payload;
                repository.save(BidHistoryEntry.builder()
                        // _id = eventId makes save() an upsert, so a redelivered event
                        // rewrites its own row instead of appending a duplicate bid.
                        .id(e.getEventId())
                        .eventId(e.getEventId())
                        .auctionId(e.getAuctionId())
                        .bidderId(e.getBidderId())
                        .bidderUsername(e.getBidderUsername())
                        .amount(e.getAmount())
                        .bidTime(e.getBidTime())
                        .build());
            }
        } catch (RuntimeException e) {
            // Do NOT ack: a swallowed failure drops the row and bypasses the
            // dead-letter recoverer, which only fires on exceptions that escape.
            log.error("BidHistoryProjection failed for eventId={}, will retry/DLT: {}",
                    payload.getEventId(), e.getMessage(), e);
            throw e;
        }
        ack.acknowledge();
    }
}

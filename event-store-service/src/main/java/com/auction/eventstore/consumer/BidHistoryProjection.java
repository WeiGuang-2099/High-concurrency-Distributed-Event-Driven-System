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
                        .eventId(e.getEventId())
                        .auctionId(e.getAuctionId())
                        .bidderId(e.getBidderId())
                        .bidderUsername(e.getBidderUsername())
                        .amount(e.getAmount())
                        .bidTime(e.getBidTime())
                        .build());
            }
        } catch (Exception e) {
            log.error("BidHistoryProjection failed: {}", e.getMessage(), e);
        } finally {
            ack.acknowledge();
        }
    }
}

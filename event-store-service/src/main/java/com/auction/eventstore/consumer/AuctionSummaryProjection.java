package com.auction.eventstore.consumer;

import com.auction.common.event.BaseEvent;
import com.auction.common.event.KafkaTopics;
import com.auction.common.event.auction.AuctionActivatedEvent;
import com.auction.common.event.auction.AuctionCreatedEvent;
import com.auction.common.event.auction.AuctionExpiredEvent;
import com.auction.common.event.auction.AuctionSettledEvent;
import com.auction.common.event.auction.BidPlacedEvent;
import com.auction.eventstore.domain.AuctionSummaryView;
import com.auction.eventstore.repository.AuctionSummaryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

/**
 * Projection: maintains the auction_summary:{auctionId} view in MongoDB.
 * BidPlaced updates currentHighestBid / bidCount; lifecycle events update status.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AuctionSummaryProjection {

    private final AuctionSummaryRepository repository;

    @KafkaListener(
            topics = KafkaTopics.AUCTION_EVENTS,
            groupId = "projection-auction-summary",
            containerFactory = "kafkaListenerContainerFactory")
    // Typed as BaseEvent: an Object parameter would receive the raw ConsumerRecord
    // (no JSON conversion) and every instanceof check below would silently miss.
    public void on(BaseEvent payload,
                   @Header(KafkaHeaders.OFFSET) long offset,
                   Acknowledgment ack) {
        try {
            if (alreadyApplied(payload.getAggregateId(), offset)) {
                log.debug("Skipping already-applied event eventId={} offset={} for auction {}",
                        payload.getEventId(), offset, payload.getAggregateId());
                ack.acknowledge();
                return;
            }
            if (payload instanceof AuctionCreatedEvent) {
                AuctionCreatedEvent e = (AuctionCreatedEvent) payload;
                upsert(e.getAggregateId(), "PENDING", null, null, null, 0, offset);
            } else if (payload instanceof AuctionActivatedEvent) {
                AuctionActivatedEvent e = (AuctionActivatedEvent) payload;
                updateStatus(e.getAggregateId(), "ACTIVE", offset);
            } else if (payload instanceof BidPlacedEvent) {
                BidPlacedEvent e = (BidPlacedEvent) payload;
                upsert(e.getAggregateId(), "ACTIVE",
                        e.getAmount(), e.getBidderId(), e.getBidderUsername(), 1, offset);
            } else if (payload instanceof AuctionSettledEvent) {
                AuctionSettledEvent e = (AuctionSettledEvent) payload;
                updateStatus(e.getAggregateId(), "SETTLED", offset);
            } else if (payload instanceof AuctionExpiredEvent) {
                AuctionExpiredEvent e = (AuctionExpiredEvent) payload;
                updateStatus(e.getAggregateId(), "EXPIRED", offset);
            }
        } catch (RuntimeException e) {
            // Do NOT ack: a swallowed failure drops the update and bypasses the
            // dead-letter recoverer, which only fires on exceptions that escape.
            log.error("AuctionSummaryProjection failed for eventId={}, will retry/DLT: {}",
                    payload.getEventId(), e.getMessage(), e);
            throw e;
        }
        ack.acknowledge();
    }

    /**
     * True when this record's offset is at or below the watermark already folded into the
     * view, i.e. Kafka is replaying an event we have applied. Safe to read-then-write
     * without a lock: an auction's events all carry its id as the Kafka key, so they sit
     * on one partition and are handled by a single consumer thread.
     */
    private boolean alreadyApplied(String aggregateId, long offset) {
        return repository.findByAuctionId(Long.valueOf(aggregateId))
                .map(AuctionSummaryView::getLastOffset)
                .filter(applied -> offset <= applied)
                .isPresent();
    }

    private void upsert(String aggregateId, String status,
                        BigDecimal highestBid, Long bidderId,
                        String bidderUsername, int bidIncrement, long offset) {
        Long auctionId = Long.valueOf(aggregateId);
        Optional<AuctionSummaryView> existing = repository.findByAuctionId(auctionId);
        if (existing.isPresent()) {
            AuctionSummaryView v = existing.get();
            v.setStatus(status);
            v.setBidCount(v.getBidCount() + bidIncrement);
            v.setLastOffset(offset);
            v.setUpdatedAt(Instant.now());
            if (highestBid != null && (v.getCurrentHighestBid() == null
                    || highestBid.compareTo(v.getCurrentHighestBid()) > 0)) {
                v.setCurrentHighestBid(highestBid);
                v.setCurrentHighestBidderId(bidderId);
                v.setCurrentHighestBidderUsername(bidderUsername);
            }
            repository.save(v);
        } else {
            repository.save(AuctionSummaryView.builder()
                    .auctionId(auctionId)
                    .status(status)
                    .currentHighestBid(highestBid)
                    .currentHighestBidderId(bidderId)
                    .currentHighestBidderUsername(bidderUsername)
                    .bidCount(bidIncrement)
                    .lastOffset(offset)
                    .updatedAt(Instant.now())
                    .build());
        }
    }

    private void updateStatus(String aggregateId, String status, long offset) {
        Long auctionId = Long.valueOf(aggregateId);
        repository.findByAuctionId(auctionId).ifPresent(v -> {
            v.setStatus(status);
            v.setLastOffset(offset);
            v.setUpdatedAt(Instant.now());
            repository.save(v);
        });
    }
}

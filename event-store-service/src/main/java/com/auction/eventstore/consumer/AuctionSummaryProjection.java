package com.auction.eventstore.consumer;

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
    public void on(Object payload, Acknowledgment ack) {
        try {
            if (payload instanceof AuctionCreatedEvent) {
                AuctionCreatedEvent e = (AuctionCreatedEvent) payload;
                upsert(e.getAggregateId(), "PENDING", null, null, null, 0);
            } else if (payload instanceof AuctionActivatedEvent) {
                AuctionActivatedEvent e = (AuctionActivatedEvent) payload;
                updateStatus(e.getAggregateId(), "ACTIVE");
            } else if (payload instanceof BidPlacedEvent) {
                BidPlacedEvent e = (BidPlacedEvent) payload;
                upsert(e.getAggregateId(), "ACTIVE",
                        e.getAmount(), e.getBidderId(), e.getBidderUsername(), 1);
            } else if (payload instanceof AuctionSettledEvent) {
                AuctionSettledEvent e = (AuctionSettledEvent) payload;
                updateStatus(e.getAggregateId(), "SETTLED");
            } else if (payload instanceof AuctionExpiredEvent) {
                AuctionExpiredEvent e = (AuctionExpiredEvent) payload;
                updateStatus(e.getAggregateId(), "EXPIRED");
            }
        } catch (Exception e) {
            log.error("AuctionSummaryProjection failed: {}", e.getMessage(), e);
        } finally {
            ack.acknowledge();
        }
    }

    private void upsert(String aggregateId, String status,
                        BigDecimal highestBid, Long bidderId,
                        String bidderUsername, int bidIncrement) {
        Long auctionId = Long.valueOf(aggregateId);
        Optional<AuctionSummaryView> existing = repository.findByAuctionId(auctionId);
        if (existing.isPresent()) {
            AuctionSummaryView v = existing.get();
            v.setStatus(status);
            v.setBidCount(v.getBidCount() + bidIncrement);
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
                    .updatedAt(Instant.now())
                    .build());
        }
    }

    private void updateStatus(String aggregateId, String status) {
        Long auctionId = Long.valueOf(aggregateId);
        repository.findByAuctionId(auctionId).ifPresent(v -> {
            v.setStatus(status);
            v.setUpdatedAt(Instant.now());
            repository.save(v);
        });
    }
}

package com.auction.eventstore.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * CQRS read model: summary of an auction aggregated from auction-events.
 * Mirrored in Redis (auction_summary:{auctionId}) for sub-ms reads.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "auction_summary")
public class AuctionSummaryView {

    @Id
    private String id;

    @Indexed(unique = true)
    private Long auctionId;

    private String status;

    private BigDecimal currentHighestBid;

    private Long currentHighestBidderId;

    private String currentHighestBidderUsername;

    private int bidCount;

    /**
     * Highest auction-events offset already folded into this view. bidCount is an
     * increment, so replaying an event would inflate it; every auction's events share
     * the auction id as their Kafka key and therefore land on one partition, which
     * makes the offset a usable per-auction watermark. Only valid while the topic
     * keeps its partition count -- repartitioning remaps keys and invalidates it.
     */
    private Long lastOffset;

    private Instant updatedAt;
}

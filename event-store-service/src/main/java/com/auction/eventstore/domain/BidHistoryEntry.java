package com.auction.eventstore.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * CQRS read model: one row per bid, used for paginated bid history queries.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "bid_history")
@CompoundIndex(name = "auction_time", def = "{'auctionId': -1, 'bidTime': -1}")
public class BidHistoryEntry {

    @Id
    private String id;

    private String eventId;

    @Indexed
    private Long auctionId;

    private Long bidderId;

    private String bidderUsername;

    private BigDecimal amount;

    private Instant bidTime;
}

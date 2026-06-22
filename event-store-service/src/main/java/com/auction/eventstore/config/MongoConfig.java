package com.auction.eventstore.config;

import com.auction.eventstore.domain.AuctionSummaryView;
import com.auction.eventstore.domain.BidHistoryEntry;
import com.auction.eventstore.domain.EventDocument;
import com.auction.eventstore.domain.OrderTimelineEntry;
import com.auction.eventstore.domain.SnapshotDocument;
import com.auction.eventstore.domain.StockMovementEntry;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexOperations;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Ensures all Mongo indexes (unique + TTL + compound) exist on startup.
 * Using @EventListener(ApplicationReadyEvent) instead of @PostConstruct so the
 * MongoTemplate is fully initialized.
 */
@Configuration
public class MongoConfig {

    private final MongoTemplate mongoTemplate;

    public MongoConfig(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void ensureIndexes() {
        // events: unique compound index (aggregateId, sequenceNumber) for idempotency
        IndexOperations events = mongoTemplate.indexOps(EventDocument.class);
        events.ensureIndex(new Index()
                .on("aggregateId", Sort.Direction.ASC)
                .on("sequenceNumber", Sort.Direction.ASC)
                .unique()
                .named("uniq_agg_seq"));
        // eventId unique
        events.ensureIndex(new Index().on("eventId", Sort.Direction.ASC).unique().named("uniq_event_id"));
        // TTL 30 days on storedAt to manage storage growth (per PRD technical considerations)
        events.ensureIndex(new Index().on("storedAt", Sort.Direction.ASC)
                .expire(Duration.ofDays(30).toSeconds()).named("ttl_stored_at"));

        // snapshots: aggregateId + sequenceNumber desc
        IndexOperations snapshots = mongoTemplate.indexOps(SnapshotDocument.class);
        snapshots.ensureIndex(new Index()
                .on("aggregateId", Sort.Direction.ASC)
                .on("sequenceNumber", Sort.Direction.DESC)
                .named("agg_seq"));

        // bid_history compound
        IndexOperations bidHistory = mongoTemplate.indexOps(BidHistoryEntry.class);
        bidHistory.ensureIndex(new Index()
                .on("auctionId", Sort.Direction.DESC)
                .on("bidTime", Sort.Direction.DESC)
                .named("auction_time"));

        // order_timeline compound
        IndexOperations orderTimeline = mongoTemplate.indexOps(OrderTimelineEntry.class);
        orderTimeline.ensureIndex(new Index()
                .on("orderId", Sort.Direction.ASC)
                .on("timestamp", Sort.Direction.ASC)
                .named("order_time"));

        // stock_movements compound
        IndexOperations stock = mongoTemplate.indexOps(StockMovementEntry.class);
        stock.ensureIndex(new Index()
                .on("eventId", Sort.Direction.ASC)
                .on("timestamp", Sort.Direction.ASC)
                .named("event_time"));

        // auction_summary unique on auctionId
        IndexOperations summary = mongoTemplate.indexOps(AuctionSummaryView.class);
        summary.ensureIndex(new Index().on("auctionId", Sort.Direction.ASC).unique().named("uniq_auction_id"));
    }
}

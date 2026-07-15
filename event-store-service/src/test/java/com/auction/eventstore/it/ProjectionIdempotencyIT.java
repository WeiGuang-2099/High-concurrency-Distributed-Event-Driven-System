package com.auction.eventstore.it;

import com.auction.common.event.auction.AuctionSettledEvent;
import com.auction.common.event.auction.BidPlacedEvent;
import com.auction.common.event.order.OrderCreatedEvent;
import com.auction.common.event.ticket.StockReservedEvent;
import com.auction.eventstore.consumer.AuctionSummaryProjection;
import com.auction.eventstore.consumer.BidHistoryProjection;
import com.auction.eventstore.consumer.OrderTimelineProjection;
import com.auction.eventstore.consumer.StockMovementProjection;
import com.auction.eventstore.domain.AuctionSummaryView;
import com.auction.eventstore.repository.AuctionSummaryRepository;
import com.auction.eventstore.repository.BidHistoryRepository;
import com.auction.eventstore.repository.OrderTimelineRepository;
import com.auction.eventstore.repository.StockMovementRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Kafka delivers at least once, so every projection has to tolerate seeing the same
 * event twice (rebalance or restart before the offset commit replays it). The write
 * model already dedupes -- AuctionEventConsumer.persistBid relies on a unique key on
 * eventId -- but the read model did not, so the same bid could appear twice in the
 * history and inflate bidCount.
 *
 * <p>Runs against a real MongoDB: a mocked repository cannot prove idempotency, since
 * the whole question is what the store does with a second write.</p>
 */
@DataMongoTest
@DisplayName("读模型投影 - 重复投递必须幂等")
class ProjectionIdempotencyIT {

    static final MongoDBContainer MONGO =
            new MongoDBContainer(DockerImageName.parse("mongo:7"));

    static {
        MONGO.start();
    }

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", () -> MONGO.getReplicaSetUrl("event_store_idempotency"));
    }

    @Autowired private BidHistoryRepository bidHistoryRepository;
    @Autowired private AuctionSummaryRepository auctionSummaryRepository;
    @Autowired private OrderTimelineRepository orderTimelineRepository;
    @Autowired private StockMovementRepository stockMovementRepository;

    private final Acknowledgment ack = mock(Acknowledgment.class);

    @BeforeEach
    void clean() {
        bidHistoryRepository.deleteAll();
        auctionSummaryRepository.deleteAll();
        orderTimelineRepository.deleteAll();
        stockMovementRepository.deleteAll();
    }

    private static BidPlacedEvent bid(long auctionId, String amount) {
        return new BidPlacedEvent(auctionId, 10L, "alice", new BigDecimal(amount),
                Instant.now(), null, null);
    }

    @Test
    @DisplayName("BidHistory - 同一事件投递两次只留一行")
    void bidHistory_sameEventTwice_storesOneRow() {
        BidHistoryProjection projection = new BidHistoryProjection(bidHistoryRepository);
        BidPlacedEvent event = bid(1L, "100.00");

        projection.on(event, ack);
        projection.on(event, ack);

        assertThat(bidHistoryRepository.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("OrderTimeline - 同一事件投递两次只留一行")
    void orderTimeline_sameEventTwice_storesOneRow() {
        OrderTimelineProjection projection = new OrderTimelineProjection(orderTimelineRepository);
        OrderCreatedEvent event = new OrderCreatedEvent(1L, 10L, "AUCTION", 5L, new BigDecimal("100.00"));

        projection.on(event, ack);
        projection.on(event, ack);

        assertThat(orderTimelineRepository.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("StockMovement - 同一事件投递两次只留一行")
    void stockMovement_sameEventTwice_storesOneRow() {
        StockMovementProjection projection = new StockMovementProjection(stockMovementRepository);
        StockReservedEvent event = new StockReservedEvent(1L, "VIP", 100L, 10L, 2);

        projection.on(event, ack);
        projection.on(event, ack);

        assertThat(stockMovementRepository.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("AuctionSummary - 重复投递不会把 bidCount 灌水")
    void auctionSummary_sameEventTwice_countsOneBid() {
        AuctionSummaryProjection projection = new AuctionSummaryProjection(auctionSummaryRepository);
        BidPlacedEvent event = bid(1L, "100.00");

        projection.on(event, 5L, ack);
        projection.on(event, 5L, ack);

        AuctionSummaryView view = auctionSummaryRepository.findByAuctionId(1L).orElseThrow();
        assertThat(view.getBidCount()).isEqualTo(1);
        assertThat(view.getCurrentHighestBid()).isEqualByComparingTo("100.00");
    }

    @Test
    @DisplayName("AuctionSummary - 迟到重投的旧出价不能把已结算拍卖打回 ACTIVE")
    void auctionSummary_replayedOldBid_doesNotRegressSettledStatus() {
        AuctionSummaryProjection projection = new AuctionSummaryProjection(auctionSummaryRepository);

        projection.on(bid(1L, "100.00"), 5L, ack);
        AuctionSettledEvent settled = new AuctionSettledEvent(1L, 10L, new BigDecimal("100.00"), 7L);
        projection.on(settled, 10L, ack);

        // Redelivery of the earlier bid: its offset is below the watermark already applied.
        projection.on(bid(1L, "100.00"), 5L, ack);

        AuctionSummaryView view = auctionSummaryRepository.findByAuctionId(1L).orElseThrow();
        assertThat(view.getStatus()).isEqualTo("SETTLED");
        assertThat(view.getBidCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("AuctionSummary - 更高 offset 的新事件正常应用")
    void auctionSummary_newerOffset_isApplied() {
        AuctionSummaryProjection projection = new AuctionSummaryProjection(auctionSummaryRepository);

        projection.on(bid(1L, "100.00"), 5L, ack);
        projection.on(bid(1L, "150.00"), 6L, ack);

        AuctionSummaryView view = auctionSummaryRepository.findByAuctionId(1L).orElseThrow();
        assertThat(view.getBidCount()).isEqualTo(2);
        assertThat(view.getCurrentHighestBid()).isEqualByComparingTo("150.00");
    }
}

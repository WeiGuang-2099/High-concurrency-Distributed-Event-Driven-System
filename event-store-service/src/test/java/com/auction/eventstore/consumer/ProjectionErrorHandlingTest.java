package com.auction.eventstore.consumer;

import com.auction.common.event.auction.BidPlacedEvent;
import com.auction.common.event.order.OrderCreatedEvent;
import com.auction.common.event.ticket.StockReservedEvent;
import com.auction.eventstore.domain.AuctionSummaryView;
import com.auction.eventstore.domain.BidHistoryEntry;
import com.auction.eventstore.domain.OrderTimelineEntry;
import com.auction.eventstore.domain.StockMovementEntry;
import com.auction.eventstore.repository.AuctionSummaryRepository;
import com.auction.eventstore.repository.BidHistoryRepository;
import com.auction.eventstore.repository.OrderTimelineRepository;
import com.auction.eventstore.repository.StockMovementRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The read-side projections must behave like {@link EventStoreConsumer}: a write failure
 * has to reach the container error handler so the record is retried and then dead-lettered.
 * Catching the exception and acking anyway drops the row silently and bypasses the
 * DeadLetterPublishingRecoverer configured on the shared listener factory.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("读模型投影 - 写入失败必须走重试/DLT，不能静默丢弃")
class ProjectionErrorHandlingTest {

    @Mock private Acknowledgment ack;

    private static BidPlacedEvent bidPlaced() {
        return new BidPlacedEvent(1L, 10L, "alice", new BigDecimal("100.00"),
                Instant.now(), null, null);
    }

    @Nested
    @DisplayName("BidHistoryProjection")
    class BidHistory {

        @Mock private BidHistoryRepository repository;
        @InjectMocks private BidHistoryProjection projection;

        @Test
        @DisplayName("save 抛异常 - 异常向容器传播且不 ack")
        void saveFailure_propagatesAndDoesNotAck() {
            when(repository.save(any(BidHistoryEntry.class)))
                    .thenThrow(new RuntimeException("mongo down"));

            assertThatThrownBy(() -> projection.on(bidPlaced(), ack))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("mongo down");

            verify(ack, never()).acknowledge();
        }

        @Test
        @DisplayName("save 成功 - 正常 ack")
        void saveSuccess_acks() {
            projection.on(bidPlaced(), ack);

            verify(ack).acknowledge();
        }
    }

    @Nested
    @DisplayName("AuctionSummaryProjection")
    class AuctionSummary {

        @Mock private AuctionSummaryRepository repository;
        @InjectMocks private AuctionSummaryProjection projection;

        @Test
        @DisplayName("save 抛异常 - 异常向容器传播且不 ack")
        void saveFailure_propagatesAndDoesNotAck() {
            when(repository.findByAuctionId(1L)).thenReturn(Optional.empty());
            when(repository.save(any(AuctionSummaryView.class)))
                    .thenThrow(new RuntimeException("mongo down"));

            assertThatThrownBy(() -> projection.on(bidPlaced(), 1L, ack))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("mongo down");

            verify(ack, never()).acknowledge();
        }

        @Test
        @DisplayName("save 成功 - 正常 ack")
        void saveSuccess_acks() {
            when(repository.findByAuctionId(1L)).thenReturn(Optional.empty());

            projection.on(bidPlaced(), 1L, ack);

            verify(ack).acknowledge();
        }
    }

    @Nested
    @DisplayName("OrderTimelineProjection")
    class OrderTimeline {

        @Mock private OrderTimelineRepository repository;
        @InjectMocks private OrderTimelineProjection projection;

        private OrderCreatedEvent orderCreated() {
            return new OrderCreatedEvent(1L, 10L, "AUCTION", 5L, new BigDecimal("100.00"));
        }

        @Test
        @DisplayName("save 抛异常 - 异常向容器传播且不 ack")
        void saveFailure_propagatesAndDoesNotAck() {
            when(repository.save(any(OrderTimelineEntry.class)))
                    .thenThrow(new RuntimeException("mongo down"));

            assertThatThrownBy(() -> projection.on(orderCreated(), ack))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("mongo down");

            verify(ack, never()).acknowledge();
        }

        @Test
        @DisplayName("save 成功 - 正常 ack")
        void saveSuccess_acks() {
            projection.on(orderCreated(), ack);

            verify(ack).acknowledge();
        }
    }

    @Nested
    @DisplayName("StockMovementProjection")
    class StockMovement {

        @Mock private StockMovementRepository repository;
        @InjectMocks private StockMovementProjection projection;

        private StockReservedEvent stockReserved() {
            return new StockReservedEvent(1L, "VIP", 100L, 10L, 2);
        }

        @Test
        @DisplayName("save 抛异常 - 异常向容器传播且不 ack")
        void saveFailure_propagatesAndDoesNotAck() {
            when(repository.save(any(StockMovementEntry.class)))
                    .thenThrow(new RuntimeException("mongo down"));

            assertThatThrownBy(() -> projection.on(stockReserved(), ack))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("mongo down");

            verify(ack, never()).acknowledge();
        }

        @Test
        @DisplayName("save 成功 - 正常 ack")
        void saveSuccess_acks() {
            projection.on(stockReserved(), ack);

            verify(ack).acknowledge();
        }
    }
}

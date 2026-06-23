package com.auction.auction.service;

import com.auction.auction.controller.dto.PlaceBidResponse;
import com.auction.auction.service.impl.BidServiceImpl;
import com.auction.auction.service.RedisKeys;
import com.auction.common.exception.BusinessException;
import com.auction.common.event.KafkaTopics;
import com.auction.common.event.auction.BidOutbidEvent;
import com.auction.common.event.auction.BidPlacedEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("BidService - 拍卖竞价核心逻辑")
class BidServiceImplTest {

    @Mock private StringRedisTemplate redis;
    @Mock private DefaultRedisScript<List> placeBidScript;
    @Mock private KafkaTemplate<String, Object> kafkaTemplate;

    @InjectMocks private BidServiceImpl bidService;

    private static final Long AUCTION_ID = 100L;
    private static final Long BIDDER_ID = 1L;
    private static final String BIDDER_USERNAME = "bidder1";

    @BeforeEach
    void setUp() {
        lenient().when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));
    }

    // ==================== placeBid ====================

    @Nested
    @DisplayName("placeBid() 竞价出价")
    class PlaceBidTest {

        @Test
        @DisplayName("正常竞价 - 出价高于当前最高价时被接受")
        void placeBid_higherThanCurrent_success() {
            // Lua returns {1=accepted, "OK", prevBidder, prevAmount}
            List<Object> luaResult = Arrays.asList(1L, "OK", "5", "100.00");
            when(redis.execute(eq(placeBidScript), anyList(), any(Object[].class)))
                    .thenReturn(luaResult);

            PlaceBidResponse response = bidService.placeBid(
                    AUCTION_ID, BIDDER_ID, BIDDER_USERNAME, new BigDecimal("150.00"));

            assertThat(response).isNotNull();
            assertThat(response.getAuctionId()).isEqualTo(AUCTION_ID);
            assertThat(response.getBidderId()).isEqualTo(BIDDER_ID);
            assertThat(response.getAmount()).isEqualByComparingTo("150.00");
        }

        @Test
        @DisplayName("出价低于当前最高价 - 抛出400异常")
        void placeBid_bidTooLow_throws400() {
            List<Object> luaResult = Arrays.asList(0L, "BID_TOO_LOW", "5", "200.00");
            when(redis.execute(eq(placeBidScript), anyList(), any(Object[].class)))
                    .thenReturn(luaResult);

            assertThatThrownBy(() -> bidService.placeBid(
                    AUCTION_ID, BIDDER_ID, BIDDER_USERNAME, new BigDecimal("150.00")))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> {
                        BusinessException be = (BusinessException) ex;
                        assertThat(be.getCode()).isEqualTo(400);
                        assertThat(be.getMessage()).contains("Bid too low");
                    });

            // No Kafka events published on rejection
            verify(kafkaTemplate, never()).send(anyString(), anyString(), any());
        }

        @Test
        @DisplayName("拍卖未激活 - 抛出400异常")
        void placeBid_auctionNotActive_throws400() {
            List<Object> luaResult = Arrays.asList(0L, "AUCTION_NOT_ACTIVE", "", "");
            when(redis.execute(eq(placeBidScript), anyList(), any(Object[].class)))
                    .thenReturn(luaResult);

            assertThatThrownBy(() -> bidService.placeBid(
                    AUCTION_ID, BIDDER_ID, BIDDER_USERNAME, new BigDecimal("100.00")))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> {
                        BusinessException be = (BusinessException) ex;
                        assertThat(be.getCode()).isEqualTo(400);
                        assertThat(be.getMessage()).contains("not active");
                    });
        }

        @Test
        @DisplayName("Lua脚本返回null - 抛出500异常")
        void placeBid_luaReturnsNull_throws500() {
            when(redis.execute(eq(placeBidScript), anyList(), any(Object[].class)))
                    .thenReturn(null);

            assertThatThrownBy(() -> bidService.placeBid(
                    AUCTION_ID, BIDDER_ID, BIDDER_USERNAME, new BigDecimal("100.00")))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(500));
        }

        @Test
        @DisplayName("首次竞价 - 无前一出价者时只发 BidPlacedEvent")
        void placeBid_firstBid_noOutbidEvent() {
            // prevBidder is empty string
            List<Object> luaResult = Arrays.asList(1L, "OK", "", "");
            when(redis.execute(eq(placeBidScript), anyList(), any(Object[].class)))
                    .thenReturn(luaResult);

            bidService.placeBid(AUCTION_ID, BIDDER_ID, BIDDER_USERNAME, new BigDecimal("50.00"));

            // Only BidPlacedEvent should be published
            verify(kafkaTemplate, times(1)).send(
                    eq(KafkaTopics.AUCTION_EVENTS), eq(String.valueOf(AUCTION_ID)),
                    argThat(event -> event instanceof BidPlacedEvent));
            verify(kafkaTemplate, never()).send(
                    anyString(), anyString(),
                    argThat(event -> event instanceof BidOutbidEvent));
        }

        @Test
        @DisplayName("竞价超越他人 - 同时发 BidPlacedEvent 和 BidOutbidEvent")
        void placeBid_outbidsOther_sendsBothEvents() {
            List<Object> luaResult = Arrays.asList(1L, "OK", "5", "100.00");
            when(redis.execute(eq(placeBidScript), anyList(), any(Object[].class)))
                    .thenReturn(luaResult);

            bidService.placeBid(AUCTION_ID, BIDDER_ID, BIDDER_USERNAME, new BigDecimal("150.00"));

            ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
            verify(kafkaTemplate, times(2)).send(
                    eq(KafkaTopics.AUCTION_EVENTS), eq(String.valueOf(AUCTION_ID)),
                    eventCaptor.capture());

            List<Object> sentEvents = eventCaptor.getAllValues();
            assertThat(sentEvents).hasSize(2);
            assertThat(sentEvents).anyMatch(e -> e instanceof BidPlacedEvent);
            assertThat(sentEvents).anyMatch(e -> e instanceof BidOutbidEvent);

            // Verify outbid event content
            BidOutbidEvent outbidEvent = sentEvents.stream()
                    .filter(e -> e instanceof BidOutbidEvent)
                    .map(e -> (BidOutbidEvent) e)
                    .findFirst()
                    .orElseThrow();
            assertThat(outbidEvent.getOutbidUserId()).isEqualTo(5L);
            assertThat(outbidEvent.getNewBidderId()).isEqualTo(BIDDER_ID);
        }

        @Test
        @DisplayName("同一用户连续出价 - 不发出价被超事件")
        void placeBid_sameUser_doesNotSendOutbid() {
            List<Object> luaResult = Arrays.asList(1L, "OK", String.valueOf(BIDDER_ID), "100.00");
            when(redis.execute(eq(placeBidScript), anyList(), any(Object[].class)))
                    .thenReturn(luaResult);

            bidService.placeBid(AUCTION_ID, BIDDER_ID, BIDDER_USERNAME, new BigDecimal("120.00"));

            verify(kafkaTemplate, times(1)).send(
                    anyString(), anyString(),
                    argThat(event -> event instanceof BidPlacedEvent));
            verify(kafkaTemplate, never()).send(
                    anyString(), anyString(),
                    argThat(event -> event instanceof BidOutbidEvent));
        }
    }
}

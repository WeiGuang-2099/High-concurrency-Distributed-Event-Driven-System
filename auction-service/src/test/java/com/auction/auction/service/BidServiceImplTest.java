package com.auction.auction.service;

import com.auction.auction.controller.dto.PlaceBidResponse;
import com.auction.auction.service.impl.BidServiceImpl;
import com.auction.common.exception.BusinessException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Publishing is BidOutboxRelay's job now: the script records the bid in the outbox
 * stream atomically with accepting it, and this service only evaluates the result.
 * See BidOutboxRelayTest for the event side.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("BidService - 拍卖竞价核心逻辑")
class BidServiceImplTest {

    @Mock private StringRedisTemplate redis;
    @Mock private DefaultRedisScript<List> placeBidScript;

    private BidServiceImpl bidService;

    private static final Long AUCTION_ID = 100L;
    private static final Long BIDDER_ID = 1L;
    private static final String BIDDER_USERNAME = "bidder1";
    private static final long OUTBOX_MAX_LEN = 1_000_000L;

    @BeforeEach
    void setUp() {
        bidService = new BidServiceImpl(redis, placeBidScript, OUTBOX_MAX_LEN);
    }

    @Nested
    @DisplayName("placeBid() 竞价出价")
    class PlaceBidTest {

        @Test
        @DisplayName("正常竞价 - 出价高于当前最高价时被接受")
        void placeBid_higherThanCurrent_success() {
            // Lua returns {1=accepted, "OK", prevBidder, prevAmount, streamId}
            when(redis.execute(eq(placeBidScript), anyList(), any(Object[].class)))
                    .thenReturn(Arrays.asList(1L, "OK", "5", "100.00", "1700000000000-0"));

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
            when(redis.execute(eq(placeBidScript), anyList(), any(Object[].class)))
                    .thenReturn(Arrays.asList(0L, "BID_TOO_LOW", "5", "200.00", ""));

            assertThatThrownBy(() -> bidService.placeBid(
                    AUCTION_ID, BIDDER_ID, BIDDER_USERNAME, new BigDecimal("150.00")))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> {
                        BusinessException be = (BusinessException) ex;
                        assertThat(be.getCode()).isEqualTo(400);
                        assertThat(be.getMessage()).contains("Bid too low");
                    });
        }

        @Test
        @DisplayName("拍卖未激活 - 抛出400异常")
        void placeBid_auctionNotActive_throws400() {
            when(redis.execute(eq(placeBidScript), anyList(), any(Object[].class)))
                    .thenReturn(Arrays.asList(0L, "AUCTION_NOT_ACTIVE", "", "", ""));

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
        @DisplayName("出价请求 - outbox 流必须作为 KEY 传给脚本，且带齐出价上下文")
        void placeBid_passesOutboxStreamAsKeyWithBidContext() {
            when(redis.execute(eq(placeBidScript), anyList(), any(Object[].class)))
                    .thenReturn(Arrays.asList(1L, "OK", "", "", "1700000000000-0"));

            bidService.placeBid(AUCTION_ID, BIDDER_ID, BIDDER_USERNAME, new BigDecimal("50.00"));

            @SuppressWarnings("unchecked")
            ArgumentCaptor<List<String>> keysCaptor = ArgumentCaptor.forClass(List.class);
            ArgumentCaptor<Object[]> argsCaptor = ArgumentCaptor.forClass(Object[].class);
            verify(redis).execute(eq(placeBidScript), keysCaptor.capture(), argsCaptor.capture());

            // The stream has to be a KEY, not an ARGV: Redis must see every key the
            // script touches.
            assertThat(keysCaptor.getValue()).containsExactly(
                    RedisKeys.auctionStatus(AUCTION_ID),
                    RedisKeys.auctionHighest(AUCTION_ID),
                    RedisKeys.bidOutboxStream());

            Object[] args = argsCaptor.getValue();
            assertThat(args[0]).isEqualTo(String.valueOf(BIDDER_ID));
            assertThat(args[1]).isEqualTo("50.00");
            assertThat(args[3]).isEqualTo(String.valueOf(AUCTION_ID));
            assertThat(args[4]).isEqualTo(BIDDER_USERNAME);
            assertThat(args[5]).isEqualTo(String.valueOf(OUTBOX_MAX_LEN));
        }
    }
}

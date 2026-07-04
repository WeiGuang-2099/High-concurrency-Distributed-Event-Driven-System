package com.auction.auction.startup;

import com.auction.auction.domain.entity.Auction;
import com.auction.auction.domain.entity.Bid;
import com.auction.auction.domain.enums.AuctionStatus;
import com.auction.auction.repository.AuctionMapper;
import com.auction.auction.repository.BidMapper;
import com.auction.auction.service.RedisKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("AuctionStateWarmer - Redis重启后从MySQL恢复ACTIVE拍卖的竞价状态")
class AuctionStateWarmerTest {

    @Mock private AuctionMapper auctionMapper;
    @Mock private BidMapper bidMapper;
    @Mock private StringRedisTemplate redis;
    @Mock private ValueOperations<String, String> valueOps;
    @Mock private HashOperations<String, Object, Object> hashOps;

    private AuctionStateWarmer warmer;

    @BeforeEach
    void setUp() {
        warmer = new AuctionStateWarmer(auctionMapper, bidMapper, redis);
        lenient().when(redis.opsForValue()).thenReturn(valueOps);
        lenient().when(redis.opsForHash()).thenReturn(hashOps);
    }

    private Auction activeAuction(Long id, String startingPrice) {
        Auction auction = new Auction();
        auction.setId(id);
        auction.setStartingPrice(new BigDecimal(startingPrice));
        auction.setStatus(AuctionStatus.ACTIVE);
        return auction;
    }

    @Test
    @DisplayName("键丢失且有历史出价 - 按最高出价重建 status 与 highest hash")
    void missingKeys_restoredFromHighestBid() {
        Auction auction = activeAuction(42L, "100");
        when(auctionMapper.findByStatus("ACTIVE")).thenReturn(List.of(auction));
        when(valueOps.setIfAbsent(RedisKeys.auctionStatus(42L), "ACTIVE")).thenReturn(true);
        when(redis.hasKey(RedisKeys.auctionHighest(42L))).thenReturn(false);

        LocalDateTime bidTime = LocalDateTime.of(2026, 7, 1, 12, 0);
        Bid highest = new Bid();
        highest.setUserId(7L);
        highest.setAmount(new BigDecimal("150.50"));
        highest.setBidTime(bidTime);
        when(bidMapper.findHighestBid(42L)).thenReturn(highest);

        warmer.run(null);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<Object, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(hashOps).putAll(eq(RedisKeys.auctionHighest(42L)), captor.capture());
        Map<Object, Object> hash = captor.getValue();
        assertThat(hash.get("bidder_id")).isEqualTo("7");
        assertThat(hash.get("amount")).isEqualTo("150.50");
        assertThat(hash.get("bid_time")).isEqualTo(
                String.valueOf(bidTime.toInstant(ZoneOffset.UTC).toEpochMilli()));
    }

    @Test
    @DisplayName("键丢失且无出价 - 用起拍价重建（bidder为空）")
    void missingKeys_noBids_restoredFromStartingPrice() {
        Auction auction = activeAuction(42L, "100");
        when(auctionMapper.findByStatus("ACTIVE")).thenReturn(List.of(auction));
        when(valueOps.setIfAbsent(RedisKeys.auctionStatus(42L), "ACTIVE")).thenReturn(true);
        when(redis.hasKey(RedisKeys.auctionHighest(42L))).thenReturn(false);
        when(bidMapper.findHighestBid(42L)).thenReturn(null);

        warmer.run(null);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<Object, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(hashOps).putAll(eq(RedisKeys.auctionHighest(42L)), captor.capture());
        Map<Object, Object> hash = captor.getValue();
        assertThat(hash.get("bidder_id")).isEqualTo("");
        assertThat(hash.get("amount")).isEqualTo("100");
        assertThat(hash.get("bid_time")).isEqualTo("0");
    }

    @Test
    @DisplayName("键仍在(Redis没重启) - 绝不覆盖现有实时状态")
    void keysPresent_untouched() {
        Auction auction = activeAuction(42L, "100");
        when(auctionMapper.findByStatus("ACTIVE")).thenReturn(List.of(auction));
        // setIfAbsent is the non-clobbering restore primitive: returns false if key exists
        when(valueOps.setIfAbsent(RedisKeys.auctionStatus(42L), "ACTIVE")).thenReturn(false);
        when(redis.hasKey(RedisKeys.auctionHighest(42L))).thenReturn(true);

        warmer.run(null);

        verify(hashOps, never()).putAll(anyString(), anyMap());
        verify(valueOps, never()).set(anyString(), anyString());
    }

    @Test
    @DisplayName("恢复过程抛异常 - 不阻断应用启动")
    void failure_doesNotPropagate() {
        when(auctionMapper.findByStatus("ACTIVE")).thenThrow(new RuntimeException("db down"));

        warmer.run(null); // must not throw
    }
}

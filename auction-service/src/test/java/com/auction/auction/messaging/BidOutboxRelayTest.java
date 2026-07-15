package com.auction.auction.messaging;

import com.auction.common.event.KafkaTopics;
import com.auction.common.event.auction.BidOutbidEvent;
import com.auction.common.event.auction.BidPlacedEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("BidOutboxRelay - outbox 条目泵到 Kafka")
class BidOutboxRelayTest {

    @Mock private StringRedisTemplate redis;
    @Mock private KafkaTemplate<String, Object> kafkaTemplate;

    private BidOutboxRelay relay;

    private static final String STREAM_ID = "1700000000000-0";
    private static final Long AUCTION_ID = 100L;
    private static final Long BIDDER_ID = 1L;

    @BeforeEach
    void setUp() {
        relay = new BidOutboxRelay(redis, kafkaTemplate, 10_000L);
        lenient().when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));
    }

    private static MapRecord<String, String, String> entry(String prevBidder, String prevAmount) {
        Map<String, String> body = new LinkedHashMap<>();
        body.put("auctionId", String.valueOf(AUCTION_ID));
        body.put("bidderId", String.valueOf(BIDDER_ID));
        body.put("bidderUsername", "bidder1");
        body.put("amount", "150.00");
        body.put("bidTime", "1700000000000");
        body.put("prevBidder", prevBidder);
        body.put("prevAmount", prevAmount);
        return MapRecord.create("auction:bid:outbox", body).withId(RecordId.of(STREAM_ID));
    }

    @Test
    @DisplayName("首次出价 - 无前一出价者时只发 BidPlacedEvent")
    void firstBid_publishesOnlyBidPlaced() {
        boolean acked = relay.relay(entry("", ""));

        assertThat(acked).isTrue();
        verify(kafkaTemplate, times(1)).send(
                eq(KafkaTopics.AUCTION_EVENTS), eq(String.valueOf(AUCTION_ID)),
                argThat(e -> e instanceof BidPlacedEvent));
        verify(kafkaTemplate, never()).send(
                anyString(), anyString(), argThat(e -> e instanceof BidOutbidEvent));
    }

    @Test
    @DisplayName("超越他人 - 同时发 BidPlacedEvent 和 BidOutbidEvent")
    void outbidsOther_publishesBothEvents() {
        boolean acked = relay.relay(entry("5", "100.00"));

        assertThat(acked).isTrue();
        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate, times(2)).send(
                eq(KafkaTopics.AUCTION_EVENTS), eq(String.valueOf(AUCTION_ID)), captor.capture());

        List<Object> sent = captor.getAllValues();
        assertThat(sent).anyMatch(e -> e instanceof BidPlacedEvent);
        BidOutbidEvent outbid = sent.stream()
                .filter(BidOutbidEvent.class::isInstance)
                .map(BidOutbidEvent.class::cast)
                .findFirst().orElseThrow();
        assertThat(outbid.getOutbidUserId()).isEqualTo(5L);
        assertThat(outbid.getNewBidderId()).isEqualTo(BIDDER_ID);
    }

    @Test
    @DisplayName("同一用户连续出价 - 不发出价被超事件")
    void sameUserAgain_doesNotPublishOutbid() {
        relay.relay(entry(String.valueOf(BIDDER_ID), "100.00"));

        verify(kafkaTemplate, times(1)).send(
                anyString(), anyString(), argThat(e -> e instanceof BidPlacedEvent));
        verify(kafkaTemplate, never()).send(
                anyString(), anyString(), argThat(e -> e instanceof BidOutbidEvent));
    }

    @Test
    @DisplayName("事件 id 由 stream 条目 id 派生 - 重投必须产生同一 id，否则下游去重失效")
    void eventIds_areDerivedFromStreamId_soReplayIsDeduplicable() {
        relay.relay(entry("5", "100.00"));
        relay.relay(entry("5", "100.00")); // same entry redelivered after a crash

        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate, times(4)).send(anyString(), anyString(), captor.capture());

        List<String> placedIds = captor.getAllValues().stream()
                .filter(BidPlacedEvent.class::isInstance)
                .map(e -> ((BidPlacedEvent) e).getEventId())
                .distinct().toList();
        List<String> outbidIds = captor.getAllValues().stream()
                .filter(BidOutbidEvent.class::isInstance)
                .map(e -> ((BidOutbidEvent) e).getEventId())
                .distinct().toList();

        assertThat(placedIds).containsExactly(STREAM_ID + ":placed");
        assertThat(outbidIds).containsExactly(STREAM_ID + ":outbid");
    }

    @Test
    @DisplayName("Kafka 发送失败 - 不 ack，条目留在 PEL 等待重试")
    void kafkaFailure_doesNotAck() {
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        boolean acked = relay.relay(entry("", ""));

        assertThat(acked).isFalse();
    }
}

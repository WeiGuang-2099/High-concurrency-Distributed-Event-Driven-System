package com.auction.eventstore.consumer;

import com.auction.common.event.BaseEvent;
import com.auction.common.event.KafkaTopics;
import com.auction.eventstore.service.CacheInvalidationService;
import com.auction.eventstore.service.EventStoreService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("EventStoreConsumer - 事件落库失败必须走重试/DLT，不能静默丢弃")
class EventStoreConsumerTest {

    @Mock private EventStoreService eventStoreService;
    @Mock private CacheInvalidationService cacheInvalidationService;
    @Mock private Acknowledgment ack;

    @InjectMocks private EventStoreConsumer consumer;

    @Test
    @DisplayName("append 抛异常 - 异常向容器传播且不 ack（触发错误处理器重试/DLT）")
    void appendFailure_propagatesAndDoesNotAck() {
        BaseEvent event = new BaseEvent("123", "BidPlaced");
        when(eventStoreService.append(any(BaseEvent.class), anyString(), anyString()))
                .thenThrow(new RuntimeException("mongo down"));

        assertThatThrownBy(() -> consumer.onEvent(event, KafkaTopics.AUCTION_EVENTS, ack))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("mongo down");

        verify(ack, never()).acknowledge();
    }

    @Test
    @DisplayName("append 成功 - 正常 ack")
    void appendSuccess_acks() {
        BaseEvent event = new BaseEvent("123", "BidPlaced");
        when(eventStoreService.append(any(BaseEvent.class), anyString(), anyString()))
                .thenReturn(true);

        consumer.onEvent(event, KafkaTopics.AUCTION_EVENTS, ack);

        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("非事件负载 - 跳过并 ack")
    void nonEventPayload_ackedAndSkipped() {
        consumer.onEvent("not-an-event", KafkaTopics.AUCTION_EVENTS, ack);

        verify(eventStoreService, never()).append(any(), anyString(), anyString());
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("缓存失效失败 - 不影响 ack（缓存只是优化，不参与一致性）")
    void cacheInvalidationFailure_stillAcks() {
        BaseEvent event = new BaseEvent("123", "BidPlaced");
        when(eventStoreService.append(any(BaseEvent.class), anyString(), anyString()))
                .thenReturn(true);
        doThrow(new RuntimeException("redis down"))
                .when(cacheInvalidationService).invalidateAuctionSummary(123L);

        consumer.onEvent(event, KafkaTopics.AUCTION_EVENTS, ack);

        verify(ack).acknowledge();
    }
}

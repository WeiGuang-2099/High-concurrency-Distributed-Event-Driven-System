package com.auction.auction.it;

import com.auction.auction.config.RedisConfig;
import com.auction.auction.messaging.BidOutboxReclaimer;
import com.auction.auction.messaging.BidOutboxRelay;
import com.auction.auction.messaging.BidOutboxRelayConfig;
import com.auction.auction.service.BidService;
import com.auction.auction.service.RedisKeys;
import com.auction.auction.service.impl.BidServiceImpl;
import com.auction.common.event.KafkaTopics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The bid outbox against real Redis and Kafka.
 *
 * <p>The property this exists to prove: a bid the API reports as placed is never lost,
 * because place_bid.lua accepts it and appends it to the outbox stream in one atomic
 * script. Publishing is then somebody else's problem -- if the relay dies mid-flight,
 * the entry is still in the stream and the reclaimer picks it up.</p>
 *
 * <p>The relay listener is disabled here (auction.bid.outbox.relay.enabled=false) so the
 * test can drive each step; a live listener would race it for every entry.</p>
 */
@SpringBootTest(classes = BidOutboxIT.OutboxTestApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.cloud.config.enabled=false",
                "eureka.client.enabled=false",
                "management.tracing.enabled=false",
                // Drive the relay by hand: a live listener races the test for entries.
                "auction.bid.outbox.relay.enabled=false",
                "auction.bid.outbox.reclaim-idle-ms=0"})
@DisplayName("出价 outbox - 接受与持久记录原子，且崩溃后可恢复")
class BidOutboxIT {

    /**
     * Slice: the bid path needs Redis and Kafka but never touches MySQL, which is the
     * whole point of the design being tested.
     */
    @SpringBootConfiguration
    @EnableAutoConfiguration(exclude = DataSourceAutoConfiguration.class)
    @Import({BidServiceImpl.class, RedisConfig.class, BidOutboxRelay.class,
            BidOutboxRelayConfig.class, BidOutboxReclaimer.class})
    static class OutboxTestApp {
    }

    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.4"));
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    static {
        KAFKA.start();
        REDIS.start();
    }

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    private static final String GROUP = "bid-outbox-relay";
    private static final Long AUCTION_ID = 4242L;

    @Autowired private BidService bidService;
    @Autowired private BidOutboxRelay relay;
    @Autowired private BidOutboxReclaimer reclaimer;
    @Autowired private StringRedisTemplate redis;

    @BeforeEach
    void setUp() {
        redis.delete(RedisKeys.bidOutboxStream());
        redis.delete(RedisKeys.auctionHighest(AUCTION_ID));
        redis.opsForValue().set(RedisKeys.auctionStatus(AUCTION_ID), "ACTIVE");
        try {
            redis.opsForStream().createGroup(RedisKeys.bidOutboxStream(), ReadOffset.from("0"), GROUP);
        } catch (Exception alreadyExists) {
            // group survives the stream delete only if the stream did; ignore either way
        }
    }

    @Test
    @DisplayName("接受出价即写入 outbox - 无需 Kafka 参与，出价已经持久")
    void acceptedBid_isRecordedInOutbox_withoutKafkaInvolvement() {
        bidService.placeBid(AUCTION_ID, 7L, "alice", new BigDecimal("100.00"));

        List<MapRecord<String, Object, Object>> entries =
                redis.opsForStream().range(RedisKeys.bidOutboxStream(), Range.unbounded());

        assertThat(entries).hasSize(1);
        Map<Object, Object> body = entries.get(0).getValue();
        assertThat(body.get("auctionId")).isEqualTo(String.valueOf(AUCTION_ID));
        assertThat(body.get("bidderId")).isEqualTo("7");
        assertThat(body.get("bidderUsername")).isEqualTo("alice");
        assertThat(body.get("amount")).isEqualTo("100.00");
    }

    @Test
    @DisplayName("被拒出价不进 outbox - 否则会凭空造出一笔没发生的出价")
    void rejectedBid_isNotRecorded() {
        bidService.placeBid(AUCTION_ID, 7L, "alice", new BigDecimal("100.00"));
        try {
            bidService.placeBid(AUCTION_ID, 8L, "bob", new BigDecimal("50.00"));
        } catch (RuntimeException expected) {
            // BID_TOO_LOW
        }

        assertThat(redis.opsForStream().size(RedisKeys.bidOutboxStream())).isEqualTo(1);
    }

    @Test
    @DisplayName("relay 泵到 Kafka 后才 ack - 事件到达 broker，PEL 清空")
    void relayPublishesThenAcknowledges() {
        bidService.placeBid(AUCTION_ID, 7L, "alice", new BigDecimal("100.00"));

        MapRecord<String, String, String> entry = readAsConsumer("relay-1");
        assertThat(relay.relay(entry)).isTrue();
        relay.acknowledge(GROUP, entry.getId());

        assertThat(kafkaContains(entry.getId().getValue() + ":placed")).isTrue();
        assertThat(pendingCount()).isZero();
    }

    @Test
    @DisplayName("relay 读了但没 ack 就崩溃 - 条目留在 PEL，reclaimer 恢复并发布")
    void entryOrphanedByCrashedRelay_isReclaimedAndPublished() {
        bidService.placeBid(AUCTION_ID, 7L, "alice", new BigDecimal("100.00"));

        // A relay instance reads the entry and dies before publishing: the entry is now
        // pending, owned by a consumer that will never come back.
        MapRecord<String, String, String> entry = readAsConsumer("relay-that-died");
        assertThat(pendingCount()).isEqualTo(1);
        assertThat(kafkaContains(entry.getId().getValue() + ":placed")).isFalse();

        reclaimer.reclaimStalePending();

        assertThat(kafkaContains(entry.getId().getValue() + ":placed"))
                .as("orphaned bid reaches Kafka after reclaim")
                .isTrue();
        assertThat(pendingCount()).isZero();
    }

    private MapRecord<String, String, String> readAsConsumer(String consumer) {
        List<MapRecord<String, String, String>> records = redis.<String, String>opsForStream().read(
                Consumer.from(GROUP, consumer),
                StreamReadOptions.empty().count(10),
                StreamOffset.create(RedisKeys.bidOutboxStream(), ReadOffset.lastConsumed()));
        assertThat(records).isNotEmpty();
        return records.get(0);
    }

    private long pendingCount() {
        PendingMessages pending = redis.<String, String>opsForStream()
                .pending(RedisKeys.bidOutboxStream(), GROUP, Range.unbounded(), 100);
        return pending == null ? 0 : pending.size();
    }

    private boolean kafkaContains(String eventId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "probe-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        long deadline = System.currentTimeMillis() + 15_000;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(KafkaTopics.AUCTION_EVENTS));
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (record.value() != null && record.value().contains(eventId)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}

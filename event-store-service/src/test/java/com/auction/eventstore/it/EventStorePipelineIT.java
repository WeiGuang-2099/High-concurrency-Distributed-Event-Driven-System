package com.auction.eventstore.it;

import com.auction.common.event.BaseEvent;
import com.auction.common.event.KafkaTopics;
import com.auction.common.event.ticket.StockReservedEvent;
import com.auction.eventstore.domain.EventDocument;
import com.auction.eventstore.repository.EventStoreRepository;
import com.auction.eventstore.service.EventStoreService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * End-to-end event-store pipeline against real Kafka and MongoDB containers:
 * a published event must land in the append-only store, and an event whose
 * append keeps failing must end up on the dead-letter topic instead of being
 * acked away (the 2026-07-04 event-loss fix, verified over the wire).
 */
@SpringBootTest(properties = "management.tracing.enabled=false")
class EventStorePipelineIT {

    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.4"));
    static final MongoDBContainer MONGO =
            new MongoDBContainer(DockerImageName.parse("mongo:7"));
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    static {
        KAFKA.start();
        MONGO.start();
        REDIS.start();
    }

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.data.mongodb.uri", () -> MONGO.getReplicaSetUrl("event_store"));
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    @SpyBean
    private EventStoreService eventStoreService;

    @Autowired
    private EventStoreRepository eventRepository;

    @Test
    void publishedEvent_isAppendedToMongo() throws Exception {
        StockReservedEvent event = new StockReservedEvent(1234L, "VIP", 555L, 42L, 2);

        KafkaTemplate<String, Object> producer = newJsonProducer();
        try {
            producer.send(KafkaTopics.TICKET_EVENTS, event.getAggregateId(), event)
                    .get(30, TimeUnit.SECONDS);

            await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofMillis(500))
                    .untilAsserted(() -> assertThat(
                            eventRepository.findByEventId(event.getEventId())).isPresent());
        } finally {
            producer.destroy();
        }

        EventDocument doc = eventRepository.findByEventId(event.getEventId()).orElseThrow();
        assertThat(doc.getAggregateId()).isEqualTo("1234");
        assertThat(doc.getAggregateType()).isEqualTo("Ticket");
        assertThat(doc.getEventType()).isEqualTo("StockReserved");
        assertThat(doc.getTopic()).isEqualTo(KafkaTopics.TICKET_EVENTS);
        assertThat(doc.getPayload()).contains("\"reservationId\":555");
    }

    @Test
    void appendFailure_routesEventToDeadLetterTopic_afterRetries() throws Exception {
        StockReservedEvent poison = new StockReservedEvent(4321L, "VIP", 777L, 43L, 1);
        String poisonId = poison.getEventId();

        doThrow(new RuntimeException("simulated MongoDB outage"))
                .when(eventStoreService)
                .append(argThat(e -> e != null && poisonId.equals(e.getEventId())),
                        anyString(), anyString());

        KafkaTemplate<String, Object> producer = newJsonProducer();
        try {
            producer.send(KafkaTopics.TICKET_EVENTS, poison.getAggregateId(), poison)
                    .get(30, TimeUnit.SECONDS);
        } finally {
            producer.destroy();
        }

        assertThat(pollDltForEvent(poisonId, Duration.ofSeconds(120)))
                .as("poisoned event published to " + KafkaTopics.TICKET_EVENTS_DLT)
                .isTrue();

        // FixedBackOff(1000ms, 3): one initial delivery plus three retries.
        verify(eventStoreService, times(4))
                .append(argThat((BaseEvent e) -> e != null && poisonId.equals(e.getEventId())),
                        anyString(), anyString());
        assertThat(eventRepository.findByEventId(poisonId)).isEmpty();
    }

    private boolean pollDltForEvent(String eventId, Duration timeout) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-probe-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        long deadline = System.currentTimeMillis() + timeout.toMillis();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(KafkaTopics.TICKET_EVENTS_DLT));
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(1))) {
                    if (record.value() != null && record.value().contains(eventId)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static KafkaTemplate<String, Object> newJsonProducer() {
        // Mirrors the producing services: JsonSerializer adds the __TypeId__ header
        // that the consumer's JsonMessageConverter uses to resolve the event subtype.
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }
}

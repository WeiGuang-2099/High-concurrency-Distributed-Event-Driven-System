package com.auction.notification.it;

import com.auction.common.event.KafkaTopics;
import com.auction.common.event.auction.BidOutbidEvent;
import com.auction.common.event.ticket.StockConfirmedEvent;
import com.auction.notification.domain.entity.Notification;
import com.auction.notification.repository.NotificationRepository;
import com.auction.notification.service.NotificationPersistenceService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.core.io.FileSystemResource;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * notification-service end to end: an event published the way the producing services
 * publish it (JsonSerializer, so it carries the __TypeId__ header) must come out the other
 * side as a persisted notification, through the real {@code @KafkaListener} containers and
 * the production listener factory.
 *
 * <p>This module had no tests at all, which is how a listener factory without a manual ack
 * mode survived: both listeners take an {@code Acknowledgment}, spring-kafka refuses to
 * invoke them without one, so every record was retried and dead-lettered before the method
 * body ran. The service started cleanly and delivered nothing.</p>
 *
 * <p>Nothing here calls a consumer method directly; the only input is a Kafka record and
 * the only thing asserted is the observable result.</p>
 */
@SpringBootTest(properties = {
        // Keep the test hermetic: no config-server fetch, no Eureka registration.
        "spring.cloud.config.enabled=false",
        "eureka.client.enabled=false",
        "management.tracing.enabled=false"})
@DisplayName("Notification 管道 - 事件经真实监听器落库")
@ExtendWith(OutputCaptureExtension.class)
class NotificationPipelineIT {

    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.4"));
    static final MongoDBContainer MONGO =
            new MongoDBContainer(DockerImageName.parse("mongo:7"));

    static {
        KAFKA.start();
        MONGO.start();
    }

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.data.mongodb.uri", () -> MONGO.getReplicaSetUrl("notification_db"));
        // The WebSocket interceptor needs the JWT certificate at startup. Config server is
        // off, so read the same value it would have served, from the same file.
        registry.add("jwt.public-key", () -> configRepoProperty("notification-service.yml", "jwt.public-key"));
    }

    @Autowired
    private NotificationRepository notificationRepository;

    // Spy, not mock: unstubbed calls reach the real service, so the happy-path tests still
    // write to MongoDB and only the dead-letter test's one user is made to fail.
    @SpyBean
    private NotificationPersistenceService persistenceService;

    @Test
    @DisplayName("BidOutbid 事件 - 被超价的用户得到一条持久化通知，且事件没有进死信")
    void outbidEvent_persistsANotification_andIsNotDeadLettered() throws Exception {
        long outbidUserId = 7_100_001L;
        BidOutbidEvent event = new BidOutbidEvent(
                9001L, outbidUserId, new BigDecimal("240.00"), 7_100_002L, new BigDecimal("241.00"));

        publish(KafkaTopics.AUCTION_EVENTS, event.getAggregateId(), event);

        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> assertThat(notificationRepository.findByUserIdOrderByCreatedAtDesc(outbidUserId))
                        .as("OUTBID notification for the outbid user")
                        .extracting(Notification::getType)
                        .containsExactly("OUTBID"));

        Notification saved = notificationRepository.findByUserIdOrderByCreatedAtDesc(outbidUserId).get(0);
        assertThat(saved.getContent()).contains("240.00").contains("241.00");
        assertThat(saved.isRead()).isFalse();

        assertThat(appearsOnTopic(KafkaTopics.AUCTION_EVENTS_DLT, event.getEventId(), Duration.ofSeconds(8)))
                .as("a handled event must not also be dead-lettered")
                .isFalse();
    }

    @Test
    @DisplayName("StockConfirmed 事件 - 第二个监听器同样经真实容器落库")
    void stockConfirmedEvent_persistsANotification_andIsNotDeadLettered() throws Exception {
        long userId = 7_200_001L;
        StockConfirmedEvent event = new StockConfirmedEvent(880_001L, userId);

        publish(KafkaTopics.TICKET_EVENTS, event.getAggregateId(), event);

        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> assertThat(notificationRepository.findByUserIdOrderByCreatedAtDesc(userId))
                        .as("STOCK_CONFIRMED notification for the buyer")
                        .extracting(Notification::getType)
                        .containsExactly("STOCK_CONFIRMED"));

        assertThat(appearsOnTopic(KafkaTopics.TICKET_EVENTS_DLT, event.getEventId(), Duration.ofSeconds(8)))
                .as("a handled event must not also be dead-lettered")
                .isFalse();
    }

    @Test
    @DisplayName("处理持续失败 - 重试后进入死信主题并留下一行错误日志，而不是被跳过")
    void handlerFailure_deadLettersTheEvent_andSaysSo(CapturedOutput output) throws Exception {
        long poisonUserId = 7_300_001L;
        doThrow(new RuntimeException("simulated MongoDB outage"))
                .when(persistenceService)
                .save(argThat(n -> n != null && Long.valueOf(poisonUserId).equals(n.getUserId())));

        BidOutbidEvent poison = new BidOutbidEvent(
                9002L, poisonUserId, new BigDecimal("50.00"), 7_300_002L, new BigDecimal("51.00"));
        publish(KafkaTopics.AUCTION_EVENTS, poison.getAggregateId(), poison);

        assertThat(appearsOnTopic(KafkaTopics.AUCTION_EVENTS_DLT, poison.getEventId(), Duration.ofSeconds(60)))
                .as("an event whose handling keeps failing is published to " + KafkaTopics.AUCTION_EVENTS_DLT)
                .isTrue();

        // FixedBackOff(1000ms, 3): one initial delivery plus three retries.
        verify(persistenceService, times(4))
                .save(argThat(n -> n != null && Long.valueOf(poisonUserId).equals(n.getUserId())));
        assertThat(notificationRepository.findByUserIdOrderByCreatedAtDesc(poisonUserId)).isEmpty();

        // Dead-lettering that succeeds is otherwise logged by nobody above DEBUG, which is
        // how a service could dead-letter everything it received without anyone noticing.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(output.getOut()).contains("Dead-lettered " + KafkaTopics.AUCTION_EVENTS));
    }

    private static void publish(String topic, String key, Object event) throws Exception {
        // Mirrors the producing services: JsonSerializer adds the __TypeId__ header that
        // the consumer's JsonMessageConverter uses to resolve the event subtype.
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(props);
        try {
            new KafkaTemplate<>(factory).send(topic, key, event).get(30, TimeUnit.SECONDS);
        } finally {
            factory.destroy();
        }
    }

    private static boolean appearsOnTopic(String topic, String eventId, Duration window) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-probe-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        long deadline = System.currentTimeMillis() + window.toMillis();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
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

    /** Reads one property from a file in config-repo/, walking up from the module directory. */
    private static String configRepoProperty(String file, String key) {
        Path dir = Paths.get("").toAbsolutePath();
        while (dir != null) {
            Path candidate = dir.resolve("config-repo").resolve(file);
            if (Files.exists(candidate)) {
                YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
                yaml.setResources(new FileSystemResource(candidate));
                String value = yaml.getObject().getProperty(key);
                if (value == null) {
                    throw new IllegalStateException(key + " not found in " + candidate);
                }
                return value;
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException("Cannot locate config-repo/" + file);
    }
}

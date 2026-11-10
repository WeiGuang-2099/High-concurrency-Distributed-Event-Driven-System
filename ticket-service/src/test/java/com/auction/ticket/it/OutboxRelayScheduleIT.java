package com.auction.ticket.it;

import com.auction.common.event.KafkaTopics;
import com.auction.ticket.controller.dto.CreateTicketRequest;
import com.auction.ticket.controller.dto.ReserveRequest;
import com.auction.ticket.domain.entity.OutboxEvent;
import com.auction.ticket.repository.OutboxMapper;
import com.auction.ticket.service.TicketStockService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The outbox relay driven by its real trigger: the {@code @Scheduled} poll, with the
 * production relay settings.
 *
 * <p>{@link OutboxIT} proves the mechanism (claim, publish, mark) by switching the schedule
 * off and calling {@code drainOnce()} by hand. That left the one thing production depends on
 * untested: that something actually calls the relay. Nothing did -- the application had no
 * {@code @EnableScheduling}, so every ticket event since the outbox was introduced sat in
 * the table unpublished while OutboxIT stayed green.</p>
 *
 * <p>So this test never touches the relay bean. It makes a reservation and waits for the
 * event to show up on Kafka on its own.</p>
 */
@SpringBootTest(properties = {
        "spring.rabbitmq.listener.simple.auto-startup=false",
        "spring.cloud.config.enabled=false",
        "eureka.client.enabled=false",
        "management.tracing.enabled=false"})
@DisplayName("Ticket outbox relay - 由真实的定时调度驱动")
class OutboxRelayScheduleIT {

    static final MySQLContainer<?> MYSQL = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
            .withDatabaseName("ticket_db")
            .withCopyFileToContainer(
                    MountableFile.forHostPath(repoFile("deploy/mysql/init/03-ticket-db.sql")),
                    "/docker-entrypoint-initdb.d/01-ticket-db.sql");

    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.4"));

    static {
        MYSQL.start();
        REDIS.start();
        KAFKA.start();
    }

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    /** The delayed-release timer is not under test here; StockReleaseTimeoutIT covers it. */
    @MockBean
    private RabbitTemplate rabbitTemplate;

    @Autowired private TicketStockService ticketStockService;
    @Autowired private OutboxMapper outboxMapper;

    @Test
    @DisplayName("预留之后无人调用 relay - 事件仍由定时任务发布到 Kafka")
    void reservation_isPublishedByTheScheduledRelay_withNoManualDrain() {
        long eventId = 950001L;
        CreateTicketRequest create = new CreateTicketRequest();
        create.setEventId(eventId);
        create.setTicketType("VIP");
        create.setTotalQuantity(5);
        ticketStockService.createTicketStock(create);

        ReserveRequest request = new ReserveRequest();
        request.setEventId(eventId);
        request.setTicketType("VIP");
        request.setQuantity(1);
        Long reservationId = ticketStockService.reserve(42L, request).getReservationId();

        OutboxEvent reserved = outboxMapper.selectList(new QueryWrapper<OutboxEvent>()
                        .eq("event_type", "StockReserved")
                        .eq("message_key", String.valueOf(reservationId)))
                .stream().findFirst().orElseThrow();

        // Production poll interval is 1s; 30s is generous even on a loaded CI runner.
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(250))
                .untilAsserted(() -> assertThat(outboxMapper.selectById(reserved.getId()).getSentAt())
                        .as("the scheduled relay stamps sent_at without anyone calling drainOnce()")
                        .isNotNull());

        assertThat(kafkaPayloadFor(reserved.getEventId()))
                .contains("\"reservationId\":" + reservationId)
                .contains("\"eventType\":\"StockReserved\"");
    }

    private String kafkaPayloadFor(String eventId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "probe-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        long deadline = System.currentTimeMillis() + 20_000;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(KafkaTopics.TICKET_EVENTS));
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (record.value() != null && record.value().contains(eventId)) {
                        return record.value();
                    }
                }
            }
        }
        throw new AssertionError("event " + eventId + " never reached Kafka");
    }

    private static Path repoFile(String relative) {
        Path dir = Paths.get("").toAbsolutePath();
        while (dir != null) {
            Path candidate = dir.resolve(relative);
            if (Files.exists(candidate)) {
                return candidate;
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException("Cannot locate " + relative);
    }
}

package com.auction.ticket.it;

import com.auction.common.event.KafkaTopics;
import com.auction.ticket.controller.dto.CreateTicketRequest;
import com.auction.ticket.controller.dto.ReserveRequest;
import com.auction.ticket.controller.dto.ReserveResponse;
import com.auction.ticket.domain.entity.OutboxEvent;
import com.auction.ticket.event.OutboxRelay;
import com.auction.ticket.event.TicketEventProducer;
import com.auction.ticket.repository.OutboxMapper;
import com.auction.ticket.service.TicketStockService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The transactional outbox on the ticket path, against real MySQL and Kafka.
 *
 * <p>Events used to be published from an afterCommit callback: correct when nothing goes
 * wrong, but a crash between the commit and the send loses the event, and stock has already
 * moved. The event row now commits with the business change, so the only question left is
 * whether the relay eventually publishes it -- which is a question about a durable row
 * rather than a lost in-flight message.</p>
 *
 * <p>The relay's schedule is disabled here so the test drives it explicitly; a background
 * poll would race every assertion about what is still pending. That makes this a test of
 * the mechanism only: it cannot tell whether anything calls the relay in a running service,
 * and for a while nothing did. {@link OutboxRelayScheduleIT} covers the real trigger.</p>
 */
@SpringBootTest(properties = {
        "spring.rabbitmq.listener.simple.auto-startup=false",
        "spring.cloud.config.enabled=false",
        "eureka.client.enabled=false",
        "management.tracing.enabled=false",
        "ticket.outbox.relay.enabled=false"})
@DisplayName("Ticket 事务性 outbox - 事件与业务改动同生共死")
class OutboxIT {

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
    @Autowired private OutboxRelay relay;
    @Autowired private TicketEventProducer eventProducer;
    @Autowired private org.springframework.transaction.support.TransactionTemplate txTemplate;

    /** The outbox table is shared by every test here; start each from a clean slate so
     *  assertions about what is pending mean this test's rows. */
    @BeforeEach
    void clearOutbox() {
        outboxMapper.delete(new QueryWrapper<>());
    }

    @Test
    @DisplayName("预留成功 - 事件行与预留同一事务提交，之后由 relay 发布")
    void reserve_writesEventToOutbox_thenRelayPublishesIt() {
        long eventId = 940001L;
        Long reservationId = reserveOne(eventId);

        // Committed with the reservation, before Kafka has heard anything.
        List<OutboxEvent> pending = unsent();
        assertThat(pending).hasSize(2); // TicketCreated + StockReserved
        OutboxEvent reserved = pending.stream()
                .filter(e -> "StockReserved".equals(e.getEventType()))
                .findFirst().orElseThrow();
        assertThat(reserved.getMessageKey()).isEqualTo(String.valueOf(reservationId));
        assertThat(reserved.getSentAt()).isNull();

        assertThat(relay.drainOnce()).isEqualTo(2);

        assertThat(outboxMapper.selectById(reserved.getId()).getSentAt())
                .as("stamped only after the broker acknowledged").isNotNull();
        assertThat(kafkaPayloadFor(reserved.getEventId()))
                .as("event reached Kafka with its subtype fields intact")
                .contains("\"reservationId\":" + reservationId)
                .contains("\"eventType\":\"StockReserved\"");
    }

    @Test
    @DisplayName("业务事务回滚 - 事件行必须一起消失，不能发出没发生的事")
    void rolledBackBusinessChange_takesTheEventRowWithIt() {
        long eventId = 940002L;
        createStock(eventId, 5);
        relay.drainOnce();

        // reserve() joins this transaction (REQUIRED), so it writes its event row and then
        // the whole thing rolls back -- the case afterCommit publishing could never get
        // wrong, and the outbox must not either.
        assertThatThrownBy(() -> txTemplate.execute(status -> {
            ReserveRequest request = new ReserveRequest();
            request.setEventId(eventId);
            request.setTicketType("VIP");
            request.setQuantity(1);
            ticketStockService.reserve(77L, request);
            throw new IllegalStateException("boom after the event row was written");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(unsent())
                .as("no StockReserved survives a rolled-back reserve")
                .noneMatch(e -> "StockReserved".equals(e.getEventType()));
    }

    @Test
    @DisplayName("relay 发布失败 - 行保持未发送，下一轮重试（不丢事件）")
    void publishFailure_leavesRowUnsent_forRetry() {
        long eventId = 940003L;
        reserveOne(eventId);
        int pendingBefore = unsent().size();

        // Point the relay at a broker that is not there, then restore it.
        try (KafkaTemplateOutage outage = new KafkaTemplateOutage()) {
            assertThat(outage.relayWithDeadBroker().drainOnce())
                    .as("nothing published while the broker is unreachable").isZero();
        }
        assertThat(unsent()).as("rows still pending, nothing lost").hasSize(pendingBefore);

        // A later pass with a working broker publishes them: the claim went stale.
        assertThat(relay.drainOnce()).isEqualTo(pendingBefore);
        assertThat(unsent()).isEmpty();
    }

    @Test
    @DisplayName("没有事务时写 outbox - 必须直接失败，而不是悄悄退化成第二次写")
    void enqueueOutsideTransaction_failsFast() {
        assertThatThrownBy(() -> eventProducer.publishStockConfirmed(1L, 1L))
                .as("MANDATORY propagation refuses to write the outbox without a transaction")
                .isInstanceOf(org.springframework.transaction.IllegalTransactionStateException.class);
    }

    /** A relay wired to a dead broker, to exercise the publish-failure path. */
    private class KafkaTemplateOutage implements AutoCloseable {
        private final org.springframework.kafka.core.DefaultKafkaProducerFactory<String, Object> factory;

        KafkaTemplateOutage() {
            Map<String, Object> props = new HashMap<>();
            props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
            props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    org.apache.kafka.common.serialization.StringSerializer.class);
            props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    org.springframework.kafka.support.serializer.JsonSerializer.class);
            props.put(org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000);
            factory = new org.springframework.kafka.core.DefaultKafkaProducerFactory<>(props);
        }

        OutboxRelay relayWithDeadBroker() {
            return new OutboxRelay(outboxMapper,
                    new org.springframework.kafka.core.KafkaTemplate<>(factory),
                    new com.fasterxml.jackson.databind.ObjectMapper()
                            .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule()),
                    false, 100, 60000L, 2000L);
        }

        @Override
        public void close() {
            factory.destroy();
        }
    }

    private Long reserveOne(long eventId) {
        createStock(eventId, 5);
        ReserveRequest request = new ReserveRequest();
        request.setEventId(eventId);
        request.setTicketType("VIP");
        request.setQuantity(1);
        ReserveResponse response = ticketStockService.reserve(42L, request);
        return response.getReservationId();
    }

    private void createStock(long eventId, int total) {
        CreateTicketRequest create = new CreateTicketRequest();
        create.setEventId(eventId);
        create.setTicketType("VIP");
        create.setTotalQuantity(total);
        ticketStockService.createTicketStock(create);
    }

    private List<OutboxEvent> unsent() {
        return outboxMapper.selectList(new QueryWrapper<OutboxEvent>().isNull("sent_at"));
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

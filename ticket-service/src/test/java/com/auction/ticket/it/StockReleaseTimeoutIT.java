package com.auction.ticket.it;

import com.auction.ticket.config.RabbitMQConfig;
import com.auction.ticket.controller.dto.CreateTicketRequest;
import com.auction.ticket.controller.dto.ReserveRequest;
import com.auction.ticket.controller.dto.ReserveResponse;
import com.auction.ticket.domain.entity.Reservation;
import com.auction.ticket.domain.entity.TicketStock;
import com.auction.ticket.domain.enums.ReservationStatus;
import com.auction.ticket.event.TicketEventProducer;
import com.auction.ticket.repository.ReservationMapper;
import com.auction.ticket.repository.TicketStockMapper;
import com.auction.ticket.service.RedisKeys;
import com.auction.ticket.service.TicketStockService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doThrow;

/**
 * The 30-minute reservation timeout, end to end over a real RabbitMQ with the
 * delayed-message plugin.
 *
 * <p>This path had no integration coverage at all: the other ticket ITs deliberately
 * switch it off ({@code listener.simple.auto-startup=false} and a mocked RabbitTemplate),
 * so nothing exercised the delayed exchange, the Long payload conversion, or the release
 * logic. It is also the path that gives stock back -- if it silently does not run, held
 * stock is never resold and the failure is invisible.</p>
 *
 * <p>Messages are published with a short delay rather than the production 30 minutes; the
 * point is that the x-delayed-message exchange routes and the listener converts, not that
 * RabbitMQ can count to 1800.</p>
 */
@SpringBootTest(properties = {
        "spring.kafka.admin.auto-create=false",
        "spring.cloud.config.enabled=false",
        "eureka.client.enabled=false",
        "management.tracing.enabled=false"})
@DisplayName("预留超时释放 - 走真实 RabbitMQ 延迟队列")
class StockReleaseTimeoutIT {

    static final MySQLContainer<?> MYSQL = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
            .withDatabaseName("ticket_db")
            .withCopyFileToContainer(
                    MountableFile.forHostPath(repoFile("deploy/mysql/init/03-ticket-db.sql")),
                    "/docker-entrypoint-initdb.d/01-ticket-db.sql");

    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    // Built from the same Dockerfile docker-compose uses: the stock plugin is not in the
    // official image, and that Dockerfile also carries the chmod that stops the broker
    // silently failing to load the .ez.
    static final GenericContainer<?> RABBIT = new GenericContainer<>(
            new ImageFromDockerfile().withDockerfile(repoFile("deploy/rabbitmq/Dockerfile")))
            .withExposedPorts(5672)
            .withEnv("RABBITMQ_DEFAULT_USER", "guest")
            .withEnv("RABBITMQ_DEFAULT_PASS", "guest")
            .waitingFor(Wait.forLogMessage(".*Server startup complete.*\\n", 1)
                    .withStartupTimeout(Duration.ofMinutes(3)));

    static {
        MYSQL.start();
        REDIS.start();
        RABBIT.start();
    }

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("spring.rabbitmq.host", RABBIT::getHost);
        registry.add("spring.rabbitmq.port", () -> RABBIT.getMappedPort(5672));
    }

    /** Kafka is not under test here; the release path's event publication is mocked out. */
    @MockBean
    private TicketEventProducer eventProducer;

    @Autowired private TicketStockService ticketStockService;
    @Autowired private TicketStockMapper stockMapper;
    // Spy, not mock: unstubbed calls hit the real mapper, so only the one failure
    // the DLQ test needs is simulated.
    @SpyBean private ReservationMapper reservationMapper;
    @Autowired private StringRedisTemplate redis;
    @Autowired private RabbitTemplate rabbitTemplate;

    /** The DLQ is durable and shared by every test in this class; drain it so each one
     *  asserts on its own poison message rather than a leftover. */
    @BeforeEach
    void drainDeadLetterQueue() {
        while (rabbitTemplate.receive(RabbitMQConfig.STOCK_RELEASE_DLQ, 100) != null) {
            // discard
        }
    }

    @Test
    @DisplayName("预留超时 - 释放库存，Redis 与 MySQL 同时归还")
    void reservationTimeout_expiresAndRestoresStockInBothStores() {
        long eventId = 930001L;
        Long reservationId = reserveOne(eventId, 5);

        assertThat(redisStock(eventId)).isEqualTo(4);
        assertThat(reservedQuantity(eventId)).isEqualTo(1);

        publishRelease(reservationId);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(reservationMapper.findById(reservationId).getStatus())
                    .isEqualTo(ReservationStatus.EXPIRED);
            assertThat(reservedQuantity(eventId)).isZero();
            assertThat(redisStock(eventId)).as("Redis stock handed back").isEqualTo(5);
        });
    }

    @Test
    @DisplayName("同一超时消息重投 - 库存只归还一次，不能凭空多出票")
    void redeliveredTimeout_releasesStockOnlyOnce() {
        long eventId = 930002L;
        Long reservationId = reserveOne(eventId, 5);

        publishRelease(reservationId);
        await().atMost(Duration.ofSeconds(30))
                .until(() -> reservationMapper.findById(reservationId).getStatus()
                        == ReservationStatus.EXPIRED);

        // RabbitMQ delivers at least once; a redelivery must not credit the stock again.
        publishRelease(reservationId);

        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(redisStock(eventId)).as("stock credited exactly once").isEqualTo(5);
            assertThat(reservedQuantity(eventId)).isZero();
        });
    }

    @Test
    @DisplayName("已确认的预留 - 超时消息必须是空操作，不能把已卖出的票放回去")
    void confirmedReservation_timeoutIsNoOp() {
        long eventId = 930003L;
        Long reservationId = reserveOne(eventId, 5);
        ticketStockService.confirm(42L, reservationId);
        int redisAfterConfirm = redisStock(eventId);

        publishRelease(reservationId);

        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(reservationMapper.findById(reservationId).getStatus())
                    .isEqualTo(ReservationStatus.CONFIRMED);
            assertThat(redisStock(eventId)).as("a sold ticket is not handed back")
                    .isEqualTo(redisAfterConfirm);
        });
    }

    @Test
    @DisplayName("毒消息 - 必须进死信队列，不能无限重投卡死队列")
    void poisonMessage_isDeadLettered_notRequeuedForever() {
        // A payload the listener cannot turn into a Long. Spring AMQP's default
        // requeue-rejected=true would put it straight back on the queue, which
        // redelivers it immediately, forever: the queue stalls and the real timeout
        // messages behind it never run, so held stock is never released.
        rabbitTemplate.convertAndSend(
                RabbitMQConfig.DELAY_EXCHANGE,
                RabbitMQConfig.STOCK_RELEASE_ROUTING_KEY,
                "not-a-reservation-id",
                msg -> {
                    msg.getMessageProperties().setDelay(100);
                    return msg;
                });

        Object dead = rabbitTemplate.receiveAndConvert(RabbitMQConfig.STOCK_RELEASE_DLQ, 20_000);
        assertThat(dead).as("poison message parked in the DLQ").isEqualTo("not-a-reservation-id");
    }

    @Test
    @DisplayName("业务异常(如MySQL抖动) - 重试后进死信，不能永久重投")
    void businessFailure_isRetriedThenDeadLettered() {
        long eventId = 930005L;
        Long reservationId = reserveOne(eventId, 5);

        // A conversion error is not enough to prove this: Spring AMQP treats those as
        // fatal and never requeues them whatever default-requeue-rejected says. The
        // dangerous case is an ordinary runtime failure -- with the default requeue=true
        // it goes back on the queue and is redelivered immediately, forever.
        doThrow(new DataAccessResourceFailureException("mysql down"))
                .when(reservationMapper).findById(reservationId);

        publishRelease(reservationId);

        Object dead = rabbitTemplate.receiveAndConvert(RabbitMQConfig.STOCK_RELEASE_DLQ, 30_000);
        assertThat(dead).as("failing release parked in the DLQ instead of spinning")
                .isEqualTo(reservationId);
    }

    @Test
    @DisplayName("毒消息之后 - 队列仍然可用，后续超时消息正常处理")
    void queueStillWorks_afterAPoisonMessage() {
        long eventId = 930004L;
        Long reservationId = reserveOne(eventId, 5);

        rabbitTemplate.convertAndSend(
                RabbitMQConfig.DELAY_EXCHANGE, RabbitMQConfig.STOCK_RELEASE_ROUTING_KEY,
                "poison", msg -> {
                    msg.getMessageProperties().setDelay(100);
                    return msg;
                });
        publishRelease(reservationId);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(reservationMapper.findById(reservationId).getStatus())
                        .as("a poison message must not block the ones behind it")
                        .isEqualTo(ReservationStatus.EXPIRED));
    }

    private Long reserveOne(long eventId, int total) {
        CreateTicketRequest create = new CreateTicketRequest();
        create.setEventId(eventId);
        create.setTicketType("VIP");
        create.setTotalQuantity(total);
        ticketStockService.createTicketStock(create);

        ReserveRequest request = new ReserveRequest();
        request.setEventId(eventId);
        request.setTicketType("VIP");
        request.setQuantity(1);
        ReserveResponse response = ticketStockService.reserve(42L, request);
        return response.getReservationId();
    }

    /** Same exchange and routing key production uses, just without the 30-minute wait. */
    private void publishRelease(Long reservationId) {
        rabbitTemplate.convertAndSend(
                RabbitMQConfig.DELAY_EXCHANGE,
                RabbitMQConfig.STOCK_RELEASE_ROUTING_KEY,
                reservationId,
                msg -> {
                    msg.getMessageProperties().setDelay(100);
                    return msg;
                });
    }

    private int redisStock(long eventId) {
        String value = redis.opsForValue().get(RedisKeys.stock(eventId, "VIP"));
        return value == null ? -1 : Integer.parseInt(value);
    }

    private int reservedQuantity(long eventId) {
        TicketStock stock = stockMapper.findByEventIdAndTicketType(eventId, "VIP");
        return stock.getReservedQuantity();
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
        throw new IllegalStateException("Cannot locate " + relative + " above " + Paths.get("").toAbsolutePath());
    }
}

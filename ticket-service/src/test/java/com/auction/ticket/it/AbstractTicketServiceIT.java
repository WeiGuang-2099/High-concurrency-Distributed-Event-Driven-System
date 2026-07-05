package com.auction.ticket.it;

import com.auction.ticket.event.TicketEventProducer;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Base class for ticket-service integration tests: boots the full application
 * context against real MySQL and Redis containers (the same images and the same
 * init script as docker-compose), with the message brokers stubbed out.
 *
 * <p>Containers are started once per JVM (singleton pattern, not @Container) and
 * shared by all IT classes in this module; Ryuk reaps them when the JVM exits.
 */
@SpringBootTest(properties = {
        // NewTopic beans exist in KafkaConfig; without a broker the auto-configured
        // KafkaAdmin would stall the context trying to create them.
        "spring.kafka.admin.auto-create=false",
        // StockReleaseConsumer (@RabbitListener) must not try to connect to RabbitMQ.
        "spring.rabbitmq.listener.simple.auto-startup=false",
        "management.tracing.enabled=false"
})
public abstract class AbstractTicketServiceIT {

    static final MySQLContainer<?> MYSQL = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
            .withDatabaseName("ticket_db")
            // Same schema the real stack gets from docker-compose; the entrypoint
            // runs it as root, so its CREATE DATABASE/USE statements are fine.
            .withCopyFileToContainer(
                    MountableFile.forHostPath(repoFile("deploy/mysql/init/03-ticket-db.sql")),
                    "/docker-entrypoint-initdb.d/01-ticket-db.sql");

    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    static {
        MYSQL.start();
        REDIS.start();
    }

    /** Kafka producer is mocked: event publication is not under test here. */
    @MockBean
    protected TicketEventProducer eventProducer;

    /** RabbitMQ delayed-release messages are not under test here. */
    @MockBean
    protected RabbitTemplate rabbitTemplate;

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    /**
     * Failsafe runs with the module directory as working directory; the init script
     * lives at the repo root, so walk upwards until it is found.
     */
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

package com.auction.auction.messaging;

import com.auction.auction.service.RedisKeys;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;

/**
 * Wires the bid outbox relay: creates the consumer group, then runs a listener that
 * hands each entry to {@link BidOutboxRelay} and acknowledges only on success.
 */
@Configuration
public class BidOutboxRelayConfig {

    private static final Logger log = LoggerFactory.getLogger(BidOutboxRelayConfig.class);

    static final String GROUP = "bid-outbox-relay";

    private LettuceConnectionFactory relayConnectionFactory;

    @Bean
    public String bidOutboxConsumerName() {
        // Stable per host so a restarted instance can reclaim the entries it left
        // pending; BidOutboxReclaimer also rescues entries from an instance that
        // never comes back.
        try {
            return "relay-" + InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "relay-unknown";
        }
    }

    // Disableable so tests can drive the relay deterministically: a live listener races
    // the test for every entry it appends.
    @Bean(destroyMethod = "stop")
    @ConditionalOnProperty(name = "auction.bid.outbox.relay.enabled",
            havingValue = "true", matchIfMissing = true)
    public StreamMessageListenerContainer<String, MapRecord<String, String, String>> bidOutboxContainer(
            RedisProperties redisProperties,
            StringRedisTemplate redis,
            BidOutboxRelay relay,
            String bidOutboxConsumerName,
            @Value("${auction.bid.outbox.batch-size:50}") int batchSize,
            @Value("${auction.bid.outbox.poll-timeout-ms:1000}") long pollTimeoutMs) {

        createGroupIfAbsent(redis);

        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>>
                options = StreamMessageListenerContainer.StreamMessageListenerContainerOptions.builder()
                .pollTimeout(Duration.ofMillis(pollTimeoutMs))
                .batchSize(batchSize)
                .build();

        StreamMessageListenerContainer<String, MapRecord<String, String, String>> container =
                StreamMessageListenerContainer.create(relayConnectionFactory(redisProperties, pollTimeoutMs), options);

        StreamMessageListenerContainer.ConsumerStreamReadRequest<String> request =
                StreamMessageListenerContainer.StreamReadRequest
                        .builder(StreamOffset.create(RedisKeys.bidOutboxStream(), ReadOffset.lastConsumed()))
                        .consumer(Consumer.from(GROUP, bidOutboxConsumerName))
                        // false: the entry must stay pending until Kafka has it.
                        .autoAcknowledge(false)
                        .errorHandler(t -> log.error("Bid outbox poll failed, will keep polling", t))
                        // Default cancels the subscription on ANY error, which silently
                        // stops the relay forever after one blip and strands every later
                        // bid in the stream. Keep polling; the reclaimer covers the gap.
                        .cancelOnError(t -> false)
                        .build();

        container.register(request, message -> {
            if (relay.relay(message)) {
                relay.acknowledge(GROUP, message.getId());
            }
        });

        container.start();
        log.info("Bid outbox relay started: stream={} group={} consumer={}",
                RedisKeys.bidOutboxStream(), GROUP, bidOutboxConsumerName);
        return container;
    }

    /**
     * The relay needs its own connection: XREADGROUP blocks for pollTimeout, but the
     * shared factory runs a deliberately tight command timeout (200ms) so a slow Redis
     * fails the bid hot path fast. Sharing it makes every poll time out, and the
     * container treats that as a fatal error. Not exposed as a bean -- a second
     * RedisConnectionFactory would switch off Boot's auto-configured one.
     */
    private LettuceConnectionFactory relayConnectionFactory(RedisProperties props, long pollTimeoutMs) {
        RedisStandaloneConfiguration standalone =
                new RedisStandaloneConfiguration(props.getHost(), props.getPort());
        standalone.setDatabase(props.getDatabase());
        if (props.getUsername() != null) {
            standalone.setUsername(props.getUsername());
        }
        if (props.getPassword() != null) {
            standalone.setPassword(props.getPassword());
        }
        LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
                .commandTimeout(Duration.ofMillis(pollTimeoutMs * 2 + 1000))
                .build();
        relayConnectionFactory = new LettuceConnectionFactory(standalone, clientConfig);
        relayConnectionFactory.afterPropertiesSet();
        return relayConnectionFactory;
    }

    @PreDestroy
    public void closeRelayConnectionFactory() {
        if (relayConnectionFactory != null) {
            relayConnectionFactory.destroy();
        }
    }

    private void createGroupIfAbsent(StringRedisTemplate redis) {
        try {
            // ReadOffset.from("0") so a group created after bids already exist still
            // sees them; MKSTREAM is implied when the stream is absent.
            redis.opsForStream().createGroup(RedisKeys.bidOutboxStream(), ReadOffset.from("0"), GROUP);
        } catch (RedisSystemException | InvalidDataAccessApiUsageException e) {
            // BUSYGROUP: another instance created it first.
            log.debug("Consumer group {} already exists", GROUP);
        }
    }
}

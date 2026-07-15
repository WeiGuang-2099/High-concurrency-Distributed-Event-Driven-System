package com.auction.ticket.event;

import com.auction.common.event.BaseEvent;
import com.auction.ticket.domain.entity.OutboxEvent;
import com.auction.ticket.repository.OutboxMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Publishes outbox rows to Kafka and stamps sent_at only once the broker has them.
 *
 * <p>Claim, publish, mark -- the same shape as the bid outbox relay on Redis Streams
 * (docs/adr/0005), because the guarantee is the same: at-least-once delivery, with
 * consumers deduplicating on the event id. A row published twice after a crash between
 * the send and the stamp is harmless; a row never published is not.</p>
 *
 * <p>Claiming and publishing are separate steps on purpose. Holding the row lock across
 * the Kafka round trip would put a network call inside a transaction -- the mistake
 * ebf22d1 already fixed once on the payment path.</p>
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxMapper outboxMapper;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String instanceId;
    private final boolean scheduleEnabled;
    private final int batchSize;
    private final Duration staleClaim;
    private final long publishTimeoutMs;

    public OutboxRelay(OutboxMapper outboxMapper,
                       KafkaTemplate<String, Object> kafkaTemplate,
                       ObjectMapper objectMapper,
                       @Value("${ticket.outbox.relay.enabled:true}") boolean scheduleEnabled,
                       @Value("${ticket.outbox.batch-size:100}") int batchSize,
                       @Value("${ticket.outbox.stale-claim-ms:60000}") long staleClaimMs,
                       @Value("${ticket.outbox.publish-timeout-ms:10000}") long publishTimeoutMs) {
        this.outboxMapper = outboxMapper;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.scheduleEnabled = scheduleEnabled;
        this.batchSize = batchSize;
        this.staleClaim = Duration.ofMillis(staleClaimMs);
        this.publishTimeoutMs = publishTimeoutMs;
        this.instanceId = resolveInstanceId();
    }

    @Scheduled(fixedDelayString = "${ticket.outbox.poll-interval-ms:1000}")
    public void publishPending() {
        // The flag gates the schedule, not the bean: tests drive drainOnce() directly and
        // a background poll would race their assertions about what is still pending.
        if (!scheduleEnabled) {
            return;
        }
        try {
            drainOnce();
        } catch (Exception e) {
            // Never let one bad pass kill the schedule; the next tick retries, and rows
            // stay unsent until they are genuinely published.
            log.error("Outbox relay pass failed", e);
        }
    }

    /** Visible for tests: one claim-publish-mark cycle. Returns how many were published. */
    public int drainOnce() {
        LocalDateTime now = LocalDateTime.now();
        outboxMapper.claimUnsent(instanceId, now, now.minus(staleClaim), batchSize);
        // Deliberately not "return early if nothing new was claimed": rows this instance
        // claimed on an earlier pass and failed to publish are still ours and still unsent,
        // and they should be retried now rather than waiting out the stale-claim window.
        List<OutboxEvent> batch = outboxMapper.findClaimed(instanceId);
        int published = 0;
        for (OutboxEvent row : batch) {
            if (publish(row)) {
                outboxMapper.markSent(row.getId(), LocalDateTime.now());
                published++;
            }
        }
        return published;
    }

    private boolean publish(OutboxEvent row) {
        try {
            // Rebuild the concrete event rather than shipping the raw JSON string: the
            // consumers resolve the subtype from the __TypeId__ header, which JsonSerializer
            // derives from the object's class. Sending the string would set that header to
            // java.lang.String and every listener would skip the event -- the exact failure
            // mode of 5faea68. Deserializing to BaseEvent would be just as bad: it compiles,
            // publishes, and silently drops every subtype field.
            BaseEvent event = (BaseEvent) objectMapper.readValue(row.getPayload(), eventClass(row));
            kafkaTemplate.send(row.getTopic(), row.getMessageKey(), event)
                    .get(publishTimeoutMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception e) {
            // Leave sent_at null: the claim goes stale and the row is retried.
            log.error("Failed to publish outbox event id={} eventId={} type={}, will retry",
                    row.getId(), row.getEventId(), row.getEventType(), e);
            return false;
        }
    }

    /**
     * Resolves the row's event class, refusing anything outside the event package. The
     * value comes from our own table, but Class.forName on stored data is a deserialization
     * gadget waiting to happen if that ever stops being true.
     */
    private static Class<?> eventClass(OutboxEvent row) throws ClassNotFoundException {
        String name = row.getPayloadClass();
        if (name == null || !name.startsWith("com.auction.common.event.")) {
            throw new IllegalStateException("Refusing to load non-event class from outbox: " + name);
        }
        return Class.forName(name);
    }

    private static String resolveInstanceId() {
        try {
            return "ticket-" + InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "ticket-unknown";
        }
    }
}

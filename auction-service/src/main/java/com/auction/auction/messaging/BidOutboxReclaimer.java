package com.auction.auction.messaging;

import com.auction.auction.service.RedisKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisStreamCommands.XClaimOptions;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

/**
 * Rescues outbox entries stuck in the pending list. XREADGROUP hands an entry to one
 * consumer and keeps it pending until XACK, so an instance that dies between the Kafka
 * publish and the ack leaves entries owned by a consumer that never returns. Without
 * this the outbox would still lose those bids -- quietly, which is the failure mode the
 * whole pattern exists to prevent.
 *
 * <p>Reclaiming is safe because relaying is idempotent: event ids are derived from the
 * stream entry id, so a re-published bid carries the id consumers already dedupe on.</p>
 */
@Component
public class BidOutboxReclaimer {

    private static final Logger log = LoggerFactory.getLogger(BidOutboxReclaimer.class);

    private final StringRedisTemplate redis;
    private final BidOutboxRelay relay;
    private final String consumerName;
    private final Duration minIdle;
    private final int batchSize;

    public BidOutboxReclaimer(StringRedisTemplate redis,
                              BidOutboxRelay relay,
                              String bidOutboxConsumerName,
                              @Value("${auction.bid.outbox.reclaim-idle-ms:60000}") long minIdleMs,
                              @Value("${auction.bid.outbox.batch-size:50}") int batchSize) {
        this.redis = redis;
        this.relay = relay;
        this.consumerName = bidOutboxConsumerName;
        this.minIdle = Duration.ofMillis(minIdleMs);
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${auction.bid.outbox.reclaim-interval-ms:30000}")
    public void reclaimStalePending() {
        try {
            List<RecordId> stale = findStale();
            if (stale.isEmpty()) {
                return;
            }
            log.warn("Reclaiming {} stale bid outbox entries idle >= {}", stale.size(), minIdle);
            for (RecordId id : stale) {
                relayClaimed(id);
            }
        } catch (Exception e) {
            // Never let a reclaim failure kill the schedule; the next tick retries.
            log.error("Bid outbox reclaim pass failed", e);
        }
    }

    private List<RecordId> findStale() {
        StreamOperations<String, String, String> streams = redis.opsForStream();
        PendingMessages pending = streams.pending(RedisKeys.bidOutboxStream(),
                BidOutboxRelayConfig.GROUP, Range.unbounded(), batchSize);
        if (pending == null || pending.isEmpty()) {
            return List.of();
        }
        return pending.stream()
                .filter(p -> p.getElapsedTimeSinceLastDelivery().compareTo(minIdle) >= 0)
                .map(PendingMessage::getId)
                .toList();
    }

    /**
     * XCLAIM re-checks the idle time server-side, so if a live consumer touched the entry
     * between findStale and here, the claim returns empty and we leave it alone.
     */
    private void relayClaimed(RecordId id) {
        StreamOperations<String, String, String> streams = redis.opsForStream();
        List<MapRecord<String, String, String>> claimed = streams.claim(
                RedisKeys.bidOutboxStream(), BidOutboxRelayConfig.GROUP, consumerName,
                XClaimOptions.minIdle(minIdle).ids(id));
        if (claimed == null || claimed.isEmpty()) {
            return;
        }
        for (MapRecord<String, String, String> record : claimed) {
            if (relay.relay(record)) {
                relay.acknowledge(BidOutboxRelayConfig.GROUP, record.getId());
            }
        }
    }
}

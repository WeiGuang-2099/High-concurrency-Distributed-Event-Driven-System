package com.auction.auction.messaging;

import com.auction.auction.service.RedisKeys;
import com.auction.common.event.BaseEvent;
import com.auction.common.event.KafkaTopics;
import com.auction.common.event.auction.BidOutbidEvent;
import com.auction.common.event.auction.BidPlacedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Drains the bid outbox stream (written atomically with the accept by place_bid.lua)
 * to Kafka, and only then acknowledges the entry. Crashing between publish and XACK
 * replays the entry, so delivery is at-least-once -- which is why every event id here
 * is derived from the stream entry id rather than freshly generated: a replay must
 * produce the same id, or the consumers' eventId-based dedupe cannot recognise it.
 *
 * <p>Durability is Redis's now, so it is only as good as Redis persistence: with the
 * default appendfsync everysec, a hard crash can lose up to a second of accepted bids.
 * That is the trade this design makes for keeping bid accept off the MySQL path.</p>
 */
@Component
public class BidOutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(BidOutboxRelay.class);

    private final StringRedisTemplate redis;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final long publishTimeoutMs;

    public BidOutboxRelay(StringRedisTemplate redis,
                          KafkaTemplate<String, Object> kafkaTemplate,
                          @Value("${auction.bid.outbox.publish-timeout-ms:10000}") long publishTimeoutMs) {
        this.redis = redis;
        this.kafkaTemplate = kafkaTemplate;
        this.publishTimeoutMs = publishTimeoutMs;
    }

    /**
     * Publishes one outbox entry and reports whether it may be acknowledged. Sends
     * synchronously: the whole point of the outbox is that the entry stays pending until
     * the broker has the event, so a fire-and-forget send here would defeat it.
     */
    public boolean relay(MapRecord<String, String, String> record) {
        Map<String, String> body = record.getValue();
        String streamId = record.getId().getValue();
        Long auctionId = Long.valueOf(body.get("auctionId"));
        Long bidderId = Long.valueOf(body.get("bidderId"));
        BigDecimal amount = new BigDecimal(body.get("amount"));
        Instant bidTime = Instant.ofEpochMilli(Long.parseLong(body.get("bidTime")));
        Long prevBidderId = emptyToNull(body.get("prevBidder")) == null
                ? null : Long.valueOf(body.get("prevBidder"));
        BigDecimal prevAmount = emptyToNull(body.get("prevAmount")) == null
                ? null : new BigDecimal(body.get("prevAmount"));

        BidPlacedEvent placed = new BidPlacedEvent(auctionId, bidderId,
                body.get("bidderUsername"), amount, bidTime, prevBidderId, prevAmount);
        placed.setEventId(streamId + ":placed");

        try {
            publish(auctionId, placed);
            if (prevBidderId != null && !prevBidderId.equals(bidderId)) {
                BidOutbidEvent outbid = new BidOutbidEvent(
                        auctionId, prevBidderId, prevAmount, bidderId, amount);
                outbid.setEventId(streamId + ":outbid");
                publish(auctionId, outbid);
            }
            return true;
        } catch (Exception ex) {
            // Leave the entry pending: it stays in this consumer's PEL and is retried on
            // the next reclaim pass rather than being lost.
            log.error("Failed to relay outbox entry {} for auction {}, will retry",
                    streamId, auctionId, ex);
            return false;
        }
    }

    private void publish(Long auctionId, BaseEvent event) throws Exception {
        kafkaTemplate.send(KafkaTopics.AUCTION_EVENTS, String.valueOf(auctionId), event)
                .get(publishTimeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Acknowledges the entry. Called only after {@link #relay} reports success, so the
     * event is on the broker before the outbox forgets about it.
     */
    public void acknowledge(String group, RecordId id) {
        redis.opsForStream().acknowledge(RedisKeys.bidOutboxStream(), group, id);
    }

    private static String emptyToNull(String s) {
        return s == null || s.isEmpty() ? null : s;
    }
}

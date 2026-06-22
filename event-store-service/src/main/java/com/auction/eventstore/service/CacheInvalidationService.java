package com.auction.eventstore.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

/**
 * Invalidates Redis cache keys after an event is processed by the write-side consumer.
 * Subsequent reads will fall back to the Mongo projection and repopulate the cache.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CacheInvalidationService {

    private final RedisTemplate<String, Object> redisTemplate;

    public void invalidateAuctionSummary(Long auctionId) {
        String key = "auction_summary:" + auctionId;
        evict(key);
    }

    public void invalidateStock(Long eventId, String ticketType) {
        // ticket-service uses key pattern stock:{eventId}:{ticketType}
        String key = "stock:" + eventId + ":" + ticketType;
        evict(key);
    }

    public void invalidateOrder(Long orderId) {
        String key = "order:" + orderId;
        evict(key);
    }

    private void evict(String key) {
        try {
            Boolean deleted = redisTemplate.delete(key);
            log.debug("Cache invalidation key={} deleted={}", key, deleted);
        } catch (Exception e) {
            log.warn("Cache invalidation failed for key={} (Redis unavailable?): {}", key, e.getMessage());
            // Don't throw: cache invalidation is best-effort.
        }
    }
}

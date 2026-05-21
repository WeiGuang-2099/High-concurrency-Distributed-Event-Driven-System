package com.auction.auction.messaging;

import com.auction.auction.domain.entity.Bid;
import com.auction.auction.repository.AuctionMapper;
import com.auction.auction.repository.BidMapper;
import com.auction.common.event.KafkaTopics;
import com.auction.common.event.auction.BidPlacedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Component
public class AuctionEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(AuctionEventConsumer.class);
    private static final String HOT_BIDCOUNT_ZSET = "auction:hot:bidcount";

    private final BidMapper bidMapper;
    private final AuctionMapper auctionMapper;
    private final StringRedisTemplate redis;

    public AuctionEventConsumer(BidMapper bidMapper,
                                AuctionMapper auctionMapper,
                                StringRedisTemplate redis) {
        this.bidMapper = bidMapper;
        this.auctionMapper = auctionMapper;
        this.redis = redis;
    }

    @KafkaListener(
            topics = KafkaTopics.AUCTION_EVENTS,
            groupId = "auction-service-bid-persistor",
            containerFactory = "kafkaListenerContainerFactory")
    @Transactional
    public void onEvent(Object payload, Acknowledgment ack) {
        try {
            if (payload instanceof BidPlacedEvent event) {
                persistBid(event);
            }
            // Other event types ignored here; bid persistence is the only DB side-effect this service handles
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("Failed to handle auction event", ex);
            throw ex;
        }
    }

    private void persistBid(BidPlacedEvent event) {
        Bid bid = new Bid();
        bid.setAuctionId(event.getAuctionId());
        bid.setUserId(event.getBidderId());
        bid.setUsername(event.getBidderUsername() == null ? "" : event.getBidderUsername());
        bid.setAmount(event.getAmount());
        bid.setBidTime(LocalDateTime.ofInstant(event.getBidTime(), ZoneOffset.UTC));
        bid.setEventId(event.getEventId());

        try {
            bidMapper.insert(bid);
        } catch (DuplicateKeyException duplicate) {
            log.info("Duplicate bid event {} skipped", event.getEventId());
            return;
        }

        // Update auction snapshot in MySQL. Use compare-and-update so out-of-order
        // events from different partitions cannot regress the highest bid.
        int rows = auctionMapper.update(null,
                new com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper<com.auction.auction.domain.entity.Auction>()
                        .eq(com.auction.auction.domain.entity.Auction::getId, event.getAuctionId())
                        .and(w -> w
                                .isNull(com.auction.auction.domain.entity.Auction::getCurrentHighestBid)
                                .or()
                                .lt(com.auction.auction.domain.entity.Auction::getCurrentHighestBid, event.getAmount()))
                        .set(com.auction.auction.domain.entity.Auction::getCurrentHighestBid, event.getAmount())
                        .set(com.auction.auction.domain.entity.Auction::getCurrentHighestBidderId, event.getBidderId()));
        if (rows == 0) {
            log.debug("Auction {} snapshot not updated (older bid amount)", event.getAuctionId());
        }

        // Increment the hot list counter (used by /api/auctions/hot)
        ZSetOperations<String, String> zset = redis.opsForZSet();
        zset.incrementScore(HOT_BIDCOUNT_ZSET, String.valueOf(event.getAuctionId()), 1.0);
    }
}

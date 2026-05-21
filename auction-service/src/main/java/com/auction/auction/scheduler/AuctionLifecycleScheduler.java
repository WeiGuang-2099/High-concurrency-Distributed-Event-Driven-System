package com.auction.auction.scheduler;

import com.auction.auction.domain.entity.Auction;
import com.auction.auction.domain.entity.Bid;
import com.auction.auction.domain.enums.AuctionStatus;
import com.auction.auction.repository.AuctionMapper;
import com.auction.auction.repository.BidMapper;
import com.auction.auction.service.RedisKeys;
import com.auction.common.event.KafkaTopics;
import com.auction.common.event.auction.AuctionActivatedEvent;
import com.auction.common.event.auction.AuctionExpiredEvent;
import com.auction.common.event.auction.AuctionSettledEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class AuctionLifecycleScheduler {

    private static final Logger log = LoggerFactory.getLogger(AuctionLifecycleScheduler.class);

    private final AuctionMapper auctionMapper;
    private final BidMapper bidMapper;
    private final StringRedisTemplate redis;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public AuctionLifecycleScheduler(AuctionMapper auctionMapper,
                                     BidMapper bidMapper,
                                     StringRedisTemplate redis,
                                     KafkaTemplate<String, Object> kafkaTemplate) {
        this.auctionMapper = auctionMapper;
        this.bidMapper = bidMapper;
        this.redis = redis;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedRateString = "${auction.scheduler.poll-interval-ms:1000}")
    public void activatePending() {
        List<Auction> readyToActivate = auctionMapper.findReadyToActivate(
                AuctionStatus.PENDING.getValue(), LocalDateTime.now());

        for (Auction auction : readyToActivate) {
            int rows = auctionMapper.markActive(auction.getId());
            if (rows == 0) {
                // Another scheduler instance won the race; skip.
                continue;
            }

            redis.opsForValue().set(RedisKeys.auctionStatus(auction.getId()), "ACTIVE");
            Map<String, String> highest = new HashMap<>();
            highest.put("bidder_id", "");
            highest.put("amount", auction.getStartingPrice().toPlainString());
            highest.put("bid_time", "0");
            redis.opsForHash().putAll(RedisKeys.auctionHighest(auction.getId()), highest);

            AuctionActivatedEvent evt = new AuctionActivatedEvent(
                    auction.getId(), auction.getStartingPrice());
            publish(auction.getId(), evt);

            log.info("Auction {} activated", auction.getId());
        }
    }

    @Scheduled(fixedRateString = "${auction.scheduler.poll-interval-ms:1000}")
    public void expireActive() {
        List<Auction> readyToExpire = auctionMapper.findReadyToExpire(
                AuctionStatus.ACTIVE.getValue(), LocalDateTime.now());

        for (Auction auction : readyToExpire) {
            // Close Redis fast-path so new bids are rejected immediately
            redis.opsForValue().set(RedisKeys.auctionStatus(auction.getId()), "CLOSED");

            Bid highest = bidMapper.findHighestBid(auction.getId());
            if (highest != null) {
                int rows = auctionMapper.markSettled(
                        auction.getId(), highest.getUserId(), highest.getAmount());
                if (rows == 0) {
                    continue;
                }
                AuctionSettledEvent evt = new AuctionSettledEvent(
                        auction.getId(), highest.getUserId(),
                        highest.getAmount(), auction.getTicketTypeId());
                publish(auction.getId(), evt);
                log.info("Auction {} settled: winner={} amount={}",
                        auction.getId(), highest.getUserId(), highest.getAmount());
            } else {
                int rows = auctionMapper.markExpired(auction.getId());
                if (rows == 0) {
                    continue;
                }
                AuctionExpiredEvent evt = new AuctionExpiredEvent(
                        auction.getId(), "no_bids");
                publish(auction.getId(), evt);
                log.info("Auction {} expired with no bids", auction.getId());
            }
        }
    }

    private void publish(Long auctionId, Object event) {
        kafkaTemplate.send(KafkaTopics.AUCTION_EVENTS, String.valueOf(auctionId), event);
    }
}

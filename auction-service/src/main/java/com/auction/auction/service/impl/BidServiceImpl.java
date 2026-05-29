package com.auction.auction.service.impl;

import com.auction.auction.controller.dto.PlaceBidResponse;
import com.auction.common.exception.BusinessException;
import com.auction.auction.service.BidService;
import com.auction.auction.service.RedisKeys;
import com.auction.common.event.KafkaTopics;
import com.auction.common.event.auction.BidOutbidEvent;
import com.auction.common.event.auction.BidPlacedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

@Service
public class BidServiceImpl implements BidService {

    private static final Logger log = LoggerFactory.getLogger(BidServiceImpl.class);

    private final StringRedisTemplate redis;
    @SuppressWarnings("rawtypes")
    private final DefaultRedisScript<List> placeBidScript;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public BidServiceImpl(StringRedisTemplate redis,
                          @SuppressWarnings("rawtypes") DefaultRedisScript<List> placeBidScript,
                          KafkaTemplate<String, Object> kafkaTemplate) {
        this.redis = redis;
        this.placeBidScript = placeBidScript;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    @SuppressWarnings("unchecked")
    public PlaceBidResponse placeBid(Long auctionId, Long bidderId, String bidderUsername, BigDecimal amount) {
        Instant bidTime = Instant.now();
        List<String> keys = Arrays.asList(
                RedisKeys.auctionStatus(auctionId),
                RedisKeys.auctionHighest(auctionId));

        List<Object> result = (List<Object>) redis.execute(
                placeBidScript,
                keys,
                String.valueOf(bidderId),
                amount.toPlainString(),
                String.valueOf(bidTime.toEpochMilli()));

        if (result == null || result.size() < 4) {
            throw new BusinessException(500, "Bid evaluation failed");
        }

        long okFlag = ((Number) result.get(0)).longValue();
        String code = (String) result.get(1);
        String prevBidderStr = (String) result.get(2);
        String prevAmountStr = (String) result.get(3);

        if (okFlag != 1L) {
            switch (code) {
                case "AUCTION_NOT_ACTIVE":
                    throw new BusinessException(400, "Auction not active");
                case "BID_TOO_LOW":
                    throw new BusinessException(400, "Bid too low; current highest is " + prevAmountStr);
                default:
                    throw new BusinessException(400, "Bid rejected: " + code);
            }
        }

        Long previousBidderId = prevBidderStr == null || prevBidderStr.isEmpty()
                ? null : Long.parseLong(prevBidderStr);
        BigDecimal previousAmount = prevAmountStr == null || prevAmountStr.isEmpty()
                ? null : new BigDecimal(prevAmountStr);

        BidPlacedEvent placedEvent = new BidPlacedEvent(
                auctionId, bidderId, bidderUsername, amount, bidTime,
                previousBidderId, previousAmount);
        publish(auctionId, placedEvent);

        if (previousBidderId != null && !previousBidderId.equals(bidderId)) {
            BidOutbidEvent outbidEvent = new BidOutbidEvent(
                    auctionId, previousBidderId, previousAmount, bidderId, amount);
            publish(auctionId, outbidEvent);
        }

        log.debug("Bid placed: auction={} bidder={} amount={}", auctionId, bidderId, amount);
        return PlaceBidResponse.builder()
                .auctionId(auctionId)
                .bidderId(bidderId)
                .amount(amount)
                .bidTime(bidTime)
                .build();
    }

    private void publish(Long auctionId, Object event) {
        kafkaTemplate.send(KafkaTopics.AUCTION_EVENTS, String.valueOf(auctionId), event);
    }
}

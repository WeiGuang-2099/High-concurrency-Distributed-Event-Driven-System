package com.auction.auction.service.impl;

import com.auction.auction.controller.dto.PlaceBidResponse;
import com.auction.common.exception.BusinessException;
import com.auction.auction.service.BidService;
import com.auction.auction.service.RedisKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
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
    private final long outboxMaxLen;

    public BidServiceImpl(StringRedisTemplate redis,
                          @SuppressWarnings("rawtypes") DefaultRedisScript<List> placeBidScript,
                          @Value("${auction.bid.outbox.max-len:1000000}") long outboxMaxLen) {
        this.redis = redis;
        this.placeBidScript = placeBidScript;
        this.outboxMaxLen = outboxMaxLen;
    }

    /**
     * Evaluates and records the bid in a single Lua script: the accept decision and the
     * outbox append are one atomic step, so a bid this method reports as placed is always
     * recoverable. Publishing to Kafka is BidOutboxRelay's job -- doing it here would
     * reintroduce the window where the caller has a 201 but the event was never sent.
     */
    @Override
    @SuppressWarnings("unchecked")
    public PlaceBidResponse placeBid(Long auctionId, Long bidderId, String bidderUsername, BigDecimal amount) {
        Instant bidTime = Instant.now();
        List<String> keys = Arrays.asList(
                RedisKeys.auctionStatus(auctionId),
                RedisKeys.auctionHighest(auctionId),
                RedisKeys.bidOutboxStream());

        List<Object> result = (List<Object>) redis.execute(
                placeBidScript,
                keys,
                String.valueOf(bidderId),
                amount.toPlainString(),
                String.valueOf(bidTime.toEpochMilli()),
                String.valueOf(auctionId),
                bidderUsername == null ? "" : bidderUsername,
                String.valueOf(outboxMaxLen));

        if (result == null || result.size() < 4) {
            throw new BusinessException(500, "Bid evaluation failed");
        }

        long okFlag = ((Number) result.get(0)).longValue();
        String code = (String) result.get(1);
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

        log.debug("Bid placed: auction={} bidder={} amount={} outboxId={}",
                auctionId, bidderId, amount, result.size() > 4 ? result.get(4) : "?");
        return PlaceBidResponse.builder()
                .auctionId(auctionId)
                .bidderId(bidderId)
                .amount(amount)
                .bidTime(bidTime)
                .build();
    }
}

package com.auction.auction.startup;

import com.auction.auction.domain.entity.Auction;
import com.auction.auction.domain.entity.Bid;
import com.auction.auction.domain.enums.AuctionStatus;
import com.auction.auction.repository.AuctionMapper;
import com.auction.auction.repository.BidMapper;
import com.auction.auction.service.RedisKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Rebuilds the Redis bidding state of ACTIVE auctions from MySQL on startup.
 *
 * <p>The bid fast-path lives entirely in Redis (auction:{id}:status +
 * auction:{id}:highest), but those keys are only written when the scheduler
 * transitions an auction. If Redis restarts mid-auction the status key vanishes and
 * place_bid.lua rejects every bid with AUCTION_NOT_ACTIVE -- permanently, because no
 * later transition rewrites it. This is the auction-side counterpart of
 * {@code RedisStockWarmer} in ticket-service.
 *
 * <p>Restore is strictly non-clobbering: the status key via SETNX, the highest hash
 * only when absent, so a healthy Redis (keys still present) is never overwritten with
 * stale MySQL data. The highest bid is rebuilt from the bid table (same source the
 * settlement scheduler trusts); with no bids it falls back to the starting price,
 * mirroring what activation writes.
 */
@Component
public class AuctionStateWarmer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(AuctionStateWarmer.class);

    private final AuctionMapper auctionMapper;
    private final BidMapper bidMapper;
    private final StringRedisTemplate redis;

    public AuctionStateWarmer(AuctionMapper auctionMapper,
                              BidMapper bidMapper,
                              StringRedisTemplate redis) {
        this.auctionMapper = auctionMapper;
        this.bidMapper = bidMapper;
        this.redis = redis;
    }

    @Override
    public void run(ApplicationArguments args) {
        try {
            List<Auction> activeAuctions = auctionMapper.findByStatus(AuctionStatus.ACTIVE.getValue());
            int restored = 0;
            for (Auction auction : activeAuctions) {
                if (restoreAuction(auction)) {
                    restored++;
                }
            }
            log.info("Auction Redis state warm-up: {} ACTIVE auctions checked, {} restored",
                    activeAuctions.size(), restored);
        } catch (Exception e) {
            log.error("Failed to warm up auction Redis state; ACTIVE auctions may reject bids "
                    + "until the keys are restored", e);
        }
    }

    private boolean restoreAuction(Auction auction) {
        boolean statusRestored = Boolean.TRUE.equals(redis.opsForValue()
                .setIfAbsent(RedisKeys.auctionStatus(auction.getId()), "ACTIVE"));

        boolean hashRestored = false;
        String highestKey = RedisKeys.auctionHighest(auction.getId());
        if (!Boolean.TRUE.equals(redis.hasKey(highestKey))) {
            redis.opsForHash().putAll(highestKey, buildHighestHash(auction));
            hashRestored = true;
        }

        if (statusRestored || hashRestored) {
            log.warn("Restored Redis bidding state for ACTIVE auction {} (status={}, highest={})",
                    auction.getId(), statusRestored, hashRestored);
            return true;
        }
        return false;
    }

    private Map<String, String> buildHighestHash(Auction auction) {
        Map<String, String> hash = new HashMap<>();
        Bid highest = bidMapper.findHighestBid(auction.getId());
        if (highest != null) {
            hash.put("bidder_id", String.valueOf(highest.getUserId()));
            hash.put("amount", highest.getAmount().toPlainString());
            hash.put("bid_time", highest.getBidTime() == null ? "0"
                    : String.valueOf(highest.getBidTime().toInstant(ZoneOffset.UTC).toEpochMilli()));
        } else {
            hash.put("bidder_id", "");
            hash.put("amount", auction.getStartingPrice().toPlainString());
            hash.put("bid_time", "0");
        }
        return hash;
    }
}

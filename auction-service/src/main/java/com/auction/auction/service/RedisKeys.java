package com.auction.auction.service;

public final class RedisKeys {

    public static String auctionStatus(Long auctionId) {
        return "auction:" + auctionId + ":status";
    }

    public static String auctionHighest(Long auctionId) {
        return "auction:" + auctionId + ":highest";
    }

    public static String hotAuctionsCache() {
        return "auction:hot:list";
    }

    /**
     * Outbox stream the place-bid script appends every accepted bid to, drained to
     * Kafka by BidOutboxRelay. One stream for all auctions: the relay wants a single
     * XREADGROUP, and Redis is single-threaded anyway, so per-auction streams would
     * buy no concurrency.
     */
    public static String bidOutboxStream() {
        return "auction:bid:outbox";
    }

    private RedisKeys() {
    }
}

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

    private RedisKeys() {
    }
}

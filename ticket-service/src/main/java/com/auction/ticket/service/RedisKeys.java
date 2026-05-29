package com.auction.ticket.service;

public final class RedisKeys {

    public static String stock(Long eventId, String ticketType) {
        return "stock:" + eventId + ":" + ticketType;
    }

    private RedisKeys() {
    }
}

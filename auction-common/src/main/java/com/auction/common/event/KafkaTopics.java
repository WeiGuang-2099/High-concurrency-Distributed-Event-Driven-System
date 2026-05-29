package com.auction.common.event;

public final class KafkaTopics {

    public static final String AUCTION_EVENTS = "auction-events";
    public static final String AUCTION_EVENTS_DLT = "auction-events-dlt";
    public static final String TICKET_EVENTS = "ticket-events";
    public static final String TICKET_EVENTS_DLT = "ticket-events-dlt";
    public static final String ORDER_EVENTS = "order-events";
    public static final String ORDER_EVENTS_DLT = "order-events-dlt";

    private KafkaTopics() {
    }
}

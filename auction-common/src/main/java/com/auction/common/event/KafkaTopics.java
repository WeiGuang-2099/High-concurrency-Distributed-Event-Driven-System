package com.auction.common.event;

public final class KafkaTopics {

    public static final String AUCTION_EVENTS = "auction-events";
    public static final String AUCTION_EVENTS_DLT = "auction-events-dlt";
    public static final String TICKET_EVENTS = "ticket-events";
    public static final String TICKET_EVENTS_DLT = "ticket-events-dlt";
    public static final String ORDER_EVENTS = "order-events";
    public static final String ORDER_EVENTS_DLT = "order-events-dlt";

    /**
     * Dead-letter topic for a source topic. Every service that dead-letters must agree on
     * these names, and each DLT must be declared with the same partition count as its
     * source: the recoverer republishes to the partition the record came from, which
     * fails if the broker auto-created the DLT with fewer partitions.
     */
    public static String dltFor(String topic) {
        switch (topic) {
            case AUCTION_EVENTS: return AUCTION_EVENTS_DLT;
            case TICKET_EVENTS: return TICKET_EVENTS_DLT;
            case ORDER_EVENTS: return ORDER_EVENTS_DLT;
            default: return topic + "-dlt";
        }
    }

    private KafkaTopics() {
    }
}

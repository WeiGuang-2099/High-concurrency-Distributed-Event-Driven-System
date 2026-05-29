package com.auction.common.event;

public final class EventTypes {

    public static final String AUCTION_CREATED = "AuctionCreated";
    public static final String AUCTION_ACTIVATED = "AuctionActivated";
    public static final String BID_PLACED = "BidPlaced";
    public static final String BID_OUTBID = "BidOutbid";
    public static final String AUCTION_SETTLED = "AuctionSettled";
    public static final String AUCTION_EXPIRED = "AuctionExpired";
    public static final String TICKET_CREATED = "TicketCreated";
    public static final String STOCK_RESERVED = "StockReserved";
    public static final String STOCK_CONFIRMED = "StockConfirmed";
    public static final String STOCK_RELEASED = "StockReleased";
    public static final String ORDER_CREATED = "OrderCreated";
    public static final String PAYMENT_INITIATED = "PaymentInitiated";
    public static final String PAYMENT_COMPLETED = "PaymentCompleted";
    public static final String ORDER_CANCELLED = "OrderCancelled";
    public static final String ORDER_EXPIRED = "OrderExpired";

    private EventTypes() {
    }
}

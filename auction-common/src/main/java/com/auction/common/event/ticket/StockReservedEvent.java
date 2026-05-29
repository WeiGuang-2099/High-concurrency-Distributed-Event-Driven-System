package com.auction.common.event.ticket;

import com.auction.common.event.BaseEvent;
import com.auction.common.event.EventTypes;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class StockReservedEvent extends BaseEvent {

    private Long eventId;
    private String ticketType;
    private Long reservationId;
    private Long userId;
    private int quantity;

    public StockReservedEvent(Long eventId, String ticketType, Long reservationId, Long userId, int quantity) {
        super(String.valueOf(eventId), EventTypes.STOCK_RESERVED);
        this.eventId = eventId;
        this.ticketType = ticketType;
        this.reservationId = reservationId;
        this.userId = userId;
        this.quantity = quantity;
    }
}

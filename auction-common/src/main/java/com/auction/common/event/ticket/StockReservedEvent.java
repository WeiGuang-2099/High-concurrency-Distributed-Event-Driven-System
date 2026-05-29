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

    private Long ticketEventId;
    private String ticketType;
    private Long reservationId;
    private Long userId;
    private int quantity;

    public StockReservedEvent(Long ticketEventId, String ticketType, Long reservationId, Long userId, int quantity) {
        super(String.valueOf(ticketEventId), EventTypes.STOCK_RESERVED);
        this.ticketEventId = ticketEventId;
        this.ticketType = ticketType;
        this.reservationId = reservationId;
        this.userId = userId;
        this.quantity = quantity;
    }
}

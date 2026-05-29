package com.auction.common.event.ticket;

import com.auction.common.event.BaseEvent;
import com.auction.common.event.EventTypes;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class StockReleasedEvent extends BaseEvent {

    private Long reservationId;
    private Long eventId;
    private String ticketType;
    private int quantity;
    private String reason;

    public StockReleasedEvent(Long reservationId, Long eventId, String ticketType, int quantity, String reason) {
        super(String.valueOf(reservationId), EventTypes.STOCK_RELEASED);
        this.reservationId = reservationId;
        this.eventId = eventId;
        this.ticketType = ticketType;
        this.quantity = quantity;
        this.reason = reason;
    }
}

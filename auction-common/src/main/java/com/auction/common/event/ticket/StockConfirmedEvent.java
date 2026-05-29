package com.auction.common.event.ticket;

import com.auction.common.event.BaseEvent;
import com.auction.common.event.EventTypes;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class StockConfirmedEvent extends BaseEvent {

    private Long reservationId;
    private Long userId;

    public StockConfirmedEvent(Long reservationId, Long userId) {
        super(String.valueOf(reservationId), EventTypes.STOCK_CONFIRMED);
        this.reservationId = reservationId;
        this.userId = userId;
    }
}

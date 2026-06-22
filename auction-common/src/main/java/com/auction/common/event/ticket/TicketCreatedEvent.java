package com.auction.common.event.ticket;

import com.auction.common.event.BaseEvent;
import com.auction.common.event.EventTypes;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class TicketCreatedEvent extends BaseEvent {

    private Long ticketEventId;
    private String ticketType;
    private int totalQuantity;

    public TicketCreatedEvent(Long ticketEventId, String ticketType, int totalQuantity) {
        super(String.valueOf(ticketEventId), EventTypes.TICKET_CREATED);
        this.ticketEventId = ticketEventId;
        this.ticketType = ticketType;
        this.totalQuantity = totalQuantity;
    }
}



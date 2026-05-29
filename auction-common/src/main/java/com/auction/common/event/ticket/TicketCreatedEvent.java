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

    private Long eventId;
    private String ticketType;
    private int totalQuantity;

    public TicketCreatedEvent(Long eventId, String ticketType, int totalQuantity) {
        super(String.valueOf(eventId), EventTypes.TICKET_CREATED);
        this.eventId = eventId;
        this.ticketType = ticketType;
        this.totalQuantity = totalQuantity;
    }
}

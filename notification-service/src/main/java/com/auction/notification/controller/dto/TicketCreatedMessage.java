package com.auction.notification.controller.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TicketCreatedMessage {

    private Long eventId;
    private String ticketType;
    private int totalQuantity;
}

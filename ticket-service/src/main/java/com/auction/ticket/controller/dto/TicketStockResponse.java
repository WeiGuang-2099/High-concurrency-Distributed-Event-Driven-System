package com.auction.ticket.controller.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TicketStockResponse {

    private Long stockId;
    private Long eventId;
    private String ticketType;
    private Integer totalQuantity;
    private Integer availableQuantity;
    private Integer reservedQuantity;
    private Integer soldQuantity;
}

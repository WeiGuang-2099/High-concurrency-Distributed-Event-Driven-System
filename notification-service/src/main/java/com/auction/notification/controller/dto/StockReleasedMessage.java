package com.auction.notification.controller.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StockReleasedMessage {

    private Long reservationId;
    private String ticketType;
    private int quantity;
    private String reason;
}

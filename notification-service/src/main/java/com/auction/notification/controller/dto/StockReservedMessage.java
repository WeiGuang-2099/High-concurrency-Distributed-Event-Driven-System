package com.auction.notification.controller.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StockReservedMessage {

    private Long reservationId;
    private Long userId;
    private int quantity;
    private String ticketType;
}

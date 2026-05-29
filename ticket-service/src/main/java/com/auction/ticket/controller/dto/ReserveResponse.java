package com.auction.ticket.controller.dto;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Builder
public class ReserveResponse {

    private Long reservationId;
    private Long eventId;
    private String ticketType;
    private Integer quantity;
    private LocalDateTime expireAt;
}

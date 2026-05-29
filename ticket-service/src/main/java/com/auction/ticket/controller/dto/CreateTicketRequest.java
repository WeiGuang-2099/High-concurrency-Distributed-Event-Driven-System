package com.auction.ticket.controller.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class CreateTicketRequest {

    @NotNull(message = "eventId is required")
    private Long eventId;

    @NotBlank(message = "ticketType is required")
    private String ticketType;

    @NotNull(message = "totalQuantity is required")
    @Min(value = 1, message = "totalQuantity must be at least 1")
    private Integer totalQuantity;
}

package com.auction.ticket.controller.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class ReserveRequest {

    @NotNull(message = "eventId is required")
    private Long eventId;

    @NotBlank(message = "ticketType is required")
    private String ticketType;

    @NotNull(message = "quantity is required")
    @Min(value = 1, message = "quantity must be at least 1")
    private Integer quantity;
}

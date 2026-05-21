package com.auction.auction.controller.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
public class CreateAuctionRequest {

    @NotBlank(message = "eventName must not be blank")
    @Size(max = 200)
    private String eventName;

    @Size(max = 1000)
    private String description;

    private Long ticketTypeId;

    @NotNull
    @DecimalMin(value = "0.01", message = "startingPrice must be > 0")
    private BigDecimal startingPrice;

    @NotNull
    private LocalDateTime startTime;

    @NotNull
    private LocalDateTime endTime;
}

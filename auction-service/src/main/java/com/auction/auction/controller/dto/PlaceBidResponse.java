package com.auction.auction.controller.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PlaceBidResponse {

    private Long auctionId;
    private Long bidderId;
    private BigDecimal amount;
    private Instant bidTime;
}

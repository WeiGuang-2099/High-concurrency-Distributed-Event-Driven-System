package com.auction.auction.controller.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BidHistoryItem {

    private Long bidderId;
    private String bidderUsername;
    private BigDecimal amount;
    private LocalDateTime bidTime;
}

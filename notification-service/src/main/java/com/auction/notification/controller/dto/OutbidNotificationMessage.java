package com.auction.notification.controller.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutbidNotificationMessage {

    private Long auctionId;
    private Long outbidUserId;
    private BigDecimal outbidAmount;
    private Long newBidderId;
    private BigDecimal newAmount;
}

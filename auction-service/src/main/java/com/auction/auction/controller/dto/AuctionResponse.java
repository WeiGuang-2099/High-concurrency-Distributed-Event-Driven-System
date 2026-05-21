package com.auction.auction.controller.dto;

import com.auction.auction.domain.entity.Auction;
import com.auction.auction.domain.enums.AuctionStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuctionResponse {

    private Long id;
    private String eventName;
    private String description;
    private Long ticketTypeId;
    private BigDecimal startingPrice;
    private BigDecimal currentHighestBid;
    private Long currentHighestBidderId;
    private AuctionStatus status;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private Long winnerId;
    private long remainingSeconds;

    public static AuctionResponse fromEntity(Auction auction) {
        long remaining = 0L;
        if (auction.getEndTime() != null) {
            long now = System.currentTimeMillis() / 1000L;
            long end = auction.getEndTime().toEpochSecond(ZoneOffset.UTC);
            remaining = Math.max(0L, end - now);
        }
        return AuctionResponse.builder()
                .id(auction.getId())
                .eventName(auction.getEventName())
                .description(auction.getDescription())
                .ticketTypeId(auction.getTicketTypeId())
                .startingPrice(auction.getStartingPrice())
                .currentHighestBid(auction.getCurrentHighestBid())
                .currentHighestBidderId(auction.getCurrentHighestBidderId())
                .status(auction.getStatus())
                .startTime(auction.getStartTime())
                .endTime(auction.getEndTime())
                .winnerId(auction.getWinnerId())
                .remainingSeconds(remaining)
                .build();
    }
}

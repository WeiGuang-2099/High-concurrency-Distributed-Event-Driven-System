package com.auction.common.event.auction;

import com.auction.common.event.BaseEvent;
import com.auction.common.event.EventTypes;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class BidPlacedEvent extends BaseEvent {

    private Long auctionId;
    private Long bidderId;
    private String bidderUsername;
    private BigDecimal amount;
    private Instant bidTime;
    private Long previousBidderId;
    private BigDecimal previousAmount;

    public BidPlacedEvent(Long auctionId,
                          Long bidderId,
                          String bidderUsername,
                          BigDecimal amount,
                          Instant bidTime,
                          Long previousBidderId,
                          BigDecimal previousAmount) {
        super(String.valueOf(auctionId), EventTypes.BID_PLACED);
        this.auctionId = auctionId;
        this.bidderId = bidderId;
        this.bidderUsername = bidderUsername;
        this.amount = amount;
        this.bidTime = bidTime;
        this.previousBidderId = previousBidderId;
        this.previousAmount = previousAmount;
    }
}

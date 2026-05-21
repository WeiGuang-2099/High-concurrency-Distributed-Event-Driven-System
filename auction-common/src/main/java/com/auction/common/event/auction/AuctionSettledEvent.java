package com.auction.common.event.auction;

import com.auction.common.event.BaseEvent;
import com.auction.common.event.EventTypes;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class AuctionSettledEvent extends BaseEvent {

    private Long auctionId;
    private Long winnerId;
    private BigDecimal finalAmount;
    private Long ticketTypeId;

    public AuctionSettledEvent(Long auctionId,
                               Long winnerId,
                               BigDecimal finalAmount,
                               Long ticketTypeId) {
        super(String.valueOf(auctionId), EventTypes.AUCTION_SETTLED);
        this.auctionId = auctionId;
        this.winnerId = winnerId;
        this.finalAmount = finalAmount;
        this.ticketTypeId = ticketTypeId;
    }
}

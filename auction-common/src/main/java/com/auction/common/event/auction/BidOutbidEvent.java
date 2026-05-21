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
public class BidOutbidEvent extends BaseEvent {

    private Long auctionId;
    private Long outbidUserId;
    private BigDecimal outbidAmount;
    private Long newBidderId;
    private BigDecimal newAmount;

    public BidOutbidEvent(Long auctionId,
                          Long outbidUserId,
                          BigDecimal outbidAmount,
                          Long newBidderId,
                          BigDecimal newAmount) {
        super(String.valueOf(auctionId), EventTypes.BID_OUTBID);
        this.auctionId = auctionId;
        this.outbidUserId = outbidUserId;
        this.outbidAmount = outbidAmount;
        this.newBidderId = newBidderId;
        this.newAmount = newAmount;
    }
}

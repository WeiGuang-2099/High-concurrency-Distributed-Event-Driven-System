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
public class AuctionActivatedEvent extends BaseEvent {

    private Long auctionId;
    private BigDecimal startingPrice;

    public AuctionActivatedEvent(Long auctionId, BigDecimal startingPrice) {
        super(String.valueOf(auctionId), EventTypes.AUCTION_ACTIVATED);
        this.auctionId = auctionId;
        this.startingPrice = startingPrice;
    }
}

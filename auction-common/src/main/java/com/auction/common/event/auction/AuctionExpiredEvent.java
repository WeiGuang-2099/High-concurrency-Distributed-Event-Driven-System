package com.auction.common.event.auction;

import com.auction.common.event.BaseEvent;
import com.auction.common.event.EventTypes;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class AuctionExpiredEvent extends BaseEvent {

    private Long auctionId;
    private String reason;

    public AuctionExpiredEvent(Long auctionId, String reason) {
        super(String.valueOf(auctionId), EventTypes.AUCTION_EXPIRED);
        this.auctionId = auctionId;
        this.reason = reason;
    }
}

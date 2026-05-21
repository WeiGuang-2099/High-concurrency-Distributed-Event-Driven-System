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
public class AuctionCreatedEvent extends BaseEvent {

    private Long auctionId;
    private String eventName;
    private String description;
    private Long ticketTypeId;
    private BigDecimal startingPrice;
    private Instant startTime;
    private Instant endTime;
    private Long createdBy;

    public AuctionCreatedEvent(Long auctionId) {
        super(String.valueOf(auctionId), EventTypes.AUCTION_CREATED);
        this.auctionId = auctionId;
    }
}

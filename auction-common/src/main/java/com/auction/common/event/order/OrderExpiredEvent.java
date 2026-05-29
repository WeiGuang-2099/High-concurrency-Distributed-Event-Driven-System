package com.auction.common.event.order;

import com.auction.common.event.BaseEvent;
import com.auction.common.event.EventTypes;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class OrderExpiredEvent extends BaseEvent {

    private Long orderId;
    private Long userId;

    public OrderExpiredEvent(Long orderId, Long userId) {
        super(String.valueOf(orderId), EventTypes.ORDER_EXPIRED);
        this.orderId = orderId;
        this.userId = userId;
    }
}

package com.auction.common.event.order;

import com.auction.common.event.BaseEvent;
import com.auction.common.event.EventTypes;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class OrderCancelledEvent extends BaseEvent {

    private Long orderId;
    private Long userId;
    private String reason;

    public OrderCancelledEvent(Long orderId, Long userId, String reason) {
        super(String.valueOf(orderId), EventTypes.ORDER_CANCELLED);
        this.orderId = orderId;
        this.userId = userId;
        this.reason = reason;
    }
}

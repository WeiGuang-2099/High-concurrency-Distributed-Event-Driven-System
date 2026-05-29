package com.auction.common.event.order;

import com.auction.common.event.BaseEvent;
import com.auction.common.event.EventTypes;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class OrderCreatedEvent extends BaseEvent {

    private Long orderId;
    private Long userId;
    private String orderType;
    private Long referenceId;
    private BigDecimal amount;

    public OrderCreatedEvent(Long orderId, Long userId, String orderType,
                             Long referenceId, BigDecimal amount) {
        super(String.valueOf(orderId), EventTypes.ORDER_CREATED);
        this.orderId = orderId;
        this.userId = userId;
        this.orderType = orderType;
        this.referenceId = referenceId;
        this.amount = amount;
    }
}

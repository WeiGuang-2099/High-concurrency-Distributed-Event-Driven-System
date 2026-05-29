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
public class PaymentInitiatedEvent extends BaseEvent {

    private Long orderId;
    private Long userId;
    private BigDecimal amount;

    public PaymentInitiatedEvent(Long orderId, Long userId, BigDecimal amount) {
        super(String.valueOf(orderId), EventTypes.PAYMENT_INITIATED);
        this.orderId = orderId;
        this.userId = userId;
        this.amount = amount;
    }
}

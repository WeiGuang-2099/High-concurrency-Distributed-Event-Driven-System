package com.auction.order.event;

import com.auction.common.event.KafkaTopics;
import com.auction.common.event.order.OrderCancelledEvent;
import com.auction.common.event.order.OrderCreatedEvent;
import com.auction.common.event.order.OrderExpiredEvent;
import com.auction.common.event.order.PaymentCompletedEvent;
import com.auction.common.event.order.PaymentInitiatedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
public class OrderEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishOrderCreated(Long orderId, Long userId, String orderType,
                                    Long referenceId, BigDecimal amount) {
        OrderCreatedEvent event = new OrderCreatedEvent(orderId, userId, orderType, referenceId, amount);
        kafkaTemplate.send(KafkaTopics.ORDER_EVENTS, String.valueOf(orderId), event);
    }

    public void publishPaymentInitiated(Long orderId, Long userId, BigDecimal amount) {
        PaymentInitiatedEvent event = new PaymentInitiatedEvent(orderId, userId, amount);
        kafkaTemplate.send(KafkaTopics.ORDER_EVENTS, String.valueOf(orderId), event);
    }

    public void publishPaymentCompleted(Long orderId, Long userId, BigDecimal amount) {
        PaymentCompletedEvent event = new PaymentCompletedEvent(orderId, userId, amount);
        kafkaTemplate.send(KafkaTopics.ORDER_EVENTS, String.valueOf(orderId), event);
    }

    public void publishOrderCancelled(Long orderId, Long userId, String reason) {
        OrderCancelledEvent event = new OrderCancelledEvent(orderId, userId, reason);
        kafkaTemplate.send(KafkaTopics.ORDER_EVENTS, String.valueOf(orderId), event);
    }

    public void publishOrderExpired(Long orderId, Long userId) {
        OrderExpiredEvent event = new OrderExpiredEvent(orderId, userId);
        kafkaTemplate.send(KafkaTopics.ORDER_EVENTS, String.valueOf(orderId), event);
    }
}

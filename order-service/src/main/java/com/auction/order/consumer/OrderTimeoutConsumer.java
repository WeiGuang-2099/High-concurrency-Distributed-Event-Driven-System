package com.auction.order.consumer;

import com.auction.order.service.impl.OrderServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OrderTimeoutConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderTimeoutConsumer.class);

    private final OrderServiceImpl orderService;

    public OrderTimeoutConsumer(OrderServiceImpl orderService) {
        this.orderService = orderService;
    }

    @RabbitListener(queues = "order-timeout-queue")
    @Transactional
    public void handleOrderTimeout(Long orderId) {
        log.info("Received order timeout delayed message for orderId={}", orderId);
        orderService.expireOrder(orderId);
    }
}

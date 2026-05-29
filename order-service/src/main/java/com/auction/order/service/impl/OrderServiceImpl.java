package com.auction.order.service.impl;

import com.auction.common.exception.BusinessException;
import com.auction.order.client.TicketFeignClient;
import com.auction.order.config.RabbitMQConfig;
import com.auction.order.controller.dto.CreateOrderRequest;
import com.auction.order.controller.dto.OrderResponse;
import com.auction.order.controller.dto.PayResponse;
import com.auction.order.domain.enums.OrderStatus;
import com.auction.order.domain.enums.OrderType;
import com.auction.order.domain.enums.PaymentStatus;
import com.auction.order.domain.entity.Order;
import com.auction.order.domain.entity.Payment;
import com.auction.order.event.OrderEventProducer;
import com.auction.order.repository.OrderMapper;
import com.auction.order.repository.PaymentMapper;
import com.auction.order.service.OrderService;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Random;

@Service
public class OrderServiceImpl implements OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderServiceImpl.class);
    private static final int ORDER_TIMEOUT_MINUTES = 30;
    private static final Random RANDOM = new Random();

    private final OrderMapper orderMapper;
    private final PaymentMapper paymentMapper;
    private final TicketFeignClient ticketFeignClient;
    private final OrderEventProducer eventProducer;
    private final RabbitTemplate rabbitTemplate;

    @Value("${payment.mock.success-rate:0.95}")
    private double successRate;

    @Value("${payment.mock.min-latency-ms:500}")
    private int minLatencyMs;

    @Value("${payment.mock.max-latency-ms:2000}")
    private int maxLatencyMs;

    public OrderServiceImpl(OrderMapper orderMapper,
                            PaymentMapper paymentMapper,
                            TicketFeignClient ticketFeignClient,
                            OrderEventProducer eventProducer,
                            RabbitTemplate rabbitTemplate) {
        this.orderMapper = orderMapper;
        this.paymentMapper = paymentMapper;
        this.ticketFeignClient = ticketFeignClient;
        this.eventProducer = eventProducer;
        this.rabbitTemplate = rabbitTemplate;
    }

    @Override
    @Transactional
    public OrderResponse createFromTicket(Long userId, CreateOrderRequest request) {
        Order order = new Order();
        order.setUserId(userId);
        order.setType(OrderType.TICKET);
        order.setReferenceId(request.getReservationId());
        order.setAmount(request.getAmount());
        order.setStatus(OrderStatus.CREATED);
        orderMapper.insert(order);

        final Long orderId = order.getId();
        sendTimeoutMessage(orderId);

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                eventProducer.publishOrderCreated(orderId, userId, "TICKET",
                        request.getReservationId(), request.getAmount());
                log.info("Created TICKET order: id={}, userId={}, reservationId={}",
                        orderId, userId, request.getReservationId());
            }
        });

        return toResponse(order);
    }

    @Override
    @Transactional
    public OrderResponse createFromAuction(Long auctionId, Long winnerId, BigDecimal amount) {
        Order order = new Order();
        order.setUserId(winnerId);
        order.setType(OrderType.AUCTION);
        order.setReferenceId(auctionId);
        order.setAmount(amount);
        order.setStatus(OrderStatus.CREATED);
        orderMapper.insert(order);

        final Long orderId = order.getId();
        sendTimeoutMessage(orderId);

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                eventProducer.publishOrderCreated(orderId, winnerId, "AUCTION",
                        auctionId, amount);
                log.info("Created AUCTION order: id={}, winnerId={}, auctionId={}",
                        orderId, winnerId, auctionId);
            }
        });

        return toResponse(order);
    }

    @Override
    @Transactional
    public PayResponse pay(Long userId, Long orderId) {
        Order order = orderMapper.selectById(orderId);
        if (order == null) {
            throw new BusinessException(404, "Order not found");
        }
        if (!order.getUserId().equals(userId)) {
            throw new BusinessException(403, "Not your order");
        }

        if (order.getStatus() == OrderStatus.PAYING) {
            return PayResponse.builder()
                    .orderId(orderId)
                    .status(OrderStatus.PAYING.name())
                    .paymentStatus("IN_PROGRESS")
                    .message("Payment already in progress")
                    .build();
        }
        if (order.getStatus() == OrderStatus.PAID) {
            return PayResponse.builder()
                    .orderId(orderId)
                    .status(OrderStatus.PAID.name())
                    .paymentStatus("SUCCESS")
                    .message("Already paid")
                    .build();
        }
        if (order.getStatus() == OrderStatus.COMPLETED) {
            return PayResponse.builder()
                    .orderId(orderId)
                    .status(OrderStatus.COMPLETED.name())
                    .paymentStatus("SUCCESS")
                    .message("Order already completed")
                    .build();
        }

        if (order.getStatus() != OrderStatus.CREATED) {
            throw new BusinessException(409, "Cannot pay order in status " + order.getStatus());
        }

        int rows = orderMapper.compareAndSetStatus(orderId,
                OrderStatus.CREATED.name(), OrderStatus.PAYING.name());
        if (rows == 0) {
            throw new BusinessException(409, "Order status changed, please retry");
        }
        order.setStatus(OrderStatus.PAYING);

        Payment payment = new Payment();
        payment.setOrderId(orderId);
        payment.setPaymentMethod("MOCK");
        payment.setAmount(order.getAmount());
        payment.setStatus(PaymentStatus.PENDING);
        paymentMapper.insert(payment);

        final Long oid = orderId;
        final Long uid = userId;
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                eventProducer.publishPaymentInitiated(oid, uid, order.getAmount());
            }
        });

        boolean success = simulatePayment();

        if (success) {
            return handlePaymentSuccess(orderId, userId, order.getAmount(), payment);
        } else {
            return handlePaymentFailure(orderId, payment);
        }
    }

    @Override
    @Transactional
    public void cancel(Long userId, Long orderId) {
        Order order = orderMapper.selectById(orderId);
        if (order == null) {
            throw new BusinessException(404, "Order not found");
        }
        if (!order.getUserId().equals(userId)) {
            throw new BusinessException(403, "Not your order");
        }
        if (order.getStatus() != OrderStatus.CREATED) {
            throw new BusinessException(409, "Can only cancel orders in CREATED status");
        }

        int rows = orderMapper.compareAndSetStatus(orderId,
                OrderStatus.CREATED.name(), OrderStatus.CANCELLED.name());
        if (rows == 0) {
            throw new BusinessException(409, "Order status changed, please retry");
        }

        if (order.getType() == OrderType.TICKET) {
            try {
                ticketFeignClient.cancelReservation(order.getReferenceId());
            } catch (Exception e) {
                log.error("Failed to cancel reservation for order={}", orderId, e);
            }
        }

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                eventProducer.publishOrderCancelled(orderId, userId, "USER_CANCEL");
                log.info("Cancelled order: id={}, userId={}", orderId, userId);
            }
        });
    }

    @Override
    public OrderResponse getById(Long userId, Long orderId) {
        Order order = orderMapper.selectById(orderId);
        if (order == null) {
            throw new BusinessException(404, "Order not found");
        }
        if (!order.getUserId().equals(userId)) {
            throw new BusinessException(403, "Not your order");
        }
        return toResponse(order);
    }

    @Override
    public Page<OrderResponse> listByUserId(Long userId, int page, int size) {
        LambdaQueryWrapper<Order> wrapper = new LambdaQueryWrapper<Order>()
                .eq(Order::getUserId, userId)
                .orderByDesc(Order::getCreatedAt);

        Page<Order> orderPage = orderMapper.selectPage(new Page<>(page, size), wrapper);

        Page<OrderResponse> responsePage = new Page<>(orderPage.getCurrent(),
                orderPage.getSize(), orderPage.getTotal());
        responsePage.setRecords(orderPage.getRecords().stream()
                .map(this::toResponse)
                .toList());
        return responsePage;
    }

    public void expireOrder(Long orderId) {
        Order order = orderMapper.selectById(orderId);
        if (order == null) {
            log.warn("Order not found for expiry: id={}", orderId);
            return;
        }
        if (order.getStatus() != OrderStatus.CREATED && order.getStatus() != OrderStatus.PAYING) {
            log.info("Order already processed: id={}, status={}", orderId, order.getStatus());
            return;
        }

        String expectedStatus = order.getStatus().name();
        int rows = orderMapper.compareAndSetStatus(orderId, expectedStatus,
                OrderStatus.EXPIRED.name());
        if (rows == 0) {
            log.info("Order status race: id={}", orderId);
            return;
        }

        if (order.getType() == OrderType.TICKET) {
            try {
                ticketFeignClient.cancelReservation(order.getReferenceId());
            } catch (Exception e) {
                log.error("Failed to cancel reservation for expired order={}", orderId, e);
            }
        }

        eventProducer.publishOrderExpired(orderId, order.getUserId());
        log.info("Expired order: id={}", orderId);
    }

    private boolean simulatePayment() {
        int latency = minLatencyMs + RANDOM.nextInt(maxLatencyMs - minLatencyMs);
        try {
            Thread.sleep(latency);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return RANDOM.nextDouble() < successRate;
    }

    @Transactional
    private PayResponse handlePaymentSuccess(Long orderId, Long userId,
                                              BigDecimal amount, Payment payment) {
        orderMapper.compareAndSetStatus(orderId,
                OrderStatus.PAYING.name(), OrderStatus.PAID.name());

        payment.setStatus(PaymentStatus.SUCCESS);
        paymentMapper.updateById(payment);

        Order paidOrder = new Order();
        paidOrder.setId(orderId);
        paidOrder.setPaidAt(LocalDateTime.now());
        orderMapper.updateById(paidOrder);

        Order order = orderMapper.selectById(orderId);
        if (order.getType() == OrderType.TICKET) {
            try {
                ticketFeignClient.confirmReservation(order.getReferenceId());
            } catch (Exception e) {
                log.error("Failed to confirm reservation for order={}", orderId, e);
            }
        }

        orderMapper.compareAndSetStatus(orderId,
                OrderStatus.PAID.name(), OrderStatus.COMPLETED.name());

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                eventProducer.publishPaymentCompleted(orderId, userId, amount);
                log.info("Payment completed: orderId={}", orderId);
            }
        });

        return PayResponse.builder()
                .orderId(orderId)
                .status(OrderStatus.COMPLETED.name())
                .paymentStatus("SUCCESS")
                .message("Payment successful")
                .build();
    }

    @Transactional
    private PayResponse handlePaymentFailure(Long orderId, Payment payment) {
        orderMapper.compareAndSetStatus(orderId,
                OrderStatus.PAYING.name(), OrderStatus.CREATED.name());

        payment.setStatus(PaymentStatus.FAILED);
        paymentMapper.updateById(payment);

        log.info("Payment failed (mock): orderId={}", orderId);

        return PayResponse.builder()
                .orderId(orderId)
                .status(OrderStatus.CREATED.name())
                .paymentStatus("FAILED")
                .message("Payment failed, please retry")
                .build();
    }

    private void sendTimeoutMessage(Long orderId) {
        rabbitTemplate.convertAndSend(
                RabbitMQConfig.DELAY_EXCHANGE,
                RabbitMQConfig.ORDER_TIMEOUT_ROUTING_KEY,
                orderId,
                msg -> {
                    msg.getMessageProperties().setDelay(ORDER_TIMEOUT_MINUTES * 60 * 1000);
                    return msg;
                });
    }

    private OrderResponse toResponse(Order order) {
        return OrderResponse.builder()
                .id(order.getId())
                .userId(order.getUserId())
                .type(order.getType().name())
                .referenceId(order.getReferenceId())
                .amount(order.getAmount())
                .status(order.getStatus().name())
                .createdAt(order.getCreatedAt())
                .paidAt(order.getPaidAt())
                .build();
    }
}

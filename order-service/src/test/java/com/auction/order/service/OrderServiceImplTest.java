package com.auction.order.service;

import com.auction.common.exception.BusinessException;
import com.auction.order.client.TicketFeignClient;
import com.auction.order.controller.dto.CreateOrderRequest;
import com.auction.order.controller.dto.OrderResponse;
import com.auction.order.controller.dto.PayResponse;
import com.auction.order.domain.entity.Order;
import com.auction.order.domain.entity.Payment;
import com.auction.order.domain.enums.OrderStatus;
import com.auction.order.domain.enums.OrderType;
import com.auction.order.domain.enums.PaymentStatus;
import com.auction.order.event.OrderEventProducer;
import com.auction.order.repository.OrderMapper;
import com.auction.order.repository.PaymentMapper;
import com.auction.order.service.impl.OrderServiceImpl;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("OrderService - 订单状态机与支付")
class OrderServiceImplTest {

    @Mock private OrderMapper orderMapper;
    @Mock private PaymentMapper paymentMapper;
    @Mock private TicketFeignClient ticketFeignClient;
    @Mock private OrderEventProducer eventProducer;
    @Mock private RabbitTemplate rabbitTemplate;

    @InjectMocks private OrderServiceImpl orderService;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(orderService, "successRate", 1.0); // Always succeed
        ReflectionTestUtils.setField(orderService, "minLatencyMs", 0);
        ReflectionTestUtils.setField(orderService, "maxLatencyMs", 1);
        TransactionSynchronizationManager.initSynchronization();
    }

    @AfterEach
    void tearDown() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    private void simulateCommit() {
        List<TransactionSynchronization> syncs = TransactionSynchronizationManager.getSynchronizations();
        for (TransactionSynchronization sync : syncs) {
            sync.afterCommit();
        }
    }

    private Order createOrder(Long id, Long userId, OrderStatus status, OrderType type, Long referenceId) {
        Order order = new Order();
        order.setId(id);
        order.setUserId(userId);
        order.setType(type);
        order.setReferenceId(referenceId);
        order.setAmount(new BigDecimal("100.00"));
        order.setStatus(status);
        return order;
    }

    // ==================== createFromTicket ====================

    @Nested
    @DisplayName("createFromTicket() 从票务预订创建订单")
    class CreateFromTicketTest {

        @Test
        @DisplayName("正常创建 - 状态为CREATED")
        void createFromTicket_success() {
            CreateOrderRequest request = new CreateOrderRequest();
            request.setReservationId(10L);
            request.setAmount(new BigDecimal("150.00"));

            doAnswer(invocation -> {
                Order order = invocation.getArgument(0);
                order.setId(1L);
                return 1;
            }).when(orderMapper).insert(any(Order.class));

            OrderResponse response = orderService.createFromTicket(1L, request);

            assertThat(response).isNotNull();
            assertThat(response.getId()).isEqualTo(1L);
            assertThat(response.getType()).isEqualTo("TICKET");
            assertThat(response.getReferenceId()).isEqualTo(10L);
            assertThat(response.getStatus()).isEqualTo("CREATED");
            assertThat(response.getAmount()).isEqualByComparingTo("150.00");
        }

        @Test
        @DisplayName("提交后 - 发送超时延迟消息和 OrderCreated 事件")
        void createFromTicket_afterCommit_sendsTimeoutAndEvent() {
            CreateOrderRequest request = new CreateOrderRequest();
            request.setReservationId(10L);
            request.setAmount(new BigDecimal("150.00"));

            doAnswer(invocation -> {
                Order order = invocation.getArgument(0);
                order.setId(1L);
                return 1;
            }).when(orderMapper).insert(any(Order.class));

            orderService.createFromTicket(1L, request);
            simulateCommit();

            verify(rabbitTemplate).convertAndSend(
                    eq("delay.exchange"),
                    eq("order.timeout"),
                    eq(1L),
                    any(org.springframework.amqp.core.MessagePostProcessor.class));
            verify(eventProducer).publishOrderCreated(eq(1L), eq(1L), eq("TICKET"), eq(10L), any());
        }
    }

    // ==================== createFromAuction ====================

    @Nested
    @DisplayName("createFromAuction() 从拍卖赢家的建订单")
    class CreateFromAuctionTest {

        @Test
        @DisplayName("正常创建 - 类型为AUCTION")
        void createFromAuction_success() {
            doAnswer(invocation -> {
                Order order = invocation.getArgument(0);
                order.setId(1L);
                return 1;
            }).when(orderMapper).insert(any(Order.class));

            OrderResponse response = orderService.createFromAuction(
                    100L, 5L, new BigDecimal("500.00"));

            assertThat(response.getType()).isEqualTo("AUCTION");
            assertThat(response.getUserId()).isEqualTo(5L);
            assertThat(response.getReferenceId()).isEqualTo(100L);
            assertThat(response.getAmount()).isEqualByComparingTo("500.00");
        }
    }

    // ==================== pay ====================

    @Nested
    @DisplayName("pay() 支付订单")
    class PayTest {

        @Test
        @DisplayName("正常支付成功 - 状态 CREATED -> PAYING -> PAID -> COMPLETED")
        void pay_success() {
            Order order = createOrder(1L, 10L, OrderStatus.CREATED, OrderType.AUCTION, 100L);

            when(orderMapper.selectById(1L)).thenReturn(order);
            when(orderMapper.compareAndSetStatus(eq(1L), eq("CREATED"), eq("PAYING"))).thenReturn(1);
            when(orderMapper.compareAndSetStatus(eq(1L), eq("PAYING"), eq("PAID"))).thenReturn(1);
            when(orderMapper.compareAndSetStatus(eq(1L), eq("PAID"), eq("COMPLETED"))).thenReturn(1);

            doAnswer(invocation -> {
                Payment payment = invocation.getArgument(0);
                payment.setId(1L);
                return 1;
            }).when(paymentMapper).insert(any(Payment.class));

            PayResponse response = orderService.pay(10L, 1L);

            assertThat(response).isNotNull();
            assertThat(response.getStatus()).isEqualTo("COMPLETED");
            assertThat(response.getPaymentStatus()).isEqualTo("SUCCESS");

            // Verify all state transitions
            verify(orderMapper).compareAndSetStatus(1L, "CREATED", "PAYING");
            verify(orderMapper).compareAndSetStatus(1L, "PAYING", "PAID");
            verify(orderMapper).compareAndSetStatus(1L, "PAID", "COMPLETED");
        }

        @Test
        @DisplayName("票务订单支付成功 - 调用Feign确认预订")
        void pay_ticketOrder_callsConfirmReservation() {
            Order order = createOrder(1L, 10L, OrderStatus.CREATED, OrderType.TICKET, 50L);

            when(orderMapper.selectById(1L)).thenReturn(order);
            when(orderMapper.compareAndSetStatus(any(), any(), any())).thenReturn(1);

            doAnswer(invocation -> {
                Payment payment = invocation.getArgument(0);
                payment.setId(1L);
                return 1;
            }).when(paymentMapper).insert(any(Payment.class));

            orderService.pay(10L, 1L);

            verify(ticketFeignClient).confirmReservation(50L);
        }

        @Test
        @DisplayName("支付确认失败 - 回滚订单状态为PAYING")
        void pay_confirmReservationFails_rollsBack() {
            Order order = createOrder(1L, 10L, OrderStatus.CREATED, OrderType.TICKET, 50L);

            when(orderMapper.selectById(1L)).thenReturn(order);
            when(orderMapper.compareAndSetStatus(eq(1L), eq("CREATED"), eq("PAYING"))).thenReturn(1);
            when(orderMapper.compareAndSetStatus(eq(1L), eq("PAYING"), eq("PAID"))).thenReturn(1);
            // Rollback: PAID -> PAYING
            when(orderMapper.compareAndSetStatus(eq(1L), eq("PAID"), eq("PAYING"))).thenReturn(1);
            when(ticketFeignClient.confirmReservation(50L))
                    .thenThrow(new RuntimeException("Service unavailable"));

            doAnswer(invocation -> {
                Payment payment = invocation.getArgument(0);
                payment.setId(1L);
                return 1;
            }).when(paymentMapper).insert(any(Payment.class));

            assertThatThrownBy(() -> orderService.pay(10L, 1L))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(500));

            // Verify rollback
            verify(orderMapper).compareAndSetStatus(1L, "PAID", "PAYING");
        }

        @Test
        @DisplayName("订单不属于当前用户 - 抛出403异常")
        void pay_notOwner_throws403() {
            Order order = createOrder(1L, 99L, OrderStatus.CREATED, OrderType.AUCTION, 100L);
            when(orderMapper.selectById(1L)).thenReturn(order);

            assertThatThrownBy(() -> orderService.pay(10L, 1L))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(403));
        }

        @Test
        @DisplayName("订单不存在 - 抛出404异常")
        void pay_orderNotFound_throws404() {
            when(orderMapper.selectById(999L)).thenReturn(null);

            assertThatThrownBy(() -> orderService.pay(10L, 999L))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(404));
        }

        @Test
        @DisplayName("订单已在支付中 - 返回幂等响应")
        void pay_alreadyPaying_returnsIdempotent() {
            Order order = createOrder(1L, 10L, OrderStatus.PAYING, OrderType.AUCTION, 100L);
            when(orderMapper.selectById(1L)).thenReturn(order);

            PayResponse response = orderService.pay(10L, 1L);

            assertThat(response.getStatus()).isEqualTo("PAYING");
            assertThat(response.getPaymentStatus()).isEqualTo("PENDING");
            assertThat(response.getMessage()).contains("already in progress");
            // No new payment record created
            verify(paymentMapper, never()).insert(any());
        }

        @Test
        @DisplayName("订单已支付完成 - 返回幂等响应")
        void pay_alreadyCompleted_returnsIdempotent() {
            Order order = createOrder(1L, 10L, OrderStatus.COMPLETED, OrderType.AUCTION, 100L);
            when(orderMapper.selectById(1L)).thenReturn(order);

            PayResponse response = orderService.pay(10L, 1L);

            assertThat(response.getStatus()).isEqualTo("COMPLETED");
            assertThat(response.getMessage()).contains("already completed");
        }

        @Test
        @DisplayName("CAS并发冲突 - 抛出409异常")
        void pay_casConflict_throws409() {
            Order order = createOrder(1L, 10L, OrderStatus.CREATED, OrderType.AUCTION, 100L);
            when(orderMapper.selectById(1L)).thenReturn(order);
            when(orderMapper.compareAndSetStatus(1L, "CREATED", "PAYING")).thenReturn(0);

            assertThatThrownBy(() -> orderService.pay(10L, 1L))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(409));
        }

        @Test
        @DisplayName("支付模拟失败 - 订单回退到CREATED")
        void pay_paymentSimulationFails_rollsBackToCreated() {
            ReflectionTestUtils.setField(orderService, "successRate", 0.0); // Always fail

            Order order = createOrder(1L, 10L, OrderStatus.CREATED, OrderType.AUCTION, 100L);
            when(orderMapper.selectById(1L)).thenReturn(order);
            when(orderMapper.compareAndSetStatus(eq(1L), eq("CREATED"), eq("PAYING"))).thenReturn(1);
            when(orderMapper.compareAndSetStatus(eq(1L), eq("PAYING"), eq("CREATED"))).thenReturn(1);

            doAnswer(invocation -> {
                Payment payment = invocation.getArgument(0);
                payment.setId(1L);
                return 1;
            }).when(paymentMapper).insert(any(Payment.class));

            PayResponse response = orderService.pay(10L, 1L);

            assertThat(response.getStatus()).isEqualTo("CREATED");
            assertThat(response.getPaymentStatus()).isEqualTo("FAILED");
            assertThat(response.getMessage()).contains("failed");

            // Order rolled back to CREATED
            verify(orderMapper).compareAndSetStatus(1L, "PAYING", "CREATED");
        }
    }

    // ==================== cancel ====================

    @Nested
    @DisplayName("cancel() 取消订单")
    class CancelTest {

        @Test
        @DisplayName("正常取消票务订单 - 调用Feign取消预订")
        void cancel_ticketOrder_callsCancelReservation() {
            Order order = createOrder(1L, 10L, OrderStatus.CREATED, OrderType.TICKET, 50L);
            when(orderMapper.selectById(1L)).thenReturn(order);
            when(orderMapper.compareAndSetStatus(1L, "CREATED", "CANCELLED")).thenReturn(1);

            orderService.cancel(10L, 1L);

            verify(ticketFeignClient).cancelReservation(50L);
        }

        @Test
        @DisplayName("取消拍卖订单 - 不调用Feign")
        void cancel_auctionOrder_doesNotCallFeign() {
            Order order = createOrder(1L, 10L, OrderStatus.CREATED, OrderType.AUCTION, 100L);
            when(orderMapper.selectById(1L)).thenReturn(order);
            when(orderMapper.compareAndSetStatus(1L, "CREATED", "CANCELLED")).thenReturn(1);

            orderService.cancel(10L, 1L);

            verify(ticketFeignClient, never()).cancelReservation(any());
        }

        @Test
        @DisplayName("取消非CREATED订单 - 抛出409异常")
        void cancel_notCreated_throws409() {
            Order order = createOrder(1L, 10L, OrderStatus.COMPLETED, OrderType.AUCTION, 100L);
            when(orderMapper.selectById(1L)).thenReturn(order);

            assertThatThrownBy(() -> orderService.cancel(10L, 1L))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(409));
        }

        @Test
        @DisplayName("Feign取消失败 - 不影响订单取消(容错)")
        void cancel_feignFailure_doesNotRollbackOrder() {
            Order order = createOrder(1L, 10L, OrderStatus.CREATED, OrderType.TICKET, 50L);
            when(orderMapper.selectById(1L)).thenReturn(order);
            when(orderMapper.compareAndSetStatus(1L, "CREATED", "CANCELLED")).thenReturn(1);
            doThrow(new RuntimeException("Feign error"))
                    .when(ticketFeignClient).cancelReservation(50L);

            // Should NOT throw - Feign failure is swallowed
            orderService.cancel(10L, 1L);

            verify(orderMapper).compareAndSetStatus(1L, "CREATED", "CANCELLED");
        }
    }

    // ==================== getById ====================

    @Nested
    @DisplayName("getById() 查询订单详情")
    class GetByIdTest {

        @Test
        @DisplayName("正常查询")
        void getById_success() {
            Order order = createOrder(1L, 10L, OrderStatus.COMPLETED, OrderType.AUCTION, 100L);
            when(orderMapper.selectById(1L)).thenReturn(order);

            OrderResponse response = orderService.getById(10L, 1L);

            assertThat(response).isNotNull();
            assertThat(response.getId()).isEqualTo(1L);
        }

        @Test
        @DisplayName("订单不存在 - 抛出404")
        void getById_notFound_throws404() {
            when(orderMapper.selectById(999L)).thenReturn(null);

            assertThatThrownBy(() -> orderService.getById(10L, 999L))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(404));
        }

        @Test
        @DisplayName("非订单所有者 - 抛出403")
        void getById_notOwner_throws403() {
            Order order = createOrder(1L, 99L, OrderStatus.COMPLETED, OrderType.AUCTION, 100L);
            when(orderMapper.selectById(1L)).thenReturn(order);

            assertThatThrownBy(() -> orderService.getById(10L, 1L))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(403));
        }
    }

    // ==================== expireOrder ====================

    @Nested
    @DisplayName("expireOrder() 订单超时过期")
    class ExpireOrderTest {

        @Test
        @DisplayName("CREATED订单超时 - 标记为EXPIRED")
        void expireOrder_created_success() {
            Order order = createOrder(1L, 10L, OrderStatus.CREATED, OrderType.TICKET, 50L);
            when(orderMapper.selectById(1L)).thenReturn(order);
            when(orderMapper.compareAndSetStatus(1L, "CREATED", "EXPIRED")).thenReturn(1);

            orderService.expireOrder(1L);

            verify(orderMapper).compareAndSetStatus(1L, "CREATED", "EXPIRED");
            verify(ticketFeignClient).cancelReservation(50L);
        }

        @Test
        @DisplayName("PAYING订单超时 - 标记为EXPIRED")
        void expireOrder_paying_success() {
            Order order = createOrder(1L, 10L, OrderStatus.PAYING, OrderType.AUCTION, 100L);
            when(orderMapper.selectById(1L)).thenReturn(order);
            when(orderMapper.compareAndSetStatus(1L, "PAYING", "EXPIRED")).thenReturn(1);

            orderService.expireOrder(1L);

            verify(orderMapper).compareAndSetStatus(1L, "PAYING", "EXPIRED");
        }

        @Test
        @DisplayName("已完成订单 - 不处理")
        void expireOrder_completed_skips() {
            Order order = createOrder(1L, 10L, OrderStatus.COMPLETED, OrderType.AUCTION, 100L);
            when(orderMapper.selectById(1L)).thenReturn(order);

            orderService.expireOrder(1L);

            verify(orderMapper, never()).compareAndSetStatus(any(), any(), any());
        }

        @Test
        @DisplayName("订单不存在 - 静默返回")
        void expireOrder_notFound_skips() {
            when(orderMapper.selectById(999L)).thenReturn(null);

            orderService.expireOrder(999L);

            verify(orderMapper, never()).compareAndSetStatus(any(), any(), any());
        }

        @Test
        @DisplayName("CAS冲突 - 静默返回")
        void expireOrder_casConflict_skips() {
            Order order = createOrder(1L, 10L, OrderStatus.CREATED, OrderType.AUCTION, 100L);
            when(orderMapper.selectById(1L)).thenReturn(order);
            when(orderMapper.compareAndSetStatus(1L, "CREATED", "EXPIRED")).thenReturn(0);

            orderService.expireOrder(1L);

            verify(ticketFeignClient, never()).cancelReservation(any());
        }

        @Test
        @DisplayName("过期后 - 发布 OrderExpired 事件")
        void expireOrder_publishesExpiredEvent() {
            Order order = createOrder(1L, 10L, OrderStatus.CREATED, OrderType.AUCTION, 100L);
            when(orderMapper.selectById(1L)).thenReturn(order);
            when(orderMapper.compareAndSetStatus(1L, "CREATED", "EXPIRED")).thenReturn(1);

            orderService.expireOrder(1L);
            simulateCommit();

            verify(eventProducer).publishOrderExpired(1L, 10L);
        }
    }

    // ==================== Order State Machine ====================

    @Nested
    @DisplayName("订单状态机验证")
    class OrderStateMachineTest {

        @Test
        @DisplayName("非法状态转换 - COMPLETED不能支付")
        void stateMachine_completedCannotPay() {
            Order order = createOrder(1L, 10L, OrderStatus.COMPLETED, OrderType.AUCTION, 100L);
            when(orderMapper.selectById(1L)).thenReturn(order);

            PayResponse response = orderService.pay(10L, 1L);

            // Should return idempotent response, not proceed with payment
            assertThat(response.getStatus()).isEqualTo("COMPLETED");
            verify(paymentMapper, never()).insert(any());
        }

        @Test
        @DisplayName("非法状态转换 - CANCELLED不能取消")
        void stateMachine_cancelledCannotCancel() {
            Order order = createOrder(1L, 10L, OrderStatus.CANCELLED, OrderType.AUCTION, 100L);
            when(orderMapper.selectById(1L)).thenReturn(order);

            assertThatThrownBy(() -> orderService.cancel(10L, 1L))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(409));
        }

        @Test
        @DisplayName("非法状态转换 - PAID不能取消")
        void stateMachine_paidCannotCancel() {
            Order order = createOrder(1L, 10L, OrderStatus.PAID, OrderType.AUCTION, 100L);
            when(orderMapper.selectById(1L)).thenReturn(order);

            assertThatThrownBy(() -> orderService.cancel(10L, 1L))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(409));
        }

        @Test
        @DisplayName("非法状态转换 - EXPIRED不能取消")
        void stateMachine_expiredCannotCancel() {
            Order order = createOrder(1L, 10L, OrderStatus.EXPIRED, OrderType.AUCTION, 100L);
            when(orderMapper.selectById(1L)).thenReturn(order);

            assertThatThrownBy(() -> orderService.cancel(10L, 1L))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(409));
        }
    }
}

package com.auction.ticket.service;

import com.auction.ticket.config.RabbitMQConfig;
import com.auction.ticket.controller.dto.CreateTicketRequest;
import com.auction.ticket.controller.dto.ReserveRequest;
import com.auction.ticket.controller.dto.ReserveResponse;
import com.auction.ticket.controller.dto.TicketStockResponse;
import com.auction.ticket.domain.entity.Reservation;
import com.auction.ticket.domain.entity.TicketStock;
import com.auction.ticket.domain.enums.ReservationStatus;
import com.auction.ticket.event.TicketEventProducer;
import com.auction.ticket.repository.ReservationMapper;
import com.auction.ticket.repository.TicketStockMapper;
import com.auction.ticket.service.impl.TicketStockServiceImpl;
import com.auction.common.exception.BusinessException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("TicketStockService - 票务防超卖核心逻辑")
class TicketStockServiceImplTest {

    @Mock private StringRedisTemplate redis;
    @Mock private DefaultRedisScript<List> reserveTicketScript;
    @Mock private DefaultRedisScript<List> releaseTicketScript;
    @Mock private TicketStockMapper stockMapper;
    @Mock private ReservationMapper reservationMapper;
    @Mock private TicketEventProducer eventProducer;
    @Mock private RabbitTemplate rabbitTemplate;

    private TicketStockServiceImpl ticketStockService;

    @BeforeEach
    void setUp() {
        // Construct manually to avoid @InjectMocks ambiguity with two DefaultRedisScript mocks
        ticketStockService = new TicketStockServiceImpl(
                redis, reserveTicketScript, releaseTicketScript,
                stockMapper, reservationMapper, eventProducer, rabbitTemplate);

        // Initialize transaction synchronization so registerSynchronization works
        TransactionSynchronizationManager.initSynchronization();
        ticketStockService.markReady();
    }

    @AfterEach
    void tearDown() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    /**
     * Simulate transaction commit - triggers all afterCommit callbacks
     */
    private void simulateCommit() {
        List<TransactionSynchronization> syncs = TransactionSynchronizationManager.getSynchronizations();
        for (TransactionSynchronization sync : syncs) {
            sync.afterCommit();
        }
    }

    // ==================== reserve ====================

    @Nested
    @DisplayName("reserve() 抢票预留库存")
    class ReserveTest {

        @Test
        @DisplayName("正常抢票 - Redis扣减成功，MySQL创建预订")
        void reserve_success() {
            ReserveRequest request = new ReserveRequest();
            request.setEventId(100L);
            request.setTicketType("VIP");
            request.setQuantity(2);

            // Lua returns {1, "OK"}
            when(redis.execute(eq(reserveTicketScript), anyList(), any(Object[].class)))
                    .thenReturn(Arrays.asList(1L, "OK"));

            TicketStock stock = new TicketStock();
            stock.setId(1L);
            stock.setEventId(100L);
            stock.setTicketType("VIP");
            stock.setTotalQuantity(100);
            stock.setReservedQuantity(0);
            stock.setSoldQuantity(0);
            when(stockMapper.findByEventIdAndTicketType(100L, "VIP")).thenReturn(stock);

            doAnswer(invocation -> {
                Reservation r = invocation.getArgument(0);
                r.setId(42L);
                return 1;
            }).when(reservationMapper).insert(any(Reservation.class));

            ReserveResponse response = ticketStockService.reserve(1L, request);

            assertThat(response).isNotNull();
            assertThat(response.getReservationId()).isEqualTo(42L);
            assertThat(response.getEventId()).isEqualTo(100L);
            assertThat(response.getTicketType()).isEqualTo("VIP");
            assertThat(response.getQuantity()).isEqualTo(2);

            verify(stockMapper).incrementReserved(1L, 2);
        }

        @Test
        @DisplayName("库存不足 - 抛出400异常，不创建预订")
        void reserve_outOfStock_throws400() {
            ReserveRequest request = new ReserveRequest();
            request.setEventId(100L);
            request.setTicketType("VIP");
            request.setQuantity(2);

            when(redis.execute(eq(reserveTicketScript), anyList(), any(Object[].class)))
                    .thenReturn(Arrays.asList(-1L, "OUT_OF_STOCK"));

            assertThatThrownBy(() -> ticketStockService.reserve(1L, request))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(400));

            verify(reservationMapper, never()).insert(any());
            verify(stockMapper, never()).incrementReserved(anyLong(), anyInt());
        }

        @Test
        @DisplayName("库存不存在 - 抛出404异常")
        void reserve_stockNotFound_throws404() {
            ReserveRequest request = new ReserveRequest();
            request.setEventId(999L);
            request.setTicketType("GA");
            request.setQuantity(1);

            when(redis.execute(eq(reserveTicketScript), anyList(), any(Object[].class)))
                    .thenReturn(Arrays.asList(-1L, "STOCK_NOT_FOUND"));

            assertThatThrownBy(() -> ticketStockService.reserve(1L, request))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(404));
        }

        @Test
        @DisplayName("服务初始化中 - 抛出503异常")
        void reserve_notReady_throws503() {
            // Create a fresh service that hasn't been marked ready
            TicketStockServiceImpl unreadyService = new TicketStockServiceImpl(
                    redis, reserveTicketScript, releaseTicketScript,
                    stockMapper, reservationMapper, eventProducer, rabbitTemplate);

            ReserveRequest request = new ReserveRequest();
            request.setEventId(100L);
            request.setTicketType("VIP");
            request.setQuantity(1);

            assertThatThrownBy(() -> unreadyService.reserve(1L, request))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(503));
        }

        @Test
        @DisplayName("Redis执行异常 - 抛出500异常")
        void reserve_redisException_throws500() {
            ReserveRequest request = new ReserveRequest();
            request.setEventId(100L);
            request.setTicketType("VIP");
            request.setQuantity(1);

            when(redis.execute(eq(reserveTicketScript), anyList(), any(Object[].class)))
                    .thenThrow(new RuntimeException("Redis connection refused"));

            assertThatThrownBy(() -> ticketStockService.reserve(1L, request))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(500));
        }

        @Test
        @DisplayName("提交后 - 发布 StockReserved 事件和延迟消息")
        void reserve_afterCommit_publishesEvents() {
            ReserveRequest request = new ReserveRequest();
            request.setEventId(100L);
            request.setTicketType("VIP");
            request.setQuantity(2);

            when(redis.execute(eq(reserveTicketScript), anyList(), any(Object[].class)))
                    .thenReturn(Arrays.asList(1L, "OK"));

            TicketStock stock = new TicketStock(1L, 100L, "VIP", 100, 0, 0, 0, null, null);
            when(stockMapper.findByEventIdAndTicketType(100L, "VIP")).thenReturn(stock);

            doAnswer(invocation -> {
                Reservation r = invocation.getArgument(0);
                r.setId(42L);
                return 1;
            }).when(reservationMapper).insert(any(Reservation.class));

            ticketStockService.reserve(1L, request);

            simulateCommit();

            verify(eventProducer).publishStockReserved(100L, "VIP", 42L, 1L, 2);
            verify(rabbitTemplate).convertAndSend(
                    eq(RabbitMQConfig.DELAY_EXCHANGE),
                    eq(RabbitMQConfig.STOCK_RELEASE_ROUTING_KEY),
                    eq(42L),
                    any(org.springframework.amqp.core.MessagePostProcessor.class));
        }
    }

    // ==================== confirm ====================

    @Nested
    @DisplayName("confirm() 确认预订")
    class ConfirmTest {

        @Test
        @DisplayName("正常确认预订 - 状态从 PENDING 变为 CONFIRMED")
        void confirm_success() {
            Reservation reservation = new Reservation();
            reservation.setId(1L);
            reservation.setStockId(10L);
            reservation.setUserId(1L);
            reservation.setQuantity(2);
            reservation.setStatus(ReservationStatus.PENDING);

            when(reservationMapper.findByIdAndUserId(1L, 1L)).thenReturn(reservation);
            when(reservationMapper.updateStatusIfPending(1L, "CONFIRMED")).thenReturn(1);

            ticketStockService.confirm(1L, 1L);

            verify(reservationMapper).updateStatusIfPending(1L, "CONFIRMED");
            verify(stockMapper).decrementReservedAndIncrementSold(10L, 2);
        }

        @Test
        @DisplayName("确认不存在的预订 - 抛出404异常")
        void confirm_notFound_throws404() {
            when(reservationMapper.findByIdAndUserId(999L, 1L)).thenReturn(null);

            assertThatThrownBy(() -> ticketStockService.confirm(1L, 999L))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(404));
        }

        @Test
        @DisplayName("确认已处理的预订 - 抛出400异常")
        void confirm_alreadyProcessed_throws400() {
            Reservation reservation = new Reservation();
            reservation.setId(1L);
            reservation.setStatus(ReservationStatus.CONFIRMED);

            when(reservationMapper.findByIdAndUserId(1L, 1L)).thenReturn(reservation);

            assertThatThrownBy(() -> ticketStockService.confirm(1L, 1L))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(400));
        }
    }

    // ==================== cancel ====================

    @Nested
    @DisplayName("cancel() 取消预订")
    class CancelTest {

        @Test
        @DisplayName("正常取消预订 - MySQL和Redis都释放库存")
        void cancel_success() {
            Reservation reservation = new Reservation();
            reservation.setId(1L);
            reservation.setStockId(10L);
            reservation.setUserId(1L);
            reservation.setQuantity(2);
            reservation.setStatus(ReservationStatus.PENDING);

            TicketStock stock = new TicketStock(10L, 100L, "VIP", 100, 5, 0, 0, null, null);

            when(reservationMapper.findByIdAndUserId(1L, 1L)).thenReturn(reservation);
            when(stockMapper.selectById(10L)).thenReturn(stock);
            when(reservationMapper.updateStatusIfPending(1L, "CANCELLED")).thenReturn(1);

            ticketStockService.cancel(1L, 1L);

            verify(reservationMapper).updateStatusIfPending(1L, "CANCELLED");
            verify(stockMapper).decrementReserved(10L, 2);

            // Simulate commit to trigger Redis rollback
            simulateCommit();

            verify(redis).execute(eq(releaseTicketScript), anyList(), eq("2"));
        }

        @Test
        @DisplayName("取消不存在的预订 - 抛出404异常")
        void cancel_notFound_throws404() {
            when(reservationMapper.findByIdAndUserId(999L, 1L)).thenReturn(null);

            assertThatThrownBy(() -> ticketStockService.cancel(1L, 999L))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(404));
        }
    }

    // ==================== createTicketStock ====================

    @Nested
    @DisplayName("createTicketStock() 创建票务库存")
    class CreateTicketStockTest {

        @Test
        @DisplayName("正常创建票务 - MySQL和Redis都初始化")
        void createTicketStock_success() {
            CreateTicketRequest request = new CreateTicketRequest();
            request.setEventId(100L);
            request.setTicketType("VIP");
            request.setTotalQuantity(500);

            doAnswer(invocation -> {
                TicketStock stock = invocation.getArgument(0);
                stock.setId(1L);
                return 1;
            }).when(stockMapper).insert(any(TicketStock.class));

            // Stub redis.opsForValue() to return a mock so set() doesn't NPE
            @SuppressWarnings("unchecked")
            ValueOperations<String, String> vo = mock(ValueOperations.class);
            doReturn(vo).when(redis).opsForValue();

            TicketStockResponse response = ticketStockService.createTicketStock(request);

            assertThat(response).isNotNull();
            assertThat(response.getEventId()).isEqualTo(100L);
            assertThat(response.getTicketType()).isEqualTo("VIP");
            assertThat(response.getTotalQuantity()).isEqualTo(500);
            assertThat(response.getAvailableQuantity()).isEqualTo(500);
            assertThat(response.getReservedQuantity()).isEqualTo(0);
            assertThat(response.getSoldQuantity()).isEqualTo(0);

            verify(vo).set("stock:100:VIP", "500");

            // Trigger commit
            simulateCommit();

            verify(eventProducer).publishTicketCreated(100L, "VIP", 500);
        }

        @Test
        @DisplayName("创建票务 - 可用数量正确计算")
        void createTicketStock_availableCalculation() {
            CreateTicketRequest request = new CreateTicketRequest();
            request.setEventId(100L);
            request.setTicketType("GA");
            request.setTotalQuantity(1000);

            doAnswer(invocation -> {
                TicketStock stock = invocation.getArgument(0);
                stock.setId(1L);
                stock.setReservedQuantity(0);
                stock.setSoldQuantity(0);
                return 1;
            }).when(stockMapper).insert(any(TicketStock.class));

            @SuppressWarnings("unchecked")
            ValueOperations<String, String> vo = mock(ValueOperations.class);
            doReturn(vo).when(redis).opsForValue();

            TicketStockResponse response = ticketStockService.createTicketStock(request);

            // available = total - reserved - sold
            assertThat(response.getAvailableQuantity()).isEqualTo(1000);
        }
    }

    // ==================== getStockByEvent ====================

    @Nested
    @DisplayName("getStockByEvent() 查询活动库存")
    class GetStockByEventTest {

        @Test
        @DisplayName("正常查询 - 返回所有票种库存")
        void getStockByEvent_success() {
            TicketStock stock1 = new TicketStock(1L, 100L, "VIP", 100, 30, 20, 0, null, null);
            TicketStock stock2 = new TicketStock(2L, 100L, "GA", 500, 100, 50, 0, null, null);

            when(stockMapper.findByEventId(100L)).thenReturn(Arrays.asList(stock1, stock2));

            List<TicketStockResponse> result = ticketStockService.getStockByEvent(100L);

            assertThat(result).hasSize(2);
            // VIP: 100 - 30 - 20 = 50
            assertThat(result.get(0).getAvailableQuantity()).isEqualTo(50);
            // GA: 500 - 100 - 50 = 350
            assertThat(result.get(1).getAvailableQuantity()).isEqualTo(350);
        }

        @Test
        @DisplayName("无库存数据 - 返回空列表")
        void getStockByEvent_empty() {
            when(stockMapper.findByEventId(999L)).thenReturn(List.of());

            List<TicketStockResponse> result = ticketStockService.getStockByEvent(999L);

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("可用数量为负数时 - 返回0而不是负数")
        void getStockByEvent_negativeAvailableClamped() {
            TicketStock stock = new TicketStock(1L, 100L, "VIP", 100, 60, 50, 0, null, null);
            // available = 100 - 60 - 50 = -10 -> should be 0

            when(stockMapper.findByEventId(100L)).thenReturn(List.of(stock));

            List<TicketStockResponse> result = ticketStockService.getStockByEvent(100L);

            assertThat(result.get(0).getAvailableQuantity()).isEqualTo(0);
        }
    }

    // ==================== settleReserve ====================

    @Nested
    @DisplayName("settleReserve() 结算预留(拍卖赢家用)")
    class SettleReserveTest {

        @Test
        @DisplayName("正常结算预留 - 有库存时成功")
        void settleReserve_success() {
            TicketStock stock = new TicketStock(1L, 100L, "VIP", 100, 10, 0, 0, null, null);

            when(stockMapper.selectById(1L)).thenReturn(stock);

            doAnswer(invocation -> {
                Reservation r = invocation.getArgument(0);
                r.setId(77L);
                return 1;
            }).when(reservationMapper).insert(any(Reservation.class));

            Long reservationId = ticketStockService.settleReserve(1L, 5L, 1);

            assertThat(reservationId).isEqualTo(77L);
            verify(stockMapper).incrementReserved(1L, 1);
        }

        @Test
        @DisplayName("库存不足 - 抛出400异常")
        void settleReserve_outOfStock_throws400() {
            TicketStock stock = new TicketStock(1L, 100L, "VIP", 100, 95, 5, 0, null, null);
            // available = 100 - 95 - 5 = 0

            when(stockMapper.selectById(1L)).thenReturn(stock);

            assertThatThrownBy(() -> ticketStockService.settleReserve(1L, 5L, 1))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(400));
        }

        @Test
        @DisplayName("票务不存在 - 抛出404异常")
        void settleReserve_notFound_throws404() {
            when(stockMapper.selectById(999L)).thenReturn(null);

            assertThatThrownBy(() -> ticketStockService.settleReserve(999L, 5L, 1))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(404));
        }
    }
}

package com.auction.ticket.it;

import com.auction.common.exception.BusinessException;
import com.auction.ticket.controller.dto.CreateTicketRequest;
import com.auction.ticket.controller.dto.ReserveRequest;
import com.auction.ticket.domain.entity.Reservation;
import com.auction.ticket.domain.entity.TicketStock;
import com.auction.ticket.repository.ReservationMapper;
import com.auction.ticket.repository.TicketStockMapper;
import com.auction.ticket.service.RedisKeys;
import com.auction.ticket.service.impl.TicketStockServiceImpl;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The k6 zero-oversell result, reproduced as a deterministic `mvn verify` test:
 * N concurrent reserve() calls against real MySQL + real Redis must sell exactly
 * the available stock, reject the rest, and leave Redis and MySQL convergent.
 */
class ConcurrentReserveIT extends AbstractTicketServiceIT {

    private static final long EVENT_ID = 910001L;
    private static final String TICKET_TYPE = "VIP";
    private static final int TOTAL_STOCK = 100;
    private static final int ATTEMPTS = 200;
    private static final int THREADS = 32;

    @Autowired
    private TicketStockServiceImpl ticketStockService;
    @Autowired
    private TicketStockMapper stockMapper;
    @Autowired
    private ReservationMapper reservationMapper;
    @Autowired
    private StringRedisTemplate redis;

    @Test
    void concurrentReserve_neverOversells_andRedisMatchesMySql() throws Exception {
        CreateTicketRequest create = new CreateTicketRequest();
        create.setEventId(EVENT_ID);
        create.setTicketType(TICKET_TYPE);
        create.setTotalQuantity(TOTAL_STOCK);
        ticketStockService.createTicketStock(create);

        AtomicInteger succeeded = new AtomicInteger();
        AtomicInteger soldOut = new AtomicInteger();
        AtomicInteger unexpected = new AtomicInteger();

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(ATTEMPTS);
        for (int i = 0; i < ATTEMPTS; i++) {
            final long userId = 1000L + i;
            pool.submit(() -> {
                try {
                    startGate.await();
                    ReserveRequest request = new ReserveRequest();
                    request.setEventId(EVENT_ID);
                    request.setTicketType(TICKET_TYPE);
                    request.setQuantity(1);
                    ticketStockService.reserve(userId, request);
                    succeeded.incrementAndGet();
                } catch (BusinessException e) {
                    if (e.getCode() == 400) {
                        soldOut.incrementAndGet();
                    } else {
                        unexpected.incrementAndGet();
                    }
                } catch (Exception e) {
                    unexpected.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }
        startGate.countDown();
        assertThat(done.await(120, TimeUnit.SECONDS)).as("all reserve attempts finished").isTrue();
        pool.shutdownNow();

        assertThat(unexpected.get()).as("no unexpected errors").isZero();
        assertThat(succeeded.get()).as("exactly the available stock was sold").isEqualTo(TOTAL_STOCK);
        assertThat(soldOut.get()).isEqualTo(ATTEMPTS - TOTAL_STOCK);

        // MySQL: zero oversell.
        TicketStock stock = stockMapper.findByEventIdAndTicketType(EVENT_ID, TICKET_TYPE);
        assertThat(stock.getReservedQuantity()).isEqualTo(TOTAL_STOCK);
        assertThat(stock.getSoldQuantity()).isZero();
        List<Reservation> reservations = reservationMapper.selectList(
                new QueryWrapper<Reservation>().eq("stock_id", stock.getId()));
        assertThat(reservations).hasSize(TOTAL_STOCK);

        // Redis and MySQL converge: remaining in Redis == available in MySQL == 0.
        String redisRemaining = redis.opsForValue().get(RedisKeys.stock(EVENT_ID, TICKET_TYPE));
        int mysqlAvailable = stock.getTotalQuantity() - stock.getReservedQuantity() - stock.getSoldQuantity();
        assertThat(redisRemaining).isEqualTo(String.valueOf(mysqlAvailable));
        assertThat(mysqlAvailable).isZero();
    }
}

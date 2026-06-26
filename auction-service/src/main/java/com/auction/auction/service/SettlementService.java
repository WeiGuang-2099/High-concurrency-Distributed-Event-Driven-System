package com.auction.auction.service;

import com.auction.auction.client.OrderFeignClient;
import com.auction.auction.client.TicketFeignClient;
import com.auction.common.dto.ApiResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * Orchestrates auction settlement as a compensation-based saga rather than a 2PC
 * distributed transaction. Settlement is a low-frequency path (the scheduler settles
 * each ended auction once), so the throughput cost and TC single-point dependency of
 * Seata AT are not justified here.
 *
 * <p>The saga has two forward steps: reserve the winner's stock, then create the order.
 * If order creation fails after the stock reservation succeeded, the reservation is
 * released by a compensating call. Both the participant steps and the compensation are
 * idempotent, so the scheduler can safely revert the auction to ACTIVE and retry the
 * whole saga on a later tick.
 */
@Service
public class SettlementService {

    private static final Logger log = LoggerFactory.getLogger(SettlementService.class);

    private final TicketFeignClient ticketFeignClient;
    private final OrderFeignClient orderFeignClient;

    public SettlementService(TicketFeignClient ticketFeignClient,
                             OrderFeignClient orderFeignClient) {
        this.ticketFeignClient = ticketFeignClient;
        this.orderFeignClient = orderFeignClient;
    }

    public void settle(Long auctionId, Long winnerId, BigDecimal amount, Long ticketTypeId) {
        log.info("Starting settlement saga: auction={}, winner={}, amount={}",
                auctionId, winnerId, amount);

        // Step 1: reserve the winner's stock. Nothing to compensate if this fails.
        Long reservationId = reserveStock(auctionId, winnerId, ticketTypeId);

        // Step 2: create the order. If it fails, compensate step 1 before propagating.
        try {
            createOrder(auctionId, winnerId, amount);
        } catch (RuntimeException e) {
            compensateReservation(auctionId, reservationId);
            throw e;
        }

        log.info("Settlement saga completed: auction={}, reservationId={}", auctionId, reservationId);
    }

    private Long reserveStock(Long auctionId, Long winnerId, Long ticketTypeId) {
        Map<String, Object> reserveBody = new HashMap<>();
        reserveBody.put("auctionId", auctionId);
        reserveBody.put("winnerId", winnerId);
        reserveBody.put("ticketTypeId", ticketTypeId);
        reserveBody.put("quantity", 1);

        ApiResponse<Map<String, Object>> reserveResult = ticketFeignClient.settleReserve(reserveBody);
        if (reserveResult == null || reserveResult.getCode() != 200 || reserveResult.getData() == null) {
            throw new RuntimeException("Stock reservation failed: " +
                    (reserveResult != null ? reserveResult.getMessage() : "null response"));
        }

        Object id = reserveResult.getData().get("reservationId");
        if (id == null) {
            throw new RuntimeException("Stock reservation returned no reservationId");
        }
        return Long.parseLong(id.toString());
    }

    private void createOrder(Long auctionId, Long winnerId, BigDecimal amount) {
        Map<String, Object> orderBody = new HashMap<>();
        orderBody.put("auctionId", auctionId);
        orderBody.put("winnerId", winnerId);
        orderBody.put("amount", amount);

        ApiResponse<Map<String, Object>> orderResult = orderFeignClient.createAuctionOrder(orderBody);
        if (orderResult == null || orderResult.getCode() != 200) {
            throw new RuntimeException("Order creation failed: " +
                    (orderResult != null ? orderResult.getMessage() : "null response"));
        }
    }

    /**
     * Best-effort compensation: release the stock reserved in step 1. The release is
     * idempotent on the ticket side, so a failure here is logged (not retried inline)
     * and the next settlement attempt — or a reconciliation job — can release it safely.
     */
    private void compensateReservation(Long auctionId, Long reservationId) {
        try {
            ticketFeignClient.settleRelease(Map.of("reservationId", reservationId));
            log.warn("Compensated stock reservation after settlement failure: auction={}, reservationId={}",
                    auctionId, reservationId);
        } catch (Exception ce) {
            log.error("Compensation FAILED; stock reservation may be leaked: auction={}, reservationId={}",
                    auctionId, reservationId, ce);
        }
    }
}

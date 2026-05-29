package com.auction.auction.service;

import com.auction.auction.client.OrderFeignClient;
import com.auction.auction.client.TicketFeignClient;
import com.auction.common.dto.ApiResponse;
import io.seata.spring.annotation.GlobalTransactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

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

    @GlobalTransactional(name = "auction-settlement", rollbackFor = Exception.class)
    public void settle(Long auctionId, Long winnerId, BigDecimal amount, Long ticketTypeId) {
        log.info("Starting Seata settlement TX: auction={}, winner={}, amount={}",
                auctionId, winnerId, amount);

        Map<String, Object> reserveBody = new HashMap<>();
        reserveBody.put("auctionId", auctionId);
        reserveBody.put("winnerId", winnerId);
        reserveBody.put("ticketTypeId", ticketTypeId);
        reserveBody.put("quantity", 1);
        ApiResponse<Map<String, Object>> reserveResult =
                ticketFeignClient.settleReserve(reserveBody);
        if (reserveResult == null || reserveResult.getCode() != 200) {
            throw new RuntimeException("Stock reservation failed: " +
                    (reserveResult != null ? reserveResult.getMessage() : "null response"));
        }

        Map<String, Object> orderBody = new HashMap<>();
        orderBody.put("auctionId", auctionId);
        orderBody.put("winnerId", winnerId);
        orderBody.put("amount", amount);
        ApiResponse<Map<String, Object>> orderResult =
                orderFeignClient.createAuctionOrder(orderBody);
        if (orderResult == null || orderResult.getCode() != 200) {
            throw new RuntimeException("Order creation failed: " +
                    (orderResult != null ? orderResult.getMessage() : "null response"));
        }

        log.info("Seata settlement TX completed: auction={}", auctionId);
    }
}

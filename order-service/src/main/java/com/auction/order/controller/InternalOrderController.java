package com.auction.order.controller;

import com.auction.common.dto.ApiResponse;
import com.auction.order.controller.dto.OrderResponse;
import com.auction.order.service.OrderService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.Map;

@RestController
@RequestMapping("/api/orders/internal")
public class InternalOrderController {

    private final OrderService orderService;

    public InternalOrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping("/auction")
    public ResponseEntity<ApiResponse<OrderResponse>> createAuctionOrder(
            @RequestBody Map<String, Object> body) {
        Long auctionId = Long.parseLong(body.get("auctionId").toString());
        Long winnerId = Long.parseLong(body.get("winnerId").toString());
        BigDecimal amount = new BigDecimal(body.get("amount").toString());

        OrderResponse response = orderService.createFromAuction(auctionId, winnerId, amount);
        return ResponseEntity.ok(ApiResponse.success(response));
    }
}

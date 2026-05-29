package com.auction.order.controller;

import com.auction.common.dto.ApiResponse;
import com.auction.common.security.UserContext;
import com.auction.common.security.UserContextHolder;
import com.auction.order.controller.dto.CreateOrderRequest;
import com.auction.order.controller.dto.OrderResponse;
import com.auction.order.controller.dto.PayResponse;
import com.auction.order.service.OrderService;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<ApiResponse<OrderResponse>> createOrder(
            @Valid @RequestBody CreateOrderRequest request) {
        UserContext ctx = UserContextHolder.get();
        OrderResponse response = orderService.createFromTicket(ctx.getUserId(), request);
        return ResponseEntity.ok(ApiResponse.success(response));
    }

    @GetMapping("/{id}")
    public ResponseEntity<ApiResponse<OrderResponse>> getOrder(@PathVariable Long id) {
        UserContext ctx = UserContextHolder.get();
        OrderResponse response = orderService.getById(ctx.getUserId(), id);
        return ResponseEntity.ok(ApiResponse.success(response));
    }

    @GetMapping
    public ResponseEntity<ApiResponse<Page<OrderResponse>>> listOrders(
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "10") int size) {
        UserContext ctx = UserContextHolder.get();
        Page<OrderResponse> result = orderService.listByUserId(ctx.getUserId(), page, size);
        return ResponseEntity.ok(ApiResponse.success(result));
    }

    @PostMapping("/{id}/pay")
    public ResponseEntity<ApiResponse<PayResponse>> pay(@PathVariable Long id) {
        UserContext ctx = UserContextHolder.get();
        PayResponse response = orderService.pay(ctx.getUserId(), id);
        return ResponseEntity.ok(ApiResponse.success(response));
    }

    @PostMapping("/{id}/cancel")
    public ResponseEntity<ApiResponse<Void>> cancel(@PathVariable Long id) {
        UserContext ctx = UserContextHolder.get();
        orderService.cancel(ctx.getUserId(), id);
        return ResponseEntity.ok(ApiResponse.success(null));
    }
}

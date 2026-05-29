package com.auction.order.service;

import com.auction.order.controller.dto.CreateOrderRequest;
import com.auction.order.controller.dto.OrderResponse;
import com.auction.order.controller.dto.PayResponse;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;

public interface OrderService {

    OrderResponse createFromTicket(Long userId, CreateOrderRequest request);

    OrderResponse createFromAuction(Long auctionId, Long winnerId,
                                    java.math.BigDecimal amount);

    PayResponse pay(Long userId, Long orderId);

    void cancel(Long userId, Long orderId);

    OrderResponse getById(Long userId, Long orderId);

    Page<OrderResponse> listByUserId(Long userId, int page, int size);

    void expireOrder(Long orderId);
}

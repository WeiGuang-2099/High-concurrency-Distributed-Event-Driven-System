package com.auction.auction.client;

import com.auction.common.dto.ApiResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.Map;

@FeignClient(name = "order-service", path = "/api/orders")
public interface OrderFeignClient {

    @PostMapping("/internal/auction")
    ApiResponse<Map<String, Object>> createAuctionOrder(@RequestBody Map<String, Object> body);
}

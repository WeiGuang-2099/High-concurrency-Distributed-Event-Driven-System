package com.auction.auction.client;

import com.auction.common.dto.ApiResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.Map;

@FeignClient(name = "ticket-service", path = "/api/tickets")
public interface TicketFeignClient {

    @PostMapping("/internal/settle-reserve")
    ApiResponse<Map<String, Object>> settleReserve(@RequestBody Map<String, Object> body);
}

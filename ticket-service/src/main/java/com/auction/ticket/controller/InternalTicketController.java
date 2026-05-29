package com.auction.ticket.controller;

import com.auction.common.dto.ApiResponse;
import com.auction.ticket.service.TicketStockService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/tickets/internal")
public class InternalTicketController {

    private final TicketStockService ticketStockService;

    public InternalTicketController(TicketStockService ticketStockService) {
        this.ticketStockService = ticketStockService;
    }

    @PostMapping("/settle-reserve")
    public ResponseEntity<ApiResponse<Map<String, Object>>> settleReserve(
            @RequestBody Map<String, Object> body) {
        Long ticketTypeId = Long.parseLong(body.get("ticketTypeId").toString());
        Long winnerId = Long.parseLong(body.get("winnerId").toString());
        int quantity = Integer.parseInt(body.get("quantity").toString());

        Long reservationId = ticketStockService.settleReserve(ticketTypeId, winnerId, quantity);

        Map<String, Object> result = new HashMap<>();
        result.put("reservationId", reservationId);
        return ResponseEntity.ok(ApiResponse.success(result));
    }
}

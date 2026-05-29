package com.auction.ticket.controller;

import com.auction.common.dto.ApiResponse;
import com.auction.common.security.UserContext;
import com.auction.ticket.controller.dto.CreateTicketRequest;
import com.auction.ticket.controller.dto.TicketStockResponse;
import com.auction.common.security.UserContextHolder;
import com.auction.ticket.service.TicketStockService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/admin/tickets")
public class AdminTicketController {

    private final TicketStockService ticketStockService;

    public AdminTicketController(TicketStockService ticketStockService) {
        this.ticketStockService = ticketStockService;
    }

    @PostMapping
    public ResponseEntity<ApiResponse<TicketStockResponse>> createTicketStock(
            @Valid @RequestBody CreateTicketRequest request) {
        UserContext ctx = UserContextHolder.get();
        if (!ctx.isAdmin()) {
            return ResponseEntity.status(403)
                    .body(ApiResponse.error(403, "Admin role required"));
        }
        TicketStockResponse response = ticketStockService.createTicketStock(request);
        return ResponseEntity.ok(ApiResponse.success(response));
    }
}

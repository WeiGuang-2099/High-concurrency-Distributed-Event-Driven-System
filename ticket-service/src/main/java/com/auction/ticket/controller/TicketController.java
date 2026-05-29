package com.auction.ticket.controller;

import com.auction.common.dto.ApiResponse;
import com.auction.common.security.UserContext;
import com.auction.ticket.controller.dto.ReserveRequest;
import com.auction.ticket.controller.dto.ReserveResponse;
import com.auction.ticket.controller.dto.TicketStockResponse;
import com.auction.common.security.UserContextHolder;
import com.auction.ticket.service.TicketStockService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/tickets")
public class TicketController {

    private final TicketStockService ticketStockService;

    public TicketController(TicketStockService ticketStockService) {
        this.ticketStockService = ticketStockService;
    }

    @GetMapping("/events/{eventId}")
    public ResponseEntity<ApiResponse<List<TicketStockResponse>>> getStockByEvent(@PathVariable Long eventId) {
        return ResponseEntity.ok(ApiResponse.success(ticketStockService.getStockByEvent(eventId)));
    }

    @PostMapping("/reserve")
    public ResponseEntity<ApiResponse<ReserveResponse>> reserve(@Valid @RequestBody ReserveRequest request) {
        UserContext ctx = UserContextHolder.get();
        ReserveResponse response = ticketStockService.reserve(ctx.getUserId(), request);
        return ResponseEntity.ok(ApiResponse.success(response));
    }

    @PostMapping("/{reservationId}/confirm")
    public ResponseEntity<ApiResponse<Void>> confirm(@PathVariable Long reservationId) {
        UserContext ctx = UserContextHolder.get();
        ticketStockService.confirm(ctx.getUserId(), reservationId);
        return ResponseEntity.ok(ApiResponse.success(null));
    }

    @DeleteMapping("/{reservationId}")
    public ResponseEntity<ApiResponse<Void>> cancel(@PathVariable Long reservationId) {
        UserContext ctx = UserContextHolder.get();
        ticketStockService.cancel(ctx.getUserId(), reservationId);
        return ResponseEntity.ok(ApiResponse.success(null));
    }
}

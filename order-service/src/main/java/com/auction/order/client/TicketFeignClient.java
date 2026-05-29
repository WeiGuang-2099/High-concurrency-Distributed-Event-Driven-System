package com.auction.order.client;

import com.auction.common.dto.ApiResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;

@FeignClient(name = "ticket-service", path = "/api/tickets")
public interface TicketFeignClient {

    @PostMapping("/{reservationId}/confirm")
    ApiResponse<Void> confirmReservation(@PathVariable("reservationId") Long reservationId);

    @DeleteMapping("/{reservationId}")
    ApiResponse<Void> cancelReservation(@PathVariable("reservationId") Long reservationId);
}

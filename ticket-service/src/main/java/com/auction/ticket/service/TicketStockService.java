package com.auction.ticket.service;

import com.auction.ticket.controller.dto.CreateTicketRequest;
import com.auction.ticket.controller.dto.ReserveRequest;
import com.auction.ticket.controller.dto.ReserveResponse;
import com.auction.ticket.controller.dto.TicketStockResponse;

import java.util.List;

public interface TicketStockService {

    List<TicketStockResponse> getStockByEvent(Long eventId);

    ReserveResponse reserve(Long userId, ReserveRequest request);

    void confirm(Long userId, Long reservationId);

    void cancel(Long userId, Long reservationId);

    TicketStockResponse createTicketStock(CreateTicketRequest request);

    Long settleReserve(Long ticketTypeId, Long userId, int quantity);
}

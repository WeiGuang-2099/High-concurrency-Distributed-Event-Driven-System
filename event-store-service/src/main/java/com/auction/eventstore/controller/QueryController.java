package com.auction.eventstore.controller;

import com.auction.common.dto.ApiResponse;
import com.auction.eventstore.domain.BidHistoryEntry;
import com.auction.eventstore.domain.OrderTimelineEntry;
import com.auction.eventstore.domain.StockMovementEntry;
import com.auction.eventstore.repository.BidHistoryRepository;
import com.auction.eventstore.repository.OrderTimelineRepository;
import com.auction.eventstore.repository.StockMovementRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * CQRS query API: reads exclusively from materialized projections in MongoDB.
 * Per PRD US-006, these endpoints never touch the write-side event stream.
 */
@RestController
@RequestMapping("/api/event-store")
@RequiredArgsConstructor
public class QueryController {

    private final BidHistoryRepository bidHistoryRepository;
    private final OrderTimelineRepository orderTimelineRepository;
    private final StockMovementRepository stockMovementRepository;

    /**
     * Paginated bid history for an auction.
     */
    @GetMapping("/auctions/{id}/history")
    public ApiResponse<Page<BidHistoryEntry>> bidHistory(
            @PathVariable("id") Long auctionId,
            @RequestParam(value = "page", defaultValue = "0") int page,
            @RequestParam(value = "size", defaultValue = "20") int size) {
        PageRequest pageable = PageRequest.of(page, size);
        Page<BidHistoryEntry> result =
                bidHistoryRepository.findByAuctionIdOrderByBidTimeDesc(auctionId, pageable);
        return ApiResponse.success(result);
    }

    /**
     * Order status timeline.
     */
    @GetMapping("/orders/{id}/timeline")
    public ApiResponse<List<OrderTimelineEntry>> orderTimeline(@PathVariable("id") Long orderId) {
        List<OrderTimelineEntry> result =
                orderTimelineRepository.findByOrderIdOrderByTimestampAsc(orderId);
        return ApiResponse.success(result);
    }

    /**
     * Stock movement log for a ticket event.
     */
    @GetMapping("/stock/{eventId}/movements")
    public ApiResponse<List<StockMovementEntry>> stockMovements(@PathVariable("eventId") Long eventId) {
        List<StockMovementEntry> result =
                stockMovementRepository.findByEventIdOrderByTimestampAsc(eventId);
        return ApiResponse.success(result);
    }
}

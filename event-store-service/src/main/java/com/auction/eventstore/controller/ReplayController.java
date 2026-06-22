package com.auction.eventstore.controller;

import com.auction.common.dto.ApiResponse;
import com.auction.eventstore.service.EventStoreService;
import com.auction.eventstore.service.ReplayResult;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Admin API for event replay / state reconstruction (PRD US-005).
 */
@RestController
@RequestMapping("/api/admin/events")
@RequiredArgsConstructor
public class ReplayController {

    private final EventStoreService eventStoreService;

    /**
     * Replay all events for an aggregate to reconstruct its state.
     * Loads the latest snapshot (if any), then applies events with sequenceNumber
     * greater than the snapshot's. Returns the reconstructed event tail + total.
     */
    @PostMapping("/replay/{aggregateType}/{aggregateId}")
    public ApiResponse<ReplayResult> replay(
            @PathVariable("aggregateType") String aggregateType,
            @PathVariable("aggregateId") String aggregateId) {
        ReplayResult result = eventStoreService.replay(aggregateId);
        return ApiResponse.success(result);
    }
}

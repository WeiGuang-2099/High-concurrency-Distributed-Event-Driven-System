package com.auction.eventstore.service;

import com.auction.eventstore.domain.EventDocument;
import com.auction.eventstore.domain.SnapshotDocument;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Result of an event replay: starting snapshot (if any) + remaining events to apply.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ReplayResult {
    private String aggregateId;
    private SnapshotDocument fromSnapshot;
    private List<EventDocument> events;
    private long totalEvents;
}

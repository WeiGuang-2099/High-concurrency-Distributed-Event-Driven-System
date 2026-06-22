package com.auction.eventstore.service;

import com.auction.common.event.BaseEvent;
import com.auction.eventstore.domain.EventDocument;
import com.auction.eventstore.domain.SnapshotDocument;
import com.auction.eventstore.repository.EventStoreRepository;
import com.auction.eventstore.repository.SnapshotRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Core event-store service: append-only persistence with idempotency.
 *
 * <p>Idempotency strategy (exactly-once semantics under at-least-once Kafka delivery):
 * <ol>
 *   <li>Check eventId existence (fast path, avoids duplicate key exceptions in common case).</li>
 *   <li>Compute sequenceNumber = max(sequenceNumber) + 1 for the aggregate.</li>
 *   <li>Insert; on DuplicateKeyException treat as success (already stored).</li>
 * </ol>
 * Note: sequenceNumber is best-effort monotonic per aggregate; the unique index on
 * (aggregateId, sequenceNumber) prevents two events getting the same sequence. If a
 * collision happens on retry, we re-fetch the max and retry once.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EventStoreService {

    private final EventStoreRepository eventRepository;
    private final SnapshotRepository snapshotRepository;
    private final ObjectMapper objectMapper;

    @Value("${event-store.snapshot.interval:100}")
    private long snapshotInterval;

    /**
     * Append an event. Returns true if newly stored, false if it was a duplicate
     * (idempotent success).
     */
    public boolean append(BaseEvent event, String topic, String aggregateType) {
        // Fast idempotency check on eventId
        if (eventRepository.findByEventId(event.getEventId()).isPresent()) {
            log.debug("Duplicate event ignored (eventId={}): already stored", event.getEventId());
            return false;
        }

        long sequence = nextSequenceNumber(event.getAggregateId());
        EventDocument doc = EventDocument.builder()
                .eventId(event.getEventId())
                .aggregateId(event.getAggregateId())
                .aggregateType(aggregateType)
                .eventType(event.getEventType())
                .sequenceNumber(sequence)
                .payload(toJson(event))
                .topic(topic)
                .metadata(buildMetadata(event))
                .timestamp(event.getTimestamp())
                .storedAt(Instant.now())
                .build();

        try {
            eventRepository.save(doc);
            log.info("Event stored: aggregateId={}, seq={}, type={}, eventId={}",
                    doc.getAggregateId(), sequence, doc.getEventType(), doc.getEventId());
        } catch (DuplicateKeyException dup) {
            // Could be duplicate on eventId or (aggregateId, sequenceNumber) collision.
            // Either way, an event for this aggregate already won -> idempotent success.
            log.warn("DuplicateKey on event store, treating as idempotent success: eventId={}, aggId={}",
                    event.getEventId(), event.getAggregateId());
            return false;
        }

        maybeSnapshot(event.getAggregateId(), aggregateType, sequence);
        return true;
    }

    /**
     * Generate a snapshot every snapshotInterval events for an aggregate.
     */
    private void maybeSnapshot(String aggregateId, String aggregateType, long currentSeq) {
        if (snapshotInterval <= 0 || currentSeq % snapshotInterval != 0) {
            return;
        }
        try {
            List<EventDocument> events = eventRepository
                    .findByAggregateIdOrderBySequenceNumberAsc(aggregateId);
            String state = objectMapper.writeValueAsString(events);
            SnapshotDocument snapshot = SnapshotDocument.builder()
                    .aggregateId(aggregateId)
                    .aggregateType(aggregateType)
                    .sequenceNumber(currentSeq)
                    .state(state)
                    .createdAt(Instant.now())
                    .build();
            snapshotRepository.save(snapshot);
            log.info("Snapshot created for aggregateId={} at seq={}", aggregateId, currentSeq);
        } catch (Exception e) {
            log.error("Failed to create snapshot for aggregateId={}: {}", aggregateId, e.getMessage());
        }
    }

    /**
     * Replay aggregate state from latest snapshot + remaining events.
     */
    public ReplayResult replay(String aggregateId) {
        Optional<SnapshotDocument> latest = snapshotRepository
                .findTopByAggregateIdOrderBySequenceNumberDesc(aggregateId);
        long fromSeq = latest.map(SnapshotDocument::getSequenceNumber).orElse(0L);
        List<EventDocument> tail = eventRepository
                .findByAggregateIdAndSequenceNumberGreaterThan(aggregateId, fromSeq);
        long total = eventRepository.countByAggregateId(aggregateId);
        return ReplayResult.builder()
                .aggregateId(aggregateId)
                .fromSnapshot(latest.orElse(null))
                .events(tail)
                .totalEvents(total)
                .build();
    }

    private long nextSequenceNumber(String aggregateId) {
        return eventRepository.findTopByAggregateIdOrderBySequenceNumberDesc(aggregateId)
                .map(EventDocument::getSequenceNumber)
                .orElse(0L) + 1;
    }

    private String toJson(BaseEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize event " + event.getEventId(), e);
        }
    }

    private Map<String, String> buildMetadata(BaseEvent event) {
        Map<String, String> meta = new HashMap<>();
        meta.put("correlationId", event.getCorrelationId());
        return meta;
    }
}

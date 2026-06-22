package com.auction.eventstore.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.Map;

/**
 * Append-only event document stored in MongoDB.
 *
 * <p>Unique compound index on (aggregateId, sequenceNumber) enforces idempotency:
 * duplicate consumption of the same event fails the insert, which is treated as success.</p>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "events")
@CompoundIndex(name = "uniq_agg_seq", def = "{'aggregateId': 1, 'sequenceNumber': 1}", unique = true)
public class EventDocument {

    @Id
    private String id;

    @Indexed(unique = true)
    private String eventId;

    private String aggregateId;

    private String aggregateType;

    private String eventType;

    private long sequenceNumber;

    private String payload;

    private String topic;

    private Map<String, String> metadata;

    private Instant timestamp;

    private Instant storedAt;
}

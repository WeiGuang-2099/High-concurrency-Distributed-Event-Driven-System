package com.auction.eventstore.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/**
 * Snapshot of an aggregate state at a given sequence number.
 *
 * <p>On replay, load the latest snapshot then apply only events with
 * sequenceNumber {@code >} snapshot.sequenceNumber to avoid scanning full history.</p>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "snapshots")
@CompoundIndex(name = "agg_seq", def = "{'aggregateId': 1, 'sequenceNumber': -1}")
public class SnapshotDocument {

    @Id
    private String id;

    private String aggregateId;

    private String aggregateType;

    /** Sequence number of the last event folded into this snapshot. */
    private long sequenceNumber;

    /** Serialized aggregate state (JSON). */
    private String state;

    private Instant createdAt;
}

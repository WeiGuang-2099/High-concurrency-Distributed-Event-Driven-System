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

/**
 * CQRS read model: a stock movement log entry (reserve / confirm / release).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "stock_movements")
@CompoundIndex(name = "event_time", def = "{'eventId': 1, 'timestamp': 1}")
public class StockMovementEntry {

    @Id
    private String id;

    private String kafkaEventId;

    @Indexed
    private Long eventId;

    private String ticketType;

    private String movementType;

    private Long userId;

    private Long reservationId;

    private int quantity;

    private Instant timestamp;
}

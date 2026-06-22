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
 * CQRS read model: an entry in the order status timeline.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "order_timeline")
@CompoundIndex(name = "order_time", def = "{'orderId': 1, 'timestamp': 1}")
public class OrderTimelineEntry {

    @Id
    private String id;

    private String eventId;

    @Indexed
    private Long orderId;

    private Long userId;

    private String status;

    private String eventType;

    private String detail;

    private Instant timestamp;
}

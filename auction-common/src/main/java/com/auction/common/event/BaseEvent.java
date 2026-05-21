package com.auction.common.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BaseEvent {

    private String eventId;
    private String aggregateId;
    private String eventType;
    private Instant timestamp;

    public BaseEvent(String aggregateId, String eventType) {
        this.eventId = UUID.randomUUID().toString();
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.timestamp = Instant.now();
    }
}

package com.auction.ticket.domain.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * A row in the transactional outbox: an event that has been decided but not yet
 * published. Written in the same transaction as the business change it describes.
 */
@Data
@TableName("outbox")
public class OutboxEvent {

    @TableId(type = IdType.AUTO)
    private Long id;

    /** The event's own id; consumers dedupe on this when the relay retries. */
    private String eventId;

    private String topic;

    /** Kafka partition key, so events about one aggregate stay ordered. */
    private String messageKey;

    private String eventType;

    /**
     * Concrete event class. The payload carries no type information, so the relay needs
     * this to rebuild the real event -- deserializing to BaseEvent would drop every
     * subtype field, and the consumers' __TypeId__ header comes from the object's class.
     */
    private String payloadClass;

    /** The serialized event. */
    private String payload;

    private LocalDateTime createdAt;

    private String claimedBy;

    private LocalDateTime claimedAt;

    /** Null until the broker has acknowledged the event. */
    private LocalDateTime sentAt;
}

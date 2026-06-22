package com.auction.eventstore.consumer;

import com.auction.common.event.BaseEvent;
import com.auction.common.event.KafkaTopics;
import com.auction.common.event.ticket.StockConfirmedEvent;
import com.auction.common.event.ticket.StockReleasedEvent;
import com.auction.common.event.ticket.StockReservedEvent;
import com.auction.eventstore.domain.StockMovementEntry;
import com.auction.eventstore.repository.StockMovementRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Projection: logs every stock reserve / confirm / release as a stock_movement entry.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StockMovementProjection {

    private final StockMovementRepository repository;

    @KafkaListener(
            topics = KafkaTopics.TICKET_EVENTS,
            groupId = "projection-stock-movement",
            containerFactory = "kafkaListenerContainerFactory")
    public void on(Object payload, Acknowledgment ack) {
        try {
            if (payload instanceof StockReservedEvent) {
                StockReservedEvent e = (StockReservedEvent) payload;
                repository.save(build(e, e.getTicketEventId(), e.getTicketType(), "RESERVED",
                        e.getUserId(), e.getReservationId(), e.getQuantity()));
            } else if (payload instanceof StockConfirmedEvent) {
                StockConfirmedEvent e = (StockConfirmedEvent) payload;
                repository.save(build(e, null, null, "CONFIRMED",
                        e.getUserId(), e.getReservationId(), 0));
            } else if (payload instanceof StockReleasedEvent) {
                StockReleasedEvent e = (StockReleasedEvent) payload;
                repository.save(build(e, e.getTicketEventId(), e.getTicketType(), "RELEASED",
                        null, e.getReservationId(), e.getQuantity()));
            }
        } catch (Exception e) {
            log.error("StockMovementProjection failed: {}", e.getMessage(), e);
        } finally {
            ack.acknowledge();
        }
    }

    private StockMovementEntry build(BaseEvent base, Long eventId, String ticketType,
                                     String movementType, Long userId, Long reservationId,
                                     int quantity) {
        return StockMovementEntry.builder()
                .kafkaEventId(base.getEventId())
                .eventId(eventId)
                .ticketType(ticketType)
                .movementType(movementType)
                .userId(userId)
                .reservationId(reservationId)
                .quantity(quantity)
                .timestamp(base.getTimestamp())
                .build();
    }
}


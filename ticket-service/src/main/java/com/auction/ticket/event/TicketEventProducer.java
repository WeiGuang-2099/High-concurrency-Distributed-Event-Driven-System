package com.auction.ticket.event;

import com.auction.common.event.BaseEvent;
import com.auction.common.event.KafkaTopics;
import com.auction.common.event.ticket.StockConfirmedEvent;
import com.auction.common.event.ticket.StockReleasedEvent;
import com.auction.common.event.ticket.StockReservedEvent;
import com.auction.common.event.ticket.TicketCreatedEvent;
import com.auction.ticket.domain.entity.OutboxEvent;
import com.auction.ticket.repository.OutboxMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Writes ticket events to the transactional outbox instead of sending them to Kafka.
 *
 * <p>Publishing used to happen in an afterCommit callback. That is better than sending
 * inside the transaction (no event for a rolled-back change) but still loses the event if
 * the process dies between the commit and the send: stock has moved and nothing says so.
 * Inserting the event in the caller's transaction closes that window -- the event row and
 * the business change commit together or not at all. {@link OutboxRelay} publishes them.</p>
 *
 * <p>MANDATORY propagation is the point, not decoration: this is only correct inside the
 * business transaction. Called without one, the insert would commit on its own and the
 * outbox would be just another write that can disagree with the first -- so fail loudly
 * rather than quietly provide no guarantee. Declared on the class so it is enforced by the
 * proxy for external callers (a self-invoked private helper would not be intercepted).</p>
 */
@Component
@Transactional(propagation = Propagation.MANDATORY)
public class TicketEventProducer {

    private final OutboxMapper outboxMapper;
    private final ObjectMapper objectMapper;

    public TicketEventProducer(OutboxMapper outboxMapper, ObjectMapper objectMapper) {
        this.outboxMapper = outboxMapper;
        this.objectMapper = objectMapper;
    }

    public void publishTicketCreated(Long eventId, String ticketType, int totalQuantity) {
        enqueue(new TicketCreatedEvent(eventId, ticketType, totalQuantity), String.valueOf(eventId));
    }

    public void publishStockReserved(Long eventId, String ticketType, Long reservationId, Long userId, int quantity) {
        enqueue(new StockReservedEvent(eventId, ticketType, reservationId, userId, quantity),
                String.valueOf(reservationId));
    }

    public void publishStockConfirmed(Long reservationId, Long userId) {
        enqueue(new StockConfirmedEvent(reservationId, userId), String.valueOf(reservationId));
    }

    public void publishStockReleased(Long reservationId, Long eventId, String ticketType, int quantity, String reason) {
        enqueue(new StockReleasedEvent(reservationId, eventId, ticketType, quantity, reason),
                String.valueOf(reservationId));
    }

    private void enqueue(BaseEvent event, String messageKey) {
        OutboxEvent row = new OutboxEvent();
        row.setEventId(event.getEventId());
        row.setTopic(KafkaTopics.TICKET_EVENTS);
        row.setMessageKey(messageKey);
        row.setEventType(event.getEventType());
        row.setPayloadClass(event.getClass().getName());
        row.setPayload(serialize(event));
        outboxMapper.insert(row);
    }

    private String serialize(BaseEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            // An unserializable event is a bug in the event class. Throwing rolls back the
            // business change too, which is the right outcome: better to reject the reserve
            // than to complete it carrying an event nobody can ever publish.
            throw new IllegalStateException(
                    "Cannot serialize " + event.getEventType() + " for the outbox", e);
        }
    }
}

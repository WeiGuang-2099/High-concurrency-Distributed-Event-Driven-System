package com.auction.ticket.event;

import com.auction.common.event.KafkaTopics;
import com.auction.common.event.ticket.StockConfirmedEvent;
import com.auction.common.event.ticket.StockReleasedEvent;
import com.auction.common.event.ticket.StockReservedEvent;
import com.auction.common.event.ticket.TicketCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class TicketEventProducer {

    private static final Logger log = LoggerFactory.getLogger(TicketEventProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public TicketEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishTicketCreated(Long eventId, String ticketType, int totalQuantity) {
        TicketCreatedEvent event = new TicketCreatedEvent(eventId, ticketType, totalQuantity);
        send(KafkaTopics.TICKET_EVENTS, String.valueOf(eventId), event);
    }

    public void publishStockReserved(Long eventId, String ticketType, Long reservationId, Long userId, int quantity) {
        StockReservedEvent event = new StockReservedEvent(eventId, ticketType, reservationId, userId, quantity);
        send(KafkaTopics.TICKET_EVENTS, String.valueOf(reservationId), event);
    }

    public void publishStockConfirmed(Long reservationId, Long userId) {
        StockConfirmedEvent event = new StockConfirmedEvent(reservationId, userId);
        send(KafkaTopics.TICKET_EVENTS, String.valueOf(reservationId), event);
    }

    public void publishStockReleased(Long reservationId, Long eventId, String ticketType, int quantity, String reason) {
        StockReleasedEvent event = new StockReleasedEvent(reservationId, eventId, ticketType, quantity, reason);
        send(KafkaTopics.TICKET_EVENTS, String.valueOf(reservationId), event);
    }

    private void send(String topic, String key, Object event) {
        kafkaTemplate.send(topic, key, event).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send event to topic={}, key={}, event={}", topic, key, event.getClass().getSimpleName(), ex);
            }
        });
    }
}

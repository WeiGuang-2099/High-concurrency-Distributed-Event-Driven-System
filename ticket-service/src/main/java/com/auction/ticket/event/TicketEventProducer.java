package com.auction.ticket.event;

import com.auction.common.event.KafkaTopics;
import com.auction.common.event.ticket.StockConfirmedEvent;
import com.auction.common.event.ticket.StockReleasedEvent;
import com.auction.common.event.ticket.StockReservedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class TicketEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public TicketEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishStockReserved(Long eventId, String ticketType, Long reservationId, Long userId, int quantity) {
        StockReservedEvent event = new StockReservedEvent(eventId, ticketType, reservationId, userId, quantity);
        kafkaTemplate.send(KafkaTopics.TICKET_EVENTS, String.valueOf(reservationId), event);
    }

    public void publishStockConfirmed(Long reservationId, Long userId) {
        StockConfirmedEvent event = new StockConfirmedEvent(reservationId, userId);
        kafkaTemplate.send(KafkaTopics.TICKET_EVENTS, String.valueOf(reservationId), event);
    }

    public void publishStockReleased(Long reservationId, Long eventId, String ticketType, int quantity, String reason) {
        StockReleasedEvent event = new StockReleasedEvent(reservationId, eventId, ticketType, quantity, reason);
        kafkaTemplate.send(KafkaTopics.TICKET_EVENTS, String.valueOf(reservationId), event);
    }
}

package com.auction.notification.event;

import com.auction.common.event.KafkaTopics;
import com.auction.common.event.ticket.StockConfirmedEvent;
import com.auction.common.event.ticket.StockReleasedEvent;
import com.auction.common.event.ticket.StockReservedEvent;
import com.auction.common.event.ticket.TicketCreatedEvent;
import com.auction.notification.controller.dto.StockConfirmedMessage;
import com.auction.notification.controller.dto.StockReleasedMessage;
import com.auction.notification.controller.dto.StockReservedMessage;
import com.auction.notification.controller.dto.TicketCreatedMessage;
import com.auction.notification.domain.entity.Notification;
import com.auction.notification.service.NotificationPersistenceService;
import com.auction.notification.service.NotificationPushService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class TicketEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(TicketEventConsumer.class);

    private final NotificationPushService pushService;
    private final NotificationPersistenceService persistenceService;

    public TicketEventConsumer(NotificationPushService pushService,
                               NotificationPersistenceService persistenceService) {
        this.pushService = pushService;
        this.persistenceService = persistenceService;
    }

    @KafkaListener(
            topics = KafkaTopics.TICKET_EVENTS,
            groupId = "notification-service",
            containerFactory = "kafkaListenerContainerFactory")
    public void onEvent(Object payload, Acknowledgment ack) {
        try {
            if (payload instanceof TicketCreatedEvent event) {
                handleTicketCreated(event);
            } else if (payload instanceof StockReservedEvent event) {
                handleStockReserved(event);
            } else if (payload instanceof StockConfirmedEvent event) {
                handleStockConfirmed(event);
            } else if (payload instanceof StockReleasedEvent event) {
                handleStockReleased(event);
            } else {
                log.debug("Ignoring unhandled event type: {}", payload.getClass().getSimpleName());
            }
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("Failed to handle ticket event in notification-service", ex);
            throw ex;
        }
    }

    private void handleTicketCreated(TicketCreatedEvent event) {
        TicketCreatedMessage message = TicketCreatedMessage.builder()
                .eventId(event.getEventId())
                .ticketType(event.getTicketType())
                .totalQuantity(event.getTotalQuantity())
                .build();
        pushService.pushToTicket(event.getEventId(), "created", message);
        log.info("Pushed TicketCreated notification: eventId={} ticketType={}",
                event.getEventId(), event.getTicketType());
    }

    private void handleStockReserved(StockReservedEvent event) {
        StockReservedMessage message = StockReservedMessage.builder()
                .reservationId(event.getReservationId())
                .userId(event.getUserId())
                .quantity(event.getQuantity())
                .ticketType(event.getTicketType())
                .build();
        pushService.pushToTicket(event.getEventId(), "reserved", message);
        persistenceService.save(Notification.builder()
                .userId(event.getUserId())
                .type("STOCK_RESERVED")
                .title("Ticket Reserved")
                .content("Your reservation for " + event.getQuantity() + " " + event.getTicketType()
                        + " ticket(s) has been created. Reservation ID: " + event.getReservationId())
                .build());
        log.info("Pushed StockReserved notification: eventId={} reservationId={} userId={}",
                event.getEventId(), event.getReservationId(), event.getUserId());
    }

    private void handleStockConfirmed(StockConfirmedEvent event) {
        StockConfirmedMessage message = StockConfirmedMessage.builder()
                .reservationId(event.getReservationId())
                .userId(event.getUserId())
                .build();
        pushService.pushToTicket(null, "confirmed", message);
        persistenceService.save(Notification.builder()
                .userId(event.getUserId())
                .type("STOCK_CONFIRMED")
                .title("Reservation Confirmed")
                .content("Your ticket reservation has been confirmed. Reservation ID: " + event.getReservationId())
                .build());
        log.info("Pushed StockConfirmed notification: reservationId={} userId={}",
                event.getReservationId(), event.getUserId());
    }

    private void handleStockReleased(StockReleasedEvent event) {
        StockReleasedMessage message = StockReleasedMessage.builder()
                .reservationId(event.getReservationId())
                .ticketType(event.getTicketType())
                .quantity(event.getQuantity())
                .reason(event.getReason())
                .build();
        pushService.pushToTicket(event.getEventId(), "released", message);
        log.info("Pushed StockReleased notification: eventId={} reservationId={}",
                event.getEventId(), event.getReservationId());
    }
}

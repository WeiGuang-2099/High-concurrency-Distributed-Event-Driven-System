package com.auction.ticket.consumer;

import com.auction.ticket.domain.entity.Reservation;
import com.auction.ticket.domain.entity.TicketStock;
import com.auction.ticket.domain.enums.ReservationStatus;
import com.auction.ticket.event.TicketEventProducer;
import com.auction.ticket.repository.ReservationMapper;
import com.auction.ticket.repository.TicketStockMapper;
import com.auction.ticket.service.RedisKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.List;

@Component
public class StockReleaseConsumer {

    private static final Logger log = LoggerFactory.getLogger(StockReleaseConsumer.class);

    private final ReservationMapper reservationMapper;
    private final TicketStockMapper stockMapper;
    private final StringRedisTemplate redis;
    @SuppressWarnings("rawtypes")
    private final DefaultRedisScript<List> releaseTicketScript;
    private final TicketEventProducer eventProducer;

    @SuppressWarnings("rawtypes")
    public StockReleaseConsumer(ReservationMapper reservationMapper,
                                TicketStockMapper stockMapper,
                                StringRedisTemplate redis,
                                DefaultRedisScript<List> releaseTicketScript,
                                TicketEventProducer eventProducer) {
        this.reservationMapper = reservationMapper;
        this.stockMapper = stockMapper;
        this.redis = redis;
        this.releaseTicketScript = releaseTicketScript;
        this.eventProducer = eventProducer;
    }

    @RabbitListener(queues = "stock-release-queue")
    @Transactional
    public void handleStockRelease(Long reservationId) {
        log.info("Received stock release delayed message for reservationId={}", reservationId);

        Reservation reservation = reservationMapper.findById(reservationId);
        if (reservation == null) {
            log.warn("Reservation not found: id={}", reservationId);
            return;
        }

        if (reservation.getStatus() != ReservationStatus.PENDING) {
            log.info("Reservation already processed: id={}, status={}", reservationId, reservation.getStatus());
            return;
        }

        int updated = reservationMapper.updateStatusIfPending(reservationId, ReservationStatus.EXPIRED.name());
        if (updated == 0) {
            log.info("Reservation status race: id={}, another thread handled it", reservationId);
            return;
        }

        stockMapper.decrementReserved(reservation.getStockId(), reservation.getQuantity());

        TicketStock stock = stockMapper.selectById(reservation.getStockId());
        if (stock != null) {
            String stockKey = RedisKeys.stock(stock.getEventId(), stock.getTicketType());
            int quantity = reservation.getQuantity();
            Long eventId = stock.getEventId();
            String ticketType = stock.getTicketType();

            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    rollbackRedis(stockKey, quantity);
                    eventProducer.publishStockReleased(
                            reservationId, eventId, ticketType, quantity, "TIMEOUT");
                }
            });
        }

        log.info("Expired reservation: id={}", reservationId);
    }

    @SuppressWarnings("unchecked")
    private void rollbackRedis(String stockKey, int quantity) {
        try {
            redis.execute(releaseTicketScript, List.of(stockKey), String.valueOf(quantity));
            log.info("Redis stock rolled back: key={}, quantity={}", stockKey, quantity);
        } catch (Exception e) {
            log.error("Redis rollback failed for delayed release, key={}", stockKey, e);
        }
    }
}

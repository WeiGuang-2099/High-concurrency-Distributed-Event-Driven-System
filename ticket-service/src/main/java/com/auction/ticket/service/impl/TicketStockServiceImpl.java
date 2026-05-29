package com.auction.ticket.service.impl;

import com.auction.ticket.config.RabbitMQConfig;
import com.auction.ticket.controller.dto.CreateTicketRequest;
import com.auction.ticket.controller.dto.ReserveRequest;
import com.auction.ticket.controller.dto.ReserveResponse;
import com.auction.ticket.controller.dto.TicketStockResponse;
import com.auction.ticket.domain.entity.Reservation;
import com.auction.ticket.domain.entity.TicketStock;
import com.auction.ticket.domain.enums.ReservationStatus;
import com.auction.ticket.event.TicketEventProducer;
import com.auction.ticket.exception.BusinessException;
import com.auction.ticket.repository.ReservationMapper;
import com.auction.ticket.repository.TicketStockMapper;
import com.auction.ticket.service.RedisKeys;
import com.auction.ticket.service.TicketStockService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class TicketStockServiceImpl implements TicketStockService {

    private static final Logger log = LoggerFactory.getLogger(TicketStockServiceImpl.class);
    private static final int RESERVATION_TIMEOUT_MINUTES = 30;

    private final AtomicBoolean ready = new AtomicBoolean(false);

    private final StringRedisTemplate redis;
    @SuppressWarnings("rawtypes")
    private final DefaultRedisScript<List> reserveTicketScript;
    @SuppressWarnings("rawtypes")
    private final DefaultRedisScript<List> releaseTicketScript;
    private final TicketStockMapper stockMapper;
    private final ReservationMapper reservationMapper;
    private final TicketEventProducer eventProducer;
    private final RabbitTemplate rabbitTemplate;

    @SuppressWarnings("rawtypes")
    public TicketStockServiceImpl(StringRedisTemplate redis,
                                  DefaultRedisScript<List> reserveTicketScript,
                                  DefaultRedisScript<List> releaseTicketScript,
                                  TicketStockMapper stockMapper,
                                  ReservationMapper reservationMapper,
                                  TicketEventProducer eventProducer,
                                  RabbitTemplate rabbitTemplate) {
        this.redis = redis;
        this.reserveTicketScript = reserveTicketScript;
        this.releaseTicketScript = releaseTicketScript;
        this.stockMapper = stockMapper;
        this.reservationMapper = reservationMapper;
        this.eventProducer = eventProducer;
        this.rabbitTemplate = rabbitTemplate;
    }

    public void markReady() {
        ready.set(true);
    }

    @Override
    public List<TicketStockResponse> getStockByEvent(Long eventId) {
        return stockMapper.findByEventId(eventId).stream()
                .map(this::toStockResponse)
                .toList();
    }

    @Override
    @SuppressWarnings("unchecked")
    @Transactional
    public ReserveResponse reserve(Long userId, ReserveRequest request) {
        if (!ready.get()) {
            throw new BusinessException(503, "Service is initializing, please retry");
        }

        String stockKey = RedisKeys.stock(request.getEventId(), request.getTicketType());

        List<Object> result;
        try {
            result = (List<Object>) redis.execute(
                    reserveTicketScript,
                    List.of(stockKey),
                    String.valueOf(request.getQuantity()));
        } catch (Exception e) {
            log.error("Redis execution failed for stock key={}", stockKey, e);
            throw new BusinessException(500, "Stock service unavailable");
        }

        if (result == null || result.size() < 2) {
            throw new BusinessException(500, "Stock evaluation failed");
        }

        long okFlag = ((Number) result.get(0)).longValue();
        if (okFlag != 1L) {
            String code = (String) result.get(1);
            if ("OUT_OF_STOCK".equals(code)) {
                throw new BusinessException(400, "Out of stock");
            }
            throw new BusinessException(404, "Ticket stock not found");
        }

        TicketStock stock = stockMapper.findByEventIdAndTicketType(
                request.getEventId(), request.getTicketType());
        if (stock == null) {
            rollbackRedis(stockKey, request.getQuantity());
            throw new BusinessException(404, "Ticket stock not found");
        }

        Reservation reservation = new Reservation();
        reservation.setStockId(stock.getId());
        reservation.setUserId(userId);
        reservation.setQuantity(request.getQuantity());
        reservation.setStatus(ReservationStatus.PENDING);
        reservation.setExpireAt(LocalDateTime.now().plusMinutes(RESERVATION_TIMEOUT_MINUTES));
        reservationMapper.insert(reservation);

        stockMapper.incrementReserved(stock.getId(), request.getQuantity());

        final Long reservationId = reservation.getId();
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                eventProducer.publishStockReserved(
                        request.getEventId(), request.getTicketType(),
                        reservationId, userId, request.getQuantity());

                rabbitTemplate.convertAndSend(
                        RabbitMQConfig.DELAY_EXCHANGE,
                        RabbitMQConfig.STOCK_RELEASE_ROUTING_KEY,
                        reservationId,
                        msg -> {
                            msg.getMessageProperties().setDelay(RESERVATION_TIMEOUT_MINUTES * 60 * 1000);
                            return msg;
                        });

                log.info("Reserved {} tickets for user={}, event={}, type={}, reservationId={}",
                        request.getQuantity(), userId, request.getEventId(),
                        request.getTicketType(), reservationId);
            }
        });

        return ReserveResponse.builder()
                .reservationId(reservationId)
                .eventId(request.getEventId())
                .ticketType(request.getTicketType())
                .quantity(request.getQuantity())
                .expireAt(reservation.getExpireAt())
                .build();
    }

    @Override
    @Transactional
    public void confirm(Long userId, Long reservationId) {
        Reservation reservation = reservationMapper.findByIdAndUserId(reservationId, userId);
        if (reservation == null) {
            throw new BusinessException(404, "Reservation not found");
        }
        if (reservation.getStatus() != ReservationStatus.PENDING) {
            throw new BusinessException(400, "Reservation is not in PENDING status");
        }

        reservationMapper.updateStatusIfPending(reservationId, ReservationStatus.CONFIRMED.name());
        stockMapper.decrementReservedAndIncrementSold(reservation.getStockId(), reservation.getQuantity());

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                eventProducer.publishStockConfirmed(reservationId, userId);
                log.info("Confirmed reservation id={}, userId={}", reservationId, userId);
            }
        });
    }

    @Override
    @Transactional
    public void cancel(Long userId, Long reservationId) {
        Reservation reservation = reservationMapper.findByIdAndUserId(reservationId, userId);
        if (reservation == null) {
            throw new BusinessException(404, "Reservation not found");
        }
        if (reservation.getStatus() != ReservationStatus.PENDING) {
            throw new BusinessException(400, "Reservation is not in PENDING status");
        }

        TicketStock stock = stockMapper.selectById(reservation.getStockId());

        reservationMapper.updateStatusIfPending(reservationId, ReservationStatus.CANCELLED.name());
        stockMapper.decrementReserved(reservation.getStockId(), reservation.getQuantity());

        if (stock != null) {
            String stockKey = RedisKeys.stock(stock.getEventId(), stock.getTicketType());
            rollbackRedis(stockKey, reservation.getQuantity());

            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    eventProducer.publishStockReleased(
                            reservationId, stock.getEventId(), stock.getTicketType(),
                            reservation.getQuantity(), "CANCELLED");
                    log.info("Cancelled reservation id={}, userId={}", reservationId, userId);
                }
            });
        }
    }

    @Override
    @Transactional
    public TicketStockResponse createTicketStock(CreateTicketRequest request) {
        TicketStock stock = new TicketStock();
        stock.setEventId(request.getEventId());
        stock.setTicketType(request.getTicketType());
        stock.setTotalQuantity(request.getTotalQuantity());
        stock.setReservedQuantity(0);
        stock.setSoldQuantity(0);
        stock.setVersion(0);
        stockMapper.insert(stock);

        String stockKey = RedisKeys.stock(request.getEventId(), request.getTicketType());
        redis.opsForValue().set(stockKey, String.valueOf(request.getTotalQuantity()));

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                eventProducer.publishTicketCreated(
                        request.getEventId(), request.getTicketType(), request.getTotalQuantity());
                log.info("Created ticket stock: eventId={}, type={}, total={}",
                        request.getEventId(), request.getTicketType(), request.getTotalQuantity());
            }
        });

        return toStockResponse(stock);
    }

    @SuppressWarnings("unchecked")
    private void rollbackRedis(String stockKey, int quantity) {
        try {
            redis.execute(releaseTicketScript, List.of(stockKey), String.valueOf(quantity));
        } catch (Exception e) {
            log.error("Redis rollback failed for key={}, quantity={}", stockKey, quantity, e);
        }
    }

    private TicketStockResponse toStockResponse(TicketStock stock) {
        int available = stock.getTotalQuantity() - stock.getReservedQuantity() - stock.getSoldQuantity();
        return TicketStockResponse.builder()
                .stockId(stock.getId())
                .eventId(stock.getEventId())
                .ticketType(stock.getTicketType())
                .totalQuantity(stock.getTotalQuantity())
                .availableQuantity(Math.max(0, available))
                .reservedQuantity(stock.getReservedQuantity())
                .soldQuantity(stock.getSoldQuantity())
                .build();
    }
}

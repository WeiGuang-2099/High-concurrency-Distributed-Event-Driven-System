package com.auction.notification.event;

import com.auction.common.event.KafkaTopics;
import com.auction.common.event.auction.AuctionExpiredEvent;
import com.auction.common.event.auction.AuctionSettledEvent;
import com.auction.common.event.auction.BidOutbidEvent;
import com.auction.common.event.auction.BidPlacedEvent;
import com.auction.notification.controller.dto.AuctionExpiredMessage;
import com.auction.notification.controller.dto.AuctionSettledMessage;
import com.auction.notification.controller.dto.BidUpdateMessage;
import com.auction.notification.controller.dto.OutbidNotificationMessage;
import com.auction.notification.service.NotificationPushService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class AuctionEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(AuctionEventConsumer.class);

    private final NotificationPushService pushService;

    public AuctionEventConsumer(NotificationPushService pushService) {
        this.pushService = pushService;
    }

    @KafkaListener(
            topics = KafkaTopics.AUCTION_EVENTS,
            groupId = "notification-service",
            containerFactory = "kafkaListenerContainerFactory")
    public void onEvent(Object payload, Acknowledgment ack) {
        try {
            if (payload instanceof BidPlacedEvent event) {
                handleBidPlaced(event);
            } else if (payload instanceof BidOutbidEvent event) {
                handleBidOutbid(event);
            } else if (payload instanceof AuctionSettledEvent event) {
                handleAuctionSettled(event);
            } else if (payload instanceof AuctionExpiredEvent event) {
                handleAuctionExpired(event);
            } else {
                log.debug("Ignoring unhandled event type: {}", payload.getClass().getSimpleName());
            }
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("Failed to handle auction event in notification-service", ex);
            throw ex;
        }
    }

    private void handleBidPlaced(BidPlacedEvent event) {
        BidUpdateMessage message = BidUpdateMessage.builder()
                .auctionId(event.getAuctionId())
                .bidderName(event.getBidderUsername())
                .amount(event.getAmount())
                .timestamp(event.getBidTime())
                .build();
        pushService.pushBidUpdate(message);
        log.info("Pushed BidPlaced notification: auction={} bidder={} amount={}",
                event.getAuctionId(), event.getBidderUsername(), event.getAmount());
    }

    private void handleBidOutbid(BidOutbidEvent event) {
        OutbidNotificationMessage message = OutbidNotificationMessage.builder()
                .auctionId(event.getAuctionId())
                .outbidUserId(event.getOutbidUserId())
                .outbidAmount(event.getOutbidAmount())
                .newBidderId(event.getNewBidderId())
                .newAmount(event.getNewAmount())
                .build();
        pushService.pushToAuction(event.getAuctionId(), "outbid", message);
        log.info("Pushed Outbid notification: auction={} outbidUser={}",
                event.getAuctionId(), event.getOutbidUserId());
    }

    private void handleAuctionSettled(AuctionSettledEvent event) {
        AuctionSettledMessage message = AuctionSettledMessage.builder()
                .auctionId(event.getAuctionId())
                .winnerId(event.getWinnerId())
                .finalAmount(event.getFinalAmount())
                .build();
        pushService.pushToAuction(event.getAuctionId(), "settled", message);
        log.info("Pushed AuctionSettled notification: auction={} winner={}",
                event.getAuctionId(), event.getWinnerId());
    }

    private void handleAuctionExpired(AuctionExpiredEvent event) {
        AuctionExpiredMessage message = AuctionExpiredMessage.builder()
                .auctionId(event.getAuctionId())
                .reason(event.getReason())
                .build();
        pushService.pushToAuction(event.getAuctionId(), "expired", message);
        log.info("Pushed AuctionExpired notification: auction={}", event.getAuctionId());
    }
}

package com.auction.notification.service;

import com.auction.notification.controller.dto.BidUpdateMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Service
public class NotificationPushService {

    private static final Logger log = LoggerFactory.getLogger(NotificationPushService.class);

    private final SimpMessagingTemplate messagingTemplate;

    public NotificationPushService(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    /**
     * Push a bid update to all subscribers of a specific auction.
     * Destination: /topic/auction/{auctionId}
     */
    public void pushBidUpdate(BidUpdateMessage message) {
        String destination = "/topic/auction/" + message.getAuctionId();
        messagingTemplate.convertAndSend(destination, message);
        log.debug("Pushed bid update to {}: bidder={} amount={}",
                destination, message.getBidderName(), message.getAmount());
    }

    /**
     * Push an arbitrary notification to all subscribers of a specific auction.
     */
    public void pushToAuction(Long auctionId, String suffix, Object payload) {
        String destination = "/topic/auction/" + auctionId + "/" + suffix;
        messagingTemplate.convertAndSend(destination, payload);
        log.debug("Pushed notification to {}: {}", destination, payload);
    }
}

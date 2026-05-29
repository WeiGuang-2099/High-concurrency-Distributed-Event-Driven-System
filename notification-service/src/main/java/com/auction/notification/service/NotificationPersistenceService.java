package com.auction.notification.service;

import com.auction.notification.domain.entity.Notification;
import com.auction.notification.repository.NotificationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class NotificationPersistenceService {

    private static final Logger log = LoggerFactory.getLogger(NotificationPersistenceService.class);

    private final NotificationRepository notificationRepository;

    public NotificationPersistenceService(NotificationRepository notificationRepository) {
        this.notificationRepository = notificationRepository;
    }

    public Notification save(Notification notification) {
        if (notification.getCreatedAt() == null) {
            notification.setCreatedAt(LocalDateTime.now());
        }
        Notification saved = notificationRepository.save(notification);
        log.debug("Saved notification: id={} userId={} type={}", saved.getId(), saved.getUserId(), saved.getType());
        return saved;
    }

    public List<Notification> getNotificationsByUser(Long userId) {
        return notificationRepository.findByUserIdOrderByCreatedAtDesc(userId);
    }

    public Notification markAsRead(String notificationId) {
        Notification notification = notificationRepository.findById(notificationId)
                .orElseThrow(() -> new IllegalArgumentException("Notification not found: " + notificationId));
        notification.setRead(true);
        return notificationRepository.save(notification);
    }

    public long getUnreadCount(Long userId) {
        return notificationRepository.countByUserIdAndReadFalse(userId);
    }
}

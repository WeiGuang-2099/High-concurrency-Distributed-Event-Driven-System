package com.auction.notification.service;

import com.auction.notification.domain.entity.Notification;
import com.auction.notification.repository.NotificationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class NotificationPersistenceService {

    private static final Logger log = LoggerFactory.getLogger(NotificationPersistenceService.class);

    private final NotificationRepository notificationRepository;
    private final MongoTemplate mongoTemplate;

    public NotificationPersistenceService(NotificationRepository notificationRepository,
                                          MongoTemplate mongoTemplate) {
        this.notificationRepository = notificationRepository;
        this.mongoTemplate = mongoTemplate;
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

    public Page<Notification> getNotificationsByUser(Long userId, Pageable pageable) {
        return notificationRepository.findByUserIdOrderByCreatedAtDesc(userId, pageable);
    }

    public Notification markAsRead(String notificationId, Long userId) {
        Query query = Query.query(Criteria.where("_id").is(notificationId).and("userId").is(userId));
        Update update = new Update().set("read", true);
        FindAndModifyOptions options = FindAndModifyOptions.options().returnNew();
        Notification notification = mongoTemplate.findAndModify(query, update, options, Notification.class);
        if (notification == null) {
            throw new IllegalArgumentException("Notification not found or not owned by user: " + notificationId);
        }
        return notification;
    }

    public long getUnreadCount(Long userId) {
        return notificationRepository.countByUserIdAndReadFalse(userId);
    }
}

package com.auction.notification.controller;

import com.auction.common.dto.ApiResponse;
import com.auction.common.security.UserContext;
import com.auction.notification.domain.entity.Notification;
import com.auction.common.security.UserContextHolder;
import com.auction.notification.service.NotificationPersistenceService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/notifications")
public class NotificationController {

    private final NotificationPersistenceService notificationService;

    public NotificationController(NotificationPersistenceService notificationService) {
        this.notificationService = notificationService;
    }

    @GetMapping
    public ResponseEntity<ApiResponse<List<Notification>>> getNotifications() {
        UserContext ctx = UserContextHolder.get();
        List<Notification> notifications = notificationService.getNotificationsByUser(ctx.getUserId());
        return ResponseEntity.ok(ApiResponse.success(notifications));
    }

    @PutMapping("/{id}/read")
    public ResponseEntity<ApiResponse<Notification>> markAsRead(@PathVariable String id) {
        Notification notification = notificationService.markAsRead(id);
        return ResponseEntity.ok(ApiResponse.success(notification));
    }
}

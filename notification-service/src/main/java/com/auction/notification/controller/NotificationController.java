package com.auction.notification.controller;

import com.auction.common.dto.ApiResponse;
import com.auction.common.security.UserContext;
import com.auction.notification.domain.entity.Notification;
import com.auction.common.security.UserContextHolder;
import com.auction.notification.service.NotificationPersistenceService;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/notifications")
public class NotificationController {

    private final NotificationPersistenceService notificationService;

    public NotificationController(NotificationPersistenceService notificationService) {
        this.notificationService = notificationService;
    }

    @GetMapping
    public ResponseEntity<ApiResponse<Page<Notification>>> getNotifications(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        UserContext ctx = requireUserContext();
        Page<Notification> notifications = notificationService.getNotificationsByUser(
                ctx.getUserId(), PageRequest.of(page, size));
        return ResponseEntity.ok(ApiResponse.success(notifications));
    }

    @PutMapping("/{id}/read")
    public ResponseEntity<ApiResponse<Notification>> markAsRead(@PathVariable String id) {
        UserContext ctx = requireUserContext();
        Notification notification = notificationService.markAsRead(id, ctx.getUserId());
        return ResponseEntity.ok(ApiResponse.success(notification));
    }

    private UserContext requireUserContext() {
        UserContext ctx = UserContextHolder.get();
        if (ctx == null) {
            throw new IllegalStateException("User context not found. Ensure the gateway forwards user headers.");
        }
        return ctx;
    }
}

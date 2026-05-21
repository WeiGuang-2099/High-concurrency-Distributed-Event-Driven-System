CREATE DATABASE IF NOT EXISTS `auction_db` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE `auction_db`;

CREATE TABLE IF NOT EXISTS `auction` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `event_name` VARCHAR(200) NOT NULL,
    `description` VARCHAR(1000) DEFAULT NULL,
    `ticket_type_id` BIGINT DEFAULT NULL,
    `starting_price` DECIMAL(10, 2) NOT NULL,
    `current_highest_bid` DECIMAL(10, 2) DEFAULT NULL,
    `current_highest_bidder_id` BIGINT DEFAULT NULL,
    `status` ENUM('PENDING', 'ACTIVE', 'SETTLED', 'EXPIRED') NOT NULL DEFAULT 'PENDING',
    `start_time` DATETIME NOT NULL,
    `end_time` DATETIME NOT NULL,
    `winner_id` BIGINT DEFAULT NULL,
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (`id`),
    INDEX `idx_auction_status_start_time` (`status`, `start_time`),
    INDEX `idx_auction_status_end_time` (`status`, `end_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `bid` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `auction_id` BIGINT NOT NULL,
    `user_id` BIGINT NOT NULL,
    `username` VARCHAR(50) NOT NULL DEFAULT '',
    `amount` DECIMAL(10, 2) NOT NULL,
    `bid_time` DATETIME(3) NOT NULL,
    `event_id` VARCHAR(40) NOT NULL,
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_bid_event_id` (`event_id`),
    INDEX `idx_bid_auction_created` (`auction_id`, `created_at` DESC),
    INDEX `idx_bid_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Seata AT undo_log table
CREATE TABLE IF NOT EXISTS `undo_log` (
    `branch_id` BIGINT NOT NULL,
    `xid` VARCHAR(100) NOT NULL,
    `context` VARCHAR(128) NOT NULL,
    `rollback_info` LONGBLOB NOT NULL,
    `log_status` INT NOT NULL,
    `log_created` DATETIME NOT NULL,
    `log_modified` DATETIME NOT NULL,
    PRIMARY KEY (`branch_id`),
    INDEX `idx_xid` (`xid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE DATABASE IF NOT EXISTS `auction_db` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE `auction_db`;

CREATE TABLE IF NOT EXISTS `auction` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `event_name` VARCHAR(200) NOT NULL,
    `starting_price` DECIMAL(10, 2) NOT NULL,
    `current_highest_bid` DECIMAL(10, 2) DEFAULT NULL,
    `status` ENUM('ACTIVE', 'SETTLED', 'EXPIRED') NOT NULL DEFAULT 'ACTIVE',
    `start_time` DATETIME NOT NULL,
    `end_time` DATETIME NOT NULL,
    `winner_id` BIGINT DEFAULT NULL,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    INDEX `idx_auction_status_end_time` (`status`, `end_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `bid` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `auction_id` BIGINT NOT NULL,
    `user_id` BIGINT NOT NULL,
    `amount` DECIMAL(10, 2) NOT NULL,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    INDEX `idx_bid_auction_id` (`auction_id`),
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

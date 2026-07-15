CREATE DATABASE IF NOT EXISTS `ticket_db` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE `ticket_db`;

CREATE TABLE IF NOT EXISTS `ticket_stock` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `event_id` BIGINT NOT NULL,
    `ticket_type` VARCHAR(50) NOT NULL,
    `total_quantity` INT NOT NULL,
    `reserved_quantity` INT NOT NULL DEFAULT 0,
    `sold_quantity` INT NOT NULL DEFAULT 0,
    `version` INT NOT NULL DEFAULT 0,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    INDEX `idx_ticket_stock_event_id` (`event_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `reservation` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `stock_id` BIGINT NOT NULL,
    `user_id` BIGINT NOT NULL,
    `quantity` INT NOT NULL,
    `status` ENUM('PENDING', 'CONFIRMED', 'CANCELLED', 'EXPIRED') NOT NULL DEFAULT 'PENDING',
    `expire_at` DATETIME NOT NULL,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    INDEX `idx_reservation_stock_id` (`stock_id`),
    INDEX `idx_reservation_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Transactional outbox. Events are inserted here in the SAME transaction as the
-- business change, so the two commit or roll back together -- there is no window
-- where stock moved but the event never existed. OutboxRelay publishes the rows to
-- Kafka afterwards and stamps sent_at.
--
-- Contrast with the auction bid path (docs/adr/0005), which cannot use this table:
-- its accept decision is made in Redis and never opens a MySQL transaction, so it
-- uses a Redis Stream as its outbox instead.
CREATE TABLE IF NOT EXISTS `outbox` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    -- The event's own id, carried into Kafka so consumers dedupe on it after a
    -- relay retry (the relay is at-least-once, like every other publisher here).
    `event_id` VARCHAR(64) NOT NULL,
    `topic` VARCHAR(100) NOT NULL,
    `message_key` VARCHAR(100) NOT NULL,
    `event_type` VARCHAR(50) NOT NULL,
    -- Concrete event class. The payload carries no type information, so without this the
    -- relay could only deserialize to BaseEvent and would silently drop every subtype
    -- field; the consumers' __TypeId__ header comes from the object's real class.
    `payload_class` VARCHAR(200) NOT NULL,
    `payload` TEXT NOT NULL,
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    -- Claimed by a relay instance; a stale claim is retaken so a relay that dies
    -- mid-publish does not strand its batch (same role as XCLAIM on the bid outbox).
    `claimed_by` VARCHAR(64) NULL,
    `claimed_at` DATETIME(3) NULL,
    `sent_at` DATETIME(3) NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_outbox_event_id` (`event_id`),
    -- Drives the relay's poll: unsent rows in insertion order.
    INDEX `idx_outbox_unsent` (`sent_at`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

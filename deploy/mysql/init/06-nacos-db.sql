CREATE DATABASE IF NOT EXISTS `nacos_config` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE `nacos_config`;

-- Nacos config table
CREATE TABLE IF NOT EXISTS `config_info` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `data_id` VARCHAR(255) NOT NULL,
    `group_id` VARCHAR(128),
    `content` LONGTEXT NOT NULL,
    `md5` VARCHAR(32),
    `gmt_create` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `gmt_modified` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `src_user` VARCHAR(128),
    `src_ip` VARCHAR(50),
    `app_name` VARCHAR(128),
    `tenant_id` VARCHAR(128) DEFAULT '',
    `c_desc` VARCHAR(256),
    `c_use` VARCHAR(64),
    `effect` VARCHAR(64),
    `type` VARCHAR(32),
    `c_schema` TEXT,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_configinfo_datagrouptenant` (`data_id`, `group_id`, `tenant_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `config_info_aggr` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `data_id` VARCHAR(255) NOT NULL,
    `group_id` VARCHAR(128) NOT NULL,
    `datum_id` VARCHAR(255) NOT NULL,
    `content` LONGTEXT NOT NULL,
    `gmt_modified` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `app_name` VARCHAR(128),
    `tenant_id` VARCHAR(128) DEFAULT '',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_configinfoaggr_datagrouptenantdatum` (`data_id`, `group_id`, `tenant_id`, `datum_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `config_info_beta` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `data_id` VARCHAR(255) NOT NULL,
    `group_id` VARCHAR(128) NOT NULL,
    `app_name` VARCHAR(128),
    `content` LONGTEXT NOT NULL,
    `beta_ips` LONGTEXT,
    `md5` VARCHAR(32),
    `gmt_create` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `gmt_modified` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `src_user` VARCHAR(128),
    `src_ip` VARCHAR(50),
    `tenant_id` VARCHAR(128) DEFAULT '',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_configinfobeta_datagrouptenant` (`data_id`, `group_id`, `tenant_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `config_info_tag` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `data_id` VARCHAR(255) NOT NULL,
    `group_id` VARCHAR(128) NOT NULL,
    `tenant_id` VARCHAR(128) DEFAULT '',
    `tag_id` VARCHAR(128) NOT NULL,
    `app_name` VARCHAR(128),
    `content` LONGTEXT NOT NULL,
    `md5` VARCHAR(32),
    `gmt_create` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `gmt_modified` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `src_user` VARCHAR(128),
    `src_ip` VARCHAR(50),
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_configinfotag_datagrouptenanttag` (`data_id`, `group_id`, `tenant_id`, `tag_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `config_tags_relation` (
    `id` BIGINT NOT NULL,
    `tag_name` VARCHAR(128) NOT NULL,
    `tag_type` VARCHAR(64),
    `data_id` VARCHAR(255) NOT NULL,
    `group_id` VARCHAR(128) NOT NULL,
    `tenant_id` VARCHAR(128) DEFAULT '',
    `nid` BIGINT NOT NULL AUTO_INCREMENT,
    PRIMARY KEY (`nid`),
    UNIQUE KEY `uk_configtagrelation_configidtag` (`id`, `tag_name`, `tag_type`),
    INDEX `idx_tenant_id` (`tenant_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `group_capacity` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `group_id` VARCHAR(128) NOT NULL DEFAULT '',
    `quota` INT NOT NULL DEFAULT 0,
    `usage` INT NOT NULL DEFAULT 0,
    `max_size` INT NOT NULL DEFAULT 0,
    `max_aggr_count` INT NOT NULL DEFAULT 0,
    `max_aggr_size` INT NOT NULL DEFAULT 0,
    `max_history_count` INT NOT NULL DEFAULT 0,
    `gmt_create` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `gmt_modified` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_group_id` (`group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `his_config_info` (
    `id` BIGINT NOT NULL,
    `nid` BIGINT NOT NULL AUTO_INCREMENT,
    `data_id` VARCHAR(255) NOT NULL,
    `group_id` VARCHAR(128) NOT NULL,
    `app_name` VARCHAR(128),
    `content` LONGTEXT NOT NULL,
    `md5` VARCHAR(32),
    `gmt_create` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `gmt_modified` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `src_user` VARCHAR(128),
    `src_ip` VARCHAR(50),
    `op_type` CHAR(10),
    `tenant_id` VARCHAR(128) DEFAULT '',
    PRIMARY KEY (`nid`),
    INDEX `idx_gmt_create` (`gmt_create`),
    INDEX `idx_did` (`data_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `tenant_capacity` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `tenant_id` VARCHAR(128) NOT NULL DEFAULT '',
    `quota` INT NOT NULL DEFAULT 0,
    `usage` INT NOT NULL DEFAULT 0,
    `max_size` INT NOT NULL DEFAULT 0,
    `max_aggr_count` INT NOT NULL DEFAULT 0,
    `max_aggr_size` INT NOT NULL DEFAULT 0,
    `max_history_count` INT NOT NULL DEFAULT 0,
    `gmt_create` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `gmt_modified` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_tenant_id` (`tenant_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `tenant_info` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `kp` VARCHAR(128) NOT NULL,
    `tenant_id` VARCHAR(128) DEFAULT '',
    `tenant_name` VARCHAR(128) DEFAULT '',
    `tenant_desc` VARCHAR(256),
    `create_source` VARCHAR(32),
    `gmt_create` BIGINT NOT NULL,
    `gmt_modified` BIGINT NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_tenant_info_kptenantid` (`kp`, `tenant_id`),
    INDEX `idx_tenant_id` (`tenant_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `users` (
    `username` VARCHAR(50) NOT NULL,
    `password` VARCHAR(500) NOT NULL,
    `enabled` BOOLEAN NOT NULL,
    PRIMARY KEY (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `roles` (
    `username` VARCHAR(50) NOT NULL,
    `role` VARCHAR(50) NOT NULL,
    UNIQUE KEY `idx_user_role` (`username`, `role`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `permissions` (
    `role` VARCHAR(50) NOT NULL,
    `resource` VARCHAR(255) NOT NULL,
    `action` VARCHAR(8) NOT NULL,
    UNIQUE KEY `uk_role_permission` (`role`, `resource`, `action`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO `users` (`username`, `password`, `enabled`)
VALUES ('nacos', '$2a$10$EuWPZHzz32dJN7jexM34MOeYirDdFAZm2kuWj7VEOJhhZkDrxfvUu', TRUE)
ON DUPLICATE KEY UPDATE `password` = `password`;

INSERT INTO `roles` (`username`, `role`)
VALUES ('nacos', 'ROLE_ADMIN')
ON DUPLICATE KEY UPDATE `role` = `role`;

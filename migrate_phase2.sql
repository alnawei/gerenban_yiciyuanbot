SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
SET time_zone = '+00:00';

CREATE TABLE IF NOT EXISTS system_config (
    config_key VARCHAR(64) NOT NULL,
    config_value TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (config_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS categories (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    category_name VARCHAR(191) NOT NULL,
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_categories_category_name (category_name),
    KEY idx_categories_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS products (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    remote_code VARCHAR(191) NOT NULL,
    remote_name VARCHAR(255) NOT NULL,
    display_name VARCHAR(255) NULL,
    category_name VARCHAR(191) NOT NULL,
    last_sync_price DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    price DECIMAL(20,2) NULL,
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_products_remote_code (remote_code),
    KEY idx_products_category_active (category_name, is_active),
    KEY idx_products_active (is_active),
    CONSTRAINT chk_products_remote_code_nonempty CHECK (remote_code <> ''),
    CONSTRAINT chk_products_remote_name_nonempty CHECK (remote_name <> ''),
    CONSTRAINT chk_products_category_name_nonempty CHECK (category_name <> ''),
    CONSTRAINT chk_products_display_name_nonempty CHECK (
        display_name IS NULL OR display_name <> ''
    )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS users (
    tg_id BIGINT NOT NULL,
    balance DECIMAL(20,6) NOT NULL DEFAULT 0.000000,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tg_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS recharge_orders (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    user_id BIGINT NOT NULL,
    product_id BIGINT UNSIGNED NULL,
    order_type ENUM('purchase', 'recharge') NOT NULL DEFAULT 'purchase',
    amount DECIMAL(20,6) NOT NULL,
    final_amount DECIMAL(20,6) NOT NULL,
    status ENUM('pending', 'paid', 'fulfilled', 'failed', 'cancelled', 'expired')
        NOT NULL DEFAULT 'pending',
    tx_hash VARCHAR(191) NULL,
    expires_at DATETIME NOT NULL,
    paid_at DATETIME NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_orders_user_id (user_id),
    KEY idx_orders_product_id (product_id),
    KEY idx_orders_user_status (user_id, status),
    KEY idx_orders_status_expires (status, expires_at),
    KEY idx_orders_final_amount (final_amount),
    UNIQUE KEY uk_orders_tx_hash (tx_hash),
    CONSTRAINT chk_orders_tx_hash_nonempty CHECK (
        tx_hash IS NULL OR tx_hash <> ''
    )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS tx_records (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tx_hash VARCHAR(191) NOT NULL,
    order_id BIGINT UNSIGNED NULL,
    amount DECIMAL(20,6) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_tx_records_tx_hash (tx_hash),
    KEY idx_tx_records_order_id (order_id),
    KEY idx_tx_records_created_at (created_at),
    CONSTRAINT chk_tx_records_tx_hash_nonempty CHECK (tx_hash <> '')
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS delivery_records (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    order_id BIGINT UNSIGNED NOT NULL,
    product_id BIGINT UNSIGNED NOT NULL,
    status ENUM('success', 'failed') NOT NULL,
    content TEXT NULL,
    error_msg TEXT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_delivery_order (order_id),
    KEY idx_delivery_product_id (product_id),
    KEY idx_delivery_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO system_config (config_key, config_value)
VALUES ('tron_listener_min_ts', '0')
ON DUPLICATE KEY UPDATE config_value = config_value;

-- ============================================================
-- Phase 2 数据库迁移脚本 v3（完整可重复执行）
-- ============================================================

-- ── 1. products 表 ───────────────────────────────────────────
ALTER TABLE products
    MODIFY COLUMN last_sync_price DECIMAL(20, 6) DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS price DECIMAL(20, 6) DEFAULT NULL COMMENT '人工设置 USDT 售价';


-- ── 2. recharge_orders 主表 ─────────────────────────────────
--    新增：fulfillment_status（提货状态，与支付状态解耦）
CREATE TABLE IF NOT EXISTS recharge_orders (
    id                  BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    user_id             BIGINT NOT NULL,
    product_id          INT UNSIGNED NOT NULL,
    amount              DECIMAL(20, 6) NOT NULL       COMMENT '基础价格',
    final_amount        DECIMAL(20, 6) NOT NULL       COMMENT '含随机小数实付金额（唯一）',
    tx_hash             VARCHAR(128)   DEFAULT NULL   COMMENT '链上哈希',
    status              ENUM('pending','paid','fulfilled','expired','cancelled')
                        NOT NULL DEFAULT 'pending',
    fulfillment_status  ENUM('pending','processing','success','failed')
                        NOT NULL DEFAULT 'pending'    COMMENT '提货状态',
    paid_at             DATETIME       DEFAULT NULL,
    expires_at          DATETIME       NOT NULL       COMMENT '订单过期时间',
    created_at          DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY  uk_final_amount       (final_amount),
    INDEX       idx_status_expires    (status, expires_at),
    INDEX       idx_fulfillment_status(fulfillment_status),
    INDEX       idx_user_id           (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- 补丁：已存在旧表时追加字段
ALTER TABLE recharge_orders
    ADD COLUMN IF NOT EXISTS tx_hash            VARCHAR(128)  DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS paid_at            DATETIME      DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS expires_at         DATETIME      NOT NULL DEFAULT '2099-01-01 00:00:00',
    ADD COLUMN IF NOT EXISTS fulfillment_status ENUM('pending','processing','success','failed')
                                                NOT NULL DEFAULT 'pending',
    MODIFY COLUMN final_amount DECIMAL(20, 6) NOT NULL,
    MODIFY COLUMN amount       DECIMAL(20, 6) NOT NULL;

ALTER TABLE recharge_orders
    ADD UNIQUE  INDEX IF NOT EXISTS uk_final_amount        (final_amount),
    ADD         INDEX IF NOT EXISTS idx_status_expires     (status, expires_at),
    ADD         INDEX IF NOT EXISTS idx_fulfillment_status (fulfillment_status);


-- ── 3. 链上交易记录 ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tx_records (
    id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tx_hash    VARCHAR(128)   NOT NULL,
    order_id   BIGINT         DEFAULT NULL,
    amount     DECIMAL(20, 6) NOT NULL,
    created_at DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_tx_hash  (tx_hash),
    INDEX      idx_order   (order_id),
    INDEX      idx_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ── 4. 发货记录 ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS delivery_records (
    id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    order_id   BIGINT NOT NULL,
    product_id INT UNSIGNED NOT NULL,
    status     ENUM('success','failed') NOT NULL DEFAULT 'failed',
    content    MEDIUMTEXT   DEFAULT NULL,
    error_msg  VARCHAR(512) DEFAULT '',
    created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_order (order_id),
    INDEX      idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ── 5. system_config 默认行 ──────────────────────────────────
INSERT IGNORE INTO system_config (config_key, config_value) VALUES
    ('shop_url',         ''),
    ('app_id',           ''),
    ('app_key',          ''),
    ('customer_service', '');

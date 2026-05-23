-- ============================================================
-- Phase 2 数据库迁移脚本
-- 执行前请先备份数据库！
-- ============================================================

-- 1. 给 recharge_orders 补充 Phase 2 所需字段
--    （如果是全新建表请直接用下面的 CREATE TABLE）

ALTER TABLE recharge_orders
    ADD COLUMN IF NOT EXISTS tx_hash VARCHAR(128) DEFAULT NULL COMMENT '链上交易哈希',
    ADD COLUMN IF NOT EXISTS paid_at DATETIME DEFAULT NULL COMMENT '支付确认时间',
    ADD COLUMN IF NOT EXISTS status ENUM('pending','paid','fulfilled','expired','cancelled')
        NOT NULL DEFAULT 'pending' COMMENT '订单状态';

-- 如 recharge_orders 尚未创建，使用完整建表语句：
CREATE TABLE IF NOT EXISTS recharge_orders (
    id            BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    user_id       BIGINT NOT NULL               COMMENT '购买者 TG ID',
    product_id    INT UNSIGNED NOT NULL          COMMENT '商品 ID',
    amount        DECIMAL(12,2) NOT NULL         COMMENT '商品基础价格',
    final_amount  DECIMAL(12,4) NOT NULL         COMMENT '含随机小数的实付金额',
    tx_hash       VARCHAR(128) DEFAULT NULL      COMMENT '链上交易哈希',
    status        ENUM('pending','paid','fulfilled','expired','cancelled')
                  NOT NULL DEFAULT 'pending'     COMMENT '订单状态',
    paid_at       DATETIME DEFAULT NULL          COMMENT '支付确认时间',
    created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_final_amount (final_amount),   -- 防并发金额冲突
    INDEX idx_status (status),
    INDEX idx_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='充值订单表';


-- 2. 链上交易记录表（去重 & 审计）
CREATE TABLE IF NOT EXISTS tx_records (
    id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tx_hash    VARCHAR(128) NOT NULL  COMMENT '链上交易哈希',
    order_id   BIGINT DEFAULT NULL    COMMENT '关联订单（NULL=未匹配）',
    amount     DECIMAL(12,4) NOT NULL COMMENT '转账 USDT 金额',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_tx_hash (tx_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='链上交易记录';


-- 3. 发货记录表
CREATE TABLE IF NOT EXISTS delivery_records (
    id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    order_id   BIGINT NOT NULL         COMMENT '关联订单',
    product_id INT UNSIGNED NOT NULL   COMMENT '关联商品',
    status     ENUM('success','failed') NOT NULL DEFAULT 'failed',
    content    MEDIUMTEXT              COMMENT '提货内容原始数据',
    error_msg  VARCHAR(512) DEFAULT '' COMMENT '失败原因',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_order (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='发货记录';


-- 4. products 表补全 price 字段（如不存在）
ALTER TABLE products
    ADD COLUMN IF NOT EXISTS price DECIMAL(12,2) DEFAULT NULL COMMENT '人工设置的 USDT 售价';

-- 5. 确保 system_config 有必要的默认行
INSERT IGNORE INTO system_config (config_key, config_value) VALUES
    ('shop_url', ''),
    ('app_id', ''),
    ('app_key', ''),
    ('customer_service', '');

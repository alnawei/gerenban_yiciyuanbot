-- ============================================================
-- Phase 2 数据库迁移脚本（修复版）
-- 执行前请先备份！建议在 1Panel 数据库管理中执行。
-- ============================================================

-- ────────────────────────────────────────────────────────────
-- 1. products 表补全字段
-- ────────────────────────────────────────────────────────────
ALTER TABLE products
    MODIFY COLUMN last_sync_price DECIMAL(20, 6) DEFAULT NULL COMMENT '上游同步价格',
    ADD COLUMN IF NOT EXISTS price DECIMAL(20, 6) DEFAULT NULL COMMENT '人工设置的 USDT 售价';


-- ────────────────────────────────────────────────────────────
-- 2. 订单主表
--    关键设计：
--      final_amount  DECIMAL(20,6) UNIQUE  —— 唯一索引防金额冲突
--      expires_at    DATETIME              —— 30 分钟后过期释放坑位
--      tx_hash       VARCHAR(128)          —— 链上哈希，核销后写入
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS recharge_orders (
    id            BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    user_id       BIGINT NOT NULL                         COMMENT '购买者 TG ID',
    product_id    INT UNSIGNED NOT NULL                   COMMENT '商品 ID',
    amount        DECIMAL(20, 6) NOT NULL                 COMMENT '商品基础价格',
    final_amount  DECIMAL(20, 6) NOT NULL                 COMMENT '含随机小数的实付金额（唯一）',
    tx_hash       VARCHAR(128)   DEFAULT NULL             COMMENT '链上交易哈希',
    status        ENUM('pending','paid','fulfilled','expired','cancelled')
                  NOT NULL DEFAULT 'pending'              COMMENT '订单状态',
    paid_at       DATETIME       DEFAULT NULL             COMMENT '支付确认时间',
    expires_at    DATETIME       NOT NULL                 COMMENT '订单过期时间',
    created_at    DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- ⚠️  UNIQUE 约束核心：同一时刻只能有一个 pending 占用某个金额
    --    注意：expired/cancelled 的记录不占用此索引（MySQL UNIQUE 对 NULL 无限制）
    --    但我们的 final_amount 不为 NULL，所以只依赖过期清理来释放坑位
    UNIQUE KEY uk_final_amount (final_amount),

    INDEX idx_status_expires (status, expires_at),
    INDEX idx_user_id        (user_id),
    INDEX idx_created_at     (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='充值/购买订单表';


-- ────────────────────────────────────────────────────────────
-- 3. 链上交易记录（去重 + 审计）
--    tx_hash UNIQUE 保证同一笔链上交易绝不重复核销
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tx_records (
    id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tx_hash    VARCHAR(128) NOT NULL                     COMMENT '链上交易哈希',
    order_id   BIGINT       DEFAULT NULL                 COMMENT '关联订单（NULL=未匹配）',
    amount     DECIMAL(20, 6) NOT NULL                   COMMENT '转账 USDT 金额',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY uk_tx_hash   (tx_hash),
    INDEX      idx_order_id (order_id),
    INDEX      idx_created  (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='链上 TRC20 交易记录';


-- ────────────────────────────────────────────────────────────
-- 4. 发货记录表
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS delivery_records (
    id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    order_id   BIGINT NOT NULL                           COMMENT '关联订单',
    product_id INT UNSIGNED NOT NULL                     COMMENT '关联商品',
    status     ENUM('success','failed') NOT NULL DEFAULT 'failed',
    content    MEDIUMTEXT               DEFAULT NULL     COMMENT '提货内容原始数据（卡密等）',
    error_msg  VARCHAR(512)             DEFAULT ''       COMMENT '失败原因',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY uk_order_id (order_id),
    INDEX      idx_status  (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='发货记录';


-- ────────────────────────────────────────────────────────────
-- 5. system_config 默认行（幂等，可重复执行）
-- ────────────────────────────────────────────────────────────
INSERT IGNORE INTO system_config (config_key, config_value) VALUES
    ('shop_url',          ''),
    ('app_id',            ''),
    ('app_key',           ''),
    ('customer_service',  '');


-- ────────────────────────────────────────────────────────────
-- 6. 索引补丁（如果 recharge_orders 已存在旧版本，补加字段/索引）
-- ────────────────────────────────────────────────────────────
-- 补 expires_at 字段
ALTER TABLE recharge_orders
    ADD COLUMN IF NOT EXISTS expires_at DATETIME NOT NULL DEFAULT '2099-01-01 00:00:00'
        COMMENT '订单过期时间',
    ADD COLUMN IF NOT EXISTS tx_hash    VARCHAR(128) DEFAULT NULL
        COMMENT '链上交易哈希',
    ADD COLUMN IF NOT EXISTS paid_at    DATETIME DEFAULT NULL
        COMMENT '支付确认时间',
    MODIFY COLUMN final_amount DECIMAL(20, 6) NOT NULL,
    MODIFY COLUMN amount       DECIMAL(20, 6) NOT NULL;

-- 补 UNIQUE 索引（如已存在会报 Duplicate key name，忽略即可）
ALTER TABLE recharge_orders
    ADD UNIQUE INDEX IF NOT EXISTS uk_final_amount (final_amount);

-- 补联合索引（加速监听器查询）
ALTER TABLE recharge_orders
    ADD INDEX IF NOT EXISTS idx_status_expires (status, expires_at);

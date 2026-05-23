"""
database.py
数据库操作封装 — 包含 Phase 1 全量 + Phase 2 新增函数。
"""

import logging
from decimal import Decimal
import aiomysql
from config import DB_CONFIG

logger = logging.getLogger(__name__)

# ============================================================
# 连接池
# ============================================================

async def get_db_pool():
    return await aiomysql.create_pool(**DB_CONFIG, autocommit=True)


# ============================================================
# 系统配置
# ============================================================

async def get_system_config(pool) -> dict:
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT config_key, config_value FROM system_config")
            rows = await cur.fetchall()
            return {row["config_key"]: row["config_value"] for row in rows}


async def update_system_config(pool, url: str, app_id: str, app_key: str):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            sql = "UPDATE system_config SET config_value = %s WHERE config_key = %s"
            await cur.execute(sql, (url, "shop_url"))
            await cur.execute(sql, (app_id, "app_id"))
            await cur.execute(sql, (app_key, "app_key"))


# ============================================================
# 商品
# ============================================================

async def save_remote_products(pool, categories: list) -> int:
    count = 0
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            for cat in categories:
                cat_name = cat.get("name")
                for item in cat.get("children", []):
                    sql = """
                    INSERT INTO products (remote_code, remote_name, category_name, last_sync_price, is_active)
                    VALUES (%s, %s, %s, %s, 1)
                    ON DUPLICATE KEY UPDATE remote_name=%s, last_sync_price=%s
                    """
                    await cur.execute(
                        sql,
                        (
                            item["code"], item["name"], cat_name, item["price"],
                            item["name"], item["price"],
                        ),
                    )
                    count += 1
    return count


async def get_active_categories(pool) -> list:
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT category_name FROM products WHERE is_active=1 GROUP BY category_name"
            )
            return await cur.fetchall()


async def get_product_by_id(pool, product_id: int) -> dict | None:
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT id, remote_code, remote_name, display_name, price FROM products WHERE id=%s",
                (product_id,),
            )
            return await cur.fetchone()


async def get_shop_stats(pool) -> tuple[int, int]:
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT COUNT(*) as total FROM products")
            t = await cur.fetchone()
            await cur.execute("SELECT COUNT(*) as active FROM products WHERE is_active=1")
            a = await cur.fetchone()
            return t["total"], a["active"]


# ============================================================
# 用户
# ============================================================

async def get_user_data(pool, tg_id: int) -> dict:
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT balance FROM users WHERE tg_id = %s", (tg_id,))
            user = await cur.fetchone()
            if not user:
                await cur.execute(
                    "INSERT INTO users (tg_id, balance) VALUES (%s, 0)", (tg_id,)
                )
                return {"balance": 0.0}
            return user


async def get_user_balance(pool, tg_id: int) -> Decimal:
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT balance FROM users WHERE tg_id = %s", (tg_id,))
            user = await cur.fetchone()
            if not user:
                await cur.execute(
                    "INSERT INTO users (tg_id, balance) VALUES (%s, 0)", (tg_id,)
                )
                return Decimal("0")
            return Decimal(str(user["balance"]))


# ============================================================
# 订单 — Phase 1：创建订单（下单入口）
# ============================================================

async def create_order(
    pool,
    user_tg_id: int,
    product_id: int,
    base_amount: Decimal,
    final_amount: Decimal,
) -> int | None:
    """
    写入 recharge_orders，final_amount 有 UNIQUE 约束，
    冲突时返回 None（调用方需重新生成随机小数重试）。
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                await cur.execute(
                    """
                    INSERT INTO recharge_orders
                        (user_id, product_id, amount, final_amount, status)
                    VALUES (%s, %s, %s, %s, 'pending')
                    """,
                    (user_tg_id, product_id, base_amount, final_amount),
                )
                return cur.lastrowid
            except aiomysql.IntegrityError:
                return None  # final_amount 冲突，调用方重试


# ============================================================
# 订单 — Phase 2：核销相关
# ============================================================

async def find_pending_order_by_amount(pool, usdt_amount: Decimal) -> dict | None:
    """
    按 final_amount 精确匹配未支付订单。
    允许 ±0.001 的浮点误差（链上金额有时末位有细微偏差）。
    """
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT id, user_id, product_id, amount, final_amount
                FROM recharge_orders
                WHERE status = 'pending'
                  AND ABS(final_amount - %s) < 0.001
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (str(usdt_amount),),
            )
            return await cur.fetchone()


async def mark_order_paid(pool, order_id: int, tx_hash: str) -> bool:
    """
    将订单状态改为 paid，同时写入 tx_hash。
    使用乐观锁防止重复核销（仅当 status='pending' 时才更新）。
    返回 True 表示成功更新。
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE recharge_orders
                SET status = 'paid', tx_hash = %s, paid_at = NOW()
                WHERE id = %s AND status = 'pending'
                """,
                (tx_hash, order_id),
            )
            return cur.rowcount == 1


async def mark_order_fulfilled(pool, order_id: int):
    """提货成功后将订单标记为 fulfilled"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE recharge_orders SET status = 'fulfilled' WHERE id = %s",
                (order_id,),
            )


async def expire_stale_orders(pool, hours: int = 2):
    """将超时未支付的订单标记为 expired（定时任务调用）"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE recharge_orders
                SET status = 'expired'
                WHERE status = 'pending'
                  AND created_at < NOW() - INTERVAL %s HOUR
                """,
                (hours,),
            )
            expired = cur.rowcount
    if expired:
        logger.info("已过期订单: %d 条", expired)
    return expired


# ============================================================
# 交易哈希记录（去重）
# ============================================================

async def get_processed_tx_hashes(pool) -> list[str]:
    """启动时加载已处理的 tx_hash，防止重启后重复核销"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT tx_hash FROM tx_records")
            rows = await cur.fetchall()
            return [r[0] for r in rows]


async def record_tx_hash(
    pool,
    tx_hash: str,
    order_id: int | None,
    amount: Decimal,
):
    """记录链上交易，无论是否匹配订单"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                await cur.execute(
                    """
                    INSERT IGNORE INTO tx_records (tx_hash, order_id, amount)
                    VALUES (%s, %s, %s)
                    """,
                    (tx_hash, order_id, str(amount)),
                )
            except Exception as e:
                logger.warning("记录 tx_hash 失败: %s", e)


# ============================================================
# 提货记录
# ============================================================

async def save_delivery_record(
    pool,
    order_id: int,
    product_id: int,
    status: str,
    content: str,
    error_msg: str,
):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO delivery_records
                    (order_id, product_id, status, content, error_msg)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    status = VALUES(status),
                    content = VALUES(content),
                    error_msg = VALUES(error_msg)
                """,
                (order_id, product_id, status, content, error_msg),
            )

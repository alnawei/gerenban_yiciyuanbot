"""
database.py — 完整版 v3
本次新增：
  - set_fulfillment_status()：写入 recharge_orders.fulfillment_status
  - get_failed_fulfillment_orders()：管理员查看待补发列表
  - find_pending_order_by_amount 精度阈值统一为 0.000001
  - get_processed_tx_hashes 仅加载近 7 天记录
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
                    await cur.execute(sql, (
                        item["code"], item["name"], cat_name, str(item["price"]),
                        item["name"], str(item["price"]),
                    ))
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
                "SELECT id, remote_code, remote_name, display_name, price "
                "FROM products WHERE id=%s",
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
                return {"balance": Decimal("0")}
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
# 订单 — 创建
# ============================================================

async def create_order(
    pool,
    user_tg_id: int,
    product_id: int,
    base_amount: Decimal,
    final_amount: Decimal,
    expire_minutes: int = 30,
) -> int | None:
    """
    写入 recharge_orders。
    final_amount UNIQUE 冲突 → 返回 None，调用方重试。
    fulfillment_status 初始为 'pending'（未开始提货）。
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                await cur.execute(
                    """
                    INSERT INTO recharge_orders
                        (user_id, product_id, amount, final_amount,
                         status, fulfillment_status, expires_at)
                    VALUES (%s, %s, %s, %s, 'pending', 'pending',
                            DATE_ADD(NOW(), INTERVAL %s MINUTE))
                    """,
                    (
                        user_tg_id, product_id,
                        str(base_amount), str(final_amount),
                        expire_minutes,
                    ),
                )
                return cur.lastrowid
            except aiomysql.IntegrityError:
                return None


# ============================================================
# 订单 — 核销
# ============================================================

async def find_pending_order_by_amount(pool, usdt_amount: Decimal) -> dict | None:
    """
    精确匹配未过期 pending 订单。
    精度阈值 < 0.000001（等价于整数比较，USDT 链上 6 位小数）。
    同时排除 expires_at <= NOW() 的订单，过期金额不核销。
    """
    amount_str = str(usdt_amount.quantize(Decimal("0.000001")))
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT id, user_id, product_id, amount, final_amount
                FROM recharge_orders
                WHERE status = 'pending'
                  AND expires_at > NOW()
                  AND ABS(final_amount - %s) < 0.000001
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (amount_str,),
            )
            return await cur.fetchone()


async def mark_order_paid(pool, order_id: int, tx_hash: str) -> bool:
    """乐观锁：仅 status='pending' 时成功，防二次触发。"""
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
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE recharge_orders SET status = 'fulfilled' WHERE id = %s",
                (order_id,),
            )


async def expire_stale_orders(pool) -> int:
    """将 expires_at 已过的 pending 订单标记为 expired，释放坑位。"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE recharge_orders
                SET status = 'expired'
                WHERE status = 'pending' AND expires_at <= NOW()
                """
            )
            expired = cur.rowcount
    if expired:
        logger.info("清理过期订单: %d 条", expired)
    return expired


# ============================================================
# 订单 — fulfillment_status（提货状态独立字段）
# ============================================================

async def set_fulfillment_status(pool, order_id: int, status: str):
    """
    单独更新提货状态，与支付状态解耦。
    status 取值：pending / processing / success / failed
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE recharge_orders SET fulfillment_status = %s WHERE id = %s",
                (status, order_id),
            )


async def get_failed_fulfillment_orders(pool, limit: int = 20) -> list:
    """查询发货失败的订单，供管理员面板展示。"""
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT r.id, r.user_id, r.product_id, r.final_amount,
                       r.paid_at, p.remote_name, p.display_name,
                       d.error_msg
                FROM recharge_orders r
                LEFT JOIN products p ON p.id = r.product_id
                LEFT JOIN delivery_records d ON d.order_id = r.id
                WHERE r.fulfillment_status = 'failed'
                ORDER BY r.paid_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            return await cur.fetchall()


# ============================================================
# 交易哈希去重
# ============================================================

async def get_processed_tx_hashes(pool) -> list[str]:
    """加载近 7 天 tx_hash，防重启后重复核销，避免全表扫描。"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT tx_hash FROM tx_records "
                "WHERE created_at >= NOW() - INTERVAL 7 DAY"
            )
            rows = await cur.fetchall()
            return [r[0] for r in rows]


async def record_tx_hash(pool, tx_hash: str, order_id: int | None, amount: Decimal):
    """记录链上交易，INSERT IGNORE 保证幂等。"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                await cur.execute(
                    "INSERT IGNORE INTO tx_records (tx_hash, order_id, amount) "
                    "VALUES (%s, %s, %s)",
                    (tx_hash, order_id, str(amount)),
                )
            except Exception as e:
                logger.warning("record_tx_hash 失败: %s", e)


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
                    status    = VALUES(status),
                    content   = VALUES(content),
                    error_msg = VALUES(error_msg)
                """,
                (order_id, product_id, status, content, error_msg),
            )

"""
database.py — 完整版，修复所有已知问题
修复点：
  - find_pending_order_by_amount 过滤已过期订单
  - expire_stale_orders 改用 expires_at 字段（精确到分钟）
  - create_order 写入 expires_at
  - DECIMAL(20,6) 统一精度，金额全程字符串传参
  - get_processed_tx_hashes 只加载近 7 天记录，防内存溢出
"""

import logging
from decimal import Decimal, ROUND_DOWN

import aiomysql

from config import DB_CONFIG

logger = logging.getLogger(__name__)

AMOUNT_QUANT = Decimal("0.000001")


def normalize_amount(amount: Decimal) -> Decimal:
    """与 migrate_phase2 DECIMAL(20,6) 对齐。"""
    return Decimal(str(amount)).quantize(AMOUNT_QUANT, rounding=ROUND_DOWN)

# ============================================================
# 连接池（只在 main.py 启动时调用一次）
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


async def credit_user_balance(pool, tg_id: int, amount: Decimal) -> None:
    """充值到账：增加用户余额（使用订单基础金额 amount，不含随机尾数）。"""
    amount_str = str(normalize_amount(amount))
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT tg_id FROM users WHERE tg_id = %s", (tg_id,)
            )
            if not await cur.fetchone():
                await cur.execute(
                    "INSERT INTO users (tg_id, balance) VALUES (%s, %s)",
                    (tg_id, amount_str),
                )
                return
            await cur.execute(
                "UPDATE users SET balance = balance + %s WHERE tg_id = %s",
                (amount_str, tg_id),
            )


# ============================================================
# 订单 — 创建（唯一金额 + 过期时间）
# ============================================================

async def pending_amount_exists(pool, final_amount: Decimal) -> bool:
    """status='pending' 且未过期的金额唯一性预检（与 uk_final_amount 双保险）。"""
    amount_str = str(normalize_amount(final_amount))
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT 1 FROM recharge_orders
                WHERE status = 'pending'
                  AND expires_at > NOW()
                  AND ABS(final_amount - %s) < 0.000001
                LIMIT 1
                """,
                (amount_str,),
            )
            return await cur.fetchone() is not None


async def create_order(
    pool,
    user_tg_id: int,
    base_amount: Decimal,
    final_amount: Decimal,
    *,
    order_type: str = "purchase",
    product_id: int | None = None,
    expire_minutes: int = 30,
) -> int | None:
    """
    写入 recharge_orders（DECIMAL(20,6)）。
    插入前再次 pending 预检；final_amount UNIQUE 冲突返回 None。
    """
    base_str = str(normalize_amount(base_amount))
    final_str = str(normalize_amount(final_amount))

    if await pending_amount_exists(pool, final_amount):
        return None

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                await cur.execute(
                    """
                    INSERT INTO recharge_orders
                        (user_id, product_id, order_type, amount, final_amount,
                         status, expires_at)
                    VALUES (%s, %s, %s, %s, %s, 'pending',
                            DATE_ADD(NOW(), INTERVAL %s MINUTE))
                    """,
                    (
                        user_tg_id,
                        product_id,
                        order_type,
                        base_str,
                        final_str,
                        expire_minutes,
                    ),
                )
                return cur.lastrowid
            except aiomysql.IntegrityError:
                return None


async def get_order_by_id(pool, order_id: int) -> dict | None:
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT id, user_id, product_id, order_type, amount, final_amount, status
                FROM recharge_orders WHERE id = %s
                """,
                (order_id,),
            )
            return await cur.fetchone()


# ============================================================
# 订单 — 核销（监听器使用）
# ============================================================

async def find_pending_order_by_amount(pool, usdt_amount: Decimal) -> dict | None:
    """
    按 final_amount 精确匹配"未过期的 pending"订单。

    精度策略：
      - USDT TRC20 链上精度 6 位，我们存 DECIMAL(20,6)
      - 用 ABS(final_amount - X) < 0.000001 做精确比对（等价于整数比较）
      - 不接受模糊匹配，避免误核销

    同时过滤 expires_at > NOW()，确保已超时的金额坑位不会被匹配。
    """
    # 先把链上金额截断到 6 位小数，再转字符串传参
    amount_str = str(normalize_amount(usdt_amount))
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT id, user_id, product_id, order_type, amount, final_amount
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
    """
    乐观锁更新：仅当 status='pending' 时才成功，
    防止监听器在同一笔 tx 上二次触发。
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
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE recharge_orders SET status = 'fulfilled' WHERE id = %s",
                (order_id,),
            )


async def expire_stale_orders(pool) -> list[Decimal]:
    """
    将 expires_at 已过、仍处于 pending 的订单标记为 expired。
    返回被过期订单的 final_amount 列表，供释放 Redis 锁。
    """
    amounts: list[Decimal] = []
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT final_amount FROM recharge_orders
                WHERE status = 'pending' AND expires_at <= NOW()
                """
            )
            rows = await cur.fetchall()
            amounts = [Decimal(str(r["final_amount"])) for r in rows]
            if not amounts:
                return []
            await cur.execute(
                """
                UPDATE recharge_orders
                SET status = 'expired'
                WHERE status = 'pending'
                  AND expires_at <= NOW()
                """
            )
    if amounts:
        logger.info("清理过期订单: %d 条", len(amounts))
    return amounts


# ============================================================
# 交易哈希去重
# ============================================================

async def get_processed_tx_hashes(pool) -> list[str]:
    """
    启动时加载近 7 天的已处理 tx_hash 到内存，
    防止重启后重复核销，同时避免全表扫描。
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT tx_hash FROM tx_records "
                "WHERE created_at >= NOW() - INTERVAL 7 DAY"
            )
            rows = await cur.fetchall()
            return [r[0] for r in rows]


async def record_tx_hash(
    pool,
    tx_hash: str,
    order_id: int | None,
    amount: Decimal,
):
    """记录链上交易，无论是否匹配订单，INSERT IGNORE 保证幂等。"""
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

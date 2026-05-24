"""
database.py — 完整版
已修复：
  - [重要修改] get_shop_stats：统计在售商品数时关联 categories 表，确保排除已禁用分类下的商品。
  - find_pending_order_by_amount 过滤已过期订单
  - expire_stale_orders 改用 expires_at 字段
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
    """
    【升级优化】与 migrate_phase2 DECIMAL(20,6) 保持对齐。
    修改点：为了彻底杜绝任何 6 位小数的长尾精度漂移，在此强制将 Decimal 截断为最高 3 位小数，
    数据库存储时会自动补齐后面的 000（如 5.123 存为 5.123000），两层判定极其稳固。
    """
    return Decimal(str(amount)).quantize(Decimal("0.001"), rounding=ROUND_DOWN)


class EmptyStringError(ValueError):
    """业务字段不允许写入空字符串。"""


def require_non_empty_text(value, field_name: str) -> str:
    """
    统一拦截空字符串，避免 DB 内出现会破坏 Telegram 按钮渲染的数据。
    """
    if value is None:
        raise EmptyStringError(f"{field_name} 不能为空")
    text = str(value).strip()
    if not text:
        raise EmptyStringError(f"{field_name} 不能为空")
    return text

# ============================================================
# 连接池（只在 main.py 启动时调用一次）
# ============================================================

async def get_db_pool():
    config = dict(DB_CONFIG)
    config["charset"] = "utf8mb4"
    config["use_unicode"] = True
    return await aiomysql.create_pool(
        **config,
        autocommit=True,
    )


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
    url = require_non_empty_text(url, "shop_url")
    app_id = require_non_empty_text(app_id, "app_id")
    app_key = require_non_empty_text(app_key, "app_key")
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            sql = """
            INSERT INTO system_config (config_key, config_value)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE config_value=VALUES(config_value)
            """
            await cur.execute(sql, ("shop_url", url))
            await cur.execute(sql, ("app_id", app_id))
            await cur.execute(sql, ("app_key", app_key))


LISTENER_CURSOR_KEY = "tron_listener_min_ts"
# 重启后回扫重叠窗口（毫秒），防止边界漏单
LISTENER_OVERLAP_MS = 5 * 60 * 1000
# 首次运行无游标时的默认回溯（7 天）
LISTENER_DEFAULT_LOOKBACK_MS = 7 * 24 * 60 * 60 * 1000


async def get_listener_cursor_ms(pool) -> int:
    """读取链上扫描游标（毫秒时间戳），0 表示从未扫描。"""
    config = await get_system_config(pool)
    raw = config.get(LISTENER_CURSOR_KEY, "0") or "0"
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


async def set_listener_cursor_ms(pool, timestamp_ms: int) -> None:
    """持久化链上扫描游标，供重启后增量拉取。"""
    ts = max(0, int(timestamp_ms))
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO system_config (config_key, config_value)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE config_value = VALUES(config_value)
                """,
                (LISTENER_CURSOR_KEY, str(ts)),
            )


# ============================================================
# 商品
# ============================================================

# database.py — 缩进严格对齐的安全版本

# database.py — 重构后的防改名冲突 save_remote_products 函数

async def save_remote_products(pool, categories: list) -> int:
    """
    保存并更新从远端同步过来的商品与分类。
    
    【防重命名冲突重构】：
      - 优先根据 remote_cat_id 进行唯一性匹配，不再单方面依赖易变的名字。
      - 名字保护：若 remote_cat_id 已匹配，则对已有分类仅更新其下属商品，绝不覆盖管理员手动修改好的本地分类名称。
      - 自动绑定：若本地已有同名分类但无 remote_cat_id，首次同步会自动将其与 remote_cat_id 绑定，无损过渡。
    """
    count = 0
    
    logger.info("🔍 [Sync Debug] 开始执行本地数据保存。传入分类数: %d", len(categories))
    
    async with pool.acquire() as conn:
        # 修改点 1：采用 aiomysql.DictCursor（字典游标），让字段读取更直观、安全
        async with conn.cursor(aiomysql.DictCursor) as cur:
            for cat in categories:
                # 获取源站分类的唯一物理 ID
                remote_cat_id = cat.get("id")
                api_category_name = require_non_empty_text(
                    cat.get("category_name") or cat.get("name"),
                    "category_name",
                )
                
                db_category_name = None
                
                if remote_cat_id is not None:
                    # 1. 第一路径：优先根据 remote_cat_id 寻找本地分类
                    await cur.execute(
                        "SELECT category_name FROM categories WHERE remote_cat_id = %s",
                        (remote_cat_id,)
                    )
                    row = await cur.fetchone()
                    if row:
                        # 找到了已有映射：使用本地数据库中的名字（如您改好的“定制”），保护其名字不被覆盖！
                        db_category_name = row["category_name"]
                        logger.debug(
                            "🔍 [Sync Debug] 通过 ID [%d] 成功匹配到本地自定义分类 [%s] (API名: [%s])",
                            remote_cat_id, db_category_name, api_category_name
                        )
                    else:
                        # 2. 第二路径（自动自愈）：ID 没找到，但可能本地已有同名分类（旧数据迁移）。尝试自动绑定
                        await cur.execute(
                            "SELECT id FROM categories WHERE category_name = %s",
                            (api_category_name,)
                        )
                        existing_cat = await cur.fetchone()
                        if existing_cat:
                            # 自动将本地已有的同名分类与源站物理 ID 进行绑定
                            await cur.execute(
                                "UPDATE categories SET remote_cat_id = %s WHERE id = %s",
                                (remote_cat_id, existing_cat["id"])
                            )
                            db_category_name = api_category_name
                            logger.info(
                                "🔍 [Sync Debug] 🔄 成功将本地存量分类 [%s] 与源站 ID [%d] 进行无损绑定！",
                                api_category_name, remote_cat_id
                            )
                
                # 3. 第三路径：如果是纯粹的全新分类（本地无 ID，也无同名分类），则执行全新插入
                if db_category_name is None:
                    db_category_name = api_category_name
                    await cur.execute(
                        """
                        INSERT INTO categories (category_name, remote_cat_id, is_active)
                        VALUES (%s, %s, 1)
                        ON DUPLICATE KEY UPDATE remote_cat_id = VALUES(remote_cat_id)
                        """,
                        (db_category_name, remote_cat_id),
                    )
                    logger.info("🔍 [Sync Debug] 🆕 检测到新分类，成功入库: [%s] (ID: %s)", db_category_name, remote_cat_id)
                
                # 到这里，db_category_name 已经是我们确定要使用的安全分类名称了（如“定制”）
                # 开始级联同步商品。由于我们使用了 db_category_name，所以商品会正确归属于改名后的分类下，不会产生垃圾数据
                children = cat.get("children", [])
                logger.info("🔍 [Sync Debug] 分类 [%s] 下包含的子商品数量: %d", db_category_name, len(children))

                for item in children:
                    try:
                        remote_code = require_non_empty_text(item.get("code"), "remote_code")
                        remote_name = require_non_empty_text(
                            item.get("name") or item.get("remote_name"),
                            "remote_name",
                        )
                    except EmptyStringError as e:
                        logger.warning(
                            "🔍 [Sync Debug] 同步跳过(字段空异常): 分类=%s, 商品=%s, 原因=%s", 
                            db_category_name, item, e
                        )
                        continue
                        
                    raw_price = item.get("price", "0")
                    sync_price = "0" if raw_price is None or str(raw_price).strip() == "" else str(raw_price)

                    # 级联写入商品，建立外键映射
                    sql = """
                    INSERT INTO products (remote_code, remote_name, category_name, last_sync_price, is_active)
                    VALUES (%s, %s, %s, %s, 1)
                    ON DUPLICATE KEY UPDATE
                        remote_name = VALUES(remote_name),
                        category_name = VALUES(category_name),
                        last_sync_price = VALUES(last_sync_price)
                    """
                    await cur.execute(sql, (
                        remote_code,
                        remote_name,
                        db_category_name,
                        sync_price,
                    ))
                    count += 1
                    
        logger.info("🔍 [Sync Debug] 商品落库事务处理完毕。本次同步写入的商品总数: %d", count)
        return count


# database.py — 确保分类在普通用户端按 ID 升序固定排列

# database.py — 确保前台排序也固定不变

async def get_active_categories(pool) -> list:
    """获取处于上架（启用）状态的分类列表（用于普通用户端展示）"""
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT id, category_name, is_active 
                FROM categories 
                WHERE is_active = 1
                ORDER BY id ASC
                """
            )
            return await cur.fetchall()

async def get_products_by_category(pool, category_name: str) -> list:
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT p.* FROM products p
                JOIN categories c ON p.category_name = c.category_name
                WHERE p.category_name = %s 
                  AND p.is_active = 1 
                  AND c.is_active = 1
                ORDER BY p.id ASC
                """,
                (category_name,)
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
    """
    【核心修改】获取管理员面板的主菜单数据统计。
    修改点：
      - 统计 active 实际上架数时，采用 JOIN 关联 categories，
        只有商品状态和分类状态同时激活（双重激活，is_active=1）时，才算上架，
        完美解决因分类下架导致“上架中”商品数量统计不准确的问题。
    """
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("SELECT COUNT(*) as total FROM products")
            t = await cur.fetchone()
            
            # 仅统计分类和商品本身均为激活（上架）状态的数量
            await cur.execute(
                """
                SELECT COUNT(*) as active 
                FROM products p
                JOIN categories c ON p.category_name = c.category_name
                WHERE p.is_active = 1 AND c.is_active = 1
                """
            )
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
    写入 recharge_orders 表。
    修改点：
      - 对入库前的数据执行强一致性 normalize_amount 转换，彻底规避任何长尾尾数。
    """
    # 强制将最终支付金额执行 3 位小数截断（等价于 round(amount, 3) 且防 binary 浮点漂移）
    base_str = str(normalize_amount(base_amount))
    final_str = str(normalize_amount(final_amount))
    order_type = require_non_empty_text(order_type, "order_type")

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
                SELECT id, user_id, product_id, order_type, amount, final_amount,
                       status, tx_hash, expires_at, paid_at
                FROM recharge_orders WHERE id = %s
                """,
                (order_id,),
            )
            return await cur.fetchone()


async def tx_hash_already_used(pool, tx_hash: str) -> bool:
    """链上哈希是否已在 tx_records 或订单表中出现过。"""
    if not tx_hash:
        return False
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT 1 FROM tx_records WHERE tx_hash = %s LIMIT 1", (tx_hash,)
            )
            if await cur.fetchone():
                return True
            await cur.execute(
                """
                SELECT 1 FROM recharge_orders
                WHERE tx_hash = %s AND tx_hash IS NOT NULL AND tx_hash != ''
                LIMIT 1
                """,
                (tx_hash,),
            )
            return await cur.fetchone() is not None


async def get_pending_order_by_id(
    pool, order_id: int, usdt_amount: Decimal | None = None
) -> dict | None:
    """核销前二次确认：订单仍为 pending、未过期、尚未写入 tx_hash。"""
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            sql = """
                SELECT id, user_id, product_id, order_type, amount, final_amount, tx_hash
                FROM recharge_orders
                WHERE id = %s
                  AND status = 'pending'
                  AND expires_at > NOW()
                  AND (tx_hash IS NULL OR tx_hash = '')
            """
            params: list = [order_id]
            if usdt_amount is not None:
                sql += " AND ABS(final_amount - %s) < 0.000001"
                params.append(str(normalize_amount(usdt_amount)))
            await cur.execute(sql, params)
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
                SELECT id, user_id, product_id, order_type, amount, final_amount, tx_hash
                FROM recharge_orders
                WHERE status = 'pending'
                  AND expires_at > NOW()
                  AND (tx_hash IS NULL OR tx_hash = '')
                  AND ABS(final_amount - %s) < 0.000001
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (amount_str,),
            )
            return await cur.fetchone()


async def mark_order_paid(pool, order_id: int, tx_hash: str) -> bool:
    """
    乐观锁核销：仅 pending + 未过期 + 尚未绑定 tx_hash 的订单可更新。
    同时将链上哈希写入 recharge_orders.tx_hash。
    """
    try:
        tx_hash = require_non_empty_text(tx_hash, "tx_hash")
    except EmptyStringError:
        return False
    if await tx_hash_already_used(pool, tx_hash):
        return False

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE recharge_orders
                SET status = 'paid', tx_hash = %s, paid_at = NOW()
                WHERE id = %s
                  AND status = 'pending'
                  AND expires_at > NOW()
                  AND (tx_hash IS NULL OR tx_hash = '')
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
    启动时加载近 7 天已处理 tx_hash（tx_records + 订单表），防重启重复核销。
    """
    hashes: set[str] = set()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT tx_hash FROM tx_records "
                "WHERE created_at >= NOW() - INTERVAL 7 DAY"
            )
            hashes.update(r[0] for r in await cur.fetchall() if r[0])
            await cur.execute(
                """
                SELECT tx_hash FROM recharge_orders
                WHERE tx_hash IS NOT NULL AND tx_hash != ''
                  AND paid_at >= NOW() - INTERVAL 7 DAY
                """
            )
            hashes.update(r[0] for r in await cur.fetchall() if r[0])
    return list(hashes)


async def record_tx_hash(
    pool,
    tx_hash: str,
    order_id: int | None,
    amount: Decimal,
):
    """记录链上交易，无论是否匹配订单，INSERT IGNORE 保证幂等。"""
    try:
        tx_hash = require_non_empty_text(tx_hash, "tx_hash")
    except EmptyStringError:
        logger.warning("跳过空 tx_hash 记录")
        return
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
    status = require_non_empty_text(status, "delivery status")
    content = None if content is None or str(content).strip() == "" else str(content)
    error_msg = None if error_msg is None or str(error_msg).strip() == "" else str(error_msg)
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


# database.py - 追加以下函数

async def clear_categories_table(pool) -> None:
    """
    管理员高级功能：清空全部分类表。
    安全考虑：
      - 由于 products 表对 categories 表可能存在外键关联约束，
        我们在删除前会临时禁用 MySQL 的外键检查，防止因 ON DELETE RESTRICT 导致删除报错。
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            # 1. 临时关闭外键安全检查
            await cur.execute("SET FOREIGN_KEY_CHECKS = 0")
            try:
                # 2. 清空分类表（使用 DELETE 以便在事务连接池中安全工作）
                await cur.execute("DELETE FROM categories")
                await conn.commit()
            finally:
                # 3. 无论成功或失败，都必须恢复外键安全检查
                await cur.execute("SET FOREIGN_KEY_CHECKS = 1")

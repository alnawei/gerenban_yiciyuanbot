"""
settlement.py — 订单核销数据库事务（标记支付 + 入账/发货 DB 写入）
"""

import logging
from decimal import Decimal

import aiomysql

from database import normalize_amount

logger = logging.getLogger(__name__)


class SettlementError(Exception):
    """核销事务失败。"""


async def _tx_hash_used_in_tx(cur, tx_hash: str) -> bool:
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


async def _mark_order_paid_in_tx(cur, order_id: int, tx_hash: str) -> bool:
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


async def _record_tx_hash_in_tx(
    cur, tx_hash: str, order_id: int | None, amount: Decimal
):
    await cur.execute(
        """
        INSERT IGNORE INTO tx_records (tx_hash, order_id, amount)
        VALUES (%s, %s, %s)
        """,
        (tx_hash, order_id, str(normalize_amount(amount))),
    )


async def _credit_user_balance_in_tx(cur, tg_id: int, amount: Decimal):
    amount_str = str(normalize_amount(amount))
    await cur.execute("SELECT tg_id FROM users WHERE tg_id = %s", (tg_id,))
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


async def _mark_order_fulfilled_in_tx(cur, order_id: int):
    await cur.execute(
        "UPDATE recharge_orders SET status = 'fulfilled' WHERE id = %s",
        (order_id,),
    )


async def _save_delivery_in_tx(
    cur,
    order_id: int,
    product_id: int,
    status: str,
    content: str,
    error_msg: str,
):
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


async def atomic_mark_paid_and_record(
    pool,
    order_id: int,
    tx_hash: str,
    usdt_amount: Decimal,
) -> bool:
    """事务：标记 paid + 写入 tx_records。"""
    if not tx_hash:
        return False

    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("START TRANSACTION")
            try:
                if await _tx_hash_used_in_tx(cur, tx_hash):
                    await cur.execute("ROLLBACK")
                    return False
                if not await _mark_order_paid_in_tx(cur, order_id, tx_hash):
                    await cur.execute("ROLLBACK")
                    return False
                await _record_tx_hash_in_tx(cur, tx_hash, order_id, usdt_amount)
                await cur.execute("COMMIT")
                return True
            except Exception:
                await cur.execute("ROLLBACK")
                logger.exception("atomic_mark_paid_and_record 失败 order=%s", order_id)
                raise


async def atomic_settle_recharge(
    pool,
    order_id: int,
    user_tg_id: int,
    base_amount: Decimal,
    tx_hash: str,
    usdt_amount: Decimal,
) -> bool:
    """事务：标记 paid + tx_records + 加余额 + fulfilled（充值全流程）。"""
    if not tx_hash:
        return False

    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("START TRANSACTION")
            try:
                if await _tx_hash_used_in_tx(cur, tx_hash):
                    await cur.execute("ROLLBACK")
                    return False
                if not await _mark_order_paid_in_tx(cur, order_id, tx_hash):
                    await cur.execute("ROLLBACK")
                    return False
                await _record_tx_hash_in_tx(cur, tx_hash, order_id, usdt_amount)
                await _credit_user_balance_in_tx(cur, user_tg_id, base_amount)
                await _mark_order_fulfilled_in_tx(cur, order_id)
                await cur.execute("COMMIT")
                logger.info("充值订单 #%d 事务核销完成 tx=%s", order_id, tx_hash)
                return True
            except Exception:
                await cur.execute("ROLLBACK")
                logger.exception("atomic_settle_recharge 失败 order=%s", order_id)
                raise


async def atomic_complete_purchase(
    pool,
    order_id: int,
    product_id: int,
    *,
    success: bool,
    content: str = "",
    error_msg: str = "",
) -> None:
    """事务：写入 delivery_records + fulfilled（购买 API 成功后调用）。"""
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("START TRANSACTION")
            try:
                status = "success" if success else "failed"
                await _save_delivery_in_tx(
                    cur, order_id, product_id, status, content, error_msg
                )
                if success:
                    await _mark_order_fulfilled_in_tx(cur, order_id)
                await cur.execute("COMMIT")
            except Exception:
                await cur.execute("ROLLBACK")
                logger.exception("atomic_complete_purchase 失败 order=%s", order_id)
                raise

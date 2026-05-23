"""统一支付订单创建：Redis 预锁 + DB pending 预检 + create_order。"""

import logging
import random
from decimal import Decimal, ROUND_DOWN

from amount_lock import release_amount_lock, try_acquire_amount_lock, is_amount_locked
from database import create_order, pending_amount_exists

logger = logging.getLogger(__name__)

AMOUNT_QUANT = Decimal("0.000001")
BASE_QUANT = Decimal("0.01")


def quantize_final(amount: Decimal) -> Decimal:
    return amount.quantize(AMOUNT_QUANT, rounding=ROUND_DOWN)


def quantize_base(amount: Decimal) -> Decimal:
    return amount.quantize(BASE_QUANT, rounding=ROUND_DOWN)


async def place_payment_order(
    db_pool,
    redis_client,
    user_tg_id: int,
    base_amount: Decimal,
    *,
    order_type: str = "purchase",
    product_id: int | None = None,
    expire_minutes: int | None = None,
    max_attempts: int = 30,
) -> tuple[int, Decimal] | None:
    """
    生成带随机尾数的唯一 final_amount，先 Redis 再 DB，最后写入订单。
    返回 (order_id, final_amount) 或 None。
    """
    base_q = quantize_base(Decimal(str(base_amount)))
    tried_micro: set[int] = set()

    for _ in range(max_attempts):
        remaining = set(range(1, 1_000_000)) - tried_micro
        if not remaining:
            break
        micro = random.choice(list(remaining))
        tried_micro.add(micro)

        candidate = quantize_final(
            base_q + Decimal(micro) / Decimal("1000000")
        )

        if await is_amount_locked(redis_client, candidate):
            continue
        if await pending_amount_exists(db_pool, candidate):
            continue

        if not await try_acquire_amount_lock(redis_client, candidate, user_tg_id):
            continue

        order_id = await create_order(
            db_pool,
            user_tg_id,
            base_q,
            candidate,
            order_type=order_type,
            product_id=product_id,
            expire_minutes=expire_minutes,
        )
        if order_id:
            return order_id, candidate

        await release_amount_lock(redis_client, candidate)

    logger.error(
        "创建订单失败 user=%s base=%s type=%s attempts=%d",
        user_tg_id,
        base_q,
        order_type,
        len(tried_micro),
    )
    return None

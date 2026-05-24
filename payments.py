"""
payments.py — 优化重构版（大幅降低高并发下 MySQL 预检开销）
"""

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
    max_attempts: int = 300,
) -> tuple[int, Decimal] | None:
    base_q = quantize_base(Decimal(str(base_amount)))

    offsets = [Decimal("0.00")]

    phase1_offsets = [Decimal(str(i)) / Decimal("100") for i in range(1, 30)]
    random.shuffle(phase1_offsets)
    offsets.extend(phase1_offsets)

    phase2_offsets = [
        Decimal(str(i)) / Decimal("1000")
        for i in range(1, 300)
        if i % 10 != 0
    ]
    random.shuffle(phase2_offsets)
    offsets.extend(phase2_offsets)

    actual_max_attempts = min(len(offsets), max_attempts)

    for attempt in range(actual_max_attempts):
        offset = offsets[attempt]
        candidate = (base_q + offset).quantize(Decimal("0.001"), rounding=ROUND_DOWN)

        # ⭐️ 1. 第一重快速筛查：在 Redis 中查找键（内存操作，极快，且不查数据库）
        if await is_amount_locked(redis_client, candidate):
            continue

        # ⭐️ 2. 第二重抢锁：直接尝试原子加锁（SETNX 并发防碰撞）
        if not await try_acquire_amount_lock(redis_client, candidate, user_tg_id):
            continue

        # ⭐️ 3. 第三重兜底：加锁成功后，再查一次 MySQL。
        # 由于 99.9% 的订单在 Redis 中就有锁，这个查 MySQL 动作一般只会在找到空闲坑位后执行 1 次。
        if await pending_amount_exists(db_pool, candidate):
            # 若数据库中极罕见地存在（由于 Redis 崩溃重启导致锁消失，但 MySQL 依旧有 pending 数据）
            # 则退回该锁，寻找下一个。
            await release_amount_lock(redis_client, candidate)
            continue

        # 4. 写入数据库，生成订单
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

        # 若极罕见情况下写库失败（比如唯一哈希冲突等），释放刚才获得的 Redis 锁
        await release_amount_lock(redis_client, candidate)

    logger.error(
        "创建订单失败（同价位并发坑位已全部满载） user=%s base=%s type=%s",
        user_tg_id,
        base_q,
        order_type,
    )
    return None

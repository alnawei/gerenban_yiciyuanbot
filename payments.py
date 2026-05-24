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


# payments.py 中的完整修改函数

async def place_payment_order(
    db_pool,
    redis_client,
    user_tg_id: int,
    base_amount: Decimal,
    *,
    order_type: str = "purchase",
    product_id: int | None = None,
    expire_minutes: int | None = None,
    max_attempts: int = 300,  # 增大尝试上限，以支持遍历完最多 300 个可用金额坑位
) -> tuple[int, Decimal] | None:
    """
    【重构版】分级防碰撞金额生成算法（最高 3 位小数精度）
    
    业务流程：
      1. 优先尝试无偏移价格（原价，如 5.00）。
      2. 若原价已被占用，第一阶段尝试 2 位小数偏移（0.01 ~ 0.29，打乱顺序防并发碰撞，容量 30）。
      3. 若两级已满，第二阶段自动升级到 3 位小数（0.001 ~ 0.299，打乱顺序，排除整分金额以去重，容量 270）。
      4. 双通道验证（先 Redis 分布式锁，后数据库未过期 pending 订单预检）。
      5. 锁定后写入订单，若因极罕见的 DB 唯一键冲突则释放 Redis 锁并自动尝试下一个候选偏移量。
    """
    # 1. 基础金额处理：确保原价规整到两位小数（如 5.00）
    base_q = quantize_base(Decimal(str(base_amount)))

    # 2. 阶梯式构造候选偏移量列表
    # 优先第一位：0.00（无偏移，即原价优先）
    offsets = [Decimal("0.00")]

    # 第一阶段候选：2位小数偏移（0.01 至 0.29），打乱顺序散列并发
    phase1_offsets = [Decimal(str(i)) / Decimal("100") for i in range(1, 30)]
    random.shuffle(phase1_offsets)
    offsets.extend(phase1_offsets)

    # 第二阶段候选：3位小数偏移（0.001 至 0.299，排除已在第一阶段出现的 0.010, 0.020 等整分金额）
    phase2_offsets = [
        Decimal(str(i)) / Decimal("1000")
        for i in range(1, 300)
        if i % 10 != 0
    ]
    random.shuffle(phase2_offsets)
    offsets.extend(phase2_offsets)

    # 3. 动态限制最大尝试次数，不超过候选池的最大总深度（300）
    actual_max_attempts = min(len(offsets), max_attempts)

    for attempt in range(actual_max_attempts):
        offset = offsets[attempt]
        # 计算候选实付金额（通过 Decimal 限制为最高 3 位小数，彻底绝迹 5.666901 等长尾）
        candidate = (base_q + offset).quantize(Decimal("0.001"), rounding=ROUND_DOWN)

        # 4. 第一重预检：检查 Redis 锁中是否已存在该金额的坑位
        if await is_amount_locked(redis_client, candidate):
            continue
            
        # 5. 第二重预检：检查数据库中是否有状态为 pending 且未过期的相同实付金额
        if await pending_amount_exists(db_pool, candidate):
            continue

        # 6. 预检通过后，尝试抢占 Redis 金额锁
        if not await try_acquire_amount_lock(redis_client, candidate, user_tg_id):
            continue

        # 7. 写入数据库，生成订单
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
            # 订单成功落地，直接返回
            return order_id, candidate

        # 8. 容错处理：若因极罕见的 DB 锁或瞬时并发在写库时产生唯一哈希冲突，立即释放 Redis 锁并尝试下一个可用金额
        await release_amount_lock(redis_client, candidate)

    logger.error(
        "创建订单失败（同价位 300 个并发坑位已全部满载） user=%s base=%s type=%s",
        user_tg_id,
        base_q,
        order_type,
    )
    return None

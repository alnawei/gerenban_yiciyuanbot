"""Redis 金额坑位锁：在 DB 写入前防止并发占用同一 final_amount。"""

from decimal import Decimal

from config import ORDER_EXPIRE_MINUTES

LOCK_PREFIX = "amount_lock:"


def amount_lock_key(final_amount: Decimal) -> str:
    normalized = str(final_amount.quantize(Decimal("0.000001")))
    return f"{LOCK_PREFIX}{normalized}"


def amount_lock_ttl_seconds() -> int:
    return ORDER_EXPIRE_MINUTES * 60


async def try_acquire_amount_lock(redis_client, final_amount: Decimal, user_tg_id: int) -> bool:
    key = amount_lock_key(final_amount)
    return bool(
        await redis_client.set(
            key,
            str(user_tg_id),
            nx=True,
            ex=amount_lock_ttl_seconds(),
        )
    )


async def release_amount_lock(redis_client, final_amount: Decimal) -> None:
    await redis_client.delete(amount_lock_key(final_amount))


async def is_amount_locked(redis_client, final_amount: Decimal) -> bool:
    return bool(await redis_client.exists(amount_lock_key(final_amount)))

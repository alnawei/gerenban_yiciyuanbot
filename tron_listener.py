"""
tron_listener.py — TronGrid USDT(TRC20) 监听器
- 从 DB 读取/更新扫描游标，重启不漏单
- 核销走 settlement 事务
"""

import asyncio
import logging
import time

import aiohttp
from decimal import Decimal, ROUND_DOWN

from database import (
    LISTENER_DEFAULT_LOOKBACK_MS,
    LISTENER_OVERLAP_MS,
    find_pending_order_by_amount,
    get_listener_cursor_ms,
    get_pending_order_by_id,
    get_processed_tx_hashes,
    record_tx_hash,
    set_listener_cursor_ms,
    tx_hash_already_used,
)

logger = logging.getLogger(__name__)

USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
TRONGRID_API = "https://api.trongrid.io"

FETCH_LIMIT = 200
MIN_POLL_INTERVAL = 2


def sun_to_usdt(sun_value) -> Decimal:
    return (Decimal(str(sun_value)) / Decimal("1000000")).quantize(
        Decimal("0.000001"), rounding=ROUND_DOWN
    )


def _tx_block_timestamp_ms(tx: dict) -> int:
    """从 TronGrid TRC20 记录提取 block_timestamp（毫秒）。"""
    for key in ("block_timestamp", "blockTimestamp"):
        raw = tx.get(key)
        if raw is not None:
            try:
                return int(raw)
            except (TypeError, ValueError):
                pass
    return int(time.time() * 1000)


class TronListener:
    def __init__(
        self,
        wallet_address: str,
        db_pool,
        bot,
        redis_client=None,
        trongrid_api_key: str = "",
        poll_interval: int = 15,
    ):
        self.wallet = wallet_address
        self.db_pool = db_pool
        self.bot = bot
        self.redis = redis_client
        self.api_key = trongrid_api_key
        self.poll_interval = max(poll_interval, MIN_POLL_INTERVAL)
        self._seen_tx: set[str] = set()
        self._fail_count: int = 0
        self._scan_min_ts: int = 0

    async def start(self):
        logger.info(
            "🔗 TronListener 启动 | 地址: %s | 轮询: %ds",
            self.wallet,
            self.poll_interval,
        )
        await self._load_seen_tx()
        await self._init_scan_cursor()
        while True:
            try:
                await self._poll()
                self._fail_count = 0
            except aiohttp.ClientResponseError as exc:
                if exc.status == 429:
                    wait = min(60, 5 * (2 ** self._fail_count))
                    logger.warning("TronGrid 限速 429，%ds 后重试", wait)
                    self._fail_count += 1
                    await asyncio.sleep(wait)
                    continue
                logger.error("HTTP 错误 %d: %s", exc.status, exc, exc_info=True)
                self._fail_count += 1
            except Exception as exc:
                logger.error("TronListener 轮询异常: %s", exc, exc_info=True)
                self._fail_count += 1

            await asyncio.sleep(self.poll_interval)

    async def _load_seen_tx(self):
        hashes = await get_processed_tx_hashes(self.db_pool)
        self._seen_tx = set(hashes)
        logger.info("已加载历史 tx_hash: %d 条", len(self._seen_tx))

    async def _init_scan_cursor(self):
        """从 DB 恢复游标，并回退重叠窗口以防重启漏单。"""
        stored = await get_listener_cursor_ms(self.db_pool)
        now_ms = int(time.time() * 1000)
        if stored > 0:
            self._scan_min_ts = max(0, stored - LISTENER_OVERLAP_MS)
            logger.info(
                "链上扫描游标: DB=%d → 实际 min_ts=%d (重叠 %ds)",
                stored,
                self._scan_min_ts,
                LISTENER_OVERLAP_MS // 1000,
            )
        else:
            self._scan_min_ts = max(0, now_ms - LISTENER_DEFAULT_LOOKBACK_MS)
            logger.info(
                "首次扫描，回溯 %d 小时",
                LISTENER_DEFAULT_LOOKBACK_MS // 3600000,
            )

    async def _poll(self):
        transfers, max_ts = await self._fetch_trc20_transfers_since(self._scan_min_ts)
        if not transfers:
            return

        processed_max = self._scan_min_ts
        for tx in transfers:
            tx_hash = tx.get("transaction_id", "").strip()
            if not tx_hash or tx_hash in self._seen_tx:
                ts = _tx_block_timestamp_ms(tx)
                processed_max = max(processed_max, ts)
                continue

            to_addr = tx.get("to_address", tx.get("to", "")).strip()
            from_addr = tx.get("from_address", tx.get("from", "")).strip()

            if to_addr.lower() != self.wallet.lower():
                self._seen_tx.add(tx_hash)
                ts = _tx_block_timestamp_ms(tx)
                processed_max = max(processed_max, ts)
                continue

            raw_value = tx.get("value", "0")
            usdt_amount = sun_to_usdt(raw_value)
            ts = _tx_block_timestamp_ms(tx)

            logger.info(
                "📥 转入 %s USDT | hash=%s | from=%s | ts=%s",
                usdt_amount,
                tx_hash,
                from_addr,
                ts,
            )

            self._seen_tx.add(tx_hash)
            await self._on_transfer(tx_hash, usdt_amount, from_addr)
            processed_max = max(processed_max, ts)

        if max_ts > 0:
            processed_max = max(processed_max, max_ts)

        if processed_max > self._scan_min_ts:
            self._scan_min_ts = processed_max + 1
            await set_listener_cursor_ms(self.db_pool, processed_max)
            logger.debug("游标已更新: %d", processed_max)

    async def _fetch_trc20_transfers_since(
        self, min_timestamp_ms: int
    ) -> tuple[list[dict], int]:
        """
        按 min_timestamp 增量拉取 TRC20 转入（支持 fingerprint 分页）。
        返回 (交易列表, 本批最大 block_timestamp)。
        """
        url = f"{TRONGRID_API}/v1/accounts/{self.wallet}/transactions/trc20"
        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["TRON-PRO-API-KEY"] = self.api_key

        all_rows: list[dict] = []
        max_ts = 0
        fingerprint = None

        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(headers=headers) as session:
            while True:
                params = {
                    "contract_address": USDT_CONTRACT,
                    "only_to": "true",
                    "limit": FETCH_LIMIT,
                    "order_by": "block_timestamp,asc",
                    "min_timestamp": str(max(0, min_timestamp_ms)),
                }
                if fingerprint:
                    params["fingerprint"] = fingerprint

                async with session.get(url, params=params, timeout=timeout) as resp:
                    if resp.status == 429:
                        raise aiohttp.ClientResponseError(
                            resp.request_info, resp.history, status=429
                        )
                    if resp.status != 200:
                        logger.warning("TronGrid 非 200: %d", resp.status)
                        break
                    data = await resp.json(content_type=None)

                batch = data.get("data", [])
                for tx in batch:
                    ts = _tx_block_timestamp_ms(tx)
                    max_ts = max(max_ts, ts)
                all_rows.extend(batch)

                fingerprint = (data.get("meta") or {}).get("fingerprint")
                if not fingerprint or len(batch) < FETCH_LIMIT:
                    break

        return all_rows, max_ts

    async def _on_transfer(
        self,
        tx_hash: str,
        usdt_amount: Decimal,
        from_addr: str,
    ):
        from fulfillment import (
            fulfill_purchase_after_paid,
            notify_payment_received,
            notify_recharge_completed,
        )
        from settlement import atomic_mark_paid_and_record, atomic_settle_recharge

        if await tx_hash_already_used(self.db_pool, tx_hash):
            logger.info("跳过已处理 tx: %s", tx_hash)
            return

        order = await find_pending_order_by_amount(self.db_pool, usdt_amount)
        if not order:
            logger.info(
                "⚠️  %s USDT 无有效 pending 订单 tx=%s",
                usdt_amount,
                tx_hash,
            )
            await record_tx_hash(self.db_pool, tx_hash, None, usdt_amount)
            return

        order_id = order["id"]
        order = await get_pending_order_by_id(self.db_pool, order_id, usdt_amount)
        if not order:
            logger.warning("订单 #%d 二次校验失败 tx=%s", order_id, tx_hash)
            await record_tx_hash(self.db_pool, tx_hash, None, usdt_amount)
            return

        user_tg_id = order["user_id"]
        product_id = order.get("product_id")
        order_type = order.get("order_type", "purchase")
        base_amount = order["amount"]
        final_amount = order["final_amount"]

        logger.info(
            "✅ 匹配 #%d | user=%d | type=%s | tx=%s",
            order_id,
            user_tg_id,
            order_type,
            tx_hash,
        )

        try:
            if order_type == "recharge":
                ok = await atomic_settle_recharge(
                    self.db_pool,
                    order_id,
                    user_tg_id,
                    base_amount,
                    tx_hash,
                    usdt_amount,
                )
                if not ok:
                    logger.warning("充值事务核销失败 #%d tx=%s", order_id, tx_hash)
                    await record_tx_hash(self.db_pool, tx_hash, None, usdt_amount)
                    return

                if self.redis and final_amount is not None:
                    from amount_lock import release_amount_lock
                    await release_amount_lock(self.redis, Decimal(str(final_amount)))

                await notify_recharge_completed(
                    self.bot,
                    self.db_pool,
                    user_tg_id,
                    order_id,
                    base_amount,
                )
            else:
                ok = await atomic_mark_paid_and_record(
                    self.db_pool, order_id, tx_hash, usdt_amount
                )
                if not ok:
                    logger.warning("支付事务核销失败 #%d tx=%s", order_id, tx_hash)
                    await record_tx_hash(self.db_pool, tx_hash, None, usdt_amount)
                    return

                await notify_payment_received(
                    self.bot, user_tg_id, order_id, usdt_amount, tx_hash
                )

                await fulfill_purchase_after_paid(
                    db_pool=self.db_pool,
                    bot=self.bot,
                    order_id=order_id,
                    user_tg_id=user_tg_id,
                    product_id=product_id,
                    tx_hash=tx_hash,
                    final_amount=final_amount,
                    redis_client=self.redis,
                )
        except Exception as e:
            logger.error("核销事务异常 #%d: %s", order_id, e, exc_info=True)
            from notifications import format_fulfillment_alert, notify_admin

            await notify_admin(
                self.bot,
                format_fulfillment_alert(
                    order_id=order_id,
                    user_tg_id=user_tg_id,
                    order_type=order_type,
                    reason=f"核销事务异常: {e}",
                    tx_hash=tx_hash,
                    final_amount=final_amount,
                ),
            )

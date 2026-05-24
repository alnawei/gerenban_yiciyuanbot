"""
tron_listener.py — TronGrid USDT(TRC20) 监听器
- 物理热加载：每次轮询先读取 DB 中的最新 usdt_wallet 与 API Key，0秒无缝切换钱包 [1]。
- 安全防崩溃：如果 DB 与 env 全为空，发出红字警报挂起轮询；无 Key 模式运行时友情提醒 [1]。
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
MAX_RETRY_DELAY = 300


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
        
        # 内部动态绑定的运行时变量
        self.active_wallet = wallet_address
        self.active_key = trongrid_api_key

    async def get_db_config(self) -> dict:
        """从数据库读取最新系统配置的防火墙封装，保障在 DB 连接繁忙时不影响监听循环"""
        try:
            from database import get_system_config
            return await get_system_config(self.db_pool)
        except Exception as e:
            logger.warning("🔍 [TronListener] 链上监听器读取 DB 配置发生轻微阻碍（可能数据库正忙），本轮采用本地配置: %s", e)
            return {}

    async def start(self):
        logger.info(
            "🔗 TronListener 启动 | 缺省监听地址: %s | 默认轮询: %ds",
            self.wallet,
            self.poll_interval,
        )
        await self._load_seen_tx()
        await self._init_scan_cursor()
        while True:
            try:
                # ── 【物理热加载】：在每次轮询的第一现场，首先从数据库动态获取最新配置 ────────────────
                config = await self.get_db_config()
                
                db_wallet = config.get("usdt_wallet", "").strip()
                db_key = config.get("trongrid_api_key", "").strip()
                
                # 优先采用数据库最新的配置，如果没有，则降级采用启动时的初始环境配置 [1]
                active_wallet = db_wallet or self.wallet
                active_key = db_key or self.api_key
                
                # 防御安全校验：若两个渠道皆为空，抛出高危警告并原地挂起，保护程序不发生雪崩 [1]
                if not active_wallet:
                    logger.error("❌ [TronListener] 致命配置空：未检测到任何 TRC-20 收款地址。监听器挂起中。请在后台面板绑定或设置 USDT_WALLET")
                    await asyncio.sleep(self.poll_interval)
                    continue
                
                if not active_key:
                    logger.warning("⚠️ [TronListener] 当前未配置 API Key，将以较低频次无密钥模式运行。强烈建议前往管理后台设置 TronGrid Key 降低限流风险 [1]")

                # 动态覆盖当前正在运行的活动地址与 Key
                self.active_wallet = active_wallet
                self.active_key = active_key

                await self._poll()
                self._fail_count = 0
                await asyncio.sleep(self.poll_interval)
            except aiohttp.ClientResponseError as exc:
                wait = self._next_retry_delay()
                logger.warning(
                    "TronGrid HTTP %d，%ds 后重试: %s",
                    exc.status,
                    wait,
                    exc.message,
                )
                await asyncio.sleep(wait)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                wait = self._next_retry_delay()
                logger.warning("TronGrid 请求异常，%ds 后重试: %s", wait, exc)
                await asyncio.sleep(wait)
            except Exception as exc:
                wait = self._next_retry_delay()
                logger.error("TronListener 轮询异常，%ds 后重试: %s", wait, exc, exc_info=True)
                await asyncio.sleep(wait)

    def _next_retry_delay(self) -> int:
        wait = min(MAX_RETRY_DELAY, self.poll_interval * (2 ** self._fail_count))
        self._fail_count += 1
        return max(MIN_POLL_INTERVAL, wait)

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
        # 采用当前动态热加载绑定的 active_wallet 参与账目轮询
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

            # 采用当前动态热加载绑定的 active_wallet 执行钱包归属校验
            if to_addr.lower() != self.active_wallet.lower():
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
        # 采用当前动态绑定的活动钱包地址与 Key 构建 API 请求头
        url = f"{TRONGRID_API}/v1/accounts/{self.active_wallet}/transactions/trc20"
        headers = {"Accept": "application/json"}
        if self.active_key:
            headers["TRON-PRO-API-KEY"] = self.active_key

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
                    if resp.status != 200:
                        body = await resp.text()
                        raise aiohttp.ClientResponseError(
                            resp.request_info,
                            resp.history,
                            status=resp.status,
                            message=body[:300],
                            headers=resp.headers,
                        )
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

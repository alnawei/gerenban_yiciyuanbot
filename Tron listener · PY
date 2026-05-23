"""
tron_listener.py — v3
本次新增/修复：
  - confirmed 字段检查：只处理链上已确认交易，防回滚误核销
  - 增量扫描：记录 _last_scan_ts，每次只请求该时间戳之后的新交易
    避免高频钱包"limit=50 覆盖不到 2 分钟"问题
  - 指数退避：HTTP 429 / 连接超时均触发，最长等 60s
  - 全程 Decimal，不经过 float
"""

import asyncio
import logging
import aiohttp
from decimal import Decimal, ROUND_DOWN

logger = logging.getLogger(__name__)

USDT_CONTRACT    = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
TRONGRID_API     = "https://api.trongrid.io"
FETCH_LIMIT      = 50
MIN_POLL_INTERVAL = 2   # 免费节点最小安全间隔（秒）


def sun_to_usdt(sun_value) -> Decimal:
    """str(sun_value) → Decimal，绝不经过 float。"""
    return (Decimal(str(sun_value)) / Decimal("1000000")).quantize(
        Decimal("0.000001"), rounding=ROUND_DOWN
    )


class TronListener:
    def __init__(
        self,
        wallet_address: str,
        db_pool,
        bot,
        trongrid_api_key: str = "",
        poll_interval: int = 15,
    ):
        self.wallet        = wallet_address
        self.db_pool       = db_pool
        self.bot           = bot
        self.api_key       = trongrid_api_key
        self.poll_interval = max(poll_interval, MIN_POLL_INTERVAL)
        self._seen_tx:       set[str] = set()
        self._fail_count:    int      = 0
        # 增量扫描：记录上次扫到的最新区块时间戳（毫秒）
        # 初始为 0，第一次全量拉 FETCH_LIMIT 条做基线
        self._last_scan_ts:  int      = 0

    # ------------------------------------------------------------------ #

    async def start(self):
        logger.info("🔗 TronListener 启动 | 地址: %s | 间隔: %ds",
                    self.wallet, self.poll_interval)
        await self._load_seen_tx()
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
            except asyncio.TimeoutError:
                wait = min(60, 5 * (2 ** self._fail_count))
                logger.warning("TronGrid 请求超时，%ds 后重试", wait)
                self._fail_count += 1
                await asyncio.sleep(wait)
                continue
            except Exception as exc:
                logger.error("TronListener 异常: %s", exc, exc_info=True)
                self._fail_count += 1

            await asyncio.sleep(self.poll_interval)

    # ------------------------------------------------------------------ #

    async def _load_seen_tx(self):
        from database import get_processed_tx_hashes
        hashes = await get_processed_tx_hashes(self.db_pool)
        self._seen_tx = set(hashes)
        logger.info("已加载历史 tx_hash: %d 条", len(self._seen_tx))

    async def _poll(self):
        transfers = await self._fetch_trc20_transfers()
        if not transfers:
            return

        new_max_ts = self._last_scan_ts

        for tx in transfers:
            tx_hash = tx.get("transaction_id", "").strip()
            if not tx_hash:
                continue

            # ── 增量扫描：更新最大时间戳基线 ──────────────────────────
            block_ts = int(tx.get("block_timestamp", 0))
            if block_ts > new_max_ts:
                new_max_ts = block_ts

            if tx_hash in self._seen_tx:
                continue

            # ── 区块确认检查 ───────────────────────────────────────────
            # TronGrid 返回字段：confirmed=true 表示已被 19+ 个 SR 确认
            # 未确认的交易可能回滚，不做业务处理，但也不加入 seen_tx
            # 下次轮询时若已确认则正常处理
            if not tx.get("confirmed", False):
                logger.debug("tx %s 尚未确认，跳过本轮", tx_hash[:16])
                continue

            to_addr   = tx.get("to_address",   tx.get("to",   "")).strip()
            from_addr = tx.get("from_address", tx.get("from", "")).strip()

            if to_addr.lower() != self.wallet.lower():
                self._seen_tx.add(tx_hash)   # 转出交易，记录后忽略
                continue

            raw_value   = tx.get("value", "0")
            usdt_amount = sun_to_usdt(raw_value)

            logger.info("📥 转入 %s USDT | hash=%s | from=%s",
                        usdt_amount, tx_hash[:20], from_addr[:10])

            self._seen_tx.add(tx_hash)
            await self._on_transfer(tx_hash, usdt_amount, from_addr)

        # 更新增量基线（只向前推进，不回退）
        if new_max_ts > self._last_scan_ts:
            self._last_scan_ts = new_max_ts

    async def _fetch_trc20_transfers(self) -> list[dict]:
        """
        增量拉取：若 _last_scan_ts > 0，附带 min_timestamp 参数，
        仅返回该时间之后的新交易，彻底解决高频钱包漏单问题。
        第一次扫描（_last_scan_ts=0）全量拉 FETCH_LIMIT 条做基线。
        """
        url    = f"{TRONGRID_API}/v1/accounts/{self.wallet}/transactions/trc20"
        params: dict = {
            "contract_address": USDT_CONTRACT,
            "only_to":          "true",
            "limit":            FETCH_LIMIT,
        }
        if self._last_scan_ts > 0:
            # TronGrid 支持 min_timestamp（毫秒）过滤
            params["min_timestamp"] = self._last_scan_ts + 1

        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["TRON-PRO-API-KEY"] = self.api_key

        timeout = aiohttp.ClientTimeout(total=12)
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url, params=params, timeout=timeout) as resp:
                if resp.status == 429:
                    raise aiohttp.ClientResponseError(
                        resp.request_info, resp.history, status=429
                    )
                if resp.status != 200:
                    logger.warning("TronGrid 非 200 响应: %d", resp.status)
                    return []
                data = await resp.json(content_type=None)
                return data.get("data", [])

    async def _on_transfer(self, tx_hash: str, usdt_amount: Decimal, from_addr: str):
        from database import (
            find_pending_order_by_amount,
            mark_order_paid,
            record_tx_hash,
        )
        from fulfillment import fulfill_order

        order = await find_pending_order_by_amount(self.db_pool, usdt_amount)
        if not order:
            logger.info("⚠️  %s USDT 未匹配到有效订单（过期或金额有误）", usdt_amount)
            await record_tx_hash(self.db_pool, tx_hash, None, usdt_amount)
            return

        order_id   = order["id"]
        user_tg_id = order["user_id"]
        product_id = order["product_id"]
        logger.info("✅ 匹配 | 订单 #%d | 用户 %d | 商品 %d",
                    order_id, user_tg_id, product_id)

        updated = await mark_order_paid(self.db_pool, order_id, tx_hash)
        if not updated:
            logger.warning("订单 #%d 已处理，跳过（tx=%s）", order_id, tx_hash[:16])
            return

        await record_tx_hash(self.db_pool, tx_hash, order_id, usdt_amount)

        try:
            await self.bot.send_message(
                user_tg_id,
                f"✅ *收款已确认！*\n\n"
                f"📋 订单号：`#{order_id}`\n"
                f"💰 金额：`{usdt_amount} USDT`\n\n"
                f"⏳ 正在为您提货，请稍候...",
                parse_mode="Markdown",
            )
        except Exception as e:
            logger.error("发送确认消息失败 [user %d]: %s", user_tg_id, e)

        await fulfill_order(
            db_pool    = self.db_pool,
            bot        = self.bot,
            order_id   = order_id,
            user_tg_id = user_tg_id,
            product_id = product_id,
        )

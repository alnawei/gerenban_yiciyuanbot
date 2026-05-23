"""
tron_listener.py — 修复版
修复点：
  1. 轮询间隔 >= 2s，防 TronGrid 限速（免费档 3 req/s）
  2. 每次拉取 50 条记录（非 1 条），防短时多笔漏单
  3. 金额转换全程 Decimal + 字符串，零浮点精度丢失
  4. 只匹配未过期 pending 订单（由 DB 层保证）
  5. 已处理 tx 写入内存 set + DB，重启不重复核销
  6. API 限速时指数退避，不无限快速重试
"""

import asyncio
import logging
import aiohttp
from decimal import Decimal, ROUND_DOWN

logger = logging.getLogger(__name__)

# USDT TRC20 合约地址（TRON 主网）
USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
TRONGRID_API  = "https://api.trongrid.io"

# 每次从 TronGrid 拉取的最大条数（最大 200，建议 50）
FETCH_LIMIT = 50

# 免费 API 最小请求间隔（秒）：3 req/s → 至少 0.34s，取 2s 留足余量
MIN_POLL_INTERVAL = 2


def sun_to_usdt(sun_value) -> Decimal:
    """
    链上 value 字段单位为"最小精度单位"，USDT TRC20 精度 6 位。
    1 USDT = 1_000_000 sun。
    全程使用字符串 → Decimal，不经过 float。
    """
    return (Decimal(str(sun_value)) / Decimal("1000000")).quantize(
        Decimal("0.000001"), rounding=ROUND_DOWN
    )


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
        self.wallet        = wallet_address
        self.db_pool       = db_pool
        self.bot           = bot
        self.redis         = redis_client
        self.api_key       = trongrid_api_key
        # 实际使用值取配置值与最小值中的较大者，防止配置过小触发限速
        self.poll_interval = max(poll_interval, MIN_POLL_INTERVAL)
        # 内存去重集合，启动时从 DB 加载
        self._seen_tx: set[str] = set()
        # 连续失败次数，用于指数退避
        self._fail_count: int = 0

    # ------------------------------------------------------------------ #
    # 公共入口
    # ------------------------------------------------------------------ #

    async def start(self):
        logger.info("🔗 TronListener 启动 | 地址: %s | 轮询间隔: %ds",
                    self.wallet, self.poll_interval)
        await self._load_seen_tx()
        while True:
            try:
                await self._poll()
                self._fail_count = 0                        # 成功后重置
            except aiohttp.ClientResponseError as exc:
                if exc.status == 429:                       # 被限速
                    wait = min(60, 5 * (2 ** self._fail_count))
                    logger.warning("TronGrid 限速 429，%ds 后重试", wait)
                    self._fail_count += 1
                    await asyncio.sleep(wait)
                    continue
                else:
                    logger.error("HTTP 错误 %d: %s", exc.status, exc, exc_info=True)
                    self._fail_count += 1
            except Exception as exc:
                logger.error("TronListener 轮询异常: %s", exc, exc_info=True)
                self._fail_count += 1

            await asyncio.sleep(self.poll_interval)

    # ------------------------------------------------------------------ #
    # 内部方法
    # ------------------------------------------------------------------ #

    async def _load_seen_tx(self):
        """从 DB 加载近 7 天的 tx_hash，防重启后重复核销"""
        from database import get_processed_tx_hashes
        hashes = await get_processed_tx_hashes(self.db_pool)
        self._seen_tx = set(hashes)
        logger.info("已加载历史 tx_hash: %d 条", len(self._seen_tx))

    async def _poll(self):
        """
        拉取最近 FETCH_LIMIT 条转入记录，逐条去重后处理。
        短时间内同时有多笔转账时，全部都能被覆盖到。
        """
        transfers = await self._fetch_trc20_transfers()
        if not transfers:
            return

        for tx in transfers:
            tx_hash = tx.get("transaction_id", "").strip()
            if not tx_hash or tx_hash in self._seen_tx:
                continue

            # TronGrid v1 TRC20 接口的 to_address / from_address 字段
            to_addr   = tx.get("to_address",   tx.get("to",   "")).strip()
            from_addr = tx.get("from_address", tx.get("from", "")).strip()

            if to_addr.lower() != self.wallet.lower():
                # 转出交易，忽略
                self._seen_tx.add(tx_hash)
                continue

            # value 字段是字符串形式的整数 sun 数量
            raw_value   = tx.get("value", "0")
            usdt_amount = sun_to_usdt(raw_value)

            logger.info(
                "📥 转入 %s USDT | hash=%s | from=%s",
                usdt_amount, tx_hash, from_addr,
            )

            # 先加入内存 set，防止本轮循环内同一 hash 重入
            self._seen_tx.add(tx_hash)

            # 核销处理（含 DB 写入）
            await self._on_transfer(tx_hash, usdt_amount, from_addr)

    async def _fetch_trc20_transfers(self) -> list[dict]:
        """
        调用 TronGrid v1 接口，获取钱包最近 FETCH_LIMIT 条 TRC20 转账。
        使用 only_to=true 只返回转入，减少无效数据量。
        """
        url    = f"{TRONGRID_API}/v1/accounts/{self.wallet}/transactions/trc20"
        params = {
            "contract_address": USDT_CONTRACT,
            "only_to":          "true",
            "limit":            FETCH_LIMIT,
        }
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

    async def _on_transfer(
        self,
        tx_hash:     str,
        usdt_amount: Decimal,
        from_addr:   str,
    ):
        """
        核销主流程：
          1. 精确匹配未过期 pending 订单（DB 层保证）
          2. 乐观锁 UPDATE status='paid'（防并发重入）
          3. 记录 tx_hash 到 DB
          4. 通知用户支付确认
          5. 调用 fulfillment 提货
        """
        from database import (
            find_pending_order_by_amount,
            mark_order_paid,
            record_tx_hash,
        )
        from fulfillment import fulfill_order

        # ── 1. 匹配订单 ──────────────────────────────────────────────
        order = await find_pending_order_by_amount(self.db_pool, usdt_amount)
        if not order:
            logger.info(
                "⚠️  %s USDT 未匹配到任何有效订单（可能已过期或金额有误）",
                usdt_amount,
            )
            await record_tx_hash(self.db_pool, tx_hash, None, usdt_amount)
            return

        order_id    = order["id"]
        user_tg_id  = order["user_id"]
        product_id  = order.get("product_id")
        order_type  = order.get("order_type", "purchase")
        base_amount = order["amount"]
        final_amount = order["final_amount"]
        logger.info(
            "✅ 匹配成功 | 订单 #%d | 用户 %d | 类型 %s",
            order_id, user_tg_id, order_type,
        )

        # ── 2. 乐观锁标记支付（幂等，防二次触发） ────────────────────
        updated = await mark_order_paid(self.db_pool, order_id, tx_hash)
        if not updated:
            logger.warning("订单 #%d 已被处理，跳过（tx=%s）", order_id, tx_hash)
            return

        # ── 3. 持久化 tx_hash ────────────────────────────────────────
        await record_tx_hash(self.db_pool, tx_hash, order_id, usdt_amount)

        # ── 4. 通知用户：收款确认 ────────────────────────────────────
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

        # ── 5. 收尾（充值入账 / 购买提货，并释放 Redis 锁）────────────
        await fulfill_order(
            db_pool=self.db_pool,
            bot=self.bot,
            order_id=order_id,
            user_tg_id=user_tg_id,
            product_id=product_id,
            order_type=order_type,
            base_amount=base_amount,
            redis_client=self.redis,
            final_amount=final_amount,
        )

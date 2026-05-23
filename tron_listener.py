"""
tron_listener.py
链上监听模块：轮询 TronGrid API，识别 TRC20-USDT 转账，触发订单核销。
"""

import asyncio
import logging
import aiohttp
from decimal import Decimal

logger = logging.getLogger(__name__)

# USDT TRC20 合约地址（主网）
USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"

# TronGrid 公共节点（无需 API Key 亦可，有 Key 速率更高）
TRONGRID_API = "https://api.trongrid.io"


def sun_to_usdt(sun: int) -> Decimal:
    """TRC20 USDT 精度为 6 位，1 USDT = 1_000_000 SUN"""
    return Decimal(sun) / Decimal("1000000")


class TronListener:
    """
    定时轮询固定收款地址的 TRC20 转账记录，
    将匹配到的交易交给回调函数处理。
    """

    def __init__(
        self,
        wallet_address: str,
        db_pool,
        bot,
        trongrid_api_key: str = "",
        poll_interval: int = 15,
    ):
        self.wallet = wallet_address
        self.db_pool = db_pool
        self.bot = bot
        self.api_key = trongrid_api_key
        self.poll_interval = poll_interval
        # 已处理的交易哈希缓存（内存级，重启后从 DB 重建）
        self._seen_tx: set[str] = set()

    # ------------------------------------------------------------------
    # 公共入口
    # ------------------------------------------------------------------
    async def start(self):
        """在后台任务中循环运行"""
        logger.info("🔗 TronListener 启动，监听地址: %s", self.wallet)
        await self._load_seen_tx()
        while True:
            try:
                await self._poll()
            except Exception as exc:
                logger.error("TronListener 轮询异常: %s", exc, exc_info=True)
            await asyncio.sleep(self.poll_interval)

    # ------------------------------------------------------------------
    # 内部方法
    # ------------------------------------------------------------------
    async def _load_seen_tx(self):
        """从数据库加载已处理的 tx_hash，防止重启后重复核销"""
        from database import get_processed_tx_hashes
        hashes = await get_processed_tx_hashes(self.db_pool)
        self._seen_tx = set(hashes)
        logger.info("已加载 %d 条历史交易哈希", len(self._seen_tx))

    async def _poll(self):
        """抓取最新转账并逐条处理"""
        transfers = await self._fetch_trc20_transfers()
        for tx in transfers:
            tx_hash = tx.get("transaction_id", "")
            if not tx_hash or tx_hash in self._seen_tx:
                continue
            # 只处理转入本钱包的交易
            to_addr = tx.get("to", "")
            if to_addr.lower() != self.wallet.lower():
                continue
            amount_sun = int(tx.get("value", 0))
            usdt_amount = sun_to_usdt(amount_sun)
            from_addr = tx.get("from", "")
            block_ts = tx.get("block_timestamp", 0)

            logger.info(
                "发现转入: %s USDT | hash=%s | from=%s",
                usdt_amount, tx_hash, from_addr,
            )

            # 交给核销逻辑处理
            await self._on_transfer(tx_hash, usdt_amount, from_addr, block_ts)
            self._seen_tx.add(tx_hash)

    async def _fetch_trc20_transfers(self) -> list[dict]:
        """调用 TronGrid 查询最近 TRC20 转入记录"""
        url = f"{TRONGRID_API}/v1/accounts/{self.wallet}/transactions/trc20"
        params = {
            "contract_address": USDT_CONTRACT,
            "only_to": "true",
            "limit": 50,
        }
        headers = {}
        if self.api_key:
            headers["TRON-PRO-API-KEY"] = self.api_key

        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    logger.warning("TronGrid 返回 %d", resp.status)
                    return []
                data = await resp.json()
                return data.get("data", [])

    async def _on_transfer(
        self,
        tx_hash: str,
        usdt_amount: Decimal,
        from_addr: str,
        block_ts: int,
    ):
        """
        核销逻辑：
        1. 按 final_amount 精确匹配 pending 订单
        2. 更新订单状态为 paid
        3. 调用 YiCiYuan API 提货
        4. 通知用户
        """
        from database import (
            find_pending_order_by_amount,
            mark_order_paid,
            get_product_by_id,
            get_system_config,
            record_tx_hash,
        )
        from yiciyuan_api import YiCiyuanAPI
        from fulfillment import fulfill_order

        # ---- 1. 匹配订单 ----
        order = await find_pending_order_by_amount(self.db_pool, usdt_amount)
        if not order:
            logger.info("金额 %s USDT 未匹配到任何待付订单，忽略", usdt_amount)
            # 仍然记录 tx_hash 防止重复处理
            await record_tx_hash(self.db_pool, tx_hash, None, usdt_amount)
            return

        order_id = order["id"]
        user_tg_id = order["user_id"]
        product_id = order["product_id"]

        logger.info("✅ 匹配订单 #%d | 用户 %d | 商品 %d", order_id, user_tg_id, product_id)

        # ---- 2. 标记已支付（幂等写入） ----
        updated = await mark_order_paid(self.db_pool, order_id, tx_hash)
        if not updated:
            logger.warning("订单 #%d 已处理，跳过", order_id)
            return

        # 记录 tx_hash
        await record_tx_hash(self.db_pool, tx_hash, order_id, usdt_amount)

        # ---- 3. 通知用户：支付已确认 ----
        try:
            await self.bot.send_message(
                user_tg_id,
                f"✅ *支付已确认！*\n\n"
                f"📦 订单号：`#{order_id}`\n"
                f"💰 金额：`{usdt_amount} USDT`\n\n"
                f"⏳ 正在为您提货，请稍候...",
                parse_mode="Markdown",
            )
        except Exception as e:
            logger.error("发送确认消息失败: %s", e)

        # ---- 4. 提货 ----
        await fulfill_order(
            db_pool=self.db_pool,
            bot=self.bot,
            order_id=order_id,
            user_tg_id=user_tg_id,
            product_id=product_id,
        )

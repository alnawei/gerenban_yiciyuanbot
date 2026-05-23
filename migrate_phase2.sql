"""
tron_listener.py — TronGrid USDT(TRC20) 监听器
- 监控波场链上 USDT 转账
- 自动核销订单并通知用户
"""

import asyncio
import logging
import time
from decimal import Decimal, ROUND_DOWN
import aiohttp

# 导入项目内部模块
from config import BOT_TOKEN, USDT_WALLET, TRONGRID_API_KEY, TRON_POLL_INTERVAL
from database import (
    get_db_pool,
    find_pending_order_by_amount,
    get_listener_cursor_ms,
    set_listener_cursor_ms,
    get_processed_tx_hashes,
    tx_hash_already_used,
    record_tx_hash,
    get_pending_order_by_id
)
from aiogram import Bot

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("TronListener")

USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
TRONGRID_API = "https://api.trongrid.io"
FETCH_LIMIT = 200
MIN_POLL_INTERVAL = 2
LISTENER_OVERLAP_MS = 60000  # 1分钟重叠，防止漏单
LISTENER_DEFAULT_LOOKBACK_MS = 604800000  # 首次扫描回溯7天

def sun_to_usdt(sun_value) -> Decimal:
    return (Decimal(str(sun_value)) / Decimal("1000000")).quantize(
        Decimal("0.000001"), rounding=ROUND_DOWN
    )

def _get_tx_timestamp(tx: dict) -> int:
    for key in ("block_timestamp", "blockTimestamp"):
        val = tx.get(key)
        if val: return int(val)
    return int(time.time() * 1000)

class TronListener:
    def __init__(self, wallet, db_pool, bot, api_key="", interval=15):
        self.wallet = wallet
        self.db_pool = db_pool
        self.bot = bot
        self.api_key = api_key
        self.poll_interval = max(interval, MIN_POLL_INTERVAL)
        self._seen_tx = set()
        self._min_ts = 0

    async def start(self):
        logger.info(f"🔗 监听启动 | 地址: {self.wallet} | 间隔: {self.poll_interval}s")
        # 1. 加载已处理的 Hash
        hashes = await get_processed_tx_hashes(self.db_pool)
        self._seen_tx = set(hashes)
        
        # 2. 初始化扫描游标
        stored = await get_listener_cursor_ms(self.db_pool)
        if stored > 0:
            self._min_ts = stored - LISTENER_OVERLAP_MS
        else:
            self._min_ts = int(time.time() * 1000) - LISTENER_DEFAULT_LOOKBACK_MS
            logger.info("首次运行，开始回溯扫描...")

        while True:
            try:
                await self._poll()
            except Exception as e:
                logger.error(f"轮询异常: {e}", exc_info=True)
            await asyncio.sleep(self.poll_interval)

    async def _poll(self):
        url = f"{TRONGRID_API}/v1/accounts/{self.wallet}/transactions/trc20"
        params = {
            "contract_address": USDT_CONTRACT,
            "only_to": "true",
            "limit": FETCH_LIMIT,
            "order_by": "block_timestamp,asc",
            "min_timestamp": str(max(0, self._min_ts))
        }
        headers = {"TRON-PRO-API-KEY": self.api_key} if self.api_key else {}

        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    logger.error(f"TronGrid 错误: {resp.status}")
                    return
                data = await resp.json()
                
                max_ts = self._min_ts
                for tx in data.get("data", []):
                    tx_hash = tx.get("transaction_id")
                    ts = _get_tx_timestamp(tx)
                    max_ts = max(max_ts, ts)

                    if tx_hash in self._seen_tx: continue
                    
                    amount = sun_to_usdt(tx.get("value", 0))
                    await self._handle_transfer(tx_hash, amount)
                    self._seen_tx.add(tx_hash)

                if max_ts > self._min_ts:
                    self._min_ts = max_ts + 1
                    await set_listener_cursor_ms(self.db_pool, max_ts)

    async def _handle_transfer(self, tx_hash, amount):
        # 延迟导入防止循环引用
        from settlement import atomic_mark_paid_and_record, atomic_settle_recharge
        from fulfillment import fulfill_purchase_after_paid, notify_payment_received, notify_recharge_completed

        order = await find_pending_order_by_amount(self.db_pool, amount)
        if not order:
            logger.info(f"忽略未匹配转账: {amount} USDT (Hash: {tx_hash})")
            await record_tx_hash(self.db_pool, tx_hash, None, amount)
            return

        order_id = order['id']
        logger.info(f"✅ 匹配订单 #{order_id} | 金额: {amount}")

        if order.get('order_type') == 'recharge':
            if await atomic_settle_recharge(self.db_pool, order_id, order['user_id'], order['amount'], tx_hash, amount):
                await notify_recharge_completed(self.bot, self.db_pool, order['user_id'], order_id, order['amount'])
        else:
            if await atomic_mark_paid_and_record(self.db_pool, order_id, tx_hash, amount):
                await notify_payment_received(self.bot, order['user_id'], order_id, amount, tx_hash)
                await fulfill_purchase_after_paid(self.db_pool, self.bot, order_id, order['user_id'], order['product_id'], tx_hash, amount)

async def main():
    pool = await get_db_pool()
    bot = Bot(token=BOT_TOKEN)
    listener = TronListener(
        wallet=USDT_WALLET,
        db_pool=pool,
        bot=bot,
        api_key=TRONGRID_API_KEY,
        interval=int(TRON_POLL_INTERVAL)
    )
    await listener.start()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

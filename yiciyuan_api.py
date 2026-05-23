"""
yiciyuan_api.py
一次元 API 封装：签名、请求、商品拉取、下单提货。
网络/HTTP 异常时记录日志并向管理员发送 Telegram 告警。
"""

import hashlib
import logging

import aiohttp

logger = logging.getLogger(__name__)


class YiCiyuanAPIError(Exception):
    """一次元 API 调用失败。"""

    def __init__(self, endpoint: str, message: str, *, http_status: int | None = None):
        self.endpoint = endpoint
        self.http_status = http_status
        super().__init__(message)


class YiCiyuanAPI:
    def __init__(
        self,
        shop_url: str,
        app_id: str,
        app_key: str,
        bot=None,
    ):
        self.base_url = shop_url.rstrip("/")
        self.app_id = app_id
        self.app_key = app_key
        self.bot = bot

    def generate_sign(self, data: dict) -> str:
        filtered = {k: v for k, v in data.items() if v != "" and k != "sign"}
        sorted_keys = sorted(filtered.keys())
        query_string = "&".join([f"{k}={filtered[k]}" for k in sorted_keys])
        sign_str = f"{query_string}&key={self.app_key}"
        return hashlib.md5(sign_str.encode("utf-8")).hexdigest()

    async def _alert_admin(self, endpoint: str, reason: str, extra: str = ""):
        logger.error("一次元 API [%s]: %s %s", endpoint, reason, extra)
        if not self.bot:
            return
        from notifications import notify_admin

        body = (
            "🚨 *一次元 API 异常*\n\n"
            f"📡 端点：`{endpoint}`\n"
            f"❗ 原因：{reason}"
        )
        if extra:
            body += f"\n📎 详情：`{extra}`"
        await notify_admin(self.bot, body)

    async def post(self, endpoint: str, data: dict | None = None) -> dict:
        if data is None:
            data = {}
        data["app_id"] = self.app_id
        data["sign"] = self.generate_sign(data)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}{endpoint}",
                    data=data,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        code = result.get("code")
                        if code not in (0, None, "0") and "/order/" in endpoint:
                            msg = result.get("msg", "业务错误")
                            await self._alert_admin(
                                endpoint,
                                f"业务码 {code}: {msg}",
                            )
                        return result
                    msg = f"HTTP {resp.status}"
                    await self._alert_admin(endpoint, msg)
                    raise YiCiyuanAPIError(endpoint, msg, http_status=resp.status)
        except YiCiyuanAPIError:
            raise
        except Exception as e:
            await self._alert_admin(endpoint, "请求异常", str(e))
            raise YiCiyuanAPIError(endpoint, str(e)) from e

    async def get_items(self) -> dict:
        return await self.post("/shared/commodity/items")

    async def create_order(self, commodity_code: str, order_id: str) -> dict:
        return await self.post(
            "/shared/order/create",
            {
                "commodity_code": commodity_code,
                "out_order_no": order_id,
                "num": 1,
            },
        )

    async def query_order(self, order_id: str) -> dict:
        return await self.post(
            "/shared/order/query",
            {"out_order_no": order_id},
        )

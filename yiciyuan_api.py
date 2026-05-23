"""
yiciyuan_api.py
一次元 API 封装：签名、请求、商品拉取、下单提货。
"""

import hashlib
import aiohttp
import logging

logger = logging.getLogger(__name__)


class YiCiyuanAPI:
    def __init__(self, shop_url: str, app_id: str, app_key: str):
        self.base_url = shop_url.rstrip("/")
        self.app_id = app_id
        self.app_key = app_key

    # ------------------------------------------------------------------
    # 签名
    # ------------------------------------------------------------------
    def generate_sign(self, data: dict) -> str:
        filtered = {k: v for k, v in data.items() if v != "" and k != "sign"}
        sorted_keys = sorted(filtered.keys())
        query_string = "&".join([f"{k}={filtered[k]}" for k in sorted_keys])
        sign_str = f"{query_string}&key={self.app_key}"
        return hashlib.md5(sign_str.encode("utf-8")).hexdigest()

    # ------------------------------------------------------------------
    # 通用 POST
    # ------------------------------------------------------------------
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
                        return await resp.json()
                    return {"code": resp.status, "msg": "网络请求失败"}
        except Exception as e:
            logger.error("API 请求异常 [%s]: %s", endpoint, e)
            return {"code": 500, "msg": str(e)}

    # ------------------------------------------------------------------
    # 业务接口
    # ------------------------------------------------------------------
    async def get_items(self) -> dict:
        """获取全量商品列表"""
        return await self.post("/shared/commodity/items")

    async def create_order(self, commodity_code: str, order_id: str) -> dict:
        """
        下单提货。
        接口文档字段（一次元标准）：
          commodity_code  商品编码
          out_order_no    外部订单号（用我们自己的 order_id）
          num             购买数量，固定 1
        """
        return await self.post(
            "/shared/order/create",
            {
                "commodity_code": commodity_code,
                "out_order_no": order_id,
                "num": 1,
            },
        )

    async def query_order(self, order_id: str) -> dict:
        """查询订单状态（用于重试核查）"""
        return await self.post(
            "/shared/order/query",
            {"out_order_no": order_id},
        )

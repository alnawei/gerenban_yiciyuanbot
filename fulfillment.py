"""
fulfillment.py
提货模块：订单核销后，调用远端 API 获取商品并推送给用户。
"""

import logging
from aiogram import Bot
from aiogram.utils.keyboard import InlineKeyboardBuilder

logger = logging.getLogger(__name__)


async def fulfill_order(
    db_pool,
    bot: Bot,
    order_id: int,
    user_tg_id: int,
    product_id: int,
):
    """
    提货主流程：
    1. 查商品信息 → 拿到 remote_code
    2. 读取系统配置 → 拿到 API 凭证
    3. 调用 YiCiYuan 下单接口
    4. 将卡密/代理信息推送给用户
    5. 写入 delivery_records 表
    """
    from database import (
        get_product_by_id,
        get_system_config,
        save_delivery_record,
        mark_order_fulfilled,
    )
    from yiciyuan_api import YiCiyuanAPI

    # ---- 1. 查商品 ----
    product = await get_product_by_id(db_pool, product_id)
    if not product:
        logger.error("提货失败：商品 #%d 不存在", product_id)
        await _notify_failure(bot, user_tg_id, order_id, "商品信息不存在，请联系客服")
        return

    remote_code = product.get("remote_code")
    product_name = product.get("display_name") or product.get("remote_name")

    # ---- 2. 读取 API 配置 ----
    config = await get_system_config(db_pool)
    shop_url = config.get("shop_url")
    app_id = config.get("app_id")
    app_key = config.get("app_key")
    customer_service = config.get("customer_service", "")

    if not all([shop_url, app_id, app_key]):
        logger.error("提货失败：远端 API 未配置")
        await _notify_failure(bot, user_tg_id, order_id, "系统配置异常，请联系客服")
        return

    # ---- 3. 调用 API 提货 ----
    api = YiCiyuanAPI(shop_url, app_id, app_key)
    result = await api.create_order(
        commodity_code=remote_code,
        order_id=str(order_id),
    )

    logger.info("YiCiYuan 提货结果 [order #%d]: %s", order_id, result)

    if result.get("code") != 0:
        err_msg = result.get("msg", "未知错误")
        logger.error("提货失败 [order #%d]: %s", order_id, err_msg)
        await save_delivery_record(db_pool, order_id, product_id, "failed", "", err_msg)
        await _notify_failure(bot, user_tg_id, order_id, err_msg, customer_service)
        return

    # ---- 4. 解析提货内容 ----
    # YiCiYuan 一般在 data.list 里返回卡密列表
    data = result.get("data", {})
    items: list = data.get("list", [])
    if not items and isinstance(data, list):
        items = data

    delivery_text = _format_delivery_items(items, product_name)

    # ---- 5. 推送给用户 ----
    builder = InlineKeyboardBuilder()
    if customer_service:
        cs_text = "联系客服" if customer_service.startswith("http") else f"@{customer_service.lstrip('@')}"
        builder.button(text="💬 联系客服", url=customer_service if customer_service.startswith("http") else f"https://t.me/{customer_service.lstrip('@')}")

    try:
        await bot.send_message(
            user_tg_id,
            f"🎉 *发货成功！*\n\n"
            f"📦 商品：{product_name}\n"
            f"📋 订单号：`#{order_id}`\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"{delivery_text}\n"
            f"━━━━━━━━━━━━━━\n\n"
            f"感谢您的购买！如有问题请联系客服。",
            reply_markup=builder.as_markup() if customer_service else None,
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error("推送发货消息失败 [user %d]: %s", user_tg_id, e)

    # ---- 6. 写交付记录 ----
    raw_content = str(items)
    await save_delivery_record(db_pool, order_id, product_id, "success", raw_content, "")
    await mark_order_fulfilled(db_pool, order_id)
    logger.info("✅ 订单 #%d 提货完成", order_id)


def _format_delivery_items(items: list, product_name: str) -> str:
    """将 API 返回的卡密列表格式化为用户可读的文本"""
    if not items:
        return "（提货内容为空，请截图联系客服）"

    lines = []
    for i, item in enumerate(items, 1):
        if isinstance(item, dict):
            # 常见字段：card_no / card_pwd / account / password / info / content
            card = item.get("card_no") or item.get("account") or ""
            pwd = item.get("card_pwd") or item.get("password") or ""
            info = item.get("info") or item.get("content") or ""

            if card and pwd:
                lines.append(f"*#{i}*\n账号：`{card}`\n密码：`{pwd}`")
            elif card:
                lines.append(f"*#{i}*\n`{card}`")
            elif info:
                lines.append(f"*#{i}*\n`{info}`")
            else:
                # 兜底：直接序列化整条记录
                lines.append(f"*#{i}*\n`{item}`")
        else:
            lines.append(f"*#{i}*\n`{item}`")

    return "\n\n".join(lines)


async def _notify_failure(
    bot: Bot,
    user_tg_id: int,
    order_id: int,
    reason: str,
    customer_service: str = "",
):
    """提货失败时通知用户"""
    builder = InlineKeyboardBuilder()
    if customer_service:
        cs_url = customer_service if customer_service.startswith("http") else f"https://t.me/{customer_service.lstrip('@')}"
        builder.button(text="💬 联系客服处理", url=cs_url)

    try:
        await bot.send_message(
            user_tg_id,
            f"❌ *提货遇到问题*\n\n"
            f"📋 订单号：`#{order_id}`\n"
            f"原因：{reason}\n\n"
            f"您的款项已到账，请联系客服手动处理，我们会第一时间为您解决！",
            reply_markup=builder.as_markup() if customer_service else None,
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error("发送失败通知异常: %s", e)
